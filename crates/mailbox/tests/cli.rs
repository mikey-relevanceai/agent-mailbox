//! CLI ↔ daemon integration tests (card 06 + daemon hardening), driving the REAL
//! `mailbox` binary.
//!
//! Every test spawns an actual `mailbox serve` daemon in a tempdir and drives it
//! as separate client processes (or raw socket bytes), so it exercises the exact
//! process boundary — socket bind/connect, one-shot request/reply, single-writer
//! daemon — that ships. Nothing here opens the database directly.
//!
//! Flakiness discipline: we NEVER sleep a fixed amount waiting for the daemon to
//! come up. We poll the socket for connectability (`wait_for_socket`). Where a
//! bound (timeout / connection cap) is under test, we shrink it via the daemon's
//! env overrides so the assertion is fast and deterministic rather than waiting
//! the production value. The daemon child is always reaped on drop.

use std::io::{Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixStream as StdUnixStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

use tempfile::TempDir;

// ---- daemon process helpers ---------------------------------------------------

mod common;

// The ONE session-stripping spawner + bin path, shared by every test binary
// (tests/common): a test must never inherit the developer's CLAUDE_CODE_SESSION_ID.
use common::{mailbox_bin as bin, mailbox_command};

/// The reference stub adapter binary, beside the `mailbox` bin (built if missing).
///
/// These CLI tests exercise the watch/status *surface*, not adapter behaviour, so
/// a `github-pr` watch points its adapter at the harmless stub (via
/// `MAILBOX_GH_ADAPTER_BIN`) rather than the real poller — the stub ignores the
/// github config's extra fields, publishes on the topic, and stays `running`, so
/// the watch lifecycle is deterministic with no `gh` / network involved. Real
/// github-pr adapter behaviour is covered in `github_pr_e2e.rs`.
fn stub_bin() -> String {
    let dir = Path::new(bin())
        .parent()
        .expect("mailbox bin has a parent dir")
        .to_path_buf();
    let stub = dir.join("mailbox-stub-adapter");
    if !stub.exists() {
        let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string());
        let status = Command::new(cargo)
            .args(["build", "-p", "mailbox-stub-adapter"])
            .status()
            .expect("build mailbox-stub-adapter");
        assert!(status.success(), "failed to build mailbox-stub-adapter");
    }
    stub.to_str().expect("stub bin path is utf8").to_string()
}

/// The socket path the daemon derives from a DB path (`<db-parent>/mailbox.sock`).
fn socket_for(db_path: &Path) -> PathBuf {
    db_path.parent().unwrap().join("mailbox.sock")
}

/// Spawn a `mailbox serve` daemon against `db_path` with extra env (e.g. limit
/// overrides). stdin is null; stderr is inherited so logs show under --nocapture.
fn spawn_serve(db_path: &Path, extra_env: &[(&str, &str)]) -> Child {
    let mut cmd = mailbox_command();
    cmd.arg("serve")
        .env("AGENT_MAILBOX_DB", db_path)
        // The daemon reads Claude Code's session registry to find each subscriber's
        // inbox socket, so it MUST be pointed at a tempdir — without this a test could
        // deliver its wake onto a REAL session. Derived from the DB path so it follows
        // every caller.
        .env(
            "MAILBOX_CLAUDE_SESSIONS_DIR",
            db_path
                .parent()
                .expect("db path has a parent")
                .join("claude-sessions"),
        )
        .env("RUST_LOG", "error")
        .stdin(Stdio::null());
    for (k, v) in extra_env {
        cmd.env(k, v);
    }
    cmd.spawn().expect("spawn mailbox serve")
}

/// Poll until the socket accepts a real connection, or panic. Connectability
/// (not mere file existence) is the right readiness signal — the stale-socket
/// test deliberately pre-creates a non-socket node at the path, and only a
/// genuine connect distinguishes "daemon is listening" from "a stale file is
/// sitting there". Polling — not a fixed sleep — keeps startup robust under load.
///
/// A successful probe connect is immediately closed; it transiently occupies a
/// connection slot, so the connection-cap test does not rely on this and holds
/// its slot via a retry loop instead.
fn wait_for_socket(socket: &Path, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        if StdUnixStream::connect(socket).is_ok() {
            return;
        }
        if Instant::now() >= deadline {
            panic!(
                "daemon socket {} not ready within {timeout:?}",
                socket.display()
            );
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// A running daemon over a fresh tempdir DB, reaped (killed + waited) on drop.
struct Daemon {
    child: Child,
    db_path: PathBuf,
    socket_path: PathBuf,
    _dir: TempDir,
}

impl Daemon {
    fn start() -> Self {
        Self::start_with_env(&[])
    }

    fn start_with_env(extra_env: &[(&str, &str)]) -> Self {
        let dir = TempDir::new().expect("tempdir");
        let db_path = dir.path().join("mailbox.db");
        let socket_path = socket_for(&db_path);
        let child = spawn_serve(&db_path, extra_env);
        let daemon = Daemon {
            child,
            db_path,
            socket_path,
            _dir: dir,
        };
        wait_for_socket(&daemon.socket_path, Duration::from_secs(10));
        daemon
    }

    /// Run a `mailbox` client command against this daemon's DB.
    fn run(&self, args: &[&str]) -> Output {
        self.client(args).output().expect("run mailbox client")
    }

    /// Run a one-shot `mailbox` client command **as `session`**, via the env var
    /// Claude Code exports into every tool call. That is the only way a command
    /// learns whose session it is — there is no `--session` flag.
    fn run_as(&self, session: &str, args: &[&str]) -> Output {
        self.client(args)
            .env("CLAUDE_CODE_SESSION_ID", session)
            .output()
            .expect("run mailbox client")
    }

    /// A client command pointed at this daemon's DB **and** its tempdir registry.
    ///
    /// Client commands read Claude Code's session registry too — `status` reports the
    /// caller's wake verdict from it, `subscribe`/`watch` refuse on it — so they get
    /// the same tempdir the daemon does. Without it a test would read the developer's
    /// real `~/.claude/sessions` and answer differently on a laptop than in CI.
    fn client(&self, args: &[&str]) -> Command {
        let mut cmd = mailbox_command();
        cmd.args(args)
            .env("AGENT_MAILBOX_DB", &self.db_path)
            .env(
                "MAILBOX_CLAUDE_SESSIONS_DIR",
                self.db_path
                    .parent()
                    .expect("db path has a parent")
                    .join("claude-sessions"),
            )
            .env("RUST_LOG", "error");
        cmd
    }

    /// Send a raw control line (a newline is appended) over the socket and return
    /// the daemon's reply, reading to EOF. For exercising malformed/adversarial
    /// frames the typed client would never send.
    fn raw_line(&self, line: &str) -> String {
        raw_send(&self.socket_path, line.as_bytes(), true)
    }

    /// Send raw bytes verbatim (no appended newline) and return the reply.
    fn raw_bytes(&self, bytes: &[u8]) -> String {
        raw_send(&self.socket_path, bytes, false)
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        // Graceful teardown: SIGTERM lets `serve` run its shutdown, which tears
        // down and reaps any supervised adapter it spawned (e.g. a `github-pr`
        // watch's poller) — so nothing is orphaned. Fall back to SIGKILL if it
        // does not exit promptly. `serve` with no adapters exits in a few ms, so
        // this does not slow the common case.
        let pid = nix::unistd::Pid::from_raw(self.child.id() as i32);
        let _ = nix::sys::signal::kill(pid, nix::sys::signal::Signal::SIGTERM);
        for _ in 0..150 {
            match self.child.try_wait() {
                Ok(Some(_)) => return,
                Ok(None) => std::thread::sleep(Duration::from_millis(20)),
                Err(_) => break,
            }
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Connect, write `bytes` (optionally + newline), close the write half, and read
/// the reply to EOF. Write errors are tolerated (the daemon may close first on a
/// rejected/oversized frame).
fn raw_send(socket: &Path, bytes: &[u8], append_newline: bool) -> String {
    let mut stream = StdUnixStream::connect(socket).expect("connect raw");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("set read timeout");
    let _ = stream.write_all(bytes);
    if append_newline {
        let _ = stream.write_all(b"\n");
    }
    let _ = stream.shutdown(std::net::Shutdown::Write);
    let mut reply = String::new();
    let _ = stream.read_to_string(&mut reply);
    reply
}

fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

fn assert_ok(output: &Output, what: &str) {
    assert!(
        output.status.success(),
        "{what} should exit 0; got {:?}\nstderr: {}",
        output.status.code(),
        stderr(output)
    );
}

fn parse_json(text: &str) -> serde_json::Value {
    serde_json::from_str(text.trim()).unwrap_or_else(|e| panic!("not JSON: {e}\n{text}"))
}

/// Whether the test runs as root (uid 0), which bypasses filesystem permission
/// checks — the permission-failure test is meaningless there and is skipped.
fn is_root() -> bool {
    Command::new("id")
        .arg("-u")
        .output()
        .ok()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim() == "0")
        .unwrap_or(false)
}

// ==== AC1: publish → read round-trip through the real binary (+ --json) ========

#[test]
fn ac1_publish_read_round_trip_via_binary() {
    let daemon = Daemon::start();
    let topic = "github.pr.octocat/hello-world#1";
    let session = "sess-rt";

    // Subscribe BEFORE publishing — baseline-on-subscribe means a subscription
    // created after the publish would baseline past it and see nothing.
    assert_ok(&daemon.run_as(session, &["subscribe", topic]), "subscribe");

    let publish = daemon.run(&[
        "publish",
        topic,
        "--body",
        r#"{"marker":"ROUND_TRIP_OK","n":7}"#,
    ]);
    assert_ok(&publish, "publish");

    let read = daemon.run_as(session, &["--json", "read"]);
    assert_ok(&read, "read");
    let value = parse_json(&stdout(&read));
    assert_eq!(value["result"], "read");
    let events = value["events"].as_array().expect("events array");
    assert_eq!(events.len(), 1, "exactly one unread event");
    assert_eq!(events[0]["topic"], topic);
    assert_eq!(events[0]["body"]["marker"], "ROUND_TRIP_OK");
    assert_eq!(events[0]["body"]["n"], 7);

    // A second read has advanced past it: nothing unread now (advance-on-read).
    let read2 = daemon.run_as(session, &["--json", "read"]);
    assert_ok(&read2, "second read");
    assert_eq!(
        parse_json(&stdout(&read2))["events"]
            .as_array()
            .unwrap()
            .len(),
        0
    );
}

// ==== AC2: a client with the daemon DOWN fails loudly (clear error, non-zero) ==

#[test]
fn ac2_client_fails_loudly_when_bridge_is_down() {
    let dir = TempDir::new().expect("tempdir");
    let db_path = dir.path().join("mailbox.db");

    let output = mailbox_command()
        .args(["read"])
        .env("CLAUDE_CODE_SESSION_ID", "s")
        .env("AGENT_MAILBOX_DB", &db_path)
        .env("RUST_LOG", "error")
        .output()
        .expect("run read with no daemon");

    assert!(
        !output.status.success(),
        "a client with no daemon must exit non-zero"
    );
    let err = stderr(&output);
    assert!(
        err.contains("bridge not running") && err.contains("mailbox serve"),
        "expected an actionable bridge-down error; got: {err}"
    );
    assert!(
        stdout(&output).trim().is_empty(),
        "human-mode error must not write to stdout"
    );
}

/// **`status` degrades instead of going blank when the bridge is down.**
///
/// A session's id and inbox topic are derivable locally, so `status` answers "who am
/// I, and what is my address" with no daemon at all — which is the one property the
/// deleted `whoami` command had that `status` did not. It still exits NON-ZERO
/// (ADR-0004: a socket client fails loud), because the rest of what `status` reports —
/// watches, subscriptions, unread — is genuinely missing, and it says so.
#[test]
fn status_still_answers_who_am_i_when_the_bridge_is_down() {
    let dir = TempDir::new().expect("tempdir");
    let db_path = dir.path().join("mailbox.db");

    let output = mailbox_command()
        .args(["status"])
        .env("CLAUDE_CODE_SESSION_ID", "s-alone")
        .env("AGENT_MAILBOX_DB", &db_path)
        .env("MAILBOX_CLAUDE_SESSIONS_DIR", dir.path().join("sessions"))
        .env("RUST_LOG", "error")
        .output()
        .expect("run status with no daemon");

    assert!(
        !output.status.success(),
        "the bridge being down is still a loud failure"
    );
    let out = stdout(&output);
    assert!(
        out.contains("session: s-alone") && out.contains("inbox topic: agent.s-alone"),
        "the identity fields are always knowable; got: {out}"
    );
    assert!(
        out.contains("UNREACHABLE"),
        "it must say what it could NOT tell you, not imply an empty mailbox; got: {out}"
    );
    assert!(
        stderr(&output).contains("mailbox serve"),
        "stderr keeps the actionable remedy"
    );

    // `--json` stays ONE document: the error shape a consumer already expects, with
    // the identity keys added.
    let output = mailbox_command()
        .args(["--json", "status"])
        .env("CLAUDE_CODE_SESSION_ID", "s-alone")
        .env("AGENT_MAILBOX_DB", &db_path)
        .env("MAILBOX_CLAUDE_SESSIONS_DIR", dir.path().join("sessions"))
        .env("RUST_LOG", "error")
        .output()
        .expect("run json status with no daemon");
    let value = parse_json(&stdout(&output));
    assert_eq!(value["result"], "error");
    assert_eq!(value["session"], "s-alone");
    assert_eq!(value["inbox_topic"], "agent.s-alone");
    assert_eq!(value["bridge"], "unreachable");
    // Derived locally, so it is knowable here too — `wake.rs` covers the verdict that
    // matters (a live session with no socket) end to end.
    assert_eq!(value["wake"], "unknown");
}

/// Both degradations at once: no session AND no bridge. Neither half is knowable, and
/// the command must say so twice over rather than fill either in. The one thing it
/// must NOT do is answer for a session nobody named — the same rule as the success
/// path, on the path where there is least to check it against.
#[test]
fn status_with_no_session_and_no_bridge_invents_neither_half() {
    let dir = TempDir::new().expect("tempdir");
    let db_path = dir.path().join("mailbox.db");

    let output = mailbox_command()
        .args(["--json", "status"])
        .env("AGENT_MAILBOX_DB", &db_path)
        .env("RUST_LOG", "error")
        .output()
        .expect("run json status with no session and no daemon");

    assert!(
        !output.status.success(),
        "the bridge being down is a loud failure even for a caller that is not a session"
    );
    let value = parse_json(&stdout(&output));
    assert_eq!(value["result"], "error");
    assert_eq!(value["bridge"], "unreachable");
    for key in ["session", "wake", "inbox_topic"] {
        assert!(
            value.get(key).is_none(),
            "{key} must be absent, not null, when there is no session to describe; got {value}"
        );
    }

    let human = mailbox_command()
        .args(["status"])
        .env("AGENT_MAILBOX_DB", &db_path)
        .env("RUST_LOG", "error")
        .output()
        .expect("run status with no session and no daemon");
    let out = stdout(&human);
    assert!(
        out.contains("session: none") && out.contains("UNREACHABLE"),
        "both missing halves must be named; got: {out}"
    );
    assert!(
        !out.contains("wake:") && !out.contains("inbox topic:"),
        "no session means no verdict and no address to report; got: {out}"
    );
}

#[test]
fn bridge_down_json_mode_emits_error_object() {
    let dir = TempDir::new().expect("tempdir");
    let db_path = dir.path().join("mailbox.db");

    let output = mailbox_command()
        .args(["--json", "status"])
        .env("CLAUDE_CODE_SESSION_ID", "s")
        .env("AGENT_MAILBOX_DB", &db_path)
        .env("RUST_LOG", "error")
        .output()
        .expect("run json status with no daemon");

    assert!(!output.status.success(), "must exit non-zero");
    let value = parse_json(&stdout(&output));
    assert_eq!(value["result"], "error");
    assert!(
        value["message"]
            .as_str()
            .unwrap_or_default()
            .contains("bridge not running"),
        "json error message should be actionable"
    );
}

#[test]
fn no_session_json_mode_emits_error_object() {
    // FIX 5 (card 16): an unresolvable session must honour the same `--json`
    // contract as a serviced failure — a typed error object on stdout — rather
    // than failing silently before any `fail()`/request call. The bridge is never
    // even contacted (resolution fails first), so no daemon is needed.
    //
    // Through `read`, whose whole answer is "my unread": `status` is no longer a
    // command that needs to know whose.
    let dir = TempDir::new().expect("tempdir");
    let db_path = dir.path().join("mailbox.db");

    let output = mailbox_command()
        .args(["--json", "read"])
        .env("AGENT_MAILBOX_DB", &db_path)
        .env("RUST_LOG", "error")
        // Ensure a real Claude Code session cannot leak in from the test runner.
        .env_remove("CLAUDE_CODE_SESSION_ID")
        .output()
        .expect("run json read with no session");

    assert!(!output.status.success(), "must exit non-zero");
    // NOT exit 2 — that is the wake code, and a resolution failure is an error.
    assert_ne!(
        output.status.code(),
        Some(2),
        "must not use the wake exit code"
    );
    let value = parse_json(&stdout(&output));
    assert_eq!(value["result"], "error");
    let message = value["message"].as_str().unwrap_or_default();
    // The message names the ONE place a session id comes from, so it is fixable.
    assert!(
        message.contains("CLAUDE_CODE_SESSION_ID"),
        "json error message should be actionable; got: {message}"
    );
}

// ==== AC3: status shows a watch with its interest count ========================

#[test]
fn ac3_status_shows_watch_with_interest_count() {
    // github-pr now resolves to a real adapter (card 10), so point it at the
    // harmless stub for a deterministic lifecycle without `gh`.
    let daemon = Daemon::start_with_env(&[("MAILBOX_GH_ADAPTER_BIN", &stub_bin())]);
    let session = "sess-watch";

    assert_ok(
        &daemon.run_as(
            session,
            &[
                "watch",
                "github-pr",
                "octocat/hello-world#42",
                "--interval",
                "30",
            ],
        ),
        "watch",
    );

    let status = daemon.run_as(session, &["--json", "status"]);
    assert_ok(&status, "status");
    let value = parse_json(&stdout(&status));
    assert_eq!(value["result"], "status");
    let watches = value["watches"].as_array().expect("watches array");
    assert_eq!(watches.len(), 1, "exactly one watch");
    let w = &watches[0];
    assert_eq!(w["kind"], "github-pr");
    assert_eq!(w["repo"], "octocat/hello-world");
    assert_eq!(w["pr"], 42);
    assert_eq!(w["interval_ms"], 30_000);
    assert_eq!(w["interest"], 1, "one interested session");
    // The supervisor spawns the adapter as part of `watch` (card 08/10), so by the
    // time it returns the watch is `running` with a child pid.
    assert_eq!(w["state"], "running");
    assert!(
        w.get("pid").and_then(|p| p.as_u64()).is_some(),
        "a running watch carries its child pid"
    );
    // The daemon's graceful Drop (SIGTERM → supervisor shutdown) reaps the adapter,
    // so no poller is orphaned when the test ends.
}

/// **`status` answers a human at a terminal.**
///
/// The watch table is bridge-global — every caller sees the same rows, with interest
/// counts summed across all sessions — so `status` has a real answer with no
/// `CLAUDE_CODE_SESSION_ID` at all. Refusing it made the one question a human most
/// wants of a bridge daemon ("what is it doing?") require inventing a session id they
/// are not; the same nonsense `send` and `agents` already avoid.
#[test]
fn status_with_no_session_reports_the_bridge_half() {
    let daemon = Daemon::start_with_env(&[("MAILBOX_GH_ADAPTER_BIN", &stub_bin())]);
    assert_ok(
        &daemon.run_as("sess-a", &["watch", "github-pr", "octocat/hello-world#42"]),
        "watch",
    );

    // `run` (not `run_as`) exports no session — the human's shell.
    let status = daemon.run(&["--json", "status"]);
    assert_ok(&status, "status with no session must succeed");
    let value = parse_json(&stdout(&status));
    assert_eq!(value["result"], "status");

    let watches = value["watches"].as_array().expect("watches array");
    assert_eq!(watches.len(), 1, "the global half answers in full");
    assert_eq!(watches[0]["repo"], "octocat/hello-world");
    assert_eq!(
        watches[0]["interest"], 1,
        "interest counts every session, so it does not depend on the caller"
    );

    // The session keys are ABSENT, not null and not zeroed. A `subscription_count`
    // of 0 here would answer "how many topics am I on?" for a caller who is nobody.
    for key in [
        "session",
        "wake",
        "inbox",
        "subscriptions",
        "subscription_count",
        "unread",
    ] {
        assert!(
            value.get(key).is_none(),
            "{key} must be absent for a caller that is not a session; got {value}"
        );
    }

    // The human line says why the rest is missing, so nobody reads it as a fault.
    let human = daemon.run(&["status"]);
    assert_ok(&human, "human status with no session");
    let text = stdout(&human);
    assert!(
        text.contains("session: none") && text.contains("CLAUDE_CODE_SESSION_ID"),
        "the missing half must be explained, not silently dropped; got: {text}"
    );
    assert!(
        text.contains("octocat/hello-world#42"),
        "the watch table is the point of a sessionless status; got: {text}"
    );
    assert!(
        !text.contains("subscriptions:") && !text.contains("unread:"),
        "no session means no lists to report, not empty ones; got: {text}"
    );
}

/// A session caller's document is unchanged by the above: same keys, same places.
/// A Claude Code status line reads `.subscription_count` off it on every prompt.
#[test]
fn status_with_a_session_still_carries_the_session_keys_at_the_top_level() {
    let daemon = Daemon::start();
    let session = "sess-still-here";
    assert_ok(
        &daemon.run_as(session, &["subscribe", "github.pr.o/r#1"]),
        "subscribe",
    );

    let status = daemon.run_as(session, &["--json", "status"]);
    assert_ok(&status, "status");
    let value = parse_json(&stdout(&status));
    assert_eq!(value["result"], "status");
    assert_eq!(value["session"], session);
    assert_eq!(value["inbox"], format!("agent.{session}"));
    assert_eq!(value["subscription_count"], 1);
    assert!(value["wake"].is_string(), "the verdict is still a word");
    assert!(value["subscriptions"].is_array());
    assert!(value["unread"].is_array());
}

/// Two sessions share one refcounted watch; unwatch reports the sum-typed outcome.
#[test]
fn two_sessions_share_one_watch_refcounted() {
    // Point github-pr at the stub so the shared adapter runs deterministically
    // without `gh` (this test is about the refcount, not adapter behaviour).
    let daemon = Daemon::start_with_env(&[("MAILBOX_GH_ADAPTER_BIN", &stub_bin())]);
    let pr = "octocat/hello-world#7";

    assert_ok(
        &daemon.run_as("s1", &["watch", "github-pr", pr]),
        "watch s1",
    );
    assert_ok(
        &daemon.run_as("s2", &["watch", "github-pr", pr]),
        "watch s2",
    );

    let status = daemon.run_as("s1", &["--json", "status"]);
    assert_ok(&status, "status");
    let value = parse_json(&stdout(&status));
    let watches = value["watches"].as_array().unwrap();
    assert_eq!(watches.len(), 1, "still ONE shared watch");
    assert_eq!(watches[0]["interest"], 2, "two interested sessions");

    let unwatch = daemon.run_as("s1", &["--json", "unwatch", "github-pr", pr]);
    assert_ok(&unwatch, "unwatch s1");
    let uv = parse_json(&stdout(&unwatch));
    assert_eq!(uv["result"], "unwatched");
    // Sum-typed outcome: "dropped" carries the remaining count.
    assert_eq!(uv["outcome"]["unwatch"], "dropped");
    assert_eq!(uv["outcome"]["remaining_interest"], 1);
    // s2's interest remains; the daemon's graceful Drop reaps the shared adapter.
}

/// `status` answers "how many topics is this session on?" as a single number, in
/// both output modes. This is the read a Claude Code status line makes on every
/// prompt: it wants one scalar, not a topic list it has to measure.
#[test]
fn status_reports_how_many_subscriptions_a_session_has() {
    let daemon = Daemon::start();
    let session = "sess-sub-count";

    // A session on nothing reports an honest 0 — a present key, not an absent one,
    // so the status line needs no fallback for the un-armed case.
    let before = daemon.run_as(session, &["--json", "status"]);
    assert_ok(&before, "status before subscribing");
    assert_eq!(parse_json(&stdout(&before))["subscription_count"], 0);

    for topic in ["test.count.alpha", "test.count.beta"] {
        assert_ok(&daemon.run_as(session, &["subscribe", topic]), "subscribe");
    }

    let status = daemon.run_as(session, &["--json", "status"]);
    assert_ok(&status, "status");
    let value = parse_json(&stdout(&status));
    assert_eq!(value["subscription_count"], 2);
    assert_eq!(
        value["subscriptions"].as_array().unwrap().len(),
        2,
        "the count must agree with the list it summarises"
    );

    // The human line carries the same number, so the two modes never tell a
    // different story about the same session.
    let human = daemon.run_as(session, &["status"]);
    assert_ok(&human, "human status");
    let text = stdout(&human);
    assert!(
        text.contains("subscriptions (2):"),
        "human status should show the count; got: {text}"
    );
}

// ==== A1: an oversized / unterminated frame is rejected, not buffered ==========

#[test]
fn oversized_frame_is_rejected_and_daemon_survives() {
    // Shrink the frame cap so we can exceed it cheaply (the production cap is MiB).
    let daemon = Daemon::start_with_env(&[("MAILBOX_MAX_FRAME_BYTES", "512")]);

    // 4 KiB with NO newline: far over the 512-byte cap. The daemon must reject it
    // via a bounded read (never buffering the whole thing) and stay alive.
    let junk = vec![b'x'; 4096];
    let reply = daemon.raw_bytes(&junk);
    let value = parse_json(&reply);
    assert_eq!(value["result"], "error", "oversized frame must be rejected");
    assert!(
        value["message"]
            .as_str()
            .unwrap_or_default()
            .contains("exceeds"),
        "error should mention the size limit: {reply}"
    );

    // The daemon did not OOM or die: a well-formed request still works.
    assert_ok(
        &daemon.run_as("s", &["subscribe", "test.after.oversize"]),
        "subscribe after oversized frame",
    );
}

#[test]
fn non_utf8_frame_is_rejected() {
    let daemon = Daemon::start();
    // Invalid UTF-8 bytes followed by a newline: rejected as a value, not a crash.
    let reply = daemon.raw_bytes(&[0xff, 0xfe, 0x00, b'\n']);
    let value = parse_json(&reply);
    assert_eq!(value["result"], "error");
    assert!(
        value["message"]
            .as_str()
            .unwrap_or_default()
            .contains("UTF-8"),
        "expected a UTF-8 error: {reply}"
    );
    assert_ok(&daemon.run_as("s", &["status"]), "status after non-utf8");
}

// ==== A2: half-open client is timed out; connection cap is enforced ============

#[test]
fn half_open_client_is_timed_out_by_the_daemon() {
    // Shrink the read timeout so the daemon closes an idle client fast.
    let daemon = Daemon::start_with_env(&[("MAILBOX_READ_TIMEOUT_MS", "300")]);

    // Connect and send NOTHING. The daemon must close us (EOF) after its read
    // timeout rather than pinning the connection forever.
    let mut stream = StdUnixStream::connect(&daemon.socket_path).expect("connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    let start = Instant::now();
    let mut buf = [0u8; 16];
    let n = stream.read(&mut buf).expect("read should not error");
    let elapsed = start.elapsed();
    assert_eq!(n, 0, "server should close the idle connection (EOF)");
    assert!(
        elapsed < Duration::from_secs(2),
        "idle connection should be dropped promptly, took {elapsed:?}"
    );

    // And the daemon is still healthy for real clients.
    assert_ok(&daemon.run_as("s", &["status"]), "status after timeout");
}

#[test]
fn connection_cap_drops_connections_over_the_limit() {
    // One connection slot, and a long read timeout so the occupying client holds
    // that slot well past the test window.
    let daemon = Daemon::start_with_env(&[
        ("MAILBOX_MAX_CONNECTIONS", "1"),
        ("MAILBOX_READ_TIMEOUT_MS", "3000"),
    ]);

    // Converge on "the single slot is occupied, so a new connection is refused".
    // Each iteration opens a fresh hog that sends a PARTIAL frame (no newline), so
    // the daemon's handler blocks reading and holds the permit; then a probe
    // connection should be refused (empty reply, EOF). If the hog did not win the
    // slot (e.g. the startup readiness probe was still settling), we retry with a
    // new hog. This is a poll-until-condition, not a fixed sleep, so it is robust.
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let mut hog = StdUnixStream::connect(&daemon.socket_path).expect("hog connect");
        hog.write_all(br#"{"version":1,"op":"status""#)
            .expect("partial write");
        hog.flush().ok();
        // Give the daemon a moment to accept the hog and take the permit.
        std::thread::sleep(Duration::from_millis(100));

        let reply = daemon.raw_line(r#"{"version":1,"op":"status","session":"s"}"#);
        if reply.trim().is_empty() {
            // Cap enforced: the extra connection got no reply. Keep the hog alive
            // until the assertion is recorded, then let it drop.
            drop(hog);
            return;
        }
        drop(hog); // this hog did not hold the slot; retry with a fresh one
        assert!(
            Instant::now() < deadline,
            "connection cap never engaged; a second connection was still served: {reply:?}"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

// ==== A3: two `serve` on one DB — exactly one wins the lock ====================

#[test]
fn second_serve_on_same_db_fails_loudly() {
    let dir = TempDir::new().expect("tempdir");
    let db_path = dir.path().join("mailbox.db");

    // First daemon acquires the lock + binds the socket.
    let mut first = spawn_serve(&db_path, &[]);
    wait_for_socket(&socket_for(&db_path), Duration::from_secs(10));

    // Second daemon on the SAME DB must fail loudly (lock held) and exit non-zero
    // WITHOUT becoming a second writer.
    let second = mailbox_command()
        .arg("serve")
        .env("AGENT_MAILBOX_DB", &db_path)
        .env(
            "MAILBOX_CLAUDE_SESSIONS_DIR",
            dir.path().join("claude-sessions"),
        )
        .env("RUST_LOG", "error")
        .stdin(Stdio::null())
        .output()
        .expect("run second serve");

    let _ = first.kill();
    let _ = first.wait();

    assert!(
        !second.status.success(),
        "a second serve on the same DB must exit non-zero"
    );
    assert!(
        stderr(&second).contains("already running"),
        "second serve should say it's already running; got: {}",
        stderr(&second)
    );
}

// ==== A3 companion: a stale socket left by a crashed daemon is reclaimed =======

#[test]
fn stale_socket_is_reclaimed_on_restart() {
    let dir = TempDir::new().expect("tempdir");
    let db_path = dir.path().join("mailbox.db");
    let socket = socket_for(&db_path);

    // Simulate the node a SIGKILLed daemon leaves behind (the dir must exist).
    std::fs::create_dir_all(dir.path()).unwrap();
    std::fs::write(&socket, b"stale").unwrap();
    assert!(socket.exists());

    // A fresh daemon holds the lock, so it may remove the stale node and bind.
    let daemon = Daemon {
        child: spawn_serve(&db_path, &[]),
        db_path: db_path.clone(),
        socket_path: socket.clone(),
        _dir: dir,
    };
    wait_for_socket(&daemon.socket_path, Duration::from_secs(10));
    assert_ok(&daemon.run_as("s", &["status"]), "status after reclaim");
}

// ==== B4: socket + dir are owner-only; a hardening failure is fatal ============

#[test]
fn socket_and_dir_are_owner_only() {
    let daemon = Daemon::start();
    let sock_mode = std::fs::metadata(&daemon.socket_path)
        .unwrap()
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(sock_mode, 0o600, "socket must be rw-------");

    let dir = daemon.db_path.parent().unwrap();
    let dir_mode = std::fs::metadata(dir).unwrap().permissions().mode() & 0o777;
    assert_eq!(dir_mode, 0o700, "daemon dir must be rwx------");
}

#[test]
fn hardening_failure_is_fatal() {
    if is_root() {
        eprintln!("skipping: running as root bypasses directory permission checks");
        return;
    }
    // A read-only (0500) parent means the daemon cannot create its 0700 dir; that
    // hardening failure must be FATAL (exit non-zero), never serve fail-open.
    let dir = TempDir::new().expect("tempdir");
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o500)).unwrap();
    let db_path = dir.path().join("sub").join("mailbox.db");

    let output = mailbox_command()
        .arg("serve")
        .env("AGENT_MAILBOX_DB", &db_path)
        .env(
            "MAILBOX_CLAUDE_SESSIONS_DIR",
            dir.path().join("claude-sessions"),
        )
        .env("RUST_LOG", "error")
        .stdin(Stdio::null())
        .output()
        .expect("run serve with unwritable parent");

    // Restore perms so the tempdir can be cleaned up.
    let _ = std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700));

    assert!(
        !output.status.success(),
        "serve must refuse to start when it cannot create an owner-only dir"
    );
}

// ==== C5: SIGTERM shuts down cleanly (exit 0, socket removed) ==================

#[test]
fn sigterm_shuts_down_cleanly_and_removes_socket() {
    let dir = TempDir::new().expect("tempdir");
    let db_path = dir.path().join("mailbox.db");
    let socket = socket_for(&db_path);
    let mut child = spawn_serve(&db_path, &[]);
    wait_for_socket(&socket, Duration::from_secs(10));

    // A publish that completes (gets its ack) before shutdown — the normal ack
    // path the drain protects during shutdown.
    let publish = mailbox_command()
        .args(["--json", "publish", "test.shutdown.topic"])
        .env("AGENT_MAILBOX_DB", &db_path)
        .env("RUST_LOG", "error")
        .output()
        .expect("publish");
    assert!(publish.status.success());
    assert_eq!(parse_json(&stdout(&publish))["result"], "published");

    // SIGTERM the daemon; it should drain, remove the socket, and exit 0.
    let status = Command::new("kill")
        .args(["-TERM", &child.id().to_string()])
        .status()
        .expect("send SIGTERM");
    assert!(status.success(), "kill -TERM should succeed");

    // Wait for the daemon to exit (bounded), then assert a clean exit.
    let exit = wait_with_timeout(&mut child, Duration::from_secs(10));
    assert_eq!(
        exit.code(),
        Some(0),
        "SIGTERM should be a clean (0) shutdown"
    );
    assert!(
        !socket.exists(),
        "the daemon should remove its socket on clean shutdown"
    );
}

/// Wait for `child` to exit within `timeout`, killing it if it overruns.
fn wait_with_timeout(child: &mut Child, timeout: Duration) -> std::process::ExitStatus {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child.try_wait().expect("try_wait") {
            return status;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            return child.wait().expect("wait");
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

// ==== E2E raw protocol: bad frames are rejected in isolation ===================

#[test]
fn malformed_frames_are_rejected_and_daemon_keeps_serving() {
    let daemon = Daemon::start();

    // Each of these is a distinct bad frame; each must get a `result:error` and
    // must NOT take the daemon down for the well-formed client that follows.
    let cases = [
        ("garbage bytes", "not json at all"),
        (
            "valid JSON, unknown op",
            r#"{"version":1,"op":"bogus","session":"s"}"#,
        ),
        (
            "newer protocol version",
            r#"{"version":9999,"op":"status","session":"s"}"#,
        ),
        ("missing version", r#"{"op":"status","session":"s"}"#),
    ];
    for (label, frame) in cases {
        let reply = daemon.raw_line(frame);
        let value = parse_json(&reply);
        assert_eq!(
            value["result"], "error",
            "{label} should be rejected: {reply}"
        );

        // Isolation: a concurrent well-formed request still works.
        let status = daemon.run_as("iso", &["--json", "status"]);
        assert_ok(&status, "status after a bad frame");
        assert_eq!(parse_json(&stdout(&status))["result"], "status");
    }
}

#[test]
fn client_disconnecting_before_sending_is_handled() {
    let daemon = Daemon::start();
    // Connect then immediately drop without sending: the daemon must not error out.
    {
        let _stream = StdUnixStream::connect(&daemon.socket_path).expect("connect");
        // dropped here
    }
    // Still serving.
    assert_ok(
        &daemon.run_as("s", &["status"]),
        "status after abrupt disconnect",
    );
}

#[test]
fn unknown_session_read_returns_empty_not_error() {
    let daemon = Daemon::start();
    let read = daemon.run_as("never-seen", &["--json", "read"]);
    assert_ok(&read, "read for unknown session");
    let value = parse_json(&stdout(&read));
    assert_eq!(value["result"], "read");
    assert_eq!(
        value["events"].as_array().unwrap().len(),
        0,
        "an unknown session simply has nothing unread"
    );
}

// ==== Concurrent publish burst THROUGH the daemon → contiguous offsets =========

#[test]
fn concurrent_publish_burst_yields_contiguous_offsets() {
    let daemon = Daemon::start();
    let topic = "github.pr.octocat/hello-world#99";
    const N: u64 = 20;

    // Fire N publish PROCESSES concurrently at one topic; the single writer must
    // still assign a contiguous 0..N offset sequence with no gap or duplicate.
    let db = daemon.db_path.clone();
    let handles: Vec<_> = (0..N)
        .map(|i| {
            let db = db.clone();
            std::thread::spawn(move || {
                let out = mailbox_command()
                    .args([
                        "--json",
                        "publish",
                        topic,
                        "--body",
                        &format!(r#"{{"i":{i}}}"#),
                    ])
                    .env("AGENT_MAILBOX_DB", &db)
                    .env("RUST_LOG", "error")
                    .output()
                    .expect("publish");
                assert!(out.status.success(), "publish {i} failed: {}", stderr(&out));
                parse_json(&stdout(&out))["offset"]
                    .as_u64()
                    .expect("offset")
            })
        })
        .collect();

    let mut offsets: Vec<u64> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    offsets.sort_unstable();
    let expected: Vec<u64> = (0..N).collect();
    assert_eq!(
        offsets, expected,
        "offsets must be contiguous 0..N with no gap/dup"
    );
}

// ==== clap edge: session identity ==============================================

#[test]
fn human_publish_output_is_readable() {
    let daemon = Daemon::start();
    let out = daemon.run(&["publish", "test.topic.human", "--body", "{}"]);
    assert_ok(&out, "publish");
    assert!(
        stdout(&out).contains("published event"),
        "human publish output: {}",
        stdout(&out)
    );
}

/// A session-scoped command run outside a Claude Code session fails with an
/// actionable error (exit 1) naming the ONE variable it needs. Deliberately NOT exit
/// 2, which is reserved for the wake hook's wake signal.
///
/// There is no `--session` flag and no `MAILBOX_SESSION_ID` any more: identity comes
/// from `$CLAUDE_CODE_SESSION_ID` and nowhere else, so there is one thing to name here
/// instead of a precedence order to explain.
#[test]
fn a_command_run_outside_a_session_is_an_actionable_error() {
    let output = mailbox_command()
        .args(["read"])
        .env_remove("CLAUDE_CODE_SESSION_ID")
        .env("AGENT_MAILBOX_DB", "/nonexistent/mailbox.db")
        .output()
        .expect("run read without session");
    assert_eq!(
        output.status.code(),
        Some(1),
        "a missing session should fail (and never with the wake code 2); stderr: {}",
        stderr(&output)
    );
    let stderr = stderr(&output);
    assert!(
        stderr.contains("CLAUDE_CODE_SESSION_ID"),
        "stderr: {stderr}"
    );
}

/// The session comes from `$CLAUDE_CODE_SESSION_ID`, which Claude Code exports into
/// every tool call — so an agent addresses itself with nothing installed but the
/// binary, and passes no flag to do it.
#[test]
fn the_session_comes_from_the_claude_code_env_var() {
    // `status`'s identity half needs no bridge, so no daemon is needed to exercise
    // resolution end to end.
    let out = mailbox_command()
        .args(["--json", "status"])
        .env("CLAUDE_CODE_SESSION_ID", "s-claude")
        .env("AGENT_MAILBOX_DB", "/nonexistent/mailbox.db")
        .output()
        .expect("run status");
    let value: serde_json::Value = serde_json::from_str(stdout(&out).trim()).expect("json");
    assert_eq!(value["session"], "s-claude");
    assert_eq!(value["inbox_topic"], "agent.s-claude");
}

/// **An empty session id must name NOBODY, loudly.**
///
/// This is what the deleted `--session` flag got wrong: `--session
/// "$MAILBOX_SESSION_ID"` — which the skill had to warn agents away from — expands to
/// `--session ""` in an agent's shell, and an explicit flag won the precedence, so the
/// command bound a phantom empty session the agent could never be woken on. With one
/// env var and no flag, an empty value simply means "no session", which is an error
/// the agent can see rather than a wrong session it cannot.
/// Exercised through `read`, one of the commands whose whole content is "whose?".
/// `status` no longer answers that question — it reports the bridge's global watch
/// table with no session at all — so it can no longer prove this.
#[test]
fn an_empty_session_env_value_is_refused_rather_than_bound() {
    let out = mailbox_command()
        .args(["--json", "read"])
        .env("CLAUDE_CODE_SESSION_ID", "")
        .env("AGENT_MAILBOX_DB", "/nonexistent/mailbox.db")
        .env_remove("RUST_LOG")
        .output()
        .expect("run status");

    assert!(
        !out.status.success(),
        "an empty session id must not resolve to a phantom empty session"
    );
    let value: serde_json::Value = serde_json::from_str(stdout(&out).trim()).expect("json");
    assert_eq!(value["result"], "error");
    assert!(
        value["message"]
            .as_str()
            .unwrap_or_default()
            .contains("CLAUDE_CODE_SESSION_ID"),
        "the error must name the variable to set; got {value}"
    );
}
