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

/// Path to the freshly built `mailbox` binary under test.
fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_mailbox")
}

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
    let mut cmd = Command::new(bin());
    cmd.arg("serve")
        .env("AGENT_MAILBOX_DB", db_path)
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
        Command::new(bin())
            .args(args)
            .env("AGENT_MAILBOX_DB", &self.db_path)
            .env("RUST_LOG", "error")
            .output()
            .expect("run mailbox client")
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
    assert_ok(
        &daemon.run(&["subscribe", "--session", session, topic]),
        "subscribe",
    );

    let publish = daemon.run(&[
        "publish",
        topic,
        "--body",
        r#"{"marker":"ROUND_TRIP_OK","n":7}"#,
    ]);
    assert_ok(&publish, "publish");

    let read = daemon.run(&["--json", "read", "--session", session]);
    assert_ok(&read, "read");
    let value = parse_json(&stdout(&read));
    assert_eq!(value["result"], "read");
    let events = value["events"].as_array().expect("events array");
    assert_eq!(events.len(), 1, "exactly one unread event");
    assert_eq!(events[0]["topic"], topic);
    assert_eq!(events[0]["body"]["marker"], "ROUND_TRIP_OK");
    assert_eq!(events[0]["body"]["n"], 7);

    // A second read has advanced past it: nothing unread now (advance-on-read).
    let read2 = daemon.run(&["--json", "read", "--session", session]);
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

    let output = Command::new(bin())
        .args(["status", "--session", "s"])
        .env("AGENT_MAILBOX_DB", &db_path)
        .env("RUST_LOG", "error")
        .output()
        .expect("run status with no daemon");

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

#[test]
fn bridge_down_json_mode_emits_error_object() {
    let dir = TempDir::new().expect("tempdir");
    let db_path = dir.path().join("mailbox.db");

    let output = Command::new(bin())
        .args(["--json", "status", "--session", "s"])
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

// ==== AC3: status shows a watch with its interest count ========================

#[test]
fn ac3_status_shows_watch_with_interest_count() {
    // github-pr now resolves to a real adapter (card 10), so point it at the
    // harmless stub for a deterministic lifecycle without `gh`.
    let daemon = Daemon::start_with_env(&[("MAILBOX_GH_ADAPTER_BIN", &stub_bin())]);
    let session = "sess-watch";

    assert_ok(
        &daemon.run(&[
            "watch",
            "github-pr",
            "octocat/hello-world#42",
            "--interval",
            "30",
            "--session",
            session,
        ]),
        "watch",
    );

    let status = daemon.run(&["--json", "status", "--session", session]);
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

/// Two sessions share one refcounted watch; unwatch reports the sum-typed outcome.
#[test]
fn two_sessions_share_one_watch_refcounted() {
    // Point github-pr at the stub so the shared adapter runs deterministically
    // without `gh` (this test is about the refcount, not adapter behaviour).
    let daemon = Daemon::start_with_env(&[("MAILBOX_GH_ADAPTER_BIN", &stub_bin())]);
    let pr = "octocat/hello-world#7";

    assert_ok(
        &daemon.run(&["watch", "github-pr", pr, "--session", "s1"]),
        "watch s1",
    );
    assert_ok(
        &daemon.run(&["watch", "github-pr", pr, "--session", "s2"]),
        "watch s2",
    );

    let status = daemon.run(&["--json", "status", "--session", "s1"]);
    assert_ok(&status, "status");
    let value = parse_json(&stdout(&status));
    let watches = value["watches"].as_array().unwrap();
    assert_eq!(watches.len(), 1, "still ONE shared watch");
    assert_eq!(watches[0]["interest"], 2, "two interested sessions");

    let unwatch = daemon.run(&["--json", "unwatch", "github-pr", pr, "--session", "s1"]);
    assert_ok(&unwatch, "unwatch s1");
    let uv = parse_json(&stdout(&unwatch));
    assert_eq!(uv["result"], "unwatched");
    // Sum-typed outcome: "dropped" carries the remaining count.
    assert_eq!(uv["outcome"]["unwatch"], "dropped");
    assert_eq!(uv["outcome"]["remaining_interest"], 1);
    // s2's interest remains; the daemon's graceful Drop reaps the shared adapter.
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
        &daemon.run(&["subscribe", "test.after.oversize", "--session", "s"]),
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
    assert_ok(
        &daemon.run(&["status", "--session", "s"]),
        "status after non-utf8",
    );
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
    assert_ok(
        &daemon.run(&["status", "--session", "s"]),
        "status after timeout",
    );
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
    let second = Command::new(bin())
        .arg("serve")
        .env("AGENT_MAILBOX_DB", &db_path)
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
    assert_ok(
        &daemon.run(&["status", "--session", "s"]),
        "status after reclaim",
    );
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

    let output = Command::new(bin())
        .arg("serve")
        .env("AGENT_MAILBOX_DB", &db_path)
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
    let publish = Command::new(bin())
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
        let status = daemon.run(&["--json", "status", "--session", "iso"]);
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
        &daemon.run(&["status", "--session", "s"]),
        "status after abrupt disconnect",
    );
}

#[test]
fn unknown_session_read_returns_empty_not_error() {
    let daemon = Daemon::start();
    let read = daemon.run(&["--json", "read", "--session", "never-seen"]);
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
                let out = Command::new(bin())
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

/// With no `--session` and neither session env var set, a session-scoped command
/// fails with an actionable error (exit 1) that names every place the id could
/// have come from. Card 16 moved this off clap's `env =` (which supports only one
/// variable) into an explicit three-way resolution, so it is a runtime error
/// rather than a clap usage error — and deliberately NOT exit 2, which is
/// reserved for the waiter's wake signal.
#[test]
fn missing_session_everywhere_is_an_actionable_error() {
    let output = Command::new(bin())
        .args(["read"])
        .env_remove("MAILBOX_SESSION_ID")
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
    assert!(stderr.contains("--session"), "stderr: {stderr}");
    assert!(stderr.contains("MAILBOX_SESSION_ID"), "stderr: {stderr}");
    assert!(
        stderr.contains("CLAUDE_CODE_SESSION_ID"),
        "stderr: {stderr}"
    );
}

/// `CLAUDE_CODE_SESSION_ID` is the LAST fallback: Claude Code exports it into
/// every tool call, so an agent can address itself with nothing installed but the
/// binary. `MAILBOX_SESSION_ID` (which the harness hooks set) still wins over it.
#[test]
fn claude_code_session_env_is_the_last_fallback() {
    // `whoami` is a local command (identity does not depend on the bridge), so no
    // daemon is needed to exercise the resolution order end to end.
    let out = Command::new(bin())
        .args(["--json", "whoami"])
        .env_remove("MAILBOX_SESSION_ID")
        .env("CLAUDE_CODE_SESSION_ID", "s-claude")
        .output()
        .expect("run whoami");
    let value: serde_json::Value = serde_json::from_str(stdout(&out).trim()).expect("json");
    assert_eq!(value["session"], "s-claude");
    assert_eq!(value["inbox_topic"], "agent.s-claude");

    // MAILBOX_SESSION_ID wins over it...
    let out = Command::new(bin())
        .args(["--json", "whoami"])
        .env("MAILBOX_SESSION_ID", "s-mailbox")
        .env("CLAUDE_CODE_SESSION_ID", "s-claude")
        .output()
        .expect("run whoami");
    let value: serde_json::Value = serde_json::from_str(stdout(&out).trim()).expect("json");
    assert_eq!(value["session"], "s-mailbox");

    // ...and the explicit flag wins over both.
    let out = Command::new(bin())
        .args(["--json", "whoami", "--session", "s-flag"])
        .env("MAILBOX_SESSION_ID", "s-mailbox")
        .env("CLAUDE_CODE_SESSION_ID", "s-claude")
        .output()
        .expect("run whoami");
    let value: serde_json::Value = serde_json::from_str(stdout(&out).trim()).expect("json");
    assert_eq!(value["session"], "s-flag");
}

#[test]
fn session_env_fallback_is_used_when_flag_absent() {
    let daemon = Daemon::start();
    let output = Command::new(bin())
        .args(["subscribe", "test.topic.env"])
        .env("AGENT_MAILBOX_DB", &daemon.db_path)
        .env("MAILBOX_SESSION_ID", "from-env")
        .env("RUST_LOG", "error")
        .output()
        .expect("run subscribe with env session");
    assert_ok(&output, "subscribe via env session");
    assert!(stdout(&output).contains("subscribed to test.topic.env"));
}
