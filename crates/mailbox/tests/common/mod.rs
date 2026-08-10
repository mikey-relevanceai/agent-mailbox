//! Shared cross-component test harness for the card-12 end-to-end suite
//! ([`tests/e2e.rs`]).
//!
//! This module lifts the per-file `Daemon` / fake-`gh` / poll helpers that cards
//! 09–11 each grew their own copy of into ONE place, so the e2e suite reuses them
//! rather than copy-pasting a fourth variant (coding-workflow: no duplicated
//! fixtures). It drives the EXACT production stack the same way the shipped
//! bridge is used:
//!
//! - a real `mailbox serve` daemon in a tempdir (with the stub + github-pr
//!   resolvers pointed at the freshly built adapters, and the github-pr adapter's
//!   `gh` overridden to a recorded FAKE — card 10's `MAILBOX_GH_BIN` seam);
//! - real `mailbox` CLI client processes for every mutation/read;
//! - the fake harness driver = feeding hook JSON to `mailbox harness arm` /
//!   `cleanup` on stdin, exactly as Claude Code would (card 11's seam).
//!
//! No network, no real GitHub, no real Claude Code.
//!
//! The load-bearing new mechanism here is [`LeakGuard`]: an RAII process-leak
//! detector scoped to THIS test's daemon subtree (never a global `pgrep`), so a
//! leaked adapter / serve fails the test loudly. That is the automated enforcement
//! of the design's headline "no zombie pollers" guarantee (design/01 § Adapter
//! lifecycle).

#![allow(dead_code)] // A shared harness: not every helper is used by every test file.

// [`mailbox_command`] — the session-stripping spawner — IS now shared by every test
// binary in the crate (`cli.rs`, `harness.rs`, `stub_e2e.rs`, `github_pr_e2e.rs`, and
// the `Env`-based suites): it is a correctness helper, not a convenience, so the
// hand-copied variants were a real hazard (one of them had already drifted and did
// NOT strip the ambient session).
//
// TODO(card-12 follow-up): `stub_e2e.rs`, `github_pr_e2e.rs`, and `harness.rs` still
// carry their own copies of `Daemon` / `poll_until` / `wait_for_socket` / `FAKE_GH`.
// Those are convenience fixtures (a drifted copy fails its own test loudly rather than
// changing behaviour), so folding them in is deferred to avoid churning green suites.

use std::collections::HashSet;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::{UnixListener, UnixStream as StdUnixStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use serde_json::Value;
use tempfile::TempDir;

/// A stand-in for a Claude Code session listening on its inbox socket, created by
/// [`Env::register_peer`].
///
/// Only the receiving half is exposed: what a test needs to know is whether the
/// daemon delivered a wake here, and what was in it.
pub struct FakePeer {
    rx: mpsc::Receiver<String>,
}

impl FakePeer {
    /// The next frame delivered to this session's inbox, or `None` if none arrives
    /// within `timeout`.
    pub fn next_frame(&self, timeout: Duration) -> Option<Value> {
        let line = self.rx.recv_timeout(timeout).ok()?;
        serde_json::from_str(line.trim_end()).ok()
    }

    /// Assert that NOTHING is delivered here within `timeout`. Used to prove the
    /// peer channel stayed silent — e.g. for a session with nothing unread.
    pub fn expect_silence(&self, timeout: Duration) {
        if let Ok(line) = self.rx.recv_timeout(timeout) {
            panic!("expected no peer delivery, but the inbox received: {line}");
        }
    }
}

/// Register a fake Claude Code session in `sessions_dir` with an inbox socket under
/// `socket_dir`, and serve it.
///
/// The socket lives directly under `socket_dir` rather than in a nested directory,
/// because a Unix socket path is capped near 104 bytes on macOS and a deep temp path
/// plus a session id blows through it.
pub fn register_fake_peer(sessions_dir: &Path, socket_dir: &Path, session: &str) -> FakePeer {
    std::fs::create_dir_all(sessions_dir).expect("create fake sessions dir");
    let socket = socket_dir.join(format!("{session}.sock"));

    let listener = UnixListener::bind(&socket).expect("bind fake inbox socket");
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        // Serve every connection: the daemon opens a fresh one per delivery.
        for stream in listener.incoming().flatten() {
            let mut line = String::new();
            if BufReader::new(stream).read_line(&mut line).is_ok() && tx.send(line).is_err() {
                break;
            }
        }
    });

    std::fs::write(
        sessions_dir.join(format!("{session}.json")),
        serde_json::json!({
            "pid": std::process::id(),
            "sessionId": session,
            "cwd": "/tmp",
            "status": "idle",
            "name": session,
            "peerProtocol": 1,
            "updatedAt": 1_786_000_000_000i64,
            "messagingSocketPath": socket,
        })
        .to_string(),
    )
    .expect("write fake session registry entry");

    FakePeer { rx }
}

// ---- binary locations ---------------------------------------------------------

/// The freshly built `mailbox` bridge binary under test.
pub fn mailbox_bin() -> &'static str {
    env!("CARGO_BIN_EXE_mailbox")
}

/// A `mailbox` command with the AMBIENT session environment stripped.
///
/// `cargo test` inherits the developer's environment, and inside a Claude Code
/// session that includes `CLAUDE_CODE_SESSION_ID` — the one place `mailbox` reads a
/// session's identity from. A test that did not strip it would run its commands as
/// the DEVELOPER's session and behave differently on a laptop than in CI. So every
/// test subprocess starts with NO session unless the test names one itself (via
/// [`Env::run_as`], which sets that variable).
pub fn mailbox_command() -> Command {
    let mut cmd = Command::new(mailbox_bin());
    cmd.env_remove("CLAUDE_CODE_SESSION_ID");
    cmd
}

/// A sibling adapter binary (`name`) beside the `mailbox` bin in the shared target
/// dir, built on demand if missing. `CARGO_BIN_EXE_*` is only set for the crate
/// that DEFINES the bin, so the mailbox-crate tests locate the adapters by path
/// (a full `cargo test --workspace` builds them in the build phase; the on-demand
/// build is the `cargo test -p mailbox`-alone fallback — mirrors cards 09/10).
fn sibling_adapter_bin(name: &str, package: &str) -> String {
    let dir = Path::new(mailbox_bin())
        .parent()
        .expect("mailbox bin has a parent dir")
        .to_path_buf();
    let bin = dir.join(name);
    if !bin.exists() {
        let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string());
        let status = Command::new(cargo)
            .args(["build", "-p", package])
            .status()
            .unwrap_or_else(|e| panic!("build {package}: {e}"));
        assert!(status.success(), "failed to build {package}");
    }
    bin.to_str().expect("adapter bin path is utf8").to_string()
}

pub fn stub_adapter_bin() -> String {
    sibling_adapter_bin("mailbox-stub-adapter", "mailbox-stub-adapter")
}

pub fn github_pr_adapter_bin() -> String {
    sibling_adapter_bin("mailbox-github-pr-adapter", "mailbox-github-pr-adapter")
}

// ---- the fake `gh` (recorded-fixture emitter, card 10's seam) -----------------

/// A fake `gh` that dispatches on the subcommand (`pr` vs `api`, and for `api` on
/// the endpoint path), advances a per-sequence counter, and clamps to the last
/// fixture so an extra poll repeats the tail rather than erroring. Identical in
/// shape to the one cards 10's `github_pr_e2e.rs` / `adapter_e2e.rs` use — kept
/// here so the e2e suite drives the real poll→diff→publish loop with no network.
const FAKE_GH: &str = r#"#!/usr/bin/env bash
set -euo pipefail
dir="${MAILBOX_FAKE_GH_DIR:?MAILBOX_FAKE_GH_DIR must be set}"
case "${1:-}" in
  pr) key="pr" ;;
  api)
    case "${2:-}" in
      */pulls/*/reviews) key="reviews" ;;
      */issues/*/comments) key="issue_comments" ;;
      */pulls/*/comments) key="thread_comments" ;;
      *) echo "fake-gh: unknown api path ${2:-}" >&2; exit 2 ;;
    esac ;;
  *) echo "fake-gh: unsupported invocation: $*" >&2; exit 2 ;;
esac
# NOTE: this counter read-then-write is NOT atomic. It is correct only because the
# adapter calls gh sequentially per key (one poll at a time); a future concurrent
# caller would need file locking here or the count (which the "no further API
# calls" assertions rely on) would silently under-count.
ctr="$dir/counter.$key"
i=0
if [ -f "$ctr" ]; then i="$(cat "$ctr")"; fi
max=0
for f in "$dir/$key".*; do
  [ -e "$f" ] || continue
  n="${f##*.}"
  case "$n" in (*[!0-9]*) continue ;; esac
  if [ "$n" -gt "$max" ]; then max="$n"; fi
done
use="$i"
if [ "$i" -gt "$max" ]; then use="$max"; fi
echo $((i + 1)) > "$ctr"
f="$dir/$key.$use"
if [ ! -f "$f" ]; then echo "fake-gh: missing fixture $f" >&2; exit 3; fi
cat "$f"
"#;

/// A stable, edge-free PR view: mergeable, one passing check. Scenarios that only
/// need the poller ALIVE (the lifecycle scenarios 2–6) baseline on this and never
/// fire an edge, so a manual `publish` is the only thing that reaches subscribers
/// — keeping their delivery assertions independent of `gh`-transition timing.
pub const PR_MERGEABLE_CI_SUCCESS: &str = r#"{"state":"OPEN","mergeable":"MERGEABLE","statusCheckRollup":[{"__typename":"CheckRun","name":"build","status":"COMPLETED","conclusion":"SUCCESS"}]}"#;
/// A conflicting PR whose one check has flipped to failure — used by scenario 1 to
/// fire a conflict edge AND a CI-failure edge from a single transition poll.
pub const PR_CONFLICTING_CI_FAILURE: &str = r#"{"state":"OPEN","mergeable":"CONFLICTING","statusCheckRollup":[{"__typename":"CheckRun","name":"build","status":"COMPLETED","conclusion":"FAILURE"}]}"#;
/// A conflicting PR whose check still passes — a single-edge transition (only
/// `mergeable_conflicting` fires), used by the mid-turn wake test to publish
/// EXACTLY ONE supervised edge deterministically.
pub const PR_CONFLICTING_CI_SUCCESS: &str = r#"{"state":"OPEN","mergeable":"CONFLICTING","statusCheckRollup":[{"__typename":"CheckRun","name":"build","status":"COMPLETED","conclusion":"SUCCESS"}]}"#;
/// A merged PR as `gh` reports it — `state` MERGED, `mergeable` gone UNKNOWN.
/// Fires a single `pr_merged` edge from a transition poll.
pub const PR_MERGED: &str = r#"{"state":"MERGED","mergeable":"UNKNOWN","statusCheckRollup":[{"__typename":"CheckRun","name":"build","status":"COMPLETED","conclusion":"SUCCESS"}]}"#;

// ---- the test environment (tempdir + fake gh, survives a daemon restart) -------

/// A self-contained test environment: an isolated tempdir DB + socket + sentinel
/// root, plus the fake `gh` and its fixture dir. Owns everything a scenario needs
/// EXCEPT the daemon process, so a scenario can stop one daemon and start another
/// on the SAME db (the bridge-restart scenario) without losing its fixtures.
pub struct Env {
    _dir: TempDir,
    gh_dir: TempDir,
    db_path: PathBuf,
    fake_gh: PathBuf,
}

impl Env {
    pub fn new() -> Self {
        let dir = TempDir::new().expect("tempdir");
        let gh_dir = TempDir::new().expect("gh tempdir");
        let db_path = dir.path().join("mailbox.db");
        let fake_gh = write_fake_gh(gh_dir.path());
        Env {
            _dir: dir,
            gh_dir,
            db_path,
            fake_gh,
        }
    }

    pub fn db_path(&self) -> &Path {
        &self.db_path
    }

    pub fn socket_path(&self) -> PathBuf {
        self.db_path.parent().unwrap().join("mailbox.sock")
    }

    /// The fake Claude Code sessions directory for this env, under the tempdir.
    /// ALWAYS passed as `MAILBOX_CLAUDE_SESSIONS_DIR` to the daemon, so a test can
    /// NEVER read the developer's real `~/.claude/sessions` — and, far worse, never
    /// deliver a wake onto a real session's inbox socket (ADR-0020).
    pub fn sessions_dir(&self) -> PathBuf {
        self.db_path.parent().unwrap().join("claude-sessions")
    }

    /// Register `session` as a Claude Code session that Claude Code gave NO inbox
    /// socket — a session nothing can wake.
    pub fn register_socketless(&self, session: &str) {
        let dir = self.sessions_dir();
        std::fs::create_dir_all(&dir).expect("create fake sessions dir");
        std::fs::write(
            dir.join(format!("{session}.json")),
            serde_json::json!({
                "pid": std::process::id(),
                "sessionId": session,
                "cwd": "/tmp",
                "status": "idle",
                "name": session,
                "peerProtocol": 1,
                "updatedAt": 1_786_000_000_000i64,
            })
            .to_string(),
        )
        .expect("write fake session registry entry");
    }

    /// Register `session` as a Claude Code session with a bound inbox socket, and
    /// start listening on it — the wake path's happy path (ADR-0020/0021).
    pub fn register_peer(&self, session: &str) -> FakePeer {
        register_fake_peer(
            &self.sessions_dir(),
            self.db_path.parent().unwrap(),
            session,
        )
    }

    /// Write a `pr` view fixture for poll index `i` (scenario 1 scripts a
    /// transition; the default is [`PR_MERGEABLE_CI_SUCCESS`] at index 0).
    pub fn set_pr_fixture(&self, i: usize, body: &str) {
        std::fs::write(self.gh_dir.path().join(format!("pr.{i}")), body).expect("write pr fixture");
    }

    /// Write a `reviews` REST fixture for poll index `i`.
    pub fn set_reviews_fixture(&self, i: usize, body: &str) {
        std::fs::write(self.gh_dir.path().join(format!("reviews.{i}")), body)
            .expect("write reviews fixture");
    }

    /// How many times the fake `gh`'s `pr` subcommand has been called — i.e. how
    /// many times the poller has hit the (fake) GitHub API. Scenario 4 asserts this
    /// stops growing once the last interest is gone ("no further API calls").
    pub fn gh_pr_call_count(&self) -> u64 {
        std::fs::read_to_string(self.gh_dir.path().join("counter.pr"))
            .ok()
            .and_then(|s| s.trim().parse().ok())
            .unwrap_or(0)
    }

    /// The github-pr topic for `octocat/hello-world#n`.
    pub fn pr_topic(&self, n: u64) -> String {
        format!("github.pr.octocat/hello-world#{n}")
    }

    /// The `owner/repo#n` spec `watch github-pr` takes.
    pub fn pr_spec(&self, n: u64) -> String {
        format!("octocat/hello-world#{n}")
    }

    /// Start a `mailbox serve` daemon over this env's DB, with BOTH adapter
    /// resolvers pointed at the freshly built binaries and the github-pr poller's
    /// `gh` overridden to the fake. Blocks until the socket is accepting.
    pub fn start_daemon(&self) -> Daemon {
        let child = mailbox_command()
            .arg("serve")
            .env("AGENT_MAILBOX_DB", &self.db_path)
            // tempdir root — without this a test would bump files under the
            // developer's real ~/.mailbox.
            // Likewise for the PEER channel (ADR-0020): without this the daemon would
            // read the developer's real ~/.claude/sessions and could deliver a test's
            // wake onto a real Claude Code session's inbox socket.
            .env("MAILBOX_CLAUDE_SESSIONS_DIR", self.sessions_dir())
            .env("MAILBOX_STUB_ADAPTER_BIN", stub_adapter_bin())
            .env("MAILBOX_GH_ADAPTER_BIN", github_pr_adapter_bin())
            .env("MAILBOX_GH_BIN", &self.fake_gh)
            .env("MAILBOX_FAKE_GH_DIR", self.gh_dir.path())
            .env("RUST_LOG", "error")
            .stdin(Stdio::null())
            .spawn()
            .expect("spawn mailbox serve");
        let daemon = Daemon {
            child,
            reaped: false,
        };
        wait_for_socket(&self.socket_path(), Duration::from_secs(10));
        daemon
    }

    /// A [`LeakGuard`] for this env. Track each daemon's pid on it (via
    /// [`LeakGuard::track_daemon`]) so its adapter subtree is watched.
    pub fn leak_guard(&self) -> LeakGuard {
        LeakGuard::default()
    }

    // ---- CLI client helpers (need only the DB path — the daemon supplies the
    //      socket) --------------------------------------------------------------

    /// Run a one-shot `mailbox` client command against this env's daemon.
    pub fn run(&self, args: &[&str]) -> Output {
        mailbox_command()
            .args(args)
            .env("AGENT_MAILBOX_DB", &self.db_path)
            .env("RUST_LOG", "error")
            .output()
            .expect("run mailbox client")
    }

    /// Run a one-shot `mailbox` client command **as `session`**, via the env var
    /// Claude Code itself exports into every tool call.
    ///
    /// This is the ONLY way to name a session: there is no `--session` flag, and this
    /// is the agent's real path (Claude Code sets the variable, the agent passes
    /// nothing).
    pub fn run_as(&self, session: &str, args: &[&str]) -> Output {
        mailbox_command()
            .args(args)
            .env("AGENT_MAILBOX_DB", &self.db_path)
            .env("CLAUDE_CODE_SESSION_ID", session)
            // Client commands read the registry too (subscribe/watch refuse when the
            // caller cannot be woken), so they must see the TEST one.
            .env("MAILBOX_CLAUDE_SESSIONS_DIR", self.sessions_dir())
            .env("RUST_LOG", "error")
            .output()
            .expect("run mailbox client")
    }

    /// Run a client command as `session` and assert it exited 0.
    pub fn run_as_ok(&self, session: &str, args: &[&str], what: &str) -> Output {
        let out = self.run_as(session, args);
        assert!(
            out.status.success(),
            "{what} should exit 0; got {:?}\nstderr: {}",
            out.status.code(),
            String::from_utf8_lossy(&out.stderr)
        );
        out
    }

    /// This session's unread count on ONE topic, from `status` (does not consume).
    pub fn unread_on(&self, session: &str, topic: &str) -> u64 {
        self.status(session)["unread"]
            .as_array()
            .and_then(|topics| {
                topics
                    .iter()
                    .find(|u| u["topic"] == topic)
                    .and_then(|u| u["unread"].as_u64())
            })
            .unwrap_or(0)
    }

    /// How many events exist on `topic` (from `topics`), so a test can prove a
    /// REFUSED publish wrote nothing at all.
    pub fn event_count(&self, topic: &str) -> u64 {
        let out = self.run_ok(&["--json", "topics"], "topics");
        parse_json(&String::from_utf8_lossy(&out.stdout))["topics"]
            .as_array()
            .and_then(|topics| {
                topics
                    .iter()
                    .find(|t| t["topic"] == topic)
                    .and_then(|t| t["events"].as_u64())
            })
            .unwrap_or(0)
    }

    /// Run a client command and assert it exited 0.
    pub fn run_ok(&self, args: &[&str], what: &str) -> Output {
        let out = self.run(args);
        assert!(
            out.status.success(),
            "{what} should exit 0; got {:?}\nstderr: {}",
            out.status.code(),
            String::from_utf8_lossy(&out.stderr)
        );
        out
    }

    /// Publish a synthetic edge to a raw topic (represents "a new edge arrives"
    /// deterministically, without racing a `gh` transition).
    pub fn publish(&self, topic: &str) {
        self.run_ok(&["publish", topic], "publish");
    }

    /// Run `mailbox harness cleanup` for a session (feeding the SessionEnd payload),
    /// the fake harness driver's teardown half. Returns its output.
    pub fn cleanup(&self, session: &str) -> Output {
        let mut child = mailbox_command()
            .args(["harness", "cleanup"])
            .env("AGENT_MAILBOX_DB", &self.db_path)
            // Always a tempdir sentinel root: cleanup removes the sentinel dir, so this
            // is what keeps it off the real ~/.mailbox.
            .env("RUST_LOG", "error")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn mailbox harness cleanup");
        let payload = format!(r#"{{"session_id":"{session}","hook_event_name":"SessionEnd"}}"#);
        let mut stdin = child.stdin.take().expect("cleanup stdin");
        stdin.write_all(payload.as_bytes()).expect("write payload");
        drop(stdin);
        child.wait_with_output().expect("cleanup output")
    }

    /// Run `mailbox harness session-start` (the SessionStart hook), feeding it the hook
    /// JSON on stdin exactly as Claude Code would. Returns its Output — stdout carries
    /// the `watchPaths` registration JSON, and as a side effect the session's wake
    /// sentinel is armed (created).
    pub fn session_start(&self, session: &str) -> Output {
        let mut child = mailbox_command()
            .args(["harness", "session-start"])
            .env("AGENT_MAILBOX_DB", &self.db_path)
            .env("RUST_LOG", "error")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn mailbox harness session-start");
        let payload = format!(r#"{{"session_id":"{session}","hook_event_name":"SessionStart"}}"#);
        let mut stdin = child.stdin.take().expect("session-start stdin");
        stdin.write_all(payload.as_bytes()).expect("write payload");
        drop(stdin);
        child.wait_with_output().expect("session-start output")
    }

    /// Run `session-start` for a session and assert it succeeded.
    ///
    /// It used to also assert that a wake sentinel had been created, because Claude
    /// Code could not watch a file that did not exist. Nothing is armed any more: a
    /// session is woken through the inbox socket Claude Code binds for it, so all this
    /// hook does is register the always-on agent inbox (ADR-0021).
    pub fn start_session(&self, session: &str) {
        let out = self.session_start(session);
        assert!(
            out.status.success(),
            "session-start must exit 0; stderr: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }

    // ---- status / read projections ------------------------------------------

    fn status(&self, session: &str) -> Value {
        let out = self.run_as_ok(session, &["--json", "status"], "status");
        parse_json(&String::from_utf8_lossy(&out.stdout))
    }

    /// The single watch's `(state, interest)` from `status`, or `None`.
    pub fn watch_state_interest(&self, session: &str) -> Option<(String, u64)> {
        let value = self.status(session);
        let watch = value["watches"].as_array()?.first()?.clone();
        Some((
            watch["state"].as_str()?.to_string(),
            watch["interest"].as_u64()?,
        ))
    }

    /// The single watch's running adapter pid from `status`, or `None`.
    pub fn watch_pid(&self, session: &str) -> Option<u32> {
        let value = self.status(session);
        u32::try_from(value["watches"].as_array()?.first()?["pid"].as_u64()?).ok()
    }

    pub fn subscriptions(&self, session: &str) -> Vec<String> {
        let value = self.status(session);
        value["subscriptions"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|t| t.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Read a session's unread events (advancing its cursor) as a JSON array.
    pub fn read_events(&self, session: &str) -> Vec<Value> {
        let out = self.run_as_ok(session, &["--json", "read"], "read");
        let value = parse_json(&String::from_utf8_lossy(&out.stdout));
        value["events"].as_array().cloned().unwrap_or_default()
    }

    /// The session's total unread count from `status`. Unlike [`Env::read_events`]
    /// this does NOT advance the cursor, so a test can confirm an edge landed while
    /// still leaving it unread for a later wake to fire on.
    pub fn unread_total(&self, session: &str) -> u64 {
        let value = self.status(session);
        value["unread"]
            .as_array()
            .map(|a| a.iter().filter_map(|u| u["unread"].as_u64()).sum())
            .unwrap_or(0)
    }
}

impl Default for Env {
    fn default() -> Self {
        Self::new()
    }
}

/// Write the fake `gh` script + its default (empty) list fixtures + a stable
/// mergeable `pr.0` into `dir`, and return the script path.
fn write_fake_gh(dir: &Path) -> PathBuf {
    let script = dir.join("fake-gh.sh");
    std::fs::write(&script, FAKE_GH).expect("write fake gh");
    let mut perms = std::fs::metadata(&script).unwrap().permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(&script, perms).expect("chmod fake gh");
    for seq in ["reviews", "issue_comments", "thread_comments"] {
        std::fs::write(dir.join(format!("{seq}.0")), "[]").expect("write default list");
    }
    std::fs::write(dir.join("pr.0"), PR_MERGEABLE_CI_SUCCESS).expect("write default pr fixture");
    script
}

// ---- daemon process handle ----------------------------------------------------

/// A running `mailbox serve` daemon. Gracefully reaped on drop (SIGTERM → grace →
/// SIGKILL) so its supervisor tears down and reaps every adapter it spawned —
/// upholding "no zombie pollers" even when a test panics.
pub struct Daemon {
    child: Child,
    reaped: bool,
}

impl Daemon {
    pub fn pid(&self) -> u32 {
        self.child.id()
    }

    /// Gracefully stop the daemon and wait for it to exit (so its adapters are torn
    /// down and reaped before we assert or restart). Consumes the handle.
    pub fn stop(mut self) {
        self.terminate();
    }

    fn terminate(&mut self) {
        if self.reaped {
            return;
        }
        let pid = nix::unistd::Pid::from_raw(self.child.id() as i32);
        // SIGTERM lets `serve` run its supervisor shutdown (tears down adapters);
        // fall back to SIGKILL if it does not exit promptly.
        let _ = nix::sys::signal::kill(pid, nix::sys::signal::Signal::SIGTERM);
        for _ in 0..250 {
            match self.child.try_wait() {
                Ok(Some(_)) => {
                    self.reaped = true;
                    return;
                }
                Ok(None) => std::thread::sleep(Duration::from_millis(20)),
                Err(_) => break,
            }
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
        self.reaped = true;
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        self.terminate();
    }
}

// ---- a stand-in for a running Claude Code process ------------------------------

/// A process that looks like Claude Code to the ONE liveness signal the bridge
/// trusts: an entry in Claude Code's session registry whose pid is still running
/// ([`mailbox::doctor::live_from`]).
///
/// It used to be a symlink to `/bin/sh` named `claude`, carrying `--session-id` in
/// its argv, because liveness was reconstructed by parsing `ps` output. Claude Code
/// publishes the id → pid mapping itself now, so the fake writes a registry entry
/// instead of impersonating a command line (ADR-0021).
///
/// The sleep loop is still a real process: the registry is deliberately NOT evidence
/// of liveness (an entry outlives its process), so a test that wants a session to
/// read as live needs something actually running to point the entry at.
///
/// Killed and reaped on drop, so a failing assertion cannot leak it.
pub struct FakeClaude {
    child: Child,
    registry_file: PathBuf,
    sessions_dir: PathBuf,
    session: String,
}

impl FakeClaude {
    /// Start a fake Claude Code for `session`, registered in `sessions_dir`, and block
    /// until the registry + process table agree it is live.
    ///
    /// `socket` optionally names an inbox socket to advertise — the difference between
    /// a session that can be woken and one that cannot.
    pub fn running(sessions_dir: &Path, session: &str, socket: Option<&Path>) -> Self {
        std::fs::create_dir_all(sessions_dir).expect("create fake sessions dir");
        let child = Command::new("/bin/sh")
            .args(["-c", "while :; do sleep 1; done"])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn the fake claude");

        let socket_field = match socket {
            Some(path) => format!(r#","messagingSocketPath":"{}""#, path.display()),
            None => String::new(),
        };
        let registry_file = sessions_dir.join(format!("{}.json", child.id()));
        std::fs::write(
            &registry_file,
            format!(
                r#"{{"pid":{},"sessionId":"{session}","cwd":"/tmp","status":"idle","name":"fake","updatedAt":1{socket_field}}}"#,
                child.id()
            ),
        )
        .expect("write fake registry entry");

        let fake = FakeClaude {
            child,
            registry_file,
            sessions_dir: sessions_dir.to_path_buf(),
            session: session.to_string(),
        };
        poll_until(
            "the fake claude reads as live",
            Duration::from_secs(10),
            || fake.is_live().then_some(()),
        );
        fake
    }

    /// Whether the bridge would currently consider this session live.
    fn is_live(&self) -> bool {
        let registry = mailbox::claude_registry::ClaudeRegistry::read_dir(&self.sessions_dir);
        mailbox::doctor::live_from(&registry)
            .is_some_and(|live| live.contains(&mailbox::storage::SessionId::new(&self.session)))
    }

    /// Kill and reap it, then block until it reads as gone — so a test asserting
    /// "this agent is gone" is not racing the kernel.
    pub fn stop(mut self, _session: &str) {
        self.terminate();
        let sessions_dir = self.sessions_dir.clone();
        let session = self.session.clone();
        poll_until(
            "the fake claude reads as gone",
            Duration::from_secs(10),
            || {
                let registry = mailbox::claude_registry::ClaudeRegistry::read_dir(&sessions_dir);
                mailbox::doctor::live_from(&registry)
                    .is_none_or(|live| !live.contains(&mailbox::storage::SessionId::new(&session)))
                    .then_some(())
            },
        );
    }

    fn terminate(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        // A real session's entry outlives it; these tests want a clean tempdir.
        let _ = std::fs::remove_file(&self.registry_file);
    }
}

impl Drop for FakeClaude {
    fn drop(&mut self) {
        self.terminate();
    }
}

/// A spawned child that is killed and reaped when the test drops it, so a failing
/// assertion can never leak a process into the next test.
pub struct ChildGuard(pub Child);

impl std::ops::Deref for ChildGuard {
    type Target = Child;
    fn deref(&self) -> &Child {
        &self.0
    }
}

impl std::ops::DerefMut for ChildGuard {
    fn deref_mut(&mut self) -> &mut Child {
        &mut self.0
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

// ---- the process-leak guard (ac-12-3) -----------------------------------------

/// A process observed still alive when the test expected it gone.
#[derive(Debug, Clone)]
pub struct LeakedProcess {
    pub pid: u32,
    /// Why this pid is a leak (today: a surviving descendant of a tracked daemon).
    pub source: &'static str,
    pub cmd: String,
}

/// The load-bearing enforcement of "no zombie pollers": a leak detector scoped to
/// THIS test's processes — the descendant subtree of each tracked `mailbox serve`
/// pid (adapters + their `gh` children are all children of `serve`).
///
/// It keys on the daemon's PID subtree, NOT a global `pgrep`, so it can never
/// mistake a concurrently-running test's adapter for a leak of this one.
/// [`LeakGuard::assert_clean`] is the load-bearing check (call it after teardown,
/// while the daemon is still alive so its subtree is walkable); [`Drop`] is a
/// best-effort backstop that also kills any stray so it cannot escape the test
/// binary.
///
/// It used to also scan the per-session waiter pidfiles. There are no per-session
/// processes any more — the daemon writes the sentinels itself — so the adapter
/// subtree is the whole population that can leak.
#[derive(Default)]
pub struct LeakGuard {
    daemon_pids: Vec<u32>,
}

impl LeakGuard {
    /// Track a daemon's pid so its whole adapter subtree is watched for leaks.
    pub fn track_daemon(&mut self, pid: u32) {
        self.daemon_pids.push(pid);
    }

    /// The set of processes that should have been torn down but are still alive.
    pub fn find_leaks(&self) -> Vec<LeakedProcess> {
        let table = process_table();
        let mut leaks = Vec::new();
        let mut seen: HashSet<u32> = HashSet::new();

        // Any surviving descendant of a tracked daemon is a leaked adapter / poller
        // child (the daemon itself is the excluded root).
        for &root in &self.daemon_pids {
            for (pid, cmd) in descendants(root, &table) {
                if seen.insert(pid) {
                    leaks.push(LeakedProcess {
                        pid,
                        source: "daemon-descendant",
                        cmd,
                    });
                }
            }
        }

        leaks
    }

    /// Poll (bounded) until no leaks remain — teardown (SIGTERM → reap) is async,
    /// so a one-shot check could race a just-signalled adapter. Panics loudly with
    /// the surviving processes if any is still alive at the deadline.
    pub fn assert_clean(&self) {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let leaks = self.find_leaks();
            if leaks.is_empty() {
                return;
            }
            if Instant::now() >= deadline {
                panic!(
                    "process leak — a poller/adapter that should have been torn down is \
                     still alive (no-zombie-pollers violated): {leaks:#?}"
                );
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }
}

impl Drop for LeakGuard {
    fn drop(&mut self) {
        // Best-effort backstop. Kill any stray so a real leak cannot escape into the
        // test runner, then (only if the test is not already unwinding — a double
        // panic aborts) fail loudly if anything survived teardown.
        let leaks = self.find_leaks();
        for leak in &leaks {
            let _ = nix::sys::signal::kill(
                nix::unistd::Pid::from_raw(leak.pid as i32),
                nix::sys::signal::Signal::SIGKILL,
            );
        }
        if !leaks.is_empty() && !std::thread::panicking() {
            panic!(
                "process leak detected at guard drop (test forgot assert_clean, or teardown \
                 leaked): {leaks:#?}"
            );
        }
    }
}

/// `(pid, ppid, command)` for every process, via `ps`. Portable across the BSD
/// `ps` (macOS) and GNU `ps` (Linux/CI) — both honour `-A` and `-o <col>=`.
fn process_table() -> Vec<(u32, u32, String)> {
    let out = match Command::new("ps")
        .args(["-Ao", "pid=,ppid=,command="])
        .output()
    {
        Ok(out) if out.status.success() => out,
        _ => return Vec::new(),
    };
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .filter_map(|line| {
            let mut parts = line.split_whitespace();
            let pid = parts.next()?.parse().ok()?;
            let ppid = parts.next()?.parse().ok()?;
            let cmd = parts.collect::<Vec<_>>().join(" ");
            Some((pid, ppid, cmd))
        })
        .collect()
}

/// The pids of every live transitive descendant of `root`, snapshotted now. Used
/// to record a daemon's whole adapter subtree BEFORE it exits, so a test can then
/// assert the entire subtree died with the bridge — a dead daemon's children get
/// reparented to init, so they are no longer walkable via `ppid` afterwards.
pub fn descendant_pids(root: u32) -> Vec<u32> {
    descendants(root, &process_table())
        .into_iter()
        .map(|(pid, _)| pid)
        .collect()
}

/// All transitive descendants of `root` (excluding `root` itself) from a process
/// table, as `(pid, command)`.
fn descendants(root: u32, table: &[(u32, u32, String)]) -> Vec<(u32, String)> {
    let mut result = Vec::new();
    let mut seen: HashSet<u32> = HashSet::from([root]);
    let mut frontier = vec![root];
    while let Some(parent) = frontier.pop() {
        for (pid, ppid, cmd) in table {
            if *ppid == parent && seen.insert(*pid) {
                result.push((*pid, cmd.clone()));
                frontier.push(*pid);
            }
        }
    }
    result
}

// ---- small polling / process utilities ----------------------------------------

/// `kill(pid, 0)`: true while `pid` still names a live (non-reaped) process.
pub fn pid_alive(pid: u32) -> bool {
    matches!(
        nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid as i32), None),
        Ok(())
    )
}

/// Poll `f` until it returns `Some`, or panic after `timeout`. A bounded poll loop
/// (never a fixed sleep waiting for a state) keeps the suite non-flaky under load.
pub fn poll_until<T>(what: &str, timeout: Duration, mut f: impl FnMut() -> Option<T>) -> T {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(value) = f() {
            return value;
        }
        assert!(Instant::now() < deadline, "condition never held: {what}");
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// Wait for `child` to exit within `timeout`, killing (and reaping) it if it
/// overruns. Returns the exit status, or `None` if it had to be killed.
pub fn wait_within(child: &mut Child, timeout: Duration) -> Option<std::process::ExitStatus> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child.try_wait().expect("try_wait") {
            return Some(status);
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            return None;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// Drain a finished child's piped stderr (small, so no deadlock).
pub fn drain_stderr(child: &mut Child) -> String {
    use std::io::Read;
    let mut buf = String::new();
    if let Some(mut err) = child.stderr.take() {
        let _ = err.read_to_string(&mut buf);
    }
    buf
}

fn wait_for_socket(socket: &Path, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        if StdUnixStream::connect(socket).is_ok() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "daemon socket {} not ready within {timeout:?}",
            socket.display()
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn parse_json(text: &str) -> Value {
    serde_json::from_str(text.trim()).unwrap_or_else(|e| panic!("not JSON: {e}\n{text}"))
}

/// Count events on `topic` carrying a given `edge` label (github-pr edge body).
pub fn count_edges(events: &[Value], edge: &str) -> usize {
    events.iter().filter(|e| e["body"]["edge"] == edge).count()
}
