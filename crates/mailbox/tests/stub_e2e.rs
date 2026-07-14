//! Stub-adapter end-to-end tests (card 09), driving the REAL `mailbox` binary
//! AND the REAL `mailbox-stub-adapter` binary through the full production path.
//!
//! Every test spawns an actual `mailbox serve` daemon in a tempdir (with
//! `MAILBOX_STUB_ADAPTER_BIN` pointing at the freshly built stub) and drives it
//! as separate client processes — so it exercises the exact chain that ships:
//! CLI `watch stub` → serve → supervisor → stub resolver → spawn the stub → it
//! publishes on its interval → `read` surfaces the events.
//!
//! Covered:
//! - **ac-09-1**: `watch stub` → synthetic events appear in `read`; `unwatch`
//!   tears the adapter down (watch goes `stopped`).
//! - **ac-09-2 (early test bar #1–#5)**: the stub publishes on a schedule (#1);
//!   sessions subscribe via `watch` (#2); `mailbox wait` wakes with exit 2 (#3);
//!   the delivery cursor advances so a mid-turn publish surfaces on the next
//!   `read` (#4); two subscribers each see events with independent cursors (#5).
//!
//! Flakiness discipline (mirrors `tests/cli.rs`): never sleep a fixed amount
//! waiting for a state — poll the socket for readiness and poll `read`/`status`
//! with bounded deadlines. The daemon child is always reaped on drop; the stub
//! adapters it spawns are torn down by the supervisor on daemon shutdown.

use std::io::{Read, Write};
use std::os::unix::net::UnixStream as StdUnixStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

use tempfile::TempDir;

// ---- locating the two binaries under test -------------------------------------

/// The freshly built `mailbox` binary.
mod common;

// The ONE session-stripping spawner + bin path (tests/common): a test must never
// inherit the developer's CLAUDE_CODE_SESSION_ID.
use common::mailbox_command;

/// The reference stub adapter binary (`mailbox-stub-adapter`), built if missing.
///
/// It lives beside the `test_adapter` bin in the shared target dir. A full
/// `cargo test --workspace` builds it in the build phase; the on-demand build is
/// a fallback for `cargo test -p mailbox` alone.
fn stub_bin() -> String {
    let dir = Path::new(env!("CARGO_BIN_EXE_test_adapter"))
        .parent()
        .expect("test_adapter bin has a parent dir")
        .to_path_buf();
    let bin = dir.join("mailbox-stub-adapter");
    if !bin.exists() {
        let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string());
        let status = Command::new(cargo)
            .args(["build", "-p", "mailbox-stub-adapter"])
            .status()
            .expect("build mailbox-stub-adapter");
        assert!(status.success(), "failed to build mailbox-stub-adapter");
    }
    bin.to_str().expect("stub bin path is utf8").to_string()
}

// ---- daemon process helpers (self-contained, like tests/cli.rs) ---------------

fn socket_for(db_path: &Path) -> PathBuf {
    db_path.parent().unwrap().join("mailbox.sock")
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

/// A running daemon whose stub resolver points at the built stub binary. Reaped
/// (killed + waited) on drop, which also tears down any stub children.
struct Daemon {
    child: Child,
    db_path: PathBuf,
    _dir: TempDir,
}

impl Daemon {
    fn start() -> Self {
        let dir = TempDir::new().expect("tempdir");
        let db_path = dir.path().join("mailbox.db");
        let socket_path = socket_for(&db_path);
        let child = mailbox_command()
            .arg("serve")
            .env("AGENT_MAILBOX_DB", &db_path)
            // The env override that makes `serve`'s stub resolver run the freshly
            // built binary instead of relying on it being installed on PATH.
            .env("MAILBOX_STUB_ADAPTER_BIN", stub_bin())
            .env("RUST_LOG", "error")
            .stdin(Stdio::null())
            .spawn()
            .expect("spawn mailbox serve");
        let daemon = Daemon {
            child,
            db_path,
            _dir: dir,
        };
        wait_for_socket(&socket_path, Duration::from_secs(10));
        daemon
    }

    /// Run a `mailbox` client command against this daemon and return its output.
    fn run(&self, args: &[&str]) -> Output {
        mailbox_command()
            .args(args)
            .env("AGENT_MAILBOX_DB", &self.db_path)
            .env("RUST_LOG", "error")
            .output()
            .expect("run mailbox client")
    }

    /// Spawn a `mailbox wait --session <session>` child (does NOT block the test).
    fn spawn_wait(&self, session: &str) -> Child {
        mailbox_command()
            .args(["wait", "--session", session])
            .env("AGENT_MAILBOX_DB", &self.db_path)
            .env("RUST_LOG", "error")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn mailbox wait")
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
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

/// Read `session`'s unread events (advancing its cursor) and return them as a
/// JSON array. Panics on a non-`read` result.
fn read_events(daemon: &Daemon, session: &str) -> Vec<serde_json::Value> {
    let out = daemon.run(&["--json", "read", "--session", session]);
    assert_ok(&out, "read");
    let value = parse_json(&stdout(&out));
    assert_eq!(value["result"], "read", "expected a read result: {value}");
    value["events"].as_array().cloned().unwrap_or_default()
}

/// Poll `f` until it returns `Some`, or panic after `timeout`. A poll loop, not a
/// fixed sleep, so the assertion is robust under load.
fn poll_until<T>(what: &str, timeout: Duration, mut f: impl FnMut() -> Option<T>) -> T {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(value) = f() {
            return value;
        }
        assert!(Instant::now() < deadline, "condition never held: {what}");
        std::thread::sleep(Duration::from_millis(50));
    }
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

/// The single watch's state string from `status`, or `None` if there is no watch.
fn watch_state(daemon: &Daemon, session: &str) -> Option<String> {
    let out = daemon.run(&["--json", "status", "--session", session]);
    assert_ok(&out, "status");
    let value = parse_json(&stdout(&out));
    let watches = value["watches"].as_array()?;
    let watch = watches.first()?;
    watch["state"].as_str().map(str::to_string)
}

// ==== ac-09-1: full production path — watch stub → read → unwatch stops =========

#[test]
fn ac09_1_watch_stub_events_appear_in_read_then_unwatch_stops() {
    let daemon = Daemon::start();
    let session = "s1";

    // Full path: CLI watch → serve → supervisor → resolver → spawn stub.
    assert_ok(
        &daemon.run(&[
            "watch",
            "stub",
            "demo",
            "--interval-ms",
            "150",
            "--session",
            session,
        ]),
        "watch stub",
    );

    // The supervisor spawned the stub: the watch reaches `running` with a pid.
    poll_until("stub watch running", Duration::from_secs(10), || {
        (watch_state(&daemon, session).as_deref() == Some("running")).then_some(())
    });

    // Synthetic events flow through the durable bus and surface in `read`.
    let events = poll_until("stub events readable", Duration::from_secs(10), || {
        let events = read_events(&daemon, session);
        (!events.is_empty()).then_some(events)
    });
    let first = &events[0];
    assert_eq!(first["topic"], "stub.demo", "events land on stub.<label>");
    assert_eq!(
        first["body"]["source"], "stub",
        "the synthetic body identifies the stub: {first}"
    );

    // Unwatch (last interest gone) → the supervisor tears the adapter down and
    // the watch reaches `stopped` (no zombie poller).
    assert_ok(
        &daemon.run(&["unwatch", "stub", "demo", "--session", session]),
        "unwatch stub",
    );
    poll_until("stub watch stopped", Duration::from_secs(10), || {
        (watch_state(&daemon, session).as_deref() == Some("stopped")).then_some(())
    });
}

// ==== ac-09-2 #4: the delivery cursor advances (mid-turn publishes surface) =====

#[test]
fn ac09_2_delivery_cursor_advances_across_reads() {
    let daemon = Daemon::start();
    let session = "s-cursor";

    assert_ok(
        &daemon.run(&[
            "watch",
            "stub",
            "cursor",
            "--interval-ms",
            "120",
            "--session",
            session,
        ]),
        "watch stub cursor",
    );

    // First read: some events, advancing the cursor to the last one seen.
    let first = poll_until("first batch", Duration::from_secs(10), || {
        let events = read_events(&daemon, session);
        (!events.is_empty()).then_some(events)
    });
    let last_offset_first = first
        .iter()
        .map(|e| e["offset"].as_u64().unwrap())
        .max()
        .unwrap();

    // A later read surfaces publishes that happened AFTER the first read (the
    // "mid-turn publish shows up on the next read" property), with strictly
    // greater offsets — proof the cursor advanced rather than replaying.
    let second = poll_until(
        "later batch after cursor advance",
        Duration::from_secs(10),
        || {
            let events = read_events(&daemon, session);
            (!events.is_empty()).then_some(events)
        },
    );
    let min_offset_second = second
        .iter()
        .map(|e| e["offset"].as_u64().unwrap())
        .min()
        .unwrap();
    assert!(
        min_offset_second > last_offset_first,
        "second read must be strictly newer events ({min_offset_second} > {last_offset_first}), not a replay"
    );
}

// ==== ac-09-2 #5: two subscribers each see events with INDEPENDENT cursors ======

#[test]
fn ac09_2_two_subscribers_independent_cursors() {
    let daemon = Daemon::start();

    // Subscribe BOTH sessions to the topic while it is still empty, so each
    // baselines at the same point and will see the very first event — a
    // deterministic setup for the fan-out assertion. (Subscribing after the
    // adapter had begun would baseline the late subscriber past early events.)
    for session in ["a", "b"] {
        assert_ok(
            &daemon.run(&["subscribe", "stub.fanout", "--session", session]),
            "subscribe to stub.fanout",
        );
    }

    // A THIRD session's watch starts the one shared adapter publishing to
    // stub.fanout (one entity, one process).
    assert_ok(
        &daemon.run(&[
            "watch",
            "stub",
            "fanout",
            "--interval-ms",
            "120",
            "--session",
            "starter",
        ]),
        "watch stub fanout",
    );

    // Session A consumes events, advancing ONLY A's cursor. Drain it a few times.
    let a_events = poll_until("A receives events", Duration::from_secs(10), || {
        let events = read_events(&daemon, "a");
        (!events.is_empty()).then_some(events)
    });
    let _ = read_events(&daemon, "a");
    let _ = read_events(&daemon, "a");

    // Session B, with its OWN cursor, still sees the SAME early events — A's
    // consumption did not advance B. Both baselined on the empty topic, so each
    // independently observes from offset 0.
    let b_events = poll_until(
        "B receives events independently",
        Duration::from_secs(10),
        || {
            let events = read_events(&daemon, "b");
            (!events.is_empty()).then_some(events)
        },
    );

    let a_offsets: Vec<u64> = a_events
        .iter()
        .map(|e| e["offset"].as_u64().unwrap())
        .collect();
    let b_offsets: Vec<u64> = b_events
        .iter()
        .map(|e| e["offset"].as_u64().unwrap())
        .collect();
    assert_eq!(
        a_offsets.first(),
        Some(&0),
        "A reads from the oldest event (it baselined on the empty topic)"
    );
    assert_eq!(
        b_offsets.first(),
        Some(&0),
        "B has an INDEPENDENT cursor: A's reads did not consume B's copy of event 0"
    );
}

// ==== ac-09-2 #3: wake fires — `mailbox wait` exits 2 when the stub publishes ===

#[test]
fn ac09_2_wait_wakes_with_exit_2_on_stub_publish() {
    let daemon = Daemon::start();
    let session = "s-wait";

    // Watching subscribes the session; the stub then publishes on its interval,
    // and each publish fires the wake kick.
    assert_ok(
        &daemon.run(&[
            "watch",
            "stub",
            "wake",
            "--interval-ms",
            "120",
            "--session",
            session,
        ]),
        "watch stub wake",
    );

    // `mailbox wait` blocks on the wake channel and exits 2 ("you have mail") when
    // the stub's publish kicks it — the asyncRewake contract, no agent re-arm.
    let mut waiter = daemon.spawn_wait(session);
    let exit = wait_with_timeout(&mut waiter, Duration::from_secs(10));
    assert_eq!(
        exit.code(),
        Some(2),
        "mailbox wait must exit 2 when the stub delivers mail"
    );
}

// ==== by-hand usability: the stub runs standalone (documented experiment) =======

#[test]
fn stub_adapter_runs_standalone_and_emits_publishes() {
    // The stub is usable BY HAND (not only bridge-supervised): feed it a config
    // line on stdin and it prints `Publish` NDJSON to stdout, then exits 0 at the
    // configured count. This is the documented experiment path.
    let mut child = Command::new(stub_bin())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn stub adapter by hand");

    child
        .stdin
        .take()
        .expect("stub stdin")
        .write_all(b"{\"topic\":\"stub.byhand\",\"interval_ms\":10,\"count\":3}\n")
        .expect("write config line");

    let mut out = String::new();
    child
        .stdout
        .take()
        .expect("stub stdout")
        .read_to_string(&mut out)
        .expect("read stub stdout");
    let status = wait_with_timeout(&mut child, Duration::from_secs(10));
    assert_eq!(status.code(), Some(0), "a finite-count stub exits 0");

    let lines: Vec<&str> = out.lines().filter(|l| !l.trim().is_empty()).collect();
    assert_eq!(lines.len(), 3, "count=3 ⇒ exactly three publishes: {out:?}");
    let first = parse_json(lines[0]);
    assert_eq!(first["type"], "publish");
    assert_eq!(first["topic"], "stub.byhand");
    assert_eq!(first["body"]["source"], "stub");
}
