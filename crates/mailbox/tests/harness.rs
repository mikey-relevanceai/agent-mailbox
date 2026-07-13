//! Claude Code harness integration tests (card 11), driving the REAL `mailbox`
//! binary end to end WITHOUT a live Claude Code.
//!
//! Each test simulates the hook environment: it feeds the hook payload JSON on
//! `mailbox harness arm` / `cleanup`'s stdin (exactly as Claude Code would) and
//! runs everything against a real `mailbox serve` daemon in a tempdir. No agent
//! ever runs an arm command — the hook launches the waiter, which self-respawns
//! and wakes on its own.
//!
//! Covered (the four acceptance criteria):
//! - **AC1**: an idle *subscribed* session wakes (exit 2) on a publish, with no
//!   agent-run arm. A *not-subscribed* session's arm exits 0 and starts no waiter.
//! - **AC2**: a publish that lands while no waiter is armed surfaces on the NEXT
//!   arm (the delivery cursor keeps it unread until read).
//! - **AC3**: `cleanup` reaps the waiter (pid gone, process dead) AND drops the
//!   session's interests/subscriptions (interest count 0).
//! - **AC4**: the waiter survives a shrunk max-block by self-respawning, and still
//!   wakes on a publish that arrives after several re-execs.
//!
//! Flakiness discipline (mirrors `tests/stub_e2e.rs`): poll for readiness with
//! bounded deadlines rather than fixed sleeps; reap every child on drop.

use std::io::{Read, Write};
use std::os::unix::net::UnixStream as StdUnixStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

use tempfile::TempDir;

fn mailbox_bin() -> &'static str {
    env!("CARGO_BIN_EXE_mailbox")
}

/// The reference stub adapter binary, built if missing (only the interest test
/// needs it). Mirrors `tests/stub_e2e.rs`.
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

/// A running daemon reaped (killed + waited) on drop.
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
        let child = Command::new(mailbox_bin())
            .arg("serve")
            .env("AGENT_MAILBOX_DB", &db_path)
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

    fn waiters_dir(&self) -> PathBuf {
        self.db_path.parent().unwrap().join("waiters")
    }

    /// The pidfile the harness writes for a session's waiter.
    fn pidfile(&self, session: &str) -> PathBuf {
        // `session` here is always in the lowercase/digit safe set, so it encodes
        // to itself (see `mailbox_harness::encode_session`).
        self.waiters_dir().join(format!("{session}.waiter.pid"))
    }

    /// Run a `mailbox` client command against this daemon and return its output.
    fn run(&self, args: &[&str]) -> Output {
        Command::new(mailbox_bin())
            .args(args)
            .env("AGENT_MAILBOX_DB", &self.db_path)
            .env("RUST_LOG", "error")
            .output()
            .expect("run mailbox client")
    }

    /// Spawn `mailbox harness arm`, feeding it the hook payload JSON on stdin and
    /// closing stdin (EOF) so it parses the session. stderr is piped so a wake
    /// reminder can be asserted; stdout is discarded. Returned in an [`ArmChild`]
    /// so a test panic can never leak the live waiter it execs into.
    fn spawn_arm(&self, session: &str, extra: &[&str]) -> ArmChild {
        let mut cmd = Command::new(mailbox_bin());
        cmd.args(["harness", "arm"])
            .args(extra)
            .env("AGENT_MAILBOX_DB", &self.db_path)
            .env("RUST_LOG", "error")
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::piped());
        let mut child = cmd.spawn().expect("spawn mailbox harness arm");
        let payload = format!(r#"{{"session_id":"{session}","hook_event_name":"Stop"}}"#);
        let mut stdin = child.stdin.take().expect("arm stdin");
        stdin
            .write_all(payload.as_bytes())
            .expect("write hook payload");
        drop(stdin); // EOF so arm's stdin read returns
        ArmChild(child)
    }

    /// Spawn `mailbox wait` directly (bypassing `arm`), to exercise the waiter's
    /// own arm-iff-subscribed re-check. Wrapped in [`ArmChild`] for the same
    /// leak-proof teardown.
    fn spawn_wait(&self, session: &str, extra: &[&str]) -> ArmChild {
        let child = Command::new(mailbox_bin())
            .args(["wait", "--session", session])
            .args(extra)
            .env("AGENT_MAILBOX_DB", &self.db_path)
            .env("RUST_LOG", "error")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn mailbox wait");
        ArmChild(child)
    }

    /// The pid recorded in a session's waiter pidfile, or `None` if absent/garbage.
    fn pidfile_pid(&self, session: &str) -> Option<u32> {
        std::fs::read_to_string(self.pidfile(session))
            .ok()?
            .trim()
            .parse()
            .ok()
    }

    /// Run `mailbox harness cleanup` for a session (feeding the SessionEnd payload)
    /// and return its output.
    fn cleanup(&self, session: &str) -> Output {
        let mut child = Command::new(mailbox_bin())
            .args(["harness", "cleanup"])
            .env("AGENT_MAILBOX_DB", &self.db_path)
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
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// A spawned `mailbox harness arm` child, which execs (same PID) into the waiter
/// and may self-respawn in place. Wrapped in a Drop guard so a test panic can
/// never leak a live waiter process (item J). Deref(Mut) to the inner `Child` so
/// the existing `child`-taking helpers keep working via deref coercion.
struct ArmChild(Child);

impl std::ops::Deref for ArmChild {
    type Target = Child;
    fn deref(&self) -> &Child {
        &self.0
    }
}

impl std::ops::DerefMut for ArmChild {
    fn deref_mut(&mut self) -> &mut Child {
        &mut self.0
    }
}

impl Drop for ArmChild {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
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

fn poll_until<T>(what: &str, timeout: Duration, mut f: impl FnMut() -> Option<T>) -> T {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(value) = f() {
            return value;
        }
        assert!(Instant::now() < deadline, "condition never held: {what}");
        std::thread::sleep(Duration::from_millis(25));
    }
}

/// Wait for `child` to exit within `timeout`, killing it if it overruns. Returns
/// `None` if it had to be killed (i.e. it did not exit on its own in time).
fn wait_within(child: &mut Child, timeout: Duration) -> Option<std::process::ExitStatus> {
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
fn drain_stderr(child: &mut Child) -> String {
    let mut buf = String::new();
    if let Some(mut err) = child.stderr.take() {
        let _ = err.read_to_string(&mut buf);
    }
    buf
}

/// The single watch's state string + interest from `status`, or `None`.
fn watch_state_interest(daemon: &Daemon, session: &str) -> Option<(String, u64)> {
    let out = daemon.run(&["--json", "status", "--session", session]);
    assert_ok(&out, "status");
    let value = parse_json(&stdout(&out));
    let watch = value["watches"].as_array()?.first()?.clone();
    Some((
        watch["state"].as_str()?.to_string(),
        watch["interest"].as_u64()?,
    ))
}

fn subscriptions(daemon: &Daemon, session: &str) -> Vec<String> {
    let out = daemon.run(&["--json", "status", "--session", session]);
    assert_ok(&out, "status");
    let value = parse_json(&stdout(&out));
    value["subscriptions"]
        .as_array()
        .map(|a| a.iter().map(|t| t.as_str().unwrap().to_string()).collect())
        .unwrap_or_default()
}

// ==== AC1: idle subscribed session wakes on a publish, with NO agent arm ========

#[test]
fn ac1_subscribed_session_wakes_on_publish_via_hook_only() {
    let daemon = Daemon::start();
    let session = "s1";
    let topic = "t.wake.x";
    assert_ok(
        &daemon.run(&["subscribe", topic, "--session", session]),
        "subscribe",
    );

    // The SessionStart/Stop hook launches the waiter — the agent runs nothing.
    let mut arm = daemon.spawn_arm(session, &[]);

    // arm records the waiter pidfile before exec-ing into `mailbox wait`.
    poll_until("waiter pidfile appears", Duration::from_secs(10), || {
        daemon.pidfile(session).exists().then_some(())
    });
    // The waiter is blocked on a quiet topic (no publish yet), so it must NOT have
    // exited already.
    assert!(
        arm.try_wait().expect("try_wait").is_none(),
        "waiter should still be blocked before any publish"
    );

    // A publish on the subscribed topic kicks the waiter.
    assert_ok(&daemon.run(&["publish", topic]), "publish");

    let status = wait_within(&mut arm, Duration::from_secs(10)).expect("waiter should exit");
    assert_eq!(
        status.code(),
        Some(2),
        "the waiter must exit 2 (wake) when mail arrives"
    );
    let reminder = drain_stderr(&mut arm);
    assert!(
        reminder.contains(&format!("mail on topic {topic}")),
        "stderr carries the payload-free reminder: {reminder:?}"
    );
}

// ==== arm-iff-subscribed: a NOT-subscribed session arms nothing =================

#[test]
fn arm_without_subscription_exits_zero_and_starts_no_waiter() {
    let daemon = Daemon::start();
    let session = "s-none";

    // No subscription: arm must decide NotSubscribed and exit 0 promptly.
    let mut arm = daemon.spawn_arm(session, &[]);
    let status = wait_within(&mut arm, Duration::from_secs(10)).expect("arm should exit");
    assert_eq!(status.code(), Some(0), "an unsubscribed arm exits 0");
    assert!(
        !daemon.pidfile(session).exists(),
        "no waiter pidfile should be written when not subscribed"
    );
}

// ==== bridge-down fail-safe: arm does NOT wake when the bridge is unreachable ====

#[test]
fn arm_with_bridge_down_exits_zero_without_waking() {
    // A tempdir with NO daemon: the socket is absent, so the probe fails.
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("mailbox.db");
    std::fs::create_dir_all(db_path.parent().unwrap()).unwrap();

    let mut child = Command::new(mailbox_bin())
        .args(["harness", "arm"])
        .env("AGENT_MAILBOX_DB", &db_path)
        .env("RUST_LOG", "error")
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn arm");
    let mut stdin = child.stdin.take().unwrap();
    stdin.write_all(br#"{"session_id":"s-bd"}"#).unwrap();
    drop(stdin);

    let status = wait_within(&mut child, Duration::from_secs(10)).expect("arm should exit");
    assert_eq!(
        status.code(),
        Some(0),
        "fail-safe: a down bridge means exit 0 (do not wake)"
    );
    let pidfile = dir.path().join("waiters").join("s-bd.waiter.pid");
    assert!(!pidfile.exists(), "no waiter armed when the bridge is down");
}

// ==== B: cleanup with the bridge down still exits 0 (TTL sweeper is the backstop) =

#[test]
fn cleanup_with_bridge_down_still_exits_zero() {
    // No daemon: EndSession is unreachable. Cleanup retries, then defers to the
    // TTL sweeper — it must never fail the SessionEnd hook.
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("mailbox.db");
    std::fs::create_dir_all(db_path.parent().unwrap()).unwrap();

    let mut child = Command::new(mailbox_bin())
        .args(["harness", "cleanup"])
        .env("AGENT_MAILBOX_DB", &db_path)
        .env("RUST_LOG", "error")
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn cleanup");
    let mut stdin = child.stdin.take().unwrap();
    stdin.write_all(br#"{"session_id":"s-cd"}"#).unwrap();
    drop(stdin);

    // The retries use short backoffs (200ms base, ~4 attempts), so this resolves
    // well within the bound.
    let status = wait_within(&mut child, Duration::from_secs(15)).expect("cleanup should exit");
    assert_eq!(
        status.code(),
        Some(0),
        "cleanup must exit 0 even when the bridge is unreachable"
    );
}

// ==== AC2: a mid-turn publish surfaces on the NEXT arm (delivery cursor) =========

#[test]
fn ac2_mid_turn_publish_surfaces_on_next_arm() {
    let daemon = Daemon::start();
    let session = "s2";
    let topic = "t.mid.x";
    assert_ok(
        &daemon.run(&["subscribe", topic, "--session", session]),
        "subscribe",
    );

    // The publish lands while NO waiter is armed (the agent is mid-turn).
    assert_ok(&daemon.run(&["publish", topic]), "publish");

    // On the next Stop, the hook arms a fresh waiter whose check-then-block sees
    // the still-unread event immediately and wakes.
    let mut arm = daemon.spawn_arm(session, &[]);
    let status = wait_within(&mut arm, Duration::from_secs(10)).expect("waiter should exit");
    assert_eq!(
        status.code(),
        Some(2),
        "a publish from before the waiter armed must still wake it (cursor kept it unread)"
    );
    assert!(drain_stderr(&mut arm).contains(&format!("mail on topic {topic}")));

    // The delivery cursor advances on read, so the event is not redelivered: a
    // read now returns it, and a subsequent read is empty.
    let out = daemon.run(&["--json", "read", "--session", session]);
    assert_ok(&out, "read");
    let events = parse_json(&stdout(&out))["events"]
        .as_array()
        .cloned()
        .unwrap();
    assert_eq!(
        events.len(),
        1,
        "the unread event is delivered exactly once"
    );
}

// ==== AC3a: cleanup reaps a LIVE waiter and drops the subscription ===============

#[test]
fn ac3_cleanup_reaps_live_waiter_and_drops_subscription() {
    let daemon = Daemon::start();
    let session = "s3";
    let topic = "quiet.topic"; // never published to, so the waiter stays blocked
    assert_ok(
        &daemon.run(&["subscribe", topic, "--session", session]),
        "subscribe",
    );

    let mut arm = daemon.spawn_arm(session, &[]);
    poll_until("waiter pidfile appears", Duration::from_secs(10), || {
        daemon.pidfile(session).exists().then_some(())
    });
    assert!(
        arm.try_wait().expect("try_wait").is_none(),
        "the waiter should be live (blocked on a quiet topic) before cleanup"
    );

    // SessionEnd cleanup: reap the waiter + drop the session's state.
    assert_ok(&daemon.cleanup(session), "cleanup");

    // The waiter process is gone (SIGTERMed), the pidfile is removed, and the
    // session no longer subscribes to anything.
    assert!(
        wait_within(&mut arm, Duration::from_secs(10)).is_some(),
        "the waiter must exit after cleanup"
    );
    assert!(
        !daemon.pidfile(session).exists(),
        "cleanup must remove the waiter pidfile"
    );
    assert!(
        subscriptions(&daemon, session).is_empty(),
        "cleanup must drop the session's subscriptions"
    );
}

// ==== AC3b: cleanup drops a watch interest (feeding the card-08 refcount) ========

#[test]
fn ac3_cleanup_drops_watch_interest_to_zero() {
    let daemon = Daemon::start();
    let session = "s3b";
    // A watch attaches this session's refcounted interest and spawns the stub.
    assert_ok(
        &daemon.run(&[
            "watch",
            "stub",
            "demo",
            "--interval-ms",
            "60000",
            "--session",
            session,
        ]),
        "watch stub",
    );
    poll_until("watch has interest 1", Duration::from_secs(10), || {
        (watch_state_interest(&daemon, session)?.1 == 1).then_some(())
    });

    // SessionEnd cleanup ends the session on the bridge: its interest is dropped
    // and, being the last, the stub adapter is stopped (no zombie poller).
    assert_ok(&daemon.cleanup(session), "cleanup");
    poll_until(
        "watch interest drops to 0 and adapter stops",
        Duration::from_secs(10),
        || {
            let (state, interest) = watch_state_interest(&daemon, session)?;
            (interest == 0 && state == "stopped").then_some(())
        },
    );
    assert!(subscriptions(&daemon, session).is_empty());
}

// ==== A / HIGH#1: a second arm is a lock loser and does NOT orphan the first =====

/// Two arms for the same idle session: the second's waiter loses the single-waiter
/// lock and exits WITHOUT touching the pidfile, so the pidfile keeps naming the
/// LIVE first waiter — which cleanup then reaps, leaving no zombie. This is the
/// regression for HIGH#1 (a doomed re-arm used to overwrite the pidfile with its
/// own dead pid, orphaning the real waiter forever).
#[test]
fn high1_second_arm_loses_lock_and_pidfile_names_the_live_waiter() {
    let daemon = Daemon::start();
    let session = "s-race1";
    let topic = "quiet.race1"; // never published to → the waiter stays blocked
    assert_ok(
        &daemon.run(&["subscribe", topic, "--session", session]),
        "subscribe",
    );

    // Arm 1 → the live waiter (writes the pidfile after taking the lock).
    let mut arm1 = daemon.spawn_arm(session, &[]);
    poll_until("waiter 1 pidfile appears", Duration::from_secs(10), || {
        daemon.pidfile_pid(session).map(|_| ())
    });
    let live_pid = daemon.pidfile_pid(session).unwrap();
    assert_eq!(
        live_pid,
        arm1.id(),
        "the pidfile names the live waiter (arm 1)"
    );

    // Arm 2 (same session): its waiter loses the lock and exits (non-2), and must
    // NOT have overwritten the pidfile.
    let mut arm2 = daemon.spawn_arm(session, &[]);
    let arm2_status = wait_within(&mut arm2, Duration::from_secs(10)).expect("arm 2 should exit");
    assert_ne!(
        arm2_status.code(),
        Some(2),
        "the lock-loser arm must not wake"
    );
    assert!(
        arm1.try_wait().expect("try_wait").is_none(),
        "the first waiter must still be alive after the second arm"
    );
    assert_eq!(
        daemon.pidfile_pid(session),
        Some(live_pid),
        "the pidfile must still name the LIVE first waiter, not the dead lock-loser"
    );

    // Cleanup reaps the recorded (live) waiter → no zombie survives.
    assert_ok(&daemon.cleanup(session), "cleanup");
    assert!(
        wait_within(&mut arm1, Duration::from_secs(10)).is_some(),
        "cleanup must reap the live waiter"
    );
    assert!(
        !daemon.pidfile(session).exists(),
        "cleanup removes the pidfile — no surviving waiter"
    );
}

// ==== A / HIGH#2: an arm that races SessionEnd leaves no orphan waiter ===========

/// Simulates an `arm` whose pre-exec probe passed but whose `SessionEnd` landed
/// before its waiter checked: the waiter (`mailbox wait`) re-checks subscriptions
/// after taking the lock, finds none, and self-exits WITHOUT waking or orphaning
/// (exit 0, pidfile removed). Regression for HIGH#2.
#[test]
fn high2_waiter_started_after_session_end_self_exits_no_orphan() {
    let daemon = Daemon::start();
    let session = "s-race2";
    let topic = "quiet.race2";
    assert_ok(
        &daemon.run(&["subscribe", topic, "--session", session]),
        "subscribe",
    );
    // SessionEnd lands FIRST (drops the subscription); no waiter is armed yet.
    assert_ok(&daemon.cleanup(session), "cleanup");
    assert!(subscriptions(&daemon, session).is_empty());

    // Now the raced arm's waiter starts (a generous max-block so it would block for
    // a long time if it did NOT self-exit).
    let mut waiter = daemon.spawn_wait(session, &["--max-block-ms", "5000"]);
    let status = wait_within(&mut waiter, Duration::from_secs(10)).expect("waiter should exit");
    assert_eq!(
        status.code(),
        Some(0),
        "a waiter with no subscription must self-exit 0 (not wake, not block as an orphan)"
    );
    assert!(
        !daemon.pidfile(session).exists(),
        "the self-exit must leave no pidfile"
    );
}

// ==== AC4: the waiter self-respawns across a shrunk max-block and still wakes ====

#[test]
fn ac4_waiter_self_respawns_across_max_block_and_still_wakes() {
    let daemon = Daemon::start();
    let session = "s4";
    let topic = "t.slow.x";
    assert_ok(
        &daemon.run(&["subscribe", topic, "--session", session]),
        "subscribe",
    );

    // A tiny max-block forces repeated self-respawns while the session stays idle.
    let mut arm = daemon.spawn_arm(session, &["--max-block-ms", "250"]);
    poll_until("waiter pidfile appears", Duration::from_secs(10), || {
        daemon.pidfile(session).exists().then_some(())
    });

    // Across ~4 max-block windows the waiter must NOT die at the first timeout —
    // it keeps re-execing. Confirm it is still alive well past one max-block.
    let alive_deadline = Instant::now() + Duration::from_millis(1200);
    while Instant::now() < alive_deadline {
        assert!(
            arm.try_wait().expect("try_wait").is_none(),
            "the waiter must survive its max-block by self-respawning, not exit"
        );
        std::thread::sleep(Duration::from_millis(100));
    }

    // A publish after several re-execs still wakes the (respawned) waiter.
    assert_ok(&daemon.run(&["publish", topic]), "publish");
    let status = wait_within(&mut arm, Duration::from_secs(10)).expect("waiter should exit");
    assert_eq!(
        status.code(),
        Some(2),
        "the self-respawned waiter must still wake on a publish"
    );
    assert!(drain_stderr(&mut arm).contains(&format!("mail on topic {topic}")));
}

// ==== install-hooks emits a valid, parseable settings.json snippet ==============

#[test]
fn install_hooks_emits_valid_settings_snippet() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("mailbox.db");
    let out = Command::new(mailbox_bin())
        .args(["--json", "harness", "install-hooks"])
        .env("AGENT_MAILBOX_DB", &db_path)
        .env("RUST_LOG", "error")
        .output()
        .expect("run install-hooks");
    assert_ok(&out, "install-hooks");
    let value = parse_json(&stdout(&out));

    // The three hooks are wired: SessionStart(startup)+Stop as asyncRewake arm,
    // SessionEnd as cleanup.
    let hooks = &value["hooks"];
    assert_eq!(hooks["SessionStart"][0]["matcher"], "startup");
    let arm = &hooks["SessionStart"][0]["hooks"][0];
    assert_eq!(arm["asyncRewake"], true);
    assert!(arm["timeout"].as_u64().unwrap() > 0);
    assert!(
        arm["command"]
            .as_str()
            .unwrap()
            .contains("harness arm --max-block-ms")
    );
    assert!(
        hooks["SessionEnd"][0]["hooks"][0]["command"]
            .as_str()
            .unwrap()
            .contains("harness cleanup")
    );
    assert!(
        hooks["SessionEnd"][0]["hooks"][0]
            .get("asyncRewake")
            .is_none()
    );

    // --settings merges into a file, preserving unrelated keys.
    let settings = dir.path().join("settings.json");
    std::fs::write(&settings, r#"{"model":"sonnet"}"#).unwrap();
    let merged_out = Command::new(mailbox_bin())
        .args(["harness", "install-hooks", "--settings"])
        .arg(&settings)
        .env("AGENT_MAILBOX_DB", &db_path)
        .env("RUST_LOG", "error")
        .output()
        .expect("run install-hooks --settings");
    assert_ok(&merged_out, "install-hooks --settings");
    let merged = parse_json(&std::fs::read_to_string(&settings).unwrap());
    assert_eq!(merged["model"], "sonnet", "unrelated settings preserved");
    assert!(merged["hooks"]["Stop"].is_array(), "hooks merged in");
}

// ==== install-skills writes the embedded skill, idempotently ====================

/// Drive the REAL binary's `install-skills` against a tempdir (never the user's
/// `~/.claude`): a fresh dir is `created`, a re-run is `unchanged`, and the file
/// on disk is the skill this binary embedded.
#[test]
fn install_skills_installs_the_embedded_skill_and_is_idempotent() {
    let dir = TempDir::new().unwrap();
    let skills_dir = dir.path().join("skills");

    let install = |args: &[&str]| -> Output {
        Command::new(mailbox_bin())
            .args(["--json", "harness", "install-skills", "--skills-dir"])
            .arg(&skills_dir)
            .args(args)
            .env("RUST_LOG", "error")
            .output()
            .expect("run install-skills")
    };

    // First run: the skills dir does not exist yet, so the skill is created.
    let first = install(&[]);
    assert_ok(&first, "install-skills");
    let report = parse_json(&stdout(&first));
    assert_eq!(report["skills"][0]["name"], "agent-mailbox");
    assert_eq!(report["skills"][0]["outcome"], "created");

    let installed = skills_dir.join("agent-mailbox").join("SKILL.md");
    assert!(
        installed.is_file(),
        "the skill lands at <dir>/<name>/SKILL.md"
    );
    let written = std::fs::read_to_string(&installed).unwrap();
    assert!(
        written.contains("name: agent-mailbox"),
        "the installed file is the real skill (with frontmatter)"
    );

    // Second run: byte-identical content already there → a visible no-op.
    let second = install(&[]);
    assert_ok(&second, "install-skills re-run");
    assert_eq!(
        parse_json(&stdout(&second))["skills"][0]["outcome"],
        "unchanged",
        "re-running install-skills must be idempotent"
    );
    assert_eq!(
        std::fs::read_to_string(&installed).unwrap(),
        written,
        "the re-run must not alter the installed content"
    );

    // A locally-edited (stale) skill is refreshed back to the shipped content.
    std::fs::write(&installed, "stale\n").unwrap();
    let third = install(&[]);
    assert_ok(&third, "install-skills refresh");
    assert_eq!(
        parse_json(&stdout(&third))["skills"][0]["outcome"],
        "updated"
    );
    assert_eq!(std::fs::read_to_string(&installed).unwrap(), written);
}
