//! Claude Code harness integration tests (card 11), driving the REAL `mailbox`
//! binary end to end WITHOUT a live Claude Code.
//!
//! Each test simulates the hook environment: it feeds the hook payload JSON on
//! `mailbox harness arm` / `cleanup`'s stdin (exactly as Claude Code would) and
//! runs everything against a real `mailbox serve` daemon in a tempdir. No agent
//! ever runs an arm command — the hook launches the waiter, and the waiter's exit-2
//! is what drives the next re-arm.
//!
//! Covered (the four acceptance criteria):
//! - **AC1**: an idle *subscribed* session wakes (exit 2) on a publish, with no
//!   agent-run arm. (Card 16 replaced the "not-subscribed arms nothing" half: arm
//!   now registers the session's agent inbox first, so a live session always has a
//!   subscription and always arms — the fail-safes are unchanged.)
//! - **AC2**: a publish that lands while no waiter is armed surfaces on the NEXT
//!   arm (the delivery cursor keeps it unread until read).
//! - **AC3**: `cleanup` reaps the waiter (pid gone, process dead) AND drops the
//!   session's interests/subscriptions (interest count 0).
//! - **AC4** (rewritten for ADR-0006): every crossing of the waiter's max-block is a
//!   **wake** (exit 2) carrying the benign re-arm notice — never a silent death — so
//!   an idle session is never left armed by nobody; and a publish across that
//!   boundary still wakes it. This is the regression test for the wake-lifetime bug:
//!   the waiter used to re-exec itself at the boundary, which did NOT reset the hook
//!   timeout, so the harness killed it — and an idle session fires no further `Stop`
//!   to re-arm it.
//!
//! Flakiness discipline (mirrors `tests/stub_e2e.rs`): poll for readiness with
//! bounded deadlines rather than fixed sleeps; reap every child on drop.

use std::io::{Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixStream as StdUnixStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

use tempfile::TempDir;

mod common;

// The ONE session-stripping spawner, shared by every test binary (tests/common).
// Every `mailbox` subprocess in this file goes through it, so no test can silently
// inherit the DEVELOPER's `CLAUDE_CODE_SESSION_ID` and behave differently on a laptop
// than in CI.
use common::{mailbox_bin, mailbox_command};

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
        let child = mailbox_command()
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

    /// Stop the bridge process (graceful SIGTERM, then reap) WITHOUT dropping the
    /// tempdir — the store survives, so a test can arm against a store whose daemon is
    /// down (the bridge-blip case) and then bring the bridge back with
    /// [`Daemon::start_bridge`].
    fn stop_bridge(&mut self) {
        let pid = nix::unistd::Pid::from_raw(self.child.id() as i32);
        let _ = nix::sys::signal::kill(pid, nix::sys::signal::Signal::SIGTERM);
        let _ = self.child.wait();
        // The socket file outlives the process; remove it so a client fails fast with
        // "bridge down" rather than blocking on a connect to a dead listener.
        let _ = std::fs::remove_file(socket_for(&self.db_path));
    }

    /// Bring the bridge back up on the SAME store.
    fn start_bridge(&mut self) {
        self.child = mailbox_command()
            .arg("serve")
            .env("AGENT_MAILBOX_DB", &self.db_path)
            .env("MAILBOX_STUB_ADAPTER_BIN", stub_bin())
            .env("RUST_LOG", "error")
            .stdin(Stdio::null())
            .spawn()
            .expect("respawn mailbox serve");
        wait_for_socket(&socket_for(&self.db_path), Duration::from_secs(10));
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
        mailbox_command()
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
        let mut cmd = mailbox_command();
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
        let child = mailbox_command()
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
        let mut child = mailbox_command()
            .args(["harness", "cleanup"])
            .env("AGENT_MAILBOX_DB", &self.db_path)
            // A tempdir sentinel root so cleanup's ADR-0008 sentinel removal can never
            // touch the real ~/.mailbox (these tests never create one, but the safety
            // rule holds regardless).
            .env(
                "MAILBOX_SENTINEL_ROOT",
                self.db_path.parent().unwrap().join("sentinel"),
            )
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

/// A spawned `mailbox harness arm` child, which execs (same PID) into the waiter.
/// Wrapped in a Drop guard so a test panic can never leak a live waiter process
/// (item J). Deref(Mut) to the inner `Child` so the existing `child`-taking helpers
/// keep working via deref coercion.
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

/// `kill(pid, 0)`: true while `pid` still names a live (non-reaped) process. The
/// same probe `waiter_alive` uses, so a test sees exactly what `mailbox agents` sees.
fn pid_alive(pid: u32) -> bool {
    matches!(
        nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid as i32), None),
        Ok(())
    )
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

// ==== always-on inbox (card 16 / ADR-0007): arm registers, then arms ============

/// A session with NO prior subscription still ends up armed, because `arm` now
/// registers its agent inbox first (always-on, ADR-0007). This replaces card 11's
/// "an unsubscribed arm exits 0 and starts no waiter": the arm-iff-subscribed rule
/// is unchanged — it is the *set of subscriptions* that is now never empty for a
/// live session, which is what makes every agent addressable by its peers.
#[test]
fn arm_registers_the_agent_inbox_and_then_arms_a_waiter() {
    let daemon = Daemon::start();
    let session = "s-none";

    let mut arm = daemon.spawn_arm(session, &[]);
    // The waiter is armed (the pidfile appears once it holds the single-waiter
    // lock), and the session is now subscribed to its own inbox topic.
    poll_until("waiter pidfile appears", Duration::from_secs(10), || {
        daemon.pidfile(session).exists().then_some(())
    });
    assert_eq!(
        subscriptions(&daemon, session),
        vec![format!("agent.{session}")],
        "arm must have registered the session's inbox, and nothing else"
    );

    // It is a REAL waiter: a peer's message to that inbox wakes it (exit 2) with
    // the payload-free reminder.
    assert_ok(
        &daemon.run(&["send", session, "--text", "hi", "--session", "s-peer"]),
        "send",
    );
    let status = wait_within(&mut arm, Duration::from_secs(10)).expect("waiter should wake");
    assert_eq!(status.code(), Some(2), "mail in the inbox wakes the waiter");
    let reminder = drain_stderr(&mut arm);
    assert!(
        reminder.contains(&format!("mail on topic agent.{session}")),
        "stderr carries the payload-free reminder: {reminder:?}"
    );
}

/// `arm` runs on every `Stop`, so registration must be idempotent: repeated arms
/// leave exactly ONE inbox subscription (and, because `subscribe` never rebaselines
/// an existing subscription, they cannot skip mail the agent has not read).
#[test]
fn repeated_arms_do_not_duplicate_the_inbox_subscription() {
    let daemon = Daemon::start();
    let session = "s-rearm";

    for attempt in 1..=3 {
        // Arm (registering the inbox and exec-ing the waiter), then drop the child
        // — `ArmChild`'s Drop kills and reaps the waiter, releasing the
        // single-waiter lock so the next arm is not a lock loser. The SUBSCRIPTION
        // survives (only SessionEnd drops it), which is exactly the state a re-arm
        // must be idempotent against.
        {
            let _arm = daemon.spawn_arm(session, &[]);
            poll_until("the inbox is registered", Duration::from_secs(10), || {
                (!subscriptions(&daemon, session).is_empty()).then_some(())
            });
        }
        assert_eq!(
            subscriptions(&daemon, session),
            vec![format!("agent.{session}")],
            "arm #{attempt} must leave exactly one inbox subscription"
        );
    }
}

/// SessionEnd deregisters the inbox: the session's subscriptions are dropped, so
/// it disappears from `mailbox agents` and is no longer addressable.
#[test]
fn cleanup_deregisters_the_agent_inbox() {
    let daemon = Daemon::start();
    let session = "s-bye";

    let mut arm = daemon.spawn_arm(session, &[]);
    poll_until("waiter pidfile appears", Duration::from_secs(10), || {
        daemon.pidfile(session).exists().then_some(())
    });
    assert!(agent_sessions(&daemon).contains(&session.to_string()));

    assert_ok(&daemon.cleanup(session), "cleanup");
    let _ = wait_within(&mut arm, Duration::from_secs(10));

    assert!(
        subscriptions(&daemon, session).is_empty(),
        "SessionEnd drops the inbox subscription"
    );
    assert!(
        !agent_sessions(&daemon).contains(&session.to_string()),
        "a departed session is no longer listed as an addressable agent"
    );
    // And it is no longer addressable: a send to it now fails loudly.
    let out = daemon.run(&["send", session, "--text", "hi", "--session", "s-peer"]);
    assert!(
        !out.status.success(),
        "sending to a departed agent must fail, not vanish into a void"
    );
}

/// The session ids `mailbox agents` reports.
fn agent_sessions(daemon: &Daemon) -> Vec<String> {
    let out = daemon.run(&["--json", "agents", "--session", "s-observer"]);
    assert_ok(&out, "agents");
    let value: serde_json::Value =
        serde_json::from_str(String::from_utf8_lossy(&out.stdout).trim()).expect("agents json");
    value["agents"]
        .as_array()
        .map(|rows| {
            rows.iter()
                .filter_map(|a| a["session"].as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

// ==== bridge-down: arm FAILS OPEN (it arms anyway) but never wakes =============

/// With no bridge AND no store at all, there is genuinely nothing to wait on: `arm`
/// still launches the waiter (fail-open — it cannot know whether the session is
/// subscribed), and the waiter finds no store and exits 0. No wake, no leaked process.
///
/// **Contract change** (was: "a down bridge means arm skips"): skipping was the bug —
/// see `arm_with_the_bridge_down_still_arms_a_waiter_that_wakes_when_it_returns`. What
/// is preserved, and is what this test guards, is that a down bridge never produces a
/// WAKE (exit 2) — that would hot-loop the session for as long as the bridge is down.
#[test]
fn arm_with_bridge_down_and_no_store_exits_zero_without_waking() {
    // A tempdir with NO daemon and NO db file: the socket is absent, so the probe
    // fails, and the waiter we fail-open into has no store to open.
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("mailbox.db");
    std::fs::create_dir_all(db_path.parent().unwrap()).unwrap();

    let mut child = mailbox_command()
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
        "a down bridge must never WAKE (exit 2 would hot-loop while it is down); with no store \
         there is nothing to wait on either, so the waiter exits 0"
    );
    let pidfile = dir.path().join("waiters").join("s-bd.waiter.pid");
    assert!(
        !pidfile.exists(),
        "the self-exiting waiter must leave no pidfile behind"
    );
}

/// **The regression test for adv-1: a bridge blip at a re-arm `Stop` must not deafen
/// the session forever.**
///
/// `arm` probes the bridge to decide whether to arm. It used to SKIP on a probe failure
/// ("fail safe: do not wake"). But the re-arm loop now depends on `arm` succeeding at
/// *every* re-arm `Stop` (ADR-0006), and an idle session fires no further `Stop` — so a
/// single momentary blip left the session with no waiter and nothing to retry it. Deaf,
/// permanently, from one dropped connection.
///
/// So arm FAILS OPEN: it arms anyway, which is safe because the waiter re-checks
/// subscriptions itself under its lock. This proves the whole path: with the bridge
/// DOWN, a live waiter is armed; when the bridge comes back and publishes, that waiter
/// — blocked on its FIFO the entire time — is kicked and wakes with the topic.
#[test]
fn arm_with_the_bridge_down_still_arms_a_waiter_that_wakes_when_it_returns() {
    let mut daemon = Daemon::start();
    let session = "s-blip";
    let topic = "t.blip";
    assert_ok(
        &daemon.run(&["subscribe", topic, "--session", session]),
        "subscribe",
    );

    // The blip: the bridge goes away just as the agent goes idle and `Stop` fires.
    daemon.stop_bridge();

    let mut arm = daemon.spawn_arm(session, &["--max-block-ms", "60000"]);
    let pid = poll_until(
        "a live waiter is armed even though the bridge probe FAILED (fail-open)",
        Duration::from_secs(10),
        || daemon.pidfile_pid(session).filter(|&pid| pid_alive(pid)),
    );

    // The bridge returns and a peer publishes. The waiter never needed the daemon to
    // block; it needs it only for the kick, which now lands on its FIFO.
    daemon.start_bridge();
    assert_ok(&daemon.run(&["publish", topic]), "publish");

    let status = wait_within(&mut arm, Duration::from_secs(10)).expect("the waiter must wake");
    let stderr = drain_stderr(&mut arm);
    assert_eq!(
        status.code(),
        Some(2),
        "the waiter armed during the blip must still be woken by the publish that follows \
         (pid {pid}); stderr: {stderr}"
    );
    assert!(
        stderr.contains(topic),
        "the wake must name the topic with mail; got: {stderr}"
    );
}

/// The other half of the fail-open rule: a CLEAN "you subscribe to nothing" still arms
/// nothing. Fail-open must not become arm-always — a session with no subscriptions has
/// nothing to be woken about, and a waiter for it would be pure noise.
///
/// The session id here cannot form an inbox topic (a slash is not in the grammar), so
/// the always-on inbox registration cannot give it a subscription — which is exactly
/// the state that must skip.
#[test]
fn arm_with_a_clean_no_subscriptions_answer_arms_nothing() {
    let daemon = Daemon::start();
    let session = "s/unaddressable";

    let mut arm = daemon.spawn_arm(session, &["--max-block-ms", "60000"]);
    let status = wait_within(&mut arm, Duration::from_secs(10)).expect("arm should exit");
    assert_eq!(
        status.code(),
        Some(0),
        "a session the bridge says subscribes to nothing must not be armed (and must not wake)"
    );
    assert!(
        !daemon.pidfile(session).exists(),
        "no waiter for an unsubscribed session"
    );
}

/// **adv-3: the `max_block < timeout` invariant, enforced where it is USED.**
///
/// `install-hooks` validating the pairing protects nothing if `arm` is launched from a
/// hand-edited settings.json — or from a hook entry with no `timeout` at all, where
/// Claude Code applies its own 600s default and our 55-minute max-block gets the waiter
/// KILLED mid-block (after which an idle session, firing no further `Stop`, is never
/// re-armed: the headline bug).
///
/// So `arm` takes the deadline it runs under (`--timeout-secs`, written into the hook
/// command by `install-hooks`) and CLAMPS an unsafe `--max-block-ms` down, loudly.
/// Here: an 11s deadline with a 60s block. Unclamped, the waiter would sit blocked for
/// 60s (long past the kill). Clamped, it yields at ~1s with the benign re-arm notice —
/// so the session stays armed, and the log says exactly what happened.
#[test]
fn arm_clamps_a_max_block_that_is_not_safely_below_the_hook_timeout() {
    let daemon = Daemon::start();
    let session = "s-clamp";
    let topic = "t.clamp";
    assert_ok(
        &daemon.run(&["subscribe", topic, "--session", session]),
        "subscribe",
    );

    let mut arm = daemon.spawn_arm(
        session,
        // 60s of blocking under an 11s kill deadline: the exact shape of the bug.
        &["--max-block-ms", "60000", "--timeout-secs", "11"],
    );
    let status = wait_within(&mut arm, Duration::from_secs(20))
        .expect("the waiter must yield for a re-arm well before its 60s block would have ended");
    let stderr = drain_stderr(&mut arm);

    assert_eq!(
        status.code(),
        Some(2),
        "the clamped waiter must yield for a re-arm (exit 2), not be killed mid-block; stderr: {stderr}"
    );
    assert!(
        stderr.contains("re-arming"),
        "the yield must carry the benign re-arm notice, not a claim of mail; got: {stderr}"
    );

    // ...and it must be LOUD: the operator has a broken hook config to fix.
    let log = std::fs::read_to_string(daemon.db_path.parent().unwrap().join("harness.log"))
        .expect("harness.log");
    assert!(
        log.contains("Clamped down to a safe block"),
        "the clamp must be reported at error level in harness.log; got:\n{log}"
    );
}

// ==== B: cleanup with the bridge down still exits 0 (TTL sweeper is the backstop) =

#[test]
fn cleanup_with_bridge_down_still_exits_zero() {
    // No daemon: EndSession is unreachable. Cleanup retries, then defers to the
    // TTL sweeper — it must never fail the SessionEnd hook.
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("mailbox.db");
    std::fs::create_dir_all(db_path.parent().unwrap()).unwrap();

    let mut child = mailbox_command()
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

// ==== AC4: the re-arm boundary — the regression test for the silent-un-arm bug ===

/// One turn of the REAL Claude Code loop: arm (the hook), let the waiter run to an
/// exit, and report `(exit code, stderr)`. Exit 2 is what makes the harness wake the
/// session — and therefore what makes the next `Stop` fire and re-arm. Returns the
/// pid the waiter recorded, so the caller can prove a live waiter actually existed.
fn arm_cycle(daemon: &Daemon, session: &str, max_block_ms: &str) -> (Option<i32>, String, u32) {
    let mut arm = daemon.spawn_arm(session, &["--max-block-ms", max_block_ms]);
    // A live waiter must exist for this cycle: the pidfile appears (written by the
    // waiter under its lock) and names a running process. This is the "never zero
    // live waiters" half — every cycle genuinely arms one.
    let pid = poll_until("a live waiter arms", Duration::from_secs(10), || {
        daemon.pidfile_pid(session).filter(|&pid| pid_alive(pid))
    });
    let status = wait_within(&mut arm, Duration::from_secs(20)).expect("the waiter must exit");
    (status.code(), drain_stderr(&mut arm), pid)
}

/// **The regression test for the headline bug.**
///
/// The waiter cannot outlive its hook process: Claude Code kills it at the hook
/// `timeout`, and `execv` does not reset that clock. The old design re-exec'd at
/// `max_block` and hoped for a fresh timeout; it did not get one, so the waiter was
/// killed mid-block — and because a truly idle session fires **no further `Stop`**,
/// nothing ever re-armed it. The session went silently, permanently unwakeable.
///
/// The fix is that the waiter *yields* at `max_block` instead: exit 2 (a wake) with a
/// benign notice, which guarantees a `Stop`, which re-arms a FRESH hook process. So
/// this test churns the boundary with a tiny `max_block` and asserts:
///
/// 1. every boundary crossing is a **wake (exit 2)**, never a silent exit — an exit 0
///    or 1 here IS the bug, because nothing would follow it;
/// 2. its stderr is the **benign re-arm notice**, and never claims mail that does not
///    exist;
/// 3. across the churn, every publish still produces a **mail wake** — the boundary
///    loses no events.
#[test]
fn ac4_the_rearm_boundary_always_wakes_so_an_idle_session_is_never_left_unarmed() {
    let daemon = Daemon::start();
    let session = "s4";
    let topic = "t.slow.x";
    assert_ok(
        &daemon.run(&["subscribe", topic, "--session", session]),
        "subscribe",
    );

    // Phase 1 — churn the boundary with NO mail. A tiny max-block makes each cycle
    // hit the re-arm boundary immediately.
    for cycle in 0..3 {
        let (code, stderr, pid) = arm_cycle(&daemon, session, "250");
        assert_eq!(
            code,
            Some(2),
            "cycle {cycle}: the re-arm boundary MUST be a wake (exit 2). Any other exit is the \
             bug itself: an idle session fires no further Stop, so nothing would ever re-arm it. \
             stderr: {stderr}"
        );
        assert!(
            stderr.contains("re-arming"),
            "cycle {cycle}: the re-arm wake must say plainly that it is a re-arm; got: {stderr}"
        );
        assert!(
            !stderr.contains("mail on topic"),
            "cycle {cycle}: the re-arm wake must NOT claim mail — there is none; got: {stderr}"
        );
        assert!(
            !pid_alive(pid),
            "cycle {cycle}: the yielded waiter must actually be gone"
        );
        assert!(
            !daemon.pidfile(session).exists(),
            "cycle {cycle}: a yielded waiter must not leave a pidfile naming its dead pid \
             (that is what made a killed waiter keep passing for a live one)"
        );
    }

    // Phase 2 — an event published while no waiter is live (the re-arm gap) must
    // still wake the session on the very next arm: the durable log + the waiter's
    // open→check→block ordering carry it across the boundary.
    assert_ok(&daemon.run(&["publish", topic]), "publish");
    let (code, stderr, _) = arm_cycle(&daemon, session, "60000");
    assert_eq!(
        code,
        Some(2),
        "a publish across the re-arm boundary must still wake the session; stderr: {stderr}"
    );
    assert!(
        stderr.contains(&format!("mail on topic {topic}")),
        "this wake IS mail, so it must name the topic (not the re-arm notice); got: {stderr}"
    );

    // ...and the event is still there to read (the wake is a nudge; the log is truth).
    let read = daemon.run(&["--json", "read", "--session", session]);
    assert_ok(&read, "read");
    assert_eq!(
        parse_json(&stdout(&read))["events"]
            .as_array()
            .map(Vec::len),
        Some(1),
        "the event that woke the session must be readable"
    );
}

/// A waiter the harness KILLED at its hook timeout leaves a pidfile naming a dead
/// pid. Until it is cleared, `mailbox agents` / `status` keep reporting a live waiter
/// that does not exist — the exact fingerprint that made the original bug so hard to
/// see. `arm` must reap it before arming a real waiter.
#[test]
fn arm_reaps_a_stale_pidfile_naming_a_dead_waiter() {
    let daemon = Daemon::start();
    let session = "s-stale";
    let topic = "t.stale";
    assert_ok(
        &daemon.run(&["subscribe", topic, "--session", session]),
        "subscribe",
    );

    // Plant the fingerprint: a pidfile for a pid that is not running. 2_000_000_000
    // is above any platform PID_MAX, so it can never be a live process.
    let dead = 2_000_000_000u32;
    std::fs::create_dir_all(daemon.waiters_dir()).unwrap();
    std::fs::write(daemon.pidfile(session), dead.to_string()).unwrap();

    let mut arm = daemon.spawn_arm(session, &["--max-block-ms", "60000"]);
    let pid = poll_until(
        "a fresh waiter replaces the stale pidfile",
        Duration::from_secs(10),
        || daemon.pidfile_pid(session).filter(|&pid| pid != dead),
    );
    assert!(
        pid_alive(pid),
        "the pidfile must now name a LIVE waiter, not the dead pid it was reaped from"
    );

    // And the freshly-armed waiter really is armed: a publish wakes it.
    assert_ok(&daemon.run(&["publish", topic]), "publish");
    let status = wait_within(&mut arm, Duration::from_secs(10)).expect("waiter should exit");
    assert_eq!(
        status.code(),
        Some(2),
        "the re-armed waiter must wake on mail"
    );
}

// ==== install-hooks emits a valid snippet, and merges where it should ===========

/// `install-hooks`, ALWAYS with the home redirected at a tempdir.
///
/// Every one of these tests must be hermetic: `install-hooks` now merges into the
/// default `~/.claude/settings.json` when it exists, so a run that inherited the
/// real `HOME` would edit the developer's own Claude Code settings. Redirecting
/// `AGENT_MAILBOX_HOME` (which wins over `HOME`) is what makes that impossible.
fn install_hooks(home: &Path, args: &[&str]) -> Output {
    mailbox_command()
        .args(["harness", "install-hooks"])
        .args(args)
        .env("AGENT_MAILBOX_HOME", home)
        .env("RUST_LOG", "error")
        .output()
        .expect("run install-hooks")
}

/// The default settings file under a redirected home.
fn default_settings(home: &Path) -> PathBuf {
    home.join(".claude").join("settings.json")
}

#[test]
fn install_hooks_emits_valid_settings_snippet() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("mailbox.db");
    let out = mailbox_command()
        .args(["--json", "harness", "install-hooks"])
        .env("AGENT_MAILBOX_DB", &db_path)
        // The home has no `.claude/settings.json`, so this prints only — and, more
        // to the point, it cannot touch the real one.
        .env("AGENT_MAILBOX_HOME", dir.path())
        .env("RUST_LOG", "error")
        .output()
        .expect("run install-hooks");
    assert_ok(&out, "install-hooks");
    let value = parse_json(&stdout(&out));

    // The ADR-0008 hooks are wired: SessionStart(startup) → plain session-start,
    // FileChanged(matcher = the sentinel basename) → asyncRewake wake, SessionEnd →
    // cleanup. There is NO Stop re-arm hook.
    let hooks = &value["hooks"];
    assert_eq!(hooks["SessionStart"][0]["matcher"], "startup");
    let session_start = &hooks["SessionStart"][0]["hooks"][0];
    assert!(
        session_start.get("asyncRewake").is_none(),
        "session-start must NOT be asyncRewake"
    );
    assert!(
        session_start["command"]
            .as_str()
            .unwrap()
            .contains("harness session-start")
    );
    assert!(hooks.get("Stop").is_none(), "no periodic re-arm hook");

    let file_changed = &hooks["FileChanged"][0];
    assert_eq!(file_changed["matcher"], ".mailbox-wake");
    let wake = &file_changed["hooks"][0];
    assert_eq!(wake["asyncRewake"], true);
    assert_eq!(wake["timeout"], 3600);
    assert!(wake["command"].as_str().unwrap().contains("harness wake"));

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

    // An explicit --settings merges into that file, preserving unrelated keys.
    let settings = dir.path().join("settings.json");
    std::fs::write(&settings, r#"{"model":"sonnet"}"#).unwrap();
    let merged_out = install_hooks(dir.path(), &["--settings", settings.to_str().unwrap()]);
    assert_ok(&merged_out, "install-hooks --settings");
    let merged = parse_json(&std::fs::read_to_string(&settings).unwrap());
    assert_eq!(merged["model"], "sonnet", "unrelated settings preserved");
    assert!(merged["hooks"]["FileChanged"].is_array(), "hooks merged in");
}

/// A `max_block` at or above the hook `timeout` silently reintroduces the headline
/// bug: Claude Code kills the waiter while it is still blocked, and an idle session
/// fires no further `Stop`, so nothing ever re-arms it. `install-hooks` must REFUSE
/// such a pairing — loudly, and without writing a single byte of settings.
#[test]
fn install_hooks_refuses_a_max_block_that_would_outlive_the_timeout() {
    let home = home_with_settings(|path| std::fs::write(path, r#"{"model":"opus"}"#).unwrap());
    let settings = default_settings(home.path());
    let before = std::fs::read_to_string(&settings).unwrap();

    for bad in [
        // Equal to the timeout: the waiter is killed exactly at its boundary.
        ["--timeout-secs", "600", "--max-block-ms", "600000"],
        // Beyond it: killed well before it would ever yield.
        ["--timeout-secs", "600", "--max-block-ms", "900000"],
        // Inside the margin: no room to notice the boundary and exit.
        ["--timeout-secs", "600", "--max-block-ms", "599000"],
    ] {
        let out = install_hooks(home.path(), &bad);
        assert!(
            !out.status.success(),
            "{bad:?} must be refused: it would let the harness kill the waiter mid-block, and an \
             idle session never fires the Stop that would re-arm it"
        );
        let err = stderr(&out);
        assert!(
            err.contains("max_block_ms") && err.contains("timeout"),
            "the refusal must name both knobs so it is actionable; got: {err}"
        );
        assert_eq!(
            std::fs::read_to_string(&settings).unwrap(),
            before,
            "a refused install must not touch the user's settings"
        );
    }

    // The shipped defaults, by contrast, install cleanly.
    assert_ok(&install_hooks(home.path(), &[]), "default install-hooks");
}

/// `--settings <path>` is an instruction, so a MISSING file is created — that is
/// how a settings file gets bootstrapped on purpose (the default path never is).
#[test]
fn install_hooks_with_an_explicit_settings_path_creates_a_missing_file() {
    let dir = TempDir::new().unwrap();
    let settings = dir.path().join("nested").join("settings.json");

    let out = install_hooks(dir.path(), &["--settings", settings.to_str().unwrap()]);

    assert_ok(&out, "install-hooks --settings (missing file)");
    let written = parse_json(&std::fs::read_to_string(&settings).unwrap());
    assert!(written["hooks"]["SessionStart"].is_array());
}

/// The change: with a Claude Code `settings.json` present at the default path, a
/// bare `install-hooks` MERGES into it — symmetric with `install-skills`. Unrelated
/// keys and foreign hooks survive, and a re-run does not duplicate our hooks.
#[test]
fn install_hooks_merges_into_the_default_settings_when_it_exists() {
    let home = TempDir::new().unwrap();
    let settings = default_settings(home.path());
    std::fs::create_dir_all(settings.parent().unwrap()).unwrap();
    std::fs::write(
        &settings,
        r#"{"model":"sonnet","hooks":{"Stop":[{"matcher":"","hooks":[{"type":"command","command":"echo other"}]}]}}"#,
    )
    .unwrap();

    let out = install_hooks(home.path(), &[]);
    assert_ok(&out, "install-hooks (default settings)");
    assert!(
        stdout(&out).contains(&format!(
            "merged agent-mailbox hooks into {}",
            settings.display()
        )),
        "human output must name the file it merged into: {}",
        stdout(&out)
    );

    let merged = parse_json(&std::fs::read_to_string(&settings).unwrap());
    assert_eq!(merged["model"], "sonnet", "unrelated settings preserved");
    // The foreign Stop hook is on an event our snippet does not write, so it is left
    // untouched (and alone — we never add a Stop hook).
    let stop = merged["hooks"]["Stop"].as_array().unwrap();
    assert_eq!(stop.len(), 1, "the foreign Stop hook survives untouched");
    assert_eq!(stop[0]["hooks"][0]["command"], "echo other");
    // Our hooks landed on their own events.
    assert!(
        merged["hooks"]["SessionStart"][0]["hooks"][0]["command"]
            .as_str()
            .unwrap()
            .contains("harness session-start")
    );
    assert!(
        merged["hooks"]["FileChanged"][0]["hooks"][0]["command"]
            .as_str()
            .unwrap()
            .contains("harness wake")
    );
    assert!(merged["hooks"]["SessionEnd"].is_array());

    // Idempotent: a re-run merges the same hooks without duplicating them.
    assert_ok(&install_hooks(home.path(), &[]), "install-hooks re-run");
    let again = parse_json(&std::fs::read_to_string(&settings).unwrap());
    assert_eq!(
        again, merged,
        "re-running must not change the settings file"
    );
}

/// With NO settings file at the default path, `install-hooks` prints the snippet and
/// writes NOTHING — it must not conjure a `settings.json` on a machine that has no
/// Claude Code — and it says why, naming the path it looked at.
#[test]
fn install_hooks_without_a_default_settings_file_prints_only_and_writes_nothing() {
    let home = TempDir::new().unwrap();

    let out = install_hooks(home.path(), &[]);

    assert_ok(&out, "install-hooks (no default settings)");
    let text = stdout(&out);
    assert!(
        text.contains("no Claude Code settings found at")
            && text.contains(&default_settings(home.path()).display().to_string()),
        "the print-only run must say WHY, naming the path it looked at: {text}"
    );
    // The snippet is still printed, so it can be installed by hand.
    assert!(text.contains("asyncRewake"), "the snippet is still printed");

    // Nothing was created ANYWHERE under the home — not even the `.claude` dir.
    assert_eq!(
        std::fs::read_dir(home.path()).unwrap().count(),
        0,
        "a print-only run must not create a settings.json (or its directory)"
    );
}

/// With no home at all (neither `AGENT_MAILBOX_HOME` nor `HOME`), the command still
/// succeeds: it prints the snippet and explains — never a panic, and never a guessed
/// path inside someone's config.
#[test]
fn install_hooks_with_no_home_prints_only_without_panicking() {
    let out = mailbox_command()
        .args(["harness", "install-hooks"])
        .env_remove("AGENT_MAILBOX_HOME")
        .env_remove("HOME")
        .env("RUST_LOG", "error")
        .output()
        .expect("run install-hooks with no home");

    assert_ok(&out, "install-hooks (no home)");
    let text = stdout(&out);
    assert!(
        text.contains("no home to resolve Claude Code settings under"),
        "a homeless run must explain itself: {text}"
    );
    assert!(text.contains("asyncRewake"), "the snippet is still printed");
}

/// `--json` keeps its machine contract: stdout is EXACTLY the snippet, and the
/// human note about the merge goes to stderr.
#[test]
fn install_hooks_json_keeps_stdout_clean_when_it_merges() {
    let home = TempDir::new().unwrap();
    let settings = default_settings(home.path());
    std::fs::create_dir_all(settings.parent().unwrap()).unwrap();
    std::fs::write(&settings, "{}").unwrap();

    let out = mailbox_command()
        .args(["--json", "harness", "install-hooks"])
        .env("AGENT_MAILBOX_HOME", home.path())
        .env("RUST_LOG", "error")
        .output()
        .expect("run install-hooks --json");

    assert_ok(&out, "install-hooks --json");
    // Parses whole: no note leaked into the JSON contract.
    let value = parse_json(&stdout(&out));
    assert!(value["hooks"]["SessionStart"].is_array());
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("merged agent-mailbox hooks into"),
        "the merge note belongs on stderr in --json mode"
    );
    assert!(
        parse_json(&std::fs::read_to_string(&settings).unwrap())["hooks"]["FileChanged"].is_array(),
        "--json still merges"
    );
}

// ==== hostile settings files: the user's config is NEVER destroyed ==============

/// A home whose `.claude/settings.json` is whatever the test puts there.
fn home_with_settings(prepare: impl FnOnce(&Path)) -> TempDir {
    let home = TempDir::new().unwrap();
    let settings = default_settings(home.path());
    std::fs::create_dir_all(settings.parent().unwrap()).unwrap();
    prepare(&settings);
    home
}

/// The keys a user actually loses if their settings are replaced — including the
/// permission DENY rules, i.e. their tool-permission policy.
const PRECIOUS_SETTINGS: &str = r#"{
  "model": "opus",
  "apiKeyHelper": "/usr/local/bin/key",
  "permissions": {"deny": ["Bash(rm -rf *)"]}
}
"#;

/// **The data-loss regression guard.** For every hostile settings file, the outcome
/// must be *either* intact *or* correctly merged — but NEVER a hooks-only document.
/// The bug this pins: every read failure (chmod 000, non-UTF-8, a directory) was
/// treated as "no file", merged from `{}`, and atomically published over the user's
/// real config — exit 0, reporting "merged".
#[test]
fn install_hooks_never_replaces_a_settings_file_it_cannot_read() {
    // 1. Unreadable (mode 000).
    let home = home_with_settings(|path| {
        std::fs::write(path, PRECIOUS_SETTINGS).unwrap();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o000)).unwrap();
    });
    let settings = default_settings(home.path());
    // (Skipped as root, where the read is permitted and the merge simply succeeds.)
    if std::fs::read(&settings).is_err() {
        let out = install_hooks(home.path(), &[]);
        assert!(
            !out.status.success(),
            "an unreadable settings.json must FAIL, not be silently replaced"
        );
        std::fs::set_permissions(&settings, std::fs::Permissions::from_mode(0o600)).unwrap();
        assert_eq!(
            std::fs::read_to_string(&settings).unwrap(),
            PRECIOUS_SETTINGS,
            "the user's settings must survive byte for byte"
        );
        // The snippet is STILL printed: hand-installation is now the only route.
        assert!(stdout(&out).contains("asyncRewake"));
    }
    std::fs::set_permissions(&settings, std::fs::Permissions::from_mode(0o600)).unwrap();

    // 2. Non-UTF-8 (a corrupt or latin-1 file), on a perfectly writable path.
    let home = home_with_settings(|path| std::fs::write(path, [b'{', 0xff, 0xfe, b'}']).unwrap());
    let settings = default_settings(home.path());
    let out = install_hooks(home.path(), &[]);
    assert!(!out.status.success(), "non-UTF-8 settings must FAIL");
    assert_eq!(
        std::fs::read(&settings).unwrap(),
        [b'{', 0xff, 0xfe, b'}'],
        "corrupt-and-kept beats silently-replaced"
    );

    // 3. Unparseable JSON.
    let home = home_with_settings(|path| std::fs::write(path, "{ not json").unwrap());
    let settings = default_settings(home.path());
    let out = install_hooks(home.path(), &[]);
    assert!(!out.status.success(), "unparseable settings must FAIL");
    assert_eq!(std::fs::read_to_string(&settings).unwrap(), "{ not json");

    // 4. A DIRECTORY where settings.json belongs.
    let home = home_with_settings(|path| std::fs::create_dir_all(path).unwrap());
    let settings = default_settings(home.path());
    let out = install_hooks(home.path(), &[]);
    assert!(!out.status.success(), "a directory must FAIL");
    assert!(settings.is_dir(), "left exactly as it was");
    assert!(
        stdout(&out).contains("asyncRewake"),
        "a failed merge must STILL print the snippet — hand-installing it is now the \
         user's only option, so the failure must not suppress the fallback"
    );
}

/// A symlinked `~/.claude/settings.json` (the dotfiles setup) is written THROUGH:
/// the tracked file receives the hooks, and the link survives. Replacing the link
/// with a regular file would leave the tracked file hookless — and the next
/// `stow -R` / `git checkout` would silently revert the hooks, killing wake.
#[test]
fn install_hooks_writes_through_a_symlinked_settings_file() {
    let home = TempDir::new().unwrap();
    let tracked = home.path().join("dotfiles").join("settings.json");
    std::fs::create_dir_all(tracked.parent().unwrap()).unwrap();
    std::fs::write(&tracked, PRECIOUS_SETTINGS).unwrap();

    let settings = default_settings(home.path());
    std::fs::create_dir_all(settings.parent().unwrap()).unwrap();
    std::os::unix::fs::symlink(&tracked, &settings).unwrap();

    let out = install_hooks(home.path(), &[]);

    assert_ok(&out, "install-hooks (symlinked settings)");
    assert!(
        std::fs::symlink_metadata(&settings).unwrap().is_symlink(),
        "the dotfiles symlink must survive"
    );
    let written = parse_json(&std::fs::read_to_string(&tracked).unwrap());
    assert_eq!(written["model"], "opus", "unrelated settings preserved");
    assert!(
        written["hooks"]["FileChanged"].is_array(),
        "the TRACKED file is what received the hooks"
    );
    assert!(
        stdout(&out).contains("via the symlink"),
        "the user must be told their hooks landed in the link's target: {}",
        stdout(&out)
    );
}

/// The default (unconfirmed, no-preview) merge keeps a `.bak` of what it replaced,
/// and does not narrow the file's 0644 mode to the temp file's 0600.
#[test]
fn install_hooks_backs_up_the_original_and_preserves_its_mode() {
    let home = home_with_settings(|path| {
        std::fs::write(path, PRECIOUS_SETTINGS).unwrap();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o644)).unwrap();
    });
    let settings = default_settings(home.path());

    let out = install_hooks(home.path(), &[]);

    assert_ok(&out, "install-hooks (backup)");
    let backup = settings.with_file_name("settings.json.bak");
    assert_eq!(
        std::fs::read_to_string(&backup).unwrap(),
        PRECIOUS_SETTINGS,
        "the pre-image is kept beside the file we edited"
    );
    assert!(stdout(&out).contains("previous settings are at"));
    assert_eq!(
        std::fs::metadata(&settings).unwrap().permissions().mode() & 0o777,
        0o644,
        "merging must not silently narrow the user's file mode"
    );
}

/// Re-running after moving the binary (the documented flow: once from `target/`,
/// again from `~/.local/bin`) must UPDATE our hook group, not append a second one
/// pointing at a binary that no longer exists.
#[test]
fn install_hooks_re_run_with_a_different_binary_updates_rather_than_appends() {
    let home = home_with_settings(|path| std::fs::write(path, "{}").unwrap());
    let settings = default_settings(home.path());

    assert_ok(
        &install_hooks(home.path(), &["--mailbox-bin", mailbox_bin()]),
        "first install",
    );
    let relocated = home.path().join("mailbox");
    std::fs::copy(mailbox_bin(), &relocated).unwrap();
    let out = install_hooks(home.path(), &["--mailbox-bin", relocated.to_str().unwrap()]);
    assert_ok(&out, "second install from a new path");

    let merged = parse_json(&std::fs::read_to_string(&settings).unwrap());
    // Exactly ONE of each of our hooks, pointing at the RELOCATED binary — the first
    // install's hooks (pointing at the old path) are replaced, not appended.
    let session_start = merged["hooks"]["SessionStart"].as_array().unwrap();
    assert_eq!(
        session_start.len(),
        1,
        "a re-run must leave exactly ONE session-start hook: {session_start:?}"
    );
    let command = session_start[0]["hooks"][0]["command"].as_str().unwrap();
    // install-hooks canonicalizes the bin path, so compare against the canonical form
    // (on macOS /var → /private/var).
    let canonical = std::fs::canonicalize(&relocated).unwrap();
    assert!(
        command.starts_with(canonical.to_str().unwrap())
            && command.contains("harness session-start"),
        "the surviving hook points at the relocated binary: {command}"
    );
    assert_eq!(merged["hooks"]["FileChanged"].as_array().unwrap().len(), 1);
    assert_eq!(merged["hooks"]["SessionEnd"].as_array().unwrap().len(), 1);
}

/// A RELATIVE `HOME` must not resolve the default settings path against the CWD —
/// which would merge into the *project's* committed `.claude/settings.json`.
#[test]
fn install_hooks_refuses_a_relative_home() {
    let cwd = TempDir::new().unwrap();
    // A project-style `.claude/settings.json` sitting in the CWD, which a relative
    // home would resolve onto.
    let project = cwd.path().join(".claude");
    std::fs::create_dir_all(&project).unwrap();
    std::fs::write(project.join("settings.json"), PRECIOUS_SETTINGS).unwrap();

    let out = mailbox_command()
        .args(["harness", "install-hooks"])
        .current_dir(cwd.path())
        .env("AGENT_MAILBOX_HOME", ".")
        .env_remove("HOME")
        .env("RUST_LOG", "error")
        .output()
        .expect("run install-hooks with a relative home");

    assert_ok(&out, "install-hooks (relative home)");
    assert!(
        stdout(&out).contains("no home to resolve Claude Code settings under"),
        "a relative home must be refused, not resolved: {}",
        stdout(&out)
    );
    assert_eq!(
        std::fs::read_to_string(project.join("settings.json")).unwrap(),
        PRECIOUS_SETTINGS,
        "the project's committed settings must NOT be touched"
    );
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
        mailbox_command()
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
