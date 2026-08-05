//! Integration tests for the ADR-0008 on-demand wake mechanism: the detached
//! watcher, the `SessionStart` hook (`session-start`), the `FileChanged` wake hook
//! (`wake`), and the `SessionEnd` sentinel teardown.
//!
//! Everything drives the REAL binaries the way Claude Code would — hook JSON fed on
//! stdin, the watcher spawned as its own detached process — against a `mailbox serve`
//! daemon in a tempdir. The sentinel root is ALWAYS a tempdir
//! (`MAILBOX_SENTINEL_ROOT`, set by [`common::Env`]) so a test can never touch a real
//! `~/.mailbox`. Every spawned process is reaped and leak-checked ([`LeakGuard`]).
//!
//! What is NOT covered headlessly (documented in ADR-0008, to smoke-test on a real
//! agent): the full `watchPaths` + `asyncRewake` + truly-idle chain — i.e. that a
//! sentinel bump actually WAKES an idle Claude Code session — and multi-session
//! isolation via `watchPaths`. Those need a live harness; here we prove every link
//! up to and including "the wake hook WOULD exit 2 with the topic".

mod common;

use std::io::Write;
use std::process::Stdio;
use std::time::Duration;

use common::{Env, mailbox_command, pid_alive, poll_until, wait_within};

/// Whether `session`'s watcher is alive per its pidfile (the same probe `cleanup`
/// and `mailbox agents` use).
fn watcher_alive(env: &Env, session: &str) -> bool {
    watcher_pid(env, session).map(pid_alive).unwrap_or(false)
}

/// The pid recorded in `session`'s watcher pidfile, or `None` if absent/garbage.
fn watcher_pid(env: &Env, session: &str) -> Option<u32> {
    std::fs::read_to_string(env.waiter_pidfile(session))
        .ok()?
        .trim()
        .parse::<u32>()
        .ok()
}

/// The mtime of `session`'s sentinel file (panics if it does not exist).
fn sentinel_mtime(env: &Env, session: &str) -> std::time::SystemTime {
    std::fs::metadata(env.sentinel_path(session))
        .unwrap()
        .modified()
        .unwrap()
}

/// Block until `session`'s watcher has armed (written its live pidfile), or panic.
fn wait_until_armed(env: &Env, session: &str) {
    poll_until(
        "watcher arms (writes its pidfile)",
        Duration::from_secs(5),
        || watcher_alive(env, session).then_some(()),
    );
}

// ==== SessionStart: prints watchPaths, registers the inbox, spawns the watcher ====

#[test]
fn session_start_prints_watchpaths_registers_inbox_and_spawns_the_watcher() {
    let env = Env::new();
    let mut guard = env.leak_guard();
    let daemon = env.start_daemon();
    guard.track_daemon(daemon.pid());
    let session = "ss-happy";

    let out = env.session_start(session);
    assert!(out.status.success(), "session-start must exit 0");

    // stdout is the watchPaths registration, naming this session's ABSOLUTE sentinel.
    let value: serde_json::Value =
        serde_json::from_str(String::from_utf8_lossy(&out.stdout).trim()).expect("watchPaths JSON");
    let hso = &value["hookSpecificOutput"];
    assert_eq!(hso["hookEventName"], "SessionStart");
    assert_eq!(
        hso["watchPaths"][0],
        env.sentinel_path(session).display().to_string(),
        "watchPaths must register this session's absolute sentinel path"
    );
    // EXACTLY one registration. The shared `by-agent` root was registered here too
    // for a while; because the FileChanged matcher is the shared sentinel basename,
    // that made every session's bump fire every other session's wake hook (a measured
    // 16:1 stray-to-genuine ratio). Registering anything wider than this session's own
    // absolute path re-breaks per-session isolation, so the count is asserted.
    assert_eq!(
        hso["watchPaths"].as_array().map(Vec::len),
        Some(1),
        "exactly one registration: this session's own sentinel, and nothing wider"
    );

    // The always-on inbox was registered (card 16 / ADR-0007).
    let subs = env.subscriptions(session);
    assert!(
        subs.iter().any(|t| t == &format!("agent.{session}")),
        "session-start must register the agent inbox; got {subs:?}"
    );

    // And it spawned a live detached watcher.
    wait_until_armed(&env, session);

    let out = env.cleanup(session);
    assert!(out.status.success());
    daemon.stop();
    guard.assert_clean();
}

/// Fail-open: with the bridge DOWN, session-start still prints the watchPaths (so the
/// session is wake-wired the moment a daemon exists) and still exits 0 — it never
/// depends on the bridge to arm.
#[test]
fn session_start_fails_open_when_the_bridge_is_down() {
    // No daemon started: every socket call fails.
    let env = Env::new();
    let session = "ss-failopen";

    let out = env.session_start(session);
    assert!(
        out.status.success(),
        "session-start must exit 0 even with the bridge down; stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let value: serde_json::Value =
        serde_json::from_str(String::from_utf8_lossy(&out.stdout).trim()).expect("watchPaths JSON");
    assert_eq!(
        value["hookSpecificOutput"]["watchPaths"][0],
        env.sentinel_path(session).display().to_string(),
        "watchPaths must be printed even when the bridge is down"
    );
    // With no store, the spawned watcher self-exits immediately, so nothing lingers.
}

// ==== the detached watcher bumps the sentinel on real mail (payload-free) =========

/// The end-to-end headless chain: a real publish → the watcher (blocked on the FIFO)
/// wakes, sees genuine unread, and writes the TOPIC NAME into the sentinel → the wake
/// hook then exits 2 with exactly that topic. Also asserts the sentinel is
/// payload-free (topic names only).
#[test]
fn the_watcher_bumps_the_sentinel_with_the_topic_and_the_wake_hook_would_exit_2() {
    let env = Env::new();
    let mut guard = env.leak_guard();
    let daemon = env.start_daemon();
    guard.track_daemon(daemon.pid());
    let session = "watch-e2e";
    let topic = env.pr_topic(1);

    env.run_as_ok(session, &["subscribe", &topic], "subscribe");
    let watcher = env.spawn_watcher(session);
    wait_until_armed(&env, session);

    // A real, anonymous publish (adapter-style) wakes every subscriber.
    env.publish(&topic);

    // The watcher bumps the sentinel with the topic name (payload-free).
    let topics = poll_until("watcher bumps the sentinel", Duration::from_secs(5), || {
        let t = env.sentinel_topics(session);
        (!t.is_empty()).then_some(t)
    });
    assert_eq!(
        topics,
        vec![topic.clone()],
        "sentinel carries the topic NAME"
    );
    let raw = std::fs::read_to_string(env.sentinel_path(session)).unwrap();
    assert!(
        !raw.contains('{'),
        "the sentinel must be payload-free: {raw:?}"
    );

    // The FileChanged wake hook, run against this state, WOULD exit 2 with the topic.
    let wake = env.wake_hook(session);
    assert_eq!(
        wake.status.code(),
        Some(2),
        "the wake hook must wake (exit 2) when there is genuine unread mail"
    );
    let reminder = String::from_utf8_lossy(&wake.stderr);
    assert!(
        reminder.contains(&format!("mail on topic {topic}")),
        "the reminder must name the topic; got {reminder}"
    );

    let out = env.cleanup(session);
    assert!(out.status.success());
    drop(watcher);
    daemon.stop();
    guard.assert_clean();
}

// ==== the wake hook is anti-loop: exit 2 on unread, exit 0 when caught up =========

/// The load-bearing anti-loop property: the FileChanged hook wakes ONLY on genuine
/// unread mail. Once the agent has read, a further sentinel change must NOT wake it —
/// else it loops forever (exactly the bug the earlier stub had).
#[test]
fn the_wake_hook_exits_2_on_unread_and_0_when_caught_up() {
    let env = Env::new();
    let mut guard = env.leak_guard();
    let daemon = env.start_daemon();
    guard.track_daemon(daemon.pid());
    let session = "wake-antiloop";
    let topic = env.pr_topic(2);

    env.run_as_ok(session, &["subscribe", &topic], "subscribe");
    env.publish(&topic);

    // Unread present → the wake hook exits 2 with the topic.
    let wake = env.wake_hook(session);
    assert_eq!(wake.status.code(), Some(2), "unread mail must wake");
    assert!(String::from_utf8_lossy(&wake.stderr).contains(&format!("mail on topic {topic}")));

    // The agent reads (consumes) the mail.
    env.run_as_ok(session, &["read"], "read");

    // Now a further FileChanged (e.g. the sentinel removed/recreated) must NOT wake:
    // there is nothing unread. This is the anti-loop guard, asserted explicitly.
    let wake = env.wake_hook(session);
    assert_eq!(
        wake.status.code(),
        Some(0),
        "a wake hook with nothing unread MUST exit 0 (anti-loop); stderr: {}",
        String::from_utf8_lossy(&wake.stderr)
    );

    daemon.stop();
    guard.assert_clean();
}

// ==== the wake hook's ack: proof the harness ran it at all (ADR-0016) ============

/// The ack record for `session` (the file `mailbox doctor` reads).
fn hook_ran_record(env: &Env, session: &str) -> Option<String> {
    std::fs::read_to_string(
        env.sentinel_root()
            .join("by-agent")
            .join(session)
            .join(".mailbox-hook-ran"),
    )
    .ok()
}

/// The ack must be stamped on EVERY run of the hook, including the no-op ones.
///
/// This is the whole basis of `mailbox doctor`: an agent that is simply caught up
/// looks, from the outside, exactly like an agent whose watch is dead. Only a record
/// written on the exit-0 path can tell them apart — so if this regressed to stamping
/// only on wakes, every healthy idle session would start reporting as deaf.
#[test]
fn the_wake_hook_records_that_it_ran_on_every_exit_path() {
    let env = Env::new();
    let mut guard = env.leak_guard();
    let daemon = env.start_daemon();
    guard.track_daemon(daemon.pid());
    let session = "wake-ack";
    let topic = env.pr_topic(3);

    env.run_as_ok(session, &["subscribe", &topic], "subscribe");

    // Nothing has run the hook yet, so there is no ack to find.
    assert_eq!(
        hook_ran_record(&env, session),
        None,
        "no ack may exist before the hook has ever run"
    );

    // The anti-loop path (nothing unread, exit 0) MUST still stamp the ack.
    let wake = env.wake_hook(session);
    assert_eq!(wake.status.code(), Some(0), "nothing unread yet");
    let after_noop = hook_ran_record(&env, session)
        .expect("the exit-0 path must still record that the hook ran");

    // And so must the wake path (unread, exit 2), with a DIFFERENT stamp — it is the
    // change that proves a fresh run rather than an old one.
    env.publish(&topic);
    let wake = env.wake_hook(session);
    assert_eq!(wake.status.code(), Some(2), "unread mail must wake");
    let after_wake = hook_ran_record(&env, session).expect("the exit-2 path must record too");
    assert_ne!(
        after_noop, after_wake,
        "each run must leave a distinguishable stamp, or a probe cannot tell a fresh \
         answer from a stale one"
    );

    daemon.stop();
    guard.assert_clean();
}

/// `SessionEnd` must take the ack away with the rest of the session's state, so a
/// stale stamp cannot make a departed session look like it answered.
#[test]
fn session_end_removes_the_hook_ran_ack_with_the_sentinel_dir() {
    let env = Env::new();
    let mut guard = env.leak_guard();
    let daemon = env.start_daemon();
    guard.track_daemon(daemon.pid());
    let session = "wake-ack-cleanup";

    env.run_as_ok(session, &["subscribe", &env.pr_topic(4)], "subscribe");
    env.wake_hook(session);
    assert!(hook_ran_record(&env, session).is_some(), "ack was written");

    env.cleanup(session);
    assert_eq!(
        hook_ran_record(&env, session),
        None,
        "SessionEnd must remove the ack along with the sentinel directory"
    );

    daemon.stop();
    guard.assert_clean();
}

// ==== the watcher is single-instance: racing spawns → one winner, no leak =========

#[test]
fn the_watcher_is_single_instance_racing_spawns_leave_one_winner() {
    let env = Env::new();
    let mut guard = env.leak_guard();
    let daemon = env.start_daemon();
    guard.track_daemon(daemon.pid());
    let session = "watch-single";
    env.run_as_ok(session, &["subscribe", &env.pr_topic(3)], "subscribe");

    // Winner arms and holds the per-session lock.
    let winner = env.spawn_watcher(session);
    wait_until_armed(&env, session);

    // A second watcher (a racing SessionStart) must LOSE the lock and exit cleanly (0),
    // without ever touching the winner's pidfile.
    let mut loser = env.spawn_watcher(session);
    let status = wait_within(&mut loser, Duration::from_secs(5))
        .expect("the lock-losing watcher must exit, not block");
    assert_eq!(
        status.code(),
        Some(0),
        "a single-instance lock loser exits 0 (benign), not as an error"
    );
    assert!(
        watcher_alive(&env, session),
        "the winner keeps running and keeps the lock"
    );

    let out = env.cleanup(session);
    assert!(out.status.success());
    drop(winner);
    daemon.stop();
    guard.assert_clean();
}

// ==== the watcher self-exits (no orphan) when the session subscribes to nothing ====

#[test]
fn the_watcher_self_exits_when_unsubscribed_leaving_no_orphan() {
    let env = Env::new();
    let mut guard = env.leak_guard();
    let daemon = env.start_daemon();
    guard.track_daemon(daemon.pid());
    let session = "watch-nosubs"; // deliberately never subscribed

    let mut watcher = env.spawn_watcher(session);
    let status = wait_within(&mut watcher, Duration::from_secs(5))
        .expect("a watcher with no subscription must self-exit, not block as an orphan");
    assert_eq!(
        status.code(),
        Some(0),
        "the unsubscribed self-exit is clean"
    );
    assert!(
        !watcher_alive(&env, session),
        "the self-exit must remove the pidfile (no phantom watcher)"
    );

    daemon.stop();
    guard.assert_clean();
}

// ==== the watcher blocks indefinitely — it has NO max-block and never times out ====

#[test]
fn the_watcher_blocks_and_does_not_time_out() {
    let env = Env::new();
    let mut guard = env.leak_guard();
    let daemon = env.start_daemon();
    guard.track_daemon(daemon.pid());
    let session = "watch-blocks";
    env.run_as_ok(session, &["subscribe", &env.pr_topic(4)], "subscribe");

    let mut watcher = env.spawn_watcher(session);
    wait_until_armed(&env, session);

    // With no mail and no re-arm timer, the watcher must STILL be blocking well past
    // any old max-block boundary — it never exits on its own (the whole ADR-0008 win).
    // A FIXED sleep is correct HERE — unlike the rest of the suite, which polls for a
    // state to appear, this test proves a NEGATIVE ("it did NOT exit"), and the only
    // way to observe a non-event is to wait a generous interval and confirm it still
    // has not happened. Do not kill it — that is what teardown is for.
    std::thread::sleep(Duration::from_millis(1500));
    assert!(
        watcher.try_wait().expect("try_wait").is_none(),
        "the watcher must keep blocking (no max-block, no re-arm exit)"
    );
    assert!(watcher_alive(&env, session), "still armed and blocked");

    let out = env.cleanup(session);
    assert!(out.status.success());
    drop(watcher);
    daemon.stop();
    guard.assert_clean();
}

// ==== SessionEnd reaps the watcher AND removes the sentinel dir ===================

#[test]
fn session_end_reaps_the_watcher_and_removes_the_sentinel_dir() {
    let env = Env::new();
    let mut guard = env.leak_guard();
    let daemon = env.start_daemon();
    guard.track_daemon(daemon.pid());
    let session = "watch-teardown";
    let topic = env.pr_topic(5);
    env.run_as_ok(session, &["subscribe", &topic], "subscribe");

    let mut watcher = env.spawn_watcher(session);
    wait_until_armed(&env, session);

    // Publish so the watcher creates the sentinel dir + file.
    env.publish(&topic);
    poll_until("sentinel created", Duration::from_secs(5), || {
        env.sentinel_path(session).exists().then_some(())
    });
    let sentinel_dir = env.sentinel_root().join("by-agent").join(session);
    assert!(
        sentinel_dir.exists(),
        "the sentinel dir exists before cleanup"
    );

    // SessionEnd reaps the watcher and removes the sentinel dir.
    let out = env.cleanup(session);
    assert!(out.status.success(), "cleanup must exit 0");

    // The watcher is reaped (its pidfile is gone), and the process actually dies.
    assert!(
        wait_within(&mut watcher, Duration::from_secs(5)).is_some(),
        "cleanup must terminate the watcher"
    );
    assert!(
        !watcher_alive(&env, session),
        "no phantom watcher after cleanup"
    );
    assert!(
        !sentinel_dir.exists(),
        "cleanup must remove the session's sentinel dir"
    );

    daemon.stop();
    guard.assert_clean();
}

// ==== FIX 3: the Stop-liveness hook respawns a dead watcher and NEVER wakes ========

/// The pessimistic Stop-liveness net (ADR-0008 FIX 3), the PRIMARY recovery for a
/// watcher that died. `ensure-watcher`, run at every turn boundary:
/// (i) respawns the watcher when none is alive; (ii) leaves a LIVE watcher strictly
/// alone (single-instance — a redundant spawn loses the lock and exits cleanly);
/// (iii) NEVER exits 2 (it is not asyncRewake, so a Stop can never itself wake).
#[test]
fn ensure_watcher_respawns_a_dead_watcher_leaves_a_live_one_and_never_wakes() {
    let env = Env::new();
    let mut guard = env.leak_guard();
    let daemon = env.start_daemon();
    guard.track_daemon(daemon.pid());
    let session = "stop-liveness";
    env.run_as_ok(session, &["subscribe", &env.pr_topic(6)], "subscribe");

    // (i) No watcher yet → ensure-watcher respawns one, exits 0, and prints NOTHING.
    // A Stop hook must not emit a `hookSpecificOutput` (Claude Code rejects a Stop hook
    // whose output carries `hookEventName: "SessionStart"` — the watchPaths registration
    // is SessionStart-only). Regression guard for that bug.
    let out = env.ensure_watcher(session);
    assert_eq!(
        out.status.code(),
        Some(0),
        "ensure-watcher must exit 0 (never a wake); stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.trim().is_empty(),
        "the Stop hook must print nothing to stdout (no watchPaths / hookEventName); got: {stdout}"
    );
    wait_until_armed(&env, session);
    let live_pid = watcher_pid(&env, session).expect("a watcher pid");

    // (ii) A live watcher is LEFT ALONE: a second ensure-watcher does not replace it.
    let out = env.ensure_watcher(session);
    assert_eq!(out.status.code(), Some(0), "ensure-watcher must exit 0");
    // Give any (wrongly) spawned replacement time to have taken over, then confirm the
    // ORIGINAL watcher still owns the pidfile and runs.
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(
        watcher_pid(&env, session),
        Some(live_pid),
        "a live watcher must be left untouched (single-instance)"
    );
    assert!(pid_alive(live_pid), "the original watcher is still running");

    // (iii) A DEAD watcher is respawned. SIGKILL leaves its pidfile stale (it cannot
    // clean up on SIGKILL), so ensure-watcher sees a dead pid and respawns.
    nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(live_pid as i32),
        nix::sys::signal::Signal::SIGKILL,
    )
    .ok();
    poll_until("the killed watcher is gone", Duration::from_secs(5), || {
        (!pid_alive(live_pid)).then_some(())
    });
    let out = env.ensure_watcher(session);
    assert_eq!(
        out.status.code(),
        Some(0),
        "ensure-watcher must exit 0 even when it respawns a dead watcher"
    );
    let new_pid = poll_until(
        "a fresh watcher replaces the dead one",
        Duration::from_secs(5),
        || watcher_pid(&env, session).filter(|&p| p != live_pid && pid_alive(p)),
    );
    assert!(pid_alive(new_pid), "the respawned watcher is live");

    let out = env.cleanup(session);
    assert!(out.status.success());
    daemon.stop();
    guard.assert_clean();
}

// ==== ADR-0013: ensure-watcher re-registers the inbox on every Stop ================

/// ADR-0013: the Stop-liveness hook re-registers the session's inbox, restoring the
/// ADR-0007 invariant (register on every SessionStart AND every Stop) that ADR-0008
/// dropped. Here NO `session-start` ran, so the inbox is unregistered; a single
/// `ensure-watcher` must make the session addressable.
///
/// This proves the *mechanism* (a Stop re-registers via `register_inbox`). The tombstone
/// **self-heal** it enables — a resume whose SessionStart registration was refused inside
/// the 10s guard, re-subscribing once the guard lapses — is proved at the writer layer,
/// instantly, by `subscribe_after_aged_tombstone_succeeds_and_clears_it` in
/// `storage/writer.rs` (which drives an aged tombstone via `now_ms`). The end-to-end
/// composition is those two facts; we deliberately do NOT re-prove it with a >10s
/// process-level wait (an inverted-pyramid test).
#[test]
fn ensure_watcher_registers_the_inbox() {
    let env = Env::new();
    let mut guard = env.leak_guard();
    let daemon = env.start_daemon();
    guard.track_daemon(daemon.pid());
    let session = "ew-registers-inbox";

    // Precondition: with no session-start, the session subscribes to nothing.
    assert!(
        env.subscriptions(session).is_empty(),
        "no inbox should exist before any hook registers it"
    );

    // A single Stop hook must register the inbox (and exit 0, never a wake).
    let out = env.ensure_watcher(session);
    assert_eq!(
        out.status.code(),
        Some(0),
        "ensure-watcher must exit 0; stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let subs = env.subscriptions(session);
    assert_eq!(
        subs,
        vec![format!("agent.{session}")],
        "ensure-watcher must register the agent inbox; got {subs:?}"
    );

    let out = env.cleanup(session);
    assert!(out.status.success());
    daemon.stop();
    guard.assert_clean();
}

/// ADR-0013 fail-open: `ensure-watcher` makes its first bridge socket call
/// (`register_inbox`) as of this change, so the "a Stop hook must NEVER fail or wake, even
/// with the bridge down" guarantee needs its own coverage — the mirror of
/// `session_start_fails_open_when_the_bridge_is_down`. With NO daemon, the socket call
/// fails; the hook must still exit 0 and leak nothing.
#[test]
fn ensure_watcher_fails_open_when_the_bridge_is_down() {
    // No daemon started: the register_inbox socket call cannot connect.
    let env = Env::new();
    let session = "ew-failopen";

    let out = env.ensure_watcher(session);
    assert_eq!(
        out.status.code(),
        Some(0),
        "ensure-watcher must exit 0 even with the bridge down; stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    // A Stop hook prints nothing on stdout (no watchPaths / hookEventName).
    assert!(
        String::from_utf8_lossy(&out.stdout).trim().is_empty(),
        "the Stop hook must print nothing to stdout"
    );
}

/// ADR-0013: `session-start` fires on every SessionStart source, so it runs again on a
/// resume. Running it twice (startup, then resume) must be idempotent — exactly ONE
/// inbox subscription and the SAME single watcher after each — never a duplicate
/// subscription or a leaked second watcher.
#[test]
fn session_start_is_idempotent_across_a_resume() {
    let env = Env::new();
    let mut guard = env.leak_guard();
    let daemon = env.start_daemon();
    guard.track_daemon(daemon.pid());
    let session = "ss-resume";

    // First SessionStart (source: startup).
    let out = env.session_start(session);
    assert!(out.status.success(), "session-start (startup) must exit 0");
    wait_until_armed(&env, session);
    assert_eq!(
        env.subscriptions(session),
        vec![format!("agent.{session}")],
        "startup registers exactly the inbox"
    );
    let first_pid = watcher_pid(&env, session).expect("a watcher pid after startup");

    // Second SessionStart (source: resume — a fresh process re-establishing itself).
    // The matcher is "" so this fires; it must be a clean idempotent no-op.
    let out = env.session_start(session);
    assert!(out.status.success(), "session-start (resume) must exit 0");
    wait_until_armed(&env, session);
    assert_eq!(
        env.subscriptions(session),
        vec![format!("agent.{session}")],
        "resume must not duplicate the inbox subscription"
    );
    // The single-instance lock means the resume's spawn loses and exits: the ORIGINAL
    // watcher still owns the pidfile. Assert it locally rather than leaning on the leak
    // guard, so "no leaked second watcher" is proved by this test.
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(
        watcher_pid(&env, session),
        Some(first_pid),
        "resume must not leak a second watcher — the original still owns the pidfile"
    );
    assert!(
        pid_alive(first_pid),
        "the original watcher is still running"
    );

    let out = env.cleanup(session);
    assert!(out.status.success());
    daemon.stop();
    guard.assert_clean();
}

// ==== every kick bumps the sentinel; the wake hook's anti-loop bounds wakes ========

/// The post-coalescing contract (ADR-0008, revised): the watcher writes the sentinel
/// UNCONDITIONALLY on every kick, so a second message on an ALREADY-unread topic DOES
/// re-bump — there is no coalescing to suppress it. What bounds the number of actual
/// model wakes is the wake hook's anti-loop (exit 2 while there is genuine unread, exit
/// 0 once the agent has caught up), NOT any suppression of sentinel writes. This is the
/// deliberate simplification: with every kick bumping, no message can be lost.
#[test]
fn every_kick_bumps_the_sentinel_and_the_wake_hook_bounds_wakes() {
    let env = Env::new();
    let mut guard = env.leak_guard();
    let daemon = env.start_daemon();
    guard.track_daemon(daemon.pid());
    let session = "every-kick-bumps";
    let t = env.pr_topic(7);
    env.run_as_ok(session, &["subscribe", &t], "subscribe");
    let watcher = env.spawn_watcher(session);
    wait_until_armed(&env, session);

    // First publish bumps the sentinel to [T].
    env.publish(&t);
    poll_until("sentinel lists T", Duration::from_secs(5), || {
        (env.sentinel_topics(session) == vec![t.clone()]).then_some(())
    });
    let after_first = sentinel_mtime(&env, session);

    // Past a coarse (1s) mtime resolution so the re-bump is observable even though the
    // sentinel CONTENT is identical ([T] again) — the mtime is the only signal.
    std::thread::sleep(Duration::from_millis(1100));

    // A SECOND publish on the SAME already-unread topic RE-bumps (every kick writes —
    // there is no coalescing). Both events remain durably unread.
    env.publish(&t);
    poll_until(
        "the sentinel re-bumps on the second same-topic message",
        Duration::from_secs(5),
        || (sentinel_mtime(&env, session) > after_first).then_some(()),
    );
    assert_eq!(
        env.sentinel_topics(session),
        vec![t.clone()],
        "the sentinel still lists T"
    );
    assert_eq!(env.unread_total(session), 2, "both T events are unread");

    // The wake hook exits 2 while there is genuine unread...
    let wake = env.wake_hook(session);
    assert_eq!(wake.status.code(), Some(2), "unread present → wake");

    // ...and 0 once the agent has caught up: the ANTI-LOOP is what bounds wakes, not
    // any coalescing of sentinel writes.
    env.run_as_ok(session, &["read"], "read");
    poll_until("caught up", Duration::from_secs(5), || {
        (env.unread_total(session) == 0).then_some(())
    });
    let wake = env.wake_hook(session);
    assert_eq!(
        wake.status.code(),
        Some(0),
        "caught up → no wake (anti-loop bounds wakes, not coalescing)"
    );

    let out = env.cleanup(session);
    assert!(out.status.success());
    drop(watcher);
    daemon.stop();
    guard.assert_clean();
}

/// Deafness regression (re-notify after a read): after the agent READS and catches up, a
/// NEW message on the SAME topic must still wake it. Under the old coalescing this was
/// the permanent-deafness trap (same unread set → suppressed forever); under the
/// unconditional-bump design it is fixed for free — every kick writes, so the re-bump
/// always fires.
#[test]
fn a_new_message_after_a_read_re_bumps_even_on_the_same_topic() {
    let env = Env::new();
    let mut guard = env.leak_guard();
    let daemon = env.start_daemon();
    guard.track_daemon(daemon.pid());
    let session = "re-notify";
    let a = env.pr_topic(9);
    env.run_as_ok(session, &["subscribe", &a], "subscribe");
    let watcher = env.spawn_watcher(session);
    wait_until_armed(&env, session);

    env.publish(&a);
    poll_until("sentinel lists A", Duration::from_secs(5), || {
        (!env.sentinel_topics(session).is_empty()).then_some(())
    });
    let after_first = sentinel_mtime(&env, session);

    // The agent READS and catches up.
    env.run_as_ok(session, &["read"], "read");
    poll_until("caught up", Duration::from_secs(5), || {
        (env.unread_total(session) == 0).then_some(())
    });

    // Wait past a coarse (1s) mtime resolution so a re-bump is observable even though
    // the sentinel CONTENT is identical ({A} again) — the mtime is the only signal.
    std::thread::sleep(Duration::from_millis(1100));

    // A NEW message on the SAME topic A must RE-bump (progress advanced on the read).
    env.publish(&a);
    poll_until(
        "the sentinel re-bumps after the read",
        Duration::from_secs(5),
        || (sentinel_mtime(&env, session) > after_first).then_some(()),
    );
    // And the wake hook wakes on this genuinely-new unread.
    let wake = env.wake_hook(session);
    assert_eq!(
        wake.status.code(),
        Some(2),
        "post-read mail on the same topic must still wake (no permanent deafness)"
    );

    let out = env.cleanup(session);
    assert!(out.status.success());
    drop(watcher);
    daemon.stop();
    guard.assert_clean();
}

// ==== silent-deafness: the wake-coalescing must not consume a read-progress edge ====

/// Deliver ONE faithful production wake byte to `session`'s watcher — the exact kick a
/// publisher sends, via [`mailbox::wake::Waker::kick`]. Asserts it reached the blocked
/// watcher (a listening reader), so the test drives the REAL kick path, not a stub.
fn deliver_kick(env: &Env, session: &str) {
    let waker = mailbox::wake::Waker::new(env.waiters_dir());
    let outcome = waker.kick(&mailbox::storage::SessionId::new(session));
    assert_eq!(
        outcome,
        mailbox::wake::KickOutcome::Delivered,
        "a faithful kick must reach the blocked watcher"
    );
}

/// The reviewer's silent-deafness repro (empty-kick-between-read-and-message). An EMPTY
/// kick (a message read before the watcher drained its kick) that finds nothing unread
/// writes the empty set; the NEXT genuine message on the same, already-listed topic must
/// still re-bump and wake. Under the old coalescing an empty kick could consume the
/// read-progress edge and coalesce msg2 away → DEAF; under the unconditional-bump design
/// this is structural: every kick writes, so msg2 always re-bumps.
#[test]
fn an_empty_kick_between_a_read_and_a_new_message_does_not_deafen_the_agent() {
    let env = Env::new();
    let mut guard = env.leak_guard();
    let daemon = env.start_daemon();
    guard.track_daemon(daemon.pid());
    let session = "empty-kick-interleave";
    let t = env.pr_topic(20);
    env.run_as_ok(session, &["subscribe", &t], "subscribe");
    let watcher = env.spawn_watcher(session);
    wait_until_armed(&env, session);

    // msg1 → the watcher bumps the sentinel to [T].
    env.publish(&t);
    poll_until(
        "sentinel lists T after msg1",
        Duration::from_secs(5),
        || (env.sentinel_topics(session) == vec![t.clone()]).then_some(()),
    );

    // The agent reads T (delivery progress advances; unread goes empty).
    env.run_as_ok(session, &["read"], "read");
    poll_until("caught up", Duration::from_secs(5), || {
        (env.unread_total(session) == 0).then_some(())
    });

    // A faithful EMPTY kick: the store now shows nothing unread. It must NOT consume the
    // read edge; it clears the stale [T] set to empty. Polling until that clear lands ALSO
    // proves the empty kick was fully processed before msg2 (deterministic ordering).
    deliver_kick(&env, session);
    poll_until(
        "the empty kick clears the sentinel to empty",
        Duration::from_secs(5),
        || env.sentinel_topics(session).is_empty().then_some(()),
    );
    let after_clear = sentinel_mtime(&env, session);

    // Past a coarse (1s) mtime resolution so a re-bump is observable.
    std::thread::sleep(Duration::from_millis(1100));

    // msg2 on the SAME topic T → the watcher MUST re-bump (not deaf).
    env.publish(&t);
    let topics = poll_until(
        "the sentinel re-bumps for msg2 on the same topic",
        Duration::from_secs(5),
        || {
            let seen = env.sentinel_topics(session);
            seen.contains(&t).then_some(seen)
        },
    );
    assert_eq!(
        topics,
        vec![t.clone()],
        "the sentinel lists T again (not deaf)"
    );
    assert!(
        sentinel_mtime(&env, session) > after_clear,
        "the sentinel mtime advanced for msg2 — the agent is NOT deaf"
    );
    let wake = env.wake_hook(session);
    assert_eq!(
        wake.status.code(),
        Some(2),
        "msg2 on the same topic after an empty kick must still wake the agent"
    );

    let out = env.cleanup(session);
    assert!(out.status.success());
    drop(watcher);
    daemon.stop();
    guard.assert_clean();
}

/// Deafness regression (restart-after-read). A watcher that dies after the agent read
/// leaves a STALE non-empty set in the sentinel. When the Stop-liveness hook respawns it,
/// arm writes the real unread set (empty) unconditionally, and the first post-restart
/// message on the same topic re-bumps and wakes. Under the old coalescing the identical
/// set could be suppressed forever (deaf); the unconditional-bump design fixes it for
/// free.
#[test]
fn a_watcher_restart_after_a_read_re_bumps_on_the_same_topic() {
    let env = Env::new();
    let mut guard = env.leak_guard();
    let daemon = env.start_daemon();
    guard.track_daemon(daemon.pid());
    let session = "restart-variant";
    let t = env.pr_topic(21);
    env.run_as_ok(session, &["subscribe", &t], "subscribe");
    let watcher = env.spawn_watcher(session);
    wait_until_armed(&env, session);

    // msg1 → bump [T].
    env.publish(&t);
    poll_until("sentinel lists T", Duration::from_secs(5), || {
        (env.sentinel_topics(session) == vec![t.clone()]).then_some(())
    });

    // The agent reads. The sentinel is left STALE at [T] (only a kick clears it, and none
    // arrives here) — exactly the state a restart must reconcile.
    env.run_as_ok(session, &["read"], "read");
    poll_until("caught up", Duration::from_secs(5), || {
        (env.unread_total(session) == 0).then_some(())
    });
    assert_eq!(
        env.sentinel_topics(session),
        vec![t.clone()],
        "the sentinel is stale at [T] after the read (no kick cleared it)"
    );

    // Kill the watcher uncleanly. `ArmChild::drop` SIGKILLs then reaps it, so it dies
    // without cleaning up — leaving the STALE [T] sentinel AND a stale pidfile, exactly
    // the post-crash state the Stop-liveness respawn must reconcile.
    let live_pid = watcher_pid(&env, session).expect("watcher pid");
    drop(watcher);
    poll_until("the killed watcher is gone", Duration::from_secs(5), || {
        (!pid_alive(live_pid)).then_some(())
    });

    // Respawn via the Stop-liveness path. Arm-sync must reconcile the stale [T] to empty.
    let out = env.ensure_watcher(session);
    assert_eq!(out.status.code(), Some(0), "ensure-watcher must exit 0");
    let new_pid = poll_until("a fresh watcher arms", Duration::from_secs(5), || {
        watcher_pid(&env, session).filter(|&p| p != live_pid && pid_alive(p))
    });
    poll_until(
        "arm-sync clears the stale [T] set to empty",
        Duration::from_secs(5),
        || env.sentinel_topics(session).is_empty().then_some(()),
    );
    let after_arm = sentinel_mtime(&env, session);
    std::thread::sleep(Duration::from_millis(1100));

    // msg2 on the SAME topic T after the restart → must re-bump (not deaf).
    env.publish(&t);
    let topics = poll_until(
        "the restarted watcher re-bumps for msg2",
        Duration::from_secs(5),
        || {
            let seen = env.sentinel_topics(session);
            seen.contains(&t).then_some(seen)
        },
    );
    assert_eq!(
        topics,
        vec![t.clone()],
        "the sentinel lists T again after the restart (not deaf)"
    );
    assert!(
        sentinel_mtime(&env, session) > after_arm,
        "the sentinel mtime advanced for msg2 after the restart"
    );
    let wake = env.wake_hook(session);
    assert_eq!(
        wake.status.code(),
        Some(2),
        "post-restart mail on the same topic must still wake"
    );
    assert!(pid_alive(new_pid), "the respawned watcher is still live");

    let out = env.cleanup(session);
    assert!(out.status.success());
    daemon.stop();
    guard.assert_clean();
}

/// Deafness regression (the dead-window case that motivated removing coalescing): the
/// watcher dies AFTER the agent read, then a new message on the ALREADY-read topic arrives
/// WHILE the watcher is dead, then the Stop-liveness hook respawns it. On respawn, arm
/// writes the current unread set {T} UNCONDITIONALLY → the sentinel bumps → the wake hook
/// would fire. Under the old coalescing the respawn saw the same {T} set the stale sentinel
/// held and suppressed the bump → the agent stayed DEAF. This must PASS now.
#[test]
fn a_message_arriving_while_the_watcher_is_dead_wakes_on_respawn() {
    let env = Env::new();
    let mut guard = env.leak_guard();
    let daemon = env.start_daemon();
    guard.track_daemon(daemon.pid());
    let session = "dead-window";
    let t = env.pr_topic(22);
    env.run_as_ok(session, &["subscribe", &t], "subscribe");
    let watcher = env.spawn_watcher(session);
    wait_until_armed(&env, session);

    // msg1 → bump [T]; the agent reads and catches up.
    env.publish(&t);
    poll_until("sentinel lists T", Duration::from_secs(5), || {
        (env.sentinel_topics(session) == vec![t.clone()]).then_some(())
    });
    env.run_as_ok(session, &["read"], "read");
    poll_until("caught up", Duration::from_secs(5), || {
        (env.unread_total(session) == 0).then_some(())
    });

    // Kill the watcher uncleanly (SIGKILL via ArmChild::drop), leaving the STALE [T]
    // sentinel and a stale pidfile — the exact post-crash state the respawn reconciles.
    let live_pid = watcher_pid(&env, session).expect("watcher pid");
    drop(watcher);
    poll_until("the killed watcher is gone", Duration::from_secs(5), || {
        (!pid_alive(live_pid)).then_some(())
    });

    // msg2 on the SAME topic arrives WHILE the watcher is dead — no live watcher to kick,
    // so nothing touches the (still stale [T]) sentinel yet.
    env.publish(&t);
    poll_until("msg2 is durably unread", Duration::from_secs(5), || {
        (env.unread_total(session) == 1).then_some(())
    });

    // The Stop-liveness hook respawns the watcher; arm writes the current unread {T}
    // unconditionally → the wake hook would fire (not deaf).
    let out = env.ensure_watcher(session);
    assert_eq!(out.status.code(), Some(0), "ensure-watcher must exit 0");
    poll_until("a fresh watcher arms", Duration::from_secs(5), || {
        watcher_pid(&env, session).filter(|&p| p != live_pid && pid_alive(p))
    });
    let topics = poll_until(
        "the respawned watcher bumps the sentinel for the dead-window message",
        Duration::from_secs(5),
        || {
            let seen = env.sentinel_topics(session);
            seen.contains(&t).then_some(seen)
        },
    );
    assert_eq!(topics, vec![t.clone()], "the sentinel lists T (not deaf)");
    let wake = env.wake_hook(session);
    assert_eq!(
        wake.status.code(),
        Some(2),
        "a message that arrived while the watcher was dead must wake on respawn"
    );

    let out = env.cleanup(session);
    assert!(out.status.success());
    daemon.stop();
    guard.assert_clean();
}

/// A kick with nothing unread writes the EMPTY set into the sentinel (every kick writes
/// unconditionally — there is no coalescing). The result is a benign `FileChanged` the
/// wake hook answers with exit 0.
#[test]
fn an_empty_kick_clears_the_sentinel_after_a_read() {
    let env = Env::new();
    let mut guard = env.leak_guard();
    let daemon = env.start_daemon();
    guard.track_daemon(daemon.pid());
    let session = "empty-kick-clears";
    let t = env.pr_topic(23);
    env.run_as_ok(session, &["subscribe", &t], "subscribe");
    let watcher = env.spawn_watcher(session);
    wait_until_armed(&env, session);

    env.publish(&t);
    poll_until("sentinel lists T", Duration::from_secs(5), || {
        (env.sentinel_topics(session) == vec![t.clone()]).then_some(())
    });

    env.run_as_ok(session, &["read"], "read");
    poll_until("caught up", Duration::from_secs(5), || {
        (env.unread_total(session) == 0).then_some(())
    });

    // A faithful kick with nothing unread clears the stale [T] to empty.
    deliver_kick(&env, session);
    poll_until(
        "the empty kick clears the sentinel to empty",
        Duration::from_secs(5),
        || env.sentinel_topics(session).is_empty().then_some(()),
    );
    assert!(
        env.sentinel_topics(session).is_empty(),
        "an empty kick must clear the sentinel to empty"
    );

    let out = env.cleanup(session);
    assert!(out.status.success());
    drop(watcher);
    daemon.stop();
    guard.assert_clean();
}

// ==== FIX 5: cross-session isolation — B's sentinel change must not wake A =========

/// When a session's cwd is an ancestor of `~/.mailbox` (e.g. `claude` launched from
/// `$HOME`, whose cwd is watched recursively), a change to session B's sentinel fires
/// session A's `FileChanged` too. There must be NO false wake: A's wake hook re-checks
/// A's OWN unread from the store, finds none, and exits 0. Isolation comes from that
/// store re-check, NOT from the sentinel path.
#[test]
fn a_change_to_session_bs_sentinel_does_not_wake_session_a() {
    let env = Env::new();
    let mut guard = env.leak_guard();
    let daemon = env.start_daemon();
    guard.track_daemon(daemon.pid());
    let a = "iso-a";
    let b = "iso-b";
    let topic_b = env.pr_topic(10);
    // A is subscribed but has NO unread; B has genuine unread mail.
    env.run_as_ok(a, &["subscribe", &env.pr_topic(11)], "subscribe a");
    env.run_as_ok(b, &["subscribe", &topic_b], "subscribe b");
    env.publish(&topic_b);

    // A's wake hook (as if fired by B's sentinel change) must exit 0 — A has no unread.
    let wake_a = env.wake_hook(a);
    assert_eq!(
        wake_a.status.code(),
        Some(0),
        "a change to B's sentinel must NOT wake A (A has no unread → exit 0); stderr: {}",
        String::from_utf8_lossy(&wake_a.stderr)
    );
    // Control: B's own wake hook DOES wake, so we know the mail path is live.
    let wake_b = env.wake_hook(b);
    assert_eq!(
        wake_b.status.code(),
        Some(2),
        "B has genuine unread and must wake"
    );

    daemon.stop();
    guard.assert_clean();
}

// ==== FIX 6: a wake hook with no store, and the explicit outlive-spawner property ==

/// Before any daemon has ever run there is no store. The `FileChanged` wake hook must
/// exit 0 (anti-loop on a missing store), never wake over a phantom sentinel.
#[test]
fn the_wake_hook_with_no_store_exits_0() {
    // No daemon started → no db file exists at the env's DB path.
    let env = Env::new();
    let wake = env.wake_hook("no-store");
    assert_eq!(
        wake.status.code(),
        Some(0),
        "a wake hook with no store must exit 0 (no wake); stderr: {}",
        String::from_utf8_lossy(&wake.stderr)
    );
}

/// The `setsid` survival property, pinned EXPLICITLY: capture the `session-start`
/// spawner's pid, confirm IT has exited, and only THEN assert the detached watcher it
/// spawned is still alive — proving the watcher outlives its spawner rather than
/// merely relying on `wait_with_output` timing.
#[test]
fn the_detached_watcher_outlives_the_session_start_spawner() {
    let env = Env::new();
    let mut guard = env.leak_guard();
    let daemon = env.start_daemon();
    guard.track_daemon(daemon.pid());
    let session = "outlive";
    env.run_as_ok(session, &["subscribe", &env.pr_topic(12)], "subscribe");

    // Spawn session-start ourselves so we hold its pid.
    let mut spawner = mailbox_command()
        .args(["harness", "session-start"])
        .env("AGENT_MAILBOX_DB", env.db_path())
        .env("MAILBOX_SENTINEL_ROOT", env.sentinel_root())
        .env("RUST_LOG", "error")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn session-start");
    let spawner_pid = spawner.id();
    spawner
        .stdin
        .take()
        .unwrap()
        .write_all(
            format!(r#"{{"session_id":"{session}","hook_event_name":"SessionStart"}}"#).as_bytes(),
        )
        .unwrap();
    let status = spawner.wait().expect("session-start exits");
    assert!(status.success(), "session-start must exit 0");

    // The spawner has exited...
    poll_until(
        "the session-start spawner exits",
        Duration::from_secs(5),
        || (!pid_alive(spawner_pid)).then_some(()),
    );
    // ...yet the detached watcher it spawned is armed and ALIVE (setsid survival).
    wait_until_armed(&env, session);
    let watcher_pid = watcher_pid(&env, session).expect("a watcher pid");
    assert!(
        pid_alive(watcher_pid),
        "the detached watcher must outlive the (now-exited) session-start spawner"
    );

    let out = env.cleanup(session);
    assert!(out.status.success());
    daemon.stop();
    guard.assert_clean();
}

// ==== FIX 2: the watcher survives a signal (EINTR) and still bumps on a later kick ==

/// A signal delivered while the watcher is blocked in `poll` interrupts it with EINTR.
/// That is NOT an error — the watcher must retry the wait and keep running, then still
/// bump on a subsequent real kick. (A broken EINTR path would kill the watcher on the
/// signal, and the later publish would never bump.)
#[test]
fn the_watcher_survives_a_signal_interruption_and_still_bumps() {
    let env = Env::new();
    let mut guard = env.leak_guard();
    let daemon = env.start_daemon();
    guard.track_daemon(daemon.pid());
    let session = "eintr";
    let topic = env.pr_topic(13);
    env.run_as_ok(session, &["subscribe", &topic], "subscribe");
    let watcher = env.spawn_watcher(session);
    wait_until_armed(&env, session);
    let pid = watcher_pid(&env, session).expect("a watcher pid");

    // Interrupt the blocked poll a few times with a benign signal (SIGCONT: it does not
    // terminate and is not the reaping SIGTERM). The watcher must survive every one.
    for _ in 0..3 {
        nix::sys::signal::kill(
            nix::unistd::Pid::from_raw(pid as i32),
            nix::sys::signal::Signal::SIGCONT,
        )
        .ok();
        std::thread::sleep(Duration::from_millis(50));
    }
    assert!(
        watcher_alive(&env, session),
        "the watcher must survive a signal interruption (EINTR is retried, not fatal)"
    );

    // ...and it still does its job: a real publish bumps the sentinel.
    env.publish(&topic);
    let topics = poll_until(
        "the watcher bumps after the signal",
        Duration::from_secs(5),
        || {
            let t = env.sentinel_topics(session);
            (!t.is_empty()).then_some(t)
        },
    );
    assert_eq!(
        topics,
        vec![topic],
        "the watcher still bumps on a real kick"
    );

    let out = env.cleanup(session);
    assert!(out.status.success());
    drop(watcher);
    daemon.stop();
    guard.assert_clean();
}

// ==== ADR-0012: the turn-boundary re-trigger rescues mail that arrived while BUSY ====

/// The re-trigger record's contents, or `None` if the Stop hook has never re-triggered
/// this session (the fail-safe state).
fn retrigger_record(env: &Env, session: &str) -> Option<String> {
    let path = env
        .sentinel_path(session)
        .parent()
        .unwrap()
        .join(".mailbox-retriggered");
    std::fs::read_to_string(path).ok()
}

/// Deafness regression (the BUSY-window case, ADR-0012 — observed in the wild on a
/// watched PR): the ADR-0008 wake is a pure EDGE, and the `FileChanged` → exit-2 wake
/// only reaches an IDLE session. Mail published while the agent is mid-turn bumps the
/// sentinel to no effect — the edge is spent against a busy session — and under the old
/// design NOTHING ever bumped it again, so the agent went idle deaf on top of unread
/// mail and stayed that way until the next publish.
///
/// The fix is level-triggered arming at the one signal that says a turn ENDED: `Stop`
/// re-bumps the sentinel iff the session is sitting on mail newer than any it has
/// already been re-triggered for. This test drives exactly that sequence, modelling
/// "the wake was spent while busy" as "the wake hook never ran for that bump".
///
/// The watcher is kept ALIVE throughout, so `ensure-watcher` cannot respawn it — which
/// means the only thing that can possibly bump the sentinel here is the re-trigger.
#[test]
fn mail_that_arrived_while_busy_is_re_triggered_at_the_turn_boundary() {
    let env = Env::new();
    let mut guard = env.leak_guard();
    let daemon = env.start_daemon();
    guard.track_daemon(daemon.pid());
    let session = "busy-window";
    let t = env.pr_topic(30);
    env.run_as_ok(session, &["subscribe", &t], "subscribe");
    let watcher = env.spawn_watcher(session);
    wait_until_armed(&env, session);
    let live_pid = watcher_pid(&env, session).expect("a watcher pid");

    // Mail arrives while the agent is BUSY mid-turn: the watcher kicks and bumps the
    // sentinel, but the FileChanged wake that bump triggers reaches a busy session and
    // is spent (modelled by never running the wake hook for it).
    env.publish(&t);
    poll_until(
        "the watcher bumps for the busy-window mail",
        Duration::from_secs(5),
        || (env.sentinel_topics(session) == vec![t.clone()]).then_some(()),
    );
    let after_publish = sentinel_mtime(&env, session);
    assert_eq!(
        retrigger_record(&env, session),
        None,
        "nothing has been re-triggered yet"
    );

    // Past a coarse (1s) mtime resolution, so the re-bump is observable even though the
    // sentinel CONTENT is identical ([T] again) — the mtime is the only signal.
    std::thread::sleep(Duration::from_millis(1100));

    // The turn ends. Stop must notice the unread mail and re-bump, so a FileChanged
    // fires against the now-idle session.
    let out = env.ensure_watcher(session);
    assert_eq!(
        out.status.code(),
        Some(0),
        "the Stop hook must still never wake the session itself; stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        String::from_utf8_lossy(&out.stdout).trim().is_empty(),
        "the Stop hook must still print nothing to stdout"
    );
    assert_eq!(
        watcher_pid(&env, session),
        Some(live_pid),
        "the live watcher was left alone, so ONLY the re-trigger can have bumped"
    );
    assert!(
        sentinel_mtime(&env, session) > after_publish,
        "the turn boundary must re-bump the sentinel for mail that arrived while busy"
    );
    assert_eq!(
        env.sentinel_topics(session),
        vec![t.clone()],
        "the re-bump names the unread topic (payload-free)"
    );
    let after_stop = sentinel_mtime(&env, session);

    // And that re-bump is a REAL wake: the hook the FileChanged fires exits 2.
    let wake = env.wake_hook(session);
    assert_eq!(
        wake.status.code(),
        Some(2),
        "the re-triggered FileChanged must wake the now-idle session"
    );

    // ANTI-LOOP: a second turn boundary over the SAME mail must NOT re-bump. Without
    // this an agent that wakes and does not read would be nudged every turn, forever.
    std::thread::sleep(Duration::from_millis(1100));
    let out = env.ensure_watcher(session);
    assert_eq!(out.status.code(), Some(0), "ensure-watcher must exit 0");
    assert_eq!(
        sentinel_mtime(&env, session),
        after_stop,
        "already-re-triggered mail must not be re-bumped again (anti-loop)"
    );

    // But the bound is per-message, not permanent: after the agent catches up, the NEXT
    // message to arrive while busy is re-triggered on its own turn boundary.
    env.run_as_ok(session, &["read"], "read");
    poll_until("caught up", Duration::from_secs(5), || {
        (env.unread_total(session) == 0).then_some(())
    });
    env.publish(&t);
    poll_until(
        "the watcher bumps for the second busy-window message",
        Duration::from_secs(5),
        || (env.unread_total(session) == 1).then_some(()),
    );
    let after_second = sentinel_mtime(&env, session);
    std::thread::sleep(Duration::from_millis(1100));
    let out = env.ensure_watcher(session);
    assert_eq!(out.status.code(), Some(0), "ensure-watcher must exit 0");
    assert!(
        sentinel_mtime(&env, session) > after_second,
        "newer mail must be re-triggered again — the watermark bounds repeats, not new messages"
    );

    let out = env.cleanup(session);
    assert!(out.status.success());
    assert_eq!(
        retrigger_record(&env, session),
        None,
        "SessionEnd removes the sentinel directory, taking the re-trigger record with it"
    );
    drop(watcher);
    daemon.stop();
    guard.assert_clean();
}

/// The re-trigger is a SAFETY NET on a hook that runs at every single turn boundary,
/// so it must degrade quietly: with no store at all (hooks installed, `mailbox serve`
/// never started) the Stop hook must still exit 0 promptly, print nothing, and create
/// no sentinel. A safety net that fails loudly — or wakes — would break every turn on
/// every session.
#[test]
fn the_turn_boundary_re_trigger_is_a_quiet_no_op_with_no_store() {
    // No daemon and no database file: nothing for the re-trigger to read.
    let env = Env::new();
    let session = "no-store-turn";

    let out = env.ensure_watcher(session);
    assert_eq!(
        out.status.code(),
        Some(0),
        "the Stop hook must exit 0 with no store; stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        String::from_utf8_lossy(&out.stdout).trim().is_empty(),
        "the Stop hook must print nothing to stdout"
    );
    assert!(
        !env.sentinel_path(session).exists(),
        "no store means no sentinel was invented"
    );
    assert_eq!(retrigger_record(&env, session), None);
}

/// The re-trigger must not fire for a session that is CAUGHT UP: an ordinary turn
/// boundary on an agent with no mail must leave the sentinel completely untouched, or
/// every Stop on every working agent would spawn a `FileChanged` for nothing.
#[test]
fn a_caught_up_session_is_never_re_triggered_at_the_turn_boundary() {
    let env = Env::new();
    let mut guard = env.leak_guard();
    let daemon = env.start_daemon();
    guard.track_daemon(daemon.pid());
    let session = "quiet-turn";
    let t = env.pr_topic(31);
    env.run_as_ok(session, &["subscribe", &t], "subscribe");
    let watcher = env.spawn_watcher(session);
    wait_until_armed(&env, session);
    let armed_mtime = sentinel_mtime(&env, session);

    std::thread::sleep(Duration::from_millis(1100));
    let out = env.ensure_watcher(session);
    assert_eq!(out.status.code(), Some(0), "ensure-watcher must exit 0");
    assert_eq!(
        sentinel_mtime(&env, session),
        armed_mtime,
        "a turn boundary with nothing unread must not touch the sentinel"
    );
    assert_eq!(
        retrigger_record(&env, session),
        None,
        "and must record nothing"
    );

    let out = env.cleanup(session);
    assert!(out.status.success());
    drop(watcher);
    daemon.stop();
    guard.assert_clean();
}
