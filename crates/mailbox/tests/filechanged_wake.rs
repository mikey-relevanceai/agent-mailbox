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

use std::time::Duration;

use common::{Env, pid_alive, poll_until, wait_within};

/// Whether `session`'s watcher is alive per its pidfile (the same probe `cleanup`
/// and `mailbox agents` use).
fn watcher_alive(env: &Env, session: &str) -> bool {
    std::fs::read_to_string(env.waiter_pidfile(session))
        .ok()
        .and_then(|s| s.trim().parse::<u32>().ok())
        .map(pid_alive)
        .unwrap_or(false)
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
    // Sleep past a generous boundary and confirm it has NOT exited (do not kill it —
    // that is what teardown is for).
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
