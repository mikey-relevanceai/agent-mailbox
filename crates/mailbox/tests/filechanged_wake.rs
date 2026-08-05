//! Integration tests for the on-demand wake mechanism: the `SessionStart` hook
//! (`session-start`) arming the sentinel, the `serve` daemon bumping it on publish,
//! the `FileChanged` wake hook (`wake`), the `Stop` hook (`turn-end`), and the
//! `SessionEnd` teardown.
//!
//! Everything drives the REAL binaries the way Claude Code would — hook JSON fed on
//! stdin, against a `mailbox serve` daemon in a tempdir. The sentinel root is ALWAYS
//! a tempdir (`MAILBOX_SENTINEL_ROOT`, set by [`common::Env`] for the daemon and for
//! every hook) so a test can never touch a real `~/.mailbox`. Every spawned process
//! is reaped and leak-checked ([`common::LeakGuard`]).
//!
//! # Why almost nothing here polls any more
//!
//! The daemon writes a subscriber's sentinel INSIDE the publish request, before it
//! answers the client. So when `mailbox publish` exits, the wake has already landed
//! and the assertion can be made directly. Under the deleted watcher design the same
//! assertions had to poll, because the write happened in a third process on its own
//! schedule — which is exactly the class of timing the wake path no longer has.
//!
//! What is NOT covered headlessly (documented in ADR-0008, to smoke-test on a real
//! agent): the full `watchPaths` + `asyncRewake` + truly-idle chain — i.e. that a
//! sentinel bump actually WAKES an idle Claude Code session — and multi-session
//! isolation via `watchPaths`. Those need a live harness; here we prove every link
//! up to and including "the wake hook WOULD exit 2 with the topic".

mod common;

use std::time::Duration;

use common::Env;

/// The mtime of `session`'s sentinel file (panics if it does not exist).
fn sentinel_mtime(env: &Env, session: &str) -> std::time::SystemTime {
    std::fs::metadata(env.sentinel_path(session))
        .unwrap()
        .modified()
        .unwrap()
}

/// A coarse mtime resolution (1s on some filesystems) means a re-bump is only
/// observable if enough time has passed. Tests that assert "the mtime ADVANCED"
/// sleep this first; tests that assert content do not need it.
const PAST_MTIME_RESOLUTION: Duration = Duration::from_millis(1100);

// ==== SessionStart: arms the sentinel, prints watchPaths, registers the inbox ====

#[test]
fn session_start_arms_the_sentinel_prints_watchpaths_and_registers_the_inbox() {
    let env = Env::new();
    let mut guard = env.leak_guard();
    let daemon = env.start_daemon();
    guard.track_daemon(daemon.pid());
    let session = "ss-happy";

    let out = env.session_start(session);
    assert!(out.status.success(), "session-start must exit 0");

    // The sentinel EXISTS before the watchPaths registration is printed. That order
    // is load-bearing: Claude Code registers a watch on the path, and the daemon's
    // later writes are MODIFY events. Were the file absent, the daemon's first write
    // would be a CREATE — a different event the watch may not deliver at all.
    assert!(
        env.sentinel_path(session).exists(),
        "session-start must ARM the sentinel (create the file), not just name it"
    );

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

    let out = env.cleanup(session);
    assert!(out.status.success());
    daemon.stop();
    guard.assert_clean();
}

/// Arming is LEVEL-triggered: a session that starts on top of mail it has never read
/// finds that mail named in its sentinel, rather than having to wait for the next
/// publish to say anything. That is what makes a resume — or an agent restarted after
/// its `~/.mailbox` was cleaned — safe rather than silently deaf.
#[test]
fn session_start_arms_from_existing_unread_mail() {
    let env = Env::new();
    let mut guard = env.leak_guard();
    let daemon = env.start_daemon();
    guard.track_daemon(daemon.pid());
    let session = "ss-existing";
    let topic = env.pr_topic(40);

    env.run_as_ok(session, &["subscribe", &topic], "subscribe");
    env.publish(&topic);

    // Model "this session has never armed": remove the sentinel the daemon wrote, so
    // the only thing that can put the topic back is session-start reading the store.
    std::fs::remove_file(env.sentinel_path(session)).unwrap();

    env.arm(session);
    assert_eq!(
        env.sentinel_topics(session),
        vec![topic],
        "arming must write the mail that was ALREADY waiting, not just an empty file"
    );
    // ...and that is a real wake: the FileChanged the arm fires exits 2.
    assert_eq!(
        env.wake_hook(session).status.code(),
        Some(2),
        "a session that starts on top of unread mail must be able to wake for it"
    );

    let out = env.cleanup(session);
    assert!(out.status.success());
    daemon.stop();
    guard.assert_clean();
}

/// Fail-open: with the bridge DOWN, session-start still arms the sentinel and prints
/// the watchPaths (so the session is wake-wired the moment a daemon exists) and still
/// exits 0 — it never depends on the bridge to arm.
#[test]
fn session_start_fails_open_when_the_bridge_is_down() {
    // No daemon started: every socket call fails, and there is no store to read.
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
    assert!(
        env.sentinel_path(session).exists(),
        "arming must not depend on the bridge: an unreadable store arms with an EMPTY \
         topic set, because a file that does not exist cannot be watched"
    );
    assert!(
        env.sentinel_topics(session).is_empty(),
        "with no store, nothing is known to be unread"
    );
}

// ==== the daemon bumps the sentinel on publish (payload-free) =====================

/// The end-to-end headless chain: a real publish → the DAEMON writes the subscriber's
/// sentinel with the topic name → the wake hook then exits 2 with exactly that topic.
/// Also asserts the sentinel is payload-free (topic names only).
#[test]
fn a_publish_bumps_the_subscribers_sentinel_and_the_wake_hook_exits_2() {
    let env = Env::new();
    let mut guard = env.leak_guard();
    let daemon = env.start_daemon();
    guard.track_daemon(daemon.pid());
    let session = "publish-e2e";
    let topic = env.pr_topic(1);

    env.run_as_ok(session, &["subscribe", &topic], "subscribe");
    env.arm(session);

    // A real, anonymous publish (adapter-style) wakes every subscriber. No poll: the
    // daemon writes the sentinel before it answers the publish.
    env.publish(&topic);

    assert_eq!(
        env.sentinel_topics(session),
        vec![topic.clone()],
        "the sentinel carries the topic NAME, written by the daemon inside the publish"
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
    daemon.stop();
    guard.assert_clean();
}

/// The daemon writes each subscriber's WHOLE unread set, not just the topic it is
/// publishing to. Without that the sentinel would contradict the `Stop`-hook
/// re-trigger (which writes the whole set), and mail the agent was already sitting on
/// would appear to vanish from the file on the next unrelated publish.
#[test]
fn a_publish_writes_the_subscribers_whole_unread_set() {
    let env = Env::new();
    let mut guard = env.leak_guard();
    let daemon = env.start_daemon();
    guard.track_daemon(daemon.pid());
    let session = "whole-set";
    let (a, b) = (env.pr_topic(41), env.pr_topic(42));

    env.run_as_ok(session, &["subscribe", &a], "subscribe a");
    env.run_as_ok(session, &["subscribe", &b], "subscribe b");
    env.arm(session);

    env.publish(&a);
    assert_eq!(env.sentinel_topics(session), vec![a.clone()]);

    // Mail on B while A is still unread: BOTH must be named.
    env.publish(&b);
    assert_eq!(
        env.sentinel_topics(session),
        vec![a.clone(), b.clone()],
        "a publish to B must not erase the mail still unread on A"
    );

    // Once the agent reads everything, the next publish names only what is unread.
    env.run_as_ok(session, &["read"], "read");
    env.publish(&b);
    assert_eq!(
        env.sentinel_topics(session),
        vec![b.clone()],
        "the read cleared A, so only B is named"
    );

    let out = env.cleanup(session);
    assert!(out.status.success());
    daemon.stop();
    guard.assert_clean();
}

/// Per-session isolation at the WRITE side: a publish to a topic only session B
/// subscribes to must not touch session A's sentinel at all. (The read side —
/// A's wake hook exiting 0 if its sentinel is touched anyway — is asserted
/// separately below.)
#[test]
fn a_publish_only_touches_the_sentinels_of_its_own_subscribers() {
    let env = Env::new();
    let mut guard = env.leak_guard();
    let daemon = env.start_daemon();
    guard.track_daemon(daemon.pid());
    let (a, b) = ("iso-write-a", "iso-write-b");
    let topic_b = env.pr_topic(43);

    env.run_as_ok(a, &["subscribe", &env.pr_topic(44)], "subscribe a");
    env.run_as_ok(b, &["subscribe", &topic_b], "subscribe b");
    env.arm(a);
    env.arm(b);
    let a_mtime = sentinel_mtime(&env, a);

    std::thread::sleep(PAST_MTIME_RESOLUTION);
    env.publish(&topic_b);

    assert_eq!(env.sentinel_topics(b), vec![topic_b], "B was woken");
    assert_eq!(
        sentinel_mtime(&env, a),
        a_mtime,
        "A is not a subscriber, so its sentinel must not be touched at all"
    );

    let _ = env.cleanup(a);
    let _ = env.cleanup(b);
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

    // Now a further FileChanged (e.g. a `doctor` probe's content-preserving bump) must
    // NOT wake: there is nothing unread. This is the anti-loop guard, asserted
    // explicitly.
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

// ==== SessionEnd removes the sentinel dir =========================================

#[test]
fn session_end_removes_the_sentinel_dir() {
    let env = Env::new();
    let mut guard = env.leak_guard();
    let daemon = env.start_daemon();
    guard.track_daemon(daemon.pid());
    let session = "teardown";
    let topic = env.pr_topic(5);
    env.run_as_ok(session, &["subscribe", &topic], "subscribe");
    env.arm(session);
    env.publish(&topic);

    let sentinel_dir = env.sentinel_root().join("by-agent").join(session);
    assert!(
        sentinel_dir.exists(),
        "the sentinel dir exists before cleanup"
    );

    let out = env.cleanup(session);
    assert!(out.status.success(), "cleanup must exit 0");
    assert!(
        !sentinel_dir.exists(),
        "cleanup must remove the session's sentinel dir"
    );

    daemon.stop();
    guard.assert_clean();
}

// ==== the Stop hook (turn-end): never wakes, re-registers, re-arms ================

/// The `Stop` hook's invariants: it exits 0 (NEVER 2 — it is not asyncRewake, so it
/// can never itself wake the session) and prints NOTHING on stdout (Claude Code
/// rejects a Stop hook whose output carries `hookEventName: "SessionStart"`, so the
/// watchPaths registration is SessionStart-only). Both are regression guards.
///
/// It also RE-ARMS a sentinel that has gone missing. That is the replacement for the
/// deleted "respawn the dead watcher" net: the sentinel is the only per-session
/// artefact the wake path has left, and a session whose sentinel was removed is deaf
/// until something recreates it — the daemon's own write would be a CREATE, which the
/// watch may not deliver.
#[test]
fn turn_end_never_wakes_prints_nothing_and_re_arms_a_missing_sentinel() {
    let env = Env::new();
    let mut guard = env.leak_guard();
    let daemon = env.start_daemon();
    guard.track_daemon(daemon.pid());
    let session = "turn-end-rearm";
    env.run_as_ok(session, &["subscribe", &env.pr_topic(6)], "subscribe");
    env.arm(session);

    let out = env.turn_end(session);
    assert_eq!(
        out.status.code(),
        Some(0),
        "turn-end must exit 0 (never a wake); stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.trim().is_empty(),
        "the Stop hook must print nothing to stdout (no watchPaths / hookEventName); got: {stdout}"
    );

    // Somebody cleans `~/.mailbox`, or the file is otherwise lost: the session is now
    // unwakeable and nothing else would notice.
    std::fs::remove_file(env.sentinel_path(session)).unwrap();
    let out = env.turn_end(session);
    assert_eq!(out.status.code(), Some(0), "turn-end must still exit 0");
    assert!(
        env.sentinel_path(session).exists(),
        "the turn boundary must re-arm a missing sentinel — it is the only self-heal a \
         Stop hook can perform now that there is no watcher to respawn"
    );

    let out = env.cleanup(session);
    assert!(out.status.success());
    daemon.stop();
    guard.assert_clean();
}

// ==== ADR-0013: turn-end re-registers the inbox on every Stop =====================

/// ADR-0013: the Stop hook re-registers the session's inbox, restoring the ADR-0007
/// invariant (register on every SessionStart AND every Stop). Here NO `session-start`
/// ran, so the inbox is unregistered; a single `turn-end` must make the session
/// addressable.
///
/// This proves the *mechanism* (a Stop re-registers via `register_inbox`). The tombstone
/// **self-heal** it enables — a resume whose SessionStart registration was refused inside
/// the 10s guard, re-subscribing once the guard lapses — is proved at the writer layer,
/// instantly, by `subscribe_after_aged_tombstone_succeeds_and_clears_it` in
/// `storage/writer.rs` (which drives an aged tombstone via `now_ms`). The end-to-end
/// composition is those two facts; we deliberately do NOT re-prove it with a >10s
/// process-level wait (an inverted-pyramid test).
#[test]
fn turn_end_registers_the_inbox() {
    let env = Env::new();
    let mut guard = env.leak_guard();
    let daemon = env.start_daemon();
    guard.track_daemon(daemon.pid());
    let session = "te-registers-inbox";

    // Precondition: with no session-start, the session subscribes to nothing.
    assert!(
        env.subscriptions(session).is_empty(),
        "no inbox should exist before any hook registers it"
    );

    // A single Stop hook must register the inbox (and exit 0, never a wake).
    let out = env.turn_end(session);
    assert_eq!(
        out.status.code(),
        Some(0),
        "turn-end must exit 0; stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let subs = env.subscriptions(session);
    assert_eq!(
        subs,
        vec![format!("agent.{session}")],
        "turn-end must register the agent inbox; got {subs:?}"
    );

    let out = env.cleanup(session);
    assert!(out.status.success());
    daemon.stop();
    guard.assert_clean();
}

/// ADR-0013 fail-open: `turn-end` makes a bridge socket call (`register_inbox`), so the
/// "a Stop hook must NEVER fail or wake, even with the bridge down" guarantee needs its
/// own coverage — the mirror of `session_start_fails_open_when_the_bridge_is_down`. With
/// NO daemon, the socket call fails; the hook must still exit 0 and leak nothing.
#[test]
fn turn_end_fails_open_when_the_bridge_is_down() {
    // No daemon started: the register_inbox socket call cannot connect.
    let env = Env::new();
    let session = "te-failopen";

    let out = env.turn_end(session);
    assert_eq!(
        out.status.code(),
        Some(0),
        "turn-end must exit 0 even with the bridge down; stderr: {}",
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
/// inbox subscription, and the session still armed.
#[test]
fn session_start_is_idempotent_across_a_resume() {
    let env = Env::new();
    let mut guard = env.leak_guard();
    let daemon = env.start_daemon();
    guard.track_daemon(daemon.pid());
    let session = "ss-resume";

    // First SessionStart (source: startup).
    env.arm(session);
    assert_eq!(
        env.subscriptions(session),
        vec![format!("agent.{session}")],
        "startup registers exactly the inbox"
    );

    // Second SessionStart (source: resume — a fresh process re-establishing itself).
    // The matcher is "" so this fires; it must be a clean idempotent no-op.
    env.arm(session);
    assert_eq!(
        env.subscriptions(session),
        vec![format!("agent.{session}")],
        "resume must not duplicate the inbox subscription"
    );
    assert!(
        env.sentinel_topics(session).is_empty(),
        "re-arming a caught-up session writes the empty set, not a phantom topic"
    );

    let out = env.cleanup(session);
    assert!(out.status.success());
    daemon.stop();
    guard.assert_clean();
}

// ==== every publish bumps; the wake hook's anti-loop bounds wakes =================

/// The no-coalescing contract: the daemon writes the sentinel UNCONDITIONALLY on every
/// publish, so a second message on an ALREADY-unread topic DOES re-bump — there is no
/// comparison that could suppress it. What bounds the number of actual model wakes is
/// the wake hook's anti-loop (exit 2 while there is genuine unread, exit 0 once the
/// agent has caught up), NOT any suppression of sentinel writes. With every publish
/// bumping, no message can be lost.
#[test]
fn every_publish_bumps_the_sentinel_and_the_wake_hook_bounds_wakes() {
    let env = Env::new();
    let mut guard = env.leak_guard();
    let daemon = env.start_daemon();
    guard.track_daemon(daemon.pid());
    let session = "every-publish-bumps";
    let t = env.pr_topic(7);
    env.run_as_ok(session, &["subscribe", &t], "subscribe");
    env.arm(session);

    env.publish(&t);
    assert_eq!(env.sentinel_topics(session), vec![t.clone()]);
    let after_first = sentinel_mtime(&env, session);

    // Past a coarse mtime resolution so the re-bump is observable even though the
    // sentinel CONTENT is identical ([T] again) — the mtime is the only signal.
    std::thread::sleep(PAST_MTIME_RESOLUTION);

    // A SECOND publish on the SAME already-unread topic RE-bumps. Both events remain
    // durably unread.
    env.publish(&t);
    assert!(
        sentinel_mtime(&env, session) > after_first,
        "the second same-topic message must re-bump (no coalescing)"
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
    assert_eq!(env.unread_total(session), 0, "caught up");
    let wake = env.wake_hook(session);
    assert_eq!(
        wake.status.code(),
        Some(0),
        "caught up → no wake (anti-loop bounds wakes, not coalescing)"
    );

    let out = env.cleanup(session);
    assert!(out.status.success());
    daemon.stop();
    guard.assert_clean();
}

/// Deafness regression (re-notify after a read): after the agent READS and catches up, a
/// NEW message on the SAME topic must still wake it. Under the old coalescing this was
/// the permanent-deafness trap (same unread set → suppressed forever).
#[test]
fn a_new_message_after_a_read_re_bumps_even_on_the_same_topic() {
    let env = Env::new();
    let mut guard = env.leak_guard();
    let daemon = env.start_daemon();
    guard.track_daemon(daemon.pid());
    let session = "re-notify";
    let a = env.pr_topic(9);
    env.run_as_ok(session, &["subscribe", &a], "subscribe");
    env.arm(session);

    env.publish(&a);
    assert_eq!(env.sentinel_topics(session), vec![a.clone()]);
    let after_first = sentinel_mtime(&env, session);

    // The agent READS and catches up.
    env.run_as_ok(session, &["read"], "read");
    assert_eq!(env.unread_total(session), 0, "caught up");

    // Wait past a coarse mtime resolution so a re-bump is observable even though the
    // sentinel CONTENT is identical ([A] again).
    std::thread::sleep(PAST_MTIME_RESOLUTION);

    // A NEW message on the SAME topic A must RE-bump.
    env.publish(&a);
    assert!(
        sentinel_mtime(&env, session) > after_first,
        "post-read mail on the same topic must re-bump the sentinel"
    );
    let wake = env.wake_hook(session);
    assert_eq!(
        wake.status.code(),
        Some(2),
        "post-read mail on the same topic must still wake (no permanent deafness)"
    );

    let out = env.cleanup(session);
    assert!(out.status.success());
    daemon.stop();
    guard.assert_clean();
}

// ==== cross-session isolation — B's sentinel change must not wake A ===============

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
/// watched PR): the steady-state wake is a pure EDGE, and the `FileChanged` → exit-2
/// wake only reaches an IDLE session. Mail published while the agent is mid-turn bumps
/// the sentinel to no effect — the edge is spent against a busy session — and nothing
/// ever bumps it again, so the agent goes idle deaf on top of unread mail and stays
/// that way until the next publish.
///
/// The fix is level-triggered arming at the one signal that says a turn ENDED: `Stop`
/// re-bumps the sentinel iff the session is sitting on mail newer than any it has
/// already been re-triggered for. This test drives exactly that sequence, modelling
/// "the wake was spent while busy" as "the wake hook never ran for that bump".
#[test]
fn mail_that_arrived_while_busy_is_re_triggered_at_the_turn_boundary() {
    let env = Env::new();
    let mut guard = env.leak_guard();
    let daemon = env.start_daemon();
    guard.track_daemon(daemon.pid());
    let session = "busy-window";
    let t = env.pr_topic(30);
    env.run_as_ok(session, &["subscribe", &t], "subscribe");
    env.arm(session);

    // Mail arrives while the agent is BUSY mid-turn: the daemon bumps the sentinel,
    // but the FileChanged wake that bump triggers reaches a busy session and is spent
    // (modelled by never running the wake hook for it).
    env.publish(&t);
    assert_eq!(env.sentinel_topics(session), vec![t.clone()]);
    let after_publish = sentinel_mtime(&env, session);
    assert_eq!(
        retrigger_record(&env, session),
        None,
        "nothing has been re-triggered yet"
    );

    // Past a coarse mtime resolution, so the re-bump is observable even though the
    // sentinel CONTENT is identical ([T] again).
    std::thread::sleep(PAST_MTIME_RESOLUTION);

    // The turn ends. Stop must notice the unread mail and re-bump, so a FileChanged
    // fires against the now-idle session. NOTHING ELSE could have bumped it: no
    // publish happened in this window, and there is no per-session process left.
    let out = env.turn_end(session);
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
    std::thread::sleep(PAST_MTIME_RESOLUTION);
    let out = env.turn_end(session);
    assert_eq!(out.status.code(), Some(0), "turn-end must exit 0");
    assert_eq!(
        sentinel_mtime(&env, session),
        after_stop,
        "already-re-triggered mail must not be re-bumped again (anti-loop)"
    );

    // But the bound is per-message, not permanent: after the agent catches up, the NEXT
    // message to arrive while busy is re-triggered on its own turn boundary.
    env.run_as_ok(session, &["read"], "read");
    assert_eq!(env.unread_total(session), 0, "caught up");
    env.publish(&t);
    assert_eq!(env.unread_total(session), 1, "new mail while busy");
    let after_second = sentinel_mtime(&env, session);
    std::thread::sleep(PAST_MTIME_RESOLUTION);
    let out = env.turn_end(session);
    assert_eq!(out.status.code(), Some(0), "turn-end must exit 0");
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

    let out = env.turn_end(session);
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
    env.arm(session);
    let armed_mtime = sentinel_mtime(&env, session);

    std::thread::sleep(PAST_MTIME_RESOLUTION);
    let out = env.turn_end(session);
    assert_eq!(out.status.code(), Some(0), "turn-end must exit 0");
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
    daemon.stop();
    guard.assert_clean();
}
