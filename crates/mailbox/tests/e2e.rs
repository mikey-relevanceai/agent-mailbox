//! Card 12 — the cross-component end-to-end suite.
//!
//! This is the ONE place the design test plan (design/01-mvp-github-watch.md
//! § Test plan) is ENFORCED as automation rather than prose. Each of the six
//! scenarios runs 1:1 through the REAL production stack — a `mailbox serve`
//! daemon + `mailbox` CLI client processes + a real supervised adapter (the
//! github-pr poller driven by a recorded FAKE `gh`, and the reference stub
//! adapter) + the fake harness driver (hook JSON fed to `mailbox harness arm`
//! / `cleanup`). No network, no real GitHub, no real Claude Code.
//!
//! The guarantees proven end to end (the invariants in design/01 + AGENTS.md):
//! - **No zombie pollers** — every scenario holds a [`LeakGuard`] that FAILS the
//!   test if any adapter / serve process survives teardown (ac-12-3).
//! - **Edge-triggered exactly-once** — scenario 1 asserts each transition
//!   publishes exactly once across many polls.
//! - **Independent per-subscriber cursors** — scenario 2 asserts two sessions
//!   each read the same edge from their own cursor.
//! - **Payload-free harness wake** — the wake-path tests wake an armed session and
//!   coalesce a publish storm into a single wake (ac-12-2).
//!
//! Unit-level proofs of the same machinery live in their own cards' suites
//! (`supervision.rs` at the library boundary, `filechanged_wake.rs` for the wake path,
//! `adapter_e2e.rs` at the adapter boundary); this suite deliberately does NOT
//! re-derive them — it proves they compose through the shipped binaries. Shared
//! harness lives in `tests/common/` so nothing is copy-pasted.
//!
//! Flakiness discipline: bounded polled deadlines (never a fixed sleep waiting for
//! a state — the two fixed waits assert a *negative*, i.e. that nothing happens),
//! a tempdir + scoped socket/db/sentinel root per test, and every child reaped on drop.

mod common;

use std::time::{Duration, Instant};

use common::{
    Env, LeakGuard, PR_CONFLICTING_CI_FAILURE, PR_CONFLICTING_CI_SUCCESS, PR_MERGED, count_edges,
    descendant_pids, pid_alive, poll_until,
};

/// Start `session` through the production `SessionStart` hook, which registers its
/// inbox and arms its wake sentinel.
fn arm(env: &Env, session: &str) {
    env.arm(session);
}

/// Block until `session`'s sentinel names `topic`, then confirm the `FileChanged`
/// hook turns that into a real wake (exit 2). This pair IS the wake wire.
fn assert_woken_for(env: &Env, session: &str, topic: &str) {
    poll_until("the sentinel names the topic", SETTLE, || {
        env.sentinel_topics(session)
            .iter()
            .any(|t| t == topic)
            .then_some(())
    });
    assert_eq!(
        env.wake_hook(session).status.code(),
        Some(2),
        "{session} must be woken for {topic}"
    );
}

/// A generous bound for "the supervisor spawned/settled the adapter", well above
/// its ~1s restart backoff, so the suite stays green under parallel load.
const SETTLE: Duration = Duration::from_secs(20);

// ===== Scenario 1 — recorded gh: conflict / review / CI each publish EXACTLY once
// (design/01 test plan #1; edge-triggered exactly-once). ========================

/// A single transition poll flips mergeable→conflicting, CI success→failure, and
/// introduces a new review — so the poller fires exactly one conflict, one
/// CI-failure, and one new-review edge. The fake `gh` then clamps to that state
/// forever, so continuing to poll re-fires NOTHING: exactly-once, end to end
/// through serve → supervisor → github-pr adapter → bridge → `read`.
#[test]
fn scenario_1_conflict_review_ci_each_publish_exactly_once() {
    let env = Env::new();
    env.set_pr_fixture(1, PR_CONFLICTING_CI_FAILURE);
    env.set_reviews_fixture(1, r#"[{"id":100}]"#);
    let daemon = env.start_daemon();
    let mut guard = env.leak_guard();
    guard.track_daemon(daemon.pid());

    let s = "s1";
    let spec = env.pr_spec(1);
    env.run_ok(
        &[
            "watch",
            "github-pr",
            &spec,
            "--interval",
            "1",
            "--session",
            s,
        ],
        "watch github-pr",
    );
    let pid = poll_until("adapter running", SETTLE, || env.watch_pid(s));

    // Accumulate reads until all three transitions have surfaced once.
    let (mut conflict, mut ci, mut review) = (0usize, 0usize, 0usize);
    let deadline = Instant::now() + SETTLE;
    loop {
        let events = env.read_events(s);
        conflict += count_edges(&events, "mergeable_conflicting");
        ci += count_edges(&events, "ci_failure");
        review += count_edges(&events, "new_reviews");
        if conflict >= 1 && ci >= 1 && review >= 1 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the three edges never all surfaced (conflict={conflict} ci={ci} review={review})"
        );
        std::thread::sleep(Duration::from_millis(150));
    }
    assert_eq!(
        (conflict, ci, review),
        (1, 1, 1),
        "each transition publishes exactly once"
    );

    // Keep polling across the clamped (stable) state: nothing re-fires.
    let more = Instant::now() + Duration::from_secs(4);
    while Instant::now() < more {
        let events = env.read_events(s);
        conflict += count_edges(&events, "mergeable_conflicting");
        ci += count_edges(&events, "ci_failure");
        review += count_edges(&events, "new_reviews");
        std::thread::sleep(Duration::from_millis(300));
    }
    assert_eq!(
        (conflict, ci, review),
        (1, 1, 1),
        "a stable state re-fires nothing — edge-triggered exactly-once end to end"
    );

    // Teardown: the last interest leaves → the poller is torn down (no zombie).
    env.run_ok(
        &["unwatch", "github-pr", &spec, "--session", s],
        "unwatch github-pr",
    );
    poll_until("adapter reaped after unwatch", SETTLE, || {
        (!pid_alive(pid)).then_some(())
    });
    guard.assert_clean();
}

// ===== Scenario 1b — a merge surfaces exactly once end to end ===================

/// A transition poll flips the PR to MERGED, so the poller fires exactly one
/// `pr_merged` edge, and the clamped merged state re-fires nothing — proving the
/// merge notification flows serve → supervisor → github-pr adapter → bridge →
/// `read`, and is terminal (edge-triggered exactly-once) end to end.
#[test]
fn scenario_1b_merge_surfaces_exactly_once() {
    let env = Env::new();
    env.set_pr_fixture(1, PR_MERGED);
    let daemon = env.start_daemon();
    let mut guard = env.leak_guard();
    guard.track_daemon(daemon.pid());

    let s = "s1";
    let spec = env.pr_spec(1);
    env.run_ok(
        &[
            "watch",
            "github-pr",
            &spec,
            "--interval",
            "1",
            "--session",
            s,
        ],
        "watch github-pr",
    );
    let pid = poll_until("adapter running", SETTLE, || env.watch_pid(s));

    let mut merged = 0usize;
    let deadline = Instant::now() + SETTLE;
    loop {
        merged += count_edges(&env.read_events(s), "pr_merged");
        if merged >= 1 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the pr_merged edge never surfaced"
        );
        std::thread::sleep(Duration::from_millis(150));
    }
    assert_eq!(merged, 1, "the merge publishes exactly once");

    // Keep polling across the clamped merged state: nothing re-fires (terminal).
    let more = Instant::now() + Duration::from_secs(4);
    while Instant::now() < more {
        merged += count_edges(&env.read_events(s), "pr_merged");
        std::thread::sleep(Duration::from_millis(300));
    }
    assert_eq!(
        merged, 1,
        "a merged PR re-fires nothing — the merge edge is terminal end to end"
    );

    env.run_ok(
        &["unwatch", "github-pr", &spec, "--session", s],
        "unwatch github-pr",
    );
    poll_until("adapter reaped after unwatch", SETTLE, || {
        (!pid_alive(pid)).then_some(())
    });
    guard.assert_clean();
}

// ===== Scenario 2 — two sessions watch the SAME PR → ONE child; independent
// cursors (design/01 test plan #2). ============================================

/// Two sessions watch the same PR: the supervisor keys the adapter by
/// `(kind, repo, pr)`, so both share ONE child (same pid, interest 2). A synthetic
/// edge on the PR topic is then delivered to BOTH from their own cursor — session
/// A's read does not consume session B's copy (independent per-subscriber cursors).
#[test]
fn scenario_2_two_sessions_share_one_child_with_independent_cursors() {
    let env = Env::new();
    let daemon = env.start_daemon();
    let mut guard = env.leak_guard();
    guard.track_daemon(daemon.pid());

    let spec = env.pr_spec(2);
    let topic = env.pr_topic(2);
    env.run_ok(
        &[
            "watch",
            "github-pr",
            &spec,
            "--interval",
            "1",
            "--session",
            "a",
        ],
        "watch a",
    );
    env.run_ok(
        &[
            "watch",
            "github-pr",
            &spec,
            "--interval",
            "1",
            "--session",
            "b",
        ],
        "watch b",
    );

    // ONE child: both sessions' status views name the same running pid, interest 2.
    let pid = poll_until("adapter running", SETTLE, || env.watch_pid("a"));
    assert_eq!(
        env.watch_pid("b"),
        Some(pid),
        "both sessions share ONE adapter process (keyed by the PR, not the session)"
    );
    assert!(pid_alive(pid));
    assert_eq!(
        env.watch_state_interest("a").map(|(_, i)| i),
        Some(2),
        "one watch row with refcounted interest 2"
    );

    // A new edge is delivered to BOTH, each from its independent cursor.
    env.publish(&topic);
    let a = poll_until("a receives the edge", SETTLE, || {
        let e = env.read_events("a");
        (!e.is_empty()).then_some(e)
    });
    let b = poll_until("b receives the edge independently", SETTLE, || {
        let e = env.read_events("b");
        (!e.is_empty()).then_some(e)
    });
    assert_eq!(a.len(), 1);
    assert_eq!(b.len(), 1);
    assert_eq!(a[0]["offset"].as_u64(), Some(0));
    assert_eq!(
        b[0]["offset"].as_u64(),
        Some(0),
        "B's cursor is independent — A's read did not consume B's copy of offset 0"
    );
    assert!(
        env.read_events("a").is_empty(),
        "A's own cursor advanced past the edge after its read"
    );

    // Teardown: both leave → the shared child is torn down.
    env.run_ok(
        &["unwatch", "github-pr", &spec, "--session", "a"],
        "unwatch a",
    );
    env.run_ok(
        &["unwatch", "github-pr", &spec, "--session", "b"],
        "unwatch b",
    );
    poll_until("shared child reaped after last interest", SETTLE, || {
        (!pid_alive(pid)).then_some(())
    });
    guard.assert_clean();
}

// ===== Scenario 3 — first session leaves → child STILL running; second still
// woken on new edges (design/01 test plan #3). =================================

/// The refcount is per session: the first session leaving must NOT kill a poller
/// the second still needs. After session A unwatches, the child keeps the SAME pid
/// for B, B still receives a new edge, and A (now unsubscribed) receives nothing.
#[test]
fn scenario_3_first_session_leaves_child_survives_second_still_woken() {
    let env = Env::new();
    let daemon = env.start_daemon();
    let mut guard = env.leak_guard();
    guard.track_daemon(daemon.pid());

    let spec = env.pr_spec(3);
    let topic = env.pr_topic(3);
    env.run_ok(
        &[
            "watch",
            "github-pr",
            &spec,
            "--interval",
            "1",
            "--session",
            "a",
        ],
        "watch a",
    );
    env.run_ok(
        &[
            "watch",
            "github-pr",
            &spec,
            "--interval",
            "1",
            "--session",
            "b",
        ],
        "watch b",
    );
    let pid = poll_until("adapter running", SETTLE, || env.watch_pid("a"));

    // First session leaves: the child MUST keep running for the second.
    env.run_ok(
        &["unwatch", "github-pr", &spec, "--session", "a"],
        "unwatch a",
    );
    let (state, interest) = poll_until("interest drops to 1", SETTLE, || {
        env.watch_state_interest("b")
    });
    assert_eq!(interest, 1, "only A's interest was dropped");
    assert_eq!(state, "running", "the poller keeps running for B");
    assert_eq!(
        env.watch_pid("b"),
        Some(pid),
        "the SAME child keeps running (not a respawn)"
    );
    assert!(pid_alive(pid));

    // B is still woken on a new edge; A, having unwatched (and unsubscribed), is not.
    env.publish(&topic);
    let b = poll_until("B still receives edges after A left", SETTLE, || {
        let e = env.read_events("b");
        (!e.is_empty()).then_some(e)
    });
    assert_eq!(b.len(), 1);
    assert!(
        env.read_events("a").is_empty(),
        "A unsubscribed on unwatch — it no longer receives the PR's edges"
    );

    // Last session leaves → child gone.
    env.run_ok(
        &["unwatch", "github-pr", &spec, "--session", "b"],
        "unwatch b",
    );
    poll_until("child reaped once the last interest leaves", SETTLE, || {
        (!pid_alive(pid)).then_some(())
    });
    poll_until("watch marked stopped", SETTLE, || {
        (env.watch_state_interest("b")?.0 == "stopped").then_some(())
    });
    guard.assert_clean();
}

// ===== Scenario 4 — last session leaves → child gone; NO further API calls
// (design/01 test plan #4). ====================================================

/// Once the last interest is gone the poller is torn down and makes NO further
/// `gh` calls — the exact "no zombie poller silently hammering the API" property.
/// Proven two ways: the adapter process dies, AND the fake `gh` call counter stops
/// advancing.
#[test]
fn scenario_4_last_session_leaves_child_gone_and_no_further_api_calls() {
    let env = Env::new();
    let daemon = env.start_daemon();
    let mut guard = env.leak_guard();
    guard.track_daemon(daemon.pid());

    let s = "solo";
    let spec = env.pr_spec(4);
    env.run_ok(
        &[
            "watch",
            "github-pr",
            &spec,
            "--interval",
            "1",
            "--session",
            s,
        ],
        "watch github-pr",
    );
    let pid = poll_until("adapter running", SETTLE, || env.watch_pid(s));
    // Let the poller actually hit the (fake) API a few times first.
    poll_until("poller has called gh at least twice", SETTLE, || {
        (env.gh_pr_call_count() >= 2).then_some(())
    });

    // Last interest gone → child torn down, watch stopped.
    env.run_ok(
        &["unwatch", "github-pr", &spec, "--session", s],
        "unwatch github-pr",
    );
    poll_until("child reaped", SETTLE, || (!pid_alive(pid)).then_some(()));
    poll_until("watch stopped", SETTLE, || {
        (env.watch_state_interest(s)?.0 == "stopped").then_some(())
    });

    // No further API calls: the count is frozen. A bounded wait is the right tool
    // here — we are asserting that NOTHING happens over a couple of poll intervals.
    let calls = env.gh_pr_call_count();
    std::thread::sleep(Duration::from_millis(2500));
    assert_eq!(
        env.gh_pr_call_count(),
        calls,
        "a stopped poller makes no further gh calls (no zombie poller)"
    );
    guard.assert_clean();
}

// ===== Scenario 5 — kill the adapter → ONE restart while interest > 0; stays
// stopped at interest 0 (design/01 test plan #5). ==============================

/// SIGKILLing the adapter out from under the supervisor while interest is held
/// triggers exactly one restart (a new, stable pid). Dropping the last interest
/// then reaps it and it stays stopped — a wanted-by-nobody watch is never
/// zombie-restarted.
#[test]
fn scenario_5_kill_adapter_restarts_once_then_stays_stopped() {
    let env = Env::new();
    let daemon = env.start_daemon();
    let mut guard = env.leak_guard();
    guard.track_daemon(daemon.pid());

    let s = "s5";
    let spec = env.pr_spec(5);
    env.run_ok(
        &[
            "watch",
            "github-pr",
            &spec,
            "--interval",
            "1",
            "--session",
            s,
        ],
        "watch github-pr",
    );
    let pid1 = poll_until("adapter running", SETTLE, || env.watch_pid(s));
    assert!(pid_alive(pid1));

    // Kill it: interest is still 1, so the supervisor must restart it with a NEW pid.
    nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(pid1 as i32),
        nix::sys::signal::Signal::SIGKILL,
    )
    .expect("SIGKILL the adapter");
    let pid2 = poll_until("adapter restarted with a new live pid", SETTLE, || {
        env.watch_pid(s).filter(|&p| p != pid1 && pid_alive(p))
    });
    assert_ne!(pid1, pid2, "the crash was restarted");
    poll_until("the killed pid is reaped", SETTLE, || {
        (!pid_alive(pid1)).then_some(())
    });

    // The replacement is stable — one restart, not a churn. Watch across MORE than
    // one restart backoff (~1s) so a slow crash-loop settling on a third pid just
    // outside a shorter window cannot slip through: the pid must never change.
    let stable_until = Instant::now() + Duration::from_millis(1800);
    while Instant::now() < stable_until {
        assert_eq!(
            env.watch_pid(s),
            Some(pid2),
            "the restarted adapter must not churn to a third pid — exactly one restart"
        );
        std::thread::sleep(Duration::from_millis(100));
    }

    // Interest 0 → reaped and stays stopped.
    env.run_ok(
        &["unwatch", "github-pr", &spec, "--session", s],
        "unwatch github-pr",
    );
    poll_until("restarted adapter reaped on last interest", SETTLE, || {
        (!pid_alive(pid2)).then_some(())
    });
    poll_until("watch stopped", SETTLE, || {
        (env.watch_state_interest(s)?.0 == "stopped").then_some(())
    });
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(
        env.watch_pid(s),
        None,
        "with no interest the watch stays stopped — no zombie restart"
    );
    guard.assert_clean();
}

// ===== Scenario 6 — bridge restart with no live interested sessions → watch NOT
// resumed (design/01 test plan #6). ============================================

/// On a bridge restart a previously-running watch is NOT resumed (the fail-safe:
/// missed events beat a poller that outlives every session that wanted it, since
/// there is no session-liveness probe yet). The old adapter dies WITH the first
/// daemon, and the second daemon spawns no replacement — no zombie on restart.
#[test]
fn scenario_6_bridge_restart_with_no_live_interest_does_not_resume() {
    let env = Env::new();
    let d1 = env.start_daemon();
    let mut guard = env.leak_guard();
    guard.track_daemon(d1.pid());

    let s = "s6";
    let spec = env.pr_spec(6);
    env.run_ok(
        &[
            "watch",
            "github-pr",
            &spec,
            "--interval",
            "1",
            "--session",
            s,
        ],
        "watch github-pr",
    );
    let pid1 = poll_until("adapter running", SETTLE, || env.watch_pid(s));
    assert!(pid_alive(pid1));

    // Snapshot d1's ENTIRE adapter subtree WHILE d1 is still alive (walkable). Once
    // d1 exits, any surviving child is reparented away from d1's pid and the guard's
    // ppid walk can no longer see it — so record it now to make "no leftover from a
    // dead bridge" a real guarantee, not just a check of the one known adapter pid.
    let d1_subtree = descendant_pids(d1.pid());
    assert!(
        d1_subtree.contains(&pid1),
        "sanity: the adapter must be in d1's subtree before restart"
    );

    // Bridge restart: stop the first daemon (which tears down its adapter), then
    // start a fresh daemon on the SAME db.
    d1.stop();
    poll_until(
        "d1's ENTIRE adapter subtree dies with the bridge",
        SETTLE,
        || d1_subtree.iter().all(|&p| !pid_alive(p)).then_some(()),
    );
    let d2 = env.start_daemon();
    guard.track_daemon(d2.pid());

    // reconcile_startup marks the previously-running watch stopped and does NOT
    // resume it (no live-session probe → fail safe).
    poll_until("watch marked stopped on restart", SETTLE, || {
        (env.watch_state_interest(s)?.0 == "stopped").then_some(())
    });
    assert_eq!(
        env.watch_pid(s),
        None,
        "no adapter resumed on restart (no zombie)"
    );

    // And it STAYS not-resumed: no adapter spawns and no gh calls happen. A bounded
    // wait, again asserting a negative.
    let calls = env.gh_pr_call_count();
    std::thread::sleep(Duration::from_millis(1500));
    assert_eq!(env.watch_pid(s), None, "still not resumed");
    assert_eq!(
        env.gh_pr_call_count(),
        calls,
        "a not-resumed watch makes no gh calls"
    );
    guard.assert_clean();
}

// ===== Wake path — a supervised adapter's publish wakes an idle session (ac-12-2) ==

/// A REAL supervised adapter's publish wakes an armed idle session: `watch stub`
/// subscribes the session and starts the stub poller; `session-start` arms the
/// sentinel; the stub's next publish makes the daemon write it → the wake hook exits
/// 2 with the payload-free topic reminder on stderr. This is the full watch →
/// adapter → bridge → harness-wake chain the other suites don't drive end to end.
#[test]
fn wake_supervised_adapter_publish_wakes_an_armed_session() {
    let env = Env::new();
    let daemon = env.start_daemon();
    let mut guard = env.leak_guard();
    guard.track_daemon(daemon.pid());

    let s = "waker";
    env.run_ok(
        &[
            "watch",
            "stub",
            "wake",
            "--interval-ms",
            "150",
            "--session",
            s,
        ],
        "watch stub",
    );

    // The SessionStart hook launches the watcher (the agent runs nothing).
    arm(&env, s);

    // The supervised stub's publish bumps the sentinel → the wake hook exits 2.
    assert_woken_for(&env, s, "stub.wake");

    // Teardown: stop the poller + drop the session (SessionEnd), leaving nothing.
    env.run_ok(&["unwatch", "stub", "wake", "--session", s], "unwatch stub");
    let _ = env.cleanup(s);
    guard.assert_clean();
}

/// Coalescing: a storm of publishes against a single armed session produces ONE
/// wake, and a later `read` still returns EVERY event (the wake advanced no
/// cursor). Driven through the real CLI + the fake harness driver.
#[test]
fn wake_many_publishes_coalesce_to_one_wake() {
    let env = Env::new();
    let daemon = env.start_daemon();
    let mut guard = env.leak_guard();
    guard.track_daemon(daemon.pid());

    let s = "coalesce";
    let topic = "t.coalesce";
    env.run_ok(&["subscribe", topic, "--session", s], "subscribe");

    arm(&env, s);
    assert!(
        env.sentinel_topics(s).is_empty(),
        "nothing is pending before any publish"
    );

    // Ten rapid publishes → the armed session wakes exactly once.
    for _ in 0..10 {
        env.publish(topic);
    }
    assert_woken_for(&env, s, topic);

    // The wake advanced no cursor, so a read now drains all ten durable events.
    let events = env.read_events(s);
    assert_eq!(
        events.len(),
        10,
        "one wake, but a later read returns every coalesced event"
    );

    let _ = env.cleanup(s);
    guard.assert_clean();
}

/// Mid-turn surfacing through the COMPOSED path: a SUPERVISED adapter's edge that
/// lands while the agent is mid-turn is not lost. The
/// github-pr poller fires exactly one conflict edge; we confirm it is unread
/// WITHOUT reading it (via `status`, which does not advance the cursor); then the
/// next wake-hook run sees the still-unread edge and wakes (exit 2),
/// delivered exactly once. Unlike a bare-`publish` version (which would duplicate
/// `harness.rs`'s AC2), this proves the full watch → supervised adapter → bridge →
/// next-arm composition.
#[test]
fn wake_mid_turn_supervised_edge_surfaces_on_next_arm() {
    let env = Env::new();
    env.set_pr_fixture(1, PR_CONFLICTING_CI_SUCCESS); // one conflict edge, then stable
    let daemon = env.start_daemon();
    let mut guard = env.leak_guard();
    guard.track_daemon(daemon.pid());

    let s = "midturn";
    let spec = env.pr_spec(7);
    env.run_ok(
        &[
            "watch",
            "github-pr",
            &spec,
            "--interval",
            "1",
            "--session",
            s,
        ],
        "watch github-pr",
    );
    let pid = poll_until("adapter running", SETTLE, || env.watch_pid(s));

    // The supervised poller fires its one conflict edge mid-turn. Confirm it is
    // unread WITHOUT reading it, so it stays pending for the wake hook.
    poll_until("the supervised edge lands unread", SETTLE, || {
        (env.unread_total(s) >= 1).then_some(())
    });

    // A fresh watcher sees the still-unread edge immediately and bumps for it.
    arm(&env, s);
    let topic = format!("github.pr.{spec}");
    assert_woken_for(&env, s, &topic);

    // Delivered exactly once, then the cursor advances (no redelivery).
    let events = env.read_events(s);
    assert_eq!(events.len(), 1, "exactly one edge is pending");
    assert_eq!(
        count_edges(&events, "mergeable_conflicting"),
        1,
        "the mid-turn conflict is delivered exactly once"
    );
    assert!(
        env.read_events(s).is_empty(),
        "and its cursor advanced — not redelivered"
    );

    // Teardown.
    env.run_ok(
        &["unwatch", "github-pr", &spec, "--session", s],
        "unwatch github-pr",
    );
    poll_until("adapter reaped", SETTLE, || (!pid_alive(pid)).then_some(()));
    let _ = env.cleanup(s);
    guard.assert_clean();
}

// ===== The leak guard PROVES it catches a leak (ac-12-3) =======================

/// The load-bearing guard must not be a no-op that always passes. This deliberately
/// leaks live processes into the two scopes the guard watches — a descendant of a
/// tracked "daemon" root — and asserts the guard REPORTS it (would flip a scenario
/// red), then reports clean once it is reaped. If the guard could not see this,
/// every scenario's `assert_clean` would be worthless.
#[test]
fn leak_guard_detects_a_surviving_process_and_clears_when_reaped() {
    use std::process::{Command, Stdio};

    // A live child of a tracked root must be caught.
    // `sh -c 'sleep 60; true'` stays alive as the PARENT of a `sleep` child (the
    // trailing command defeats sh's exec-optimization), so `sleep` is a genuine
    // descendant of a UNIQUE root we own — never another test's process.
    let mut root = Command::new("sh")
        .args(["-c", "sleep 60; true"])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn sh root");

    let mut guard = LeakGuard::default();
    guard.track_daemon(root.id());

    let leaks = poll_until("guard sees the descendant leak", SETTLE, || {
        let leaks = guard.find_leaks();
        leaks
            .iter()
            .any(|l| l.source == "daemon-descendant")
            .then_some(leaks)
    });
    // Reap the whole subtree: kill the descendant(s) explicitly (so no orphaned
    // `sleep` lingers) and the root.
    for leak in &leaks {
        let _ = nix::sys::signal::kill(
            nix::unistd::Pid::from_raw(leak.pid as i32),
            nix::sys::signal::Signal::SIGKILL,
        );
    }
    let _ = root.kill();
    let _ = root.wait();
    poll_until("guard clears once the subtree is reaped", SETTLE, || {
        guard.find_leaks().is_empty().then_some(())
    });
}
