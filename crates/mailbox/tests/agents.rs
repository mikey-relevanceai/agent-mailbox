//! Inter-agent messaging end to end (card 16), driving the REAL binaries.
//!
//! Everything here runs against a live `mailbox serve` daemon in a tempdir and
//! real `mailbox` CLI client processes, with the fake harness driver (hook JSON on
//! `harness arm` / `cleanup`'s stdin) standing in for Claude Code — the same seams
//! the card 11/12 suites use. No agent ever runs an arm command, and no wake is
//! simulated: the waiters are real blocked processes and the wakes are real
//! process exits.
//!
//! The headline is [`round_trip_two_idle_agents_wake_each_other`]: two registered
//! sessions, both idle on genuine waiters, message each other with no human in the
//! loop.
//!
//! Every test scopes a [`LeakGuard`] to its own daemon subtree + waiters dir, so a
//! leaked waiter fails the test loudly rather than escaping into the runner.

mod common;

use std::time::Duration;

use serde_json::Value;

use common::{Env, drain_stderr, poll_until, wait_within};

/// Generous bound for a real process to arm / wake / exit under CI load. Every
/// wait is a bounded poll, never a fixed sleep.
const SETTLE: Duration = Duration::from_secs(10);

/// Arm `session` (registering its inbox, ADR-0007) and block until its waiter is
/// genuinely live — the pidfile is written only AFTER the waiter takes the
/// single-waiter lock, so its presence means "blocked and listening", not merely
/// "process spawned".
fn arm_idle(env: &Env, session: &str) -> common::ArmChild {
    let arm = env.spawn_arm(session, &[]);
    poll_until("waiter pidfile appears", SETTLE, || {
        env.waiter_pidfile(session).exists().then_some(())
    });
    arm
}

/// `mailbox agents --json` as seen by `caller`.
fn agents(env: &Env, caller: &str) -> Vec<Value> {
    let out = env.run_ok(&["--json", "agents", "--session", caller], "agents");
    let value: Value =
        serde_json::from_str(String::from_utf8_lossy(&out.stdout).trim()).expect("agents json");
    value["agents"].as_array().cloned().unwrap_or_default()
}

/// The row for `session` in `mailbox agents`, if it is registered.
fn agent_row(env: &Env, caller: &str, session: &str) -> Option<Value> {
    agents(env, caller)
        .into_iter()
        .find(|a| a["session"] == Value::String(session.to_string()))
}

// ==== the headline: two idle agents poke each other, no human in the loop =======

/// A and B are both registered and idle on REAL blocked waiters. A sends to B: B's
/// waiter exits 2 with the payload-free reminder naming B's inbox topic; B reads
/// the message and sees `from: A`; B replies; A's waiter wakes the same way.
#[test]
fn round_trip_two_idle_agents_wake_each_other() {
    let env = Env::new();
    let daemon = env.start_daemon();
    let mut guard = env.leak_guard();
    guard.track_daemon(daemon.pid());

    let (a, b) = ("s-alice", "s-bob");

    // Both sessions go idle. The hooks register each inbox and arm each waiter —
    // the agents run nothing themselves.
    let mut arm_a = arm_idle(&env, a);
    let mut arm_b = arm_idle(&env, b);

    // --- A → B ---------------------------------------------------------------
    env.run_ok(
        &["send", b, "--text", "please review PR 42", "--session", a],
        "send a->b",
    );

    let status = wait_within(&mut arm_b, SETTLE).expect("B's waiter must wake");
    assert_eq!(status.code(), Some(2), "a peer message wakes B (exit 2)");
    let reminder = drain_stderr(&mut arm_b);
    assert!(
        reminder.contains(&format!("mail on topic agent.{b}")),
        "the wake names B's inbox topic and nothing else: {reminder:?}"
    );
    // Payload-free: the message text NEVER crosses the wake boundary.
    assert!(
        !reminder.contains("please review PR 42"),
        "the wake must not carry the body: {reminder:?}"
    );

    // B reads its mail and can see who to reply to.
    let events = env.read_events(b);
    assert_eq!(events.len(), 1, "B has exactly one message");
    assert_eq!(events[0]["topic"], format!("agent.{b}"));
    assert_eq!(events[0]["body"]["from"], a, "the sender is stamped");
    assert_eq!(events[0]["body"]["text"], "please review PR 42");

    // --- B → A (the reply) ----------------------------------------------------
    env.run_ok(
        &["send", a, "--text", "done, approved", "--session", b],
        "send b->a",
    );

    let status = wait_within(&mut arm_a, SETTLE).expect("A's waiter must wake");
    assert_eq!(status.code(), Some(2), "B's reply wakes A (exit 2)");
    assert!(
        drain_stderr(&mut arm_a).contains(&format!("mail on topic agent.{a}")),
        "A's wake names A's inbox"
    );

    let events = env.read_events(a);
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["body"]["from"], b);
    assert_eq!(events[0]["body"]["text"], "done, approved");

    // Teardown: both SessionEnds; nothing may survive.
    drop(arm_a);
    drop(arm_b);
    let _ = env.cleanup(a);
    let _ = env.cleanup(b);
    guard.assert_clean();
}

// ==== send to an unregistered agent fails loudly (baseline-on-subscribe) ========

/// A message to a session with no registered inbox could never be delivered — a
/// later subscribe would baseline past it — so `send` refuses instead of writing
/// into a void. The error names the unknown target, and nothing is published.
#[test]
fn send_to_an_unregistered_agent_fails_loudly_and_publishes_nothing() {
    let env = Env::new();
    let daemon = env.start_daemon();
    let mut guard = env.leak_guard();
    guard.track_daemon(daemon.pid());

    let out = env.run(&["send", "s-ghost", "--text", "hello?", "--session", "s-a"]);
    assert!(
        !out.status.success(),
        "sending to an unregistered agent must exit non-zero"
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("s-ghost"),
        "the error names the unknown target: {stderr}"
    );
    assert!(
        stderr.contains("no registered inbox"),
        "the error says WHY it failed: {stderr}"
    );

    // The refused send created no topic at all — so if the ghost ever registers,
    // there is no baselined-away message sitting in the log pretending to exist.
    let out = env.run_ok(&["--json", "topics"], "topics");
    let value: Value =
        serde_json::from_str(String::from_utf8_lossy(&out.stdout).trim()).expect("topics json");
    let topics = value["topics"].as_array().cloned().unwrap_or_default();
    assert!(
        !topics
            .iter()
            .any(|t| t["topic"] == Value::String("agent.s-ghost".to_string())),
        "a refused send must not create the inbox topic: {topics:?}"
    );

    guard.assert_clean();
}

/// The same refusal, in the case that actually bites: the target session EXISTS
/// (it has other subscriptions) but never registered an inbox — e.g. it is running
/// without the hooks installed. It is still not addressable, and `send` says so.
#[test]
fn send_to_a_session_without_an_inbox_still_fails() {
    let env = Env::new();
    let daemon = env.start_daemon();
    let mut guard = env.leak_guard();
    guard.track_daemon(daemon.pid());

    // A session that subscribed to something, but never armed (so never registered).
    env.run_ok(
        &["subscribe", "some.other.topic", "--session", "s-hookless"],
        "subscribe",
    );

    let out = env.run(&["send", "s-hookless", "--text", "hi", "--session", "s-a"]);
    assert!(
        !out.status.success(),
        "a session with no INBOX is not addressable, even though it has subscriptions"
    );
    assert!(agent_row(&env, "s-a", "s-hookless").is_none());

    guard.assert_clean();
}

// ==== FIX 2: an inbox is writable only via `send`, never a generic publish ======

/// The generic `publish` path must refuse an `agent.*` topic: it stamps no
/// provenance and runs no registration check, so allowing it would let any caller
/// forge a `from` into a victim's inbox (or write into an unregistered one, where
/// baseline-on-subscribe guarantees the message is unreadable). `send` — which
/// stamps the sender and checks the target — stays the only way in.
#[test]
fn generic_publish_to_an_inbox_topic_is_rejected() {
    let env = Env::new();
    let daemon = env.start_daemon();
    let mut guard = env.leak_guard();
    guard.track_daemon(daemon.pid());

    let b = "s-b";
    // B registers its inbox (a subscribe to its own inbox IS registration).
    env.run_ok(
        &["subscribe", &format!("agent.{b}"), "--session", b],
        "register",
    );

    // A forged publish straight into B's inbox is refused, non-zero, and points at
    // the sanctioned path.
    let out = env.run(&[
        "publish",
        &format!("agent.{b}"),
        "--body",
        r#"{"from":"attacker"}"#,
    ]);
    assert!(
        !out.status.success(),
        "publishing to an inbox topic must exit non-zero"
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("mailbox send"),
        "the error names the sanctioned path: {stderr}"
    );
    // The rejected publish wrote nothing into B's inbox.
    assert_eq!(
        env.unread_total(b),
        0,
        "a rejected publish delivers nothing"
    );

    // The sanctioned path is unaffected: a peer `send` still delivers.
    env.run_ok(&["send", b, "--text", "legit", "--session", "s-a"], "send");
    assert_eq!(
        env.unread_total(b),
        1,
        "send still delivers to a registered inbox"
    );

    guard.assert_clean();
}

// ==== FIX 1: the resurrection race — a re-registration after end is refused ======

/// After a session ends (cleanup → `EndSession` tombstones the id), a
/// re-registration landing within the guard window is REFUSED, so the dead session
/// is not resurrected in `agents` (ADR-0007). Driven through the CLI, deterministic
/// because everything happens far inside the 10s guard.
#[test]
fn a_reregistration_after_cleanup_does_not_resurrect_the_inbox() {
    let env = Env::new();
    let daemon = env.start_daemon();
    let mut guard = env.leak_guard();
    guard.track_daemon(daemon.pid());

    let b = "s-b";
    env.run_ok(
        &["subscribe", &format!("agent.{b}"), "--session", b],
        "register",
    );
    assert!(agent_row(&env, "s-a", b).is_some(), "B is registered");

    // SessionEnd drops the subscription and tombstones the id.
    let _ = env.cleanup(b);
    assert!(
        agent_row(&env, "s-a", b).is_none(),
        "an ended session is no longer an agent"
    );

    // A racing re-registration within the guard window is refused: it exits 0 (a
    // no-op, not an error) but creates no subscription.
    let out = env.run_ok(
        &["subscribe", &format!("agent.{b}"), "--session", b],
        "re-register",
    );
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(
        text.contains("refused"),
        "the re-register is refused: {text}"
    );
    assert!(
        agent_row(&env, "s-a", b).is_none(),
        "the dead session must not be resurrected as an agent"
    );

    guard.assert_clean();
}

// ==== discovery: agents (liveness + self) and topics ===========================

/// `agents` lists registered inboxes, marks the caller, and reports live-waiter
/// liveness honestly — live while the peer is idle on a waiter, not live once that
/// waiter is gone. Both the human and `--json` shapes are checked.
#[test]
fn agents_reports_registration_liveness_and_self() {
    let env = Env::new();
    let daemon = env.start_daemon();
    let mut guard = env.leak_guard();
    guard.track_daemon(daemon.pid());

    let (a, b) = ("s-a", "s-b");
    let arm_a = arm_idle(&env, a);
    let arm_b = arm_idle(&env, b);

    // --json: both registered, both live, and A is marked as itself.
    let rows = agents(&env, a);
    assert_eq!(rows.len(), 2, "both agents are listed: {rows:?}");
    let row_a = agent_row(&env, a, a).expect("A is listed");
    let row_b = agent_row(&env, a, b).expect("B is listed");
    assert_eq!(row_a["inbox"], format!("agent.{a}"));
    assert_eq!(row_a["is_self"], true);
    assert_eq!(row_b["is_self"], false, "B is not the caller");
    assert_eq!(row_a["live_waiter"], true, "A is idle on a live waiter");
    assert_eq!(row_b["live_waiter"], true, "B is idle on a live waiter");

    // human: names both agents, their inbox topics, and marks the caller.
    let out = env.run_ok(&["agents", "--session", a], "agents (human)");
    let text = String::from_utf8_lossy(&out.stdout).into_owned();
    assert!(text.contains("2 agent(s):"), "{text}");
    assert!(text.contains(&format!("inbox=agent.{a}")), "{text}");
    assert!(text.contains(&format!("inbox=agent.{b}")), "{text}");
    assert!(text.contains("<- you"), "the caller is marked: {text}");

    // Kill B's waiter (as a busy, mid-turn agent has none): B stays REGISTERED and
    // addressable, but is no longer reported as idle-and-listening.
    drop(arm_b);
    poll_until("B's waiter is gone", SETTLE, || {
        let row = agent_row(&env, a, b).expect("B stays registered");
        (row["live_waiter"] == Value::Bool(false)).then_some(())
    });
    assert!(
        agent_row(&env, a, b).is_some(),
        "a busy agent is still addressable"
    );

    drop(arm_a);
    let _ = env.cleanup(a);
    let _ = env.cleanup(b);
    guard.assert_clean();
}

/// `topics` lists every known topic — including one with subscribers but no events
/// (a fresh inbox) — with its counts, and `--prefix` filters.
#[test]
fn topics_reports_counts_and_filters_by_prefix() {
    let env = Env::new();
    let daemon = env.start_daemon();
    let mut guard = env.leak_guard();
    guard.track_daemon(daemon.pid());

    let (a, b) = ("s-a", "s-b");
    // Register two inboxes WITHOUT arming (subscribe is what registration is).
    for s in [a, b] {
        env.run_ok(
            &["subscribe", &format!("agent.{s}"), "--session", s],
            "register inbox",
        );
    }
    env.run_ok(&["subscribe", "team.ci", "--session", a], "subscribe");
    env.run_ok(&["send", b, "--text", "one", "--session", a], "send");
    env.run_ok(&["send", b, "--text", "two", "--session", a], "send");

    let out = env.run_ok(&["--json", "topics"], "topics");
    let value: Value =
        serde_json::from_str(String::from_utf8_lossy(&out.stdout).trim()).expect("topics json");
    let rows = value["topics"].as_array().cloned().unwrap_or_default();
    let names: Vec<&str> = rows.iter().filter_map(|t| t["topic"].as_str()).collect();
    assert_eq!(
        names,
        ["agent.s-a", "agent.s-b", "team.ci"],
        "every known topic, in topic order"
    );

    let inbox_b = &rows[1];
    assert_eq!(inbox_b["subscribers"], 1);
    assert_eq!(inbox_b["events"], 2, "both messages landed in B's inbox");
    assert!(
        inbox_b["last_event_ms"].as_i64().unwrap_or(0) > 0,
        "a topic with events carries its newest-event time: {inbox_b:?}"
    );

    // A topic with a subscriber and no traffic is listed honestly (this is the
    // state every fresh inbox is in — hiding it would hide who is addressable).
    let quiet = &rows[2];
    assert_eq!(quiet["topic"], "team.ci");
    assert_eq!(quiet["subscribers"], 1);
    assert_eq!(quiet["events"], 0);
    assert_eq!(quiet["last_event_ms"], Value::Null);

    // --prefix filters to a namespace.
    let out = env.run_ok(
        &["--json", "topics", "--prefix", "agent."],
        "topics --prefix",
    );
    let value: Value =
        serde_json::from_str(String::from_utf8_lossy(&out.stdout).trim()).expect("topics json");
    let names: Vec<String> = value["topics"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|t| t["topic"].as_str().map(str::to_string))
        .collect();
    assert_eq!(names, ["agent.s-a", "agent.s-b"]);

    // human output
    let out = env.run_ok(&["topics", "--prefix", "team."], "topics (human)");
    let text = String::from_utf8_lossy(&out.stdout).into_owned();
    assert!(text.contains("1 topic(s):"), "{text}");
    assert!(
        text.contains("team.ci  subscribers=1 events=0 last_event=-"),
        "a quiet topic shows no invented timestamp: {text}"
    );

    guard.assert_clean();
}

// ==== whoami / status surface the session's own address ========================

#[test]
fn whoami_and_status_surface_the_inbox_topic() {
    let env = Env::new();
    let daemon = env.start_daemon();
    let mut guard = env.leak_guard();
    guard.track_daemon(daemon.pid());

    let s = "s-me";

    // Before registration, `status` says plainly that peers cannot reach us.
    let out = env.run_ok(&["status", "--session", s], "status");
    let text = String::from_utf8_lossy(&out.stdout).into_owned();
    assert!(
        text.contains(&format!("inbox: agent.{s} (NOT registered")),
        "an unregistered session is told so: {text}"
    );

    let arm = arm_idle(&env, s);

    let out = env.run_ok(&["--json", "whoami", "--session", s], "whoami");
    let value: Value =
        serde_json::from_str(String::from_utf8_lossy(&out.stdout).trim()).expect("whoami json");
    assert_eq!(value["session"], s);
    assert_eq!(value["inbox_topic"], format!("agent.{s}"));

    let out = env.run_ok(&["status", "--session", s], "status");
    let text = String::from_utf8_lossy(&out.stdout).into_owned();
    assert!(
        text.contains(&format!("inbox: agent.{s} (registered)")),
        "an armed session's status shows its live address: {text}"
    );

    drop(arm);
    let _ = env.cleanup(s);
    guard.assert_clean();
}
