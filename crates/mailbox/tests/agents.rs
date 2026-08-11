//! Inter-agent messaging end to end (card 16), driving the REAL binaries.
//!
//! Everything here runs against a live `mailbox serve` daemon in a tempdir and
//! real `mailbox` CLI client processes, with the fake harness driver (hook JSON on
//! `harness session-start` / `wake` / `cleanup`'s stdin) standing in for Claude
//! Code — the same seams the card 11/12 suites use. No agent ever runs an arm
//! command, and no wake is simulated: the sentinels are written by the real daemon
//! and the wake decisions are real hook exits.
//!
//! The headline is [`round_trip_two_idle_agents_wake_each_other`]: two registered,
//! armed sessions message each other with no human in the loop.
//!
//! Every test scopes a [`LeakGuard`] to its own daemon subtree, so a leaked adapter
//! fails the test loudly rather than escaping into the runner.

mod common;

use std::time::Duration;

use serde_json::Value;

use common::{Env, FakeClaude, poll_until};

/// Generous bound for a real process to arm / wake / exit under CI load. Every
/// wait is a bounded poll, never a fixed sleep.
const SETTLE: Duration = Duration::from_secs(10);

/// Start `session` through the production `SessionStart` hook: register its inbox
/// (ADR-0007) and arm its wake sentinel. Both are synchronous, so when this returns
/// the session is genuinely addressable and wakeable.
fn arm_idle(env: &Env, session: &str) {
    env.start_session(session);
}

/// `mailbox agents --json` as seen by `caller`.
fn agents(env: &Env, caller: &str) -> Vec<Value> {
    let out = env.run_as_ok(caller, &["--json", "agents"], "agents");
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

/// A and B are both registered, and both have an inbox socket. A sends to B: the
/// daemon delivers to B's socket naming only the topic (and nothing else —
/// payload-free), B reads the message and sees `from: A`; B replies; A wakes the
/// same way.
#[test]
fn round_trip_two_idle_agents_wake_each_other() {
    let env = Env::new();
    let daemon = env.start_daemon();
    let mut guard = env.leak_guard();
    guard.track_daemon(daemon.pid());

    let (a, b) = ("s-alice", "s-bob");

    // Both sessions go idle. The hook registers each inbox; the agents run nothing
    // themselves. Each binds an inbox socket, which is what makes it wakeable.
    arm_idle(&env, a);
    arm_idle(&env, b);
    let inbox_a = env.register_peer(a);
    let inbox_b = env.register_peer(b);

    // --- A → B ---------------------------------------------------------------
    env.run_as_ok(
        a,
        &["send", b, "--text", "please review PR 42"],
        "send a->b",
    );

    // The daemon delivers to B's inbox socket, naming only the topic — that frame IS
    // the wake wire, so it is where payload-freeness has to hold.
    let frame = inbox_b.next_frame(SETTLE).expect("B is woken on its inbox");
    let content = frame["message"]["content"].as_str().unwrap_or_default();
    assert!(
        content.contains(&format!("agent.{b}")),
        "the wake names B's inbox topic: {content}"
    );
    assert!(
        !content.contains("please review PR 42"),
        "the wake must not carry the body: {content}"
    );

    // B reads its mail and can see who to reply to.
    let events = env.read_events(b);
    assert_eq!(events.len(), 1, "B has exactly one message");
    assert_eq!(events[0]["topic"], format!("agent.{b}"));
    assert_eq!(events[0]["body"]["from"], a, "the sender is stamped");
    assert_eq!(events[0]["body"]["text"], "please review PR 42");

    // --- B → A (the reply) ----------------------------------------------------
    env.run_as_ok(b, &["send", a, "--text", "done, approved"], "send b->a");

    let frame = inbox_a
        .next_frame(SETTLE)
        .expect("B's reply wakes A on its inbox");
    assert!(
        frame["message"]["content"]
            .as_str()
            .unwrap_or_default()
            .contains(&format!("agent.{a}")),
        "the reply's wake names A's inbox topic"
    );

    let events = env.read_events(a);
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["body"]["from"], b);
    assert_eq!(events[0]["body"]["text"], "done, approved");

    // Teardown: both SessionEnds; nothing may survive.
    let _ = env.cleanup(a);
    let _ = env.cleanup(b);
    guard.assert_clean();
}

// ==== the human manual poke: no session in the environment at all ==============

/// A HUMAN in an ordinary terminal — no `$CLAUDE_CODE_SESSION_ID` anywhere — can
/// look at the fleet and poke an agent. Both commands ran through `Env::run`, which
/// strips the variable, so this is the literal `env -u CLAUDE_CODE_SESSION_ID` case.
///
/// This is a regression guard. Making `$CLAUDE_CODE_SESSION_ID` the single source of
/// a session's identity was right for the session-scoped commands, but it was applied
/// to `agents` and `send` too, and both then refused to run outside a Claude Code
/// session — advising the human to invent a `CLAUDE_CODE_SESSION_ID`, which for these
/// two commands is nonsense. Neither needs an identity to do its job: `agents` only
/// marks which row is the caller, and `send` only stamps a reply address.
#[test]
fn a_human_with_no_session_can_list_agents_and_poke_one() {
    let env = Env::new();
    let daemon = env.start_daemon();
    let mut guard = env.leak_guard();
    guard.track_daemon(daemon.pid());

    let b = "s-bob";
    arm_idle(&env, b);
    let inbox_b = env.register_peer(b);

    // 1. Discovery, with no caller: the agent is listed, and NO row is marked self.
    let out = env.run_ok(&["--json", "agents"], "agents with no session");
    let value: Value =
        serde_json::from_str(String::from_utf8_lossy(&out.stdout).trim()).expect("agents json");
    let rows = value["agents"].as_array().cloned().unwrap_or_default();
    assert_eq!(rows.len(), 1, "the registered agent is listed: {rows:?}");
    assert_eq!(rows[0]["session"], b);
    assert_eq!(rows[0]["inbox"], format!("agent.{b}"));
    assert!(
        rows.iter().all(|r| r["is_self"] == Value::Bool(false)),
        "with no caller there is no self to mark: {rows:?}"
    );

    let out = env.run_ok(&["agents"], "agents (human, no session)");
    let text = String::from_utf8_lossy(&out.stdout).into_owned();
    assert!(text.contains("1 agent(s):"), "{text}");
    assert!(text.contains(&format!("inbox=agent.{b}")), "{text}");
    assert!(
        !text.contains("<- you"),
        "nobody may be marked as the caller when there is no caller: {text}"
    );

    // 2. The poke itself lands, and the sender is told it carries no reply address.
    let out = env.run_ok(&["send", b, "--text", "please rebase"], "send, no session");
    let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
    assert!(
        stderr.contains("no `from`"),
        "the sender is told the message has no reply address: {stderr}"
    );

    // 3. It wakes B exactly like a peer's message does — the wake path is unchanged.
    let frame = inbox_b
        .next_frame(SETTLE)
        .expect("a human's message wakes B like any other");
    assert!(
        frame["message"]["content"]
            .as_str()
            .unwrap_or_default()
            .contains(&format!("agent.{b}")),
        "the wake names B's inbox topic"
    );

    // 4. B reads it. The content is there; `from` is ABSENT, not null and not a
    //    placeholder — which is how B knows there is nobody to reply to.
    let events = env.read_events(b);
    assert_eq!(events.len(), 1, "B has exactly one message");
    assert_eq!(events[0]["body"]["text"], "please rebase");
    assert!(
        events[0]["body"].get("from").is_none(),
        "a human's message must carry no `from` key at all: {}",
        events[0]["body"]
    );

    let _ = env.cleanup(b);
    guard.assert_clean();
}

/// The refusal that DOES survive with no session: an unregistered target is still a
/// hard error. Tolerating a missing caller is not tolerating an undeliverable send.
#[test]
fn a_human_send_to_an_unregistered_agent_still_fails() {
    let env = Env::new();
    let daemon = env.start_daemon();
    let mut guard = env.leak_guard();
    guard.track_daemon(daemon.pid());

    let out = env.run(&["send", "s-ghost", "--text", "hello?"]);
    assert!(
        !out.status.success(),
        "a human's send to an unregistered agent must still exit non-zero"
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("no registered inbox"),
        "the same actionable error a peer gets: {stderr}"
    );

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

    let out = env.run_as("s-a", &["send", "s-ghost", "--text", "hello?"]);
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
    // Regression guard: the message must NOT frame a registration failure as an
    // IDENTITY failure. The old "unknown agent" wording led a real peer agent to
    // conclude a resumed coordinator had come back with a new session id (it had
    // not — only its registration had lapsed) and to waste ~20 minutes on that
    // false theory. The id may be perfectly correct, and the text must say so.
    assert!(
        !stderr.to_lowercase().contains("unknown agent"),
        "the error must not imply the session id is unknown/stale: {stderr}"
    );
    assert!(
        stderr.contains("does NOT mean the session id is wrong"),
        "the error must rule out the stale-id misreading explicitly: {stderr}"
    );
    assert!(
        stderr.contains("DROPPED, not queued"),
        "the error must say the message was dropped rather than queued: {stderr}"
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
    env.run_as_ok(
        "s-hookless",
        &["subscribe", "some.other.topic"],
        "subscribe",
    );

    let out = env.run_as("s-a", &["send", "s-hookless", "--text", "hi"]);
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
    env.run_as_ok(b, &["subscribe", &format!("agent.{b}")], "register");

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
    env.run_as_ok("s-a", &["send", b, "--text", "legit"], "send");
    assert_eq!(
        env.unread_total(b),
        1,
        "send still delivers to a registered inbox"
    );

    guard.assert_clean();
}

// ==== FIX 1: the resurrection race — a re-registration after end is refused ======

/// After a session ends (cleanup → `EndSession` tombstones the id), the AUTOMATIC
/// inbox re-registration that `harness arm` fires — the ONLY path that can race the
/// teardown — is REFUSED within the guard window, so the dead session is not
/// resurrected in `agents` (ADR-0007). Driven through the real `harness arm`, which
/// is what performs the guarded auto-registration; deterministic because everything
/// happens far inside the 10s guard.
#[test]
fn a_racing_auto_reregistration_after_cleanup_does_not_resurrect_the_inbox() {
    let env = Env::new();
    let daemon = env.start_daemon();
    let mut guard = env.leak_guard();
    guard.track_daemon(daemon.pid());

    let b = "s-b";
    // B registers + arms via the real hook (the auto-inbox path), then goes idle.
    arm_idle(&env, b);
    assert!(agent_row(&env, "s-a", b).is_some(), "B is registered");

    // SessionEnd removes the sentinel, drops the subscription, and tombstones the id.
    let _ = env.cleanup(b);
    assert!(
        agent_row(&env, "s-a", b).is_none(),
        "an ended session is no longer an agent"
    );

    // A racing SessionStart within the guard window is refused: the inbox subscribe
    // is rejected, so the session is not resurrected as an agent. (The sentinel is
    // still written — it is a file, not a claim of registration — and `SessionEnd`
    // has already removed the directory once; a doomed re-arm leaves nothing but a
    // path `mailbox doctor` will report as never having answered.)
    let out = env.session_start(b);
    assert!(out.status.success(), "session-start still exits 0");
    assert!(
        agent_row(&env, "s-a", b).is_none(),
        "the dead session must not be resurrected as an agent"
    );

    guard.assert_clean();
}

/// The fix, end to end: a session ends (tombstone written), then the SAME id
/// genuinely resumes and issues an EXPLICIT `mailbox subscribe` WITHIN the guard
/// window. Unlike the guarded auto-registration above, the explicit subscribe
/// PROCEEDS (proof-of-life) — the subscription is created and a subsequently
/// published event is delivered to that session's read cursor. Driven through the
/// real CLI + daemon; deterministic because it stays far inside the 10s guard.
#[test]
fn an_explicit_subscribe_by_a_resumed_session_within_guard_delivers() {
    let env = Env::new();
    let daemon = env.start_daemon();
    let mut guard = env.leak_guard();
    guard.track_daemon(daemon.pid());

    let b = "s-resumed";
    let topic = "team.updates";
    // B subscribes explicitly, then its session ends (tombstone written).
    env.run_as_ok(b, &["subscribe", topic], "subscribe");
    let _ = env.cleanup(b);

    // Within the guard window the resumed session re-subscribes explicitly. It must
    // NOT be refused (that is the whole bug): an explicit subscribe is a live-turn
    // action, never the doomed post-teardown arm.
    let out = env.run_as_ok(b, &["subscribe", topic], "explicit re-subscribe");
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(
        !text.contains("refused"),
        "an explicit subscribe by a resumed session must proceed, not be refused: {text}"
    );
    assert!(
        env.subscriptions(b).iter().any(|t| t == topic),
        "the resumed session's subscription was created"
    );

    // A publish after the resume lands and is delivered to B's cursor — proving the
    // subscription is real, not a silently-dropped no-op.
    env.publish(topic);
    let events = env.read_events(b);
    assert_eq!(
        events.len(),
        1,
        "the resumed session receives the post-resume event: {events:?}"
    );
    assert_eq!(events[0]["topic"], topic);

    guard.assert_clean();
}

/// Publish-namespace injectivity at the publish layer: three near-miss topics that
/// look inbox-ish but address NO registerable inbox are ordinary topics — a generic
/// `publish` to each is ALLOWED (harmless) — while the exact `agent.<valid-session>`
/// form stays REJECTED (writable only via `send`). This pins the load-bearing
/// injectivity of the inbox mapping.
#[test]
fn publish_namespace_near_misses_are_allowed_but_a_real_inbox_is_rejected() {
    let env = Env::new();
    let daemon = env.start_daemon();
    let mut guard = env.leak_guard();
    guard.track_daemon(daemon.pid());

    // None of these is a registerable inbox, so each is an ordinary topic a generic
    // publish may write to:
    //  - `agent.agent.x`: the session segment would itself start with `agent.`, a
    //    form `inbox_topic` refuses to mint (injectivity), so it is not an inbox.
    //  - `agent.`:        an empty session segment is never a valid inbox.
    //  - `Agent.x`:       the `agent.` namespace is case-sensitive; this is not it.
    for topic in ["agent.agent.x", "agent.", "Agent.x"] {
        env.run_ok(&["publish", topic], "publish near-miss");
    }

    // But the exact `agent.<valid-session>` form IS an inbox and stays rejected on
    // the generic publish path (only `send` may write it).
    let out = env.run(&["publish", "agent.s-real", "--body", r#"{"from":"x"}"#]);
    assert!(
        !out.status.success(),
        "a real inbox topic must be rejected on generic publish"
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("mailbox send"),
        "the rejection names the sanctioned path: {stderr}"
    );

    guard.assert_clean();
}

// ==== discovery: agents (liveness + self) and topics ===========================

/// `agents` lists registered inboxes, marks the caller, and reports liveness
/// honestly — live while the peer's Claude Code process exists, not live once it has
/// exited. Both the human and `--json` shapes are checked.
///
/// Liveness is driven with real processes, not a planted file: the bridge reads it
/// from the process table, so anything a test could plant would be testing a
/// different mechanism than production uses.
#[test]
fn agents_reports_registration_liveness_and_self() {
    let env = Env::new();
    let daemon = env.start_daemon();
    let mut guard = env.leak_guard();
    guard.track_daemon(daemon.pid());

    let (a, b) = ("agent-alpha", "agent-bravo");
    arm_idle(&env, a);
    arm_idle(&env, b);
    let claude_a = FakeClaude::running(&env.sessions_dir(), a, None);
    let claude_b = FakeClaude::running(&env.sessions_dir(), b, None);

    // --json: both registered, both live, and A is marked as itself.
    let rows = agents(&env, a);
    assert_eq!(rows.len(), 2, "both agents are listed: {rows:?}");
    let row_a = agent_row(&env, a, a).expect("A is listed");
    let row_b = agent_row(&env, a, b).expect("B is listed");
    assert_eq!(row_a["inbox"], format!("agent.{a}"));
    assert_eq!(row_a["is_self"], true);
    assert_eq!(row_b["is_self"], false, "B is not the caller");
    assert_eq!(row_a["live"], true, "A's agent process is running");
    assert_eq!(row_b["live"], true, "B's agent process is running");

    // human: names both agents, their inbox topics, and marks the caller.
    let out = env.run_as_ok(a, &["agents"], "agents (human)");
    let text = String::from_utf8_lossy(&out.stdout).into_owned();
    assert!(text.contains("2 agent(s):"), "{text}");
    assert!(text.contains(&format!("inbox=agent.{a}")), "{text}");
    assert!(text.contains(&format!("inbox=agent.{b}")), "{text}");
    assert!(text.contains("<- you"), "the caller is marked: {text}");

    // B's agent exits (a closed terminal, a killed process — no `SessionEnd` runs).
    // B stays REGISTERED and addressable; it is simply no longer running.
    claude_b.stop(b);
    poll_until("B's agent process is gone", SETTLE, || {
        let row = agent_row(&env, a, b).expect("B stays registered");
        (row["live"] == Value::Bool(false)).then_some(())
    });
    assert!(
        agent_row(&env, a, b).is_some(),
        "an agent that has exited is still addressable"
    );
    assert_eq!(
        agent_row(&env, a, a).expect("A is listed")["live"],
        Value::Bool(true),
        "A is unaffected by B exiting"
    );

    claude_a.stop(a);
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
        env.run_as_ok(s, &["subscribe", &format!("agent.{s}")], "register inbox");
    }
    env.run_as_ok(a, &["subscribe", "team.ci"], "subscribe");
    env.run_as_ok(a, &["send", b, "--text", "one"], "send");
    env.run_as_ok(a, &["send", b, "--text", "two"], "send");

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

// ==== status surfaces the session's own address ================================

#[test]
fn status_surfaces_this_sessions_own_address() {
    let env = Env::new();
    let daemon = env.start_daemon();
    let mut guard = env.leak_guard();
    guard.track_daemon(daemon.pid());

    let s = "s-me";

    // Before registration, `status` says plainly that peers cannot reach us.
    let out = env.run_as_ok(s, &["status"], "status");
    let text = String::from_utf8_lossy(&out.stdout).into_owned();
    assert!(
        text.contains(&format!("inbox topic: agent.{s} (NOT registered")),
        "an unregistered session is told so: {text}"
    );

    arm_idle(&env, s);

    let out = env.run_as_ok(s, &["--json", "status"], "status");
    let value: Value =
        serde_json::from_str(String::from_utf8_lossy(&out.stdout).trim()).expect("status json");
    assert_eq!(value["session"], s);
    assert_eq!(value["inbox"], format!("agent.{s}"));

    let out = env.run_as_ok(s, &["status"], "status");
    let text = String::from_utf8_lossy(&out.stdout).into_owned();
    assert!(
        text.contains(&format!("inbox topic: agent.{s} (registered)")),
        "an armed session's status shows its live address: {text}"
    );

    let _ = env.cleanup(s);
    guard.assert_clean();
}
