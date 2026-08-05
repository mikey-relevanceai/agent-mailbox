//! Caller-aware publish: the rules that apply when the publisher is an AGENT, and
//! the rules that must NOT apply when it is an adapter.
//!
//! Threading the caller's session through `publish` makes these rules expressible,
//! and this file is where they are held:
//!
//! 1. **Be caught up to speak.** A publish is REFUSED (its own exit code, nothing
//!    written) if the caller is subscribed to the topic and has unread events on it
//!    **that someone else wrote**.
//! 2. **Every subscriber is woken, the publisher included** (ADR-0014). Authorship is
//!    provenance, not evidence of what the agent knows — the same transition arriving
//!    from `github-pr` has no author and has always woken it.
//! 3. **Its own event does not block its next publish** — rule 1 counts only what it
//!    did not author, or a first publish would be an agent's last — **and it is never
//!    hidden from it either**: it stays unread, and `read` returns it. The publish used
//!    to advance the publisher's own cursor instead, and that was silent mail loss,
//!    because the "publisher" is inferred from `$CLAUDE_CODE_SESSION_ID`, which Claude
//!    Code exports into every process an agent spawns. `--no-session` publishes as
//!    nobody, which additionally exempts the event from rule 1.
//!
//! And the two contracts that must be untouched: an **adapter** publish (no session
//! anywhere) is exempt from rule 1, and **`send`** — which writes to a *peer's* inbox,
//! a topic the sender does not subscribe to — is unaffected.
//!
//! Every test holds a [`LeakGuard`], so a leaked adapter process fails
//! the test loudly rather than escaping into the runner.

mod common;

use std::time::Duration;

use common::{Env, poll_until};

/// How long to wait for a wake that SHOULD happen.
const WAKE: Duration = Duration::from_secs(10);

/// Start `session` through the production `SessionStart` hook, which registers its
/// inbox and arms its wake sentinel.
fn arm(env: &Env, session: &str) {
    env.arm(session);
}

/// Block until `session`'s sentinel names `topic` — the wake wire: the daemon writes
/// the topic there and the `FileChanged` hook turns it into a wake.
fn assert_woken_for(env: &Env, session: &str, topic: &str) {
    poll_until(&format!("{session}'s sentinel names {topic}"), WAKE, || {
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

// ==== rule 2: every subscriber is woken, publisher included ====================

/// An event wakes EVERY subscriber to its topic, including the session that published
/// it (ADR-0014). Authorship is provenance, not evidence the agent already knows: the
/// same transition arriving via `github-pr` carries no author and has always woken it,
/// so suppressing the attributable case only made the rule inconsistent.
#[test]
fn a_publisher_is_woken_by_its_own_event_just_like_any_peer_subscriber() {
    let env = Env::new();
    let mut guard = env.leak_guard();
    let daemon = env.start_daemon();
    guard.track_daemon(daemon.pid());

    let topic = "team.standup";
    let publisher = "s-pub";
    let peer = "s-peer";
    env.run_ok(&["subscribe", topic, "--session", publisher], "sub pub");
    env.run_ok(&["subscribe", topic, "--session", peer], "sub peer");

    arm(&env, publisher);
    arm(&env, peer);

    // The publisher speaks. It resolves its own session from the environment, exactly
    // as an agent's `mailbox publish` does (no --session flag anywhere).
    env.run_as_ok(
        publisher,
        &["publish", topic, "--body", r#"{"text":"standup at 10"}"#],
        "publish",
    );

    // The PEER wakes, naming the topic.
    assert_woken_for(&env, peer, topic);

    // ...and so does the PUBLISHER, on the same wire.
    assert_woken_for(&env, publisher, topic);

    // Its own event is visible to it, as it always has been. Nothing may mark an event
    // read except a `read` — the publisher's cursor is NOT advanced by publishing.
    assert_eq!(
        env.unread_on(publisher, topic),
        1,
        "a publisher's own event stays VISIBLE to it (publishing never advances its cursor)"
    );
    assert_eq!(
        env.unread_on(peer, topic),
        1,
        "the peer must have the event to read"
    );

    env.cleanup(publisher);
    env.cleanup(peer);
    daemon.stop();
    guard.assert_clean();
}

// ==== rule 1: be caught up to speak ============================================

/// A caller with unread mail on a topic may not publish to it: it would be talking
/// past whatever it has not read. The refusal must be non-zero, name the count and
/// the topic, and — the load-bearing half — write NOTHING.
#[test]
fn publish_is_refused_when_the_caller_has_unread_on_that_topic() {
    let env = Env::new();
    let daemon = env.start_daemon();

    let topic = "team.plan";
    let agent = "s-behind";
    env.run_ok(&["subscribe", topic, "--session", agent], "subscribe");

    // Someone else (an adapter — no session) publishes. The agent now has unread.
    env.publish(topic);
    assert_eq!(env.unread_on(agent, topic), 1);
    let before = env.event_count(topic);

    let out = env.run_as(
        agent,
        &["publish", topic, "--body", r#"{"text":"my turn"}"#],
    );
    assert_eq!(
        out.status.code(),
        Some(3),
        "publishing with unread on the topic must be REFUSED, with the refusal's own exit code \
         (be caught up to speak)"
    );
    // The refusal is this command's RESULT, not an error, so it is rendered on stdout.
    let rendered = String::from_utf8_lossy(&out.stdout);
    assert!(
        rendered.contains('1') && rendered.contains(topic) && rendered.contains("mailbox read"),
        "the refusal must name the count, the topic, and the command that fixes it; got: {rendered}"
    );

    // Nothing was written: the log is exactly as it was.
    assert_eq!(
        env.event_count(topic),
        before,
        "a refused publish must not append an event"
    );

    // Reading clears the block, and the same publish then succeeds.
    env.run_as_ok(agent, &["read"], "read");
    assert_eq!(env.unread_on(agent, topic), 0);
    env.run_as_ok(
        agent,
        &["publish", topic, "--body", r#"{"text":"my turn"}"#],
        "publish after read",
    );
    assert_eq!(env.event_count(topic), before + 1);

    daemon.stop();
}

/// The refusal must not deadlock the publisher on its OWN message: a second publish
/// (and a third) still goes through. Without that, rule 1 would make an agent's first
/// publish its last.
///
/// The mechanism CHANGED (adv-2): the publish no longer advances the publisher's cursor
/// past its own event — that silently marked mail read, and the "publisher" is inferred
/// from an ambient env var, so it could be the wrong session entirely. Instead the rule
/// counts only events the caller did NOT author. Same property, nothing hidden: the
/// agent's own words remain fully visible to it.
#[test]
fn a_publishers_own_event_never_blocks_its_next_publish_but_stays_visible() {
    let env = Env::new();
    let daemon = env.start_daemon();

    let topic = "team.chatty";
    let agent = "s-chatty";
    env.run_ok(&["subscribe", topic, "--session", agent], "subscribe");

    for i in 0..3 {
        env.run_as_ok(
            agent,
            &["publish", topic, "--body", r#"{"n":1}"#],
            &format!("publish {i}"),
        );
    }
    assert_eq!(env.event_count(topic), 3, "all three publishes landed");

    // Its own events do not GAG it (the rule ignores what you wrote)...
    assert_eq!(
        env.unread_on(agent, topic),
        3,
        "...but they are not HIDDEN from it either: they count as unread, and `read` \
         returns them. Only a `read` may mark an event read."
    );
    let events = env.read_events(agent);
    assert_eq!(
        events.len(),
        3,
        "the agent's own messages are readable back to it: {events:?}"
    );

    daemon.stop();
}

/// **The anti-silent-loss test (adv-2).**
///
/// Claude Code exports `$CLAUDE_CODE_SESSION_ID` into EVERY process an agent spawns — a
/// build script, a git hook, a subagent. So such a process's `mailbox publish` is
/// attributed to the AGENT. That used to silently advance the agent's cursor past the
/// event AND exclude it from the kick: the agent never saw the message and was never
/// woken. Reproduced, and it is real message loss.
///
/// Now the event is stamped with its (mis-attributed) author but nothing is marked read:
/// it is STILL in the agent's `read` and STILL counted as unread. The residual, stated
/// honestly, is that it will not WAKE the session it was attributed to — which is what
/// `--no-session` exists to fix (see the test below).
#[test]
fn a_publish_carrying_an_ambient_session_id_is_still_visible_to_that_session() {
    let env = Env::new();
    let daemon = env.start_daemon();

    let topic = "team.build";
    let agent = "s-ambient";
    env.run_ok(&["subscribe", topic, "--session", agent], "subscribe");

    // A build script the agent spawned. It passes no --session and does not mean to
    // publish "as" the agent — it simply inherited the env var.
    env.run_as_ok(
        agent,
        &["publish", topic, "--body", r#"{"build":"failed"}"#],
        "a spawned script publishes with the agent's ambient session id",
    );

    assert_eq!(
        env.unread_on(agent, topic),
        1,
        "the message must NOT vanish: it is unread for the session it was mis-attributed to"
    );
    let events = env.read_events(agent);
    assert_eq!(
        events.len(),
        1,
        "and `read` must return it — a mis-attributed publish is not silently consumed: {events:?}"
    );
    assert_eq!(events[0]["body"]["build"], "failed");

    daemon.stop();
}

/// A session publishing to a topic it does NOT subscribe to is unaffected by the
/// unread rule on any OTHER topic it is behind on: the rule is per-topic ("be caught
/// up on what you are about to speak into"), not a global gag.
#[test]
fn unread_on_one_topic_does_not_block_publishing_to_another() {
    let env = Env::new();
    let daemon = env.start_daemon();

    let agent = "s-multi";
    env.run_ok(&["subscribe", "team.a", "--session", agent], "sub a");
    env.run_ok(&["subscribe", "team.b", "--session", agent], "sub b");

    env.publish("team.a"); // the agent is now behind on `team.a`...
    assert_eq!(env.unread_on(agent, "team.a"), 1);

    // ...which must not stop it speaking on `team.b`, where it is caught up.
    env.run_as_ok(
        agent,
        &["publish", "team.b"],
        "publish to the caught-up topic",
    );
    assert_eq!(env.event_count("team.b"), 1);

    daemon.stop();
}

// ==== the adapter contract is untouched ========================================

/// An adapter has no session anywhere (it is not a Claude Code session), so its
/// publish must behave exactly as it always did: it kicks EVERY subscriber, and the
/// unread rule does not apply to it — an adapter can publish into a topic whose
/// subscribers are hopelessly behind, which is the entire point of a mailbox.
#[test]
fn an_adapter_publish_has_no_session_kicks_everyone_and_ignores_the_unread_rule() {
    let env = Env::new();
    let mut guard = env.leak_guard();
    let daemon = env.start_daemon();
    guard.track_daemon(daemon.pid());

    let topic = "github.pr.o/r#7";
    let agent = "s-watcher";
    env.run_ok(&["subscribe", topic, "--session", agent], "subscribe");

    // Pile up unread for the agent — the state that would refuse the AGENT a publish.
    env.publish(topic);
    env.publish(topic);
    assert_eq!(
        env.unread_on(agent, topic),
        2,
        "an adapter publish is never refused, however far behind the subscribers are"
    );

    // A subscriber with unread still gets kicked by the next adapter publish.
    arm(&env, agent);
    env.publish(topic);
    assert_woken_for(&env, agent, topic);

    env.cleanup(agent);
    daemon.stop();
    guard.assert_clean();
}

// ==== `--no-session`: the escape hatch from the ambient-session trap ============

/// `--no-session` publishes ANONYMOUSLY: the event has no author, so it obeys no
/// caller-aware rule and kicks EVERY subscriber — including the agent whose
/// `$CLAUDE_CODE_SESSION_ID` this process inherited. That is what any script, hook or
/// subagent an agent spawns should use, and it is the complete answer to the
/// mis-attribution trap (the residual left by the test above).
#[test]
fn a_no_session_publish_wakes_every_subscriber_including_the_ambient_agent() {
    let env = Env::new();
    let mut guard = env.leak_guard();
    let daemon = env.start_daemon();
    guard.track_daemon(daemon.pid());

    let topic = "team.ci";
    let agent = "s-ci";
    env.run_ok(&["subscribe", topic, "--session", agent], "subscribe");
    arm(&env, agent);

    // The spawned script publishes with the agent's session id in its environment —
    // but says, explicitly, "this is not from the agent".
    env.run_as_ok(
        agent,
        &[
            "publish",
            topic,
            "--no-session",
            "--body",
            r#"{"ci":"red"}"#,
        ],
        "an anonymous publish from a process that inherited the agent's session id",
    );

    // An anonymous publish must wake the subscriber, ambient session id or not.
    assert_woken_for(&env, agent, topic);
    assert_eq!(env.unread_on(agent, topic), 1);

    env.cleanup(agent);
    daemon.stop();
    guard.assert_clean();
}

/// `--no-session` is also exempt from the unread rule (it is nobody's message, so
/// "be caught up to speak" cannot apply to it) — an adapter-shaped publish in every
/// respect, from inside a session's environment.
#[test]
fn a_no_session_publish_is_exempt_from_the_unread_rule() {
    let env = Env::new();
    let daemon = env.start_daemon();

    let topic = "team.exempt";
    let agent = "s-exempt";
    env.run_ok(&["subscribe", topic, "--session", agent], "subscribe");
    env.publish(topic); // the agent is now behind: a session publish would be REFUSED.
    assert_eq!(env.unread_on(agent, topic), 1);

    env.run_as_ok(
        agent,
        &["publish", topic, "--no-session"],
        "an anonymous publish is never refused",
    );
    assert_eq!(env.event_count(topic), 2);

    daemon.stop();
}

// ==== the refusal has its own exit code =========================================

/// A refusal is not a failure: nothing was written, nothing is broken, and the remedy
/// is defined ("read, then retry"). It used to exit 1 — the same code as "the bridge is
/// down" — so a scripted publisher could not tell them apart without parsing stderr.
/// It now has its own code (3, never 2: 2 is the WAKE code).
#[test]
fn a_refused_publish_exits_with_its_own_code_distinct_from_a_hard_failure() {
    let env = Env::new();
    let daemon = env.start_daemon();

    let topic = "team.codes";
    let agent = "s-codes";
    env.run_ok(&["subscribe", topic, "--session", agent], "subscribe");
    env.publish(topic); // the agent is behind.

    let refused = env.run_as(agent, &["publish", topic]);
    assert_eq!(
        refused.status.code(),
        Some(3),
        "a refusal has its own exit code, so a script can retry after reading; stderr: {}",
        String::from_utf8_lossy(&refused.stderr)
    );
    assert!(
        String::from_utf8_lossy(&refused.stdout).contains("mailbox read"),
        "the refusal must name the command that fixes it"
    );

    // A REAL failure is still exit 1, and is distinguishable: the bridge is down.
    daemon.stop();
    let broken = env.run_as(agent, &["publish", topic]);
    assert_eq!(
        broken.status.code(),
        Some(1),
        "a genuine failure (bridge down) stays exit 1 — the whole point of giving the \
         refusal its own code"
    );
}

// ==== `send` is unaffected ======================================================

/// `send` writes to a PEER's inbox — a topic the sender does not subscribe to — so
/// neither caller-aware rule can bite it: a sender that is behind on its OWN inbox
/// can still send, and the recipient is still woken. (If `send` were ever routed
/// through the session-aware publish on the recipient's topic, this would break —
/// which is exactly why it is asserted.)
#[test]
fn send_to_a_peer_inbox_is_unaffected_by_the_publisher_rules() {
    let env = Env::new();
    let mut guard = env.leak_guard();
    let daemon = env.start_daemon();
    guard.track_daemon(daemon.pid());

    let alice = "s-alice";
    let bob = "s-bob";
    // Arming registers each session's always-on inbox (ADR-0007), which is what makes
    // them addressable at all.
    arm(&env, alice);
    arm(&env, bob);

    // Bob pokes Alice, so Alice now has unread on her OWN inbox.
    env.run_as_ok(bob, &["send", alice, "--text", "you up?"], "bob -> alice");
    assert_woken_for(&env, alice, &format!("agent.{alice}"));
    assert_eq!(
        env.unread_on(alice, &format!("agent.{alice}")),
        1,
        "alice is behind on her own inbox"
    );

    // Alice — deliberately NOT reading first — can still send to Bob: his inbox is a
    // topic she does not subscribe to, so she is not "behind" on it.
    env.run_as_ok(
        alice,
        &["send", bob, "--text", "yes, replying"],
        "alice -> bob while behind on her own inbox",
    );
    assert_woken_for(&env, bob, &format!("agent.{bob}"));

    env.cleanup(alice);
    env.cleanup(bob);
    daemon.stop();
    guard.assert_clean();
}
