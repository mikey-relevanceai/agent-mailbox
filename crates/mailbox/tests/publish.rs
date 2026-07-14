//! Caller-aware publish: the rules that apply when the publisher is an AGENT, and
//! the rules that must NOT apply when it is an adapter.
//!
//! `publish` used to be session-less: it fanned a wake kick out to every subscriber,
//! including the publisher itself, so an agent subscribed to a topic it published to
//! **woke itself on its own message**. Threading the caller's session through makes
//! these rules expressible, and this file is where they are held:
//!
//! 1. **Be caught up to speak.** A publish is REFUSED (its own exit code, nothing
//!    written) if the caller is subscribed to the topic and has unread events on it
//!    **that someone else wrote**.
//! 2. **No self-wake.** The publisher is never kicked for its own event; a PEER
//!    subscriber is.
//! 3. **Its own event does not block its next publish** — the rule counts only what it
//!    did not author — **but it is never hidden from it either**: it stays unread, and
//!    `read` returns it. The publish used to advance the publisher's own cursor
//!    instead, and that was silent mail loss, because the "publisher" is inferred from
//!    `$CLAUDE_CODE_SESSION_ID`, which Claude Code exports into every process an agent
//!    spawns. `--no-session` is the explicit way for such a process to publish as
//!    nobody (no author → no rules, and it wakes EVERY subscriber).
//!
//! And the two contracts that must be untouched: an **adapter** publish (no session
//! anywhere) still kicks everyone and is exempt from rule 1, and **`send`** — which
//! writes to a *peer's* inbox, a topic the sender does not subscribe to — is
//! unaffected.
//!
//! Every test that spawns a waiter holds a [`LeakGuard`], so a leaked process fails
//! the test loudly rather than escaping into the runner.

mod common;

use std::time::Duration;

use common::{Env, drain_stderr, poll_until, wait_within};

/// How long to wait for a wake that SHOULD happen.
const WAKE: Duration = Duration::from_secs(10);
/// How long to watch a waiter that must NOT wake. A negative can only ever be
/// bounded — this is long enough that a kick (which is a synchronous FIFO write
/// inside the publish) would have landed many times over.
const NO_WAKE_GRACE: Duration = Duration::from_millis(1500);

/// Arm `session` (the `Stop` hook) and block until its waiter is really listening.
fn arm(env: &Env, session: &str) -> common::ArmChild {
    let child = env.spawn_arm(session, &["--max-block-ms", "60000"]);
    poll_until("the waiter arms", WAKE, || {
        env.waiter_pidfile(session).exists().then_some(())
    });
    child
}

// ==== rule 2: no self-wake; a peer IS woken ====================================

/// The headline publish bug: an agent subscribed to a topic it publishes to woke
/// ITSELF on its own message. The publisher must not be kicked — while a peer
/// subscribed to the same topic must be.
#[test]
fn a_publisher_is_not_woken_by_its_own_event_but_a_peer_subscriber_is() {
    let env = Env::new();
    let mut guard = env.leak_guard();
    let daemon = env.start_daemon();
    guard.track_daemon(daemon.pid());

    let topic = "team.standup";
    let publisher = "s-pub";
    let peer = "s-peer";
    env.run_ok(&["subscribe", topic, "--session", publisher], "sub pub");
    env.run_ok(&["subscribe", topic, "--session", peer], "sub peer");

    let mut publisher_waiter = arm(&env, publisher);
    let mut peer_waiter = arm(&env, peer);

    // The publisher speaks. It resolves its own session from the environment, exactly
    // as an agent's `mailbox publish` does (no --session flag anywhere).
    env.run_as_ok(
        publisher,
        &["publish", topic, "--body", r#"{"text":"standup at 10"}"#],
        "publish",
    );

    // The PEER wakes, naming the topic.
    let status = wait_within(&mut peer_waiter, WAKE).expect("the peer's waiter must wake");
    assert_eq!(status.code(), Some(2), "a peer subscriber must be kicked");
    assert!(
        drain_stderr(&mut peer_waiter).contains(&format!("mail on topic {topic}")),
        "the peer's wake must name the topic it has mail on"
    );

    // The PUBLISHER does not. It is mid-turn and knows what it just said.
    assert!(
        wait_within(&mut publisher_waiter, NO_WAKE_GRACE).is_none(),
        "the publisher must NOT be woken by its own event (no self-wake)"
    );

    // But its own event is NOT hidden from it. It used to be — the publish advanced the
    // publisher's own cursor — and that was silent mail loss, because the "publisher" is
    // inferred from an ambient env var that Claude Code exports into every process an
    // agent spawns. Nothing may mark an event read except a `read`.
    assert_eq!(
        env.unread_on(publisher, topic),
        1,
        "a publisher's own event stays VISIBLE to it (it just never wakes or blocks it)"
    );
    assert_eq!(
        env.unread_on(peer, topic),
        1,
        "the peer must have the event to read"
    );

    drop(publisher_waiter);
    drop(peer_waiter);
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
    let mut waiter = arm(&env, agent);
    env.publish(topic);
    let status = wait_within(&mut waiter, WAKE).expect("the subscriber's waiter must wake");
    assert_eq!(
        status.code(),
        Some(2),
        "a session-less (adapter) publish must kick every subscriber, as it always has"
    );
    assert!(drain_stderr(&mut waiter).contains(&format!("mail on topic {topic}")));

    drop(waiter);
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
    let mut waiter = arm(&env, agent);

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

    let status = wait_within(&mut waiter, WAKE)
        .expect("an anonymous publish must wake the subscriber, ambient session id or not");
    assert_eq!(status.code(), Some(2));
    assert!(
        drain_stderr(&mut waiter).contains(&format!("mail on topic {topic}")),
        "the wake must name the topic"
    );
    assert_eq!(env.unread_on(agent, topic), 1);

    drop(waiter);
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
    let mut alice_waiter = arm(&env, alice);
    let mut bob_waiter = arm(&env, bob);

    // Bob pokes Alice, so Alice now has unread on her OWN inbox.
    env.run_as_ok(bob, &["send", alice, "--text", "you up?"], "bob -> alice");
    let status = wait_within(&mut alice_waiter, WAKE).expect("alice must wake");
    assert_eq!(status.code(), Some(2), "a send must wake the recipient");
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
    let status = wait_within(&mut bob_waiter, WAKE).expect("bob must wake");
    assert_eq!(status.code(), Some(2), "the reply must wake bob");
    assert!(drain_stderr(&mut bob_waiter).contains(&format!("mail on topic agent.{bob}")));

    drop(alice_waiter);
    drop(bob_waiter);
    env.cleanup(alice);
    env.cleanup(bob);
    daemon.stop();
    guard.assert_clean();
}
