//! Caller-aware publish: the rules that apply when the publisher is an AGENT, and
//! the rules that must NOT apply when it is an adapter.
//!
//! `publish` used to be session-less: it fanned a wake kick out to every subscriber,
//! including the publisher itself, so an agent subscribed to a topic it published to
//! **woke itself on its own message**. Threading the caller's session through makes
//! three rules expressible, and this file is where they are held:
//!
//! 1. **Be caught up to speak.** A publish is REFUSED (non-zero, nothing written) if
//!    the caller is subscribed to the topic and has unread events on it.
//! 2. **No self-wake.** The publisher is never kicked for its own event; a PEER
//!    subscriber is.
//! 3. **Its own event does not block its next publish** — the publisher's cursor
//!    advances past what it wrote, or rule 1 would deadlock it on its own message.
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

    // The PUBLISHER does not. It is mid-turn, it knows what it just said, and (by the
    // cursor advance below) it has nothing unread to read.
    assert!(
        wait_within(&mut publisher_waiter, NO_WAKE_GRACE).is_none(),
        "the publisher must NOT be woken by its own event (no self-wake)"
    );

    assert_eq!(
        env.unread_on(publisher, topic),
        0,
        "a publisher's own event must not count as unread against it"
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
    assert!(
        !out.status.success(),
        "publishing with unread on the topic must FAIL (be caught up to speak)"
    );
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(
        err.contains('1') && err.contains(topic) && err.contains("mailbox read"),
        "the refusal must name the count, the topic, and the command that fixes it; got: {err}"
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

/// The refusal must not deadlock the publisher on its OWN message: publishing
/// advances the publisher's cursor past what it wrote, so a second publish (and a
/// third) still goes through. Without that, rule 1 would make an agent's first
/// publish its last.
#[test]
fn a_publishers_own_event_never_blocks_its_next_publish() {
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
        assert_eq!(
            env.unread_on(agent, topic),
            0,
            "publish {i}: an agent's own event must never sit unread against it"
        );
    }
    assert_eq!(env.event_count(topic), 3, "all three publishes landed");

    // And it really has nothing to read — the cursor advanced, it did not just hide.
    let events = env.read_events(agent);
    assert!(
        events.is_empty(),
        "a publisher must not be handed back its own messages to read: {events:?}"
    );

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
