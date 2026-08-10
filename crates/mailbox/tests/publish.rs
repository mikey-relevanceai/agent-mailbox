//! `publish`, end to end: **one rule, no exceptions** (ADR-0018).
//!
//! An event goes to the topic and wakes every subscriber, its author included.
//! There is no caller-aware behaviour left to test — no refusal, no author
//! suppression, no anonymous variant — so what this file holds is the absence of
//! those things as much as the presence of the rule:
//!
//! 1. A publisher is woken by its own event, exactly like a peer subscriber
//!    (ADR-0014), and the event stays in its `read` and its unread count.
//! 2. A subscriber that is hopelessly behind is still woken by the next publish,
//!    and the publisher is never blocked by anyone's unread state.
//! 3. Publishing NEVER marks anything read: only a `read` moves a cursor.
//!
//! Deleted with the rules they covered: the "be caught up to speak" refusal (and its
//! exit code 3), and the `--no-session` escape hatch that existed only to opt out of
//! being mis-attributed by the ambient `$CLAUDE_CODE_SESSION_ID`.
//!
//! `send` — which writes to a *peer's* inbox — is exercised in `tests/agents.rs`.
//!
//! Every test holds a [`LeakGuard`], so a leaked adapter process fails
//! the test loudly rather than escaping into the runner.

mod common;

use std::time::Duration;

use common::Env;

/// How long to wait for a wake that SHOULD happen.
const WAKE: Duration = Duration::from_secs(10);

/// Start `session` through the production `SessionStart` hook, which registers its
/// inbox so peers can address it.
fn arm(env: &Env, session: &str) {
    env.start_session(session);
}

/// Block until a wake naming `topic` lands on `inbox` — the wake wire: the daemon
/// writes that frame to the session's socket, and its arrival IS the turn.
fn assert_woken_for(inbox: &common::FakePeer, session: &str, topic: &str) {
    let frame = inbox
        .next_frame(WAKE)
        .unwrap_or_else(|| panic!("{session} must be woken for {topic}"));
    assert!(
        frame["message"]["content"]
            .as_str()
            .unwrap_or_default()
            .contains(topic),
        "{session}'s wake must name {topic}"
    );
}

/// An event wakes EVERY subscriber to its topic, including the session that published
/// it (ADR-0014). Authorship is not evidence the agent already knows: the same
/// transition arriving via `github-pr` carries no author and has always woken it, so
/// suppressing the attributable case only made the rule inconsistent.
#[test]
fn a_publisher_is_woken_by_its_own_event_just_like_any_peer_subscriber() {
    let env = Env::new();
    let mut guard = env.leak_guard();
    let daemon = env.start_daemon();
    guard.track_daemon(daemon.pid());

    let topic = "team.standup";
    let publisher = "s-pub";
    let peer = "s-peer";
    env.run_as_ok(publisher, &["subscribe", topic], "sub pub");
    env.run_as_ok(peer, &["subscribe", topic], "sub peer");

    arm(&env, publisher);
    arm(&env, peer);
    let inbox_publisher = env.register_peer(publisher);
    let inbox_peer = env.register_peer(peer);

    // The publisher speaks. It passes no session and needs none — `publish` resolves
    // nobody, because who is speaking changes nothing about where the event goes.
    env.run_as_ok(
        publisher,
        &["publish", topic, "--body", r#"{"text":"standup at 10"}"#],
        "publish",
    );

    // The PEER wakes, naming the topic.
    assert_woken_for(&inbox_peer, peer, topic);

    // ...and so does the PUBLISHER, on the same wire.
    assert_woken_for(&inbox_publisher, publisher, topic);

    // Its own event is visible to it. Nothing may mark an event read except a `read`.
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

/// **Nobody's read state may block anybody's write.** A session sitting on unread mail
/// can still publish to that very topic, as many times as it likes, and its own
/// messages remain fully readable back to it.
///
/// This is what replaced "be caught up to speak", which refused exactly this publish
/// (exit 3) until the caller ran `mailbox read`. That rule enforced a politeness norm
/// in the transport: it blocked a *write* because of the writer's *read* state, and it
/// decided who the writer was from the ambient `$CLAUDE_CODE_SESSION_ID` that Claude
/// Code exports into every process an agent spawns — so a build script was routinely
/// gagged by its parent agent's inbox.
#[test]
fn unread_mail_on_a_topic_never_blocks_publishing_to_it() {
    let env = Env::new();
    let daemon = env.start_daemon();

    let topic = "team.plan";
    let agent = "s-behind";
    env.run_as_ok(agent, &["subscribe", topic], "subscribe");

    // Someone else publishes. The agent is now behind on the topic.
    env.publish(topic);
    assert_eq!(env.unread_on(agent, topic), 1);

    for i in 0..3 {
        env.run_as_ok(
            agent,
            &["publish", topic, "--body", r#"{"text":"my turn"}"#],
            &format!("publish {i} while behind"),
        );
    }
    assert_eq!(env.event_count(topic), 4, "every publish landed");

    // ...and none of the four was hidden from the agent: its own words included, they
    // are all still unread, and `read` returns them.
    assert_eq!(
        env.unread_on(agent, topic),
        4,
        "publishing must never mark mail read — only a `read` may do that"
    );
    assert_eq!(env.read_events(agent).len(), 4);

    daemon.stop();
}

/// A subscriber that is far behind is still woken by the next publish. That is the
/// entire point of a mailbox: the bus does not gate delivery on how well the reader
/// is keeping up, and it never gated it on who was writing either.
#[test]
fn a_publish_wakes_a_subscriber_that_is_already_far_behind() {
    let env = Env::new();
    let mut guard = env.leak_guard();
    let daemon = env.start_daemon();
    guard.track_daemon(daemon.pid());

    let topic = "github.pr.o/r#7";
    let agent = "s-watcher";
    env.run_as_ok(agent, &["subscribe", topic], "subscribe");

    env.publish(topic);
    env.publish(topic);
    assert_eq!(env.unread_on(agent, topic), 2);

    arm(&env, agent);
    let inbox_agent = env.register_peer(agent);
    env.publish(topic);
    assert_woken_for(&inbox_agent, agent, topic);
    assert_eq!(env.unread_on(agent, topic), 3);

    env.cleanup(agent);
    daemon.stop();
    guard.assert_clean();
}

/// A publish has exactly two outcomes now: it worked (exit 0), or it could not reach
/// the bridge (exit 1). There is no third "refused, read and retry" code to tell apart
/// from a real failure — which is what made the refusal need its own exit code at all.
#[test]
fn a_publish_either_succeeds_or_fails_because_the_bridge_is_down() {
    let env = Env::new();
    let daemon = env.start_daemon();

    let topic = "team.codes";
    let agent = "s-codes";
    env.run_as_ok(agent, &["subscribe", topic], "subscribe");
    env.publish(topic); // the agent is behind — once grounds for a refusal.

    let out = env.run_as(agent, &["publish", topic]);
    assert_eq!(
        out.status.code(),
        Some(0),
        "a publish is never refused; stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    daemon.stop();
    let broken = env.run_as(agent, &["publish", topic]);
    assert_eq!(
        broken.status.code(),
        Some(1),
        "a genuine failure (bridge down) is exit 1"
    );
}
