//! Integration tests for the PEER wake channel: the `serve` daemon delivering a
//! wake straight onto a Claude Code session's inbox socket (ADR-0020).
//!
//! Everything drives the real `mailbox serve` daemon in a tempdir. The Claude Code
//! sessions directory is ALWAYS a tempdir (`MAILBOX_CLAUDE_SESSIONS_DIR`, set by
//! [`common::Env`]) and every inbox socket is a fake bound by the test — so a test
//! can never read the developer's real `~/.claude/sessions`, and can never deliver a
//! wake onto a real session.
//!
//! # What is NOT covered headlessly
//!
//! That a delivered frame actually makes an idle Claude Code session take a turn,
//! and what its inbound permission gate decides (deliver / hold / refuse), both need
//! a live harness. Those were confirmed by hand against real sessions while ADR-0020
//! was written — including the full permission-class matrix — and are recorded there.
//! Here we prove every link up to and including "the right bytes reached the right
//! socket, and nothing else was written".

mod common;

use std::time::Duration;

use common::Env;

/// Generous enough for a loaded CI box, short enough that a genuine failure to
/// deliver does not stall the suite.
const DELIVERY: Duration = Duration::from_secs(10);

/// The happy path: a subscriber with a bound inbox socket is woken in ONE hop.
///
/// The sentinel assertion is half the point. Writing both channels would leave the
/// `FileChanged` hook firing for a session that has already taken its turn — a
/// second, redundant wake for one message.
#[test]
fn a_subscriber_with_an_inbox_socket_is_woken_on_the_peer_channel() {
    let env = Env::new();
    let mut guard = env.leak_guard();
    let daemon = env.start_daemon();
    guard.track_daemon(daemon.pid());
    let session = "peer-happy";
    let topic = "stub.demo";

    let peer = env.register_peer(session);
    env.run_as_ok(session, &["subscribe", topic], "subscribe");

    env.publish(topic);

    let frame = peer
        .next_frame(DELIVERY)
        .expect("the daemon should have delivered a wake onto the inbox socket");
    assert_eq!(frame["type"], "user");
    assert_eq!(frame["message"]["role"], "user");
    assert_eq!(
        frame["message"]["content"], "mail on topic stub.demo",
        "the wake names the topic and nothing else"
    );
    assert!(
        !env.sentinel_path(session).exists(),
        "a peer delivery must not ALSO write the sentinel, or the session wakes twice \
         for one message"
    );
}

/// Claude Code's `agents_cross_session_inbox` gate leaves most sessions with no
/// socket, and it cannot be turned on from outside Claude Code. Those sessions must
/// keep waking exactly as they did before ADR-0020.
#[test]
fn a_subscriber_without_an_inbox_socket_still_wakes_through_its_sentinel() {
    let env = Env::new();
    let mut guard = env.leak_guard();
    let daemon = env.start_daemon();
    guard.track_daemon(daemon.pid());
    let session = "peer-absent";
    let topic = "stub.demo";

    // Deliberately NO register_peer: this session is not in the registry at all.
    env.run_as_ok(session, &["subscribe", topic], "subscribe");

    env.publish(topic);

    assert_eq!(
        env.sentinel_topics(session),
        vec![topic.to_string()],
        "with no socket the sentinel must carry the wake, as it always did"
    );
}

/// Wake is payload-free (ADR-0001), and a change of transport must not quietly end
/// that. The socket COULD carry the event body; it must not. The body stays in the
/// durable log until the agent's `read`.
#[test]
fn the_peer_frame_carries_topic_names_and_never_the_event_body() {
    let env = Env::new();
    let mut guard = env.leak_guard();
    let daemon = env.start_daemon();
    guard.track_daemon(daemon.pid());
    let session = "peer-payload";
    let topic = "stub.demo";

    let peer = env.register_peer(session);
    env.run_as_ok(session, &["subscribe", topic], "subscribe");

    env.run_ok(
        &[
            "publish",
            topic,
            "--body",
            r#"{"secret":"do-not-put-me-on-the-wake-wire"}"#,
        ],
        "publish with a body",
    );

    let frame = peer.next_frame(DELIVERY).expect("a wake should arrive");
    let wire = frame.to_string();
    assert!(
        !wire.contains("do-not-put-me-on-the-wake-wire"),
        "the event body must never cross the wake boundary: {wire}"
    );
    assert_eq!(frame["message"]["content"], "mail on topic stub.demo");

    // The body is still there to be read — payload-free wake, durable payload.
    let read = env.run_as_ok(session, &["read"], "read");
    assert!(
        String::from_utf8_lossy(&read.stdout).contains("do-not-put-me-on-the-wake-wire"),
        "the body must survive in the durable log for the agent's read"
    );
}

/// A session subscribed to a DIFFERENT topic has nothing unread, so nothing may be
/// delivered to it. On the sentinel channel a stray write is harmless (the hook
/// re-checks the store and exits 0); on this channel a frame IS a model turn, so an
/// unnecessary one is a real cost with no second opinion to catch it.
#[test]
fn a_subscriber_with_nothing_unread_gets_no_peer_frame() {
    let env = Env::new();
    let mut guard = env.leak_guard();
    let daemon = env.start_daemon();
    guard.track_daemon(daemon.pid());
    let bystander = "peer-bystander";

    let peer = env.register_peer(bystander);
    env.run_as_ok(bystander, &["subscribe", "stub.other"], "subscribe");

    env.publish("stub.demo");

    peer.expect_silence(Duration::from_millis(500));
}
