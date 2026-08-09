//! Integration tests for the wake path: the `serve` daemon delivering a wake straight
//! onto a Claude Code session's inbox socket (ADR-0020/0021).
//!
//! Everything drives the real `mailbox serve` daemon in a tempdir. The Claude Code
//! sessions directory is ALWAYS a tempdir (`MAILBOX_CLAUDE_SESSIONS_DIR`, set by
//! [`common::Env`]) and every inbox socket is a fake bound by the test — so a test
//! can never read the developer's real `~/.claude/sessions`, and can never deliver a
//! wake onto a real session.
//!
//! This is now the ONLY wake path: the sentinel + `FileChanged` fallback was deleted
//! in ADR-0021, so there is nothing else for a session to be woken by.
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

/// The happy path, and the whole wake path: a subscriber with a bound inbox socket is
/// woken in ONE hop.
#[test]
fn a_subscriber_with_an_inbox_socket_is_woken() {
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
}

/// A subscriber with no inbox socket cannot be woken by anyone — there is no second
/// channel to fall back to (ADR-0021). The event must still be durable, so it
/// surfaces the moment that session reads; what must NOT happen is a silent claim
/// that it was delivered.
#[test]
fn a_subscriber_without_an_inbox_socket_is_not_woken_but_keeps_its_mail() {
    let env = Env::new();
    let mut guard = env.leak_guard();
    let daemon = env.start_daemon();
    guard.track_daemon(daemon.pid());
    let session = "peer-absent";
    let topic = "stub.demo";

    // Deliberately NO register_peer: this session is not in the registry at all.
    env.run_as_ok(session, &["subscribe", topic], "subscribe");

    env.publish(topic);

    // The mail is durable and readable, even though nothing could wake the session.
    let events = env.read_events(session);
    assert_eq!(
        events.len(),
        1,
        "an unwakeable session still receives its mail durably"
    );
    assert_eq!(events[0]["topic"], topic);
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
/// delivered to it. There is no anti-loop between the socket and the model: a frame IS
/// a model turn, so an unnecessary one is a real cost with nothing to catch it.
#[test]
fn a_subscriber_with_nothing_unread_is_not_woken() {
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
