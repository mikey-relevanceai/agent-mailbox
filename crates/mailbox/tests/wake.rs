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
    let content = frame["message"]["content"]
        .as_str()
        .expect("string content");
    assert!(
        content.starts_with("[agent-mailbox]"),
        "the tag that tells the agent who woke it: {content}"
    );
    assert!(
        content.contains("stub.demo — 1 unread"),
        "the wake names the topic and what is waiting on it: {content}"
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

/// A wake points at mail; it never carries it (ADR-0022, over ADR-0001). The socket
/// COULD carry the event body — it must not, no matter what else the frame gained.
/// The body stays in the durable log until the agent's `read`.
#[test]
fn the_peer_frame_describes_the_mail_and_never_carries_the_event_body() {
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
            "--subject",
            "something changed",
            "--link",
            "https://example.com/the-thing",
        ],
        "publish with a body and a subject",
    );

    let frame = peer.next_frame(DELIVERY).expect("a wake should arrive");
    let wire = frame.to_string();
    assert!(
        !wire.contains("do-not-put-me-on-the-wake-wire"),
        "the event body must never cross the wake boundary: {wire}"
    );
    let content = frame["message"]["content"]
        .as_str()
        .expect("string content");
    assert!(content.contains("stub.demo — 1 unread"), "{content}");
    // What DOES cross: the one line the publisher wrote for this purpose, and the
    // link it points at.
    assert!(content.contains("· something changed"), "{content}");
    assert!(
        content.contains("https://example.com/the-thing"),
        "{content}"
    );

    // The body is still there to be read — a wake describes, `read` delivers.
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

// ==== the refusal: an agent is told it cannot be woken while it can still hear ====

/// `subscribe` and `watch` mean "tell me when this changes". If Claude Code gave this
/// session no inbox socket, that promise cannot be kept — and the ONE moment the agent
/// can be told is while it is still awake, asking. So the request fails loudly with the
/// remedy, rather than succeeding and leaving it to wait forever on a wake that will
/// never come.
#[test]
fn subscribe_refuses_for_a_session_that_nothing_could_wake() {
    let env = Env::new();
    let mut guard = env.leak_guard();
    let daemon = env.start_daemon();
    guard.track_daemon(daemon.pid());
    let session = "no-inbox";

    env.register_socketless(session);

    let out = env.run_as(session, &["subscribe", "stub.demo"]);

    assert!(
        !out.status.success(),
        "subscribing a session nothing can wake must FAIL, not silently succeed"
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("no Claude Code inbox socket"),
        "the failure must say what is wrong: {stderr}"
    );
    assert!(
        stderr.contains("Restart the session"),
        "and it must say what to do about it: {stderr}"
    );
}

/// The same gate on `watch`, which is the command whose entire purpose is being woken.
#[test]
fn watch_refuses_for_a_session_that_nothing_could_wake() {
    let env = Env::new();
    let mut guard = env.leak_guard();
    let daemon = env.start_daemon();
    guard.track_daemon(daemon.pid());
    let session = "no-inbox-watch";

    env.register_socketless(session);

    let out = env.run_as(session, &["watch", "stub", "demo"]);

    assert!(!out.status.success(), "watch must refuse too");
    guard.assert_clean();
}

/// A session with a socket is exactly as usable as before — the gate must not become
/// a tax on the working case.
#[test]
fn subscribe_succeeds_for_a_session_with_an_inbox() {
    let env = Env::new();
    let mut guard = env.leak_guard();
    let daemon = env.start_daemon();
    guard.track_daemon(daemon.pid());
    let session = "has-inbox";

    let _inbox = env.register_peer(session);

    env.run_as_ok(session, &["subscribe", "stub.demo"], "subscribe");
}

/// A session Claude Code has NOT registered is not necessarily unwakeable — it may be
/// a harness that is not Claude Code at all. Only an explicit "registered, and given no
/// socket" is unambiguous enough to refuse on, so an unknown session is let through.
#[test]
fn subscribe_allows_a_session_claude_code_has_never_heard_of() {
    let env = Env::new();
    let mut guard = env.leak_guard();
    let daemon = env.start_daemon();
    guard.track_daemon(daemon.pid());

    // No register_peer, no register_socketless: absent from the registry entirely.
    env.run_as_ok("stranger", &["subscribe", "stub.demo"], "subscribe");
}

// ==== `status` answers "can I be woken?" — the same verdict, from the same read ====
//
// ADR-0023. `status` is the command an agent runs to check itself, and it used to
// report only its inbox TOPIC — a fact about the bus — while `watch` refused on the
// inbox SOCKET. An agent read the two as contradicting each other, distrusted the
// refusal, and went back to polling.

/// The wake verdict out of `status --json`, which is the field a status line reads.
fn wake_of(out: &std::process::Output) -> String {
    let value: serde_json::Value = serde_json::from_str(&String::from_utf8_lossy(&out.stdout))
        .expect("status must emit one JSON document");
    value["wake"]
        .as_str()
        .unwrap_or_else(|| panic!("status --json must carry a wake verdict: {value}"))
        .to_string()
}

/// A session Claude Code bound a socket reads `reachable` — and the JSON keys that
/// were already there keep their exact names, because a Claude Code status line reads
/// this object on every prompt.
#[test]
fn status_reports_a_session_with_an_inbox_socket_as_reachable() {
    let env = Env::new();
    let mut guard = env.leak_guard();
    let daemon = env.start_daemon();
    guard.track_daemon(daemon.pid());
    let session = "status-has-inbox";

    let _inbox = env.register_peer(session);

    let out = env.run_as_ok(session, &["--json", "status"], "status");
    assert_eq!(wake_of(&out), "reachable");
    let value: serde_json::Value =
        serde_json::from_str(&String::from_utf8_lossy(&out.stdout)).expect("status json");
    assert_eq!(
        value["inbox"],
        format!("agent.{session}"),
        "the pre-existing `inbox` key must NOT be renamed: a status line reads it"
    );

    let human = env.run_as_ok(session, &["status"], "status");
    let text = String::from_utf8_lossy(&human.stdout);
    assert!(
        text.contains("wake: reachable"),
        "the human line must answer it too: {text}"
    );
    assert!(
        text.contains(&format!("inbox topic: agent.{session}")),
        "the topic line says `inbox topic`, so the two senses of `inbox` stop \
         colliding on screen: {text}"
    );
}

/// **The failure this whole change exists for.** A live session Claude Code gave no
/// socket is one nothing can wake, `watch` refuses it — and `status` used to reassure
/// it that everything was fine.
///
/// Asserted together, in one test, on one session: the point is not that each command
/// is individually right, it is that they cannot disagree.
#[test]
fn status_and_watch_agree_a_socketless_session_cannot_be_woken() {
    let env = Env::new();
    let mut guard = env.leak_guard();
    let daemon = env.start_daemon();
    guard.track_daemon(daemon.pid());
    let session = "status-no-inbox";

    env.register_socketless(session);

    let out = env.run_as_ok(session, &["--json", "status"], "status");
    assert_eq!(
        wake_of(&out),
        "no-inbox",
        "status must report the fault, not just the inbox topic"
    );

    let human = env.run_as_ok(session, &["status"], "status");
    let text = String::from_utf8_lossy(&human.stdout);
    assert!(
        text.contains("wake: no-inbox"),
        "the verdict leads the line: {text}"
    );
    assert!(
        text.contains("Restart the session"),
        "and carries the same remedy the refusal does: {text}"
    );

    let refused = env.run_as(session, &["watch", "stub", "demo"]);
    assert!(
        !refused.status.success(),
        "the command that refuses and the command that reports must agree"
    );
    guard.assert_clean();
}

/// **An unreadable registry is not evidence of `no-inbox`.** Absence of evidence is not
/// evidence of absence (ADR-0009): `status` must say `unknown` and nothing stronger,
/// exactly as `subscribe`/`watch` decline to refuse on it.
#[test]
fn status_reports_unknown_when_the_registry_cannot_be_read() {
    let env = Env::new();
    let mut guard = env.leak_guard();
    let daemon = env.start_daemon();
    guard.track_daemon(daemon.pid());
    let session = "status-unknown";

    // Deliberately NOT `run_as`, which always points at this env's sessions dir: the
    // case under test is a registry the CLI cannot read at all.
    let missing = env.db_path().parent().unwrap().join("no-such-sessions-dir");
    let out = common::mailbox_command()
        .args(["--json", "status"])
        .env("AGENT_MAILBOX_DB", env.db_path())
        .env("CLAUDE_CODE_SESSION_ID", session)
        .env("MAILBOX_CLAUDE_SESSIONS_DIR", &missing)
        .env("RUST_LOG", "error")
        .output()
        .expect("run status with an unreadable registry");

    assert!(
        out.status.success(),
        "an unreadable registry is not a failure of `status`"
    );
    assert_eq!(
        wake_of(&out),
        "unknown",
        "unknown must render as unknown, never as a verdict"
    );
}

/// The verdict is derived LOCALLY — `doctor` needs no daemon — so the session that most
/// needs the answer still gets it when the bridge is the broken thing.
#[test]
fn status_still_reports_the_wake_verdict_with_the_bridge_down() {
    let env = Env::new();
    let session = "status-no-bridge";

    // No `start_daemon`: this is the degraded path.
    env.register_socketless(session);

    let out = env.run_as(session, &["status"]);

    assert!(
        !out.status.success(),
        "the bridge being down is still a loud failure (ADR-0004)"
    );
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(
        text.contains("wake: no-inbox"),
        "the wake verdict belongs to the half that survives a dead bridge: {text}"
    );
    assert!(
        text.contains("bridge: UNREACHABLE"),
        "and it still says what it could not tell you: {text}"
    );

    let json = env.run_as(session, &["--json", "status"]);
    assert_eq!(
        wake_of(&json),
        "no-inbox",
        "the bridge-down document carries the verdict too"
    );
}
