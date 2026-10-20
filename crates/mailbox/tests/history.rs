//! `mailbox history` end to end (ADR-0032), driving the REAL binaries.
//!
//! Everything here runs against a live `mailbox serve` daemon in a tempdir and
//! real `mailbox` CLI client processes. Nothing is faked: the cursors these tests
//! assert about are moved by a real `read` against a real store.
//!
//! The headline is [`history_returns_mail_that_a_lost_read_already_consumed`] —
//! the failure the surface exists for. `read` advances the cursor in the same
//! transaction that returns the events, so a turn that dies after the commit has
//! consumed its mail without ever seeing it, and `status` then reports it as
//! caught up. Every other test here defends a property that recovery depends on,
//! above all [`history_advances_no_cursor`]: a look-back that consumed anything
//! would be a second `read` wearing a different name.

mod common;

use serde_json::Value;

use common::Env;

/// `mailbox history --json` as seen by `caller`, with the extra args appended.
fn history(env: &Env, caller: &str, args: &[&str]) -> Vec<Value> {
    let mut argv = vec!["--json", "history"];
    argv.extend_from_slice(args);
    let out = env.run_as_ok(caller, &argv, "history");
    let value: Value =
        serde_json::from_str(String::from_utf8_lossy(&out.stdout).trim()).expect("history json");
    value["events"].as_array().cloned().unwrap_or_default()
}

/// The same read with no session at all — a human at a terminal.
fn history_anon(env: &Env, args: &[&str]) -> Vec<Value> {
    let mut argv = vec!["--json", "history"];
    argv.extend_from_slice(args);
    let out = env.run_ok(&argv, "history (no session)");
    let value: Value =
        serde_json::from_str(String::from_utf8_lossy(&out.stdout).trim()).expect("history json");
    value["events"].as_array().cloned().unwrap_or_default()
}

/// Publish `body` to `topic` with a subject, so tests can tell events apart by
/// something more legible than an offset.
fn publish(env: &Env, topic: &str, body: &str, subject: &str) {
    env.run_ok(
        &["publish", topic, "--body", body, "--subject", subject],
        "publish",
    );
}

fn subjects(events: &[Value]) -> Vec<&str> {
    events
        .iter()
        .filter_map(|e| e["subject"]["text"].as_str())
        .collect()
}

// ==== the failure this surface exists for ====

/// A turn that dies after `read` commits has consumed its mail without seeing it,
/// and `status` reports zero unread — so nothing in the system says otherwise.
/// `history` is the only thing that can hand those events back, and it marks them
/// `read` so the agent can tell what it had already consumed.
#[test]
fn history_returns_mail_that_a_lost_read_already_consumed() {
    let env = Env::new();
    let daemon = env.start_daemon();
    let mut guard = env.leak_guard();
    guard.track_daemon(daemon.pid());

    let session = "s-a";
    env.run_as_ok(session, &["subscribe", "team.ci"], "subscribe");
    publish(&env, "team.ci", r#"{"n":1}"#, "build failed");
    publish(&env, "team.ci", r#"{"n":2}"#, "build fixed");

    // The read that "was lost": its cursor advance committed, its result did not
    // survive the turn.
    assert_eq!(env.read_events(session).len(), 2, "read drained both");
    assert_eq!(
        env.unread_total(session),
        0,
        "the session now looks caught up, which is exactly the trap"
    );

    let events = history(&env, session, &[]);
    assert_eq!(
        subjects(&events),
        ["build fixed", "build failed"],
        "both events come back, newest first"
    );
    assert!(
        events.iter().all(|e| e["read"] == Value::Bool(true)),
        "each is marked as already consumed: {events:?}"
    );
    // The bodies are the point — the wake only ever carried the subject.
    assert_eq!(events[0]["body"]["n"], 2);
    assert_eq!(events[1]["body"]["n"], 1);

    guard.assert_clean();
}

// ==== the property recovery depends on ====

/// History advances NOTHING. If it consumed, it would be a second delivery path
/// with a different name, and an agent that browsed would silently lose the mail
/// it was browsing. Asserted from both sides: the unread count is unchanged, and
/// a later `read` still delivers everything that was unread before.
#[test]
fn history_advances_no_cursor() {
    let env = Env::new();
    let daemon = env.start_daemon();
    let mut guard = env.leak_guard();
    guard.track_daemon(daemon.pid());

    let session = "s-a";
    env.run_as_ok(session, &["subscribe", "team.ci"], "subscribe");
    publish(&env, "team.ci", r#"{"n":1}"#, "one");
    publish(&env, "team.ci", r#"{"n":2}"#, "two");

    assert_eq!(env.unread_total(session), 2);
    let events = history(&env, session, &[]);
    assert_eq!(events.len(), 2, "history saw both");
    assert!(
        events.iter().all(|e| e["read"] == Value::Bool(false)),
        "and reported both as still unread: {events:?}"
    );

    assert_eq!(
        env.unread_total(session),
        2,
        "looking is not reading: the count is untouched"
    );
    assert_eq!(
        env.unread_on(session, "team.ci"),
        2,
        "per-topic count untouched too"
    );
    // The real proof: the events are still deliverable.
    assert_eq!(
        env.read_events(session).len(),
        2,
        "a later read still delivers everything history showed"
    );

    guard.assert_clean();
}

/// Read-state is per event, not per call: a session that has read some of a topic
/// sees exactly which ones. This is what makes a recovered turn actionable —
/// "these three I consumed, that fourth arrived after I died".
#[test]
fn history_marks_read_state_per_event() {
    let env = Env::new();
    let daemon = env.start_daemon();
    let mut guard = env.leak_guard();
    guard.track_daemon(daemon.pid());

    let session = "s-a";
    env.run_as_ok(session, &["subscribe", "team.ci"], "subscribe");
    publish(&env, "team.ci", r#"{"n":1}"#, "before the read");
    env.read_events(session);
    publish(&env, "team.ci", r#"{"n":2}"#, "after the read");

    let events = history(&env, session, &[]);
    assert_eq!(subjects(&events), ["after the read", "before the read"]);
    assert_eq!(events[0]["read"], Value::Bool(false), "{events:?}");
    assert_eq!(events[1]["read"], Value::Bool(true), "{events:?}");

    guard.assert_clean();
}

/// The default view spans every subscribed topic and orders them against each
/// other. Offsets cannot do this — they are per-topic, so "offset 0" on two topics
/// says nothing about which happened first — which is why the query orders by the
/// global row id instead.
#[test]
fn history_orders_newest_first_across_topics() {
    let env = Env::new();
    let daemon = env.start_daemon();
    let mut guard = env.leak_guard();
    guard.track_daemon(daemon.pid());

    let session = "s-a";
    env.run_as_ok(session, &["subscribe", "team.ci"], "subscribe");
    env.run_as_ok(session, &["subscribe", "team.deploy"], "subscribe");

    // Interleaved, so a per-topic ordering would visibly disagree with this.
    publish(&env, "team.ci", r#"{"n":1}"#, "ci first");
    publish(&env, "team.deploy", r#"{"n":2}"#, "deploy second");
    publish(&env, "team.ci", r#"{"n":3}"#, "ci third");

    let events = history(&env, session, &[]);
    assert_eq!(
        subjects(&events),
        ["ci third", "deploy second", "ci first"],
        "one newest-first sequence across both topics"
    );
    // Both topics really are represented (a same-topic ordering could pass above
    // by accident if one topic were silently dropped).
    assert_eq!(events[0]["topic"], "team.ci");
    assert_eq!(events[1]["topic"], "team.deploy");

    guard.assert_clean();
}

// ==== scope: mailboxes are not private, but read-state is not invented ====

/// Any topic can be browsed by name, another agent's inbox included — mailboxes
/// here are not private (ADR-0032). What the caller does NOT get is read-state:
/// it has no cursor on a topic it does not subscribe to, so `read` is absent
/// rather than guessed at. Saying "unread" there would be a claim about someone
/// else's cursor.
#[test]
fn history_browses_a_peer_inbox_but_withholds_its_read_state() {
    let env = Env::new();
    let daemon = env.start_daemon();
    let mut guard = env.leak_guard();
    guard.track_daemon(daemon.pid());

    let (owner, peer, onlooker) = ("s-owner", "s-peer", "s-onlooker");
    env.run_as_ok(owner, &["subscribe", "agent.s-owner"], "register inbox");
    env.run_as_ok(peer, &["send", owner, "--text", "please rebase"], "send");

    // The onlooker can read the mail...
    let seen = history(&env, onlooker, &["--topic", "agent.s-owner"]);
    assert_eq!(seen.len(), 1, "a peer inbox is readable: {seen:?}");
    assert_eq!(seen[0]["body"]["text"], "please rebase");
    assert_eq!(
        seen[0]["body"]["from"], "s-peer",
        "including who sent it, which is what makes it worth reading"
    );
    // ...but is told nothing about whether the OWNER has read it.
    assert!(
        seen[0].get("read").is_none(),
        "no subscription, no read-state — absent, never false: {seen:?}"
    );

    // The owner asking about its own inbox does get read-state.
    let mine = history(&env, owner, &["--topic", "agent.s-owner"]);
    assert_eq!(
        mine[0]["read"],
        Value::Bool(false),
        "the owner has a cursor here, and has not read it: {mine:?}"
    );

    guard.assert_clean();
}

/// A human at a terminal has no session, and that is a supported way to look at
/// what an agent was told — so `--topic` works with no identity at all, and
/// reports no read-state because there is no one to report it for.
#[test]
fn history_works_without_a_session_when_a_topic_is_named() {
    let env = Env::new();
    let daemon = env.start_daemon();
    let mut guard = env.leak_guard();
    guard.track_daemon(daemon.pid());

    publish(&env, "team.ci", r#"{"n":1}"#, "build failed");

    let events = history_anon(&env, &["--topic", "team.ci"]);
    assert_eq!(subjects(&events), ["build failed"]);
    assert!(
        events[0].get("read").is_none(),
        "nobody asked, so there is no read-state: {events:?}"
    );

    guard.assert_clean();
}

/// With neither a session nor a topic there is no set to read, and the bridge
/// refuses instead of picking one. "Every topic" and "no topics" are both guesses,
/// and a guess returned as a result is worse than a refusal that says what to pass.
#[test]
fn history_refuses_with_no_session_and_no_topic() {
    let env = Env::new();
    let daemon = env.start_daemon();
    let mut guard = env.leak_guard();
    guard.track_daemon(daemon.pid());

    let out = env.run(&["history"]);
    assert!(
        !out.status.success(),
        "should not exit 0 with nothing to scope it"
    );
    let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
    assert!(
        stderr.contains("--topic"),
        "the refusal must say what to pass: {stderr}"
    );

    guard.assert_clean();
}

// ==== bounding the window ====

/// `--limit` bounds the page from the NEWEST end, which is the end that matters
/// for "what did I just miss".
#[test]
fn history_limit_takes_the_newest_events() {
    let env = Env::new();
    let daemon = env.start_daemon();
    let mut guard = env.leak_guard();
    guard.track_daemon(daemon.pid());

    let session = "s-a";
    env.run_as_ok(session, &["subscribe", "team.ci"], "subscribe");
    for n in 1..=5 {
        publish(
            &env,
            "team.ci",
            &format!(r#"{{"n":{n}}}"#),
            &format!("e{n}"),
        );
    }

    let events = history(&env, session, &["--limit", "2"]);
    assert_eq!(
        subjects(&events),
        ["e5", "e4"],
        "the two newest, not the two oldest"
    );

    guard.assert_clean();
}

/// `--since` filters on the stored timestamp, so an older event falls out of a
/// narrow window while a newer one stays.
///
/// This is the one place a real sleep is warranted: wall-clock advancing IS the
/// thing under test, so there is no state to poll for. It is kept just past the
/// one-second boundary the assertion uses.
#[test]
fn history_since_excludes_events_older_than_the_window() {
    let env = Env::new();
    let daemon = env.start_daemon();
    let mut guard = env.leak_guard();
    guard.track_daemon(daemon.pid());

    let session = "s-a";
    env.run_as_ok(session, &["subscribe", "team.ci"], "subscribe");
    publish(&env, "team.ci", r#"{"n":1}"#, "old");
    std::thread::sleep(std::time::Duration::from_millis(1_200));
    publish(&env, "team.ci", r#"{"n":2}"#, "new");

    assert_eq!(
        subjects(&history(&env, session, &["--since", "1d"])),
        ["new", "old"],
        "a generous window keeps both"
    );
    assert_eq!(
        subjects(&history(&env, session, &["--since", "1s"])),
        ["new"],
        "a one-second window drops the older event"
    );

    guard.assert_clean();
}

// ==== human output ====

/// The human rendering has to answer two questions the JSON answers structurally:
/// which events were already consumed, and whether running this consumed them.
/// A command that prints mail looks exactly like one that just drained it.
#[test]
fn history_human_output_shows_read_state_and_says_it_consumed_nothing() {
    let env = Env::new();
    let daemon = env.start_daemon();
    let mut guard = env.leak_guard();
    guard.track_daemon(daemon.pid());

    let session = "s-a";
    env.run_as_ok(session, &["subscribe", "team.ci"], "subscribe");
    publish(&env, "team.ci", r#"{"n":1}"#, "already seen");
    env.read_events(session);
    publish(&env, "team.ci", r#"{"n":2}"#, "still waiting");

    let out = env.run_as_ok(session, &["history"], "history (human)");
    let text = String::from_utf8_lossy(&out.stdout).into_owned();
    assert!(
        text.contains("2 event(s), newest first — nothing was consumed:"),
        "{text}"
    );
    assert!(
        text.contains("unread [team.ci]") && text.contains("read   [team.ci]"),
        "each line is prefixed with its read-state: {text}"
    );
    assert!(
        text.contains("· still waiting") && text.contains("· already seen"),
        "subjects are shown under their events: {text}"
    );

    // The quiet case says the same thing rather than looking like a drained inbox.
    let out = env.run_as_ok(
        session,
        &["history", "--since", "1s", "--topic", "team.x"],
        "empty",
    );
    let text = String::from_utf8_lossy(&out.stdout).into_owned();
    assert!(text.contains("history never consumes anything"), "{text}");

    guard.assert_clean();
}
