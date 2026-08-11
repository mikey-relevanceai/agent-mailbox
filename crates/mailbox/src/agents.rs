//! Inter-agent messaging: agent inboxes, discovery, and the `send` verb.
//!
//! # What an agent inbox is
//!
//! Every live session registers a subscription to its own `agent.<session-id>`
//! topic (`mailbox_protocol::inbox_topic`). The harness does this on every
//! `SessionStart`/`Stop` — always-on, not opt-in (ADR-0007) — so an agent is
//! addressable by its peers from the moment it exists, without anyone switching
//! into its session to arrange it.
//!
//! Nothing about the bus changes: an inbox is an ordinary topic, a `send` is an
//! ordinary publish, and the wake it produces is the ordinary one — the recipient
//! is told a message arrived and who from, never what it says (see
//! [`message_subject`]). This module is the thin policy that makes those primitives
//! usable as agent addressing.
//!
//! # Why `send` to an unregistered agent is an ERROR, not a publish
//!
//! Baseline-on-subscribe (see [`crate::bus`]) means a fresh subscription starts
//! at the topic head: events published *before* it subscribed are baselined away
//! and never delivered. So a message published to a session that has NOT
//! registered an inbox is **guaranteed undeliverable** — if that session later
//! registers, its baseline skips exactly the message we just wrote. Publishing it
//! anyway would durably store a message no one can ever read while telling the
//! sender it succeeded. [`send`] therefore refuses ([`SendError::InboxNotRegistered`])
//! rather than write into a void, and there is deliberately no `--force`.
//!
//! # Trust
//!
//! Any local same-user process can publish to any inbox. That is the accepted
//! trust boundary (one user, one machine — ADR-0007). The `from` stamp is
//! *provenance*, not authority: a message body remains untrusted data and must
//! never be treated as an instruction to obey (ADR-0001).
//!
//! # Why a message may have NO `from`
//!
//! `from` is a **reply address**, not a permission and not a requirement. A sender
//! that is itself a session has one, and it is stamped. A HUMAN running `mailbox
//! send` in an ordinary terminal has no session id and therefore no address to reply
//! to — and refusing that send would mean losing the manual poke rather than losing a
//! courtesy. So such a message is delivered with the [`FROM_FIELD`] key **absent**.
//!
//! Absent, not `null` and not a placeholder: a reader asking "can I reply?" then gets
//! its answer from whether the key exists, and every placeholder we could invent
//! ("human", "-", "") is a string that `mailbox send` would happily accept as a
//! target and fail on. A receiving agent must therefore treat `from` as optional:
//! when it is missing, act on the content and do not try to reply.

use std::collections::BTreeSet;

use serde_json::{Map, Value};
use tracing::{info, warn};

use mailbox_protocol::{AdapterId, Event, Subject, Timestamp, Topic, TopicError, inbox_topic};

use crate::bus::{Bus, BusError};
use crate::clock::now_millis;
use crate::storage::{SessionId, Storage, StorageError};

/// The body key carrying the sender's session id. The receiver replies by
/// `send`ing back to this value, so it is a documented part of the (bus-opaque)
/// message convention — see `docs/04-usage.md`.
pub const FROM_FIELD: &str = "from";

/// The provenance label a peer-to-peer message is published under. Adapter ids
/// are labels, not authority (ADR-0001); this one says "a local agent wrote
/// this", and the sender's identity travels in the [`FROM_FIELD`] stamp.
const AGENT_ADAPTER: &str = "agent";

/// Why a [`send`] could not be delivered.
#[derive(Debug, thiserror::Error)]
pub enum SendError {
    /// The target has no registered inbox, so the message could never be
    /// delivered (see the module docs). Names the target so the error is
    /// actionable.
    ///
    /// # The wording is load-bearing: this is NOT "the id is wrong"
    ///
    /// This used to read "unknown agent {id}", and the variant was named
    /// `UnknownAgent`. Both framed a *registration* failure as an *identity*
    /// failure, and it actively misled a real agent: a peer that could not reach a
    /// resumed coordinator concluded the coordinator "came back with a NEW session
    /// id" and spent ~20 minutes retrying against that false theory. The id was
    /// stable the whole time; only the registration was missing. So the message now
    /// leads with the registration, says outright that the id may well be correct,
    /// and states plainly that the message was DROPPED rather than queued.
    #[error(
        "agent {session:?} has no registered inbox, so nothing was published. This does NOT \
         mean the session id is wrong or stale — a live session can have an unregistered \
         inbox (it registers on its SessionStart hook and re-registers at each turn \
         boundary, so one that was resumed may not have re-registered yet). The message was \
         DROPPED, not queued: baseline-on-subscribe means anything published now would be \
         baselined away when that agent does register, so sending it later is the only way \
         it arrives. Check `mailbox agents` for who is addressable right now."
    )]
    InboxNotRegistered { session: String },

    /// The target's session id cannot form an inbox topic at all.
    #[error("cannot address that agent: {0}")]
    Topic(#[from] TopicError),

    #[error(transparent)]
    Bus(#[from] BusError),

    #[error(transparent)]
    Storage(#[from] StorageError),
}

/// One registered agent inbox, as discovery sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentInbox {
    /// The agent's session id — the address `send` takes.
    pub session: SessionId,
    /// Its inbox topic.
    pub inbox: Topic,
    /// Whether a Claude Code process is still running for this session
    /// ([`crate::doctor::live_claude_sessions`]).
    ///
    /// It says the agent EXISTS, not that it is idle, healthy, or reachable — a
    /// live process may be mid-turn, and `mailbox doctor` is the command that
    /// actually proves wakeability. A `send` to an agent that reads `false` still
    /// lands durably in its inbox; it simply has nobody left to collect it.
    pub live: bool,
    /// Whether this is the caller itself. `false` for every row when there is no
    /// caller (a human listing the fleet is not one of the agents in it).
    pub is_self: bool,
}

/// The message that was delivered to a peer's inbox.
#[derive(Debug, Clone, PartialEq)]
pub struct Sent {
    pub to: SessionId,
    pub topic: Topic,
    pub event: Event,
}

/// Publish `body` to `to`'s inbox, stamped with `from` so the receiver can reply.
///
/// The registration check and the publish are two steps, not one transaction. The
/// race is benign and deliberately not closed: the only interleaving is `to`
/// ending its session between the check and the publish, in which case the event
/// lands on a topic nobody is subscribed to — exactly what an ordinary publish to
/// a departed session does today. What the check DOES buy is that the common,
/// deterministic mistake — messaging an agent that never registered — fails
/// loudly instead of silently vanishing.
///
/// The [`FROM_FIELD`] is written last and overwrites any caller-supplied `from`, so
/// the field always means "the session this bridge accepted the message from" —
/// provenance a receiver can rely on for *routing a reply*, never authority
/// (ADR-0001).
///
/// `from` is `None` for a sender that is not a session at all (a human at a
/// terminal). The key is then REMOVED rather than stamped — including from a body
/// that arrived carrying one, so an anonymous sender cannot forge a reply address
/// the bridge did not verify. See the module docs for why absent beats a
/// placeholder.
///
/// `subject` is the sender's optional description of what the message is about; the
/// bridge composes the final subject line around it (see [`message_subject`]).
pub async fn send(
    bus: &Bus,
    storage: &Storage,
    from: Option<SessionId>,
    to: SessionId,
    mut body: Map<String, Value>,
    subject: Option<Subject>,
) -> Result<Sent, SendError> {
    let topic = inbox_topic(&to)?;
    // One rendering of "who sent this" for every log line below; a send with no
    // session is a real case, so it gets a legible label rather than a blank.
    let sender = from
        .as_ref()
        .map(SessionId::as_str)
        .unwrap_or("(no session)");

    if !is_registered(storage, &to).await? {
        // Log the rejection server-side (identifiers only, NEVER the body): the
        // daemon's generic request log shows `topic="-"` for a send, so without
        // this a refused send leaves no trace of who tried to reach whom or why.
        warn!(
            from = sender,
            to = to.as_str(),
            "rejected a send: target has no registered inbox"
        );
        return Err(SendError::InboxNotRegistered {
            session: to.as_str().to_string(),
        });
    }

    match &from {
        Some(from) => {
            body.insert(
                FROM_FIELD.to_string(),
                Value::String(from.as_str().to_string()),
            );
        }
        // No reply address to give. Say that by absence, and drop any `from` the
        // caller supplied: an unstamped message must not claim a sender.
        None => {
            body.remove(FROM_FIELD);
        }
    }

    // A normal publish: durable append, then the wake to every subscriber of the
    // inbox topic (the recipient).
    let event = bus
        .publish(
            topic.clone(),
            AdapterId(AGENT_ADAPTER.to_string()),
            Timestamp(now_millis()),
            Value::Object(body),
            message_subject(subject, from.as_ref()),
        )
        .await?;

    info!(
        from = sender,
        to = to.as_str(),
        topic = topic.as_str(),
        offset = event.offset.0,
        // Never the body: a peer message is untrusted content like any other.
        "delivered a message to a peer agent's inbox"
    );
    Ok(Sent { to, topic, event })
}

/// The subject line a peer message wakes its recipient with.
///
/// Composed HERE rather than at the CLI so every caller of `send` — the command, a
/// script on the control socket, a future adapter — produces the same line, and so
/// the sender is stamped by the bridge that verified it rather than claimed by the
/// message.
///
/// # Why the message text is not the subject
///
/// A wake describes what is waiting; it does not deliver it. Folding the message
/// body into the wake would make the mail readable without `read`, which is the one
/// thing the wake wire is not for (ADR-0022) — and it would put a peer's words into
/// a turn the recipient has not chosen to spend on them yet. So the default says
/// only that a message arrived and who from, and a sender with something more useful
/// to say says it deliberately, with `--subject`.
///
/// # Why the sender comes FIRST
///
/// A `Subject` truncates from the end, so anything after the sender's text is what
/// a long subject eats. Leading with the attribution makes "who is asking" the one
/// part that cannot be crowded out — by an over-long subject, or by one written to
/// push the identity off the line.
fn message_subject(subject: Option<Subject>, from: Option<&SessionId>) -> Option<Subject> {
    // The bridge states the sender it VERIFIED, or says plainly that there is none.
    // Never a placeholder that reads like an address (see the module docs).
    let sender = match from {
        Some(from) => from.as_str(),
        None => "an unidentified sender",
    };
    let text = match &subject {
        Some(subject) => format!("from {sender}: {}", subject.text()),
        // Nothing to describe: all we can honestly report is that something arrived,
        // which is still worth a turn — there is mail to read.
        None => format!("message from {sender}"),
    };
    // The sender's link travels untouched — only the text gains the attribution, and
    // where the sender was pointing is not ours to rewrite.
    let link = subject.as_ref().and_then(Subject::link);
    // A peer message with no describable subject is still delivered; it just wakes
    // with the topic and a count, like any other subject-less publish.
    Subject::new(&text, link).ok()
}

/// Whether `session` is subscribed to its own inbox topic (i.e. is addressable).
async fn is_registered(storage: &Storage, session: &SessionId) -> Result<bool, StorageError> {
    Ok(storage
        .list_agent_inboxes()
        .await?
        .iter()
        .any(|s| s == session))
}

/// Every registered agent inbox, with liveness, marking `caller` as itself.
///
/// `live` is the set of session ids that currently have a Claude Code process, as
/// read from the process table by [`crate::doctor::live_claude_sessions`]. It is
/// passed in rather than scanned here so the whole listing is measured in ONE `ps`
/// call, and so this function stays a pure projection a test can drive directly.
///
/// A session missing from `live` reads as `live: false`. That understates when the
/// process table could not be read at all — the same direction the pidfile probe
/// this replaces erred in, and the safe one: never claim an agent is there.
///
/// `caller` is `None` when the lister is not a session — a human at a terminal.
/// Marking a row is the ONLY thing the caller is used for, so the listing is
/// otherwise identical and simply marks nobody. Discovery is not privileged: who
/// is addressable is the same question whoever asks it.
pub async fn list(
    storage: &Storage,
    live: &BTreeSet<SessionId>,
    caller: Option<&SessionId>,
) -> Result<Vec<AgentInbox>, SendError> {
    let sessions = storage.list_agent_inboxes().await?;
    let mut agents = Vec::with_capacity(sessions.len());
    for session in sessions {
        // Every session in this list is registered, which means its id already
        // formed an inbox topic — so this cannot fail. `?` rather than an
        // `expect` keeps the impossible case a value, not a panic.
        let inbox = inbox_topic(&session)?;
        agents.push(AgentInbox {
            live: live.contains(&session),
            is_self: caller.is_some_and(|caller| caller == &session),
            session,
            inbox,
        });
    }
    Ok(agents)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::StorageConfig;

    async fn fresh() -> (Bus, Storage, tempfile::TempDir) {
        let dir = tempfile::TempDir::new().unwrap();
        let storage = Storage::open(StorageConfig::at(dir.path().join("mailbox.db")))
            .await
            .unwrap();
        let bus = Bus::new(storage.clone());
        (bus, storage, dir)
    }

    #[test]
    fn a_message_subject_states_the_sender_the_bridge_verified() {
        let alice = SessionId::new("s-alice");

        let stated = message_subject(None, Some(&alice)).unwrap();
        assert_eq!(stated.text(), "message from s-alice");

        let described = message_subject(
            Subject::new("PR 42 review finished", Some("https://example.com/pull/42")).ok(),
            Some(&alice),
        )
        .unwrap();
        assert_eq!(described.text(), "from s-alice: PR 42 review finished");
        assert_eq!(
            described.link(),
            Some("https://example.com/pull/42"),
            "the sender's link is carried, not rewritten"
        );

        // A human at a terminal has no reply address, and the subject says so rather
        // than naming a sender nobody checked.
        let anonymous = message_subject(Subject::new("stop", None).ok(), None).unwrap();
        assert_eq!(anonymous.text(), "from an unidentified sender: stop");
        assert_eq!(
            message_subject(None, None).unwrap().text(),
            "message from an unidentified sender"
        );
    }

    /// A subject long enough to overflow the line must lose its own tail, never the
    /// sender: "who is asking" is the part the recipient cannot reconstruct.
    #[test]
    fn a_crowding_subject_cannot_push_the_sender_off_the_line() {
        let alice = SessionId::new("s-alice");
        let crowding = Subject::new(&"x".repeat(mailbox_protocol::MAX_TEXT_CHARS), None).unwrap();

        let composed = message_subject(Some(crowding), Some(&alice)).unwrap();

        assert!(
            composed.text().starts_with("from s-alice: "),
            "{}",
            composed.text()
        );
        assert!(composed.text().ends_with('…'), "{}", composed.text());
    }

    /// Register `session`'s inbox exactly as `harness arm` does — via the guarded
    /// auto-inbox path (the only path the tombstone scopes to).
    async fn register(bus: &Bus, session: &SessionId) {
        let topic = inbox_topic(session).unwrap();
        bus.subscribe_auto_inbox(session.clone(), std::slice::from_ref(&topic))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn send_stamps_the_sender_and_lands_in_the_targets_inbox() {
        let (bus, storage, _dir) = fresh().await;
        let (a, b) = (SessionId::new("s-a"), SessionId::new("s-b"));
        register(&bus, &b).await;

        let body = serde_json::json!({ "text": "review is done" });
        let sent = send(
            &bus,
            &storage,
            Some(a.clone()),
            b.clone(),
            body.as_object().unwrap().clone(),
            None,
        )
        .await
        .unwrap();
        assert_eq!(sent.topic.as_str(), "agent.s-b");

        // B reads it and can see who to reply to.
        let delivered = bus.read(b, None).await.unwrap();
        assert_eq!(delivered.len(), 1);
        let event = &delivered.events()[0];
        assert_eq!(event.body[FROM_FIELD], serde_json::json!("s-a"));
        assert_eq!(event.body["text"], serde_json::json!("review is done"));
    }

    #[tokio::test]
    async fn send_overwrites_a_forged_from_stamp() {
        let (bus, storage, _dir) = fresh().await;
        let (a, b) = (SessionId::new("s-a"), SessionId::new("s-b"));
        register(&bus, &b).await;

        // A caller that supplies its own `from` does not get to keep it: the
        // bridge stamps the session it accepted the send from.
        let body = serde_json::json!({ "from": "somebody-else" });
        send(
            &bus,
            &storage,
            Some(a),
            b.clone(),
            body.as_object().unwrap().clone(),
            None,
        )
        .await
        .unwrap();

        let delivered = bus.read(b, None).await.unwrap();
        assert_eq!(delivered.events()[0].body[FROM_FIELD], "s-a");
    }

    /// A send with no sender session — a human at a terminal — delivers, and the
    /// recipient's copy has **no `from` key at all**.
    ///
    /// The assertion is on the key's ABSENCE rather than on some sentinel value,
    /// because that is the contract a receiving agent branches on: `from` present
    /// means "you can reply to this"; `from` missing means "there is nobody to reply
    /// to". A `null`, an empty string, or a "human" placeholder would each be a value
    /// an agent might hand straight back to `mailbox send`.
    #[tokio::test]
    async fn a_send_with_no_session_omits_the_from_key_entirely() {
        let (bus, storage, _dir) = fresh().await;
        let b = SessionId::new("s-b");
        register(&bus, &b).await;

        let body = serde_json::json!({ "text": "poked by a human" });
        send(
            &bus,
            &storage,
            None,
            b.clone(),
            body.as_object().unwrap().clone(),
            None,
        )
        .await
        .unwrap();

        let delivered = bus.read(b, None).await.unwrap();
        let body = &delivered.events()[0].body;
        assert_eq!(body["text"], serde_json::json!("poked by a human"));
        assert!(
            body.get(FROM_FIELD).is_none(),
            "an unattributed message must omit `from`, not carry a placeholder: {body}"
        );
    }

    /// The anonymous path must not let a caller SUPPLY the reply address the bridge
    /// could not verify. Without this, `--body '{"from":"s-victim"}'` from any local
    /// process would be a forged `from` — the exact hole the stamp-last rule closes
    /// on the attributed path.
    #[tokio::test]
    async fn a_send_with_no_session_strips_a_caller_supplied_from() {
        let (bus, storage, _dir) = fresh().await;
        let b = SessionId::new("s-b");
        register(&bus, &b).await;

        let body = serde_json::json!({ "from": "s-somebody-else" });
        send(
            &bus,
            &storage,
            None,
            b.clone(),
            body.as_object().unwrap().clone(),
            None,
        )
        .await
        .unwrap();

        let delivered = bus.read(b, None).await.unwrap();
        assert!(
            delivered.events()[0].body.get(FROM_FIELD).is_none(),
            "an unattributed send must not carry a `from` the bridge did not stamp"
        );
    }

    #[tokio::test]
    async fn send_to_an_unregistered_agent_fails_and_publishes_nothing() {
        let (bus, storage, _dir) = fresh().await;
        let ghost = SessionId::new("s-ghost");

        let err = send(
            &bus,
            &storage,
            Some(SessionId::new("s-a")),
            ghost.clone(),
            Map::new(),
            None,
        )
        .await
        .unwrap_err();
        assert!(matches!(err, SendError::InboxNotRegistered { .. }));

        // Nothing was written: were the ghost to register later, its baseline
        // would have skipped the message anyway — so the durable log stays clean.
        let topics = storage.list_topics(None).await.unwrap();
        assert!(
            topics.is_empty(),
            "a refused send must not create the topic: {topics:?}"
        );
    }

    #[tokio::test]
    async fn send_to_a_recently_ended_agent_hard_errors() {
        // After a peer ends, its inbox subscription is gone (and it is tombstoned),
        // so a send must hard-error rather than report success for a message that
        // could never be delivered (ADR-0007 / FIX 1).
        let (bus, storage, _dir) = fresh().await;
        let b = SessionId::new("s-b");
        register(&bus, &b).await;
        storage.end_session(b.clone(), now_millis()).await.unwrap();

        let err = send(
            &bus,
            &storage,
            Some(SessionId::new("s-a")),
            b.clone(),
            Map::new(),
            None,
        )
        .await
        .unwrap_err();
        assert!(matches!(err, SendError::InboxNotRegistered { .. }));
    }

    #[tokio::test]
    async fn a_subscribe_racing_a_recent_end_does_not_resurrect_the_inbox() {
        // The arm-vs-cleanup race at the bus level: end the session, then a racing
        // auto-inbox re-registration (an arm's Subscribe) within the guard window is
        // refused, so the dead session never reappears in `agents` (FIX 1). Only the
        // AutoInbox path is guarded — the path the doomed arm actually uses.
        let (bus, storage, _dir) = fresh().await;
        let b = SessionId::new("s-b");
        register(&bus, &b).await;
        storage.end_session(b.clone(), now_millis()).await.unwrap();

        let summary = bus
            .subscribe_auto_inbox(b.clone(), std::slice::from_ref(&inbox_topic(&b).unwrap()))
            .await
            .unwrap();
        assert_eq!(
            summary[0].1,
            crate::storage::SubscribeOutcome::RefusedSessionRecentlyEnded
        );

        assert!(
            list(&storage, &BTreeSet::new(), Some(&b))
                .await
                .unwrap()
                .is_empty(),
            "a tombstoned session must not be listed as an agent"
        );
    }

    #[tokio::test]
    async fn list_reports_registered_inboxes_and_marks_self() {
        let (bus, storage, _dir) = fresh().await;
        let (a, b) = (SessionId::new("s-a"), SessionId::new("s-b"));
        register(&bus, &a).await;
        register(&bus, &b).await;
        // A subscription to a PEER's inbox does not make the subscriber an agent,
        // and does not list the peer twice.
        bus.subscribe(SessionId::new("s-lurker"), &[inbox_topic(&a).unwrap()])
            .await
            .unwrap();

        let agents = list(&storage, &BTreeSet::new(), Some(&a)).await.unwrap();
        assert_eq!(
            agents
                .iter()
                .map(|x| x.session.as_str())
                .collect::<Vec<_>>(),
            ["s-a", "s-b"]
        );
        assert!(agents[0].is_self, "the caller is marked");
        assert!(!agents[1].is_self);
    }

    /// With NO caller — a human listing the fleet from a terminal — the listing is
    /// the same listing, and nobody is marked as self. Discovery is not privileged:
    /// the answer to "who is addressable" does not depend on who is asking.
    #[tokio::test]
    async fn list_with_no_caller_lists_everyone_and_marks_nobody() {
        let (bus, storage, _dir) = fresh().await;
        let (a, b) = (SessionId::new("s-a"), SessionId::new("s-b"));
        register(&bus, &a).await;
        register(&bus, &b).await;

        let anonymous = list(&storage, &BTreeSet::new(), None).await.unwrap();
        assert_eq!(
            anonymous
                .iter()
                .map(|x| x.session.as_str())
                .collect::<Vec<_>>(),
            ["s-a", "s-b"],
            "every agent is listed to a caller that is not one of them"
        );
        assert!(
            anonymous.iter().all(|x| !x.is_self),
            "no row may be marked as self when there is no self"
        );

        // The rows are otherwise identical to what a session sees, so a human and an
        // agent are reading the same fleet — only the marking differs.
        let as_a = list(&storage, &BTreeSet::new(), Some(&a)).await.unwrap();
        assert_eq!(
            anonymous
                .iter()
                .map(|x| (&x.session, &x.inbox, x.live))
                .collect::<Vec<_>>(),
            as_a.iter()
                .map(|x| (&x.session, &x.inbox, x.live))
                .collect::<Vec<_>>()
        );
    }

    /// Liveness is exactly membership of the live-session set — nothing is inferred
    /// from the mailbox's own state. That is the whole point of the signal: a
    /// session's subscriptions, sentinel and interests all outlive the Claude Code
    /// process they belong to, so only the process table can answer this.
    #[tokio::test]
    async fn liveness_is_membership_of_the_live_session_set() {
        let (bus, storage, _dir) = fresh().await;
        let (a, b) = (SessionId::new("s-a"), SessionId::new("s-b"));
        register(&bus, &a).await;
        register(&bus, &b).await;

        let live = BTreeSet::from([SessionId::new("s-a")]);
        let agents = list(&storage, &live, Some(&a)).await.unwrap();
        assert!(agents[0].live, "s-a has a live Claude Code process");
        assert!(
            !agents[1].live,
            "s-b is registered and addressable, but nobody is running it"
        );

        // An empty set (including the "could not read the process table" case) never
        // claims an agent is there.
        let agents = list(&storage, &BTreeSet::new(), Some(&a)).await.unwrap();
        assert!(agents.iter().all(|x| !x.live));
    }
}
