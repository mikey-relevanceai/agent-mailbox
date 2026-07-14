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
//! ordinary publish, and the wake it produces is the ordinary payload-free kick.
//! This module is the thin policy that makes those primitives usable as agent
//! addressing.
//!
//! # Why `send` to an unregistered agent is an ERROR, not a publish
//!
//! Baseline-on-subscribe (see [`crate::bus`]) means a fresh subscription starts
//! at the topic head: events published *before* it subscribed are baselined away
//! and never delivered. So a message published to a session that has NOT
//! registered an inbox is **guaranteed undeliverable** — if that session later
//! registers, its baseline skips exactly the message we just wrote. Publishing it
//! anyway would durably store a message no one can ever read while telling the
//! sender it succeeded. [`send`] therefore refuses ([`SendError::UnknownAgent`])
//! rather than write into a void, and there is deliberately no `--force`.
//!
//! # Trust
//!
//! Any local same-user process can publish to any inbox. That is the accepted
//! trust boundary (one user, one machine — ADR-0007). The `from` stamp is
//! *provenance*, not authority: a message body remains untrusted data and must
//! never be treated as an instruction to obey (ADR-0001).

use std::path::Path;

use serde_json::{Map, Value};
use tracing::{info, warn};

use mailbox_protocol::{AdapterId, Event, Timestamp, Topic, TopicError, inbox_topic};

use crate::bus::{Bus, BusError};
use crate::clock::now_millis;
use crate::storage::{SessionId, Storage, StorageError};
use crate::wake::waiter_alive;

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
    #[error(
        "unknown agent {session:?}: it has no registered inbox, so nothing was published \
         (a message to an unregistered agent can never be delivered — it would be baselined \
         away if that agent later registered). Check `mailbox agents` for who is addressable."
    )]
    UnknownAgent { session: String },

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
    /// Whether a waiter is blocked for it right now (idle and listening). See
    /// [`crate::wake::waiter_alive`] for exactly what this does and does not
    /// claim — it is a live-waiter probe, not a heartbeat.
    pub live_waiter: bool,
    /// Whether this is the caller itself.
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
/// The `from` stamp is written last and overwrites any caller-supplied `from`, so
/// the field always means "the session this bridge accepted the message from" —
/// provenance a receiver can rely on for *routing a reply*, never authority
/// (ADR-0001).
pub async fn send(
    bus: &Bus,
    storage: &Storage,
    from: SessionId,
    to: SessionId,
    mut body: Map<String, Value>,
) -> Result<Sent, SendError> {
    let topic = inbox_topic(&to)?;

    if !is_registered(storage, &to).await? {
        // Log the rejection server-side (identifiers only, NEVER the body): the
        // daemon's generic request log shows `topic="-"` for a send, so without
        // this a refused send leaves no trace of who tried to reach whom or why.
        warn!(
            from = from.as_str(),
            to = to.as_str(),
            "rejected a send: target has no registered inbox"
        );
        return Err(SendError::UnknownAgent {
            session: to.as_str().to_string(),
        });
    }

    body.insert(
        FROM_FIELD.to_string(),
        Value::String(from.as_str().to_string()),
    );

    // A normal publish: durable append, then the payload-free kick to every
    // subscriber of the inbox topic (the recipient's waiter).
    let event = bus
        .publish(
            topic.clone(),
            AdapterId(AGENT_ADAPTER.to_string()),
            Timestamp(now_millis()),
            Value::Object(body),
        )
        .await?;

    info!(
        from = from.as_str(),
        to = to.as_str(),
        topic = topic.as_str(),
        offset = event.offset.0,
        // Never the body: a peer message is untrusted content like any other.
        "delivered a message to a peer agent's inbox"
    );
    Ok(Sent { to, topic, event })
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
/// `waiters_dir` is the directory holding the per-session waiter pidfiles (the
/// daemon's own, derived from the storage path), which is where liveness comes
/// from.
pub async fn list(
    storage: &Storage,
    waiters_dir: &Path,
    caller: &SessionId,
) -> Result<Vec<AgentInbox>, SendError> {
    let sessions = storage.list_agent_inboxes().await?;
    let mut agents = Vec::with_capacity(sessions.len());
    for session in sessions {
        // Every session in this list is registered, which means its id already
        // formed an inbox topic — so this cannot fail. `?` rather than an
        // `expect` keeps the impossible case a value, not a panic.
        let inbox = inbox_topic(&session)?;
        agents.push(AgentInbox {
            live_waiter: waiter_alive(waiters_dir, &session),
            is_self: &session == caller,
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
            a.clone(),
            b.clone(),
            body.as_object().unwrap().clone(),
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
            a,
            b.clone(),
            body.as_object().unwrap().clone(),
        )
        .await
        .unwrap();

        let delivered = bus.read(b, None).await.unwrap();
        assert_eq!(delivered.events()[0].body[FROM_FIELD], "s-a");
    }

    #[tokio::test]
    async fn send_to_an_unregistered_agent_fails_and_publishes_nothing() {
        let (bus, storage, _dir) = fresh().await;
        let ghost = SessionId::new("s-ghost");

        let err = send(
            &bus,
            &storage,
            SessionId::new("s-a"),
            ghost.clone(),
            Map::new(),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, SendError::UnknownAgent { .. }));

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

        let err = send(&bus, &storage, SessionId::new("s-a"), b.clone(), Map::new())
            .await
            .unwrap_err();
        assert!(matches!(err, SendError::UnknownAgent { .. }));
    }

    #[tokio::test]
    async fn a_subscribe_racing_a_recent_end_does_not_resurrect_the_inbox() {
        // The arm-vs-cleanup race at the bus level: end the session, then a racing
        // auto-inbox re-registration (an arm's Subscribe) within the guard window is
        // refused, so the dead session never reappears in `agents` (FIX 1). Only the
        // AutoInbox path is guarded — the path the doomed arm actually uses.
        let (bus, storage, dir) = fresh().await;
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

        let waiters = dir.path().join("waiters");
        assert!(
            list(&storage, &waiters, &b).await.unwrap().is_empty(),
            "a tombstoned session must not be listed as an agent"
        );
    }

    #[tokio::test]
    async fn list_reports_registered_inboxes_and_marks_self() {
        let (bus, storage, dir) = fresh().await;
        let (a, b) = (SessionId::new("s-a"), SessionId::new("s-b"));
        register(&bus, &a).await;
        register(&bus, &b).await;
        // A subscription to a PEER's inbox does not make the subscriber an agent,
        // and does not list the peer twice.
        bus.subscribe(SessionId::new("s-lurker"), &[inbox_topic(&a).unwrap()])
            .await
            .unwrap();

        let waiters = dir.path().join("waiters");
        let agents = list(&storage, &waiters, &a).await.unwrap();
        assert_eq!(
            agents
                .iter()
                .map(|x| x.session.as_str())
                .collect::<Vec<_>>(),
            ["s-a", "s-b"]
        );
        assert!(agents[0].is_self, "the caller is marked");
        assert!(!agents[1].is_self);
        // No waiter processes exist in this unit test, so nothing is live.
        assert!(agents.iter().all(|x| !x.live_waiter));
    }
}
