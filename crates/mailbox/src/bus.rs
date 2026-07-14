//! The topic bus: the session-facing business layer over [`crate::storage`].
//!
//! Storage is the data layer — it owns the connection and the primitive ops.
//! The bus is the *policy* on top: it speaks in whole-session operations
//! (subscribe to a set of topics, read everything unread across them) and owns
//! the two delivery decisions that make this a durable multi-subscriber bus and
//! not just an event log:
//!
//! - **Baseline-on-subscribe.** A fresh subscription starts at the topic head,
//!   so a new subscriber only ever sees events published *after* it subscribed —
//!   history is never replayed (docs/01-wake-and-rearm.md).
//! - **Advance-on-read, no ack.** [`Bus::read`] returns a session's unread events
//!   and advances that session's per-topic cursors to what it returned, in one
//!   step. The agent loop is subscribe → idle → wake → read → react; there is no
//!   separate acknowledgement. A publish that lands *after* a read (mid-turn)
//!   sits beyond the cursor and is surfaced on the NEXT read — not lost, not
//!   delivered twice.
//!
//! # Delivery guarantee (be precise): at-most-once, exactly-once *while subscribed*
//!
//! The load-bearing property is exactly-once delivery per subscriber — but only
//! for the window in which that subscriber is continuously subscribed and doing
//! the reads. Stated honestly: delivery is **at-most-once**, and **exactly-once
//! for every event published while the session is subscribed, provided it keeps
//! reading**. Events that a session never reads before it unsubscribes are
//! *forfeited* — [`Bus::unsubscribe`] means "stop future delivery", and
//! re-subscribing baselines to the current head rather than replaying the gap.
//! That is intended, not a bug.
//!
//! # Why atomicity lives in storage
//!
//! That guarantee rests on two compound operations being genuinely atomic:
//! "record the subscription THEN baseline the cursor" and "read after the cursor
//! THEN advance the cursor". If the bus composed those from separate awaited
//! [`Storage`] calls, a publish could interleave between the two halves and be
//! missed or double-delivered. So each compound operation is a SINGLE writer
//! command inside storage (`subscribe_and_baseline`, `read_unread`); the bus only
//! chooses *which* command to issue and per which topic. Layering stays one-way:
//! bus → storage.
//!
//! # Untrusted bodies
//!
//! Event bodies are opaque `serde_json::Value` (ADR-0001). The bus routes them
//! and never interprets or logs them.

use serde_json::Value;
use tracing::{info, warn};

use mailbox_protocol::{AdapterId, Event, Timestamp, Topic};

use crate::clock::now_millis;
use crate::storage::{Storage, StorageError};
use crate::wake::Waker;
// Re-exported so callers depend on `bus::SessionId` / `bus::SubscribeOutcome` and
// storage stays free to change its representation without touching call sites.
pub use crate::storage::{SessionId, SubscribeOutcome};

/// Errors from a bus operation.
///
/// The bus adds no failure modes of its own for the MVP — every fallible step is
/// a storage operation — so this is a thin wrapper that keeps the layer's error
/// surface its own type (callers match on `BusError`, not `StorageError`) and
/// leaves room for genuinely bus-level errors later without a breaking change.
#[derive(Debug, thiserror::Error)]
pub enum BusError {
    /// A failure in the durable store beneath the bus.
    #[error(transparent)]
    Storage(#[from] StorageError),
}

/// A session's unread events across all of its subscribed topics, as returned by
/// one [`Bus::read`].
///
/// The events are ordered by `(topic, offset)`: deterministic, and within a
/// topic strictly increasing. By the time this value exists, the session's
/// per-topic cursors have already advanced past every event in it — reading is
/// what consumes them (advance-on-read). A dedicated type (rather than a bare
/// `Vec<Event>`) names the concept and leaves room to carry paging/truncation
/// state later.
#[derive(Debug, Clone, PartialEq)]
pub struct Delivery {
    // Private so the invariant "a Delivery is the result of a completed, ordered
    // read whose cursors have already advanced" lives in the type: only the bus
    // (via `new`) can mint one, and callers can read but not fabricate the list.
    events: Vec<Event>,
}

impl Delivery {
    /// Wrap the events of one completed read. Crate-private: a `Delivery` may only
    /// be produced by [`Bus::read`], never assembled by a caller.
    pub(crate) fn new(events: Vec<Event>) -> Self {
        Self { events }
    }

    /// The unread events delivered by this read, in `(topic, offset)` order. The
    /// session's cursors have already advanced past every one of them.
    pub fn events(&self) -> &[Event] {
        &self.events
    }

    /// Take ownership of the delivered events.
    pub fn into_events(self) -> Vec<Event> {
        self.events
    }

    /// How many events this delivery carried.
    pub fn len(&self) -> usize {
        self.events.len()
    }

    /// Whether this delivery carried no unread events (an idle wake, or nothing
    /// new since the last read).
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }
}

/// The durable topic bus.
///
/// Cheap to clone — it holds a [`Storage`] handle, which is itself just a channel
/// to the single writer. Every clone talks to the same durable log.
///
/// A bus optionally carries a [`Waker`]: when present, [`Bus::publish`] kicks the
/// FIFO of every session subscribed to the published topic (payload-free wake).
/// Without one, publish is a pure durable append — useful for tests and for any
/// caller that does not own the wake channel.
#[derive(Clone, Debug)]
pub struct Bus {
    storage: Storage,
    waker: Option<Waker>,
}

/// The per-topic outcome of one [`Bus::subscribe`] call, in the order requested.
///
/// Returned (rather than discarded) so a caller can see, per topic, whether it
/// created a fresh subscription and at what baseline, or found it already
/// subscribed — useful for the harness's arm/re-arm bookkeeping and for tests.
pub type SubscribeSummary = Vec<(Topic, SubscribeOutcome)>;

impl Bus {
    /// Build a bus over an open [`Storage`] handle, with no wake channel: publish
    /// appends durably but kicks no waiters.
    pub fn new(storage: Storage) -> Self {
        Self {
            storage,
            waker: None,
        }
    }

    /// Build a bus that kicks waiters on publish, using `waker` as the wake
    /// primitive. The bridge wires this so a publish wakes idle sessions; the
    /// bus depends only on the [`Waker`] abstraction, so the primitive (FIFO
    /// today, socket later) can change without touching this layer.
    pub fn with_waker(storage: Storage, waker: Waker) -> Self {
        Self {
            storage,
            waker: Some(waker),
        }
    }

    /// Publish `body` to `topic` under `adapter`, appending it to the durable log
    /// and assigning the next per-topic offset.
    ///
    /// Appending and offset assignment are already one atomic writer op, and
    /// publish carries no bus-level policy over the body — it is stored verbatim
    /// and never interpreted (ADR-0001).
    ///
    /// # Kick-on-publish (payload-free)
    ///
    /// After the event is durably appended, a bus with a [`Waker`] signals every
    /// session subscribed to `topic`. The kick is a bare byte and the topic name
    /// is used only for logging — no body ever crosses the wake boundary. The
    /// kick happens *after* the durable append, which is what makes the waiter's
    /// open→check→block ordering race-free (see [`crate::wake`]).
    ///
    /// A kick is best-effort and must never fail a publish: the event is already
    /// durable, and a session with no live waiter is normal. If listing the
    /// subscribers itself fails (a store error on the read path), we log and
    /// return the published event anyway — a late waiter's unread check still
    /// covers the mail.
    pub async fn publish(
        &self,
        topic: Topic,
        adapter: AdapterId,
        timestamp: Timestamp,
        body: Value,
    ) -> Result<Event, BusError> {
        let event = self
            .storage
            .publish(topic.clone(), adapter, timestamp, body)
            .await?;

        if let Some(waker) = &self.waker {
            match self.storage.sessions_subscribed(topic.clone()).await {
                Ok(sessions) => waker.kick_all(&sessions, &topic),
                Err(err) => warn!(
                    topic = topic.as_str(),
                    error = %err,
                    "could not list subscribers to kick after publish; \
                     relying on waiter unread-check"
                ),
            }
        }

        Ok(event)
    }

    /// Subscribe `session` to each of `topics`, baselining its delivery cursor to
    /// each topic's head so history is not replayed. Idempotent per topic: a
    /// repeat subscribe is a no-op that leaves the cursor untouched (so it never
    /// skips events the session has not yet read).
    ///
    /// Each topic is its own atomic `subscribe_and_baseline` writer command. That
    /// is sufficient: the atomicity that matters is subscribe-then-baseline for a
    /// single topic; distinct topics are independent, so there is no cross-topic
    /// invariant to hold. On the first topic that errors this returns early — but
    /// the topics processed before it are already durably subscribed (each committed
    /// in its own command), so a partial subscribe is a real, observable state.
    ///
    /// Returns the per-topic [`SubscribeOutcome`]s (see [`SubscribeSummary`]) so the
    /// decision is not thrown away.
    pub async fn subscribe(
        &self,
        session: SessionId,
        topics: &[Topic],
    ) -> Result<SubscribeSummary, BusError> {
        let mut summary = SubscribeSummary::with_capacity(topics.len());
        // One clock read for the whole call: the tombstone guard compares this
        // against the session's own recent `end_session` (ADR-0007).
        let now_ms = now_millis();
        for topic in topics {
            let outcome = self
                .storage
                .subscribe_and_baseline(session.clone(), topic.clone(), now_ms)
                .await?;
            // Log the decision (the storage layer stays silent on success). Flatten
            // the baseline to a grep-able numeric field, present only when there was
            // a head to baseline to.
            match outcome {
                SubscribeOutcome::Subscribed {
                    baseline: Some(offset),
                } => info!(
                    session = session.as_str(),
                    topic = topic.as_str(),
                    baseline_offset = offset.0,
                    "subscribed session to topic"
                ),
                SubscribeOutcome::Subscribed { baseline: None } => info!(
                    session = session.as_str(),
                    topic = topic.as_str(),
                    "subscribed session to topic (empty topic; no baseline)"
                ),
                SubscribeOutcome::AlreadySubscribed => info!(
                    session = session.as_str(),
                    topic = topic.as_str(),
                    "subscribe was a no-op (already subscribed)"
                ),
                // The session ended within the guard window; refusing here is what
                // stops a racing re-registration from resurrecting a dead inbox
                // (ADR-0007). Not an error — the session really did just end.
                SubscribeOutcome::RefusedSessionRecentlyEnded => warn!(
                    session = session.as_str(),
                    topic = topic.as_str(),
                    "refused a subscribe: the session ended moments ago (tombstone guard); \
                     not resurrecting its inbox"
                ),
            }
            summary.push((topic.clone(), outcome));
        }
        Ok(summary)
    }

    /// Unsubscribe `session` from each of `topics`. Idempotent: unsubscribing a
    /// topic the session is not subscribed to is a harmless no-op.
    ///
    /// This stops FUTURE delivery only. It deletes the subscription, never the
    /// durable log — the events remain for other subscribers and for this session
    /// if it re-subscribes (which will baseline afresh to the head at that time).
    /// The delivery cursor is deliberately left in place so a later re-subscribe's
    /// baseline can only ever move it forward.
    ///
    /// Note the honest guarantee: any events still UNREAD by this session at the
    /// moment it unsubscribes are forfeited — re-subscribing baselines to the head,
    /// not to the old cursor, so the gap is not replayed. This is "stop future
    /// delivery", by design (see the module-level delivery-guarantee note).
    pub async fn unsubscribe(&self, session: SessionId, topics: &[Topic]) -> Result<(), BusError> {
        for topic in topics {
            self.storage
                .unsubscribe(session.clone(), topic.clone())
                .await?;
            info!(
                session = session.as_str(),
                topic = topic.as_str(),
                "unsubscribed session from topic (durable log retained)"
            );
        }
        Ok(())
    }

    /// Read `session`'s unread events across ALL its subscribed topics and advance
    /// its cursors past them, atomically (advance-on-read). Returns an empty
    /// [`Delivery`] when nothing is unread — the common case on an idle wake.
    ///
    /// `limit` bounds the page per topic (bridge default if `None`); anything
    /// beyond it is not lost, it surfaces on the next read via the advanced
    /// cursor.
    pub async fn read(&self, session: SessionId, limit: Option<u32>) -> Result<Delivery, BusError> {
        let events = self.storage.read_unread(session, limit).await?;
        Ok(Delivery::new(events))
    }
}
