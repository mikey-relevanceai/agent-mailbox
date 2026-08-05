//! Domain types the storage handle speaks in.
//!
//! These sit alongside the `mailbox-protocol` newtypes (`Topic`, `Offset`,
//! `EventId`, …) and cover the bridge-internal concepts that never appear on
//! the wire: sessions, watches, interest, and baselines. They follow the
//! type-driven rules from AGENTS.md — brand the ids, model watch lifecycle as a
//! named enum (not a stringly column), and keep the creation model
//! ([`WatchSpec`]) separate from the read model ([`Watch`]) so callers cannot
//! invent an id or a lifecycle state for a watch that does not exist yet.

use std::time::Duration;

use serde::{Deserialize, Serialize};

use mailbox_protocol::{Cursor, Event, Offset, Topic};
// The session identity is shared with the harness, so it lives in the protocol
// crate (see `mailbox_protocol::session`). Re-exported here so the many existing
// `mailbox::storage::SessionId` call sites keep working unchanged.
pub use mailbox_protocol::SessionId;

/// Stable identifier for a watch row, assigned by the store on insert.
///
/// The inner id is crate-private on purpose: a caller can only obtain a
/// `WatchId` from [`crate::storage::Storage::upsert_watch`] (or a read), never
/// fabricate one. A fabricated id (e.g. `WatchId(999)` for a nonexistent row)
/// would make `set_watch_state`/`set_baseline` silently affect zero rows yet
/// return `Ok`, so we forbid constructing one at all outside this crate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct WatchId(i64);

impl WatchId {
    /// Wrap a row id. Crate-private: only the store, having read/written the
    /// row, mints these.
    pub(crate) fn new(id: i64) -> Self {
        Self(id)
    }

    /// The underlying row id (for binding into SQL and for callers that need to
    /// persist the id elsewhere).
    pub fn get(self) -> i64 {
        self.0
    }
}

/// Operating-system process id of a supervised adapter child.
///
/// Wraps a `u32` (the tokio/OS pid width) behind a private field so a pid can be
/// constructed only through [`Pid::new`] and cannot be confused with a `WatchId`
/// or an offset, nor silently narrowed to a signed value at a call site.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Pid(u32);

impl Pid {
    /// Wrap an OS pid.
    pub fn new(pid: u32) -> Self {
        Self(pid)
    }

    /// The underlying pid value.
    pub fn get(self) -> u32 {
        self.0
    }
}

/// How far a session's UNREAD mail extends: the `event.event_row_id` of the newest
/// event it has not yet read.
///
/// `event_row_id` is the store's single global monotonic sequence (see
/// `schema::SCHEMA_V1`), so this is comparable ACROSS topics — which per-topic
/// [`Offset`]s are not. That is exactly the property the ADR-0012 turn-boundary
/// re-trigger needs: "is there unread mail NEWER than the mail I have already
/// re-triggered a wake for?" is one `>` on this value, whatever topic it arrived on.
///
/// Branded (not a bare `i64`) so it cannot be confused with an offset, a
/// [`WatchId`], or a pid. It is `Ord` because comparing two watermarks is the whole
/// point.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct WakeWatermark(i64);

impl WakeWatermark {
    /// Wrap a row id. Crate-private: a watermark is minted only by the store that
    /// read it, or by [`WakeWatermark::parse`] reading one back that a store minted.
    pub(crate) fn new(row_id: i64) -> Self {
        Self(row_id)
    }

    /// The underlying row id, for persisting the watermark.
    pub fn get(self) -> i64 {
        self.0
    }

    /// Parse a persisted watermark, or `None` if the text is not one.
    ///
    /// Only a POSITIVE row id parses: SQLite `INTEGER PRIMARY KEY AUTOINCREMENT`
    /// starts at 1, so `0` and negatives are not watermarks any store ever minted —
    /// they are corruption. Accepting them would be actively harmful in the wrong
    /// direction: the comparison is `last >= high_water`, so a garbled `-1` is
    /// harmless but a garbled huge value would suppress every future re-trigger.
    /// Rejecting the whole out-of-range domain keeps the brand's promise ("a row id
    /// some store really assigned") true of every value that inhabits the type.
    ///
    /// A `None` means "we have no usable record", which callers must treat as
    /// "nothing re-triggered yet" — the fail-safe direction (a redundant wake, never
    /// a lost one).
    pub fn parse(raw: &str) -> Option<Self> {
        raw.trim()
            .parse::<i64>()
            .ok()
            .filter(|id| *id > 0)
            .map(Self)
    }
}

/// Mail waiting for a session: the topics it is on, and how far it extends.
///
/// The fields are PRIVATE and there is no public constructor, which is what makes
/// "pending mail always has at least one topic" a fact rather than a comment. The
/// only way to obtain one is [`Unread::from_parts`], which collapses an empty topic
/// set to [`Unread::CaughtUp`] — so a `PendingMail` naming no topics cannot be built,
/// and the re-trigger can never bump a sentinel with an empty topic list while
/// recording a watermark for mail it never named.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingMail {
    topics: Vec<Topic>,
    high_water: WakeWatermark,
}

impl PendingMail {
    /// The topics with mail waiting, ascending — the payload-free list written into
    /// the wake reminder and the sentinel. Never empty.
    pub fn topics(&self) -> &[Topic] {
        &self.topics
    }

    /// The newest waiting event's watermark.
    pub fn high_water(&self) -> WakeWatermark {
        self.high_water
    }
}

/// A session's unread mail as ONE snapshot: which subscribed topics have mail
/// waiting, and how far that mail extends.
///
/// A sum type rather than a `(Vec<Topic>, Option<WakeWatermark>)` because the two
/// fields are not independent: unread topics ALWAYS have a newest unread event, and
/// no unread topics never do. The pair form lets a caller ask for the watermark of
/// nothing; this one does not.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Unread {
    /// Nothing to be woken about: the session's cursors cover every event on every
    /// topic it subscribes to (or it subscribes to none).
    CaughtUp,
    /// Mail is waiting — see [`PendingMail`], which guarantees at least one topic.
    Pending(PendingMail),
}

impl Unread {
    /// Build the snapshot from a query's two outputs, collapsing "no topics" to
    /// [`Unread::CaughtUp`].
    ///
    /// This is the ONLY way a [`PendingMail`] is minted, which is what makes the
    /// "pending implies non-empty" invariant structural: the caller cannot skip the
    /// collapse, because it cannot construct the variant itself.
    pub(crate) fn from_parts(topics: Vec<Topic>, high_water: Option<WakeWatermark>) -> Self {
        match (topics.is_empty(), high_water) {
            (false, Some(high_water)) => Unread::Pending(PendingMail { topics, high_water }),
            // No topics, or no watermark: nothing to wake about either way. The two
            // always agree (a topic row exists only because an event set the
            // watermark), so this is the same state reached two ways, not a fallback.
            _ => Unread::CaughtUp,
        }
    }

    /// The unread topics, empty when caught up — the payload-free view the wake
    /// reminder and the sentinel are written from.
    pub fn topics(&self) -> &[Topic] {
        match self {
            Unread::CaughtUp => &[],
            Unread::Pending(mail) => mail.topics(),
        }
    }
}

/// What kind of external entity a watch polls.
///
/// A closed enum (not a free string) so adding a watch kind forces every match
/// site to handle it. `github-pr` is the MVP product target; `stub` is the
/// trivial reference adapter (card 09) that proves the whole path before any
/// real poller exists.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WatchKind {
    /// A GitHub pull request poller (`github-pr`).
    GithubPr,
    /// The reference stub publisher (`stub`), keyed by its `(kind, repo=label, pr=0)`
    /// identity — a synthetic edge emitter for tests and experiments.
    Stub,
}

impl WatchKind {
    /// The stable string persisted in the `watch.kind` column and used as part
    /// of the `(kind, repo, pr)` identity. Pinned here so a rename can't
    /// silently change what an existing row means.
    pub fn as_str(self) -> &'static str {
        match self {
            WatchKind::GithubPr => "github-pr",
            WatchKind::Stub => "stub",
        }
    }

    /// Parse the persisted string back into a kind.
    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "github-pr" => Some(WatchKind::GithubPr),
            "stub" => Some(WatchKind::Stub),
            _ => None,
        }
    }
}

/// Lifecycle state of a watch (design/01: desired → running → stopped).
///
/// Modelled so invalid states are unrepresentable: `child_pid` is meaningful
/// **only** while running, so it lives inside the `Running` variant rather than
/// as a nullable field callers must remember to check. A `Desired` or `Stopped`
/// watch simply has no pid to get wrong.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WatchState {
    /// Wanted by at least one session, adapter not yet spawned.
    Desired,
    /// Adapter child is running under this pid.
    Running { pid: Pid },
    /// Torn down (last interest gone). A clean stop, distinct from [`Failed`].
    Stopped,
    /// The adapter crashed repeatedly and the supervisor gave up restarting it
    /// (card 08): it exceeded the restart policy's consecutive-failure budget, so
    /// an error event was surfaced on the entity's topic and no more restarts are
    /// attempted. Distinct from [`Stopped`] so `status` can tell "torn down
    /// because nobody wanted it" from "torn down because it kept dying".
    Failed,
}

/// The entity a watch is *for*, as a sum type so each kind carries only the
/// fields it actually uses.
///
/// The durable `watch` row is flat (`kind`, `repo`, `pr`, `publish_count`) — a
/// legacy shape shared by both kinds. This domain type is the parsed form of
/// that row, minted at the storage boundary ([`crate::storage`]'s `build_watch`)
/// so nonsense states are unrepresentable downstream: a `github-pr` cannot carry
/// a publish count, and a `stub` cannot carry a PR number. Everything above
/// storage destructures the variant instead of re-deriving meaning from the flat
/// `repo`/`pr` fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WatchTarget {
    /// A GitHub pull request: `owner/repo` + PR number.
    GithubPr { repo: String, pr: u64 },
    /// A stub publisher: a label + how many events to publish (`0` = unbounded).
    Stub { label: String, count: u64 },
}

impl WatchTarget {
    /// The discriminant persisted in the `watch.kind` column.
    pub fn kind(&self) -> WatchKind {
        match self {
            WatchTarget::GithubPr { .. } => WatchKind::GithubPr,
            WatchTarget::Stub { .. } => WatchKind::Stub,
        }
    }

    /// The flat `repo` column: `owner/repo` for github, the label for stub.
    pub fn repo_column(&self) -> &str {
        match self {
            WatchTarget::GithubPr { repo, .. } => repo,
            WatchTarget::Stub { label, .. } => label,
        }
    }

    /// The flat `pr` column: the PR number for github, `0` (unused) for stub.
    pub fn pr_column(&self) -> u64 {
        match self {
            WatchTarget::GithubPr { pr, .. } => *pr,
            WatchTarget::Stub { .. } => 0,
        }
    }

    /// The flat `publish_count` column: `0` (unused) for github, the count for stub.
    pub fn count_column(&self) -> u64 {
        match self {
            WatchTarget::GithubPr { .. } => 0,
            WatchTarget::Stub { count, .. } => *count,
        }
    }
}

/// What to persist when creating or reusing a watch.
///
/// The creation model: no `id` (the store assigns it) and no lifecycle state
/// (a fresh watch is always [`WatchState::Desired`]). Keeping this distinct
/// from [`Watch`] means a caller cannot smuggle a bogus id or a `Running` state
/// into an insert.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatchSpec {
    /// The entity this watch is for (kind + its per-kind fields).
    pub target: WatchTarget,
    /// Desired poll interval. Stored with millisecond precision so a sub-second
    /// stub interval survives the round trip.
    pub interval: Duration,
}

/// A watch as stored — the read model returned from queries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Watch {
    pub id: WatchId,
    /// The entity this watch is for (kind + its per-kind fields).
    pub target: WatchTarget,
    pub interval: Duration,
    pub state: WatchState,
}

/// What ending a session removed (the SessionEnd teardown, card 11).
///
/// A session's departure drops both halves of its state in one transaction: its
/// `subscription` rows (so it is woken about nothing more) and its
/// `watch_interest` rows (so the card-08 refcount can stop adapters nobody else
/// wants). `emptied_watches` are exactly the watches whose interest thereby fell
/// to zero — the ones whose adapter the caller must now stop, mirroring
/// [`crate::storage::Storage::sweep_stale_interests`]'s return. The counts are
/// kept for an honest, body-free teardown log.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct EndSessionOutcome {
    /// How many `subscription` rows were removed for the session.
    pub subscriptions_removed: u64,
    /// How many `watch_interest` rows were removed for the session.
    pub interests_removed: u64,
    /// Watches whose interest reached zero because this session left — the
    /// caller stops each one's adapter (design/01 rule 5).
    pub emptied_watches: Vec<WatchId>,
}

/// One topic as discovery (`mailbox topics`) sees it: what it is, who listens,
/// and how much traffic it has carried.
///
/// A pure projection of the durable tables — there is no `topic` table, a topic
/// exists precisely because something subscribed to it or published to it — so
/// this is a read model with no creation twin. `last_event` is `None` exactly
/// when `events` is 0 (a topic that only has subscribers), which is why it is an
/// `Option` rather than a sentinel timestamp.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicSummary {
    pub topic: mailbox_protocol::Topic,
    /// Sessions currently subscribed (the fan-out width).
    pub subscribers: u64,
    /// Events durably retained on the topic.
    pub events: u64,
    /// Timestamp of the newest event, or `None` on a topic with no events.
    pub last_event: Option<mailbox_protocol::Timestamp>,
}

/// A page of events plus the cursor to continue from.
///
/// Carrying `next` explicitly means a caller never derives paging state from
/// the event list itself (mirrors `mailbox_protocol::ReadResponse`).
#[derive(Debug, Clone, PartialEq)]
pub struct ReadPage {
    pub events: Vec<Event>,
    pub next: Cursor,
}

/// Which caller is asking to subscribe — the axis the tombstone guard branches on.
///
/// Threaded from the request edge down to `do_subscribe_and_baseline` (exactly
/// like `now_ms`), never derived from a clock or a global, so the writer stays a
/// pure function of its inputs and a test can drive either path directly.
///
/// # Why the guard is scoped to one path (ADR-0007)
///
/// The resurrection race the tombstone defends against has ONE culprit: the
/// automatic inbox re-registration `harness arm` fires on every SessionStart/Stop.
/// That arm is asynchronous and can land on the writer *just after* `SessionEnd`'s
/// delete, permanently re-creating a dead session's inbox. An EXPLICIT
/// `subscribe`/`watch`, by contrast, is issued synchronously from a live turn — it
/// completes before that turn's `SessionEnd` — so it can never BE the doomed racing
/// command. It is therefore legitimate proof-of-life and must not be refused; on the
/// contrary, it clears any stale tombstone so the resumed session is healthy again.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SubscribeKind {
    /// The automatic `harness arm` inbox auto-registration (the ONLY guarded path):
    /// a subscribe within the tombstone window is refused rather than resurrect a
    /// just-ended inbox.
    AutoInbox,
    /// An explicit, user/agent-initiated `subscribe` or `watch` (unguarded): it
    /// proceeds and clears any existing tombstone for the session id.
    Explicit,
}

/// What an atomic subscribe-and-baseline did.
///
/// A named enum rather than an ambiguous `Option<Offset>` so the two
/// distinct-but-both-cursorless outcomes ("already subscribed, cursor left
/// alone" vs "newly subscribed on an empty topic, no head to baseline to") can
/// never be confused at a call site or in a log line. The baseline policy that
/// produces this lives in [`crate::storage::Storage::subscribe_and_baseline`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubscribeOutcome {
    /// The session was already subscribed; this call was an idempotent no-op and
    /// deliberately left the delivery cursor untouched (re-subscribing must not
    /// skip events the session has not yet read).
    AlreadySubscribed,
    /// A fresh subscription was created. The delivery cursor was baselined to the
    /// topic's current head so history is not replayed: `Some(head)` when the
    /// topic already had events (the session starts strictly after `head`), or
    /// `None` when the topic was empty (no head yet, so the next read starts at
    /// the oldest event — which will itself be a post-subscribe publish).
    Subscribed { baseline: Option<Offset> },
    /// Refused: the session ended within the tombstone guard window
    /// (`SUBSCRIBE_TOMBSTONE_GUARD_MS`), so re-subscribing it now would resurrect a
    /// session that has just gone away — the arm-vs-cleanup race (ADR-0007). No
    /// subscription row was created and no cursor was touched; the honest outcome
    /// is reported rather than a silent success, so a caller (e.g. the harness
    /// inbox registrar) can log that the session was recently ended.
    RefusedSessionRecentlyEnded,
}
