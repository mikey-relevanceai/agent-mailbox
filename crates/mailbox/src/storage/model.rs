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

use mailbox_protocol::{Cursor, Event, Offset};
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

/// A page of events plus the cursor to continue from.
///
/// Carrying `next` explicitly means a caller never derives paging state from
/// the event list itself (mirrors `mailbox_protocol::ReadResponse`).
#[derive(Debug, Clone, PartialEq)]
pub struct ReadPage {
    pub events: Vec<Event>,
    pub next: Cursor,
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
}
