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

/// Identity of a Claude/Codex session that expresses interest in a topic or
/// watch. Sourced from the harness (hook `session_id`); the bridge treats it as
/// an opaque label. Branded so it cannot be swapped with a `Topic` or any other
/// string at a call site.
///
/// The inner string is private and minted only through [`SessionId::new`] — the
/// same "opaque, constructed at the edge" story as [`WatchId`] — so a call site
/// cannot reach in and treat it as a bare `String`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SessionId(String);

impl SessionId {
    /// Wrap a session label coming from the harness. Accepts anything
    /// string-like so call sites need not pre-convert.
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// Borrow as a string slice (for binding into SQL).
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

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
/// Newtype rather than a bare `i32` so it reads as a pid at call sites and
/// cannot be confused with a `WatchId` or an offset.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Pid(pub i32);

/// What kind of external entity a watch polls.
///
/// A closed enum (not a free string) so adding a watch kind forces every match
/// site to handle it. Only `github-pr` exists for the MVP.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WatchKind {
    /// A GitHub pull request poller (`github-pr`).
    GithubPr,
}

impl WatchKind {
    /// The stable string persisted in the `watch.kind` column and used as part
    /// of the `(kind, repo, pr)` identity. Pinned here so a rename can't
    /// silently change what an existing row means.
    pub fn as_str(self) -> &'static str {
        match self {
            WatchKind::GithubPr => "github-pr",
        }
    }

    /// Parse the persisted string back into a kind.
    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "github-pr" => Some(WatchKind::GithubPr),
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
    /// Torn down (last interest gone, or crashed and given up).
    Stopped,
}

/// What to persist when creating or reusing a watch.
///
/// The creation model: no `id` (the store assigns it) and no lifecycle state
/// (a fresh watch is always [`WatchState::Desired`]). Keeping this distinct
/// from [`Watch`] means a caller cannot smuggle a bogus id or a `Running` state
/// into an insert.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatchSpec {
    pub kind: WatchKind,
    /// `owner/repo` string. Opaque to storage; the adapter/CLI validates it.
    pub repo: String,
    /// Pull-request number.
    pub pr: u64,
    /// Desired poll interval.
    pub interval: Duration,
}

/// A watch as stored — the read model returned from queries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Watch {
    pub id: WatchId,
    pub kind: WatchKind,
    pub repo: String,
    pub pr: u64,
    pub interval: Duration,
    pub state: WatchState,
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
