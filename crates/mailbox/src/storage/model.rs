//! Domain types the storage handle speaks in.
//!
//! These sit alongside the `mailbox-protocol` newtypes (`Topic`, `Offset`,
//! `EventId`, …) and cover the bridge-internal concepts that never appear on
//! the wire: sessions, watches, interest, and baselines. They follow the
//! type-driven rules from AGENTS.md — brand the ids, model watch lifecycle as a
//! named enum (not a stringly column), and keep the creation model
//! ([`WatchSpec`]) separate from the read model ([`Watch`]) so callers cannot
//! invent an id or a lifecycle state for a watch that does not exist yet.

use std::num::NonZeroU32;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use mailbox_protocol::{Cursor, Event, GithubLogin, Offset, SlackFilters, SlackWatch};
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
    /// New top-level messages in a Slack channel (`slack-channel`, design/02).
    SlackChannel,
    /// New replies in one Slack thread (`slack-thread`, design/02).
    SlackThread,
    /// PRs a GitHub user was newly asked to review (`github-review-requests`,
    /// design/03).
    GithubReviewRequests,
    /// @mentions of the Slack app's bot and DMs to it (`slack-bot`, design/04).
    SlackBot,
}

impl WatchKind {
    /// The stable string persisted in the `watch.kind` column and used as part
    /// of the `(kind, repo, pr)` identity. Pinned here so a rename can't
    /// silently change what an existing row means.
    pub fn as_str(self) -> &'static str {
        match self {
            WatchKind::GithubPr => "github-pr",
            WatchKind::Stub => "stub",
            WatchKind::SlackChannel => "slack-channel",
            WatchKind::SlackThread => "slack-thread",
            WatchKind::GithubReviewRequests => "github-review-requests",
            WatchKind::SlackBot => "slack-bot",
        }
    }

    /// Parse the persisted string back into a kind.
    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "github-pr" => Some(WatchKind::GithubPr),
            "stub" => Some(WatchKind::Stub),
            "slack-channel" => Some(WatchKind::SlackChannel),
            "slack-thread" => Some(WatchKind::SlackThread),
            "github-review-requests" => Some(WatchKind::GithubReviewRequests),
            "slack-bot" => Some(WatchKind::SlackBot),
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
    /// (card 08): it exceeded the restart policy's consecutive-failure budget.
    /// Nothing is published (ADR-0028), and the sweep retries it while an
    /// interested session is alive (ADR-0011). Distinct from [`Stopped`] so `status` can tell "torn down
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
    /// A Slack channel or thread. Stored with its [`SlackWatch::key`] in the
    /// `repo` column and `0` in the others, the same way a stub stores its label.
    ///
    /// `skip` is the watch's filters (ADR-0029): like a stub's count, part of what
    /// the adapter is told to do, not part of the watch's identity.
    Slack {
        watch: SlackWatch,
        skip: SlackFilters,
    },
    /// The review requests of one GitHub user (design/03). Stored with the login
    /// in the `repo` column and `0` in the others, as a stub stores its label.
    GithubReviewRequests { login: GithubLogin },
    /// Every @mention of the Slack app's bot and every DM to it (design/04).
    /// There is one per bridge, so its `repo` column holds the fixed
    /// [`SLACK_BOT_KEY`]. Slack pushes to it, so its interval is always zero.
    SlackBot { skip: SlackFilters },
}

/// The `repo` column of the one bot watch. It names what is watched: the bot
/// whose tokens are in the Keychain.
pub const SLACK_BOT_KEY: &str = "bot";

impl WatchTarget {
    /// The discriminant persisted in the `watch.kind` column.
    pub fn kind(&self) -> WatchKind {
        match self {
            WatchTarget::GithubPr { .. } => WatchKind::GithubPr,
            WatchTarget::Stub { .. } => WatchKind::Stub,
            WatchTarget::Slack { watch, .. } => slack_kind(watch),
            WatchTarget::GithubReviewRequests { .. } => WatchKind::GithubReviewRequests,
            WatchTarget::SlackBot { .. } => WatchKind::SlackBot,
        }
    }

    /// The flat `repo` column: `owner/repo` for github, the label for stub, the
    /// watch key (`<channel>` or `<channel>/<thread-ts>`) for Slack, the login for
    /// review requests.
    pub fn repo_column(&self) -> String {
        match self {
            WatchTarget::GithubPr { repo, .. } => repo.clone(),
            WatchTarget::Stub { label, .. } => label.clone(),
            WatchTarget::Slack { watch, .. } => watch.key(),
            WatchTarget::GithubReviewRequests { login } => login.to_string(),
            WatchTarget::SlackBot { .. } => SLACK_BOT_KEY.to_string(),
        }
    }

    /// The flat `pr` column: the PR number for github, `0` (unused) for stub.
    pub fn pr_column(&self) -> u64 {
        match self {
            WatchTarget::GithubPr { pr, .. } => *pr,
            WatchTarget::Stub { .. }
            | WatchTarget::Slack { .. }
            | WatchTarget::GithubReviewRequests { .. }
            | WatchTarget::SlackBot { .. } => 0,
        }
    }

    /// The flat `interval_ms` column: what was asked for, except for the bot
    /// watch, which Slack pushes to and which is always written as zero, so the
    /// write side cannot produce a row the reader calls corrupt.
    pub fn interval_column(&self, requested: Duration) -> Duration {
        match self {
            WatchTarget::SlackBot { .. } => Duration::ZERO,
            WatchTarget::GithubPr { .. }
            | WatchTarget::Stub { .. }
            | WatchTarget::Slack { .. }
            | WatchTarget::GithubReviewRequests { .. } => requested,
        }
    }

    /// The flat `publish_count` column: `0` (unused) for github, the count for stub.
    pub fn count_column(&self) -> u64 {
        match self {
            WatchTarget::GithubPr { .. }
            | WatchTarget::Slack { .. }
            | WatchTarget::GithubReviewRequests { .. }
            | WatchTarget::SlackBot { .. } => 0,
            WatchTarget::Stub { count, .. } => *count,
        }
    }

    /// The watch's filters, for the kinds that have any.
    pub fn skip(&self) -> Option<&SlackFilters> {
        match self {
            WatchTarget::Slack { skip, .. } | WatchTarget::SlackBot { skip } => Some(skip),
            WatchTarget::GithubPr { .. }
            | WatchTarget::Stub { .. }
            | WatchTarget::GithubReviewRequests { .. } => None,
        }
    }

    /// The `filters` column: the Slack filters as a JSON array, `[]` for the
    /// kinds that have none. This is `SlackFilters`' serde form, so that form is
    /// now persisted: changing it is a schema migration, and a row an older
    /// reader cannot parse fails as corrupt rather than running unfiltered.
    pub fn filters_column(&self) -> String {
        match self {
            WatchTarget::GithubPr { .. }
            | WatchTarget::Stub { .. }
            | WatchTarget::GithubReviewRequests { .. } => "[]".to_string(),
            WatchTarget::Slack { skip, .. } | WatchTarget::SlackBot { skip } => {
                serde_json::to_string(skip).expect("Slack filters always serialize")
            }
        }
    }
}

/// The watch kind of a Slack target, which its shape alone decides.
pub fn slack_kind(watch: &SlackWatch) -> WatchKind {
    match watch {
        SlackWatch::Channel { .. } => WatchKind::SlackChannel,
        SlackWatch::Thread { .. } => WatchKind::SlackThread,
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

/// What recording a session's interest in a watch did (ADR-0029).
///
/// A watch's filters belong to every session watching it, because one adapter
/// serves them all. So a re-watch asking for different filters is refused while
/// another session holds interest, rather than changing what that session hears
/// without telling it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecordInterestOutcome {
    /// The watch exists (created or reused) and this session's interest is on it.
    Recorded {
        watch: WatchId,
        /// Interested sessions, this one included.
        interest: u64,
        filter_change: FilterChange,
        /// The filters now stored on the watch, read back after the write;
        /// `None` for a kind that has none.
        skip: Option<SlackFilters>,
    },
    /// Nothing was written: the watch has different filters and other sessions
    /// are interested in it. `current` is the filter set it has.
    FiltersInUse {
        current: SlackFilters,
        other_sessions: u64,
    },
}

/// Whether recording a watch changed its filters. A named pair rather than a
/// bool so the caller that must restart the adapter on a change reads as such.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterChange {
    /// A new watch, or the same filters it already had.
    Unchanged,
    /// The stored filters were replaced; a running adapter still has the old ones.
    Replaced,
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
/// A session's departure takes both halves of its state out of the live tables in
/// one transaction: its `subscription` rows (so it is woken about nothing more) and
/// its `watch_interest` rows (so the card-08 refcount can stop adapters nobody else
/// wants). They are SUSPENDED, not forgotten — copied aside so a resume of the same
/// session id can restore them (ADR-0026). `emptied_watches` are exactly the watches
/// whose interest thereby fell to zero — the ones whose adapter the caller must now
/// stop, mirroring [`crate::storage::Storage::sweep_stale_interests`]'s return. The
/// counts are kept for an honest, body-free teardown log.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct EndSessionOutcome {
    /// How many `subscription` rows left the live table (and were suspended).
    pub subscriptions_removed: u64,
    /// How many `watch_interest` rows left the live table (and were suspended).
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

/// Why a history read named no readable slice of the log.
///
/// One reason, so a unit struct rather than an enum with a single variant: the
/// caller is nobody AND named no topic. A business error in the return type of
/// [`HistoryScope::from_parts`] rather than a panic or a silent default, because
/// the honest answer is a refusal — see that constructor for why.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NoHistoryScope;

impl std::fmt::Display for NoHistoryScope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Neutral on purpose: this reaches every client of the socket, and only the
        // CLI has a `--topic` flag to suggest. The CLI adds that hint itself.
        write!(f, "history needs a topic when the caller has no session")
    }
}

impl std::error::Error for NoHistoryScope {}

/// Which slice of the durable log a history read covers (ADR-0032).
///
/// Modelled as a sum rather than two nullable fields because the two shapes ask
/// genuinely different questions and answer them with different SQL: "everything
/// addressed to me, newest first across topics" versus "this one topic". A pair
/// of `Option`s would make "no session and no topic" — which has no answer —
/// spellable everywhere downstream; as a sum it cannot be spelled past
/// [`from_parts`](HistoryScope::from_parts), which is the one place it is refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HistoryScope {
    /// Every topic `session` is subscribed to, ordered newest-first ACROSS all of
    /// them. Read-state is reported for every entry, because a subscription is
    /// exactly what gives a session read-state on a topic.
    Subscribed { session: SessionId },
    /// One named topic, whether or not anyone subscribes to it — mailboxes are
    /// not private (ADR-0032), so `agent.<someone-else>` is a legal target here.
    ///
    /// `reader` is the CALLER, not a filter: it decides whether read-state can be
    /// reported, and only when that caller is subscribed to this topic. A caller
    /// with no subscription has no cursor on it, so there is nothing honest to
    /// say and [`HistoryEntry::read`] stays `None`.
    Topic {
        topic: mailbox_protocol::Topic,
        reader: Option<SessionId>,
    },
}

impl HistoryScope {
    /// Build a scope from the two optional halves a caller supplies, or refuse.
    ///
    /// This is the whole of history's scoping rule, kept in ONE place because more
    /// than one client reaches it: the CLI is the usual caller, but `Request::History`
    /// is equally reachable by anything speaking the socket protocol, and a rule
    /// enforced at the CLI edge would simply not apply to those. Living on the type
    /// it produces also means it is unit-testable without a daemon.
    ///
    /// A named topic always wins, and a session alongside it is the READER, not a
    /// second filter — that is what lets someone look at a peer's inbox while still
    /// being told their own read-state where they have one.
    ///
    /// Neither half is a refusal, not a default. "The newest events across the
    /// topics belonging to nobody" has no answer, and both available defaults —
    /// every topic, or none — would be a guess returned as a result.
    pub fn from_parts(
        topic: Option<mailbox_protocol::Topic>,
        session: Option<SessionId>,
    ) -> Result<Self, NoHistoryScope> {
        match (topic, session) {
            (Some(topic), reader) => Ok(HistoryScope::Topic { topic, reader }),
            (None, Some(session)) => Ok(HistoryScope::Subscribed { session }),
            (None, None) => Err(NoHistoryScope),
        }
    }
}

/// The oldest stamp a history read admits, given the reader's clock and a lookback.
///
/// Pure, with `now_ms` passed in, so the edge cases are testable without a daemon
/// or a real clock. The daemon is the true edge for this value: the CLI bounds what
/// it sends, but any socket client can send any `u64`.
///
/// Saturating at every step, never wrapping. A lookback longer than the clock has
/// run yields a cutoff before every stored event — so it reads the whole log —
/// rather than wrapping into a FUTURE cutoff, which would return an empty page that
/// reads as "nothing happened".
pub fn history_cutoff(now_ms: i64, within_ms: Option<u64>) -> Option<mailbox_protocol::Timestamp> {
    within_ms.map(|within| {
        let within = i64::try_from(within).unwrap_or(i64::MAX);
        mailbox_protocol::Timestamp(now_ms.saturating_sub(within))
    })
}

/// One event as `mailbox history` reports it: the stored event, plus the caller's
/// read-state on it when there is one to report (ADR-0032).
///
/// A read model with no creation twin, like [`TopicSummary`] — only a query mints
/// one. It is deliberately NOT [`crate::bus::Delivery`]: that type's invariant is
/// a completed read whose cursors have already advanced, and history advances
/// nothing, so reusing it would state something untrue in the type system.
#[derive(Debug, Clone, PartialEq)]
pub struct HistoryEntry {
    pub event: mailbox_protocol::Event,
    /// Whether the caller's delivery cursor is already at or past this event.
    ///
    /// `None` means "no read-state to report" — no session asked, or the session
    /// that asked is not subscribed to this topic — and is deliberately distinct
    /// from `Some(false)` ("you are subscribed and this is still unread"). An
    /// unsubscribed caller reading a peer's inbox must not be told that peer's
    /// mail is unread, nor that it is read.
    pub read: Option<bool>,
}

/// How many subjects a caller of `unread_digest` wants per topic.
///
/// A named choice rather than a count with a magic zero: `status` prints counts and
/// the wake describes them, and `unread_digest(session, 0)` at a call site reads like
/// a forgotten argument rather than the deliberate decision it is. `NonZeroU32` means
/// "some, but none" cannot be spelled at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubjectBudget {
    /// Report what is unread, not what it is about.
    CountsOnly,
    /// Describe up to this many of the newest unread events per topic.
    Newest(NonZeroU32),
}

impl SubjectBudget {
    /// The budget as the SQL's per-topic rank cutoff. Private to storage: zero is a
    /// fine *query parameter* — the subjects are LEFT JOINed onto the counts, so at
    /// zero every topic still reports its count — it is only a bad thing to make a
    /// caller write.
    pub(super) fn rank_cutoff(self) -> u32 {
        match self {
            SubjectBudget::CountsOnly => 0,
            SubjectBudget::Newest(n) => n.get(),
        }
    }
}

/// What one subscribed topic has waiting for a session: how much, and what the
/// newest of it is about.
///
/// The read model behind the wake wire (ADR-0022) and `mailbox status`. A read
/// model with no creation twin, like [`TopicSummary`]: only a query mints one.
///
/// `unread` counts EVERY unread event on the topic, while `subjects` holds at most
/// the newest few — and only those whose publisher gave one. So `subjects.len()`
/// is a floor on `unread`, never a restatement of it, and the two are deliberately
/// not folded into one list: "6 unread" plus three descriptions is an honest
/// summary, whereas three descriptions alone would read as three events.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicDigest {
    pub topic: mailbox_protocol::Topic,
    /// Events on this topic beyond the session's delivery cursor.
    pub unread: u64,
    /// Subjects of the newest unread events that have one, newest first.
    pub subjects: Vec<mailbox_protocol::Subject>,
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

/// What restoring a session's suspended state did (ADR-0026).
///
/// A sum type because the refusal and the success carry different facts: a refused
/// resume touched nothing, so it has no counts to misread as "restored nothing".
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResumeOutcome {
    /// The session's suspended rows (if any) are live again.
    Resumed {
        /// How many suspended subscriptions were restored.
        subscriptions_restored: u64,
        /// How many suspended interests were restored.
        interests_restored: u64,
        /// EVERY watch the session now has a live interest in — restored or not —
        /// for the caller to ensure is running. Not just the restored ones: an
        /// interest that survived a daemon restart can belong to a watch the
        /// startup reconcile stopped because the session was not running yet.
        watches: Vec<WatchId>,
    },
    /// Refused: the session ended within the tombstone guard window, the same
    /// guard the automatic inbox registration honours (ADR-0007). Nothing was
    /// restored; the suspended rows are left for the next `SessionStart`.
    RefusedSessionRecentlyEnded,
}

/// What expiring long-suspended state removed (ADR-0026). Counts only.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ExpiredSuspensions {
    pub interests: u64,
    pub subscriptions: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    use mailbox_protocol::{Timestamp, Topic};

    /// The whole of history's scoping rule. A named topic wins and KEEPS the
    /// session as its reader — dropping it would silently withhold the caller's
    /// own read-state on a topic they do subscribe to.
    #[test]
    fn from_parts_resolves_every_combination_or_refuses() {
        let topic = Topic::parse("team.ci").unwrap();
        let session = SessionId::new("s-a");

        assert_eq!(
            HistoryScope::from_parts(Some(topic.clone()), Some(session.clone())),
            Ok(HistoryScope::Topic {
                topic: topic.clone(),
                reader: Some(session.clone()),
            })
        );
        assert_eq!(
            HistoryScope::from_parts(Some(topic.clone()), None),
            Ok(HistoryScope::Topic {
                topic,
                reader: None
            })
        );
        assert_eq!(
            HistoryScope::from_parts(None, Some(session.clone())),
            Ok(HistoryScope::Subscribed { session })
        );
        assert_eq!(HistoryScope::from_parts(None, None), Err(NoHistoryScope));
    }

    /// The refusal reaches every socket client, so it must not name a CLI flag;
    /// the CLI adds its own remedy.
    #[test]
    fn the_refusal_is_client_neutral() {
        let message = NoHistoryScope.to_string();
        assert!(message.contains("needs a topic"), "{message}");
        assert!(!message.contains("--"), "no CLI flag names here: {message}");
    }

    #[test]
    fn cutoff_subtracts_the_lookback_from_the_given_clock() {
        assert_eq!(history_cutoff(10_000, None), None, "no lookback, no floor");
        assert_eq!(history_cutoff(10_000, Some(3_000)), Some(Timestamp(7_000)));
    }

    /// A lookback past what the clock can represent must still read EVERYTHING.
    /// Wrapping instead would produce a future cutoff and an empty page that looks
    /// exactly like a quiet log.
    #[test]
    fn an_enormous_lookback_saturates_rather_than_wrapping() {
        let cutoff = history_cutoff(1_700_000_000_000, Some(u64::MAX)).unwrap();
        assert!(cutoff.0 < 0, "before every stored stamp: {cutoff:?}");
        let cutoff = history_cutoff(i64::MIN + 5, Some(u64::MAX)).unwrap();
        assert_eq!(cutoff, Timestamp(i64::MIN), "saturates, never wraps");
    }
}
