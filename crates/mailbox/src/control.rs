//! The CLI ↔ `serve` daemon control protocol.
//!
//! # Why this is daemon-local, not in `mailbox-protocol`
//!
//! `mailbox-protocol` is the **adapter-facing** pub/sub protocol: the small,
//! versioned [`Message`](mailbox_protocol::Message) set an adapter in any
//! language speaks to publish/subscribe/read over a persistent connection. This
//! module is a different surface — the CLI's one-shot control channel to its own
//! daemon — so it lives in the `mailbox` binary, keeping the adapter protocol
//! clean (AGENTS.md hard boundary: "adapters never import bridge internals").
//!
//! Two forces make a small daemon-local envelope the right call rather than
//! reusing the `Message` enum verbatim on the socket:
//!
//! 1. **Control ops aren't adapter-facing.** `watch` / `unwatch` / `status` are
//!    CLI/daemon operations with no place in the adapter pub/sub set.
//! 2. **One-shot clients must carry `SessionId` per request.** The `Message` set
//!    is designed for a *persistent* subscriber connection where session identity
//!    is implicit in the connection. Our clients connect, send exactly one
//!    request, read one reply, and disconnect (ADR-0004), so each mutating
//!    request names its session explicitly — a field the persistent-connection
//!    `Message` types deliberately omit.
//!
//! # What we DO reuse
//!
//! The load-bearing reuse is the **domain types** and the **compatibility rule**,
//! not the enum shape: publish/subscribe/read payloads are built from
//! `mailbox_protocol` types, session identity is the branded
//! [`SessionId`](mailbox::storage::SessionId) carried transparently, and every
//! frame carries the same `version` field checked with the same reject-newer
//! rule ([`check_version`]). So the socket speaks the same versioned NDJSON
//! dialect as the adapter protocol; only the message set differs.
//!
//! Bodies stay opaque `serde_json::Value` and are never interpreted here
//! (ADR-0001).

use serde::{Deserialize, Serialize};
use serde_json::Value;

use mailbox_protocol::{
    AdapterId, Event, EventId, IncompatibleVersion, Offset, PROTOCOL_VERSION, Subject, Topic,
    check_version, inbox_topic,
};

use mailbox::storage::{
    SessionId, SubscribeKind, SubscribeOutcome, TopicSummary, WatchKind, WatchState,
};
use mailbox::watch::{SessionResumed, SessionStatusView, StatusView, UnwatchOutcome, WatchEntry};

/// A one-shot request from a CLI client to the `serve` daemon.
///
/// Internally tagged by `"op"` so a request can be dispatched on one well-known
/// key, mirroring the `mailbox-protocol` `Message` convention.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Request {
    /// Publish an event to a topic. The daemon stamps the timestamp, exactly as the
    /// durable bridge does; provenance is the [`AdapterId`] (a name, not authority).
    ///
    /// It carries no caller: **every publisher is the same publisher** (ADR-0018).
    /// An adapter, an agent and a script an agent spawned all append to the topic and
    /// wake every subscriber, author included. The frame used to carry an optional
    /// `session` so the daemon could refuse a publish from a caller with unread mail
    /// on the topic; that rule is deleted, and an extra field a rule no longer reads
    /// is a place for the rule to grow back.
    Publish {
        topic: Topic,
        adapter: AdapterId,
        body: Value,
        /// One line describing what this event is, for the wake wire (ADR-0022).
        /// Optional at every layer: an event with nothing worth saying wakes its
        /// subscribers with the topic and a count.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        subject: Option<Subject>,
    },
    /// Subscribe `session` to `topic` (baseline-on-subscribe). `kind` distinguishes
    /// an explicit user/agent subscribe from the automatic `harness arm` inbox
    /// re-registration, which is the only path the tombstone guard scopes to
    /// (ADR-0007); it is carried on the wire because both use this one variant.
    Subscribe {
        session: SessionId,
        topic: Topic,
        kind: SubscribeKind,
    },
    /// Unsubscribe `session` from `topic`.
    Unsubscribe { session: SessionId, topic: Topic },
    /// Read `session`'s unread events across all its topics (advance-on-read).
    Read {
        session: SessionId,
        limit: Option<u32>,
    },
    /// Declare `session`'s interest in a GitHub PR watch and subscribe it to the
    /// PR topic. Records the watch + interest; does NOT spawn the adapter (card 08).
    Watch {
        session: SessionId,
        target: GithubPrTarget,
        interval_secs: u64,
    },
    /// Drop `session`'s interest in a GitHub PR watch and unsubscribe it.
    Unwatch {
        session: SessionId,
        target: GithubPrTarget,
    },
    /// Declare `session`'s interest in a `stub` watch and subscribe it to the
    /// `stub.<label>` topic. The stub carries its own params (interval in
    /// milliseconds, publish count) rather than the github `interval_secs`, so it
    /// is a distinct variant rather than an overloaded `Watch`.
    WatchStub {
        session: SessionId,
        label: String,
        interval_ms: u64,
        count: u64,
    },
    /// Drop `session`'s interest in a `stub` watch and unsubscribe it.
    UnwatchStub { session: SessionId, label: String },
    /// Report watches (interest + child pid), and — when the caller is a session —
    /// that session's subscriptions and unread counts.
    ///
    /// **`session` is OPTIONAL**, because it buys only half the answer. The watch
    /// table is bridge-global: every caller sees the same rows, so a caller with no
    /// session still has a question worth answering ("what is the bridge doing?").
    /// `None` means "report the bridge's half only" — the daemon then omits the
    /// session fields rather than answering them for nobody.
    Status { session: Option<SessionId> },
    /// Message a peer agent: publish `body` to the target's inbox topic, stamped
    /// with the sender's id (card 16). `to` is a [`SessionId`], not a `Topic`: the
    /// daemon mints the inbox topic through the one canonical constructor, so a
    /// caller cannot address a *non*-inbox topic through this op.
    ///
    /// The body is a JSON **object** on the wire (a `Map`, not a `Value`), so
    /// "there is somewhere to stamp `from`" is a type-level guarantee rather than
    /// a runtime check. It stays opaque to the bus either way (ADR-0001).
    ///
    /// **`from` is OPTIONAL**, because the caller's identity is not what `send`
    /// exists for: its job is to deliver, and `from` is only the reply address it
    /// stamps on the way. An agent messaging a peer has one and it is stamped; a
    /// HUMAN poking an agent from an ordinary terminal has none, and refusing to
    /// deliver would be trading a working poke for a missing courtesy. `None` means
    /// "this message has no reply address", which the daemon states by omitting the
    /// `from` key entirely rather than inventing a placeholder.
    Send {
        from: Option<SessionId>,
        to: SessionId,
        body: serde_json::Map<String, Value>,
        /// What the sender says the message is about, if anything. The daemon
        /// composes the delivered subject from this and the verified `from`
        /// ([`mailbox::agents::send`]) — a sender cannot state its own identity
        /// here any more than it can in the body.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        subject: Option<Subject>,
    },
    /// List the sessions with a registered agent inbox (card-16 discovery).
    /// `session` is the *caller*, so the reply can mark which agent is itself —
    /// and it is OPTIONAL for that reason: marking a row is all it does. A human
    /// listing the fleet from a terminal is not one of the agents, so with `None`
    /// every agent is listed and no row is marked.
    Agents { session: Option<SessionId> },
    /// List known topics with subscriber/event counts, optionally filtered to a
    /// prefix (card-16 discovery).
    Topics { prefix: Option<String> },
    /// End a session (the harness `SessionEnd` hook, card 11): suspend all its
    /// subscriptions and interests, stopping any adapter whose last interest it
    /// held. No topic here — it tears down everything for the session at once.
    EndSession { session: SessionId },
    /// Resume a session (the harness `SessionStart` hook, ADR-0026): restore what
    /// `EndSession` suspended and ensure its watches are running. A no-op beyond
    /// the ensure for a session that was never suspended.
    ResumeSession { session: SessionId },
}

/// Identity of a GitHub PR to watch/unwatch. Only `github-pr` exists for the MVP;
/// modelled as its own struct so a second watch kind is an additive change rather
/// than an overloaded string. The daemon validates it into a
/// [`GithubPr`](mailbox_protocol::GithubPr) at the edge.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GithubPrTarget {
    pub owner: String,
    pub repo: String,
    pub number: u64,
}

/// The daemon's reply to a [`Request`]. Internally tagged by `"result"`.
///
/// Business failures come back as [`Response::Error`] — a value the client
/// surfaces — never as a dropped connection, so "the command failed" and "the
/// bridge is down" stay distinguishable (the latter is a client-side connect
/// error; see [`crate::client`]).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "result", rename_all = "snake_case")]
pub enum Response {
    /// A publish committed at these durable coordinates (≈ `Ack::Published`).
    Published { id: EventId, offset: Offset },
    /// A subscribe took effect, with its per-topic outcome.
    Subscribed {
        topic: Topic,
        outcome: SubscribeState,
    },
    /// An unsubscribe took effect (idempotent).
    Unsubscribed { topic: Topic },
    /// A read returned these unread events (cursors already advanced).
    Read { events: Vec<Event> },
    /// A watch was recorded: this session's interest is attached and it is
    /// subscribed to the PR topic. `interest` is the refcount driving card-08
    /// adapter supervision.
    Watched {
        topic: Topic,
        interest: u64,
        subscribe: SubscribeState,
    },
    /// A watch interest op completed; `outcome` distinguishes "dropped" from
    /// "no such watch". The topic was unsubscribed either way.
    Unwatched {
        topic: Topic,
        outcome: UnwatchResultWire,
    },
    /// A status snapshot.
    Status(StatusReport),
    /// A message was published to a peer's inbox. Carries the durable coordinates
    /// (like `Published`) plus the resolved target, so the sender can log exactly
    /// where its message landed.
    Sent {
        to: SessionId,
        topic: Topic,
        id: EventId,
        offset: Offset,
    },
    /// The registered agent inboxes (card-16 discovery).
    Agents { agents: Vec<AgentSummary> },
    /// The known topics (card-16 discovery).
    Topics { topics: Vec<TopicStatus> },
    /// A session was resumed, or refused as having ended moments ago (ADR-0026).
    SessionResumed { outcome: ResumeState },
    /// A session was ended (card 11): counts of what its teardown removed from the
    /// live tables. The `dropped` names predate suspension (ADR-0026) and are kept
    /// so an older client and a newer daemon still understand each other.
    SessionEnded {
        subscriptions_dropped: u64,
        interests_dropped: u64,
        adapters_stopped: u64,
    },
    /// The command was well-formed but could not be serviced (bad topic, storage
    /// error, …). Human-readable detail only; not machine-dispatched on.
    Error { message: String },
}

impl Response {
    /// Build the standard "bad request / could not service" reply.
    pub fn error(message: impl Into<String>) -> Self {
        Response::Error {
            message: message.into(),
        }
    }
}

/// Wire twin of [`SubscribeOutcome`] (a storage domain type, not a wire type).
/// Kept separate so storage stays free of the wire shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum SubscribeState {
    /// A fresh subscription; `baseline` is the head it baselined to (`None` on an
    /// empty topic).
    Subscribed { baseline: Option<Offset> },
    /// Already subscribed; an idempotent no-op that left the cursor untouched.
    AlreadySubscribed,
    /// Refused: the session ended within the tombstone guard window, so no
    /// subscription was created (the resurrection guard, ADR-0007). The caller can
    /// see the subscribe honestly did nothing rather than assume it took effect.
    RefusedSessionRecentlyEnded,
}

impl From<SubscribeOutcome> for SubscribeState {
    fn from(outcome: SubscribeOutcome) -> Self {
        match outcome {
            SubscribeOutcome::Subscribed { baseline } => SubscribeState::Subscribed { baseline },
            SubscribeOutcome::AlreadySubscribed => SubscribeState::AlreadySubscribed,
            SubscribeOutcome::RefusedSessionRecentlyEnded => {
                SubscribeState::RefusedSessionRecentlyEnded
            }
        }
    }
}

/// Wire twin of [`SessionResumed`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum ResumeState {
    Resumed {
        subscriptions_restored: u64,
        interests_restored: u64,
        watches_ensured: u64,
    },
    RefusedSessionRecentlyEnded,
}

impl From<SessionResumed> for ResumeState {
    fn from(resumed: SessionResumed) -> Self {
        match resumed {
            SessionResumed::Resumed {
                subscriptions_restored,
                interests_restored,
                watches_ensured,
            } => ResumeState::Resumed {
                subscriptions_restored,
                interests_restored,
                watches_ensured,
            },
            SessionResumed::RefusedSessionRecentlyEnded => ResumeState::RefusedSessionRecentlyEnded,
        }
    }
}

/// Wire twin of [`UnwatchOutcome`]. A sum type so "dropped, zero remaining" and
/// "no such watch" can never be confused (the dead-field combo of a `bool` +
/// separate count is unrepresentable).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "unwatch", rename_all = "snake_case")]
pub enum UnwatchResultWire {
    /// This session's interest was removed; this many sessions still care.
    Dropped { remaining_interest: u64 },
    /// No watch existed for this entity (the topic was still unsubscribed).
    NoSuchWatch,
}

impl From<UnwatchOutcome> for UnwatchResultWire {
    fn from(outcome: UnwatchOutcome) -> Self {
        match outcome {
            UnwatchOutcome::Dropped { remaining_interest } => {
                UnwatchResultWire::Dropped { remaining_interest }
            }
            UnwatchOutcome::NoSuchWatch => UnwatchResultWire::NoSuchWatch,
        }
    }
}

/// One registered agent inbox as `mailbox agents` reports it (card 16).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentSummary {
    /// The agent's session id — what you pass to `mailbox send`.
    pub session: SessionId,
    /// Its inbox topic (`agent.<session-id>`), carried explicitly so a consumer
    /// never has to re-derive the grammar.
    pub inbox: Topic,
    /// Whether a Claude Code process is still running for this session, read from
    /// the process table ([`mailbox::doctor::live_claude_sessions`]).
    ///
    /// It says the agent EXISTS, not that it is idle or reachable: a live agent may
    /// be mid-turn, and only `mailbox doctor` proves wakeability. `false` means
    /// nobody is running that session any more; a message still lands durably in its
    /// inbox, it just has nobody left to collect it.
    pub live: bool,
    /// Whether this row is the caller itself. Always `false` when the request
    /// carried no caller (a human at a terminal is not one of these agents), so
    /// "nobody is marked" and "I am not listed" read the same — which they are.
    pub is_self: bool,
}

/// One topic as `mailbox topics` reports it. Wire twin of [`TopicSummary`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicStatus {
    pub topic: Topic,
    pub subscribers: u64,
    pub events: u64,
    /// Unix millis of the newest event; absent on a topic with no events.
    pub last_event_ms: Option<i64>,
}

impl From<TopicSummary> for TopicStatus {
    fn from(summary: TopicSummary) -> Self {
        TopicStatus {
            topic: summary.topic,
            subscribers: summary.subscribers,
            events: summary.events,
            last_event_ms: summary.last_event.map(|t| t.0),
        }
    }
}

/// What `status` reports (card 06): the bridge's watch table, plus the caller's own
/// half when a session ran the command.
///
/// **Decoded through [`StatusReportWire`]**, not by the flatten below. `#[serde(flatten)]`
/// on an `Option` cannot tell "the half is absent" from "the half did not parse" — it
/// answers `None` to both — so a reply that lost or renamed one session field would
/// decode as a perfectly plausible sessionless report. The CLI would then print
/// `session: none (no CLAUDE_CODE_SESSION_ID …)`, which is not merely unhelpful but
/// FALSE, and a status line would lose `subscription_count` with no error anywhere.
/// The flatten stays for serialization, where it is exactly right.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "StatusReportWire")]
pub struct StatusReport {
    /// Every watch the bridge knows about, with interest counts + lifecycle.
    /// Bridge-global, so it is the whole report when no session ran the command.
    pub watches: Vec<WatchStatus>,
    /// The caller's own half: absent when no session ran the command.
    ///
    /// **Flattened**, so every key keeps the exact top-level place and name it has
    /// always had for a session caller — a Claude Code status line reads
    /// `.subscription_count` off this document on every prompt, and nesting it would
    /// break that consumer silently. `Option` rather than emptied-out fields for the
    /// same reason `WakeVerdict::Unknown` is a variant: "nobody asked" and "the answer
    /// is zero" are different claims, and only one of them is true here.
    #[serde(flatten)]
    pub session: Option<SessionStatus>,
}

/// The session-scoped half of a [`StatusReport`] — everything that is true of *this*
/// caller rather than of the bridge.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionStatus {
    /// The session this half is for (whose unread counts are shown).
    pub session: SessionId,
    /// This session's inbox topic (card 16), so an agent can see its own address
    /// without re-deriving it. `None` only if the session id cannot form a topic
    /// (see `mailbox_protocol::inbox_topic`) — a session that is then not
    /// addressable at all, which `status` should say plainly rather than hide.
    /// Whether it is REGISTERED is visible in `subscriptions`.
    pub inbox: Option<Topic>,
    /// The topics `session` is subscribed to (card 11): the read behind
    /// "arm-iff-subscribed", also handy by hand.
    pub subscriptions: Vec<Topic>,
    /// How many topics `session` is subscribed to — always `subscriptions.len()`,
    /// carried as its own field so a caller that wants only the number reads one
    /// scalar instead of measuring a list it then discards. That caller is a
    /// status line: it re-renders on every prompt, so `.subscription_count` keeps
    /// it a one-key `jq` that does not change shape as the topic list grows.
    ///
    /// This counts the session's own `agent.<id>` inbox topic when registered, so
    /// a freshly-armed session with no watches reads `1`, not `0` — the inbox is a
    /// real subscription, and hiding it would make the number disagree with the
    /// list beside it. `inbox` says whether that particular one is registered.
    pub subscription_count: u64,
    /// Per-topic unread counts for `session` (topics with zero are omitted).
    pub unread: Vec<TopicUnread>,
}

impl StatusReport {
    /// Assemble the wire report from the domain [`StatusView`].
    ///
    /// The session travels *inside* the view, so there is no second argument that
    /// could name a different session than the subscriptions belong to.
    pub fn from_view(view: StatusView) -> Self {
        StatusReport {
            watches: view.watches.into_iter().map(WatchStatus::from).collect(),
            session: view.session.map(SessionStatus::from),
        }
    }
}

/// Domain → wire for the session half, matching the `From` impls the rest of this
/// module uses. `subscription_count` is derived here from the very list it counts, so
/// the two cannot be handed in disagreeing.
impl From<SessionStatusView> for SessionStatus {
    fn from(view: SessionStatusView) -> Self {
        SessionStatus {
            inbox: inbox_topic(&view.session).ok(),
            session: view.session,
            subscription_count: view.subscriptions.len() as u64,
            subscriptions: view.subscriptions,
            unread: view
                .unread
                .into_iter()
                .map(|(topic, unread)| TopicUnread { topic, unread })
                .collect(),
        }
    }
}

/// The decode mirror for [`StatusReport`]: every session-half field independently
/// optional, so a half-decoded reply becomes an ERROR instead of a plausible-looking
/// sessionless one (see [`StatusReport`]'s note on why the flatten cannot do this).
///
/// The realistic way a skewed reply arrives is a long-lived `mailbox serve` from an
/// older install answering a newer CLI: [`check_version`] rejects only frames NEWER
/// than ours, so an older daemon's reply is accepted and read.
///
/// `subscription_count` is deliberately absent here. Serde ignores unknown keys, so a
/// wire value is accepted and dropped, and the count is recomputed from the list on
/// the way in — the two are then structurally incapable of disagreeing, whatever the
/// sender claimed.
#[derive(Deserialize)]
struct StatusReportWire {
    watches: Vec<WatchStatus>,
    session: Option<SessionId>,
    inbox: Option<Topic>,
    subscriptions: Option<Vec<Topic>>,
    unread: Option<Vec<TopicUnread>>,
}

impl TryFrom<StatusReportWire> for StatusReport {
    type Error = String;

    fn try_from(wire: StatusReportWire) -> Result<Self, Self::Error> {
        // `session` is the discriminator; `subscriptions` and `unread` must travel with
        // it. `inbox` is exempt because absent and `null` mean the same thing there — a
        // session whose id cannot form an inbox topic — so its absence proves nothing.
        let session = match (wire.session, wire.subscriptions, wire.unread) {
            (None, None, None) => None,
            (Some(session), Some(subscriptions), Some(unread)) => Some(SessionStatus {
                inbox: wire.inbox,
                session,
                subscription_count: subscriptions.len() as u64,
                subscriptions,
                unread,
            }),
            (session, subscriptions, unread) => {
                return Err(format!(
                    "a status reply carried an incomplete session half (session: {}, \
                     subscriptions: {}, unread: {}) — they travel together or not at all, \
                     so this reply cannot be read as either a session's status or nobody's",
                    field_state(&session),
                    field_state(&subscriptions),
                    field_state(&unread),
                ));
            }
        };
        Ok(StatusReport {
            watches: wire.watches,
            session,
        })
    }
}

/// Name a field's presence for the error above. The values themselves are never
/// printed: a decode error is read by whoever ran the command, and the report is
/// theirs to see in full or not at all.
fn field_state<T>(field: &Option<T>) -> &'static str {
    match field {
        Some(_) => "present",
        None => "missing",
    }
}

/// Wire twin of [`WatchKind`]. A unit enum so an unknown kind is unrepresentable
/// on the wire (not a free string). The wire string is pinned to match
/// [`WatchKind::as_str`] (`github-pr`) exactly — a compatibility contract for
/// non-Rust peers, so it must not drift with a rename.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WatchKindWire {
    #[serde(rename = "github-pr")]
    GithubPr,
    #[serde(rename = "stub")]
    Stub,
}

impl From<WatchKind> for WatchKindWire {
    fn from(kind: WatchKind) -> Self {
        match kind {
            WatchKind::GithubPr => WatchKindWire::GithubPr,
            WatchKind::Stub => WatchKindWire::Stub,
        }
    }
}

/// Wire twin of [`WatchState`]. The child pid lives INSIDE `Running`, so a
/// "stopped/desired but has a pid" combination is unrepresentable — the same
/// invariant the storage model holds, preserved across the wire. Flattened into
/// [`WatchStatus`], so it serialises as a flat `"state":"desired"` (plus `"pid"`
/// only when running) rather than a nested object.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum WatchStateWire {
    /// Wanted by ≥1 session; adapter not spawned (or no adapter resolved yet).
    Desired,
    /// Adapter child running under this pid (card 08 sets it).
    Running { pid: u32 },
    /// Torn down cleanly (last interest gone).
    Stopped,
    /// The adapter crashed repeatedly and the supervisor gave up (card 08).
    Failed,
}

impl From<WatchState> for WatchStateWire {
    fn from(state: WatchState) -> Self {
        match state {
            WatchState::Desired => WatchStateWire::Desired,
            WatchState::Running { pid } => WatchStateWire::Running { pid: pid.get() },
            WatchState::Stopped => WatchStateWire::Stopped,
            WatchState::Failed => WatchStateWire::Failed,
        }
    }
}

/// One watch row as `status` presents it. The lifecycle `state` (and its pid, if
/// running) is flattened in; there is no separate nullable pid field to get
/// wrong.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WatchStatus {
    pub kind: WatchKindWire,
    /// The flat `repo` column: `owner/repo` for github, the label for stub.
    pub repo: String,
    /// The flat `pr` column: the PR number for github, `0` for stub.
    pub pr: u64,
    /// Desired poll interval, **milliseconds** — carried at millisecond precision
    /// so a sub-second stub interval (`--interval-ms 200`) is not flattened to
    /// `0s` in `status` (schema v3 widened storage to ms for exactly this reason).
    pub interval_ms: u64,
    /// Interested sessions (the refcount that keeps a poller alive).
    pub interest: u64,
    /// Lifecycle state (+ child pid when running). Card 06 is always `desired`
    /// with no pid; adapter supervision that sets `running` is card 08.
    #[serde(flatten)]
    pub state: WatchStateWire,
}

impl From<WatchEntry> for WatchStatus {
    fn from(entry: WatchEntry) -> Self {
        // Project the sum-typed target back onto the flat wire fields (the wire
        // mirrors the flat storage row): kind + repo/label + pr.
        WatchStatus {
            kind: entry.target.kind().into(),
            repo: entry.target.repo_column().to_string(),
            pr: entry.target.pr_column(),
            interval_ms: u64::try_from(entry.interval.as_millis()).unwrap_or(u64::MAX),
            interest: entry.interest,
            state: entry.state.into(),
        }
    }
}

/// A topic and how many events are unread on it for the status session.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicUnread {
    pub topic: Topic,
    pub unread: u64,
}

/// A framing/decoding failure on the control channel.
#[derive(Debug, thiserror::Error)]
pub enum ControlError {
    #[error("could not (de)serialize a control frame: {0}")]
    Json(#[from] serde_json::Error),

    #[error(transparent)]
    IncompatibleVersion(#[from] IncompatibleVersion),
}

/// Just the version header, peeked before trusting the rest of a line — same
/// reject-newer discipline as `mailbox_protocol::framing`.
#[derive(Deserialize)]
struct VersionHeader {
    version: u32,
}

/// Borrowing frame used on encode so we stamp the version without cloning the
/// payload. Flattened: `{"version":1,"op":"publish",...}`.
#[derive(Serialize)]
struct FrameRef<'a, T: Serialize> {
    version: u32,
    #[serde(flatten)]
    payload: &'a T,
}

/// Encode a control message as a single NDJSON line (no trailing newline),
/// stamped with the current [`PROTOCOL_VERSION`].
pub fn encode_frame<T: Serialize>(payload: &T) -> Result<String, ControlError> {
    let frame = FrameRef {
        version: PROTOCOL_VERSION,
        payload,
    };
    Ok(serde_json::to_string(&frame)?)
}

/// Decode a single control NDJSON line, enforcing reject-newer BEFORE the body
/// so a future frame is reported as an incompatible version, not a confusing
/// parse error (same order as `mailbox_protocol::decode_line`).
pub fn decode_frame<T: serde::de::DeserializeOwned>(line: &str) -> Result<T, ControlError> {
    let line = line.strip_suffix('\r').unwrap_or(line);
    let header: VersionHeader = serde_json::from_str(line)?;
    check_version(header.version)?;
    Ok(serde_json::from_str(line)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    use mailbox::watch::SessionStatusView;

    fn round_trip_request(req: Request) {
        let line = encode_frame(&req).unwrap();
        assert!(!line.contains('\n'), "a frame must be one line");
        let back: Request = decode_frame(&line).unwrap();
        assert_eq!(req, back);
    }

    #[test]
    fn request_variants_round_trip() {
        round_trip_request(Request::Publish {
            topic: Topic::parse("github.pr.o/r#1").unwrap(),
            adapter: AdapterId("cli".to_string()),
            body: serde_json::json!({ "hello": "world" }),
            subject: Subject::new("new comment", Some("https://example.com/c/1")).ok(),
        });
        round_trip_request(Request::Subscribe {
            session: SessionId::new("s1"),
            topic: Topic::parse("t.a.b").unwrap(),
            kind: SubscribeKind::Explicit,
        });
        round_trip_request(Request::Read {
            session: SessionId::new("s1"),
            limit: None,
        });
        round_trip_request(Request::Watch {
            session: SessionId::new("s1"),
            target: GithubPrTarget {
                owner: "o".to_string(),
                repo: "r".to_string(),
                number: 7,
            },
            interval_secs: 60,
        });
        round_trip_request(Request::Status {
            session: Some(SessionId::new("s1")),
        });
    }

    /// The three ops whose caller is optional survive the wire in BOTH shapes. A
    /// missing caller has to be a real value on the frame, not an encoding that
    /// happens to decode — otherwise "a human sent this" would be indistinguishable
    /// from a truncated frame.
    #[test]
    fn send_agents_and_status_round_trip_with_and_without_a_caller() {
        for from in [Some(SessionId::new("s-a")), None] {
            round_trip_request(Request::Send {
                from,
                to: SessionId::new("s-b"),
                body: serde_json::Map::new(),
                subject: None,
            });
        }
        for session in [Some(SessionId::new("s-a")), None] {
            round_trip_request(Request::Agents {
                session: session.clone(),
            });
            round_trip_request(Request::Status { session });
        }
    }

    #[test]
    fn session_is_transparent_on_the_wire() {
        // A branded SessionId serialises as its bare string (no envelope).
        let req = Request::Status {
            session: Some(SessionId::new("plain-id")),
        };
        let value: serde_json::Value = serde_json::from_str(&encode_frame(&req).unwrap()).unwrap();
        assert_eq!(value["session"], "plain-id");
    }

    #[test]
    fn watch_status_state_is_flat() {
        // A desired watch flattens to a bare "state":"desired" with NO pid key.
        let status = WatchStatus {
            kind: WatchKindWire::GithubPr,
            repo: "o/r".to_string(),
            pr: 1,
            interval_ms: 30_000,
            interest: 1,
            state: WatchStateWire::Desired,
        };
        let value: serde_json::Value = serde_json::to_value(&status).unwrap();
        assert_eq!(value["state"], "desired");
        assert_eq!(value["kind"], "github-pr");
        assert!(value.get("pid").is_none(), "desired watch carries no pid");
    }

    #[test]
    fn response_round_trips_and_carries_version() {
        let resp = Response::Watched {
            topic: Topic::parse("github.pr.o/r#1").unwrap(),
            interest: 2,
            subscribe: SubscribeState::Subscribed {
                baseline: Some(Offset(4)),
            },
        };
        let line = encode_frame(&resp).unwrap();
        let value: serde_json::Value = serde_json::from_str(&line).unwrap();
        assert_eq!(value["version"], serde_json::json!(PROTOCOL_VERSION));
        let back: Response = decode_frame(&line).unwrap();
        assert_eq!(resp, back);
    }

    /// Build a status view for session `s1` holding just `subscriptions` (the axis
    /// under test).
    fn view_of(subscriptions: &[&str]) -> StatusView {
        StatusView {
            watches: vec![],
            session: Some(SessionStatusView {
                session: SessionId::new("s1"),
                subscriptions: subscriptions
                    .iter()
                    .map(|t| Topic::parse(*t).unwrap())
                    .collect(),
                unread: vec![],
            }),
        }
    }

    /// The session half of a report built for a session. Panics rather than
    /// unwrapping at each call site, so a test that loses it fails on the assertion
    /// it was written for.
    fn session_half(report: &StatusReport) -> &SessionStatus {
        report
            .session
            .as_ref()
            .expect("a report built from a view with a session has a session half")
    }

    /// `subscription_count` is derived from the list it counts, and reaches the
    /// wire as its own key — the one a status line reads instead of measuring
    /// `subscriptions`. The two can never disagree, so the assertion is written as
    /// that invariant rather than as a hard-coded 2.
    #[test]
    fn status_carries_a_subscription_count_that_matches_its_list() {
        let report = StatusReport::from_view(view_of(&["agent.s1", "github.pr.o/r#1"]));
        let half = session_half(&report);
        assert_eq!(half.subscription_count as usize, half.subscriptions.len());

        let value = serde_json::to_value(Response::Status(report)).unwrap();
        assert_eq!(value["subscription_count"], 2);
        assert_eq!(value["subscriptions"].as_array().unwrap().len(), 2);
    }

    /// The un-armed session: no subscriptions is `0`, not a missing key — a status
    /// line must be able to render it without a `// 0` fallback in its `jq`.
    #[test]
    fn a_session_with_no_subscriptions_counts_zero() {
        let report = StatusReport::from_view(view_of(&[]));
        assert_eq!(session_half(&report).subscription_count, 0);
        let value = serde_json::to_value(Response::Status(report)).unwrap();
        assert_eq!(value["subscription_count"], 0);
    }

    /// The session half is FLATTENED, so a session caller's document is byte-for-byte
    /// the shape it has always been — `session` and `subscription_count` at the top
    /// level, beside `result`. A Claude Code status line reads those keys on every
    /// prompt, so nesting them would break a live consumer silently.
    #[test]
    fn a_session_callers_keys_stay_at_the_top_level() {
        let value =
            serde_json::to_value(Response::Status(StatusReport::from_view(view_of(&[])))).unwrap();
        assert_eq!(value["result"], "status");
        assert_eq!(value["session"], "s1");
        assert_eq!(value["inbox"], "agent.s1");
        assert!(value["watches"].is_array());
    }

    /// A caller that is not a session gets the bridge's half and NOTHING standing in
    /// for the rest: the session keys are absent, not null and not zeroed. A `0`
    /// `subscription_count` there would answer "how many topics am I on?" for a
    /// caller who never asked, and a status line would print it as fact.
    #[test]
    fn a_report_for_no_session_omits_the_session_keys_entirely() {
        let report = StatusReport::from_view(StatusView {
            watches: vec![],
            session: None,
        });
        assert!(report.session.is_none());

        let value = serde_json::to_value(Response::Status(report)).unwrap();
        assert_eq!(value["result"], "status");
        assert!(value["watches"].is_array(), "the global half still answers");
        for key in [
            "session",
            "inbox",
            "subscriptions",
            "subscription_count",
            "unread",
        ] {
            assert!(
                value.get(key).is_none(),
                "{key} must be absent, not null or zero, when no session ran the command"
            );
        }
    }

    /// **A half-decoded session half is an ERROR, not a sessionless report.**
    ///
    /// `#[serde(flatten)]` on an `Option` answers `None` to both "absent" and "did not
    /// parse", so without the [`StatusReportWire`] decode these inputs would each read
    /// as a perfectly plausible report for nobody — and the CLI would print
    /// `session: none (no CLAUDE_CODE_SESSION_ID …)` about a session that named itself,
    /// while a status line lost `subscription_count` with no error anywhere. The lie is
    /// the point: absence of the half must never be inferred from failure to read it.
    #[test]
    fn an_incomplete_session_half_fails_to_decode_rather_than_reading_as_nobody() {
        for (name, raw) in [
            (
                "session with no subscriptions or unread",
                r#"{"version":1,"result":"status","watches":[],"session":"s1"}"#,
            ),
            (
                "session and subscriptions but no unread",
                r#"{"version":1,"result":"status","watches":[],"session":"s1","subscriptions":[],"inbox":"agent.s1"}"#,
            ),
            (
                "the half's contents with no session to own them",
                r#"{"version":1,"result":"status","watches":[],"subscriptions":[],"unread":[]}"#,
            ),
        ] {
            let decoded = decode_frame::<Response>(raw);
            let err = decoded
                .err()
                .unwrap_or_else(|| panic!("{name} must not decode as a valid report"))
                .to_string();
            assert!(
                err.contains("incomplete session half"),
                "{name}: the error must name what was wrong; got: {err}"
            );
        }
    }

    /// The count is recomputed from the list it counts, so a sender that claims
    /// otherwise cannot make the two disagree in a reader's hands.
    #[test]
    fn a_wire_subscription_count_is_recomputed_rather_than_trusted() {
        let raw = r#"{"version":1,"result":"status","watches":[],"session":"s1","inbox":"agent.s1","subscriptions":["agent.s1","stub.x"],"subscription_count":99,"unread":[]}"#;
        let Response::Status(report) = decode_frame::<Response>(raw).expect("a valid report")
        else {
            panic!("expected a status reply");
        };
        let half = session_half(&report);
        assert_eq!(half.subscription_count, 2, "the claimed 99 is not believed");
        assert_eq!(half.subscription_count as usize, half.subscriptions.len());
    }

    /// Both shapes of the report survive the wire. The sessionless one especially:
    /// `#[serde(flatten)]` on an `Option` is what keeps the keys top-level, and a
    /// flatten that serialises but does not decode would break every client.
    #[test]
    fn a_status_reply_round_trips_with_and_without_a_session_half() {
        for view in [
            view_of(&["agent.s1"]),
            StatusView {
                watches: vec![],
                session: None,
            },
        ] {
            let resp = Response::Status(StatusReport::from_view(view));
            let line = encode_frame(&resp).unwrap();
            assert_eq!(decode_frame::<Response>(&line).unwrap(), resp);
        }
    }

    #[test]
    fn decode_rejects_newer_version() {
        let future = format!(
            r#"{{"version":{},"op":"status","session":"s"}}"#,
            PROTOCOL_VERSION + 1
        );
        assert!(matches!(
            decode_frame::<Request>(&future),
            Err(ControlError::IncompatibleVersion(_))
        ));
    }
}
