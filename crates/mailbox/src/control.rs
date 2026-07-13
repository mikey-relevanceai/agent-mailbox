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
    AdapterId, Event, EventId, IncompatibleVersion, Offset, PROTOCOL_VERSION, Topic, check_version,
    inbox_topic,
};

use mailbox::storage::{SessionId, SubscribeOutcome, TopicSummary, WatchKind, WatchState};
use mailbox::watch::{StatusView, UnwatchOutcome, WatchEntry};

/// A one-shot request from a CLI client to the `serve` daemon.
///
/// Internally tagged by `"op"` so a request can be dispatched on one well-known
/// key, mirroring the `mailbox-protocol` `Message` convention.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Request {
    /// Publish an event to a topic. No session: provenance is the [`AdapterId`]
    /// (the daemon stamps the timestamp, exactly as the durable bridge does).
    Publish {
        topic: Topic,
        adapter: AdapterId,
        body: Value,
    },
    /// Subscribe `session` to `topic` (baseline-on-subscribe).
    Subscribe { session: SessionId, topic: Topic },
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
    /// Report watches (interest + child pid), this session's subscriptions, and
    /// its unread counts.
    Status { session: SessionId },
    /// Message a peer agent: publish `body` to the target's inbox topic, stamped
    /// with the sender's id (card 16). `to` is a [`SessionId`], not a `Topic`: the
    /// daemon mints the inbox topic through the one canonical constructor, so a
    /// caller cannot address a *non*-inbox topic through this op.
    ///
    /// The body is a JSON **object** on the wire (a `Map`, not a `Value`), so
    /// "there is somewhere to stamp `from`" is a type-level guarantee rather than
    /// a runtime check. It stays opaque to the bus either way (ADR-0001).
    Send {
        from: SessionId,
        to: SessionId,
        body: serde_json::Map<String, Value>,
    },
    /// List the sessions with a registered agent inbox (card-16 discovery).
    /// `session` is the *caller*, so the reply can mark which agent is itself.
    Agents { session: SessionId },
    /// List known topics with subscriber/event counts, optionally filtered to a
    /// prefix (card-16 discovery).
    Topics { prefix: Option<String> },
    /// End a session (the harness `SessionEnd` hook, card 11): drop all its
    /// subscriptions and interests, stopping any adapter whose last interest it
    /// held. No topic here — it tears down everything for the session at once.
    EndSession { session: SessionId },
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
    /// A session was ended (card 11): counts of what its teardown removed.
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
}

impl From<SubscribeOutcome> for SubscribeState {
    fn from(outcome: SubscribeOutcome) -> Self {
        match outcome {
            SubscribeOutcome::Subscribed { baseline } => SubscribeState::Subscribed { baseline },
            SubscribeOutcome::AlreadySubscribed => SubscribeState::AlreadySubscribed,
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
    /// Whether a waiter is blocked for this session right now — i.e. the agent is
    /// idle and a `send` will wake it immediately. `false` means it is busy
    /// (mid-turn) or never armed; a message still lands durably in its inbox and
    /// surfaces on its next read. This is NOT a heartbeat (see
    /// [`mailbox::wake::waiter_alive`]).
    pub live_waiter: bool,
    /// Whether this row is the caller itself.
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

/// What `status` reports (card 06).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StatusReport {
    /// The session this snapshot is for (whose unread counts are shown).
    pub session: SessionId,
    /// This session's inbox topic (card 16), so an agent can see its own address
    /// without re-deriving it. `None` only if the session id cannot form a topic
    /// (see `mailbox_protocol::inbox_topic`) — a session that is then not
    /// addressable at all, which `status` should say plainly rather than hide.
    /// Whether it is REGISTERED is visible in `subscriptions`.
    pub inbox: Option<Topic>,
    /// Every watch the bridge knows about, with interest counts + lifecycle.
    pub watches: Vec<WatchStatus>,
    /// The topics `session` is subscribed to (card 11): the read behind
    /// "arm-iff-subscribed", also handy by hand.
    pub subscriptions: Vec<Topic>,
    /// Per-topic unread counts for `session` (topics with zero are omitted).
    pub unread: Vec<TopicUnread>,
}

impl StatusReport {
    /// Assemble the wire report from the domain [`StatusView`] and its session.
    pub fn from_view(session: SessionId, view: StatusView) -> Self {
        StatusReport {
            inbox: inbox_topic(&session).ok(),
            session,
            watches: view.watches.into_iter().map(WatchStatus::from).collect(),
            subscriptions: view.subscriptions,
            unread: view
                .unread
                .into_iter()
                .map(|(topic, unread)| TopicUnread { topic, unread })
                .collect(),
        }
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
        });
        round_trip_request(Request::Subscribe {
            session: SessionId::new("s1"),
            topic: Topic::parse("t.a.b").unwrap(),
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
            session: SessionId::new("s1"),
        });
    }

    #[test]
    fn session_is_transparent_on_the_wire() {
        // A branded SessionId serialises as its bare string (no envelope).
        let req = Request::Status {
            session: SessionId::new("plain-id"),
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
