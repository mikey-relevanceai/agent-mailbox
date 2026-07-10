//! Shared wire and domain types for agent-mailbox.
//!
//! Adapters (in any language), the `mailbox` CLI, and the bridge speak this
//! protocol. Transport — how an adapter is hosted (subprocess today, WASI
//! later) — stays behind a separate host boundary and does **not** belong here
//! (see `docs/02-tech-stack.md`, ADR-0001). This crate is protocol only: it has
//! no dependency on the bridge, on SQLite, or on tokio, and it never spawns or
//! plumbs a process. The NDJSON helpers in [`framing`] are pure serialization.
//!
//! # Message set
//!
//! Everything on the wire is a [`Message`]: [`Publish`], [`Subscribe`],
//! [`Unsubscribe`], [`ReadRequest`], [`Event`], [`ReadResponse`], [`Ack`], and
//! [`ProtocolError`]. See [`message`] for the shape of each.
//!
//! # Untrusted bodies
//!
//! Event and publish bodies are opaque `serde_json::Value` that this crate
//! never interprets — adapter output is untrusted content (ADR-0001). Keeping
//! them schemaless here stops higher layers from accidentally trusting them.
//!
//! # Compatibility rule: reject-newer
//!
//! Each framed line carries a `version` field. A receiver accepts its own
//! [`PROTOCOL_VERSION`] and anything **older**, and rejects anything **newer**
//! with [`IncompatibleVersion`] (see [`check_version`]).
//!
//! We chose reject-newer over silently ignoring unknown fields because a newer
//! frame may depend on fields or semantics this build does not implement;
//! quietly dropping them could turn a "PR was closed" into an apparent no-op and
//! leave an agent acting on stale state. For a wake/notification bus, failing
//! loudly and letting the operator upgrade is safer than best-effort
//! misinterpretation. The trade-off — no forward compatibility for peers ahead
//! of us — is acceptable at v0, where the bridge and its adapters are versioned
//! and rolled together; a future version may introduce an explicit additive
//! range if independent upgrades become a requirement.

mod error;
mod framing;
mod ids;
mod message;
mod topic;

/// Protocol schema version carried on every framed line so peers reject frames
/// newer than they understand (see the module docs for the reject-newer rule).
pub const PROTOCOL_VERSION: u32 = 1;

pub use error::{FramingError, IncompatibleVersion, LineError, TopicError, check_version};
pub use framing::{decode_line, encode_line, read_lines, write_line};
pub use ids::{AdapterId, Cursor, EventId, Offset, Timestamp};
pub use message::{
    Ack, ErrorCode, Event, Message, ProtocolError, Publish, ReadRequest, ReadResponse, Subscribe,
    Unsubscribe,
};
pub use topic::{GithubPr, Topic, stub_topic};
