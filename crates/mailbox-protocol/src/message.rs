//! The versioned message set adapters, the CLI, and the bridge exchange.
//!
//! Every message is a variant of [`Message`], which is internally tagged by a
//! `"type"` field so a non-Rust adapter can dispatch on one well-known key. The
//! set is intentionally small; anything an adapter needs to do maps to one of
//! these.
//!
//! ## Untrusted bodies (ADR-0001)
//!
//! [`Publish::body`] and [`Event::body`] are `serde_json::Value` — opaque
//! content this crate never inspects or interprets. Adapters are separate,
//! same-user processes whose output is treated as untrusted *content*; giving
//! the body a concrete schema here would tempt higher layers to trust it. The
//! protocol's job is to carry the bytes, not to understand them.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::ids::{AdapterId, Cursor, EventId, Offset, Timestamp};
use crate::topic::Topic;

/// Publish a new event to a topic (adapter/CLI → bridge).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Publish {
    pub topic: Topic,
    /// Who is publishing — provenance only (see [`AdapterId`]).
    pub adapter: AdapterId,
    /// Opaque, untrusted content. Never interpreted by this crate.
    pub body: Value,
}

/// Register interest in a topic (subscriber → bridge).
///
/// Subscribing governs *pushed* [`Event`]s only; durable catch-up is a separate
/// [`ReadRequest`], keeping "tell me about new things" and "replay from a
/// cursor" as distinct operations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Subscribe {
    pub topic: Topic,
}

/// Drop interest in a topic (subscriber → bridge).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Unsubscribe {
    pub topic: Topic,
}

/// A durable event on a topic (bridge → subscriber, and the unit of [`ReadResponse`]).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Event {
    /// Stable opaque identity.
    pub id: EventId,
    /// Monotonic per-topic cursor coordinate.
    pub offset: Offset,
    pub topic: Topic,
    /// When the event was created, Unix milliseconds UTC.
    pub timestamp: Timestamp,
    /// Opaque, untrusted content. Never interpreted by this crate.
    pub body: Value,
}

/// Cursor-based read of a topic's durable log (consumer → bridge).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadRequest {
    pub topic: Topic,
    /// Where to start reading from.
    pub cursor: Cursor,
    /// Maximum number of events to return; `None` lets the bridge choose a page
    /// size. Bounding is the bridge's call, not the wire format's.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
}

/// Reply to a [`ReadRequest`] (bridge → consumer).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReadResponse {
    pub topic: Topic,
    pub events: Vec<Event>,
    /// Cursor to pass to the next [`ReadRequest`] to continue where this page
    /// ended. Carrying it explicitly means the consumer never has to derive
    /// paging state from the event list itself.
    pub next: Cursor,
}

/// Positive acknowledgement of a command (bridge → sender).
///
/// Tagged by an `"ack"` field so each acknowledged command reports exactly the
/// data that command produces — no nullable "sometimes an offset" field.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "ack", rename_all = "snake_case")]
pub enum Ack {
    /// A [`Publish`] was committed to the durable log at these coordinates.
    Published { id: EventId, offset: Offset },
    /// A [`Subscribe`] took effect.
    Subscribed { topic: Topic },
    /// An [`Unsubscribe`] took effect.
    Unsubscribed { topic: Topic },
}

/// Category of a [`ProtocolError`]. Exhaustive so both sides handle every case.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    /// Frame declared a protocol version this peer cannot read (reject-newer).
    UnsupportedVersion,
    /// A line could not be parsed as a valid message.
    MalformedMessage,
    /// The referenced topic is not known / not subscribable.
    UnknownTopic,
    /// The command was well-formed but the bridge failed to service it.
    Internal,
}

/// Negative acknowledgement / error report (bridge → sender).
///
/// This is the on-wire error *message*, distinct from Rust-side error types in
/// [`crate::error`]: it is data one peer sends the other, not something a caller
/// returns from a function.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProtocolError {
    pub code: ErrorCode,
    /// Human-readable detail for logs/diagnostics. Not machine-dispatched on.
    pub message: String,
}

/// Every message that can travel on the wire, tagged by `"type"`.
///
/// Exhaustive matching on this enum is how a peer dispatches an incoming line;
/// adding a message forces every handler to acknowledge it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Message {
    Publish(Publish),
    Subscribe(Subscribe),
    Unsubscribe(Unsubscribe),
    Read(ReadRequest),
    Event(Event),
    ReadResponse(ReadResponse),
    Ack(Ack),
    Error(ProtocolError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topic::GithubPr;

    fn sample_topic() -> Topic {
        GithubPr::new("octocat", "hello-world", 42).unwrap().topic()
    }

    /// Serialize → deserialize → assert equal, for one `Message` variant.
    fn assert_round_trips(message: Message) {
        let json = serde_json::to_string(&message).unwrap();
        let back: Message = serde_json::from_str(&json).unwrap();
        assert_eq!(message, back, "round-trip mismatch via {json}");
    }

    fn sample_event() -> Event {
        Event {
            id: EventId("01J0".to_string()),
            offset: Offset(5),
            topic: sample_topic(),
            timestamp: Timestamp(1_720_000_000_000),
            body: serde_json::json!({ "action": "opened", "nested": [1, 2, 3] }),
        }
    }

    #[test]
    fn publish_round_trips() {
        assert_round_trips(Message::Publish(Publish {
            topic: sample_topic(),
            adapter: AdapterId("github-watch".to_string()),
            body: serde_json::json!({ "hello": "world" }),
        }));
    }

    #[test]
    fn subscribe_and_unsubscribe_round_trip() {
        assert_round_trips(Message::Subscribe(Subscribe {
            topic: sample_topic(),
        }));
        assert_round_trips(Message::Unsubscribe(Unsubscribe {
            topic: sample_topic(),
        }));
    }

    #[test]
    fn event_round_trips() {
        assert_round_trips(Message::Event(sample_event()));
    }

    #[test]
    fn read_request_round_trips() {
        assert_round_trips(Message::Read(ReadRequest {
            topic: sample_topic(),
            cursor: Cursor::After { offset: Offset(9) },
            limit: Some(100),
        }));
        // And with the optional limit omitted.
        assert_round_trips(Message::Read(ReadRequest {
            topic: sample_topic(),
            cursor: Cursor::Oldest,
            limit: None,
        }));
    }

    #[test]
    fn read_response_round_trips() {
        assert_round_trips(Message::ReadResponse(ReadResponse {
            topic: sample_topic(),
            events: vec![sample_event()],
            next: Cursor::After { offset: Offset(5) },
        }));
    }

    #[test]
    fn ack_variants_round_trip() {
        assert_round_trips(Message::Ack(Ack::Published {
            id: EventId("01J0".to_string()),
            offset: Offset(5),
        }));
        assert_round_trips(Message::Ack(Ack::Subscribed {
            topic: sample_topic(),
        }));
        assert_round_trips(Message::Ack(Ack::Unsubscribed {
            topic: sample_topic(),
        }));
    }

    #[test]
    fn error_code_snake_case_wire_strings() {
        // The wire strings are a compatibility contract for non-Rust peers;
        // pin each one so a rename can't silently change the protocol.
        for (code, expected) in [
            (ErrorCode::UnsupportedVersion, "\"unsupported_version\""),
            (ErrorCode::MalformedMessage, "\"malformed_message\""),
            (ErrorCode::UnknownTopic, "\"unknown_topic\""),
            (ErrorCode::Internal, "\"internal\""),
        ] {
            assert_eq!(serde_json::to_string(&code).unwrap(), expected);
        }
    }

    #[test]
    fn error_round_trips() {
        assert_round_trips(Message::Error(ProtocolError {
            code: ErrorCode::MalformedMessage,
            message: "expected object".to_string(),
        }));
    }

    #[test]
    fn body_is_preserved_opaquely() {
        // The crate must carry arbitrary JSON bodies unchanged without needing a
        // schema for them (ADR-0001: bodies are untrusted, opaque content).
        let weird = serde_json::json!({
            "arbitrary": { "deeply": ["nested", { "x": null, "y": 3.5 }] },
            "unicode": "café \u{1F680}"
        });
        let msg = Message::Publish(Publish {
            topic: sample_topic(),
            adapter: AdapterId("x".to_string()),
            body: weird.clone(),
        });
        let json = serde_json::to_string(&msg).unwrap();
        let Message::Publish(back) = serde_json::from_str(&json).unwrap() else {
            panic!("wrong variant");
        };
        assert_eq!(back.body, weird);
    }
}
