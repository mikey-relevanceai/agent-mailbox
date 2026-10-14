//! Socket Mode's wire: the text frames Slack sends down the WebSocket, parsed
//! once here, and the acknowledgement it expects back (design/04).
//!
//! Slack documents three frame types that matter here
//! ([Socket Mode](https://docs.slack.dev/apis/events-api/using-socket-mode)):
//! `hello` after connecting, `disconnect` before it closes a connection, and an
//! envelope (`events_api`, `slash_commands`, `interactive`) carrying a payload.
//! Every envelope is acknowledged by sending back its `envelope_id`, whatever
//! it carries, "so that Slack knows whether to retry".
//!
//! An envelope's payload holds the message text and a legacy verification
//! token. Neither is deserialized: [`BotMessage`] reads only the fields a wake
//! and a `--skip` filter need.

use std::fmt;

use mailbox_protocol::{SlackAppId, SlackTs, SlackUserId};
use serde::Deserialize;
use serde_json::Value;

use crate::message::{Subtype, lenient};

/// Slack ids are short; this bounds what an untrusted frame can make us carry.
const MAX_ID_LEN: usize = 64;

/// The id Slack gave one envelope, which is what an ack names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnvelopeId(String);

impl EnvelopeId {
    pub fn as_str(&self) -> &str {
        &self.0
    }

    #[cfg(test)]
    pub fn for_tests(raw: &str) -> Self {
        Self(raw.to_string())
    }
}

impl fmt::Display for EnvelopeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// The Events API's id for one event. Logged and carried in the body; duplicates
/// are dropped by the message, not by this (see [`crate::bot_messages`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventId(String);

impl EventId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for EventId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A conversation the bot can be addressed in: a channel (`C…`), a legacy
/// private channel or group DM (`G…`), or a DM (`D…`). Unlike the protocol's
/// `SlackChannelId`, which names what a channel watch may target, this accepts
/// DMs, because a DM to the bot is one of the things this watch is for.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ConversationId(String);

impl ConversationId {
    pub fn parse(raw: &str) -> Option<Self> {
        let mut chars = raw.chars();
        let first = chars.next()?;
        let ok = matches!(first, 'C' | 'G' | 'D')
            && raw.len() >= 3
            && raw.len() <= MAX_ID_LEN
            && chars.all(|c| c.is_ascii_uppercase() || c.is_ascii_digit());
        ok.then(|| Self(raw.to_string()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ConversationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// One text frame from Slack.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Frame {
    /// The connection is ready. `num_connections` counts every connection open
    /// for the app, this one included.
    Hello { num_connections: Option<u64> },
    /// Slack is about to close this connection.
    Disconnect { reason: DisconnectReason },
    /// A payload to acknowledge. Boxed: it is far larger than the other frames.
    Envelope(Box<Envelope>),
    /// A frame with a `type` this adapter does not know, or no `type` at all.
    /// Logged by type and otherwise ignored.
    Other { kind: Option<String> },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DisconnectReason {
    /// Socket Mode was turned off for the app. Reconnecting cannot work.
    LinkDisabled,
    /// A routine refresh (`refresh_requested`), a warning that one is coming
    /// (`warning`), or a reason Slack has added since. All mean: connect again.
    Refresh(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Envelope {
    pub id: EnvelopeId,
    /// `events_api`, `slash_commands`, `interactive`, …
    pub kind: String,
    /// Slack's redelivery count: `0` on the first attempt. Logged only. A retry
    /// is NOT dropped because it is a retry: the first attempt may have gone to
    /// a connection that died before this adapter read it.
    pub retry_attempt: u64,
    pub content: Content,
}

/// What an envelope carries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Content {
    /// A message addressed to the bot.
    Message(Box<BotMessage>),
    /// An `app_mention` or a DM whose channel or `ts` will not parse. Still
    /// acknowledged; logged by field and dropped.
    Malformed { field: &'static str },
    /// Some other event type, named for the log.
    OtherEvent(String),
    /// An envelope that is not an Events API event (`slash_commands`, …), or
    /// one carrying no event.
    NotAnEvent,
}

/// How a message reached the bot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Via {
    /// An `app_mention`: someone @mentioned the bot in a conversation it is in.
    Mention,
    /// A `message` event with `channel_type: "im"`: someone sent the bot a DM.
    Dm,
}

impl Via {
    pub fn as_str(self) -> &'static str {
        match self {
            Via::Mention => "mention",
            Via::Dm => "dm",
        }
    }
}

/// A message addressed to the bot, by mention or DM.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BotMessage {
    pub via: Via,
    pub event_id: Option<EventId>,
    /// When Slack says the event happened, in Unix seconds. Logged against the
    /// time it arrived, which is how a delivery delay is measured.
    pub event_time: Option<u64>,
    pub conversation: ConversationId,
    pub ts: SlackTs,
    /// Present when the message is in a thread.
    pub thread_ts: Option<SlackTs>,
    pub user: Option<SlackUserId>,
    pub bot_id: Option<String>,
    pub app_id: Option<SlackAppId>,
    /// `message_changed`, `message_deleted`, `bot_message`, `file_share`, …
    pub subtype: Option<Subtype>,
    pub hidden: bool,
}

impl BotMessage {
    /// The thread the message is a reply in. A thread's parent carries its own
    /// `ts` as `thread_ts`, and is not a reply.
    pub fn parent(&self) -> Option<&SlackTs> {
        self.thread_ts.as_ref().filter(|parent| **parent != self.ts)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum FrameError {
    /// The frame is not JSON. The serde error is not kept: it can quote the
    /// frame, which can hold message text.
    #[error("a Socket Mode frame was not JSON")]
    NotJson,
    /// An envelope with no usable `envelope_id` cannot be acknowledged.
    #[error("a Socket Mode envelope of type {kind:?} had no usable envelope_id")]
    NoEnvelopeId { kind: String },
}

#[derive(Deserialize)]
struct RawFrame {
    #[serde(rename = "type", default)]
    kind: Option<String>,
    #[serde(default)]
    envelope_id: Option<String>,
    #[serde(default)]
    num_connections: Option<u64>,
    #[serde(default)]
    reason: Option<String>,
    #[serde(default)]
    retry_attempt: Option<u64>,
    #[serde(default)]
    payload: Option<Value>,
}

#[derive(Deserialize)]
struct RawMessage {
    #[serde(rename = "type", default)]
    kind: String,
    #[serde(default)]
    channel_type: Option<String>,
    #[serde(default)]
    channel: Option<String>,
    #[serde(default)]
    ts: Option<String>,
    #[serde(default)]
    thread_ts: Option<String>,
    #[serde(default, deserialize_with = "lenient")]
    user: Option<SlackUserId>,
    #[serde(default)]
    bot_id: Option<String>,
    #[serde(default, deserialize_with = "lenient")]
    app_id: Option<SlackAppId>,
    #[serde(default)]
    subtype: Option<Subtype>,
    #[serde(default)]
    hidden: bool,
}

impl Frame {
    pub fn parse(text: &str) -> Result<Frame, FrameError> {
        let raw: RawFrame = serde_json::from_str(text).map_err(|_| FrameError::NotJson)?;
        let kind = raw.kind.clone();
        match kind.as_deref() {
            Some("hello") => {
                return Ok(Frame::Hello {
                    num_connections: raw.num_connections,
                });
            }
            Some("disconnect") => {
                let reason = match raw.reason.as_deref() {
                    Some("link_disabled") => DisconnectReason::LinkDisabled,
                    other => DisconnectReason::Refresh(other.unwrap_or("").to_string()),
                };
                return Ok(Frame::Disconnect { reason });
            }
            _ => {}
        }
        // Anything carrying an envelope id is an envelope, whatever its type:
        // an unknown envelope type still needs its ack.
        let Some(raw_id) = raw.envelope_id else {
            if raw.payload.is_some() {
                return Err(FrameError::NoEnvelopeId {
                    kind: kind.unwrap_or_default(),
                });
            }
            return Ok(Frame::Other { kind });
        };
        // The id is echoed back in the ack and logged, so it is bounded.
        if raw_id.is_empty() || raw_id.len() > MAX_ID_LEN {
            return Err(FrameError::NoEnvelopeId {
                kind: kind.unwrap_or_default(),
            });
        }
        let content = match (kind.as_deref(), raw.payload) {
            (Some("events_api"), Some(payload)) => content_of(payload),
            _ => Content::NotAnEvent,
        };
        Ok(Frame::Envelope(Box::new(Envelope {
            id: EnvelopeId(raw_id),
            kind: kind.unwrap_or_default(),
            retry_attempt: raw.retry_attempt.unwrap_or(0),
            content,
        })))
    }
}

/// What an `events_api` payload carries. The wrapper's `event_id` and
/// `event_time` only decorate a message, so they are read leniently: an odd one
/// must not cost the message.
fn content_of(mut payload: Value) -> Content {
    let event_id = payload
        .get("event_id")
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty() && id.len() <= MAX_ID_LEN)
        .map(|id| EventId(id.to_string()));
    let event_time = payload.get("event_time").and_then(Value::as_u64);
    let Some(event) = payload.get_mut("event").map(Value::take) else {
        return Content::NotAnEvent;
    };
    let Ok(raw) = serde_json::from_value::<RawMessage>(event) else {
        return Content::OtherEvent("<unreadable event>".to_string());
    };
    let via = match (raw.kind.as_str(), raw.channel_type.as_deref()) {
        ("app_mention", _) => Via::Mention,
        ("message", Some("im")) => Via::Dm,
        ("message", Some(other)) => return Content::OtherEvent(format!("message.{other}")),
        (other, _) => return Content::OtherEvent(other.to_string()),
    };
    let Some(conversation) = raw.channel.as_deref().and_then(ConversationId::parse) else {
        return Content::Malformed { field: "channel" };
    };
    let Some(ts) = raw.ts.as_deref().and_then(|ts| SlackTs::parse(ts).ok()) else {
        return Content::Malformed { field: "ts" };
    };
    let thread_ts = match raw.thread_ts.as_deref().map(SlackTs::parse) {
        None => None,
        Some(Ok(parent)) => Some(parent),
        // Read as absent, a malformed `thread_ts` would pass a reply off as a
        // top-level message.
        Some(Err(_)) => return Content::Malformed { field: "thread_ts" },
    };
    Content::Message(Box::new(BotMessage {
        via,
        event_id,
        event_time,
        conversation,
        ts,
        thread_ts,
        user: raw.user,
        bot_id: raw.bot_id,
        app_id: raw.app_id,
        subtype: raw.subtype,
        hidden: raw.hidden,
    }))
}

/// The acknowledgement for an envelope.
pub fn ack(id: &EnvelopeId) -> String {
    serde_json::json!({ "envelope_id": id.as_str() }).to_string()
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    /// An `events_api` envelope carrying `event`, shaped like the ones measured
    /// live (design/04).
    fn events_api(envelope_id: &str, event: Value) -> String {
        json!({
            "envelope_id": envelope_id, "type": "events_api",
            "accepts_response_payload": false, "retry_attempt": 0, "retry_reason": "",
            "payload": {
                "token": "legacy-verification-token", "team_id": "T0123",
                "api_app_id": "A0123", "type": "event_callback", "event_id": "Ev1",
                "event_time": 1_791_349_500, "event": event,
            },
        })
        .to_string()
    }

    fn mention(channel: &str, ts: &str, thread_ts: Option<&str>) -> Value {
        let mut event = json!({
            "type": "app_mention", "user": "U0AB7RJSQBE", "text": "<@U0C6VHAA4LX> help",
            "ts": ts, "channel": channel, "event_ts": ts,
        });
        if let Some(parent) = thread_ts {
            event["thread_ts"] = json!(parent);
        }
        event
    }

    fn dm(extra: Value) -> Value {
        let mut event = json!({
            "type": "message", "channel_type": "im", "channel": "D0C8ABCDEF",
            "user": "U0AB7RJSQBE", "text": "private words", "ts": "1791935400.000100",
        });
        for (key, value) in extra.as_object().unwrap() {
            event[key] = value.clone();
        }
        event
    }

    fn content(frame: &str) -> Content {
        match Frame::parse(frame).unwrap() {
            Frame::Envelope(envelope) => envelope.content,
            other => panic!("expected an envelope, got {other:?}"),
        }
    }

    fn message(frame: &str) -> BotMessage {
        match content(frame) {
            Content::Message(message) => *message,
            other => panic!("expected a message, got {other:?}"),
        }
    }

    #[test]
    fn hello_and_disconnect_frames_parse() {
        let hello = r#"{"type":"hello","num_connections":2,"debug_info":{"approximate_connection_time":18060},"connection_info":{"app_id":"A1"}}"#;
        assert_eq!(
            Frame::parse(hello).unwrap(),
            Frame::Hello {
                num_connections: Some(2)
            }
        );
        let disconnect = |reason: &str| {
            Frame::parse(&format!(r#"{{"type":"disconnect","reason":"{reason}"}}"#)).unwrap()
        };
        assert_eq!(
            disconnect("link_disabled"),
            Frame::Disconnect {
                reason: DisconnectReason::LinkDisabled
            }
        );
        assert_eq!(
            disconnect("refresh_requested"),
            Frame::Disconnect {
                reason: DisconnectReason::Refresh("refresh_requested".to_string())
            }
        );
    }

    #[test]
    fn a_mention_reads_where_and_who_and_never_the_text() {
        let frame = events_api(
            "env-1",
            mention(
                "C0C83CXLUL8",
                "1791349500.000002",
                Some("1791349480.652779"),
            ),
        );
        let envelope = match Frame::parse(&frame).unwrap() {
            Frame::Envelope(envelope) => envelope,
            other => panic!("expected an envelope, got {other:?}"),
        };
        assert_eq!(envelope.id.as_str(), "env-1");
        assert!(!format!("{envelope:?}").contains("help"));
        let message = message(&frame);
        assert_eq!(message.via, Via::Mention);
        assert_eq!(message.event_id.as_ref().unwrap().as_str(), "Ev1");
        assert_eq!(message.conversation.as_str(), "C0C83CXLUL8");
        assert_eq!(
            message.parent().map(ToString::to_string).as_deref(),
            Some("1791349480.652779")
        );
    }

    #[test]
    fn a_dm_is_a_message_event_in_an_im_and_never_carries_its_text() {
        let message = message(&events_api("env-1", dm(json!({}))));
        assert_eq!(message.via, Via::Dm);
        assert_eq!(message.conversation.as_str(), "D0C8ABCDEF");
        assert!(!format!("{message:?}").contains("private words"));
    }

    /// Subtypes are kept, so the wake rules can skip edits and deletions.
    #[test]
    fn a_dm_edit_keeps_its_subtype() {
        let message = message(&events_api(
            "env-1",
            dm(json!({"subtype": "message_changed", "hidden": true})),
        ));
        assert_eq!(
            message.subtype,
            Some(Subtype::Other("message_changed".to_string()))
        );
        assert!(message.hidden);
    }

    #[test]
    fn a_message_outside_a_dm_is_another_event() {
        let frame = events_api("env-1", dm(json!({"channel_type": "channel"})));
        assert_eq!(
            content(&frame),
            Content::OtherEvent("message.channel".to_string())
        );
    }

    #[test]
    fn a_threads_parent_is_not_a_reply() {
        let ts = "1791349480.652779";
        let frame = events_api("env-1", mention("C0C83CXLUL8", ts, Some(ts)));
        assert_eq!(message(&frame).parent(), None);
    }

    /// Every envelope is acknowledged, so one that carries no message still
    /// parses into an envelope with its id.
    #[test]
    fn other_envelopes_and_events_keep_their_id_for_the_ack() {
        let reaction = events_api("env-2", json!({"type": "reaction_added"}));
        assert_eq!(
            content(&reaction),
            Content::OtherEvent("reaction_added".to_string())
        );
        let slash = r#"{"envelope_id":"env-3","type":"slash_commands","payload":{"command":"/x"}}"#;
        assert_eq!(content(slash), Content::NotAnEvent);
    }

    #[test]
    fn a_malformed_message_is_still_an_envelope_to_ack() {
        let bad_ts = events_api("env-4", mention("C0C83CXLUL8", "not-a-ts", None));
        assert_eq!(content(&bad_ts), Content::Malformed { field: "ts" });
        let bad_thread = events_api(
            "env-5",
            mention("C0C83CXLUL8", "1791349500.000002", Some("nope")),
        );
        assert_eq!(
            content(&bad_thread),
            Content::Malformed { field: "thread_ts" }
        );
        let bad_channel = events_api("env-6", mention("#general", "1791349500.000002", None));
        assert_eq!(
            content(&bad_channel),
            Content::Malformed { field: "channel" }
        );
    }

    #[test]
    fn an_odd_event_time_or_id_keeps_the_message() {
        let frame = json!({
            "envelope_id": "env-1", "type": "events_api",
            "payload": {"event_id": 42, "event_time": "soon",
                "event": mention("C0C83CXLUL8", "1791349500.000002", None)},
        })
        .to_string();
        let message = message(&frame);
        assert_eq!((message.event_id, message.event_time), (None, None));
    }

    #[test]
    fn garbage_is_an_error_that_quotes_nothing() {
        let err = Frame::parse("secret text {").unwrap_err();
        assert_eq!(err, FrameError::NotJson);
        assert!(!err.to_string().contains("secret"));
    }

    #[test]
    fn conversation_ids_take_channels_groups_and_dms_only() {
        for good in ["C0C83CXLUL8", "G0123ABC", "D0C8ABCDEF"] {
            assert!(ConversationId::parse(good).is_some(), "{good}");
        }
        for bad in ["", "U0AB7RJSQBE", "#general", "Cab", "D"] {
            assert!(ConversationId::parse(bad).is_none(), "{bad}");
        }
    }

    #[test]
    fn the_ack_is_the_envelope_id() {
        assert_eq!(
            ack(&EnvelopeId("env-1".to_string())),
            r#"{"envelope_id":"env-1"}"#
        );
    }
}
