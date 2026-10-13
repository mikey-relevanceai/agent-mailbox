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
//! token. Neither is deserialized: [`AppMention`] reads only the fields a wake
//! and a `--skip` filter need.

use mailbox_protocol::{SlackAppId, SlackChannelId, SlackTs, SlackUserId};
use serde::Deserialize;
use serde_json::Value;

use crate::message::lenient;

/// One text frame from Slack.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Frame {
    /// The connection is ready. `num_connections` counts every connection open
    /// for the app, this one included.
    Hello { num_connections: Option<u64> },
    /// Slack is about to close this connection.
    Disconnect { reason: DisconnectReason },
    /// A payload to acknowledge, and an app mention when it carries one. Boxed:
    /// it is far larger than the other frames.
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
    pub envelope_id: String,
    /// `events_api`, `slash_commands`, `interactive`, …
    pub kind: String,
    /// Slack's redelivery count: `0` on the first attempt. Logged only. A retry
    /// is NOT dropped because it is a retry: the first attempt may have gone to
    /// a connection that died before this adapter read it.
    pub retry_attempt: u64,
    pub mention: Option<AppMention>,
    /// The inner event's `type` when it is not an `app_mention`, for the log.
    pub other_event: Option<String>,
}

/// An `app_mention` event: someone @mentioned the bot in a conversation it is in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppMention {
    /// The Events API's id for the event. A redelivery carries the same one
    /// (inferred from Slack's Events API retry docs; see design/04), which is
    /// what duplicates are dropped by.
    pub event_id: Option<String>,
    /// When Slack says the event happened, in Unix seconds. Logged against the
    /// time it arrived, which is how a delivery delay is measured.
    pub event_time: Option<u64>,
    pub channel: SlackChannelId,
    pub ts: SlackTs,
    /// Present when the mention is a reply in a thread.
    pub thread_ts: Option<SlackTs>,
    pub user: Option<SlackUserId>,
    pub bot_id: Option<String>,
    pub app_id: Option<SlackAppId>,
}

impl AppMention {
    /// The thread the mention is a reply in. A thread's parent carries its own
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
    /// An envelope with no `envelope_id` cannot be acknowledged.
    #[error("a Socket Mode envelope of type {kind:?} had no envelope_id")]
    NoEnvelopeId { kind: String },
    /// An `app_mention` whose channel or `ts` will not parse. The envelope id is
    /// kept, so it is still acknowledged.
    #[error("an app_mention in envelope {envelope_id} had a malformed {field}")]
    MalformedMention {
        envelope_id: String,
        field: &'static str,
    },
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

/// The Events API wrapper inside an `events_api` envelope.
#[derive(Deserialize)]
struct EventCallback {
    #[serde(default)]
    event_id: Option<String>,
    #[serde(default)]
    event_time: Option<u64>,
    #[serde(default)]
    event: Option<Value>,
}

#[derive(Deserialize)]
struct RawMention {
    channel: String,
    ts: String,
    #[serde(default)]
    thread_ts: Option<String>,
    #[serde(default, deserialize_with = "lenient")]
    user: Option<SlackUserId>,
    #[serde(default)]
    bot_id: Option<String>,
    #[serde(default, deserialize_with = "lenient")]
    app_id: Option<SlackAppId>,
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
        let Some(envelope_id) = raw.envelope_id else {
            if raw.payload.is_some() {
                return Err(FrameError::NoEnvelopeId {
                    kind: kind.unwrap_or_default(),
                });
            }
            return Ok(Frame::Other { kind });
        };
        let (mention, other_event) = match (kind.as_deref(), raw.payload) {
            (Some("events_api"), Some(payload)) => event_of(&envelope_id, payload)?,
            _ => (None, None),
        };
        Ok(Frame::Envelope(Box::new(Envelope {
            envelope_id,
            kind: kind.unwrap_or_default(),
            retry_attempt: raw.retry_attempt.unwrap_or(0),
            mention,
            other_event,
        })))
    }
}

/// The `app_mention` an `events_api` payload carries, or the type of the event
/// it carries instead.
fn event_of(
    envelope_id: &str,
    payload: Value,
) -> Result<(Option<AppMention>, Option<String>), FrameError> {
    let malformed = |field| FrameError::MalformedMention {
        envelope_id: envelope_id.to_string(),
        field,
    };
    let Ok(callback) = serde_json::from_value::<EventCallback>(payload) else {
        return Ok((None, Some("<unreadable payload>".to_string())));
    };
    let Some(event) = callback.event else {
        return Ok((None, Some("<no event>".to_string())));
    };
    let event_type = event
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    if event_type != "app_mention" {
        return Ok((None, Some(event_type)));
    }
    let raw: RawMention = serde_json::from_value(event).map_err(|_| malformed("shape"))?;
    let mention = AppMention {
        event_id: callback.event_id,
        event_time: callback.event_time,
        channel: SlackChannelId::parse(&raw.channel).map_err(|_| malformed("channel"))?,
        ts: SlackTs::parse(&raw.ts).map_err(|_| malformed("ts"))?,
        thread_ts: raw
            .thread_ts
            .map(|ts| SlackTs::parse(&ts))
            .transpose()
            .map_err(|_| malformed("thread_ts"))?,
        user: raw.user,
        bot_id: raw.bot_id,
        app_id: raw.app_id,
    };
    Ok((Some(mention), None))
}

/// The acknowledgement for an envelope.
pub fn ack(envelope_id: &str) -> String {
    serde_json::json!({ "envelope_id": envelope_id }).to_string()
}

#[cfg(test)]
pub mod tests_support {
    use serde_json::{Value, json};

    /// An `events_api` envelope carrying `event`, shaped like Slack's.
    pub fn events_api(envelope_id: &str, event_id: &str, event: Value) -> String {
        json!({
            "envelope_id": envelope_id,
            "type": "events_api",
            "accepts_response_payload": false,
            "retry_attempt": 0,
            "retry_reason": "",
            "payload": {
                "token": "legacy-verification-token",
                "team_id": "T0123",
                "api_app_id": "A0123",
                "type": "event_callback",
                "event_id": event_id,
                "event_time": 1_791_349_500,
                "event": event,
            },
        })
        .to_string()
    }

    pub fn mention_event(channel: &str, ts: &str, thread_ts: Option<&str>) -> Value {
        let mut event = json!({
            "type": "app_mention", "user": "U0AB7RJSQBE", "text": "<@U0C6VHAA4LX> help",
            "ts": ts, "channel": channel, "event_ts": ts,
        });
        if let Some(parent) = thread_ts {
            event["thread_ts"] = json!(parent);
        }
        event
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::tests_support::{events_api, mention_event};
    use super::*;

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
    fn an_app_mention_reads_where_and_who_and_never_the_text() {
        let frame = events_api(
            "env-1",
            "Ev1",
            mention_event(
                "C0C83CXLUL8",
                "1791349500.000002",
                Some("1791349480.652779"),
            ),
        );
        let Frame::Envelope(envelope) = Frame::parse(&frame).unwrap() else {
            panic!("an events_api frame is an envelope");
        };
        assert_eq!(envelope.envelope_id, "env-1");
        assert!(!format!("{envelope:?}").contains("help"));
        let mention = envelope.mention.unwrap();
        assert_eq!(mention.event_id.as_deref(), Some("Ev1"));
        assert_eq!(mention.channel.as_str(), "C0C83CXLUL8");
        assert_eq!(
            mention.parent().map(ToString::to_string).as_deref(),
            Some("1791349480.652779")
        );
        assert_eq!(mention.user.unwrap().as_str(), "U0AB7RJSQBE");
    }

    #[test]
    fn a_threads_parent_is_not_a_reply() {
        let ts = "1791349480.652779";
        let frame = events_api("env-1", "Ev1", mention_event("C0C83CXLUL8", ts, Some(ts)));
        let Frame::Envelope(envelope) = Frame::parse(&frame).unwrap() else {
            panic!("an envelope");
        };
        assert_eq!(envelope.mention.unwrap().parent(), None);
    }

    /// Every envelope is acknowledged, so one that carries no mention still
    /// parses into an envelope with its id.
    #[test]
    fn other_envelopes_and_events_keep_their_id_for_the_ack() {
        let reaction = events_api("env-2", "Ev2", json!({"type": "reaction_added"}));
        let Frame::Envelope(envelope) = Frame::parse(&reaction).unwrap() else {
            panic!("an envelope");
        };
        assert_eq!(envelope.mention, None);
        assert_eq!(envelope.other_event.as_deref(), Some("reaction_added"));

        let slash = r#"{"envelope_id":"env-3","type":"slash_commands","payload":{"command":"/x"}}"#;
        let Frame::Envelope(envelope) = Frame::parse(slash).unwrap() else {
            panic!("an envelope");
        };
        assert_eq!(envelope.envelope_id, "env-3");
    }

    #[test]
    fn a_malformed_mention_keeps_its_envelope_id_so_it_is_still_acked() {
        let frame = events_api(
            "env-4",
            "Ev4",
            mention_event("C0C83CXLUL8", "not-a-ts", None),
        );
        assert_eq!(
            Frame::parse(&frame).unwrap_err(),
            FrameError::MalformedMention {
                envelope_id: "env-4".to_string(),
                field: "ts"
            }
        );
    }

    #[test]
    fn garbage_is_an_error_that_quotes_nothing() {
        let err = Frame::parse("secret text {").unwrap_err();
        assert_eq!(err, FrameError::NotJson);
        assert!(!err.to_string().contains("secret"));
    }

    #[test]
    fn the_ack_is_the_envelope_id() {
        assert_eq!(ack("env-1"), r#"{"envelope_id":"env-1"}"#);
    }
}
