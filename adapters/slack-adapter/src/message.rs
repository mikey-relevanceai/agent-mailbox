//! A Slack message, parsed once where it leaves the API.
//!
//! Slack's reply is untrusted input. Everything the wake rules look at is parsed
//! here into typed fields, so the rules never read raw JSON, and a message whose
//! `ts` or `thread_ts` is malformed is dropped whole instead of being read as if
//! the field were absent (which would let a reply pass for a top-level message).
//! Fields the adapter does not use, `text` above all, are never deserialized.

use mailbox_protocol::SlackTs;
use serde::Deserialize;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct SlackMessage {
    pub ts: SlackTs,
    #[serde(default)]
    pub thread_ts: Option<SlackTs>,
    /// On a thread's parent: its newest reply.
    #[serde(default)]
    pub latest_reply: Option<SlackTs>,
    #[serde(default)]
    pub subtype: Option<Subtype>,
    #[serde(default)]
    pub hidden: bool,
    #[serde(default)]
    pub user: Option<SlackUserId>,
    #[serde(default)]
    pub bot_id: Option<String>,
    /// The name a bot or integration posted under.
    #[serde(default)]
    pub username: Option<String>,
    #[serde(default)]
    pub bot_profile: Option<BotProfile>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct BotProfile {
    #[serde(default)]
    pub name: Option<String>,
}

/// A Slack user id (`U…`/`W…`). Distinct from a bot id so a bot's id cannot be
/// sent to `users.info`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize)]
#[serde(transparent)]
pub struct SlackUserId(String);

impl SlackUserId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A message subtype. The four named ones are someone saying something; every
/// other subtype is a change to the channel (`channel_join`, `channel_topic`, …)
/// or to a message (`message_changed`, `message_deleted`).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(from = "String")]
pub enum Subtype {
    BotMessage,
    FileShare,
    MeMessage,
    ThreadBroadcast,
    Other(String),
}

impl From<String> for Subtype {
    fn from(raw: String) -> Self {
        match raw.as_str() {
            "bot_message" => Subtype::BotMessage,
            "file_share" => Subtype::FileShare,
            "me_message" => Subtype::MeMessage,
            "thread_broadcast" => Subtype::ThreadBroadcast,
            _ => Subtype::Other(raw),
        }
    }
}

impl Subtype {
    pub fn as_str(&self) -> &str {
        match self {
            Subtype::BotMessage => "bot_message",
            Subtype::FileShare => "file_share",
            Subtype::MeMessage => "me_message",
            Subtype::ThreadBroadcast => "thread_broadcast",
            Subtype::Other(raw) => raw,
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn parses_the_fields_the_rules_use_and_ignores_the_text() {
        let message: SlackMessage = serde_json::from_value(json!({
            "ts": "1791349500.000002", "thread_ts": "1791349480.652779",
            "subtype": "thread_broadcast", "user": "U1", "text": "anything at all",
            "blocks": [{"type": "rich_text"}]
        }))
        .unwrap();
        assert_eq!(message.subtype, Some(Subtype::ThreadBroadcast));
        assert_eq!(message.user.unwrap().as_str(), "U1");
        assert!(!message.hidden);
    }

    #[test]
    fn a_malformed_thread_ts_rejects_the_message_rather_than_dropping_the_field() {
        let parsed = serde_json::from_value::<SlackMessage>(json!({
            "ts": "1791349500.000002", "thread_ts": "not-a-ts"
        }));
        assert!(parsed.is_err());
    }

    #[test]
    fn unknown_subtypes_are_kept_by_name() {
        assert_eq!(
            Subtype::from("channel_join".to_string()),
            Subtype::Other("channel_join".to_string())
        );
    }
}
