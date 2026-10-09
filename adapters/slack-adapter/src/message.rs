//! A Slack message, parsed once in the watcher's read step, before any rule
//! sees it.
//!
//! Slack's reply is untrusted input. Everything the wake rules look at is parsed
//! here into typed fields, so the rules never read raw JSON, and a message whose
//! `ts` or `thread_ts` is malformed is dropped whole instead of being read as if
//! the field were absent (which would let a reply pass for a top-level message).
//! Fields the adapter does not use, `text` above all, are never deserialized.

use mailbox_protocol::{SlackAppId, SlackTs, SlackUserId};
use serde::{Deserialize, Deserializer};

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
    #[serde(default, deserialize_with = "lenient")]
    pub user: Option<SlackUserId>,
    #[serde(default)]
    pub bot_id: Option<String>,
    /// The app a message was posted through, e.g. the claude.ai connector posting
    /// as a person's own user. Absent on a message typed in a Slack client
    /// (ADR-0029 has the measurement).
    #[serde(default, deserialize_with = "lenient")]
    pub app_id: Option<SlackAppId>,
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

/// An id field that reads as absent when it is not a valid id, rather than
/// failing the whole message. Unlike `ts`, these only decorate a message or feed
/// a `--skip` filter, and an absent id is the safe reading for both: no `user=` or
/// `app=` condition can match it, so a malformed id wakes rather than silences.
fn lenient<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: TryFrom<String>,
{
    let raw: Option<String> = Option::deserialize(deserializer)?;
    Ok(raw.and_then(|raw| T::try_from(raw).ok()))
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
            "subtype": "thread_broadcast", "user": "U0AB7RJSQBE", "text": "anything at all",
            "blocks": [{"type": "rich_text"}], "app_id": "A08SF47R6P4"
        }))
        .unwrap();
        assert_eq!(message.subtype, Some(Subtype::ThreadBroadcast));
        assert_eq!(message.user.unwrap().as_str(), "U0AB7RJSQBE");
        assert_eq!(message.app_id.unwrap().as_str(), "A08SF47R6P4");
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
    fn a_malformed_user_or_app_id_reads_as_absent_and_keeps_the_message() {
        let message: SlackMessage = serde_json::from_value(json!({
            "ts": "1791349500.000002", "user": "not a user", "app_id": "Claude"
        }))
        .unwrap();
        assert_eq!(message.user, None);
        assert_eq!(message.app_id, None);
    }

    #[test]
    fn unknown_subtypes_are_kept_by_name() {
        assert_eq!(
            Subtype::from("channel_join".to_string()),
            Subtype::Other("channel_join".to_string())
        );
    }
}
