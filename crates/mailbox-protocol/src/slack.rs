//! Slack watch targets and their topic grammar (design/02).
//!
//! ```text
//! slack.channel.<channel-id>                 e.g. slack.channel.C0C83CXLUL8
//! slack.thread.<channel-id>/<thread-ts>      e.g. slack.thread.C0C83CXLUL8/1791349480.652779
//! ```
//!
//! A channel watch wakes on new top-level messages; a thread watch wakes on new
//! replies in one thread. They are separate targets because a channel's top level
//! and its threads are separate Slack reads (`conversations.history` returns no
//! replies), and because an agent following one conversation should not be woken
//! by every other conversation in the channel.
//!
//! Channels are addressed by **id**, never by name: a channel can be renamed, and
//! a watch keyed by a name would silently start watching nothing.

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::topic::Topic;

/// Prefix shared by every Slack channel topic.
const SLACK_CHANNEL_PREFIX: &str = "slack.channel.";

/// Prefix shared by every Slack thread topic.
const SLACK_THREAD_PREFIX: &str = "slack.thread.";

/// Slack ids are short; this bounds what an untrusted CLI arg or stored row can
/// make us carry, well under the topic length limit.
const MAX_CHANNEL_ID_LEN: usize = 32;

/// Slack message timestamps are `<seconds>.<6-digit microseconds>`.
const TS_FRACTION_DIGITS: usize = 6;

/// Why a string is not a valid Slack watch target.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SlackTargetError {
    // The echoed value is a bounded id the caller typed, never message content.
    #[error(
        "Slack channel id {value:?} is invalid: expected an id like C0C83CXLUL8 (a channel name will not do)"
    )]
    InvalidChannel { value: String },
    #[error(
        "Slack message timestamp {value:?} is invalid: expected <seconds>.<6 digits>, e.g. 1791349480.652779"
    )]
    InvalidTs { value: String },
    #[error(
        "not a Slack thread target: expected <channel-id>/<thread-ts>, e.g. C0C83CXLUL8/1791349480.652779"
    )]
    NotThread,
    #[error(
        "not a Slack link: expected https://<workspace>.slack.com/archives/<channel-id>[/p<ts>]"
    )]
    NotPermalink,
}

/// A Slack conversation id for a public or private channel (`C…`, or the legacy
/// `G…` private-channel prefix). Direct messages (`D…`) are not watchable: the
/// bot is granted no `im:history` scope.
///
/// Deserialization parses (`#[serde(try_from)]`), so a channel id decoded from a
/// control frame or a Slack reply is as valid as one built by hand.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct SlackChannelId(String);

impl SlackChannelId {
    pub fn parse(raw: &str) -> Result<Self, SlackTargetError> {
        let invalid = || SlackTargetError::InvalidChannel {
            value: raw.to_string(),
        };
        let mut chars = raw.chars();
        let first = chars.next().ok_or_else(invalid)?;
        let rest_ok = chars
            .clone()
            .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit());
        if !matches!(first, 'C' | 'G')
            || !rest_ok
            || raw.len() < 3
            || raw.len() > MAX_CHANNEL_ID_LEN
        {
            return Err(invalid());
        }
        Ok(Self(raw.to_string()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for SlackChannelId {
    type Error = SlackTargetError;

    fn try_from(raw: String) -> Result<Self, Self::Error> {
        Self::parse(&raw)
    }
}

impl From<SlackChannelId> for String {
    fn from(id: SlackChannelId) -> Self {
        id.0
    }
}

impl fmt::Display for SlackChannelId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A Slack message timestamp, which is also a message's id within its channel
/// and a thread's id (its parent's `ts`).
///
/// Held as numbers, not the string it was parsed from, so equality and order
/// agree: `01791349480.652779` and `1791349480.652779` are the same message, and
/// "newer than the cursor" cannot go wrong on a seconds field that changes width.
/// It always renders in Slack's canonical form. Field order is `seconds` then
/// `micros`, which is what the derived `Ord` compares.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct SlackTs {
    seconds: u64,
    micros: u32,
}

impl SlackTs {
    /// The cursor for "before any message": a channel with no history baselines
    /// here, so its first message is new.
    pub fn zero() -> Self {
        Self {
            seconds: 0,
            micros: 0,
        }
    }

    pub fn parse(raw: &str) -> Result<Self, SlackTargetError> {
        let invalid = || SlackTargetError::InvalidTs {
            value: raw.to_string(),
        };
        let (seconds, micros) = raw.split_once('.').ok_or_else(invalid)?;
        let digits = |s: &str| !s.is_empty() && s.chars().all(|c| c.is_ascii_digit());
        if !digits(seconds) || !digits(micros) || micros.len() != TS_FRACTION_DIGITS {
            return Err(invalid());
        }
        Ok(Self {
            seconds: seconds.parse().map_err(|_| invalid())?,
            micros: micros.parse().map_err(|_| invalid())?,
        })
    }

    /// Parse the `p<digits>` message segment of a Slack permalink, which is the
    /// timestamp with its dot removed (`p1791349480652779` ⇒ `1791349480.652779`).
    pub fn from_permalink_segment(segment: &str) -> Result<Self, SlackTargetError> {
        let invalid = || SlackTargetError::InvalidTs {
            value: segment.to_string(),
        };
        let digits = segment.strip_prefix('p').ok_or_else(invalid)?;
        if digits.len() <= TS_FRACTION_DIGITS || !digits.chars().all(|c| c.is_ascii_digit()) {
            return Err(invalid());
        }
        let (seconds, micros) = digits.split_at(digits.len() - TS_FRACTION_DIGITS);
        Self::parse(&format!("{seconds}.{micros}"))
    }

    /// The permalink form: the timestamp with its dot removed.
    pub fn permalink_digits(&self) -> String {
        format!("{}{:06}", self.seconds, self.micros)
    }
}

impl fmt::Display for SlackTs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{:06}", self.seconds, self.micros)
    }
}

impl TryFrom<String> for SlackTs {
    type Error = SlackTargetError;

    fn try_from(raw: String) -> Result<Self, Self::Error> {
        Self::parse(&raw)
    }
}

impl From<SlackTs> for String {
    fn from(ts: SlackTs) -> Self {
        ts.to_string()
    }
}

/// What a Slack watch is for.
///
/// It crosses the control socket as itself, tagged by `kind`, so the daemon
/// decodes a parsed value rather than strings it must remember to check.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SlackWatch {
    /// New top-level messages in a channel.
    Channel { channel: SlackChannelId },
    /// New replies in one thread, named by its parent message's `ts`.
    Thread {
        channel: SlackChannelId,
        thread_ts: SlackTs,
    },
}

impl SlackWatch {
    pub fn channel(&self) -> &SlackChannelId {
        match self {
            SlackWatch::Channel { channel } | SlackWatch::Thread { channel, .. } => channel,
        }
    }

    pub fn thread_ts(&self) -> Option<&SlackTs> {
        match self {
            SlackWatch::Channel { .. } => None,
            SlackWatch::Thread { thread_ts, .. } => Some(thread_ts),
        }
    }

    /// The watch's identity as one string: `<channel>` or `<channel>/<thread-ts>`.
    /// This is what the CLI accepts, what `status` shows, and what storage keys
    /// the watch row by. [`SlackWatch::parse_channel_key`] and
    /// [`SlackWatch::parse_thread_key`] are its inverse.
    pub fn key(&self) -> String {
        match self {
            SlackWatch::Channel { channel } => channel.to_string(),
            SlackWatch::Thread { channel, thread_ts } => format!("{channel}/{thread_ts}"),
        }
    }

    /// Parse a channel key (`<channel>`).
    pub fn parse_channel_key(raw: &str) -> Result<Self, SlackTargetError> {
        SlackChannelId::parse(raw).map(|channel| SlackWatch::Channel { channel })
    }

    /// Parse a thread key (`<channel>/<thread-ts>`).
    pub fn parse_thread_key(raw: &str) -> Result<Self, SlackTargetError> {
        let (channel, thread_ts) = raw.split_once('/').ok_or(SlackTargetError::NotThread)?;
        Ok(SlackWatch::Thread {
            channel: SlackChannelId::parse(channel)?,
            thread_ts: SlackTs::parse(thread_ts)?,
        })
    }

    /// Parse a Slack link into the watch it names, so an agent can watch what it
    /// is looking at without taking a link apart by hand.
    ///
    /// - `…/archives/<C>` names the channel.
    /// - `…/archives/<C>/p<ts>` names the thread that message starts.
    /// - `…/archives/<C>/p<ts>?thread_ts=<parent>&…` is a *reply*'s link, and
    ///   names the thread it is in (its parent), not a thread of its own.
    pub fn parse_link(url: &str) -> Result<Self, SlackTargetError> {
        let rest = url
            .strip_prefix("https://")
            .and_then(|rest| rest.split_once("/archives/"))
            .map(|(_host, rest)| rest)
            .ok_or(SlackTargetError::NotPermalink)?;
        let (path, query) = rest.split_once('?').unwrap_or((rest, ""));
        let mut segments = path.trim_end_matches('/').split('/');
        let channel = SlackChannelId::parse(segments.next().unwrap_or(""))?;
        let message = segments.next();
        if segments.next().is_some() {
            return Err(SlackTargetError::NotPermalink);
        }
        let Some(message) = message else {
            return Ok(SlackWatch::Channel { channel });
        };
        let parent = query
            .split('&')
            .find_map(|pair| pair.strip_prefix("thread_ts="));
        let thread_ts = match parent {
            Some(parent) => SlackTs::parse(parent)?,
            None => SlackTs::from_permalink_segment(message)?,
        };
        Ok(SlackWatch::Thread { channel, thread_ts })
    }

    /// The canonical topic this watch publishes on.
    pub fn topic(&self) -> Topic {
        let raw = match self {
            SlackWatch::Channel { channel } => format!("{SLACK_CHANNEL_PREFIX}{channel}"),
            SlackWatch::Thread { channel, thread_ts } => {
                format!("{SLACK_THREAD_PREFIX}{channel}/{thread_ts}")
            }
        };
        // Both segments are bounded ASCII with no whitespace or control
        // characters, so the assembled topic always satisfies the grammar.
        Topic::parse(raw).expect("a Slack watch topic is always a valid topic")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn channel_ids_must_look_like_ids_not_names() {
        assert!(SlackChannelId::parse("C0C83CXLUL8").is_ok());
        assert!(SlackChannelId::parse("G01ABCDEF").is_ok());
        for bad in [
            "",
            "#team-arg-agent-watercooler",
            "team-arg-agent-watercooler",
            "c0c83cxlul8",
            "D012345678",
            "C",
            "C0C8 3CX",
        ] {
            assert!(
                SlackChannelId::parse(bad).is_err(),
                "{bad:?} must be rejected"
            );
        }
    }

    #[test]
    fn ts_requires_six_fraction_digits() {
        assert!(SlackTs::parse("1791349480.652779").is_ok());
        for bad in [
            "",
            "1791349480",
            "1791349480.65277",
            "1791349480.6527790",
            "a.123456",
            ".123456",
        ] {
            assert!(SlackTs::parse(bad).is_err(), "{bad:?} must be rejected");
        }
    }

    #[test]
    fn ts_orders_numerically_not_lexically() {
        // As strings "999999999.000000" > "1000000000.000000"; as times it is older.
        let older = SlackTs::parse("999999999.000000").unwrap();
        let newer = SlackTs::parse("1000000000.000000").unwrap();
        assert!(older < newer);
        assert!(SlackTs::zero() < older);
    }

    #[test]
    fn equal_times_are_equal_however_they_were_written() {
        let padded = SlackTs::parse("01791349480.652779").unwrap();
        let plain = SlackTs::parse("1791349480.652779").unwrap();
        assert_eq!(padded, plain);
        assert_eq!(
            padded.to_string(),
            "1791349480.652779",
            "renders canonically"
        );
        assert_eq!(
            SlackWatch::parse_thread_key("C0C83CXLUL8/01791349480.652779")
                .unwrap()
                .key(),
            "C0C83CXLUL8/1791349480.652779",
            "so it cannot mint a second watch for the same thread"
        );
    }

    #[test]
    fn a_watch_crosses_the_wire_tagged_and_parsed() {
        let thread = SlackWatch::parse_thread_key("C0C83CXLUL8/1791349480.652779").unwrap();
        let wire = serde_json::to_value(&thread).unwrap();
        assert_eq!(
            wire,
            serde_json::json!({"kind": "thread", "channel": "C0C83CXLUL8",
                "thread_ts": "1791349480.652779"})
        );
        assert_eq!(serde_json::from_value::<SlackWatch>(wire).unwrap(), thread);
        let bad = serde_json::json!({"kind": "channel", "channel": "general"});
        assert!(
            serde_json::from_value::<SlackWatch>(bad).is_err(),
            "decoding parses"
        );
    }

    #[test]
    fn permalink_segment_round_trips() {
        let ts = SlackTs::from_permalink_segment("p1791349480652779").unwrap();
        assert_eq!(ts.to_string(), "1791349480.652779");
        assert_eq!(ts.permalink_digits(), "1791349480652779");
        assert!(SlackTs::from_permalink_segment("1791349480652779").is_err());
        assert!(SlackTs::from_permalink_segment("p123").is_err());
    }

    #[test]
    fn keys_and_topics_round_trip() {
        let channel = SlackWatch::parse_channel_key("C0C83CXLUL8").unwrap();
        assert_eq!(channel.key(), "C0C83CXLUL8");
        assert_eq!(channel.topic().as_str(), "slack.channel.C0C83CXLUL8");

        let thread = SlackWatch::parse_thread_key("C0C83CXLUL8/1791349480.652779").unwrap();
        assert_eq!(thread.key(), "C0C83CXLUL8/1791349480.652779");
        assert_eq!(
            thread.topic().as_str(),
            "slack.thread.C0C83CXLUL8/1791349480.652779"
        );
        assert_eq!(SlackWatch::parse_thread_key(&thread.key()).unwrap(), thread);

        assert_eq!(
            SlackWatch::parse_thread_key("C0C83CXLUL8"),
            Err(SlackTargetError::NotThread)
        );
    }

    #[test]
    fn links_name_the_channel_or_the_thread() {
        let base = "https://tryrelevance.slack.com/archives/C0C83CXLUL8";
        assert_eq!(
            SlackWatch::parse_link(base).unwrap(),
            SlackWatch::parse_channel_key("C0C83CXLUL8").unwrap()
        );
        // A top-level message's link names the thread it starts.
        assert_eq!(
            SlackWatch::parse_link(&format!("{base}/p1791349480652779"))
                .unwrap()
                .key(),
            "C0C83CXLUL8/1791349480.652779"
        );
        // A reply's link names its parent's thread, not the reply.
        assert_eq!(
            SlackWatch::parse_link(&format!(
                "{base}/p1791349999000001?thread_ts=1791349480.652779&cid=C0C83CXLUL8"
            ))
            .unwrap()
            .key(),
            "C0C83CXLUL8/1791349480.652779"
        );
        for bad in [
            "http://tryrelevance.slack.com/archives/C0C83CXLUL8",
            "https://tryrelevance.slack.com/C0C83CXLUL8",
            "https://tryrelevance.slack.com/archives/general",
            "https://tryrelevance.slack.com/archives/C0C83CXLUL8/p1/extra",
        ] {
            assert!(
                SlackWatch::parse_link(bad).is_err(),
                "{bad:?} must be rejected"
            );
        }
    }
}
