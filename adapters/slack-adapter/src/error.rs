//! How the adapter fails, in every mode. A run ends with one of these, logged
//! once, and a non-zero exit the supervisor acts on.

use mailbox_protocol::SlackTargetError;

use crate::api::SlackError;
use crate::token::TokenError;

#[derive(Debug, thiserror::Error)]
pub enum AdapterError {
    #[error("could not read the config line from stdin: {0}")]
    ReadConfig(#[source] std::io::Error),
    #[error("no config line on stdin (expected one JSON object as the first line)")]
    EmptyConfig,
    #[error("config line exceeds the {0}-byte limit")]
    ConfigTooLarge(u64),
    #[error("could not parse the config line as JSON: {0}")]
    ParseConfig(#[source] serde_json::Error),
    #[error("the config names a watch kind this adapter does not run: {0:?}")]
    UnknownKind(String),
    #[error("config names an invalid Slack target: {0}")]
    InvalidTarget(#[from] SlackTargetError),
    // The resolver derives both from the same watch row; disagreement means the
    // bridge and this adapter have drifted, and publishing would be rejected.
    #[error("config topic {topic:?} does not match the Slack target's topic {expected:?}")]
    TopicMismatch { topic: String, expected: String },
    #[error("could not install the {0} signal handler: {1}")]
    Signal(&'static str, #[source] std::io::Error),
    #[error(transparent)]
    Token(#[from] TokenError),
    #[error(transparent)]
    Slack(#[from] SlackError),
    #[error("could not encode a line: {0}")]
    Encode(#[from] mailbox_protocol::FramingError),
    #[error("could not write a line to stdout: {0}")]
    Write(#[source] std::io::Error),
}
