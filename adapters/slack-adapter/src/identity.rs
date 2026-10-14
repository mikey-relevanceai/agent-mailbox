//! Who the bot token belongs to, from `auth.test`: the workspace to build links
//! in, and the bot's own ids, so the bot is never woken by itself.

use std::fmt;

use mailbox_protocol::{SlackTs, SlackUserId};
use serde::Deserialize;

use crate::api::{SlackApi, SlackError, parse};
use crate::message::lenient;

/// `https://<workspace>.slack.com/`, always ending in `/`, so a permalink is
/// this followed by `archives/…`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceUrl(String);

impl WorkspaceUrl {
    pub fn parse(raw: &str) -> Self {
        let mut url = raw.to_string();
        if !url.ends_with('/') {
            url.push('/');
        }
        Self(url)
    }

    /// The link to message `ts` in `conversation`, inside its thread when
    /// `thread` names one.
    pub fn permalink(&self, conversation: &str, ts: &SlackTs, thread: Option<&SlackTs>) -> String {
        let base = format!(
            "{}archives/{conversation}/p{}",
            self.0,
            ts.permalink_digits()
        );
        match thread {
            Some(parent) => format!("{base}?thread_ts={parent}&cid={conversation}"),
            None => base,
        }
    }
}

impl fmt::Display for WorkspaceUrl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Deserialize)]
struct AuthTest {
    url: String,
    #[serde(default, deserialize_with = "lenient")]
    user_id: Option<SlackUserId>,
    #[serde(default)]
    bot_id: Option<String>,
}

/// What `auth.test` says about the bot token.
#[derive(Debug, Clone)]
pub struct Identity {
    pub workspace: WorkspaceUrl,
    /// The bot's user. Present for a bot token (measured: `U0C6VHAA4LX`).
    pub bot_user: Option<SlackUserId>,
    /// The bot's bot id, which a message it posts carries as `bot_id`.
    pub bot_id: Option<String>,
}

/// Check the token and learn who it belongs to.
pub async fn identify<A: SlackApi>(api: &A) -> Result<Identity, SlackError> {
    let auth: AuthTest = parse("auth.test", api.call("auth.test", &[]).await?)?;
    Ok(Identity {
        workspace: WorkspaceUrl::parse(&auth.url),
        bot_user: auth.user_id,
        bot_id: auth.bot_id,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_workspace_url_always_ends_in_a_slash() {
        let ts = SlackTs::parse("1791349500.000002").unwrap();
        for raw in [
            "https://tryrelevance.slack.com",
            "https://tryrelevance.slack.com/",
        ] {
            assert_eq!(
                WorkspaceUrl::parse(raw).permalink("C1", &ts, None),
                "https://tryrelevance.slack.com/archives/C1/p1791349500000002"
            );
        }
    }

    #[test]
    fn a_reply_links_into_its_thread() {
        let ts = SlackTs::parse("1791349500.000002").unwrap();
        let parent = SlackTs::parse("1791349480.652779").unwrap();
        assert_eq!(
            WorkspaceUrl::parse("https://w.slack.com/").permalink("D9", &ts, Some(&parent)),
            "https://w.slack.com/archives/D9/p1791349500000002?thread_ts=1791349480.652779&cid=D9"
        );
    }
}
