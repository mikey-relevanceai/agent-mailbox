//! Who the token belongs to, from `auth.test`: the workspace to build links in,
//! and, for a bot token, the bot's own ids, so the bot is never woken by itself.

use std::fmt;

use mailbox_protocol::{SlackTs, SlackUserId};
use serde::Deserialize;

use crate::api::{SlackApi, SlackError, parse};
use crate::message::lenient;
use crate::socket::{ConversationId, SlackBotId};

/// `https://<workspace>.slack.com/`, always ending in `/`, so a permalink is
/// this followed by `archives/…`. Taken from `auth.test`, which is trusted to
/// name a Slack URL; only the trailing slash is normalised.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceUrl(String);

impl WorkspaceUrl {
    pub fn from_auth_test(raw: &str) -> Self {
        let mut url = raw.to_string();
        if !url.ends_with('/') {
            url.push('/');
        }
        Self(url)
    }

    /// The link to message `ts` in `conversation`, inside its thread when
    /// `thread` names one.
    pub fn permalink(
        &self,
        conversation: &ConversationId,
        ts: &SlackTs,
        thread: Option<&SlackTs>,
    ) -> String {
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
    #[serde(default, deserialize_with = "lenient")]
    bot_id: Option<SlackBotId>,
}

/// What `auth.test` says about a token. The poll modes accept a user token too,
/// so the ids are optional here; the bot watch insists on them ([`BotIdentity`]).
#[derive(Debug, Clone)]
pub struct Identity {
    pub workspace: WorkspaceUrl,
    /// The token's user: the bot's, for a bot token (measured: `U0C6VHAA4LX`).
    pub user: Option<SlackUserId>,
    /// Present only for a bot token (measured: `B0C74MURRK5`).
    pub bot_id: Option<SlackBotId>,
}

/// Check the token and learn who it belongs to.
pub async fn identify<A: SlackApi>(api: &A) -> Result<Identity, SlackError> {
    let auth: AuthTest = parse("auth.test", api.call("auth.test", &[]).await?)?;
    Ok(Identity {
        workspace: WorkspaceUrl::from_auth_test(&auth.url),
        user: auth.user_id,
        bot_id: auth.bot_id,
    })
}

/// A bot token's identity, with both of the bot's ids.
///
/// The bot watch skips messages from these ids as the bot's own. With a user
/// token (`xoxp-`, which the Keychain item also accepts), the "own" user would
/// be a person, and their DMs to the bot would be skipped as the bot's. So the
/// bot watch refuses to run without a `bot_id`, which only a bot token has.
#[derive(Debug, Clone)]
pub struct BotIdentity {
    pub workspace: WorkspaceUrl,
    pub user: SlackUserId,
    pub bot_id: SlackBotId,
}

impl BotIdentity {
    pub fn require(identity: Identity) -> Result<Self, SlackError> {
        match (identity.user, identity.bot_id) {
            (Some(user), Some(bot_id)) => Ok(Self {
                workspace: identity.workspace,
                user,
                bot_id,
            }),
            _ => Err(SlackError::Failed(
                "the token under Keychain service agent-mailbox.slack is not a bot token (auth.test returned no user_id or bot_id); the bot watch needs the app's xoxb- token".to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::*;

    fn conversation(raw: &str) -> ConversationId {
        ConversationId::parse(raw).unwrap()
    }

    struct AuthTestReply(Value);

    impl SlackApi for AuthTestReply {
        async fn call(&self, method: &str, _: &[(&str, &str)]) -> Result<Value, SlackError> {
            assert_eq!(method, "auth.test");
            Ok(self.0.clone())
        }
    }

    /// The bot's own ids are what stop it waking itself, so the field names
    /// they are read from are pinned to the reply measured on 2026-10-14.
    #[tokio::test(flavor = "current_thread")]
    async fn identify_reads_the_bots_own_ids_from_auth_test() {
        let reply = json!({"ok": true, "url": "https://tryrelevance.slack.com/",
            "team": "Relevance AI", "user": "dispatch", "team_id": "T0",
            "user_id": "U0C6VHAA4LX", "bot_id": "B0C74MURRK5", "is_enterprise_install": false});
        let identity = identify(&AuthTestReply(reply)).await.unwrap();
        let bot = BotIdentity::require(identity).unwrap();
        assert_eq!(bot.user.as_str(), "U0C6VHAA4LX");
        assert_eq!(bot.bot_id.as_str(), "B0C74MURRK5");
        assert_eq!(bot.workspace.to_string(), "https://tryrelevance.slack.com/");
    }

    /// What a user token's `auth.test` looks like: a user, no bot.
    #[tokio::test(flavor = "current_thread")]
    async fn a_token_with_no_bot_id_cannot_run_the_bot_watch() {
        let reply = json!({"ok": true, "url": "https://w.slack.com", "user_id": "U0AB7RJSQBE"});
        let identity = identify(&AuthTestReply(reply)).await.unwrap();
        assert_eq!(identity.user.as_ref().unwrap().as_str(), "U0AB7RJSQBE");
        assert!(identity.bot_id.is_none());
        assert!(BotIdentity::require(identity).is_err());
    }

    #[test]
    fn a_workspace_url_always_ends_in_a_slash() {
        let ts = SlackTs::parse("1791349500.000002").unwrap();
        for raw in [
            "https://tryrelevance.slack.com",
            "https://tryrelevance.slack.com/",
        ] {
            assert_eq!(
                WorkspaceUrl::from_auth_test(raw).permalink(&conversation("C11"), &ts, None),
                "https://tryrelevance.slack.com/archives/C11/p1791349500000002"
            );
        }
    }

    #[test]
    fn a_reply_links_into_its_thread() {
        let ts = SlackTs::parse("1791349500.000002").unwrap();
        let parent = SlackTs::parse("1791349480.652779").unwrap();
        assert_eq!(
            WorkspaceUrl::from_auth_test("https://w.slack.com/").permalink(
                &conversation("D99"),
                &ts,
                Some(&parent)
            ),
            "https://w.slack.com/archives/D99/p1791349500000002?thread_ts=1791349480.652779&cid=D99"
        );
    }
}
