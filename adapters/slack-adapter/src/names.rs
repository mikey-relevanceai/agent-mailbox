//! Display names for the subjects a wake carries, looked up once per process.
//!
//! A name only decorates a subject, so every failure falls back to the id rather
//! than failing the event it describes.

use std::collections::HashMap;

use mailbox_protocol::SlackUserId;
use serde::Deserialize;
use tracing::debug;

use crate::api::{SlackApi, parse};

#[derive(Deserialize)]
struct UserInfo {
    user: UserNames,
}

#[derive(Deserialize)]
struct UserNames {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    real_name: Option<String>,
    #[serde(default)]
    profile: Option<UserProfile>,
}

#[derive(Deserialize)]
struct UserProfile {
    #[serde(default)]
    display_name: Option<String>,
}

#[derive(Deserialize)]
struct ChannelInfo {
    channel: ChannelName,
}

#[derive(Deserialize)]
struct ChannelName {
    #[serde(default)]
    name: Option<String>,
}

/// Users' display names, cached for the life of the process.
#[derive(Debug, Default)]
pub(crate) struct Names {
    users: HashMap<SlackUserId, String>,
}

impl Names {
    /// A user's display name, else their id.
    pub async fn get<A: SlackApi>(&mut self, api: &A, user: &SlackUserId) -> String {
        if let Some(name) = self.users.get(user) {
            return name.clone();
        }
        let reply = api.call("users.info", &[("user", user.as_str())]).await;
        let names = match reply.and_then(|reply| parse::<UserInfo>("users.info", reply)) {
            Ok(info) => info.user,
            Err(err) => {
                debug!(user = user.as_str(), error = %err, "could not look up a user's name; using the id");
                return user.as_str().to_string();
            }
        };
        let name = [
            names.profile.and_then(|profile| profile.display_name),
            names.real_name,
            names.name,
        ]
        .into_iter()
        .flatten()
        .find(|name| !name.is_empty())
        .unwrap_or_else(|| user.as_str().to_string());
        self.users.insert(user.clone(), name.clone());
        name
    }
}

/// `#<name>` for a channel, or `None` when Slack would not say. Errors are the
/// caller's to classify: the channel watch treats a failure here as part of
/// connecting, the bot watch as a missing decoration.
pub(crate) async fn channel_label<A: SlackApi>(
    api: &A,
    channel: &str,
) -> Result<Option<String>, crate::api::SlackError> {
    let info: ChannelInfo = parse(
        "conversations.info",
        api.call("conversations.info", &[("channel", channel)])
            .await?,
    )?;
    Ok(info.channel.name.map(|name| format!("#{name}")))
}
