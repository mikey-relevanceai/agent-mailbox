//! Which `app_mention` events wake, and what the wake says (design/04).
//!
//! A mention wakes once. Slack redelivers an event it thinks was not
//! acknowledged, and a redelivery carries the event's original `event_id`, so
//! the ids of published mentions are remembered and a repeat is dropped. The
//! remembered ids are this mode's baseline, persisted through the bridge
//! (ADR-0005), so a redelivery that lands after an adapter restart is dropped
//! too.
//!
//! A mention a `--skip` filter matches is dropped like a filtered channel
//! message (ADR-0029): acknowledged, logged, never published.

use std::collections::{HashMap, VecDeque};
use std::time::Duration;

use mailbox_protocol::{Poster, SlackChannelId, SlackFilter, SlackFilters, Subject};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tracing::debug;

use crate::api::{SlackApi, SlackError, parse};
use crate::names::{Names, channel_label};
use crate::socket::AppMention;

/// How many published event ids are remembered. A redelivery follows its
/// original within minutes (Slack's Events API retries span about six minutes,
/// per its docs; not measured for Socket Mode), and even a busy bot is
/// mentioned far fewer than this many times in that window.
const SEEN_CAPACITY: usize = 256;

/// The longest the name lookups for one subject may take. Slack expects an
/// envelope to be acknowledged within a few seconds, and the ack is sent after
/// the publish (design/04). Past this, the subject uses ids.
const LOOKUP_BUDGET: Duration = Duration::from_secs(2);

/// The event ids of recently published mentions, oldest first.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Seen {
    seen_event_ids: VecDeque<String>,
}

impl Seen {
    /// How many ids are remembered, for the log.
    pub fn remembered(&self) -> usize {
        self.seen_event_ids.len()
    }

    pub fn contains(&self, event_id: &str) -> bool {
        self.seen_event_ids.iter().any(|seen| seen == event_id)
    }

    pub fn insert(&mut self, event_id: String) {
        if self.contains(&event_id) {
            return;
        }
        if self.seen_event_ids.len() == SEEN_CAPACITY {
            self.seen_event_ids.pop_front();
        }
        self.seen_event_ids.push_back(event_id);
    }

    pub fn to_json(&self) -> Value {
        serde_json::to_value(self).expect("a list of ids always serializes")
    }

    /// `None` for a value this mode did not write; the caller starts empty.
    pub fn from_json(value: Value) -> Option<Self> {
        let mut seen: Seen = serde_json::from_value(value).ok()?;
        while seen.seen_event_ids.len() > SEEN_CAPACITY {
            seen.seen_event_ids.pop_front();
        }
        Some(seen)
    }
}

/// What to do with one `app_mention`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Disposition {
    Wake(Mention),
    /// Its `event_id` was already published: a redelivery.
    Duplicate,
    Filtered(SlackFilter),
}

/// A mention that wakes the watch's subscribers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mention {
    pub event: AppMention,
    pub author: String,
    pub channel_label: String,
    pub permalink: String,
}

impl Mention {
    /// Names the mention and links to it. Never quotes the text.
    pub fn subject(&self) -> Option<Subject> {
        let text = match self.event.parent() {
            None => format!("mention from {} in {}", self.author, self.channel_label),
            Some(_) => format!(
                "mention from {} in a thread in {}",
                self.author, self.channel_label
            ),
        };
        // A subject is a courtesy, never a gate (design/02).
        Subject::new(&text, Some(&self.permalink)).ok()
    }

    /// Where the mention is and who made it, never what it says.
    pub fn body(&self) -> Value {
        let event = &self.event;
        let mut body = json!({
            "kind": "slack_mention",
            "channel": event.channel.as_str(),
            "ts": event.ts.to_string(),
            "permalink": self.permalink,
        });
        let fields = [
            ("thread_ts", event.parent().map(ToString::to_string)),
            ("user", event.user.as_ref().map(|u| u.as_str().to_string())),
            ("bot_id", event.bot_id.clone()),
            (
                "app_id",
                event.app_id.as_ref().map(|a| a.as_str().to_string()),
            ),
            ("event_id", event.event_id.clone()),
        ];
        for (key, value) in fields {
            if let Some(value) = value {
                body[key] = Value::String(value);
            }
        }
        body
    }
}

#[derive(Deserialize)]
struct AuthTest {
    url: String,
    #[serde(default)]
    user_id: Option<String>,
}

/// What `auth.test` says about the bot token.
pub struct Identity {
    /// `https://<workspace>.slack.com/`, for building permalinks without a call.
    pub workspace_url: String,
    pub bot_user: Option<String>,
}

/// Check the bot token and learn the workspace URL.
pub async fn identify<A: SlackApi>(api: &A) -> Result<Identity, SlackError> {
    let auth: AuthTest = parse("auth.test", api.call("auth.test", &[]).await?)?;
    let mut workspace_url = auth.url;
    if !workspace_url.ends_with('/') {
        workspace_url.push('/');
    }
    Ok(Identity {
        workspace_url,
        bot_user: auth.user_id,
    })
}

/// The mention watch's state across connections.
pub(crate) struct Mentions<A> {
    /// Web API calls with the bot token, for names.
    api: A,
    workspace_url: String,
    skip: SlackFilters,
    seen: Seen,
    names: Names,
    channels: HashMap<SlackChannelId, String>,
}

impl<A: SlackApi> Mentions<A> {
    pub fn new(api: A, workspace_url: String, skip: SlackFilters, seen: Seen) -> Self {
        Self {
            api,
            workspace_url,
            skip,
            seen,
            names: Names::default(),
            channels: HashMap::new(),
        }
    }

    pub fn seen(&self) -> &Seen {
        &self.seen
    }

    /// Decide what one mention does. Recording it as published is the caller's
    /// job, after the publish is written: [`Mentions::published`].
    pub async fn dispose(&mut self, event: AppMention) -> Disposition {
        if event
            .event_id
            .as_deref()
            .is_some_and(|id| self.seen.contains(id))
        {
            return Disposition::Duplicate;
        }
        let poster = Poster {
            user: event.user.as_ref(),
            app: event.app_id.as_ref(),
        };
        if let Some(filter) = self.skip.matching(poster) {
            return Disposition::Filtered(filter.clone());
        }
        let permalink = self.permalink(&event);
        let fallback = (author_id(&event), event.channel.as_str().to_string());
        let (author, channel_label) =
            match tokio::time::timeout(LOOKUP_BUDGET, self.describe(&event)).await {
                Ok(described) => described,
                Err(_) => {
                    debug!("name lookups ran past their budget; the subject uses ids");
                    fallback
                }
            };
        Disposition::Wake(Mention {
            event,
            author,
            channel_label,
            permalink,
        })
    }

    /// Remember a published mention, so a redelivery of it is dropped.
    pub fn published(&mut self, mention: &Mention) {
        if let Some(id) = &mention.event.event_id {
            self.seen.insert(id.clone());
        }
    }

    async fn describe(&mut self, event: &AppMention) -> (String, String) {
        let author = match &event.user {
            Some(user) => self.names.get(&self.api, user).await,
            None => author_id(event),
        };
        let label = match self.channels.get(&event.channel) {
            Some(label) => label.clone(),
            None => match channel_label(&self.api, &event.channel).await {
                Ok(label) => {
                    let label = label.unwrap_or_else(|| event.channel.to_string());
                    self.channels.insert(event.channel.clone(), label.clone());
                    label
                }
                Err(err) => {
                    debug!(channel = event.channel.as_str(), error = %err, "could not look up a channel's name; using the id");
                    event.channel.to_string()
                }
            },
        };
        (author, label)
    }

    fn permalink(&self, event: &AppMention) -> String {
        let channel = &event.channel;
        let base = format!(
            "{}archives/{channel}/p{}",
            self.workspace_url,
            event.ts.permalink_digits()
        );
        match event.parent() {
            Some(parent) => format!("{base}?thread_ts={parent}&cid={channel}"),
            None => base,
        }
    }
}

fn author_id(event: &AppMention) -> String {
    event
        .user
        .as_ref()
        .map(|user| user.as_str().to_string())
        .or_else(|| event.bot_id.clone())
        .unwrap_or_else(|| "someone".to_string())
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use mailbox_protocol::{SlackTs, SlackUserId};

    use super::*;

    const WORKSPACE: &str = "https://tryrelevance.slack.com/";
    const CHANNEL: &str = "C0C83CXLUL8";
    const PARENT: &str = "1791349480.652779";
    const REPLY: &str = "1791349500.000002";

    /// Slack's lookups, answered from memory, counting calls.
    #[derive(Default)]
    struct FakeSlack {
        calls: RefCell<Vec<String>>,
        down: bool,
    }

    impl SlackApi for &FakeSlack {
        async fn call(&self, method: &str, params: &[(&str, &str)]) -> Result<Value, SlackError> {
            self.calls.borrow_mut().push(method.to_string());
            if self.down {
                return Err(SlackError::Transient("offline".into()));
            }
            match method {
                "users.info" => Ok(json!({"ok": true, "user": {"profile":
                    {"display_name": format!("Name of {}", params[0].1)}}})),
                "conversations.info" => {
                    Ok(json!({"ok": true, "channel": {"name": "agent-mailbox-test"}}))
                }
                other => panic!("unexpected call {other}"),
            }
        }
    }

    fn mention(event_id: &str, thread_ts: Option<&str>) -> AppMention {
        AppMention {
            event_id: Some(event_id.to_string()),
            event_time: Some(1_791_349_500),
            channel: SlackChannelId::parse(CHANNEL).unwrap(),
            ts: SlackTs::parse(REPLY).unwrap(),
            thread_ts: thread_ts.map(|ts| SlackTs::parse(ts).unwrap()),
            user: Some(SlackUserId::parse("U0AB7RJSQBE").unwrap()),
            bot_id: None,
            app_id: None,
        }
    }

    fn mentions(slack: &FakeSlack, skip: SlackFilters) -> Mentions<&FakeSlack> {
        Mentions::new(slack, WORKSPACE.to_string(), skip, Seen::default())
    }

    fn woken(disposition: Disposition) -> Mention {
        match disposition {
            Disposition::Wake(mention) => mention,
            other => panic!("expected a wake, got {other:?}"),
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_mention_wakes_with_a_named_subject_and_a_link() {
        let slack = FakeSlack::default();
        let mut watch = mentions(&slack, SlackFilters::default());
        let mention = woken(watch.dispose(mention("Ev1", None)).await);
        let subject = mention.subject().unwrap();
        assert_eq!(
            subject.text(),
            "mention from Name of U0AB7RJSQBE in #agent-mailbox-test"
        );
        assert_eq!(
            subject.link(),
            Some("https://tryrelevance.slack.com/archives/C0C83CXLUL8/p1791349500000002")
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_mention_in_a_thread_says_so_and_links_into_the_thread() {
        let slack = FakeSlack::default();
        let mut watch = mentions(&slack, SlackFilters::default());
        let mention = woken(watch.dispose(mention("Ev1", Some(PARENT))).await);
        let subject = mention.subject().unwrap();
        assert!(
            subject
                .text()
                .contains("in a thread in #agent-mailbox-test")
        );
        assert_eq!(
            mention.permalink,
            format!(
                "{WORKSPACE}archives/{CHANNEL}/p1791349500000002?thread_ts={PARENT}&cid={CHANNEL}"
            )
        );
        assert_eq!(mention.body()["thread_ts"], PARENT);
    }

    /// The point of the seen set: one mention, delivered twice, wakes once.
    #[tokio::test(flavor = "current_thread")]
    async fn a_redelivered_mention_is_dropped_once_the_first_was_published() {
        let slack = FakeSlack::default();
        let mut watch = mentions(&slack, SlackFilters::default());
        let first = woken(watch.dispose(mention("Ev1", None)).await);
        watch.published(&first);
        assert_eq!(
            watch.dispose(mention("Ev1", None)).await,
            Disposition::Duplicate
        );
        assert!(matches!(
            watch.dispose(mention("Ev2", None)).await,
            Disposition::Wake(_)
        ));
    }

    /// Recorded only once published: if the publish never happened, the
    /// redelivery is the mention's second chance and must wake.
    #[tokio::test(flavor = "current_thread")]
    async fn an_unpublished_mention_is_not_remembered() {
        let slack = FakeSlack::default();
        let mut watch = mentions(&slack, SlackFilters::default());
        let _ = watch.dispose(mention("Ev1", None)).await;
        assert!(matches!(
            watch.dispose(mention("Ev1", None)).await,
            Disposition::Wake(_)
        ));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_filter_drops_the_mention_it_matches() {
        let slack = FakeSlack::default();
        let skip = SlackFilters::new(vec![SlackFilter::parse("user=U0AB7RJSQBE").unwrap()]);
        let mut watch = mentions(&slack, skip);
        assert!(matches!(
            watch.dispose(mention("Ev1", None)).await,
            Disposition::Filtered(_)
        ));
        assert!(
            slack.calls.borrow().is_empty(),
            "no lookup for a skipped mention"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn names_are_looked_up_once_and_a_failed_lookup_uses_ids() {
        let slack = FakeSlack::default();
        let mut watch = mentions(&slack, SlackFilters::default());
        let _ = watch.dispose(mention("Ev1", None)).await;
        let _ = watch.dispose(mention("Ev2", None)).await;
        assert_eq!(
            slack.calls.borrow().len(),
            2,
            "one users.info, one conversations.info"
        );

        let offline = FakeSlack {
            down: true,
            ..FakeSlack::default()
        };
        let mut watch = mentions(&offline, SlackFilters::default());
        let mention = woken(watch.dispose(mention("Ev1", None)).await);
        assert_eq!(
            mention.subject().unwrap().text(),
            "mention from U0AB7RJSQBE in C0C83CXLUL8"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn the_body_points_at_the_mention_and_never_carries_its_text() {
        let slack = FakeSlack::default();
        let mut watch = mentions(&slack, SlackFilters::default());
        let body = woken(watch.dispose(mention("Ev1", None)).await).body();
        assert_eq!(
            body,
            json!({"kind": "slack_mention", "channel": CHANNEL, "ts": REPLY,
                "permalink": format!("{WORKSPACE}archives/{CHANNEL}/p1791349500000002"),
                "user": "U0AB7RJSQBE", "event_id": "Ev1"})
        );
    }

    #[test]
    fn the_seen_set_is_bounded_and_round_trips() {
        let mut seen = Seen::default();
        for i in 0..SEEN_CAPACITY + 10 {
            seen.insert(format!("Ev{i}"));
        }
        assert!(!seen.contains("Ev0"), "the oldest ids are forgotten");
        assert!(seen.contains(&format!("Ev{}", SEEN_CAPACITY + 9)));
        assert_eq!(Seen::from_json(seen.to_json()), Some(seen));
        assert_eq!(Seen::from_json(json!({"last_ts": "1.000001"})), None);
    }
}
