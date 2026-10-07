//! What a Slack watch reads, and which messages wake a session (design/02).
//!
//! A **channel** watch reads `conversations.history` and wakes on new top-level
//! messages, including a reply someone chose to also send to the channel
//! (`thread_broadcast`). A **thread** watch reads `conversations.replies` and
//! wakes on new replies. Both skip joins, leaves, topic changes, edits and
//! deletions: those change the channel, but nobody said anything.
//!
//! # The cursor
//!
//! The baseline is the newest message `ts` this watch has seen. Slack assigns a
//! message's `ts` when it is posted and orders a conversation by it, so "newer
//! than the cursor" is "posted since we last looked". The cursor advances past
//! skipped messages too, so a join is read once rather than on every poll.
//!
//! # Authorship is not filtered
//!
//! This assumes the watching agents share one Slack identity, as they do when
//! they all post through one person's connector. Then a session's own post is
//! indistinguishable from a peer's, and filtering on author would silence the
//! peers along with the self. So nothing is filtered, and a session is woken by
//! its own post too: the trade ADR-0014 made for `publish` (design/02).

use std::collections::HashMap;

use mailbox_protocol::{SlackTs, SlackWatch, Subject};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tracing::{debug, info, warn};

use crate::api::{SlackApi, SlackError};
use crate::message::{SlackMessage, SlackUserId, Subtype};

/// Messages per page. Slack's cap for an internal app is 1,000; 200 keeps each
/// reply small, and a second page is only needed when more than 200 messages
/// arrive between polls.
const PAGE_SIZE: usize = 200;

/// Pages read per poll before stopping. Only more than 2,000 new messages between
/// polls reach it. History pages newest first, so a channel watch then skips the
/// oldest; replies page oldest first, so a thread watch reads the rest next poll.
const MAX_PAGES: usize = 10;

/// The newest `ts` this watch has seen, persisted through the bridge.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Baseline {
    pub last_ts: SlackTs,
}

impl Baseline {
    pub fn to_json(&self) -> Value {
        serde_json::to_value(self).expect("a baseline of one timestamp always serializes")
    }

    /// `None` for a value this adapter did not write; the caller re-baselines.
    pub fn from_json(value: Value) -> Option<Self> {
        serde_json::from_value(value).ok()
    }
}

/// A message that wakes the watch's subscribers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewMessage {
    pub ts: SlackTs,
    /// A display name when Slack gave one, else the user or bot id.
    pub author: String,
    pub user: Option<SlackUserId>,
    pub bot_id: Option<String>,
    pub subtype: Option<Subtype>,
    pub permalink: String,
}

impl NewMessage {
    /// Names the message and links to it. It never quotes the text: that is
    /// content, and the woken agent reads it through its own Slack access.
    pub fn subject(&self, watch: &SlackWatch, channel_label: &str) -> Option<Subject> {
        let text = match watch {
            SlackWatch::Channel { .. } => {
                format!("new message from {} in {channel_label}", self.author)
            }
            SlackWatch::Thread { .. } => {
                format!(
                    "new reply from {} in a thread in {channel_label}",
                    self.author
                )
            }
        };
        // A subject is a courtesy, never a gate: if a display name will not parse,
        // the event still publishes and wakes its subscribers.
        Subject::new(&text, Some(&self.permalink)).ok()
    }

    /// The event body: where the message is and who sent it, never what it says.
    pub fn body(&self, watch: &SlackWatch) -> Value {
        let mut body = json!({
            "kind": match watch {
                SlackWatch::Channel { .. } => "slack_message",
                SlackWatch::Thread { .. } => "slack_reply",
            },
            "channel": watch.channel().as_str(),
            "ts": self.ts.to_string(),
            "permalink": self.permalink,
        });
        let fields = [
            ("thread_ts", watch.thread_ts().map(|ts| ts.to_string())),
            ("user", self.user.as_ref().map(|u| u.as_str().to_string())),
            ("bot_id", self.bot_id.clone()),
            (
                "subtype",
                self.subtype.as_ref().map(|s| s.as_str().to_string()),
            ),
        ];
        for (key, value) in fields {
            if let Some(value) = value {
                body[key] = Value::String(value);
            }
        }
        body
    }
}

/// Whether a message wakes the watch, and if not, why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Disposition {
    Wake,
    Skip(SkipReason),
}

/// Why a message does not wake. A log line about a skip carries one of these and
/// nothing from the message's content: `NotSpoken` holds the parsed subtype.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SkipReason {
    Hidden,
    /// A change to the channel or a message, named by its Slack subtype.
    NotSpoken(Subtype),
    /// A plain reply seen by a channel watch; it belongs to a thread watch.
    ThreadReply,
    /// The parent, which `conversations.replies` always returns first.
    ThreadParent,
}

pub(crate) fn disposition(watch: &SlackWatch, message: &SlackMessage) -> Disposition {
    if message.hidden {
        return Disposition::Skip(SkipReason::Hidden);
    }
    let broadcast = match &message.subtype {
        None | Some(Subtype::BotMessage | Subtype::FileShare | Subtype::MeMessage) => false,
        Some(Subtype::ThreadBroadcast) => true,
        Some(other @ Subtype::Other(_)) => {
            return Disposition::Skip(SkipReason::NotSpoken(other.clone()));
        }
    };
    match watch {
        SlackWatch::Channel { .. } => {
            let is_reply = message
                .thread_ts
                .as_ref()
                .is_some_and(|parent| parent != &message.ts);
            if is_reply && !broadcast {
                return Disposition::Skip(SkipReason::ThreadReply);
            }
        }
        SlackWatch::Thread { thread_ts, .. } => {
            if &message.ts == thread_ts {
                return Disposition::Skip(SkipReason::ThreadParent);
            }
        }
    }
    Disposition::Wake
}

/// How much of the conversation to read.
enum Read<'a> {
    /// The newest message only: what a baseline needs.
    Newest,
    /// Everything after the cursor, up to the page cap.
    Since(&'a SlackTs),
}

/// The messages one read returned, and whether the page cap cut it short.
struct Pages {
    messages: Vec<SlackMessage>,
    truncated: bool,
}

/// Workspace facts fetched once per process, on the first successful poll.
#[derive(Debug, Clone)]
pub struct Context {
    /// `https://<workspace>.slack.com/`, for building permalinks without a call.
    workspace_url: String,
    /// `#<name>`, or the channel id if Slack would not say.
    pub channel_label: String,
}

#[derive(Deserialize)]
struct AuthTest {
    url: String,
    #[serde(default)]
    user: Option<String>,
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
struct Page {
    messages: Vec<Value>,
    #[serde(default)]
    has_more: bool,
    #[serde(default)]
    response_metadata: Option<ResponseMetadata>,
}

#[derive(Deserialize)]
struct ResponseMetadata {
    #[serde(default)]
    next_cursor: Option<String>,
}

/// Parse a reply into `T`; a shape Slack does not normally send skips the poll.
fn parse<T: serde::de::DeserializeOwned>(method: &str, reply: Value) -> Result<T, SlackError> {
    // The serde error itself is not included: it can quote the value it choked on.
    serde_json::from_value(reply).map_err(|err| {
        SlackError::Transient(format!(
            "{method} reply had an unexpected shape ({:?})",
            err.classify()
        ))
    })
}

/// One watch's reads against Slack.
pub(crate) struct Watcher<A> {
    api: A,
    watch: SlackWatch,
    context: Option<Context>,
    /// Joining is tried once per process: a second `not_in_channel` after a
    /// successful join means something else is wrong.
    tried_join: bool,
    names: HashMap<SlackUserId, String>,
    page_size: usize,
    max_pages: usize,
}

impl<A: SlackApi> Watcher<A> {
    pub fn new(api: A, watch: SlackWatch) -> Self {
        Self {
            api,
            watch,
            context: None,
            tried_join: false,
            names: HashMap::new(),
            page_size: PAGE_SIZE,
            max_pages: MAX_PAGES,
        }
    }

    /// Small pages so a test can reach pagination and the page cap cheaply.
    #[cfg(test)]
    fn with_paging(mut self, page_size: usize, max_pages: usize) -> Self {
        self.page_size = page_size;
        self.max_pages = max_pages;
        self
    }

    pub fn watch(&self) -> &SlackWatch {
        &self.watch
    }

    /// `#<name>` once connected, else the channel id.
    pub fn channel_label(&self) -> String {
        self.context.as_ref().map_or_else(
            || self.watch.channel().to_string(),
            |c| c.channel_label.clone(),
        )
    }

    /// The workspace URL and channel name, fetched on first use. Done lazily so a
    /// laptop that starts offline gets the transient-retry path, not a crash.
    pub async fn context(&mut self) -> Result<Context, SlackError> {
        if let Some(context) = &self.context {
            return Ok(context.clone());
        }
        let auth: AuthTest = parse("auth.test", self.api.call("auth.test", &[]).await?)?;
        let mut workspace_url = auth.url;
        if !workspace_url.ends_with('/') {
            workspace_url.push('/');
        }
        let channel = self.watch.channel().to_string();
        let info: ChannelInfo = parse(
            "conversations.info",
            self.api
                .call("conversations.info", &[("channel", &channel)])
                .await?,
        )?;
        let channel_label = info.channel.name.map_or(channel, |name| format!("#{name}"));
        info!(
            workspace = %workspace_url,
            channel = %channel_label,
            bot = auth.user.as_deref().unwrap_or(""),
            "connected to Slack"
        );
        let context = Context {
            workspace_url,
            channel_label,
        };
        self.context = Some(context.clone());
        Ok(context)
    }

    /// The cursor to start from when the watch has none: the newest message now.
    /// Nothing already posted wakes anyone.
    pub async fn baseline(&mut self) -> Result<Baseline, SlackError> {
        self.context().await?;
        let pages = self.read(Read::Newest).await?;
        let newest = match &self.watch {
            SlackWatch::Channel { .. } => pages
                .messages
                .iter()
                .map(|message| message.ts.clone())
                .max()
                .unwrap_or_else(SlackTs::zero),
            SlackWatch::Thread { thread_ts, .. } => {
                let parent = pages.messages.first().ok_or_else(|| {
                    SlackError::Access(format!("thread {thread_ts} has no parent message"))
                })?;
                if &parent.ts != thread_ts {
                    return Err(SlackError::Access(format!(
                        "{thread_ts} is a reply, not the start of a thread; watch its parent"
                    )));
                }
                // The parent carries its newest reply's ts, so one call baselines
                // a thread of any length.
                parent
                    .latest_reply
                    .clone()
                    .map_or_else(|| thread_ts.clone(), |latest| latest.max(thread_ts.clone()))
            }
        };
        Ok(Baseline { last_ts: newest })
    }

    /// Everything posted after `prior`, oldest first, with the advanced cursor.
    pub async fn poll(
        &mut self,
        prior: &Baseline,
    ) -> Result<(Baseline, Vec<NewMessage>), SlackError> {
        let context = self.context().await?;
        let pages = self.read(Read::Since(&prior.last_ts)).await?;
        if pages.truncated {
            match &self.watch {
                SlackWatch::Channel { .. } => warn!(
                    max_pages = self.max_pages,
                    page_size = self.page_size,
                    "more new messages than one poll reads; the oldest were skipped"
                ),
                SlackWatch::Thread { .. } => info!(
                    max_pages = self.max_pages,
                    page_size = self.page_size,
                    "more new replies than one poll reads; the rest are read next poll"
                ),
            }
        }

        let mut newer: Vec<SlackMessage> = pages
            .messages
            .into_iter()
            .filter(|message| message.ts > prior.last_ts)
            .collect();
        newer.sort_by(|a, b| a.ts.cmp(&b.ts));

        let last_ts = newer
            .last()
            .map_or_else(|| prior.last_ts.clone(), |message| message.ts.clone());
        let mut woken = Vec::new();
        for message in &newer {
            match disposition(&self.watch, message) {
                Disposition::Wake => woken.push(self.new_message(&context, message).await),
                Disposition::Skip(reason) => {
                    debug!(ts = %message.ts, reason = ?reason, "skipped a message that does not wake");
                }
            }
        }
        Ok((Baseline { last_ts }, woken))
    }

    async fn read(&mut self, read: Read<'_>) -> Result<Pages, SlackError> {
        let (method, mut params): (&str, Vec<(&str, String)>) = match &self.watch {
            SlackWatch::Channel { channel } => (
                "conversations.history",
                vec![("channel", channel.to_string())],
            ),
            SlackWatch::Thread { channel, thread_ts } => (
                "conversations.replies",
                vec![
                    ("channel", channel.to_string()),
                    ("ts", thread_ts.to_string()),
                ],
            ),
        };
        let (limit, max_pages) = match read {
            Read::Newest => (1, 1),
            Read::Since(oldest) => {
                params.push(("oldest", oldest.to_string()));
                (self.page_size, self.max_pages)
            }
        };
        params.push(("limit", limit.to_string()));

        let mut messages = Vec::new();
        let mut cursor: Option<String> = None;
        for _ in 0..max_pages {
            let mut page_params = params.clone();
            if let Some(cursor) = &cursor {
                page_params.push(("cursor", cursor.clone()));
            }
            let page: Page = parse(method, self.call_as_member(method, &page_params).await?)?;
            for raw in page.messages {
                match serde_json::from_value::<SlackMessage>(raw) {
                    Ok(message) => messages.push(message),
                    // The serde error is not logged: it can quote the value it
                    // choked on, and that value came from someone's message.
                    Err(err) => warn!(
                        category = ?err.classify(),
                        "dropped a message Slack sent in an unexpected shape"
                    ),
                }
            }
            cursor = page
                .response_metadata
                .and_then(|meta| meta.next_cursor)
                .filter(|next| !next.is_empty());
            if !page.has_more || cursor.is_none() {
                return Ok(Pages {
                    messages,
                    truncated: false,
                });
            }
        }
        Ok(Pages {
            messages,
            truncated: true,
        })
    }

    /// A call that joins the channel and retries once if the bot is not in it.
    /// Only public channels can be joined; a private one needs an invite.
    async fn call_as_member(
        &mut self,
        method: &str,
        params: &[(&str, String)],
    ) -> Result<Value, SlackError> {
        let borrowed: Vec<(&str, &str)> = params.iter().map(|(k, v)| (*k, v.as_str())).collect();
        match self.api.call(method, &borrowed).await {
            Err(SlackError::NotInChannel) if !self.tried_join => {
                let channel = self.watch.channel().to_string();
                match self
                    .api
                    .call("conversations.join", &[("channel", &channel)])
                    .await
                {
                    Ok(_) => {}
                    // The join itself was refused: that is the membership problem.
                    Err(
                        err @ (SlackError::Access(_)
                        | SlackError::Failed(_)
                        | SlackError::NotInChannel),
                    ) => {
                        self.tried_join = true;
                        return Err(not_a_member(&channel, &err.to_string()));
                    }
                    // Anything else keeps its own class, so a rate limit backs off,
                    // a blip skips the poll and the join is tried again, and a bad
                    // token is reported as a bad token.
                    Err(
                        err @ (SlackError::RateLimited { .. }
                        | SlackError::Transient(_)
                        | SlackError::Auth(_)
                        | SlackError::Spawn { .. }),
                    ) => return Err(err),
                }
                self.tried_join = true;
                info!(channel = %channel, "joined the channel to read it");
                self.api
                    .call(method, &borrowed)
                    .await
                    .map_err(|err| match err {
                        SlackError::NotInChannel => {
                            not_a_member(&channel, "still not a member after joining")
                        }
                        other => other,
                    })
            }
            Err(SlackError::NotInChannel) => {
                Err(not_a_member(self.watch.channel().as_str(), "not a member"))
            }
            other => other,
        }
    }

    async fn new_message(&mut self, context: &Context, message: &SlackMessage) -> NewMessage {
        let posted_as = message
            .bot_profile
            .as_ref()
            .and_then(|profile| profile.name.clone())
            .or_else(|| message.username.clone());
        let author = match (posted_as, &message.user) {
            (Some(name), _) => name,
            (None, Some(user)) => self.user_name(user).await,
            (None, None) => message
                .bot_id
                .clone()
                .unwrap_or_else(|| "someone".to_string()),
        };
        NewMessage {
            ts: message.ts.clone(),
            author,
            user: message.user.clone(),
            bot_id: message.bot_id.clone(),
            subtype: message.subtype.clone(),
            permalink: self.permalink(context, &message.ts),
        }
    }

    /// A user's display name, cached for the life of the process. Any failure
    /// falls back to the id: a name only decorates the subject.
    async fn user_name(&mut self, user: &SlackUserId) -> String {
        if let Some(name) = self.names.get(user) {
            return name.clone();
        }
        let reply = self
            .api
            .call("users.info", &[("user", user.as_str())])
            .await;
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
        self.names.insert(user.clone(), name.clone());
        name
    }

    fn permalink(&self, context: &Context, ts: &SlackTs) -> String {
        let channel = self.watch.channel();
        let base = format!(
            "{}archives/{channel}/p{}",
            context.workspace_url,
            ts.permalink_digits()
        );
        match self.watch.thread_ts() {
            Some(parent) => format!("{base}?thread_ts={parent}&cid={channel}"),
            None => base,
        }
    }
}

fn not_a_member(channel: &str, detail: &str) -> SlackError {
    SlackError::Access(format!(
        "the bot cannot read {channel} ({detail}); invite it to the channel with /invite"
    ))
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use super::*;

    const CHANNEL: &str = "C0C83CXLUL8";
    const PARENT: &str = "1791349480.652779";

    fn channel_watch() -> SlackWatch {
        SlackWatch::parse_channel_key(CHANNEL).unwrap()
    }

    fn thread_watch() -> SlackWatch {
        SlackWatch::parse_thread_key(&format!("{CHANNEL}/{PARENT}")).unwrap()
    }

    fn ts(raw: &str) -> SlackTs {
        SlackTs::parse(raw).unwrap()
    }

    fn message(value: Value) -> SlackMessage {
        serde_json::from_value(value).unwrap()
    }

    /// An in-memory Slack: one channel's top level, one thread, and a member
    /// flag, served in Slack's shapes. History is newest first, replies oldest
    /// first with the parent leading the first page, and both page by `limit`
    /// and an opaque `cursor`.
    struct FakeSlack {
        top_level: RefCell<Vec<Value>>,
        replies: RefCell<Vec<Value>>,
        member: RefCell<bool>,
        joinable: bool,
        /// The next join fails transiently, once.
        join_blip: RefCell<bool>,
        calls: RefCell<Vec<String>>,
    }

    impl FakeSlack {
        fn new() -> Self {
            Self::with_top_level(vec![json!({"ts": PARENT, "user": "U1", "text": "hi",
                "thread_ts": PARENT, "latest_reply": "1791349481.000001"})])
        }

        fn with_top_level(top_level: Vec<Value>) -> Self {
            Self {
                top_level: RefCell::new(top_level),
                replies: RefCell::new(vec![json!({"ts": "1791349481.000001", "user": "U2",
                    "thread_ts": PARENT})]),
                member: RefCell::new(true),
                joinable: true,
                join_blip: RefCell::new(false),
                calls: RefCell::new(Vec::new()),
            }
        }

        fn post(&self, message: Value) {
            self.top_level.borrow_mut().push(message);
        }

        fn reply(&self, message: Value) {
            self.replies.borrow_mut().push(message);
        }

        fn calls_to(&self, method: &str) -> usize {
            self.calls.borrow().iter().filter(|m| *m == method).count()
        }
    }

    fn param<'a>(params: &[(&str, &'a str)], key: &str) -> Option<&'a str> {
        params.iter().find(|(k, _)| *k == key).map(|(_, v)| *v)
    }

    fn newer(messages: &[Value], oldest: Option<&str>) -> Vec<Value> {
        let oldest = oldest.map(ts);
        messages
            .iter()
            .filter(|m| {
                oldest
                    .as_ref()
                    .is_none_or(|o| &ts(m["ts"].as_str().unwrap()) > o)
            })
            .cloned()
            .collect()
    }

    /// One page of `all`, starting where `cursor` says, in Slack's envelope.
    fn page(all: Vec<Value>, params: &[(&str, &str)]) -> Value {
        let limit: usize = param(params, "limit").map_or(100, |l| l.parse().unwrap());
        let start: usize = param(params, "cursor").map_or(0, |c| c.parse().unwrap());
        let end = (start + limit).min(all.len());
        let has_more = end < all.len();
        json!({"ok": true, "messages": all[start..end], "has_more": has_more,
            "response_metadata": {"next_cursor": if has_more { end.to_string() } else { String::new() }}})
    }

    impl SlackApi for &FakeSlack {
        async fn call(&self, method: &str, params: &[(&str, &str)]) -> Result<Value, SlackError> {
            self.calls.borrow_mut().push(method.to_string());
            match method {
                "auth.test" => Ok(
                    json!({"ok": true, "url": "https://tryrelevance.slack.com/", "user": "watch"}),
                ),
                "conversations.info" => {
                    Ok(json!({"ok": true, "channel": {"name": "team-arg-agent-watercooler"}}))
                }
                "users.info" => Ok(json!({"ok": true, "user": {"name": "ben", "profile":
                    {"display_name": format!("Name of {}", param(params, "user").unwrap())}}})),
                "conversations.join" if self.join_blip.replace(false) => {
                    Err(SlackError::Transient("blip".into()))
                }
                "conversations.join" if self.joinable => {
                    *self.member.borrow_mut() = true;
                    Ok(json!({"ok": true}))
                }
                "conversations.join" => Err(SlackError::Access(
                    "method_not_supported_for_channel_type".into(),
                )),
                _ if !*self.member.borrow() => Err(SlackError::NotInChannel),
                "conversations.history" => {
                    let mut messages = newer(&self.top_level.borrow(), param(params, "oldest"));
                    messages.reverse();
                    Ok(page(messages, params))
                }
                "conversations.replies" => {
                    let mut messages = vec![self.top_level.borrow()[0].clone()];
                    messages.extend(newer(&self.replies.borrow(), param(params, "oldest")));
                    Ok(page(messages, params))
                }
                other => panic!("unexpected call {other}"),
            }
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn channel_baseline_is_the_newest_message_and_wakes_nobody() {
        let slack = FakeSlack::new();
        let mut watcher = Watcher::new(&slack, channel_watch());
        let baseline = watcher.baseline().await.unwrap();
        assert_eq!(baseline.last_ts, ts(PARENT));
        let (next, woken) = watcher.poll(&baseline).await.unwrap();
        assert!(woken.is_empty(), "nothing posted since the baseline");
        assert_eq!(next, baseline);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn an_empty_channel_baselines_before_everything_so_its_first_message_wakes() {
        let slack = FakeSlack::with_top_level(Vec::new());
        let mut watcher = Watcher::new(&slack, channel_watch());
        let baseline = watcher.baseline().await.unwrap();
        assert_eq!(baseline.last_ts, SlackTs::zero());
        slack.post(json!({"ts": "1791349500.000001", "user": "U1"}));
        let (_, woken) = watcher.poll(&baseline).await.unwrap();
        assert_eq!(woken.len(), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_new_top_level_message_wakes_once_and_noise_does_not() {
        let slack = FakeSlack::new();
        let mut watcher = Watcher::new(&slack, channel_watch());
        let baseline = watcher.baseline().await.unwrap();

        slack.post(json!({"ts": "1791349500.000001", "subtype": "channel_join", "user": "U3"}));
        slack.post(json!({"ts": "1791349500.000002", "user": "U3", "text": "status update"}));
        slack.post(json!({"ts": "1791349500.000003", "subtype": "message_changed"}));
        slack.post(
            json!({"ts": "1791349500.000004", "subtype": "bot_message", "bot_id": "B1",
            "bot_profile": {"name": "ci-bot"}}),
        );

        let (next, woken) = watcher.poll(&baseline).await.unwrap();
        let authors: Vec<_> = woken.iter().map(|m| m.author.as_str()).collect();
        assert_eq!(
            authors,
            ["Name of U3", "ci-bot"],
            "oldest first, noise skipped"
        );
        assert_eq!(
            next.last_ts,
            ts("1791349500.000004"),
            "cursor passes skipped messages"
        );

        let (_, again) = watcher.poll(&next).await.unwrap();
        assert!(again.is_empty(), "a message wakes exactly once");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_message_in_an_unexpected_shape_is_dropped_not_misread() {
        let slack = FakeSlack::new();
        let mut watcher = Watcher::new(&slack, channel_watch());
        let baseline = watcher.baseline().await.unwrap();
        slack.post(json!({"ts": "1791349500.000001", "user": "U1", "thread_ts": "garbage"}));
        slack.post(json!({"ts": "1791349500.000002", "user": "U1"}));
        let (_, woken) = watcher.poll(&baseline).await.unwrap();
        assert_eq!(woken.len(), 1);
        assert_eq!(woken[0].ts, ts("1791349500.000002"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_poll_follows_every_page_in_order() {
        let slack = FakeSlack::new();
        let mut watcher = Watcher::new(&slack, channel_watch()).with_paging(2, 10);
        let baseline = watcher.baseline().await.unwrap();
        for i in 1..=5 {
            slack.post(json!({"ts": format!("1791349500.00000{i}"), "user": "U1"}));
        }
        let (next, woken) = watcher.poll(&baseline).await.unwrap();
        let order: Vec<_> = woken.iter().map(|m| m.ts.to_string()).collect();
        assert_eq!(
            order,
            (1..=5)
                .map(|i| format!("1791349500.00000{i}"))
                .collect::<Vec<_>>()
        );
        assert_eq!(next.last_ts, ts("1791349500.000005"));
        assert_eq!(
            slack.calls_to("conversations.history"),
            1 + 3,
            "baseline + 3 pages"
        );
    }

    /// The documented loss: past the page cap, history (newest first) has given
    /// us the newest messages, and the cursor moves past the ones it never read.
    #[tokio::test(flavor = "current_thread")]
    async fn past_the_page_cap_the_oldest_new_messages_are_skipped() {
        let slack = FakeSlack::new();
        let mut watcher = Watcher::new(&slack, channel_watch()).with_paging(2, 2);
        let baseline = watcher.baseline().await.unwrap();
        for i in 1..=5 {
            slack.post(json!({"ts": format!("1791349500.00000{i}"), "user": "U1"}));
        }
        let (next, woken) = watcher.poll(&baseline).await.unwrap();
        let order: Vec<_> = woken.iter().map(|m| m.ts.to_string()).collect();
        assert_eq!(
            order,
            (2..=5)
                .map(|i| format!("1791349500.00000{i}"))
                .collect::<Vec<_>>()
        );
        assert_eq!(next.last_ts, ts("1791349500.000005"));
    }

    /// Replies page oldest first, so the page cap loses nothing on a thread: the
    /// cursor stops at the newest reply read and the next poll carries on.
    #[tokio::test(flavor = "current_thread")]
    async fn a_thread_past_the_page_cap_reads_the_rest_next_poll() {
        let slack = FakeSlack::new();
        let mut watcher = Watcher::new(&slack, thread_watch()).with_paging(2, 2);
        let baseline = watcher.baseline().await.unwrap();
        for i in 1..=5 {
            slack.reply(json!({"ts": format!("1791349600.00000{i}"), "user": "U2",
                "thread_ts": PARENT}));
        }
        let (next, first) = watcher.poll(&baseline).await.unwrap();
        let (_, second) = watcher.poll(&next).await.unwrap();
        let all: Vec<_> = first
            .iter()
            .chain(&second)
            .map(|m| m.ts.to_string())
            .collect();
        assert_eq!(
            all,
            (1..=5)
                .map(|i| format!("1791349600.00000{i}"))
                .collect::<Vec<_>>()
        );
        assert!(!first.is_empty() && !second.is_empty(), "it took two polls");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn replies_wake_the_thread_watch_not_the_channel_watch() {
        let slack = FakeSlack::new();
        let mut channel = Watcher::new(&slack, channel_watch());
        let mut thread = Watcher::new(&slack, thread_watch());
        let channel_base = channel.baseline().await.unwrap();
        let thread_base = thread.baseline().await.unwrap();
        assert_eq!(
            thread_base.last_ts,
            ts("1791349481.000001"),
            "a thread baselines on its newest existing reply"
        );

        slack.reply(json!({"ts": "1791349600.000001", "user": "U2", "thread_ts": PARENT}));
        let (_, from_channel) = channel.poll(&channel_base).await.unwrap();
        let (_, from_thread) = thread.poll(&thread_base).await.unwrap();
        assert!(from_channel.is_empty());
        assert_eq!(from_thread.len(), 1);
        assert_eq!(
            from_thread[0].permalink,
            format!(
                "https://tryrelevance.slack.com/archives/{CHANNEL}/p1791349600000001?thread_ts={PARENT}&cid={CHANNEL}"
            )
        );
        assert_eq!(
            from_thread[0]
                .subject(thread.watch(), "#team-arg-agent-watercooler")
                .unwrap()
                .text(),
            "new reply from Name of U2 in a thread in #team-arg-agent-watercooler"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_thread_with_no_replies_baselines_on_its_parent() {
        let slack = FakeSlack::with_top_level(vec![json!({"ts": PARENT, "user": "U1"})]);
        slack.replies.borrow_mut().clear();
        let mut watcher = Watcher::new(&slack, thread_watch());
        assert_eq!(watcher.baseline().await.unwrap().last_ts, ts(PARENT));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_reply_ts_is_not_a_thread() {
        let slack = FakeSlack::new();
        let reply = SlackWatch::parse_thread_key(&format!("{CHANNEL}/1791349481.000001")).unwrap();
        let mut watcher = Watcher::new(&slack, reply);
        assert!(matches!(
            watcher.baseline().await,
            Err(SlackError::Access(_))
        ));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_bot_outside_the_channel_joins_once_then_reads() {
        let slack = FakeSlack::new();
        *slack.member.borrow_mut() = false;
        let mut watcher = Watcher::new(&slack, channel_watch());
        watcher.baseline().await.unwrap();
        assert_eq!(slack.calls_to("conversations.join"), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_join_that_blips_keeps_its_class_and_is_tried_again() {
        let slack = FakeSlack::new();
        *slack.member.borrow_mut() = false;
        *slack.join_blip.borrow_mut() = true;
        let mut watcher = Watcher::new(&slack, channel_watch());
        assert!(matches!(
            watcher.baseline().await,
            Err(SlackError::Transient(_))
        ));
        watcher.baseline().await.unwrap();
        assert_eq!(slack.calls_to("conversations.join"), 2);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_bot_removed_mid_watch_and_refused_rejoin_is_fatal() {
        let mut slack = FakeSlack::new();
        slack.joinable = false;
        let mut watcher = Watcher::new(&slack, channel_watch());
        let baseline = watcher.baseline().await.unwrap();
        *slack.member.borrow_mut() = false;
        assert!(matches!(
            watcher.poll(&baseline).await,
            Err(SlackError::Access(_))
        ));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_channel_it_cannot_join_is_fatal_and_says_to_invite() {
        let mut slack = FakeSlack::new();
        slack.joinable = false;
        *slack.member.borrow_mut() = false;
        let mut watcher = Watcher::new(&slack, channel_watch());
        match watcher.baseline().await {
            Err(SlackError::Access(detail)) => assert!(detail.contains("/invite"), "{detail}"),
            other => panic!("expected an access error, got {other:?}"),
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn user_names_are_looked_up_once() {
        let slack = FakeSlack::new();
        let mut watcher = Watcher::new(&slack, channel_watch());
        let base = watcher.baseline().await.unwrap();
        slack.post(json!({"ts": "1791349800.000001", "user": "U9"}));
        slack.post(json!({"ts": "1791349800.000002", "user": "U9"}));
        watcher.poll(&base).await.unwrap();
        assert_eq!(slack.calls_to("users.info"), 1);
    }

    /// The whole wake table from design/02, one row per case.
    #[test]
    fn the_wake_table() {
        use Disposition::{Skip, Wake};
        let reply = json!({"ts": "1791349600.000001", "thread_ts": PARENT});
        let cases = [
            (
                "plain message",
                channel_watch(),
                json!({"ts": "1.000001"}),
                Wake,
            ),
            (
                "bot_message",
                channel_watch(),
                json!({"ts": "1.000001", "subtype": "bot_message"}),
                Wake,
            ),
            (
                "file_share",
                channel_watch(),
                json!({"ts": "1.000001", "subtype": "file_share"}),
                Wake,
            ),
            (
                "me_message",
                channel_watch(),
                json!({"ts": "1.000001", "subtype": "me_message"}),
                Wake,
            ),
            (
                "a thread's parent, on the channel",
                channel_watch(),
                json!({"ts": PARENT, "thread_ts": PARENT}),
                Wake,
            ),
            (
                "plain reply, on the channel",
                channel_watch(),
                reply.clone(),
                Skip(SkipReason::ThreadReply),
            ),
            (
                "broadcast reply, on the channel",
                channel_watch(),
                json!({"ts": "1791349600.000001", "thread_ts": PARENT, "subtype": "thread_broadcast"}),
                Wake,
            ),
            ("plain reply, on the thread", thread_watch(), reply, Wake),
            (
                "the parent, on the thread",
                thread_watch(),
                json!({"ts": PARENT, "thread_ts": PARENT}),
                Skip(SkipReason::ThreadParent),
            ),
            (
                "channel_join",
                channel_watch(),
                json!({"ts": "1.000001", "subtype": "channel_join"}),
                Skip(SkipReason::NotSpoken(Subtype::from(
                    "channel_join".to_string(),
                ))),
            ),
            (
                "message_changed",
                channel_watch(),
                json!({"ts": "1.000001", "subtype": "message_changed"}),
                Skip(SkipReason::NotSpoken(Subtype::from(
                    "message_changed".to_string(),
                ))),
            ),
            (
                "message_deleted",
                thread_watch(),
                json!({"ts": "1.000001", "subtype": "message_deleted"}),
                Skip(SkipReason::NotSpoken(Subtype::from(
                    "message_deleted".to_string(),
                ))),
            ),
            (
                "hidden",
                channel_watch(),
                json!({"ts": "1.000001", "hidden": true}),
                Skip(SkipReason::Hidden),
            ),
        ];
        for (name, watch, raw, expected) in cases {
            assert_eq!(disposition(&watch, &message(raw)), expected, "{name}");
        }
    }

    #[test]
    fn the_body_points_at_the_message_and_never_carries_its_text() {
        let woken = NewMessage {
            ts: ts("1791349900.000001"),
            author: "Ben".to_string(),
            user: message(json!({"ts": "1.000001", "user": "U1"})).user,
            bot_id: None,
            subtype: None,
            permalink: "https://x.slack.com/archives/C1/p1791349900000001".to_string(),
        };
        let body = woken.body(&channel_watch());
        assert_eq!(body["kind"], "slack_message");
        assert_eq!(body["ts"], "1791349900.000001");
        assert_eq!(body["user"], "U1");
        assert!(body.get("text").is_none());
        assert!(
            body.get("bot_id").is_none(),
            "absent fields are omitted, not null"
        );
    }

    #[test]
    fn baseline_round_trips_and_rejects_foreign_values() {
        let baseline = Baseline {
            last_ts: ts(PARENT),
        };
        assert_eq!(
            baseline.to_json(),
            json!({"last_ts": PARENT}),
            "the persisted shape"
        );
        assert_eq!(Baseline::from_json(baseline.to_json()), Some(baseline));
        assert_eq!(Baseline::from_json(json!({"mergeable": "clean"})), None);
        assert_eq!(Baseline::from_json(json!({"last_ts": "yesterday"})), None);
    }
}
