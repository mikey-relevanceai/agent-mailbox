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
//! than the cursor" is exactly "posted since we last looked". The cursor advances
//! past skipped messages too, so a join is read once rather than on every poll.
//!
//! # Authorship is not filtered
//!
//! Every agent Mikey runs posts as the same Slack user, so a session's own post
//! is indistinguishable from a peer's. Filtering on author would silence the
//! peers along with the self, so nothing is filtered and a session is woken by
//! its own post too — the same trade ADR-0014 made for `publish`.

use std::collections::HashMap;

use mailbox_protocol::{SlackTs, SlackWatch, Subject};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tracing::{debug, info, warn};

use crate::api::{SlackApi, SlackError};

/// Messages per page. Slack's cap for an internal app is 1,000; 200 keeps each
/// reply small while making a second page rare at one poll a minute.
const PAGE_SIZE: &str = "200";

/// Pages read per poll before giving up on the rest. Only a channel taking more
/// than 2,000 messages between polls reaches it, and then the oldest are skipped
/// with a warning rather than stalling the watch.
const MAX_PAGES: usize = 10;

/// The newest `ts` this watch has seen, persisted through the bridge.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Baseline {
    pub last_ts: SlackTs,
}

/// The persisted form: the `ts` as Slack writes it.
#[derive(Serialize, Deserialize)]
struct StoredBaseline {
    last_ts: String,
}

impl Baseline {
    pub fn to_json(&self) -> Value {
        serde_json::to_value(StoredBaseline {
            last_ts: self.last_ts.to_string(),
        })
        .expect("a baseline of one string always serializes")
    }

    /// `None` for a value this adapter did not write; the caller re-baselines.
    pub fn from_json(value: Value) -> Option<Self> {
        let stored: StoredBaseline = serde_json::from_value(value).ok()?;
        Some(Self {
            last_ts: SlackTs::parse(&stored.last_ts).ok()?,
        })
    }
}

/// A message that wakes the watch's subscribers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewMessage {
    pub ts: SlackTs,
    /// A display name when Slack gave one, else the user or bot id.
    pub author: String,
    pub user: Option<String>,
    pub bot_id: Option<String>,
    pub subtype: Option<String>,
    pub permalink: String,
}

impl NewMessage {
    /// Names the message and links to it. It never quotes the text: that is
    /// content, and the woken agent reads it through its own Slack access.
    pub fn subject(&self, watch: &SlackWatch, channel_label: &str) -> Option<Subject> {
        let text = match watch {
            SlackWatch::Channel(_) => {
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
                SlackWatch::Channel(_) => "slack_message",
                SlackWatch::Thread { .. } => "slack_reply",
            },
            "channel": watch.channel().as_str(),
            "ts": self.ts.as_str(),
            "permalink": self.permalink,
        });
        let fields = [
            ("thread_ts", watch.thread_ts().map(|ts| ts.to_string())),
            ("user", self.user.clone()),
            ("bot_id", self.bot_id.clone()),
            ("subtype", self.subtype.clone()),
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
    Skip(String),
}

/// Subtypes that are someone saying something. Every other subtype is a change
/// to the channel (`channel_join`, `channel_topic`, …) or to a message
/// (`message_changed`, `message_deleted`).
const SPOKEN_SUBTYPES: [&str; 4] = [
    "bot_message",
    "file_share",
    "me_message",
    "thread_broadcast",
];

pub(crate) fn disposition(watch: &SlackWatch, message: &Value, ts: &SlackTs) -> Disposition {
    if message.get("hidden").and_then(Value::as_bool) == Some(true) {
        return Disposition::Skip("hidden".to_string());
    }
    let subtype = message.get("subtype").and_then(Value::as_str);
    if let Some(subtype) = subtype
        && !SPOKEN_SUBTYPES.contains(&subtype)
    {
        return Disposition::Skip(subtype.to_string());
    }
    let thread_ts = message
        .get("thread_ts")
        .and_then(Value::as_str)
        .and_then(|raw| SlackTs::parse(raw).ok());
    match watch {
        // History holds parents and broadcasts; a plain reply here would be a
        // Slack quirk, and it belongs to a thread watch, not this one.
        SlackWatch::Channel(_) => {
            let is_reply = thread_ts.is_some_and(|parent| &parent != ts);
            if is_reply && subtype != Some("thread_broadcast") {
                return Disposition::Skip("thread reply".to_string());
            }
        }
        // `conversations.replies` always returns the parent first.
        SlackWatch::Thread { thread_ts, .. } => {
            if ts == thread_ts {
                return Disposition::Skip("thread parent".to_string());
            }
        }
    }
    Disposition::Wake
}

/// Workspace facts fetched once per process, on the first successful poll.
#[derive(Debug, Clone)]
pub struct Context {
    /// `https://<workspace>.slack.com/`, for building permalinks without a call.
    workspace_url: String,
    /// `#<name>`, or the channel id if Slack would not say.
    pub channel_label: String,
}

/// One watch's reads against Slack.
pub(crate) struct Watcher<A> {
    api: A,
    watch: SlackWatch,
    context: Option<Context>,
    /// Joining is tried once per process: a second `not_in_channel` after a
    /// successful join means something else is wrong.
    tried_join: bool,
    names: HashMap<String, String>,
}

impl<A: SlackApi> Watcher<A> {
    pub fn new(api: A, watch: SlackWatch) -> Self {
        Self {
            api,
            watch,
            context: None,
            tried_join: false,
            names: HashMap::new(),
        }
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
        let auth = self.api.call("auth.test", &[]).await?;
        let mut workspace_url = auth
            .get("url")
            .and_then(Value::as_str)
            .ok_or_else(|| SlackError::Transient("auth.test returned no url".to_string()))?
            .to_string();
        if !workspace_url.ends_with('/') {
            workspace_url.push('/');
        }
        let channel = self.watch.channel().as_str().to_string();
        let info = self
            .api
            .call("conversations.info", &[("channel", &channel)])
            .await?;
        let channel_label = info
            .pointer("/channel/name")
            .and_then(Value::as_str)
            .map(|name| format!("#{name}"))
            .unwrap_or(channel);
        // Bound outside the macro: inside it, `Value` names tracing's trait.
        let bot = auth.get("user").and_then(Value::as_str).unwrap_or("");
        info!(
            workspace = %workspace_url,
            channel = %channel_label,
            bot,
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
        let (messages, _) = self.read_pages(None, "1", 1).await?;
        let newest = match &self.watch {
            SlackWatch::Channel(_) => messages
                .iter()
                .filter_map(message_ts)
                .max()
                .unwrap_or_else(SlackTs::zero),
            SlackWatch::Thread { thread_ts, .. } => {
                let parent = messages.first().ok_or_else(|| {
                    SlackError::Access(format!("thread {thread_ts} has no parent message"))
                })?;
                if message_ts(parent).as_ref() != Some(thread_ts) {
                    return Err(SlackError::Access(format!(
                        "{thread_ts} is a reply, not the start of a thread; watch its parent"
                    )));
                }
                // The parent carries its newest reply's ts, so one call baselines
                // a thread of any length.
                parent
                    .get("latest_reply")
                    .and_then(Value::as_str)
                    .and_then(|raw| SlackTs::parse(raw).ok())
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
        let (messages, truncated) = self
            .read_pages(Some(&prior.last_ts), PAGE_SIZE, MAX_PAGES)
            .await?;
        if truncated {
            warn!(
                pages = MAX_PAGES,
                "more new messages than one poll reads; the oldest were skipped"
            );
        }

        let mut newer: Vec<(SlackTs, Value)> = messages
            .into_iter()
            .filter_map(|message| message_ts(&message).map(|ts| (ts, message)))
            .filter(|(ts, _)| ts > &prior.last_ts)
            .collect();
        newer.sort_by(|(a, _), (b, _)| a.cmp(b));

        let last_ts = newer
            .last()
            .map_or_else(|| prior.last_ts.clone(), |(ts, _)| ts.clone());
        let mut woken = Vec::new();
        for (ts, message) in &newer {
            match disposition(&self.watch, message, ts) {
                Disposition::Wake => woken.push(self.new_message(&context, ts, message).await),
                Disposition::Skip(reason) => {
                    debug!(ts = %ts, reason = %reason, "skipped a message that does not wake");
                }
            }
        }
        Ok((Baseline { last_ts }, woken))
    }

    /// Read up to `max_pages` pages newer than `oldest`. Returns whether more
    /// remained unread.
    async fn read_pages(
        &mut self,
        oldest: Option<&SlackTs>,
        limit: &str,
        max_pages: usize,
    ) -> Result<(Vec<Value>, bool), SlackError> {
        let (method, mut params): (&str, Vec<(&str, String)>) = match &self.watch {
            SlackWatch::Channel(channel) => (
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
        params.push(("limit", limit.to_string()));
        if let Some(oldest) = oldest {
            params.push(("oldest", oldest.to_string()));
        }

        let mut messages = Vec::new();
        let mut cursor: Option<String> = None;
        for _ in 0..max_pages {
            let mut page_params = params.clone();
            if let Some(cursor) = &cursor {
                page_params.push(("cursor", cursor.clone()));
            }
            let reply = self.read(method, &page_params).await?;
            let page = reply
                .get("messages")
                .and_then(Value::as_array)
                .ok_or_else(|| SlackError::Transient(format!("{method} returned no messages")))?;
            messages.extend(page.iter().cloned());
            cursor = reply
                .pointer("/response_metadata/next_cursor")
                .and_then(Value::as_str)
                .filter(|next| !next.is_empty())
                .map(str::to_string);
            let has_more = reply.get("has_more").and_then(Value::as_bool) == Some(true);
            if !has_more || cursor.is_none() {
                return Ok((messages, false));
            }
        }
        Ok((messages, true))
    }

    /// A read that joins the channel and retries once if the bot is not in it.
    /// Only public channels can be joined; a private one needs an invite.
    async fn read(&mut self, method: &str, params: &[(&str, String)]) -> Result<Value, SlackError> {
        let borrowed: Vec<(&str, &str)> = params.iter().map(|(k, v)| (*k, v.as_str())).collect();
        match self.api.call(method, &borrowed).await {
            Err(SlackError::NotInChannel) if !self.tried_join => {
                self.tried_join = true;
                let channel = self.watch.channel().to_string();
                self.api
                    .call("conversations.join", &[("channel", &channel)])
                    .await
                    .map_err(|err| not_a_member(&channel, &err.to_string()))?;
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

    async fn new_message(
        &mut self,
        context: &Context,
        ts: &SlackTs,
        message: &Value,
    ) -> NewMessage {
        let field = |key: &str| message.get(key).and_then(Value::as_str).map(str::to_string);
        let user = field("user");
        let bot_id = field("bot_id");
        let author = match message
            .pointer("/bot_profile/name")
            .and_then(Value::as_str)
            .or_else(|| message.get("username").and_then(Value::as_str))
        {
            Some(name) => name.to_string(),
            None => match &user {
                Some(user) => self.user_name(user).await,
                None => bot_id.clone().unwrap_or_else(|| "someone".to_string()),
            },
        };
        NewMessage {
            ts: ts.clone(),
            author,
            user,
            bot_id,
            subtype: field("subtype"),
            permalink: self.permalink(context, ts),
        }
    }

    /// A user's display name, cached for the life of the process. Any failure
    /// falls back to the id: a name only decorates the subject.
    async fn user_name(&mut self, user: &str) -> String {
        if let Some(name) = self.names.get(user) {
            return name.clone();
        }
        let name = match self.api.call("users.info", &[("user", user)]).await {
            Ok(reply) => [
                "/user/profile/display_name",
                "/user/real_name",
                "/user/name",
            ]
            .iter()
            .find_map(|path| {
                reply
                    .pointer(path)
                    .and_then(Value::as_str)
                    .filter(|name| !name.is_empty())
            })
            .unwrap_or(user)
            .to_string(),
            Err(err) => {
                debug!(user, error = %err, "could not look up a user's name; using the id");
                return user.to_string();
            }
        };
        self.names.insert(user.to_string(), name.clone());
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

fn message_ts(message: &Value) -> Option<SlackTs> {
    message
        .get("ts")
        .and_then(Value::as_str)
        .and_then(|raw| SlackTs::parse(raw).ok())
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

    /// An in-memory Slack: one channel's top level, one thread, and a member
    /// flag. History and replies are served with Slack's real shapes: history
    /// newest first, replies oldest first with the parent always included.
    struct FakeSlack {
        top_level: RefCell<Vec<Value>>,
        replies: RefCell<Vec<Value>>,
        member: RefCell<bool>,
        joinable: bool,
        calls: RefCell<Vec<String>>,
    }

    impl FakeSlack {
        fn new() -> Self {
            Self {
                top_level: RefCell::new(vec![json!({"ts": PARENT, "user": "U1", "text": "hi",
                    "thread_ts": PARENT, "latest_reply": "1791349481.000001"})]),
                replies: RefCell::new(vec![json!({"ts": "1791349481.000001", "user": "U2",
                    "thread_ts": PARENT})]),
                member: RefCell::new(true),
                joinable: true,
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
            .filter(|m| oldest.as_ref().is_none_or(|o| &message_ts(m).unwrap() > o))
            .cloned()
            .collect()
    }

    impl SlackApi for &FakeSlack {
        async fn call(&self, method: &str, params: &[(&str, &str)]) -> Result<Value, SlackError> {
            self.calls.borrow_mut().push(method.to_string());
            let limit: usize = param(params, "limit").map_or(100, |l| l.parse().unwrap());
            match method {
                "auth.test" => Ok(
                    json!({"ok": true, "url": "https://tryrelevance.slack.com/", "user": "watch"}),
                ),
                "conversations.info" => {
                    Ok(json!({"ok": true, "channel": {"name": "team-arg-agent-watercooler"}}))
                }
                "users.info" => Ok(json!({"ok": true, "user": {"name": "ben", "profile":
                    {"display_name": format!("Name of {}", param(params, "user").unwrap())}}})),
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
                    messages.truncate(limit);
                    Ok(json!({"ok": true, "messages": messages, "has_more": false}))
                }
                "conversations.replies" => {
                    let parent = self.top_level.borrow()[0].clone();
                    let mut messages = vec![parent];
                    messages.extend(newer(&self.replies.borrow(), param(params, "oldest")));
                    messages.truncate(limit);
                    Ok(json!({"ok": true, "messages": messages, "has_more": false}))
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
    async fn a_reply_sent_to_the_channel_wakes_the_channel_watch() {
        let slack = FakeSlack::new();
        let mut channel = Watcher::new(&slack, channel_watch());
        let base = channel.baseline().await.unwrap();
        slack.post(
            json!({"ts": "1791349700.000001", "user": "U2", "thread_ts": PARENT,
            "subtype": "thread_broadcast"}),
        );
        let (_, woken) = channel.poll(&base).await.unwrap();
        assert_eq!(woken.len(), 1);
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

    #[test]
    fn the_body_points_at_the_message_and_never_carries_its_text() {
        let message = NewMessage {
            ts: ts("1791349900.000001"),
            author: "Ben".to_string(),
            user: Some("U1".to_string()),
            bot_id: None,
            subtype: None,
            permalink: "https://x.slack.com/archives/C1/p1791349900000001".to_string(),
        };
        let body = message.body(&channel_watch());
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
        assert_eq!(Baseline::from_json(baseline.to_json()), Some(baseline));
        assert_eq!(Baseline::from_json(json!({"mergeable": "clean"})), None);
        assert_eq!(Baseline::from_json(json!({"last_ts": "yesterday"})), None);
    }

    #[test]
    fn hidden_messages_never_wake() {
        let message = json!({"ts": "1.000001", "hidden": true});
        assert_eq!(
            disposition(&channel_watch(), &message, &ts("1.000001")),
            Disposition::Skip("hidden".to_string())
        );
    }
}
