//! Which messages to the bot wake, and what the wake says (design/04).
//!
//! A message wakes once. Slack redelivers an event it thinks was not
//! acknowledged, so the messages already published are remembered by where they
//! are, `<conversation>/<ts>`, and a repeat is dropped. Keying on the message
//! rather than the event id needs nothing from Slack's retry semantics, and it
//! also catches one message arriving as two events (a DM that @mentions the bot,
//! if Slack sends both). The remembered keys are this mode's baseline, persisted
//! through the bridge (ADR-0005), so a redelivery after a restart is dropped too.
//!
//! Nobody saying anything does not wake: edits, deletions and other subtypes are
//! skipped, as are the bot's own messages. Then the `--skip` filters apply
//! (ADR-0029).

use std::collections::{HashMap, VecDeque};
use std::time::Duration;

use mailbox_protocol::{Poster, SlackFilter, SlackFilters, Subject};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tracing::debug;

use crate::api::SlackApi;
use crate::identity::Identity;
use crate::message::Subtype;
use crate::names::{Names, channel_label};
use crate::socket::{BotMessage, ConversationId, Via};

/// How many published messages are remembered. A redelivery follows its
/// original within minutes (Slack's Events API retries span about six minutes,
/// per its docs), and even a busy bot is addressed far fewer than this many
/// times in that window.
const SEEN_CAPACITY: usize = 256;

/// The longest the name lookups for one subject may take. Slack expects an
/// envelope to be acknowledged within a few seconds, and the ack is sent after
/// the publish (design/04). Past this, the subject uses ids.
const LOOKUP_BUDGET: Duration = Duration::from_secs(2);

/// Where a message is: `<conversation>/<ts>`. Two events about one message
/// share it.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct MessageKey(String);

impl MessageKey {
    pub fn of(message: &BotMessage) -> Self {
        Self(format!("{}/{}", message.conversation, message.ts))
    }
}

/// The messages recently published, oldest first.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Seen {
    seen_messages: VecDeque<MessageKey>,
}

impl Seen {
    /// How many messages are remembered, for the log.
    pub fn remembered(&self) -> usize {
        self.seen_messages.len()
    }

    pub fn contains(&self, key: &MessageKey) -> bool {
        self.seen_messages.contains(key)
    }

    pub fn insert(&mut self, key: MessageKey) {
        if self.contains(&key) {
            return;
        }
        if self.seen_messages.len() == SEEN_CAPACITY {
            self.seen_messages.pop_front();
        }
        self.seen_messages.push_back(key);
    }

    pub fn to_json(&self) -> Value {
        serde_json::to_value(self).expect("a list of keys always serializes")
    }

    /// `None` for a value this mode did not write; the caller starts empty.
    pub fn from_json(value: Value) -> Option<Self> {
        let mut seen: Seen = serde_json::from_value(value).ok()?;
        while seen.seen_messages.len() > SEEN_CAPACITY {
            seen.seen_messages.pop_front();
        }
        Some(seen)
    }
}

/// Why a message does not wake.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Skip {
    /// Already published: a redelivery, or a second event about one message.
    Duplicate,
    /// Hidden from the conversation, as an edit's event is.
    Hidden,
    /// A change to a message or the conversation (`message_changed`,
    /// `message_deleted`, …) or a `bot_message`: nobody said anything to the bot.
    Subtype(String),
    /// Posted by the bot itself.
    Own,
    Filtered(SlackFilter),
}

impl Skip {
    /// The reason, for the log.
    pub fn reason(&self) -> String {
        match self {
            Skip::Duplicate => "duplicate".to_string(),
            Skip::Hidden => "hidden".to_string(),
            Skip::Subtype(subtype) => format!("subtype {subtype}"),
            Skip::Own => "the bot's own message".to_string(),
            Skip::Filtered(filter) => format!("--skip {filter}"),
        }
    }
}

/// What to do with one message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Disposition {
    /// Boxed: a wake carries the whole message.
    Wake(Box<Wake>),
    Skip(Skip),
}

/// A message that wakes the watch's subscribers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Wake {
    pub message: BotMessage,
    pub author: String,
    /// `#<name>` for a mention; a DM has no name to show.
    pub place: Option<String>,
    pub permalink: String,
    /// Whether the name lookups finished; when not, the subject uses ids.
    pub names_resolved: bool,
}

impl Wake {
    /// Names the message and links to it. Never quotes the text.
    pub fn subject(&self) -> Option<Subject> {
        let author = &self.author;
        let in_thread = self.message.parent().is_some();
        let place = self.place.as_deref().unwrap_or("a conversation");
        let text = match (self.message.via, in_thread) {
            (Via::Mention, false) => format!("mention from {author} in {place}"),
            (Via::Mention, true) => format!("mention from {author} in a thread in {place}"),
            (Via::Dm, false) => format!("DM from {author}"),
            (Via::Dm, true) => format!("DM from {author} in a thread"),
        };
        // A subject is a courtesy, never a gate (design/02).
        Subject::new(&text, Some(&self.permalink)).ok()
    }

    /// Where the message is and who sent it, never what it says.
    pub fn body(&self) -> Value {
        let message = &self.message;
        let mut body = json!({
            "kind": match message.via {
                Via::Mention => "slack_mention",
                Via::Dm => "slack_dm",
            },
            "channel": message.conversation.as_str(),
            "ts": message.ts.to_string(),
            "permalink": self.permalink,
        });
        let fields = [
            ("thread_ts", message.parent().map(ToString::to_string)),
            (
                "user",
                message.user.as_ref().map(|u| u.as_str().to_string()),
            ),
            ("bot_id", message.bot_id.clone()),
            (
                "app_id",
                message.app_id.as_ref().map(|a| a.as_str().to_string()),
            ),
            (
                "subtype",
                message.subtype.as_ref().map(|s| s.as_str().to_string()),
            ),
            (
                "event_id",
                message.event_id.as_ref().map(|e| e.as_str().to_string()),
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

/// The bot watch's state across connections.
pub(crate) struct BotMessages<A> {
    /// Web API calls with the bot token, for names.
    api: A,
    identity: Identity,
    skip: SlackFilters,
    seen: Seen,
    names: Names,
    places: HashMap<ConversationId, String>,
}

impl<A: SlackApi> BotMessages<A> {
    pub fn new(api: A, identity: Identity, skip: SlackFilters, seen: Seen) -> Self {
        Self {
            api,
            identity,
            skip,
            seen,
            names: Names::default(),
            places: HashMap::new(),
        }
    }

    pub fn seen(&self) -> &Seen {
        &self.seen
    }

    /// Decide what one message does. Recording it as published is the caller's
    /// job, after the publish is written: [`BotMessages::published`].
    pub async fn dispose(&mut self, message: BotMessage) -> Disposition {
        if let Some(skip) = self.skip_reason(&message) {
            return Disposition::Skip(skip);
        }
        let permalink = self.identity.workspace.permalink(
            message.conversation.as_str(),
            &message.ts,
            message.parent(),
        );
        let (author, place, names_resolved) =
            match tokio::time::timeout(LOOKUP_BUDGET, self.describe(&message)).await {
                Ok((author, place)) => (author, place, true),
                Err(_) => {
                    debug!("name lookups ran past their budget; the subject uses ids");
                    let place =
                        (message.via == Via::Mention).then(|| message.conversation.to_string());
                    (author_id(&message), place, false)
                }
            };
        Disposition::Wake(Box::new(Wake {
            message,
            author,
            place,
            permalink,
            names_resolved,
        }))
    }

    /// Remember a published message, so a redelivery of it is dropped.
    pub fn published(&mut self, wake: &Wake) {
        self.seen.insert(MessageKey::of(&wake.message));
    }

    /// The first rule a message fails, in a fixed order, so the logged reason is
    /// the most basic one.
    fn skip_reason(&self, message: &BotMessage) -> Option<Skip> {
        if self.seen.contains(&MessageKey::of(message)) {
            return Some(Skip::Duplicate);
        }
        if message.hidden {
            return Some(Skip::Hidden);
        }
        match &message.subtype {
            None | Some(Subtype::FileShare | Subtype::MeMessage | Subtype::ThreadBroadcast) => {}
            Some(other @ (Subtype::BotMessage | Subtype::Other(_))) => {
                return Some(Skip::Subtype(other.as_str().to_string()));
            }
        }
        let by_bot_user = message.user.is_some() && message.user == self.identity.bot_user;
        let by_bot_id = message.bot_id.is_some() && message.bot_id == self.identity.bot_id;
        if by_bot_user || by_bot_id {
            return Some(Skip::Own);
        }
        let poster = Poster {
            user: message.user.as_ref(),
            app: message.app_id.as_ref(),
        };
        self.skip
            .matching(poster)
            .map(|filter| Skip::Filtered(filter.clone()))
    }

    async fn describe(&mut self, message: &BotMessage) -> (String, Option<String>) {
        let author = match &message.user {
            Some(user) => self.names.get(&self.api, user).await,
            None => author_id(message),
        };
        let place = match message.via {
            Via::Mention => Some(self.place(&message.conversation).await),
            Via::Dm => None,
        };
        (author, place)
    }

    async fn place(&mut self, conversation: &ConversationId) -> String {
        if let Some(label) = self.places.get(conversation) {
            return label.clone();
        }
        match channel_label(&self.api, conversation.as_str()).await {
            Ok(label) => {
                let label = label.unwrap_or_else(|| conversation.to_string());
                self.places.insert(conversation.clone(), label.clone());
                label
            }
            Err(err) => {
                debug!(channel = conversation.as_str(), error = %err, "could not look up a channel's name; using the id");
                conversation.to_string()
            }
        }
    }
}

fn author_id(message: &BotMessage) -> String {
    message
        .user
        .as_ref()
        .map(|user| user.as_str().to_string())
        .or_else(|| message.bot_id.clone())
        .unwrap_or_else(|| "someone".to_string())
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use mailbox_protocol::{SlackTs, SlackUserId};

    use super::*;
    use crate::api::SlackError;
    use crate::identity::WorkspaceUrl;

    const WORKSPACE: &str = "https://tryrelevance.slack.com/";
    const CHANNEL: &str = "C0C83CXLUL8";
    const DM: &str = "D0C8ABCDEF";
    const PARENT: &str = "1791349480.652779";
    const REPLY: &str = "1791349500.000002";
    const BOT_USER: &str = "U0C6VHAA4LX";
    const MIKEY: &str = "U0AB7RJSQBE";

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

    fn message(via: Via, conversation: &str, thread_ts: Option<&str>) -> BotMessage {
        BotMessage {
            via,
            event_id: None,
            event_time: Some(1_791_349_500),
            conversation: ConversationId::parse(conversation).unwrap(),
            ts: SlackTs::parse(REPLY).unwrap(),
            thread_ts: thread_ts.map(|ts| SlackTs::parse(ts).unwrap()),
            user: Some(SlackUserId::parse(MIKEY).unwrap()),
            bot_id: None,
            app_id: None,
            subtype: None,
            hidden: false,
        }
    }

    fn mention(thread_ts: Option<&str>) -> BotMessage {
        message(Via::Mention, CHANNEL, thread_ts)
    }

    fn dm() -> BotMessage {
        message(Via::Dm, DM, None)
    }

    fn watch(slack: &FakeSlack, skip: SlackFilters) -> BotMessages<&FakeSlack> {
        let identity = Identity {
            workspace: WorkspaceUrl::parse(WORKSPACE),
            bot_user: Some(SlackUserId::parse(BOT_USER).unwrap()),
            bot_id: Some("B0C74MURRK5".to_string()),
        };
        BotMessages::new(slack, identity, skip, Seen::default())
    }

    fn woken(disposition: Disposition) -> Wake {
        match disposition {
            Disposition::Wake(wake) => *wake,
            other => panic!("expected a wake, got {other:?}"),
        }
    }

    fn skipped(disposition: Disposition) -> Skip {
        match disposition {
            Disposition::Skip(skip) => skip,
            other => panic!("expected a skip, got {other:?}"),
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_mention_wakes_with_a_named_subject_and_a_link() {
        let slack = FakeSlack::default();
        let wake = woken(
            watch(&slack, SlackFilters::default())
                .dispose(mention(None))
                .await,
        );
        let subject = wake.subject().unwrap();
        assert_eq!(
            subject.text(),
            format!("mention from Name of {MIKEY} in #agent-mailbox-test")
        );
        assert_eq!(
            subject.link(),
            Some("https://tryrelevance.slack.com/archives/C0C83CXLUL8/p1791349500000002")
        );
        assert!(wake.names_resolved);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_mention_in_a_thread_says_so_and_links_into_the_thread() {
        let slack = FakeSlack::default();
        let wake = woken(
            watch(&slack, SlackFilters::default())
                .dispose(mention(Some(PARENT)))
                .await,
        );
        assert!(
            wake.subject()
                .unwrap()
                .text()
                .contains("in a thread in #agent-mailbox-test")
        );
        assert_eq!(
            wake.permalink,
            format!(
                "{WORKSPACE}archives/{CHANNEL}/p1791349500000002?thread_ts={PARENT}&cid={CHANNEL}"
            )
        );
        assert_eq!(wake.body()["thread_ts"], PARENT);
    }

    /// A DM names its sender and needs no channel lookup.
    #[tokio::test(flavor = "current_thread")]
    async fn a_dm_wakes_naming_its_sender() {
        let slack = FakeSlack::default();
        let wake = woken(watch(&slack, SlackFilters::default()).dispose(dm()).await);
        assert_eq!(
            wake.subject().unwrap().text(),
            format!("DM from Name of {MIKEY}")
        );
        assert_eq!(wake.body()["kind"], "slack_dm");
        assert_eq!(wake.body()["channel"], DM);
        assert_eq!(*slack.calls.borrow(), ["users.info"]);
    }

    /// The point of the seen set: one message, delivered twice, wakes once.
    #[tokio::test(flavor = "current_thread")]
    async fn a_redelivered_message_is_dropped_once_the_first_was_published() {
        let slack = FakeSlack::default();
        let mut watch = watch(&slack, SlackFilters::default());
        let first = woken(watch.dispose(mention(None)).await);
        watch.published(&first);
        assert_eq!(skipped(watch.dispose(mention(None)).await), Skip::Duplicate);
        assert!(matches!(watch.dispose(dm()).await, Disposition::Wake(_)));
    }

    /// Recorded only once published: if the publish never happened, the
    /// redelivery is the message's second chance and must wake.
    #[tokio::test(flavor = "current_thread")]
    async fn an_unpublished_message_is_not_remembered() {
        let slack = FakeSlack::default();
        let mut watch = watch(&slack, SlackFilters::default());
        let _ = watch.dispose(mention(None)).await;
        assert!(matches!(
            watch.dispose(mention(None)).await,
            Disposition::Wake(_)
        ));
    }

    /// The wake table for messages that say nothing to the bot.
    #[tokio::test(flavor = "current_thread")]
    async fn edits_deletions_bot_messages_and_the_bots_own_posts_do_not_wake() {
        let slack = FakeSlack::default();
        let mut watch = watch(&slack, SlackFilters::default());
        let with = |edit: fn(&mut BotMessage)| {
            let mut message = dm();
            edit(&mut message);
            message
        };
        let cases: [(BotMessage, Skip); 5] = [
            (
                with(|m| m.subtype = Some(Subtype::Other("message_changed".into()))),
                Skip::Subtype("message_changed".into()),
            ),
            (
                with(|m| m.subtype = Some(Subtype::Other("message_deleted".into()))),
                Skip::Subtype("message_deleted".into()),
            ),
            (
                with(|m| m.subtype = Some(Subtype::BotMessage)),
                Skip::Subtype("bot_message".into()),
            ),
            (
                with(|m| m.user = Some(SlackUserId::parse(BOT_USER).unwrap())),
                Skip::Own,
            ),
            (
                with(|m| {
                    m.user = None;
                    m.bot_id = Some("B0C74MURRK5".into());
                }),
                Skip::Own,
            ),
        ];
        for (message, expected) in cases {
            assert_eq!(skipped(watch.dispose(message).await), expected);
        }
        let hidden = with(|m| m.hidden = true);
        assert_eq!(skipped(watch.dispose(hidden).await), Skip::Hidden);
        let file = with(|m| m.subtype = Some(Subtype::FileShare));
        assert!(matches!(watch.dispose(file).await, Disposition::Wake(_)));
    }

    /// The two forms the bot's own DM posts were seen in on 2026-10-15, once it
    /// started posting (measured by the orchestrator in a DM with it): as its
    /// user with no subtype, and as a `bot_message` with no user. Both carry the
    /// bot's `bot_id` and its app's `app_id`. Neither may wake the bot watch.
    #[tokio::test(flavor = "current_thread")]
    async fn the_bots_own_dm_posts_do_not_wake_in_either_measured_form() {
        let slack = FakeSlack::default();
        let mut watch = watch(&slack, SlackFilters::default());
        let own = |user: Option<&str>, subtype: Option<Subtype>, ts: &str| {
            let mut message = dm();
            message.ts = SlackTs::parse(ts).unwrap();
            message.user = user.map(|user| SlackUserId::parse(user).unwrap());
            message.bot_id = Some("B0C74MURRK5".into());
            message.app_id = Some(mailbox_protocol::SlackAppId::parse("A0C7AJ9AMUJ").unwrap());
            message.subtype = subtype;
            message
        };
        let as_user = own(Some(BOT_USER), None, "1791935500.000001");
        assert_eq!(skipped(watch.dispose(as_user).await), Skip::Own);
        let as_bot_message = own(None, Some(Subtype::BotMessage), "1791935500.000002");
        assert_eq!(
            skipped(watch.dispose(as_bot_message).await),
            Skip::Subtype("bot_message".into())
        );
        assert!(slack.calls.borrow().is_empty());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_filter_drops_the_message_it_matches() {
        let slack = FakeSlack::default();
        let skip = SlackFilters::new(vec![SlackFilter::parse(&format!("user={MIKEY}")).unwrap()]);
        let mut watch = watch(&slack, skip);
        for message in [mention(None), dm()] {
            assert!(matches!(
                skipped(watch.dispose(message).await),
                Skip::Filtered(_)
            ));
        }
        assert!(
            slack.calls.borrow().is_empty(),
            "no lookup for a skipped message"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn names_are_looked_up_once_and_a_failed_lookup_uses_ids() {
        let slack = FakeSlack::default();
        let mut watch = watch(&slack, SlackFilters::default());
        let _ = watch.dispose(mention(None)).await;
        let _ = watch.dispose(mention(None)).await;
        assert_eq!(
            slack.calls.borrow().len(),
            2,
            "one users.info, one conversations.info"
        );

        let offline = FakeSlack {
            down: true,
            ..FakeSlack::default()
        };
        let mut watch = self::watch(&offline, SlackFilters::default());
        let wake = woken(watch.dispose(mention(None)).await);
        assert_eq!(
            wake.subject().unwrap().text(),
            format!("mention from {MIKEY} in {CHANNEL}")
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn the_body_points_at_the_message_and_never_carries_its_text() {
        let slack = FakeSlack::default();
        let mut message = mention(None);
        message.event_id = None;
        let body = woken(
            watch(&slack, SlackFilters::default())
                .dispose(message)
                .await,
        )
        .body();
        assert_eq!(
            body,
            json!({"kind": "slack_mention", "channel": CHANNEL, "ts": REPLY,
                "permalink": format!("{WORKSPACE}archives/{CHANNEL}/p1791349500000002"),
                "user": MIKEY})
        );
    }

    #[test]
    fn the_seen_set_is_bounded_and_round_trips() {
        let mut seen = Seen::default();
        for i in 0..SEEN_CAPACITY + 10 {
            seen.insert(MessageKey(format!("C1/{i}.000000")));
        }
        assert!(!seen.contains(&MessageKey("C1/0.000000".into())));
        assert!(seen.contains(&MessageKey(format!("C1/{}.000000", SEEN_CAPACITY + 9))));
        assert_eq!(Seen::from_json(seen.to_json()), Some(seen));
        assert_eq!(Seen::from_json(json!({"last_ts": "1.000001"})), None);
    }
}
