//! Filters that stop a Slack message from waking a watch (ADR-0029).
//!
//! A filter is written `key=value[,key=value…]` and matches a message when every
//! condition holds. A watch skips a message that matches any of its filters.
//!
//! ```text
//! user=U0000000001,app=A0000000001   posts by that user made through that app
//! app=A0000000001                    posts made through that app, by anyone
//! user=U0000000001                   everything that user posts
//! ```
//!
//! # Why `app` and not the footer or `client_msg_id`
//!
//! The case this was built for is a person whose agents post through the claude.ai
//! Slack connector, which posts as that person's own user. `app_id` names the app
//! a post came through; a missing `client_msg_id` only says "not typed in a Slack
//! client", and a footer is text, which the adapter does not read. ADR-0029 has
//! the measurement this rests on.
//!
//! These types live in the protocol because the bridge stores and forwards them
//! and the adapter applies them; both must agree on what a filter means.

use std::fmt;

use serde::{Deserialize, Serialize};

/// Slack ids are short; this bounds what an untrusted CLI arg can make us carry.
const MAX_ID_LEN: usize = 32;

/// Why a string is not a valid Slack filter.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SlackFilterError {
    #[error("a Slack filter needs at least one condition, e.g. user=U0000000001,app=A0000000001")]
    Empty,
    #[error("Slack filter condition {part:?} is not key=value")]
    NotKeyValue { part: String },
    #[error("Slack filter key {key:?} is not supported; use user=<U…> and/or app=<A…>")]
    UnknownKey { key: String },
    #[error("Slack filter key {key:?} is given twice")]
    Duplicate { key: String },
    #[error("Slack user id {value:?} is invalid: expected an id like U0000000001")]
    InvalidUser { value: String },
    #[error("Slack app id {value:?} is invalid: expected an id like A0000000001")]
    InvalidApp { value: String },
}

/// `<prefix><uppercase letters and digits>`, the shape every Slack object id has.
/// Only the shape is checked, not a length Slack happens to use today: the adapter
/// parses message fields with this too, and rejecting an unusual but real id
/// there would read a message as having no author.
fn is_slack_id(raw: &str, prefixes: &[char]) -> bool {
    let mut chars = raw.chars();
    chars.next().is_some_and(|first| prefixes.contains(&first))
        && chars.all(|c| c.is_ascii_uppercase() || c.is_ascii_digit())
        && (2..=MAX_ID_LEN).contains(&raw.len())
}

/// A Slack user id: `U…`, or `W…` on an Enterprise Grid org.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct SlackUserId(String);

impl SlackUserId {
    pub fn parse(raw: &str) -> Result<Self, SlackFilterError> {
        if is_slack_id(raw, &['U', 'W']) {
            Ok(Self(raw.to_string()))
        } else {
            Err(SlackFilterError::InvalidUser {
                value: raw.to_string(),
            })
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for SlackUserId {
    type Error = SlackFilterError;

    fn try_from(raw: String) -> Result<Self, Self::Error> {
        Self::parse(&raw)
    }
}

impl From<SlackUserId> for String {
    fn from(id: SlackUserId) -> Self {
        id.0
    }
}

/// A Slack app id (`A…`): the app a message was posted through.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct SlackAppId(String);

impl SlackAppId {
    pub fn parse(raw: &str) -> Result<Self, SlackFilterError> {
        if is_slack_id(raw, &['A']) {
            Ok(Self(raw.to_string()))
        } else {
            Err(SlackFilterError::InvalidApp {
                value: raw.to_string(),
            })
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for SlackAppId {
    type Error = SlackFilterError;

    fn try_from(raw: String) -> Result<Self, Self::Error> {
        Self::parse(&raw)
    }
}

impl From<SlackAppId> for String {
    fn from(id: SlackAppId) -> Self {
        id.0
    }
}

/// One filter: the conditions a message must all meet to be skipped.
///
/// At least one condition is always present, so a filter can never match
/// everything by accident. That is why decoding goes through [`SlackFilterWire`].
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "SlackFilterWire", into = "SlackFilterWire")]
pub struct SlackFilter {
    user: Option<SlackUserId>,
    app: Option<SlackAppId>,
}

#[derive(Serialize, Deserialize)]
struct SlackFilterWire {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    user: Option<SlackUserId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    app: Option<SlackAppId>,
}

impl TryFrom<SlackFilterWire> for SlackFilter {
    type Error = SlackFilterError;

    fn try_from(wire: SlackFilterWire) -> Result<Self, Self::Error> {
        Self::new(wire.user, wire.app)
    }
}

impl From<SlackFilter> for SlackFilterWire {
    fn from(filter: SlackFilter) -> Self {
        SlackFilterWire {
            user: filter.user,
            app: filter.app,
        }
    }
}

impl SlackFilter {
    pub fn new(
        user: Option<SlackUserId>,
        app: Option<SlackAppId>,
    ) -> Result<Self, SlackFilterError> {
        if user.is_none() && app.is_none() {
            return Err(SlackFilterError::Empty);
        }
        Ok(Self { user, app })
    }

    /// Parse the CLI form, `key=value[,key=value…]`.
    pub fn parse(raw: &str) -> Result<Self, SlackFilterError> {
        let mut user = None;
        let mut app = None;
        for part in raw
            .split(',')
            .map(str::trim)
            .filter(|part| !part.is_empty())
        {
            let (key, value) =
                part.split_once('=')
                    .ok_or_else(|| SlackFilterError::NotKeyValue {
                        part: part.to_string(),
                    })?;
            let duplicate = || SlackFilterError::Duplicate {
                key: key.to_string(),
            };
            match key.trim() {
                "user" if user.is_some() => return Err(duplicate()),
                "user" => user = Some(SlackUserId::parse(value.trim())?),
                "app" if app.is_some() => return Err(duplicate()),
                "app" => app = Some(SlackAppId::parse(value.trim())?),
                _ => {
                    return Err(SlackFilterError::UnknownKey {
                        key: key.to_string(),
                    });
                }
            }
        }
        Self::new(user, app)
    }

    /// Whether a message from `poster` meets every condition. A message with no
    /// app (typed in a Slack client) never matches an `app` condition, which is
    /// what keeps a person's own messages out of a filter aimed at their agents.
    pub fn matches(&self, poster: Poster<'_>) -> bool {
        let user_ok = self
            .user
            .as_ref()
            .is_none_or(|want| poster.user == Some(want));
        let app_ok = self
            .app
            .as_ref()
            .is_none_or(|want| poster.app == Some(want));
        user_ok && app_ok
    }
}

/// Who posted a message and through what, as a filter sees it. Named fields of
/// distinct types, so a user id cannot be passed where an app id belongs.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Poster<'a> {
    pub user: Option<&'a SlackUserId>,
    pub app: Option<&'a SlackAppId>,
}

/// Renders the CLI form, conditions in a fixed order, so it parses back to itself.
impl fmt::Display for SlackFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let conditions = [
            self.user.as_ref().map(|u| format!("user={}", u.as_str())),
            self.app.as_ref().map(|a| format!("app={}", a.as_str())),
        ];
        let rendered: Vec<String> = conditions.into_iter().flatten().collect();
        f.write_str(&rendered.join(","))
    }
}

/// A watch's filters as a set: sorted, without duplicates.
///
/// A set because two sessions asking for the same filters in a different order
/// are asking for the same watch, and the bridge compares a re-watch's filters
/// with the stored ones to decide whether anything changed (ADR-0029).
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(from = "Vec<SlackFilter>", into = "Vec<SlackFilter>")]
pub struct SlackFilters(Vec<SlackFilter>);

impl SlackFilters {
    pub fn new(mut filters: Vec<SlackFilter>) -> Self {
        filters.sort();
        filters.dedup();
        Self(filters)
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = &SlackFilter> {
        self.0.iter()
    }

    /// The first filter a message from `poster` matches.
    pub fn matching(&self, poster: Poster<'_>) -> Option<&SlackFilter> {
        self.0.iter().find(|filter| filter.matches(poster))
    }
}

impl From<Vec<SlackFilter>> for SlackFilters {
    fn from(filters: Vec<SlackFilter>) -> Self {
        Self::new(filters)
    }
}

impl From<SlackFilters> for Vec<SlackFilter> {
    fn from(filters: SlackFilters) -> Self {
        filters.0
    }
}

/// Each filter in its CLI form, separated by `; `. Empty for no filters.
impl fmt::Display for SlackFilters {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let rendered: Vec<String> = self.0.iter().map(ToString::to_string).collect();
        f.write_str(&rendered.join("; "))
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    const MIKEY: &str = "U0AB7RJSQBE";
    const CLAUDE_APP: &str = "A08SF47R6P4";

    /// A poster from raw ids, so each case reads as the message it models.
    fn by(user: Option<&str>, app: Option<&str>) -> (Option<SlackUserId>, Option<SlackAppId>) {
        (
            user.map(|u| SlackUserId::parse(u).unwrap()),
            app.map(|a| SlackAppId::parse(a).unwrap()),
        )
    }

    fn poster<'a>(ids: &'a (Option<SlackUserId>, Option<SlackAppId>)) -> Poster<'a> {
        Poster {
            user: ids.0.as_ref(),
            app: ids.1.as_ref(),
        }
    }

    fn app_posts_by_mikey() -> SlackFilter {
        SlackFilter::parse(&format!("user={MIKEY},app={CLAUDE_APP}")).unwrap()
    }

    #[test]
    fn a_user_and_app_filter_skips_their_app_posts_and_keeps_their_typing() {
        let filter = app_posts_by_mikey();
        assert!(
            filter.matches(poster(&by(Some(MIKEY), Some(CLAUDE_APP)))),
            "agent post"
        );
        assert!(
            !filter.matches(poster(&by(Some(MIKEY), None))),
            "typed by hand"
        );
        assert!(
            !filter.matches(poster(&by(Some("U0B16QNQEFR"), Some(CLAUDE_APP)))),
            "someone else's agent"
        );
        assert!(
            !filter.matches(poster(&by(Some(MIKEY), Some("A0BH7B5SNC9")))),
            "another app"
        );
    }

    #[test]
    fn single_condition_filters_match_on_that_condition_alone() {
        let app = SlackFilter::parse(&format!("app={CLAUDE_APP}")).unwrap();
        assert!(app.matches(poster(&by(Some("U0B16QNQEFR"), Some(CLAUDE_APP)))));
        assert!(app.matches(poster(&by(None, Some(CLAUDE_APP)))));
        assert!(!app.matches(poster(&by(Some(MIKEY), None))));

        let user = SlackFilter::parse(&format!("user={MIKEY}")).unwrap();
        assert!(user.matches(poster(&by(Some(MIKEY), None))));
        assert!(user.matches(poster(&by(Some(MIKEY), Some(CLAUDE_APP)))));
        assert!(!user.matches(poster(&by(None, Some(CLAUDE_APP)))));
    }

    #[test]
    fn parse_accepts_either_order_and_renders_canonically() {
        let reversed = SlackFilter::parse(&format!(" app={CLAUDE_APP} , user={MIKEY} ")).unwrap();
        assert_eq!(reversed, app_posts_by_mikey());
        assert_eq!(
            reversed.to_string(),
            format!("user={MIKEY},app={CLAUDE_APP}")
        );
        assert_eq!(SlackFilter::parse(&reversed.to_string()).unwrap(), reversed);
    }

    #[test]
    fn parse_rejects_what_it_cannot_mean() {
        assert_eq!(SlackFilter::parse(""), Err(SlackFilterError::Empty));
        assert_eq!(SlackFilter::parse(" , "), Err(SlackFilterError::Empty));
        assert!(matches!(
            SlackFilter::parse("user"),
            Err(SlackFilterError::NotKeyValue { .. })
        ));
        assert!(matches!(
            SlackFilter::parse("text=hello"),
            Err(SlackFilterError::UnknownKey { .. })
        ));
        assert!(matches!(
            SlackFilter::parse(&format!("user={MIKEY},user={MIKEY}")),
            Err(SlackFilterError::Duplicate { .. })
        ));
        for bad in ["u0ab7rjsqbe", "C0C83CXLUL8", "U", "@mikey", "U0AB 7RJ"] {
            assert!(
                matches!(
                    SlackFilter::parse(&format!("user={bad}")),
                    Err(SlackFilterError::InvalidUser { .. })
                ),
                "{bad:?} must be rejected"
            );
        }
        assert!(matches!(
            SlackFilter::parse("app=Claude"),
            Err(SlackFilterError::InvalidApp { .. })
        ));
    }

    #[test]
    fn decoding_parses_and_refuses_an_empty_filter() {
        let wire = json!({"user": MIKEY, "app": CLAUDE_APP});
        assert_eq!(
            serde_json::from_value::<SlackFilter>(wire.clone()).unwrap(),
            app_posts_by_mikey()
        );
        assert_eq!(serde_json::to_value(app_posts_by_mikey()).unwrap(), wire);
        assert_eq!(
            serde_json::to_value(SlackFilter::parse(&format!("app={CLAUDE_APP}")).unwrap())
                .unwrap(),
            json!({"app": CLAUDE_APP}),
            "an absent condition is omitted, not null"
        );
        assert!(serde_json::from_value::<SlackFilter>(json!({})).is_err());
        assert!(serde_json::from_value::<SlackFilter>(json!({"user": "mikey"})).is_err());
    }

    #[test]
    fn filters_are_a_set_so_order_and_repeats_do_not_make_a_new_watch() {
        let user = SlackFilter::parse(&format!("user={MIKEY}")).unwrap();
        let both = app_posts_by_mikey();
        let one = SlackFilters::new(vec![both.clone(), user.clone(), both.clone()]);
        let other = SlackFilters::new(vec![user.clone(), both.clone()]);
        assert_eq!(one, other);
        assert_eq!(
            serde_json::to_string(&one).unwrap(),
            serde_json::to_string(&other).unwrap(),
            "the stored form is canonical too"
        );
        let decoded: SlackFilters =
            serde_json::from_value(json!([{"user": MIKEY}, {"user": MIKEY}])).unwrap();
        assert_eq!(decoded, SlackFilters::new(vec![user]));
    }

    #[test]
    fn matching_names_the_filter_that_matched() {
        let filters = SlackFilters::new(vec![app_posts_by_mikey()]);
        assert_eq!(
            filters.matching(poster(&by(Some(MIKEY), Some(CLAUDE_APP)))),
            Some(&app_posts_by_mikey())
        );
        assert_eq!(filters.matching(poster(&by(Some(MIKEY), None))), None);
        assert_eq!(
            SlackFilters::default().matching(poster(&by(Some(MIKEY), None))),
            None
        );
    }
}
