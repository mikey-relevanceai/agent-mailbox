//! Topic identifiers and the GitHub-PR topic grammar.
//!
//! # Topic grammar
//!
//! A [`Topic`] is a non-empty, bounded string with no ASCII control characters
//! or whitespace. Topics are a dot-delimited namespace by convention
//! (`<domain>.<kind>.<selector>`), but this layer only enforces the character
//! rules — the *meaning* of a topic is owned by whoever mints it.
//!
//! The one structured topic this crate knows about is a GitHub pull request:
//!
//! ```text
//! github.pr.<owner>/<repo>#<number>
//! e.g.  github.pr.octocat/hello-world#42
//! ```
//!
//! This is deliberately a *parsed* type ([`GithubPr`]), not a stringly
//! convention: constructing a topic and parsing one back go through one place,
//! so the format cannot drift between producers and consumers (parse, don't
//! validate).
//!
//! The second structured topic is an **agent inbox** — the per-session address
//! every live session registers so peers can message it (card 16, ADR-0007):
//!
//! ```text
//! agent.<session-id>
//! e.g.  agent.4f9c1a2b-…
//! ```
//!
//! Same discipline: [`inbox_topic`] mints one and [`Topic::as_agent_inbox`]
//! parses it back to a [`SessionId`], so the two directions can never drift.

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::error::TopicError;
use crate::session::SessionId;

/// Upper bound on topic length. Generous for `owner/repo#n` style ids while
/// still bounding memory for anything that reaches us over the wire.
const MAX_TOPIC_LEN: usize = 512;

/// Prefix shared by every GitHub pull-request topic.
const GITHUB_PR_PREFIX: &str = "github.pr.";

/// Prefix shared by every stub-adapter topic (`stub.<label>`).
const STUB_PREFIX: &str = "stub.";

/// Prefix shared by every agent-inbox topic (`agent.<session-id>`).
const AGENT_INBOX_PREFIX: &str = "agent.";

/// A validated topic identifier.
///
/// Deserialization goes through [`Topic::try_from`] (`#[serde(try_from)]`), so a
/// `Topic` decoded from an untrusted line is guaranteed to satisfy the grammar
/// — there is no way to construct an invalid one.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Topic(String);

impl Topic {
    /// Parse and validate an arbitrary string into a `Topic`.
    pub fn parse(raw: impl Into<String>) -> Result<Self, TopicError> {
        let raw = raw.into();
        if raw.is_empty() {
            return Err(TopicError::Empty);
        }
        if raw.len() > MAX_TOPIC_LEN {
            return Err(TopicError::TooLong {
                len: raw.len(),
                max: MAX_TOPIC_LEN,
            });
        }
        if let Some(ch) = raw.chars().find(|c| c.is_control() || c.is_whitespace()) {
            return Err(TopicError::ForbiddenChar { ch });
        }
        Ok(Self(raw))
    }

    /// Borrow the topic as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Parse this topic as a GitHub pull-request topic, if it is one.
    pub fn as_github_pr(&self) -> Result<GithubPr, TopicError> {
        GithubPr::parse_topic(self)
    }

    /// Parse this topic as an agent inbox (`agent.<session-id>`), recovering the
    /// [`SessionId`] it addresses. The reverse of [`inbox_topic`], and the map
    /// discovery (`mailbox agents`) uses to turn a registered inbox topic back
    /// into the agent that owns it.
    ///
    /// Three distinguishable outcomes, deliberately: [`TopicError::NotAgentInbox`]
    /// when the topic is simply not in the `agent.` namespace (a caller may then
    /// treat the string as something else), and [`TopicError::InvalidSegment`]
    /// when it IS in that namespace but the session segment is malformed (an
    /// error, not a fall-through).
    pub fn as_agent_inbox(&self) -> Result<SessionId, TopicError> {
        let rest = self
            .0
            .strip_prefix(AGENT_INBOX_PREFIX)
            .ok_or(TopicError::NotAgentInbox)?;
        check_inbox_segment(rest)?;
        Ok(SessionId::new(rest))
    }
}

/// The canonical inbox topic for `session`: `agent.<session-id>`.
///
/// Every live session registers this on `arm` (always-on, ADR-0007), which is
/// what makes an agent addressable by its peers. Minted here — never formatted at
/// a call site — so the producer (`send`), the registrar (`harness arm`), and the
/// reverse map ([`Topic::as_agent_inbox`]) can never disagree.
///
/// This is fallible rather than total on purpose: [`SessionId`] is an opaque
/// label from the harness with no grammar of its own, so it is NOT a subset of
/// the topic grammar (a session id containing whitespace, a control character, or
/// the `/`/`#` delimiters used by the other topic schemes cannot form an
/// unambiguous inbox topic). Real harness session ids are UUID-like and always
/// pass; a pathological one is refused loudly instead of silently mangled.
pub fn inbox_topic(session: &SessionId) -> Result<Topic, TopicError> {
    check_inbox_segment(session.as_str())?;
    // Route the assembled string through `Topic::parse` too, so the length bound
    // and the full grammar apply to the whole topic, not just the segment.
    Topic::parse(format!("{AGENT_INBOX_PREFIX}{}", session.as_str()))
}

/// The session segment's rule, shared by [`inbox_topic`] (construction) and
/// [`Topic::as_agent_inbox`] (parsing) so the round trip is lossless by
/// construction. A `.` is allowed (session ids may contain one, and the prefix is
/// stripped exactly once), so `agent.a.b` addresses the session `a.b`.
///
/// A session id that itself begins with the `agent.` prefix is refused
/// ([`TopicError::SessionLooksLikeInbox`]): minting `agent.agent.<id>` would break
/// the injectivity discovery and `parse_send_target` rely on (both strip the
/// prefix exactly once). Enforcing it here keeps construction and parsing in
/// agreement — a topic we would never mint is also one we refuse to parse.
fn check_inbox_segment(session: &str) -> Result<(), TopicError> {
    if session.starts_with(AGENT_INBOX_PREFIX) {
        return Err(TopicError::SessionLooksLikeInbox {
            value: session.to_string(),
        });
    }
    let invalid = session.is_empty()
        || session
            .chars()
            .any(|c| matches!(c, '/' | '#') || c.is_control() || c.is_whitespace());
    if invalid {
        return Err(TopicError::InvalidSegment {
            field: "session",
            value: session.to_string(),
        });
    }
    Ok(())
}

impl fmt::Display for Topic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl TryFrom<String> for Topic {
    type Error = TopicError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        Topic::parse(value)
    }
}

impl From<Topic> for String {
    fn from(topic: Topic) -> Self {
        topic.0
    }
}

/// The canonical topic for a stub-adapter watch labelled `label`.
///
/// The stub topic scheme is `stub.<label>` (e.g. `stub.demo`) — the trivial
/// reference adapter's namespace, minted here so the bridge (the stub resolver)
/// and any consumer agree on one format (parse, don't validate). `label` may not
/// be empty, contain whitespace/control characters, or carry the `/`/`#`
/// delimiters the other topic schemes use (keeping stub topics visually distinct
/// and unambiguous). A `.` in the label is allowed, so a label can itself carve
/// sub-namespaces (`stub.team.ci`).
pub fn stub_topic(label: &str) -> Result<Topic, TopicError> {
    if label.is_empty() {
        return Err(TopicError::InvalidSegment {
            field: "label",
            value: label.to_string(),
        });
    }
    if label
        .chars()
        .any(|c| matches!(c, '/' | '#') || c.is_control() || c.is_whitespace())
    {
        return Err(TopicError::InvalidSegment {
            field: "label",
            value: label.to_string(),
        });
    }
    // Route through `Topic::parse` too, so the length bound and the full grammar
    // apply to the assembled `stub.<label>` string, not just the label segment.
    Topic::parse(format!("{STUB_PREFIX}{label}"))
}

/// A GitHub pull request, the structured form of a `github.pr.*` [`Topic`].
///
/// Construct one with [`GithubPr::new`] (validates the segments once) and turn
/// it into its canonical topic with [`GithubPr::topic`]. Round-tripping a
/// canonical topic through [`Topic::as_github_pr`] yields an equal value.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GithubPr {
    owner: String,
    repo: String,
    number: u64,
}

impl GithubPr {
    /// Validate the parts of a PR reference and build a `GithubPr`.
    ///
    /// `owner`/`repo` may not be empty or contain the topic delimiters
    /// (`/`, `#`), whitespace, or control characters; `number` must be
    /// positive. We intentionally do not re-implement GitHub's full naming
    /// rules — GitHub enforces those — we only guarantee the value is a
    /// losslessly-encodable, unambiguous topic segment.
    pub fn new(
        owner: impl Into<String>,
        repo: impl Into<String>,
        number: u64,
    ) -> Result<Self, TopicError> {
        let owner = owner.into();
        let repo = repo.into();
        Self::check_segment("owner", &owner)?;
        Self::check_segment("repo", &repo)?;
        if number == 0 {
            return Err(TopicError::InvalidPrNumber {
                value: number.to_string(),
            });
        }
        let pr = Self {
            owner,
            repo,
            number,
        };
        // Validate the assembled topic through the SAME path `topic()` uses, so
        // `new`'s invariant matches `topic()`'s precondition. Without this an
        // over-long segment could pass the per-segment checks yet blow the
        // topic-length bound, and `topic()`'s `expect` would later panic on a
        // value this constructor called `Ok`.
        Topic::parse(pr.canonical())?;
        Ok(pr)
    }

    pub fn owner(&self) -> &str {
        &self.owner
    }

    pub fn repo(&self) -> &str {
        &self.repo
    }

    pub fn number(&self) -> u64 {
        self.number
    }

    /// The canonical topic for this PR. Deterministic: equal inputs always
    /// produce the same string.
    pub fn topic(&self) -> Topic {
        // `new` already ran this exact string through `Topic::parse`, so the
        // whole-topic grammar (including the length bound) is guaranteed to
        // hold and the expect is unreachable for any value the constructor
        // returned `Ok`.
        Topic::parse(self.canonical()).expect("canonical github.pr topic is always a valid topic")
    }

    /// Assemble the canonical topic string. The single source of truth for the
    /// grammar, shared by `new` (validation) and `topic()` (construction).
    fn canonical(&self) -> String {
        format!(
            "{GITHUB_PR_PREFIX}{}/{}#{}",
            self.owner, self.repo, self.number
        )
    }

    /// Parse a topic of the form `github.pr.<owner>/<repo>#<number>`.
    fn parse_topic(topic: &Topic) -> Result<Self, TopicError> {
        let rest = topic
            .as_str()
            .strip_prefix(GITHUB_PR_PREFIX)
            .ok_or(TopicError::NotGithubPr)?;

        // `owner/repo#number`: split on the first `/` then the last `#`, so a
        // `.` in a repo name (e.g. `repo.js`) does not confuse us.
        let (owner, repo_and_number) = rest.split_once('/').ok_or(TopicError::NotGithubPr)?;
        let (repo, number) = repo_and_number
            .rsplit_once('#')
            .ok_or(TopicError::NotGithubPr)?;

        Self::check_segment("owner", owner)?;
        Self::check_segment("repo", repo)?;

        let number: u64 = number.parse().map_err(|_| TopicError::InvalidPrNumber {
            value: number.to_string(),
        })?;
        if number == 0 {
            return Err(TopicError::InvalidPrNumber {
                value: number.to_string(),
            });
        }

        Ok(Self {
            owner: owner.to_string(),
            repo: repo.to_string(),
            number,
        })
    }

    fn check_segment(field: &'static str, value: &str) -> Result<(), TopicError> {
        let invalid = value.is_empty()
            || value
                .chars()
                .any(|c| matches!(c, '/' | '#') || c.is_control() || c.is_whitespace());
        if invalid {
            return Err(TopicError::InvalidSegment {
                field,
                value: value.to_string(),
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_empty_topic() {
        assert_eq!(Topic::parse(""), Err(TopicError::Empty));
    }

    #[test]
    fn rejects_whitespace_and_control_chars() {
        assert!(matches!(
            Topic::parse("has space"),
            Err(TopicError::ForbiddenChar { ch: ' ' })
        ));
        assert!(matches!(
            Topic::parse("has\nnewline"),
            Err(TopicError::ForbiddenChar { .. })
        ));
    }

    #[test]
    fn github_pr_topic_is_stable() {
        // Same inputs always produce the same canonical string.
        let a = GithubPr::new("octocat", "hello-world", 42).unwrap();
        let b = GithubPr::new("octocat", "hello-world", 42).unwrap();
        assert_eq!(a.topic(), b.topic());
        assert_eq!(a.topic().as_str(), "github.pr.octocat/hello-world#42");
    }

    #[test]
    fn github_pr_parse_construct_round_trips() {
        let pr = GithubPr::new("octocat", "hello-world", 42).unwrap();
        let parsed = pr.topic().as_github_pr().unwrap();
        assert_eq!(pr, parsed);
    }

    #[test]
    fn dot_in_repo_name_round_trips() {
        // Regression guard: `.` is not a delimiter, so `repo.js` must survive.
        let pr = GithubPr::new("acme", "widget.js", 7).unwrap();
        assert_eq!(pr.topic().as_str(), "github.pr.acme/widget.js#7");
        assert_eq!(pr.topic().as_github_pr().unwrap(), pr);
    }

    #[test]
    fn stub_topic_is_valid_and_prefixed() {
        let topic = stub_topic("demo").unwrap();
        assert_eq!(topic.as_str(), "stub.demo");
        // A dotted label carves a sub-namespace and is still valid.
        assert_eq!(stub_topic("team.ci").unwrap().as_str(), "stub.team.ci");
    }

    #[test]
    fn stub_topic_rejects_bad_labels() {
        for bad in ["", "has space", "with/slash", "with#hash", "tab\tlabel"] {
            assert!(
                matches!(
                    stub_topic(bad),
                    Err(TopicError::InvalidSegment { field: "label", .. })
                ),
                "expected {bad:?} to be rejected"
            );
        }
    }

    #[test]
    fn inbox_topic_round_trips_to_its_session() {
        let session = SessionId::new("4f9c1a2b-7d3e-4c5f-8a1b-2c3d4e5f6a7b");
        let topic = inbox_topic(&session).unwrap();
        assert_eq!(topic.as_str(), "agent.4f9c1a2b-7d3e-4c5f-8a1b-2c3d4e5f6a7b");
        assert_eq!(topic.as_agent_inbox().unwrap(), session);
    }

    #[test]
    fn inbox_topic_keeps_a_dotted_session_id_whole() {
        // The prefix is stripped exactly once, so a `.` in the session id survives
        // the round trip rather than splitting the address.
        let session = SessionId::new("a.b");
        let topic = inbox_topic(&session).unwrap();
        assert_eq!(topic.as_str(), "agent.a.b");
        assert_eq!(topic.as_agent_inbox().unwrap(), session);
    }

    #[test]
    fn inbox_topic_rejects_session_ids_that_cannot_be_addressed() {
        for bad in ["", "has space", "with/slash", "with#hash", "tab\tid"] {
            assert!(
                matches!(
                    inbox_topic(&SessionId::new(bad)),
                    Err(TopicError::InvalidSegment {
                        field: "session",
                        ..
                    })
                ),
                "expected session {bad:?} to be rejected"
            );
        }
    }

    #[test]
    fn inbox_topic_refuses_a_session_that_looks_like_an_inbox() {
        // Injectivity (card 16 / FIX 6): a session id beginning with `agent.` would
        // mint `agent.agent.<id>`, which discovery + `parse_send_target` (each
        // stripping the prefix once) would misroute. It is refused outright.
        assert!(matches!(
            inbox_topic(&SessionId::new("agent.victim")),
            Err(TopicError::SessionLooksLikeInbox { .. })
        ));
        // A normal id that merely CONTAINS "agent" (no `agent.` prefix) round-trips.
        let ok = SessionId::new("agent-smith");
        let topic = inbox_topic(&ok).unwrap();
        assert_eq!(topic.as_str(), "agent.agent-smith");
        assert_eq!(topic.as_agent_inbox().unwrap(), ok);
    }

    #[test]
    fn as_agent_inbox_separates_wrong_namespace_from_malformed_address() {
        // Not in the namespace at all: the caller may legitimately treat the
        // string as something else.
        assert_eq!(
            Topic::parse("stub.demo").unwrap().as_agent_inbox(),
            Err(TopicError::NotAgentInbox)
        );
        // In the namespace but with an empty session segment: a real error.
        assert!(matches!(
            Topic::parse("agent.").unwrap().as_agent_inbox(),
            Err(TopicError::InvalidSegment {
                field: "session",
                ..
            })
        ));
    }

    #[test]
    fn inbox_topic_length_boundary() {
        // The whole assembled topic is bounded, not just the segment.
        let ok = "a".repeat(MAX_TOPIC_LEN - AGENT_INBOX_PREFIX.len());
        assert_eq!(
            inbox_topic(&SessionId::new(ok)).unwrap().as_str().len(),
            MAX_TOPIC_LEN
        );
        let over = "a".repeat(MAX_TOPIC_LEN - AGENT_INBOX_PREFIX.len() + 1);
        assert!(matches!(
            inbox_topic(&SessionId::new(over)),
            Err(TopicError::TooLong { .. })
        ));
    }

    #[test]
    fn rejects_non_github_pr_topics() {
        let topic = Topic::parse("slack.channel.C123").unwrap();
        assert_eq!(topic.as_github_pr(), Err(TopicError::NotGithubPr));
    }

    #[test]
    fn rejects_zero_and_non_numeric_pr() {
        assert!(matches!(
            GithubPr::new("o", "r", 0),
            Err(TopicError::InvalidPrNumber { .. })
        ));
        let bad = Topic::parse("github.pr.o/r#abc").unwrap();
        assert!(matches!(
            bad.as_github_pr(),
            Err(TopicError::InvalidPrNumber { .. })
        ));
    }

    #[test]
    fn rejects_empty_segments() {
        let bad = Topic::parse("github.pr./r#1").unwrap();
        assert!(matches!(
            bad.as_github_pr(),
            Err(TopicError::InvalidSegment { field: "owner", .. })
        ));
        // Mirror for an empty repo segment.
        let bad_repo = Topic::parse("github.pr.o/#1").unwrap();
        assert!(matches!(
            bad_repo.as_github_pr(),
            Err(TopicError::InvalidSegment { field: "repo", .. })
        ));
    }

    #[test]
    fn topic_parse_length_boundary() {
        // Exactly at the bound is fine; one byte over is rejected.
        let ok = "a".repeat(MAX_TOPIC_LEN);
        assert_eq!(Topic::parse(ok.clone()).unwrap().as_str(), ok);
        let over = "a".repeat(MAX_TOPIC_LEN + 1);
        assert!(matches!(
            Topic::parse(over),
            Err(TopicError::TooLong { len, max }) if len == MAX_TOPIC_LEN + 1 && max == MAX_TOPIC_LEN
        ));
    }

    #[test]
    fn github_pr_new_topic_length_boundary() {
        // Regression guard for the latent panic: `new` must reject an
        // over-long assembled topic rather than returning a value whose
        // `topic()` later panics. Build an owner that makes the canonical
        // string land on exactly MAX_TOPIC_LEN, then one longer.
        let fixed = GITHUB_PR_PREFIX.len() + "/r#1".len(); // repo="r", number=1
        let owner_ok = "o".repeat(MAX_TOPIC_LEN - fixed);
        let pr = GithubPr::new(owner_ok, "r", 1).unwrap();
        assert_eq!(pr.topic().as_str().len(), MAX_TOPIC_LEN);

        let owner_over = "o".repeat(MAX_TOPIC_LEN - fixed + 1);
        assert!(matches!(
            GithubPr::new(owner_over, "r", 1),
            Err(TopicError::TooLong { .. })
        ));
    }

    #[test]
    fn github_pr_new_rejects_delimiters_and_control_in_segments() {
        for (owner, repo) in [
            ("a/b", "repo"),
            ("owner", "re#po"),
            ("ow ner", "repo"),
            ("owner", "re\tpo"),
        ] {
            assert!(
                matches!(
                    GithubPr::new(owner, repo, 1),
                    Err(TopicError::InvalidSegment { .. })
                ),
                "expected {owner:?}/{repo:?} to be rejected"
            );
        }
    }

    #[test]
    fn parse_topic_missing_delimiters() {
        // Prefix present but no `/`.
        let no_slash = Topic::parse("github.pr.ownerrepo#5").unwrap();
        assert_eq!(no_slash.as_github_pr(), Err(TopicError::NotGithubPr));
        // Prefix and `/` present but no `#`.
        let no_hash = Topic::parse("github.pr.owner/repo").unwrap();
        assert_eq!(no_hash.as_github_pr(), Err(TopicError::NotGithubPr));
    }

    #[test]
    fn parse_topic_pr_number_out_of_u64_range() {
        let overflow = Topic::parse("github.pr.o/r#99999999999999999999").unwrap();
        assert!(matches!(
            overflow.as_github_pr(),
            Err(TopicError::InvalidPrNumber { .. })
        ));
    }
}
