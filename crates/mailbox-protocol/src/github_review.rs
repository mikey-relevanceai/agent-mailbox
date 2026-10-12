//! The GitHub review-request watch's target: whose review requests to watch.
//!
//! ```text
//! github.review-requests.<login>
//! e.g.  github.review-requests.octocat
//! ```
//!
//! The watch is per *person*, not per PR — a review request arrives on a PR
//! nobody is watching yet (design/03). So its identity is a GitHub login, parsed
//! once here and carried as itself across the control socket, the same way a
//! [`crate::SlackWatch`] is.

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::topic::Topic;

/// Prefix shared by every review-request topic (`github.review-requests.<login>`).
const REVIEW_REQUESTS_PREFIX: &str = "github.review-requests.";

/// GitHub's own cap on a login's length.
const MAX_LOGIN_LEN: usize = 39;

/// Why a string is not a GitHub login this watch accepts.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum GithubLoginError {
    // `@me` is refused rather than resolved: it means whoever the daemon's `gh`
    // is logged in as, which can change under a running watch, and a topic must
    // name one person for as long as the watch exists (design/03).
    #[error(
        "`@me` is not accepted: name the GitHub login (find yours with `gh api user --jq .login`)"
    )]
    Me,
    // The echoed value is a bounded login the caller typed, never content.
    #[error(
        "GitHub login {value:?} is invalid: expected 1-39 letters, digits or hyphens, not starting with a hyphen"
    )]
    Invalid { value: String },
}

/// A GitHub login, lowercased.
///
/// Lowercased because GitHub logins are case-insensitive: `Octocat` and
/// `octocat` are one person, so they must be one watch and one topic, not two
/// pollers that each wake their own subscribers.
///
/// The character rule (ASCII letters, digits, hyphen) is also what makes it safe
/// to splice into a search query: a login can carry no space or `:`, so it cannot
/// smuggle another qualifier into the adapter's search. GitHub's finer rules (no
/// double or trailing hyphen) are GitHub's to enforce; this only guarantees an
/// unambiguous topic segment and an inert query term.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct GithubLogin(String);

impl GithubLogin {
    /// Parse a login as a person typed it.
    pub fn parse(raw: &str) -> Result<Self, GithubLoginError> {
        if raw.eq_ignore_ascii_case("@me") {
            return Err(GithubLoginError::Me);
        }
        let valid = !raw.is_empty()
            && raw.len() <= MAX_LOGIN_LEN
            && !raw.starts_with('-')
            && raw.chars().all(|c| c.is_ascii_alphanumeric() || c == '-');
        if !valid {
            return Err(GithubLoginError::Invalid {
                value: raw.to_string(),
            });
        }
        Ok(Self(raw.to_ascii_lowercase()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The topic a review-request watch for this login publishes on.
    pub fn review_requests_topic(&self) -> Topic {
        // A login is at most 39 ASCII letters, digits and hyphens, so the
        // assembled topic always satisfies the grammar.
        Topic::parse(format!("{REVIEW_REQUESTS_PREFIX}{}", self.0))
            .expect("a review-request topic is always a valid topic")
    }
}

impl fmt::Display for GithubLogin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl TryFrom<String> for GithubLogin {
    type Error = GithubLoginError;

    fn try_from(raw: String) -> Result<Self, Self::Error> {
        Self::parse(&raw)
    }
}

impl From<GithubLogin> for String {
    fn from(login: GithubLogin) -> Self {
        login.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_login_is_lowercased_so_case_variants_are_one_watch() {
        let upper = GithubLogin::parse("Mikey-RelevanceAI").unwrap();
        let lower = GithubLogin::parse("mikey-relevanceai").unwrap();
        assert_eq!(upper, lower);
        assert_eq!(
            upper.review_requests_topic().as_str(),
            "github.review-requests.mikey-relevanceai"
        );
    }

    #[test]
    fn at_me_is_refused_with_a_way_to_find_the_login() {
        assert_eq!(GithubLogin::parse("@me"), Err(GithubLoginError::Me));
        assert_eq!(GithubLogin::parse("@ME"), Err(GithubLoginError::Me));
        assert!(GithubLoginError::Me.to_string().contains("gh api user"));
    }

    /// The character rule is what keeps a login from adding a search qualifier
    /// (`x review-requested:y`, `x:y`) to the adapter's query.
    #[test]
    fn anything_that_could_change_the_search_query_is_refused() {
        for raw in [
            "",
            "-flag",
            "has space",
            "a:b",
            "a/b",
            "a#1",
            "a.b",
            "a\nb",
            "octocat review-requested:someone-else",
            &"a".repeat(MAX_LOGIN_LEN + 1),
        ] {
            assert!(
                matches!(
                    GithubLogin::parse(raw),
                    Err(GithubLoginError::Invalid { .. })
                ),
                "{raw:?} must be refused"
            );
        }
        assert!(GithubLogin::parse(&"a".repeat(MAX_LOGIN_LEN)).is_ok());
    }

    #[test]
    fn deserialising_parses() {
        let login: GithubLogin = serde_json::from_str("\"Octocat\"").unwrap();
        assert_eq!(login.as_str(), "octocat");
        assert!(serde_json::from_str::<GithubLogin>("\"a b\"").is_err());
        assert_eq!(serde_json::to_string(&login).unwrap(), "\"octocat\"");
    }
}
