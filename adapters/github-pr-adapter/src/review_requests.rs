//! The `github-review-requests` mode: whose review was newly requested, and on
//! which PRs (design/03).
//!
//! A poll asks GitHub's search for every open PR with a pending review request
//! for the login (`review-requested:<login>`, which includes a request to a team
//! the login is on). The watch is **edge-triggered on entries into that set**: a
//! PR that appears wakes the subscriber; a PR that leaves (reviewed, closed,
//! request withdrawn) wakes nobody. GitHub drops a reviewer from the pending set
//! when they review, so a re-request after a review is an entry again, and wakes.
//!
//! # Why a PR is forgotten only after two absent polls
//!
//! Search is an index, not the PR's own record. If one poll's result were
//! missing PRs that are still requested, forgetting them at once would make the
//! next poll see every one of them "enter" again — a wake per PR for nothing.
//! We have not seen that happen (unmeasured), but its cost is a burst of turns,
//! so a PR missing from one poll is held as `leaving` and forgotten only if the
//! next poll is missing it too. The price: a re-request within one interval of
//! the review does not wake.
//!
//! # Fail-closed parsing
//!
//! As in the PR mode (review item A), a response that is structurally wrong, or
//! that GitHub marks `incomplete_results`, is a [`GhError::Parse`] — a skipped
//! poll that keeps the baseline — never an empty set, which would forget every
//! PR and re-fire them all on the next healthy poll.

use std::collections::{BTreeMap, BTreeSet};

use mailbox_protocol::Subject;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::gh::{Gh, GhError};

/// The most results GitHub's search API returns for one query, whatever the
/// paging (GitHub REST docs, "Search": "up to 1,000 results for each search").
const MAX_SEARCH_RESULTS: u64 = 1_000;

/// Results per page: the search API's maximum, so a typical queue is one request.
const PER_PAGE: u64 = 100;

/// A PR with a pending review request for the watched login.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestedPr {
    /// `owner/repo`.
    pub repo: String,
    pub number: u64,
    /// GitHub's own URL for the PR (correct on GitHub Enterprise too).
    pub url: String,
}

impl RequestedPr {
    /// The PR's identity in the baseline: `owner/repo#number`.
    fn key(&self) -> String {
        format!("{}#{}", self.repo, self.number)
    }
}

/// What one poll saw: every open PR currently requesting the login's review.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Observation {
    pub requested: Vec<RequestedPr>,
}

impl Observation {
    fn keys(&self) -> BTreeSet<String> {
        self.requested.iter().map(RequestedPr::key).collect()
    }
}

/// The persisted set, round-tripped through the bridge (design/01's
/// baseline-via-protocol). Bounded by [`MAX_SEARCH_RESULTS`] entries per field,
/// because nothing enters it that a search did not return.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Baseline {
    /// PRs the last poll saw requested.
    #[serde(default)]
    pub requested: BTreeSet<String>,
    /// PRs known to be requested until one poll ago, and missing from the last.
    /// Still known: reappearing does not wake. Forgotten if the next poll is
    /// missing them too.
    #[serde(default)]
    pub leaving: BTreeSet<String>,
}

impl Baseline {
    /// The first poll's baseline: everything already pending is known, so the
    /// requests that predate the watch never wake anyone.
    pub fn from_observation(obs: &Observation) -> Self {
        Baseline {
            requested: obs.keys(),
            leaving: BTreeSet::new(),
        }
    }

    fn knows(&self, key: &str) -> bool {
        self.requested.contains(key) || self.leaving.contains(key)
    }
}

/// A review request that newly appeared.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Edge(pub RequestedPr);

impl Edge {
    pub fn kind(&self) -> &'static str {
        "review_requested"
    }

    pub fn describe(&self) -> String {
        self.0.key()
    }

    /// `review requested: owner/repo#123`, linking to the PR. It names the PR and
    /// never quotes it: no title, no author, no body (ADR-0022) — the agent reads
    /// those from GitHub.
    pub fn subject(&self) -> Option<Subject> {
        Subject::new(
            &format!("review requested: {}", self.0.key()),
            Some(&self.0.url),
        )
        .ok()
    }

    pub fn body(&self) -> Value {
        json!({
            "source": "github-review-requests",
            "edge": self.kind(),
            "repo": self.0.repo,
            "pr": self.0.number,
            "url": self.0.url,
        })
    }
}

/// Fold an observation into the baseline: the PRs that entered the set fire,
/// in key order; the PRs that left it fire nothing.
pub fn apply(prior: &Baseline, obs: &Observation) -> (Baseline, Vec<Edge>) {
    let mut entered: BTreeMap<String, &RequestedPr> = BTreeMap::new();
    for pr in &obs.requested {
        let key = pr.key();
        if !prior.knows(&key) {
            entered.insert(key, pr);
        }
    }
    let requested = obs.keys();
    // Missing now but seen last poll → leaving. Missing now and already leaving →
    // forgotten (dropped from both sets).
    let leaving = prior
        .requested
        .iter()
        .filter(|key| !requested.contains(*key))
        .cloned()
        .collect();
    let edges = entered.into_values().cloned().map(Edge).collect();
    (Baseline { requested, leaving }, edges)
}

/// The search query. `login` is a validated GitHub login (letters, digits,
/// hyphens — `mailbox_protocol::GithubLogin`), so it cannot add a qualifier.
/// `archived:false` because nobody can review a PR in an archived repo.
fn query(login: &str) -> String {
    format!("is:pr is:open archived:false review-requested:{login}")
}

/// A poll client for one login's review requests.
pub struct Client {
    gh: Gh,
    login: String,
}

impl Client {
    pub fn new(gh: Gh, login: impl Into<String>) -> Self {
        Self {
            gh,
            login: login.into(),
        }
    }

    pub fn login(&self) -> &str {
        &self.login
    }

    /// Poll once: one search request per 100 requested PRs (so, typically, one).
    ///
    /// Pages are sorted oldest-first so a request that lands mid-poll joins the
    /// last page instead of shifting earlier results across a page boundary. A
    /// set that still changed between pages fails the count check below and the
    /// poll is skipped, rather than diffing a set that never existed.
    pub async fn observe(&self) -> Result<Observation, GhError> {
        let mut requested: BTreeMap<String, RequestedPr> = BTreeMap::new();
        let mut page = 1;
        loop {
            let body = self
                .gh
                .run(&[
                    "api".to_string(),
                    "search/issues".to_string(),
                    "--method".to_string(),
                    "GET".to_string(),
                    "-f".to_string(),
                    format!("q={}", query(&self.login)),
                    "-f".to_string(),
                    "sort=created".to_string(),
                    "-f".to_string(),
                    "order=asc".to_string(),
                    "-f".to_string(),
                    format!("per_page={PER_PAGE}"),
                    "-f".to_string(),
                    format!("page={page}"),
                ])
                .await?;
            let page_result = parse_search_page(&body)?;
            if page_result.total_count > MAX_SEARCH_RESULTS {
                // Search cannot return the whole set, so entries and exits cannot
                // be told apart. Fatal, so `mailbox status` shows the watch failed
                // instead of it waking on PRs that rotate in and out of view.
                return Err(GhError::Failed(format!(
                    "{} open review requests for {}; GitHub search returns at most {MAX_SEARCH_RESULTS}",
                    page_result.total_count, self.login
                )));
            }
            let got = page_result.items.len() as u64;
            for pr in page_result.items {
                requested.insert(pr.key(), pr);
            }
            let seen = requested.len() as u64;
            if seen == page_result.total_count {
                break;
            }
            if got < PER_PAGE
                || seen > page_result.total_count
                || page * PER_PAGE >= MAX_SEARCH_RESULTS
            {
                return Err(GhError::Parse(format!(
                    "the review-request set changed while paging: {seen} PRs seen, search reported {}",
                    page_result.total_count
                )));
            }
            page += 1;
        }
        Ok(Observation {
            requested: requested.into_values().collect(),
        })
    }
}

/// One page of a `search/issues` response.
#[derive(Debug)]
struct SearchPage {
    total_count: u64,
    items: Vec<RequestedPr>,
}

/// Parse one page, fail-closed: a missing field, a non-PR item, or
/// `incomplete_results: true` (GitHub's "the search timed out, these are
/// partial") is a [`GhError::Parse`].
fn parse_search_page(body: &str) -> Result<SearchPage, GhError> {
    let value: Value = serde_json::from_str(body)
        .map_err(|err| GhError::Parse(format!("search response was not JSON: {err}")))?;
    let total_count = value
        .get("total_count")
        .and_then(Value::as_u64)
        .ok_or_else(|| GhError::Parse("search response had no integer `total_count`".into()))?;
    match value.get("incomplete_results").and_then(Value::as_bool) {
        Some(false) => {}
        Some(true) => {
            return Err(GhError::Parse(
                "search reported incomplete_results; skipping a partial set".into(),
            ));
        }
        None => {
            return Err(GhError::Parse(
                "search response had no boolean `incomplete_results`".into(),
            ));
        }
    }
    let items = value
        .get("items")
        .and_then(Value::as_array)
        .ok_or_else(|| GhError::Parse("search response `items` was not an array".into()))?;
    let items = items
        .iter()
        .map(parse_item)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(SearchPage { total_count, items })
}

fn parse_item(item: &Value) -> Result<RequestedPr, GhError> {
    if item.get("pull_request").is_none() {
        return Err(GhError::Parse("search item is not a pull request".into()));
    }
    let number = item
        .get("number")
        .and_then(Value::as_u64)
        .ok_or_else(|| GhError::Parse("search item had no integer `number`".into()))?;
    let url = item
        .get("html_url")
        .and_then(Value::as_str)
        .filter(|url| !url.is_empty())
        .ok_or_else(|| GhError::Parse("search item had no `html_url`".into()))?;
    // `https://api.github.com/repos/<owner>/<repo>`; the search item carries no
    // other field naming the repo.
    let repo = item
        .get("repository_url")
        .and_then(Value::as_str)
        .and_then(|url| url.rsplit_once("/repos/"))
        .map(|(_, slug)| slug)
        .filter(|slug| {
            slug.split_once('/')
                .is_some_and(|(owner, repo)| !owner.is_empty() && !repo.is_empty())
        })
        .ok_or_else(|| {
            GhError::Parse("search item had no `repository_url` naming a repo".into())
        })?;
    Ok(RequestedPr {
        repo: repo.to_string(),
        number,
        url: url.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pr(repo: &str, number: u64) -> RequestedPr {
        RequestedPr {
            repo: repo.to_string(),
            number,
            url: format!("https://github.com/{repo}/pull/{number}"),
        }
    }

    fn obs(prs: &[RequestedPr]) -> Observation {
        Observation {
            requested: prs.to_vec(),
        }
    }

    fn fired(edges: &[Edge]) -> Vec<String> {
        edges.iter().map(Edge::describe).collect()
    }

    #[test]
    fn the_first_poll_knows_everything_already_pending() {
        let base = Baseline::from_observation(&obs(&[pr("o/r", 1), pr("o/r", 2)]));
        let (_, edges) = apply(&base, &obs(&[pr("o/r", 1), pr("o/r", 2)]));
        assert!(edges.is_empty());
    }

    #[test]
    fn a_new_request_fires_once() {
        let base = Baseline::from_observation(&obs(&[pr("o/r", 1)]));
        let (next, edges) = apply(&base, &obs(&[pr("o/r", 1), pr("o/r", 2)]));
        assert_eq!(fired(&edges), ["o/r#2"]);
        let (_, again) = apply(&next, &obs(&[pr("o/r", 1), pr("o/r", 2)]));
        assert!(again.is_empty(), "a stable set fires nothing");
    }

    #[test]
    fn leaving_the_set_fires_nothing() {
        let base = Baseline::from_observation(&obs(&[pr("o/r", 1)]));
        let (next, edges) = apply(&base, &obs(&[]));
        assert!(edges.is_empty());
        let (_, edges) = apply(&next, &obs(&[]));
        assert!(edges.is_empty());
    }

    /// The re-request case: reviewed (gone for two polls, so forgotten), then
    /// asked again.
    #[test]
    fn a_re_request_after_the_pr_was_forgotten_fires() {
        let base = Baseline::from_observation(&obs(&[pr("o/r", 1)]));
        let (base, _) = apply(&base, &obs(&[]));
        let (base, _) = apply(&base, &obs(&[]));
        assert_eq!(
            base,
            Baseline::default(),
            "forgotten after two absent polls"
        );
        let (_, edges) = apply(&base, &obs(&[pr("o/r", 1)]));
        assert_eq!(fired(&edges), ["o/r#1"]);
    }

    /// One poll missing PRs that are still requested must not make them all
    /// "enter" again on the next.
    #[test]
    fn a_pr_missing_from_one_poll_does_not_fire_when_it_reappears() {
        let all = [pr("o/r", 1), pr("o/r", 2), pr("o/r", 3)];
        let base = Baseline::from_observation(&obs(&all));
        let (blip, edges) = apply(&base, &obs(&[]));
        assert!(edges.is_empty());
        let (after, edges) = apply(&blip, &obs(&all));
        assert!(edges.is_empty(), "fired {:?}", fired(&edges));
        assert_eq!(after, base);
    }

    #[test]
    fn edges_fire_in_key_order_whatever_order_search_returned() {
        let (_, edges) = apply(
            &Baseline::default(),
            &obs(&[pr("z/z", 1), pr("a/a", 9), pr("a/a", 10)]),
        );
        assert_eq!(fired(&edges), ["a/a#10", "a/a#9", "z/z#1"]);
    }

    #[test]
    fn the_subject_names_the_pr_and_links_to_it() {
        let subject = Edge(pr("RelevanceAI/arg", 7353)).subject().unwrap();
        assert_eq!(subject.text(), "review requested: RelevanceAI/arg#7353");
        assert_eq!(
            subject.link(),
            Some("https://github.com/RelevanceAI/arg/pull/7353")
        );
    }

    #[test]
    fn the_body_points_and_never_quotes() {
        let body = Edge(pr("o/r", 5)).body();
        assert_eq!(
            body,
            json!({
                "source": "github-review-requests",
                "edge": "review_requested",
                "repo": "o/r",
                "pr": 5,
                "url": "https://github.com/o/r/pull/5",
            })
        );
    }

    #[test]
    fn the_query_is_open_prs_requesting_the_login_directly_or_by_team() {
        assert_eq!(
            query("octocat"),
            "is:pr is:open archived:false review-requested:octocat"
        );
    }

    fn item(repo: &str, number: u64) -> Value {
        json!({
            "number": number,
            "html_url": format!("https://github.com/{repo}/pull/{number}"),
            "repository_url": format!("https://api.github.com/repos/{repo}"),
            "pull_request": { "url": "x" },
            "title": "never read",
        })
    }

    #[test]
    fn parses_a_search_page() {
        let body = json!({
            "total_count": 1,
            "incomplete_results": false,
            "items": [item("RelevanceAI/arg", 7353)],
        });
        let page = parse_search_page(&body.to_string()).unwrap();
        assert_eq!(page.total_count, 1);
        assert_eq!(page.items, [pr("RelevanceAI/arg", 7353)]);
    }

    #[test]
    fn a_partial_or_malformed_page_is_transient_never_empty() {
        let mut no_repo = item("o/r", 1);
        no_repo["repository_url"] = json!("https://api.github.com/repos/");
        let mut an_issue = item("o/r", 1);
        an_issue.as_object_mut().unwrap().remove("pull_request");
        for body in [
            json!({ "total_count": 1, "incomplete_results": true, "items": [item("o/r", 1)] }),
            json!({ "total_count": 1, "items": [item("o/r", 1)] }),
            json!({ "incomplete_results": false, "items": [] }),
            json!({ "total_count": 0, "incomplete_results": false }),
            json!({ "total_count": 1, "incomplete_results": false, "items": [no_repo] }),
            json!({ "total_count": 1, "incomplete_results": false, "items": [an_issue] }),
        ] {
            assert!(
                matches!(parse_search_page(&body.to_string()), Err(GhError::Parse(_))),
                "{body} must be a parse error"
            );
        }
    }
}
