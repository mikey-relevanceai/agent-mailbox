//! The I/O half of the review-request mode: ask GitHub search which open PRs are
//! requesting a login's review, and parse what comes back. The pure half — the
//! set, the diff, the edge — is [`crate::review_requests`], as [`crate::gh`] and
//! [`crate::snapshot`] split the PR mode.
//!
//! # Fail-closed
//!
//! As in the PR mode (review item A), a response that is structurally wrong, or
//! that GitHub marks `incomplete_results`, is a [`GhError::Parse`] — a skipped
//! poll that keeps the baseline — never an empty set. The `leaving` guard in
//! [`crate::review_requests`] absorbs one wrong poll, but two in a row would
//! forget every PR and re-fire them all on the next healthy poll.

use std::collections::BTreeMap;
use std::future::Future;

use mailbox_protocol::{GithubLogin, GithubPr};
use serde_json::Value;
use tracing::debug;

use crate::gh::{Gh, GhError};
use crate::review_requests::{Observation, RequestedPr};

/// The most results GitHub's search API returns for one query, whatever the
/// paging (GitHub REST docs, "Search": "up to 1,000 results for each search").
pub const MAX_SEARCH_RESULTS: u64 = 1_000;

/// Results per page: the search API's maximum. A poll costs one search request
/// per 100 pending requests; the one queue measured (13–14 PRs, ADR-0030) is one.
const PER_PAGE: u64 = 100;

/// The search query. The login's type is what makes splicing it in safe: a
/// [`GithubLogin`] is letters, digits and hyphens, so it cannot add a qualifier.
/// `archived:false` because nobody can review a PR in an archived repo.
fn query(login: &GithubLogin) -> String {
    format!("is:pr is:open archived:false review-requested:{login}")
}

/// A poll client for one login's review requests.
pub struct Client {
    gh: Gh,
    login: GithubLogin,
}

impl Client {
    pub fn new(gh: Gh, login: GithubLogin) -> Self {
        Self { gh, login }
    }

    /// Poll once: every open PR requesting the login's review.
    pub async fn observe(&self) -> Result<Observation, GhError> {
        let query = query(&self.login);
        collect_pages(|page| self.fetch_page(&query, page)).await
    }

    async fn fetch_page(&self, query: &str, page: u64) -> Result<String, GhError> {
        self.gh
            .run(&[
                "api".to_string(),
                "search/issues".to_string(),
                "--method".to_string(),
                "GET".to_string(),
                "-f".to_string(),
                format!("q={query}"),
                "-f".to_string(),
                "sort=created".to_string(),
                "-f".to_string(),
                "order=asc".to_string(),
                "-f".to_string(),
                format!("per_page={PER_PAGE}"),
                "-f".to_string(),
                format!("page={page}"),
            ])
            .await
    }
}

/// Fetch pages from 1 until the distinct PRs collected equal the search's
/// `total_count`, and return them as one observation.
///
/// The pages are separate requests, so the set can change between them. Results
/// are sorted by PR creation time (`sort=created`), which gives a stable order
/// but not an append-only one: a new request on an old PR lands mid-list. Two
/// guards cover it. A change that leaves the count inconsistent (a short page,
/// an overshoot, or the page cap reached first) fails the poll as
/// [`GhError::SetChangedWhilePaging`], a transient skip. A removal and a shift
/// that happen to cancel can pass the count with one PR missing; that PR is then
/// absent for one poll, which [`crate::review_requests`] holds as `leaving`
/// rather than forgetting — so it does not fire when it reappears. (Reasoned from
/// the paging, not observed.)
pub async fn collect_pages<F, Fut>(mut fetch_page: F) -> Result<Observation, GhError>
where
    F: FnMut(u64) -> Fut,
    Fut: Future<Output = Result<String, GhError>>,
{
    // Collected by key so a PR repeated across pages counts once in `seen`.
    let mut requested: BTreeMap<String, RequestedPr> = BTreeMap::new();
    let mut page = 1;
    loop {
        let SearchPage { total_count, items } = parse_search_page(&fetch_page(page).await?)?;
        if total_count > MAX_SEARCH_RESULTS {
            return Err(GhError::SearchTruncated { total_count });
        }
        let got = items.len() as u64;
        for pr in items {
            requested.insert(pr.key(), pr);
        }
        let seen = requested.len() as u64;
        if seen == total_count {
            debug!(total_count, pages = page, "review-request search complete");
            break;
        }
        if got < PER_PAGE || seen > total_count || page * PER_PAGE >= MAX_SEARCH_RESULTS {
            return Err(GhError::SetChangedWhilePaging { seen, total_count });
        }
        page += 1;
    }
    Ok(Observation::new(requested.into_values()))
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
    // other field naming the repo. `GithubPr::new` is the one parser for a PR's
    // identity, here as for a `github-pr` watch.
    let (owner, repo) = item
        .get("repository_url")
        .and_then(Value::as_str)
        .and_then(|url| url.rsplit_once("/repos/"))
        .and_then(|(_, slug)| slug.split_once('/'))
        .ok_or_else(|| {
            GhError::Parse("search item had no `repository_url` naming a repo".into())
        })?;
    let pr = GithubPr::new(owner, repo, number)
        .map_err(|err| GhError::Parse(format!("search item is not a valid PR: {err}")))?;
    Ok(RequestedPr {
        pr,
        url: url.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn item(repo: &str, number: u64) -> Value {
        json!({
            "number": number,
            "html_url": format!("https://github.com/{repo}/pull/{number}"),
            "repository_url": format!("https://api.github.com/repos/{repo}"),
            "pull_request": { "url": "x" },
            "title": "never read",
        })
    }

    fn page_of(numbers: impl IntoIterator<Item = u64>, total_count: u64) -> String {
        let items: Vec<Value> = numbers.into_iter().map(|n| item("o/r", n)).collect();
        json!({ "total_count": total_count, "incomplete_results": false, "items": items })
            .to_string()
    }

    /// Drive [`collect_pages`] over canned pages, recording which were asked for.
    async fn collect(pages: Vec<String>) -> (Result<Observation, GhError>, Vec<u64>) {
        let mut asked = Vec::new();
        let result = collect_pages(|page| {
            asked.push(page);
            let body = pages
                .get(page as usize - 1)
                .cloned()
                .ok_or_else(|| GhError::Failed(format!("no page {page}")));
            std::future::ready(body)
        })
        .await;
        (result, asked)
    }

    fn numbers(obs: &Observation) -> Vec<u64> {
        obs.prs().map(|pr| pr.pr.number()).collect()
    }

    #[test]
    fn the_query_is_open_prs_requesting_the_login_directly_or_by_team() {
        assert_eq!(
            query(&GithubLogin::parse("Octocat").unwrap()),
            "is:pr is:open archived:false review-requested:octocat"
        );
    }

    #[tokio::test]
    async fn a_queue_under_one_page_is_one_request() {
        let (result, asked) = collect(vec![page_of([3, 1, 2], 3)]).await;
        assert_eq!(numbers(&result.unwrap()), [1, 2, 3]);
        assert_eq!(asked, [1]);
    }

    #[tokio::test]
    async fn an_empty_queue_is_an_empty_set_not_an_error() {
        let (result, _) = collect(vec![page_of([], 0)]).await;
        assert_eq!(result.unwrap().len(), 0);
    }

    #[tokio::test]
    async fn a_long_queue_is_paged_into_one_observation() {
        let (result, asked) = collect(vec![
            page_of(1..=100, 250),
            page_of(101..=200, 250),
            page_of(201..=250, 250),
        ])
        .await;
        assert_eq!(result.unwrap().len(), 250);
        assert_eq!(asked, [1, 2, 3]);
    }

    /// Search cannot return more than 1,000, so the set cannot be told apart
    /// from churn: fatal, so the watch shows `failed`.
    #[tokio::test]
    async fn more_than_search_can_return_is_fatal() {
        let (result, asked) = collect(vec![page_of(1..=100, 1_001)]).await;
        assert!(matches!(
            result,
            Err(GhError::SearchTruncated { total_count: 1_001 })
        ));
        assert_eq!(asked, [1], "no point paging a set that cannot be complete");
    }

    /// A request withdrawn between page 1 and page 2 shortens page 2 below the
    /// count page 1 reported.
    #[tokio::test]
    async fn a_short_page_before_the_count_is_reached_skips_the_poll() {
        let (result, _) = collect(vec![page_of(1..=100, 150), page_of(101..=148, 149)]).await;
        assert!(matches!(
            result,
            Err(GhError::SetChangedWhilePaging {
                seen: 148,
                total_count: 149
            })
        ));
    }

    #[tokio::test]
    async fn more_distinct_prs_than_the_count_skips_the_poll() {
        let (result, _) = collect(vec![page_of(1..=100, 101), page_of(200..=201, 101)]).await;
        assert!(matches!(
            result,
            Err(GhError::SetChangedWhilePaging {
                seen: 102,
                total_count: 101
            })
        ));
    }

    /// Full pages of duplicates never reach the count; the 10-page cap stops the
    /// loop rather than paging past what search will return.
    #[tokio::test]
    async fn paging_stops_at_the_search_cap() {
        let pages = vec![page_of(1..=100, 1_000); 12];
        let (result, asked) = collect(pages).await;
        assert!(matches!(result, Err(GhError::SetChangedWhilePaging { .. })));
        assert_eq!(asked.len(), 10);
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
        assert_eq!(
            page.items,
            [RequestedPr {
                pr: GithubPr::new("RelevanceAI", "arg", 7353).unwrap(),
                url: "https://github.com/RelevanceAI/arg/pull/7353".to_string(),
            }]
        );
    }

    #[test]
    fn a_partial_or_malformed_page_is_transient_never_empty() {
        let mut no_repo = item("o/r", 1);
        no_repo["repository_url"] = json!("https://api.github.com/repos/");
        let mut zero = item("o/r", 1);
        zero["number"] = json!(0);
        let mut nested = item("o/r", 1);
        nested["repository_url"] = json!("https://api.github.com/repos/o/r/x");
        let mut an_issue = item("o/r", 1);
        an_issue.as_object_mut().unwrap().remove("pull_request");
        for body in [
            json!({ "total_count": 1, "incomplete_results": true, "items": [item("o/r", 1)] }),
            json!({ "total_count": 1, "items": [item("o/r", 1)] }),
            json!({ "incomplete_results": false, "items": [] }),
            json!({ "total_count": 0, "incomplete_results": false }),
            json!({ "total_count": 1, "incomplete_results": false, "items": [no_repo] }),
            json!({ "total_count": 1, "incomplete_results": false, "items": [an_issue] }),
            json!({ "total_count": 1, "incomplete_results": false, "items": [zero] }),
            json!({ "total_count": 1, "incomplete_results": false, "items": [nested] }),
        ] {
            assert!(
                matches!(parse_search_page(&body.to_string()), Err(GhError::Parse(_))),
                "{body} must be a parse error"
            );
        }
    }
}
