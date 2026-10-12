//! The `github-review-requests` mode: whose review was newly requested, and on
//! which PRs (design/03).
//!
//! A poll asks GitHub's search for every open PR with a pending review request
//! for the login (`review-requested:<login>`, which includes a request to a team
//! the login is on; [`crate::review_search`] does the asking). This module is the
//! pure half: what the set is and what an entry into it fires.
//!
//! The watch is **edge-triggered on entries into that set**: a PR that appears
//! wakes the subscriber; a PR that leaves (reviewed, closed, request withdrawn)
//! wakes nobody. A re-request after a review is an entry again, and wakes, because
//! "once a requested reviewer submits a review, they are no longer considered a
//! requested reviewer" (GitHub REST docs, "Review requests"). Consistent with
//! that, for the one user measured (ADR-0030), 13 open PRs he had reviewed were all
//! absent from his `review-requested:` search. A re-request reappearing in search
//! has not been observed end to end.
//!
//! # Why a PR is forgotten only after two absent polls
//!
//! If one poll's result were missing PRs that are still requested — a search
//! glitch (guessed, never observed), or a page boundary shifting mid-poll (see
//! [`crate::review_search::collect_pages`]) — forgetting them at once would make
//! the next poll see every one of them "enter" again: a wake per PR for nothing.
//! So a PR missing from one poll is held as `leaving` and forgotten only if the
//! next poll is missing it too. The price: a re-request within one interval of
//! the review does not wake.

use std::collections::{BTreeMap, BTreeSet};

use mailbox_protocol::{GithubPr, Subject};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

/// A PR with a pending review request for the watched login. The PR is the
/// protocol's parsed [`GithubPr`], the same type a `github-pr` watch is keyed by,
/// so `owner/repo#number` is unambiguous (no `/` or `#` inside a segment).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestedPr {
    pub pr: GithubPr,
    /// GitHub's own URL for the PR (correct on GitHub Enterprise too).
    pub url: String,
}

impl RequestedPr {
    /// `owner/repo`.
    pub fn repo(&self) -> String {
        format!("{}/{}", self.pr.owner(), self.pr.repo())
    }

    /// The PR's identity in the baseline: `owner/repo#number`.
    pub fn key(&self) -> String {
        format!("{}#{}", self.repo(), self.pr.number())
    }
}

/// What one poll saw: every open PR currently requesting the login's review.
/// A set by construction, keyed as the baseline is, so the same PR cannot be
/// counted twice however the observation was built.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Observation {
    requested: BTreeMap<String, RequestedPr>,
}

impl Observation {
    pub fn new(prs: impl IntoIterator<Item = RequestedPr>) -> Self {
        Self {
            requested: prs.into_iter().map(|pr| (pr.key(), pr)).collect(),
        }
    }

    /// The PRs, in key order.
    pub fn prs(&self) -> impl Iterator<Item = &RequestedPr> {
        self.requested.values()
    }

    pub fn len(&self) -> usize {
        self.requested.len()
    }

    pub fn contains(&self, key: &str) -> bool {
        self.requested.contains_key(key)
    }

    fn keys(&self) -> BTreeSet<String> {
        self.requested.keys().cloned().collect()
    }
}

/// The persisted set, round-tripped through the bridge (design/01's
/// baseline-via-protocol). At most 1,000 entries per field, because
/// [`crate::review_search`] refuses a search reporting more than that.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Baseline {
    /// PRs the last poll saw requested. Deliberately NOT defaulted: an injected
    /// baseline without it (`{}`, or another mode's) must fail to parse and
    /// re-baseline silently, not read as "nothing known" and wake for every
    /// pending PR.
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

    pub fn knows(&self, key: &str) -> bool {
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
            "repo": self.0.repo(),
            "pr": self.0.pr.number(),
            "url": self.0.url,
        })
    }
}

/// Fold an observation into the baseline: the PRs that entered the set fire,
/// in key order; the PRs that left it fire nothing.
pub fn apply(prior: &Baseline, obs: &Observation) -> (Baseline, Vec<Edge>) {
    let edges = obs
        .prs()
        .filter(|pr| !prior.knows(&pr.key()))
        .cloned()
        .map(Edge)
        .collect();
    let requested = obs.keys();
    // Missing now but seen last poll → leaving. Missing now and already leaving →
    // forgotten (dropped from both sets).
    let leaving = prior
        .requested
        .iter()
        .filter(|key| !requested.contains(*key))
        .cloned()
        .collect();
    (Baseline { requested, leaving }, edges)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pr(repo: &str, number: u64) -> RequestedPr {
        let (owner, name) = repo.split_once('/').unwrap();
        RequestedPr {
            pr: GithubPr::new(owner, name, number).unwrap(),
            url: format!("https://github.com/{repo}/pull/{number}"),
        }
    }

    fn obs(prs: &[RequestedPr]) -> Observation {
        Observation::new(prs.iter().cloned())
    }

    #[test]
    fn an_observation_is_a_set() {
        let twice = obs(&[pr("o/r", 1), pr("o/r", 1), pr("o/r", 2)]);
        assert_eq!(twice.len(), 2);
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
    fn the_baseline_round_trips_and_a_foreign_one_does_not_parse() {
        let (base, _) = apply(
            &Baseline::from_observation(&obs(&[pr("o/r", 1), pr("o/r", 2)])),
            &obs(&[pr("o/r", 1)]),
        );
        let value = serde_json::to_value(&base).unwrap();
        assert_eq!(
            value,
            json!({ "requested": ["o/r#1"], "leaving": ["o/r#2"] })
        );
        assert_eq!(serde_json::from_value::<Baseline>(value).unwrap(), base);
        // Read as "nothing known", these would wake for every pending PR.
        for foreign in [json!({}), json!({ "max_review_id": 3, "ci": "failure" })] {
            assert!(serde_json::from_value::<Baseline>(foreign).is_err());
        }
    }

    /// The subject's link is bounded (ADR-0022); a URL past the bound is dropped,
    /// and the event still names the PR.
    #[test]
    fn an_unlinkable_url_still_gives_a_subject() {
        let mut long = pr("o/r", 5);
        long.url = format!("https://github.com/{}", "x".repeat(600));
        let subject = Edge(long).subject().expect("a subject");
        assert_eq!(subject.text(), "review requested: o/r#5");
        assert_eq!(subject.link(), None);
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
}
