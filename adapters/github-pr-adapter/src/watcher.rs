//! What the poll loop needs from a mode: how to observe, how to diff, and how to
//! describe what fired. The loop itself — backoff, transient skips, baseline
//! persistence, signals — is the same for both GitHub watch kinds (ADR-0030).

use std::future::Future;

use mailbox_protocol::Subject;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use tracing::{debug, info};

use crate::gh::{GhClient, GhError};
use crate::review_requests;
use crate::review_search;
use crate::snapshot;

/// One mode of the adapter. Every method but [`Watcher::observe`] is
/// deterministic and free of side effects other than logging, so call each
/// once per poll.
pub trait Watcher {
    type Observation;
    /// Round-tripped through the bridge, so it must serialise.
    type Baseline: Serialize + DeserializeOwned + PartialEq;
    type Edge;

    fn observe(&self) -> impl Future<Output = Result<Self::Observation, GhError>>;
    /// The first poll's baseline. Nothing fires on it.
    fn first_baseline(&self, obs: &Self::Observation) -> Self::Baseline;
    fn apply(
        &self,
        prior: &Self::Baseline,
        obs: &Self::Observation,
    ) -> (Self::Baseline, Vec<Self::Edge>);
    fn edge_kind(&self, edge: &Self::Edge) -> &'static str;
    fn describe(&self, edge: &Self::Edge) -> String;
    fn body(&self, edge: &Self::Edge) -> Value;
    fn subject(&self, edge: &Self::Edge, obs: &Self::Observation) -> Option<Subject>;
}

/// The `github-pr` mode: one pull request's transitions (card 10). The client
/// is the one record of which PR, so what is polled and what is published about
/// cannot disagree.
pub struct PrWatcher {
    client: GhClient,
}

impl PrWatcher {
    pub fn new(client: GhClient) -> Self {
        Self { client }
    }

    /// `owner/repo`.
    pub fn repo(&self) -> String {
        self.client.slug()
    }

    pub fn number(&self) -> u64 {
        self.client.number()
    }
}

impl Watcher for PrWatcher {
    type Observation = snapshot::Observation;
    type Baseline = snapshot::Baseline;
    type Edge = snapshot::Edge;

    fn observe(&self) -> impl Future<Output = Result<Self::Observation, GhError>> {
        self.client.observe()
    }

    fn first_baseline(&self, obs: &Self::Observation) -> Self::Baseline {
        snapshot::Baseline::from_observation(obs)
    }

    fn apply(
        &self,
        prior: &Self::Baseline,
        obs: &Self::Observation,
    ) -> (Self::Baseline, Vec<Self::Edge>) {
        snapshot::apply(prior, obs)
    }

    fn edge_kind(&self, edge: &Self::Edge) -> &'static str {
        edge.kind()
    }

    fn describe(&self, edge: &Self::Edge) -> String {
        edge.describe()
    }

    fn body(&self, edge: &Self::Edge) -> Value {
        edge.body(&self.repo(), self.number())
    }

    fn subject(&self, edge: &Self::Edge, obs: &Self::Observation) -> Option<Subject> {
        edge.subject(obs.pr_url.as_deref())
    }
}

/// The `github-review-requests` mode: PRs newly asking one login for a review
/// (design/03).
pub struct ReviewRequestsWatcher {
    client: review_search::Client,
}

impl ReviewRequestsWatcher {
    pub fn new(client: review_search::Client) -> Self {
        Self { client }
    }
}

impl Watcher for ReviewRequestsWatcher {
    type Observation = review_requests::Observation;
    type Baseline = review_requests::Baseline;
    type Edge = review_requests::Edge;

    fn observe(&self) -> impl Future<Output = Result<Self::Observation, GhError>> {
        self.client.observe()
    }

    fn first_baseline(&self, obs: &Self::Observation) -> Self::Baseline {
        info!(
            pending = obs.len(),
            "recorded the review requests already pending; none of them will wake"
        );
        review_requests::Baseline::from_observation(obs)
    }

    /// Logs the counts behind the diff, so "why did this request not wake me?"
    /// can be told apart: never seen, already known, or held as leaving.
    fn apply(
        &self,
        prior: &Self::Baseline,
        obs: &Self::Observation,
    ) -> (Self::Baseline, Vec<Self::Edge>) {
        let (next, edges) = review_requests::apply(prior, obs);
        let already_known = obs.prs().filter(|pr| prior.knows(&pr.key())).count();
        let back_from_leaving = prior.leaving.iter().filter(|key| obs.contains(key)).count();
        let forgotten = prior
            .leaving
            .iter()
            .filter(|key| !next.requested.contains(*key))
            .count();
        debug!(
            observed = obs.len(),
            entered = edges.len(),
            already_known,
            back_from_leaving,
            now_leaving = next.leaving.len(),
            forgotten,
            "diffed the review-request set"
        );
        (next, edges)
    }

    fn edge_kind(&self, edge: &Self::Edge) -> &'static str {
        edge.kind()
    }

    fn describe(&self, edge: &Self::Edge) -> String {
        edge.describe()
    }

    fn body(&self, edge: &Self::Edge) -> Value {
        edge.body()
    }

    fn subject(&self, edge: &Self::Edge, _obs: &Self::Observation) -> Option<Subject> {
        edge.subject()
    }
}
