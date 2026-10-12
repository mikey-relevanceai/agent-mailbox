//! What the poll loop needs from a mode: how to observe, how to diff, and how to
//! describe what fired. The loop itself — backoff, transient skips, baseline
//! persistence, signals — is the same for both GitHub watch kinds (ADR-0030).

use std::future::Future;

use mailbox_protocol::Subject;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::gh::{GhClient, GhError};
use crate::review_requests;
use crate::snapshot;

/// One mode of the adapter. Every method but [`Watcher::observe`] is pure.
pub trait Watcher {
    type Observation;
    /// Round-tripped through the bridge, so it must serialise.
    type Baseline: Serialize + DeserializeOwned + PartialEq;
    type Edge;

    /// What this process watches, for its logs.
    fn label(&self) -> String;
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

/// The `github-pr` mode: one pull request's transitions (card 10).
pub struct PrWatcher {
    client: GhClient,
    /// `owner/repo`.
    repo: String,
    number: u64,
}

impl PrWatcher {
    pub fn new(client: GhClient, owner: String, repo: String) -> Self {
        let number = client.number();
        Self {
            client,
            repo: format!("{owner}/{repo}"),
            number,
        }
    }
}

impl Watcher for PrWatcher {
    type Observation = snapshot::Observation;
    type Baseline = snapshot::Baseline;
    type Edge = snapshot::Edge;

    fn label(&self) -> String {
        format!("{}#{}", self.repo, self.number)
    }

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
        edge.body(&self.repo, self.number)
    }

    fn subject(&self, edge: &Self::Edge, obs: &Self::Observation) -> Option<Subject> {
        edge.subject(obs.pr_url.as_deref())
    }
}

/// The `github-review-requests` mode: PRs newly asking one login for a review
/// (design/03).
pub struct ReviewRequestsWatcher {
    client: review_requests::Client,
}

impl ReviewRequestsWatcher {
    pub fn new(client: review_requests::Client) -> Self {
        Self { client }
    }
}

impl Watcher for ReviewRequestsWatcher {
    type Observation = review_requests::Observation;
    type Baseline = review_requests::Baseline;
    type Edge = review_requests::Edge;

    fn label(&self) -> String {
        format!("review requests for {}", self.client.login())
    }

    fn observe(&self) -> impl Future<Output = Result<Self::Observation, GhError>> {
        self.client.observe()
    }

    fn first_baseline(&self, obs: &Self::Observation) -> Self::Baseline {
        review_requests::Baseline::from_observation(obs)
    }

    fn apply(
        &self,
        prior: &Self::Baseline,
        obs: &Self::Observation,
    ) -> (Self::Baseline, Vec<Self::Edge>) {
        review_requests::apply(prior, obs)
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
