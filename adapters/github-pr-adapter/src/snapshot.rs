//! The typed baseline snapshot and the pure poll→diff logic.
//!
//! This module is deliberately free of any I/O (no `gh`, no stdout, no tokio):
//! it is the edge-detection core, so the interesting behaviour — baseline on the
//! first poll, fire on transitions only, the UNKNOWN-ignore rule, the monotonic
//! id-cursor diff, the CI-into-failure rule — is unit-testable as pure functions
//! off recorded observations.
//!
//! # What is an edge
//!
//! An agent cares about *transitions*, not levels. A [`Baseline`] is the last-seen
//! level of every signal we track; an [`Observation`] is what a poll saw now;
//! [`apply`] folds the observation into the baseline and returns both the new
//! baseline and the [`Edge`]s that fired. The first poll has no prior baseline, so
//! it only baselines (via [`Baseline::from_observation`]) and fires nothing.
//!
//! # Why id cursors, not counts (review item B)
//!
//! Reviews / comments / review-threads are diffed by a **monotonic highest-seen
//! id cursor**, not a bare count. Counts are not monotone: deleting comment A and
//! adding comment B in one interval keeps the count at 5, so B would be MISSED;
//! and a 5→3→5 flap would fire a false "new reviews". A max-id cursor detects a
//! genuine new item (a strictly higher id) even when a deletion cancels the count,
//! and never false-fires on a pure decrease (the cursor never goes down — we keep
//! the prior max). It also keeps the baseline tiny (one integer per signal),
//! unlike a seen-id *set* which would grow unbounded or, if capped, silently
//! re-fire old items that rotated out.

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

/// Hard cap on the number of failed-check names carried in a baseline / CI event
/// (review item F): a pathological PR with thousands of checks must not grow the
/// persisted baseline (and therefore the re-injected config line) unbounded.
pub const MAX_FAILED_CHECKS: usize = 256;

/// The mergeable states we *remember*. Deliberately excludes UNKNOWN: GitHub
/// reports UNKNOWN transiently while it recomputes mergeability, so baselining to
/// it (or firing on it) would spew spurious conflict edges every time it flaps.
/// We only ever store the last *known* state (see [`MergeableObserved`] for the
/// three-state value a poll actually observes).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Mergeable {
    Mergeable,
    Conflicting,
}

/// Mergeability as a single poll observed it — including the transient `Unknown`
/// that [`apply`] refuses to baseline to or fire on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeableObserved {
    Mergeable,
    Conflicting,
    Unknown,
}

impl MergeableObserved {
    /// The known state this observation represents, or `None` for `Unknown`.
    fn known(self) -> Option<Mergeable> {
        match self {
            MergeableObserved::Mergeable => Some(Mergeable::Mergeable),
            MergeableObserved::Conflicting => Some(Mergeable::Conflicting),
            MergeableObserved::Unknown => None,
        }
    }
}

/// The PR lifecycle state a single poll observed. Only `Merged` drives an edge
/// today; the others are carried so the merge check is explicit and a future
/// closed-without-merge edge has an obvious home. An unmodelled state string
/// (a value GitHub adds later) folds to `Other`, which never fires — the same
/// fail-safe stance as `mergeable`'s `Unknown`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrStateObserved {
    Open,
    Closed,
    Merged,
    Other,
}

/// Whole-PR CI rollup (card-10 decision 3). We fire on the ROLLUP transition, not
/// per individual check, and carry the newly-failed check *names* in the event
/// body — so an agent learns "CI went red, because of build+test" from one edge
/// rather than a storm of per-check events.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CiRollup {
    /// No checks configured / reported on the PR.
    #[default]
    None,
    /// At least one check still running, none failed yet.
    Pending,
    /// All checks completed successfully.
    Success,
    /// At least one check concluded in failure.
    Failure,
}

/// The adapter's persisted baseline snapshot — the last-seen level of every
/// tracked signal. Opaque to the bridge (it just stores the JSON); the adapter
/// owns this schema. Serialized into a `mailbox_protocol::Baseline` message after
/// each poll that changed it, and injected back via config on the next spawn so a
/// restart resumes edge-detection from here (design/01: baseline persists so a
/// restart does not re-fire).
///
/// Every field is `#[serde(default)]` so a snapshot persisted by an older adapter
/// version (missing a field) still deserializes — the missing signal simply reads
/// as "nothing seen yet" and re-baselines on the next poll.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Baseline {
    /// Whether the PR has been observed merged. Monotonic and terminal: once a
    /// poll sees `MERGED` this stays `true`, so the merge edge fires exactly once
    /// and a restart from this baseline never re-fires it.
    #[serde(default)]
    pub merged: bool,
    /// Last KNOWN mergeable state, or `None` if never yet known (UNKNOWN-only so
    /// far). Never `Unknown` — that is the UNKNOWN-ignore rule.
    #[serde(default)]
    pub mergeable: Option<Mergeable>,
    /// Highest review id seen (monotonic cursor). A strictly higher id next poll
    /// means a new review.
    #[serde(default)]
    pub max_review_id: u64,
    /// Highest review-thread (inline review comment) id seen.
    #[serde(default)]
    pub max_review_thread_id: u64,
    /// Highest PR-level (issue) comment id seen.
    #[serde(default)]
    pub max_comment_id: u64,
    /// Whole-PR CI rollup last seen.
    #[serde(default)]
    pub ci: CiRollup,
    /// Names of the checks currently failing, so a later poll reports only the
    /// *newly* failed ones. Bounded to [`MAX_FAILED_CHECKS`].
    #[serde(default)]
    pub failed_checks: Vec<String>,
}

impl Baseline {
    /// Build the first baseline directly from an observation — no edges fire on
    /// the first poll. Applies the UNKNOWN-ignore rule: a first poll that sees
    /// UNKNOWN mergeability baselines to `None` (not yet known), never to UNKNOWN.
    pub fn from_observation(obs: &Observation) -> Self {
        Baseline {
            merged: obs.state == PrStateObserved::Merged,
            mergeable: obs.mergeable.known(),
            max_review_id: obs.max_review_id,
            max_review_thread_id: obs.max_review_thread_id,
            max_comment_id: obs.max_comment_id,
            ci: obs.ci,
            failed_checks: capped(obs.failed_checks.clone()),
        }
    }
}

/// Truncate a failed-check list to [`MAX_FAILED_CHECKS`] so a baseline can never
/// grow unbounded (review item F).
fn capped(mut checks: Vec<String>) -> Vec<String> {
    checks.truncate(MAX_FAILED_CHECKS);
    checks
}

/// What a single poll observed. The un-remembered counterpart of [`Baseline`]:
/// mergeability is the three-state [`MergeableObserved`] (it can be `Unknown`),
/// and the id fields are the *max id seen this poll* (not a running cursor).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Observation {
    pub state: PrStateObserved,
    pub mergeable: MergeableObserved,
    pub max_review_id: u64,
    pub max_review_thread_id: u64,
    pub max_comment_id: u64,
    pub ci: CiRollup,
    pub failed_checks: Vec<String>,
}

/// A transition worth waking an agent for. Each variant maps to exactly one
/// published event; the bodies are small opaque content (never a full gh dump).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Edge {
    /// The PR was merged (observed `MERGED` for the first time). Terminal: fires
    /// exactly once, never re-fires from a restart, and is not undone.
    Merged,
    /// Mergeability transitioned into CONFLICTING (from a known non-conflicting
    /// state, or from never-known). Never fired off a transient UNKNOWN.
    Conflicting,
    /// A new review appeared (highest review id increased).
    NewReviews { from: u64, to: u64 },
    /// A new review thread (inline review comment) appeared.
    NewReviewThreads { from: u64, to: u64 },
    /// A new PR-level comment appeared.
    NewComments { from: u64, to: u64 },
    /// CI transitioned into failure (from success/pending/none), or gained new
    /// failing checks while already in failure. Never fired on a transition to
    /// pending or success (review item H).
    CiFailure {
        from: CiRollup,
        newly_failed: Vec<String>,
    },
}

impl Edge {
    /// A stable machine label for the edge (goes in the body's `edge` field and
    /// in logs).
    pub fn kind(&self) -> &'static str {
        match self {
            Edge::Merged => "pr_merged",
            Edge::Conflicting => "mergeable_conflicting",
            Edge::NewReviews { .. } => "new_reviews",
            Edge::NewReviewThreads { .. } => "new_review_threads",
            Edge::NewComments { .. } => "new_comments",
            Edge::CiFailure { .. } => "ci_failure",
        }
    }

    /// A concise, structured description carrying the decision data — used for the
    /// per-edge `info!` log (review item J) without dumping gh JSON.
    pub fn describe(&self) -> String {
        match self {
            Edge::Merged => "pr merged".to_string(),
            Edge::Conflicting => "mergeable→conflicting".to_string(),
            Edge::NewReviews { from, to } => format!("reviews max id {from}→{to}"),
            Edge::NewReviewThreads { from, to } => format!("review-thread max id {from}→{to}"),
            Edge::NewComments { from, to } => format!("comment max id {from}→{to}"),
            Edge::CiFailure { from, newly_failed } => {
                format!("ci {from:?}→failure newly_failed={newly_failed:?}")
            }
        }
    }

    /// The opaque event body for this edge. Small by design (a label + the deltas
    /// that fired it) — an event body is opaque content and must never be a large
    /// gh dump (ADR-0001).
    pub fn body(&self, repo: &str, pr: u64) -> Value {
        let mut body =
            json!({ "source": "github-pr", "edge": self.kind(), "repo": repo, "pr": pr });
        let object = body
            .as_object_mut()
            .expect("json! object literal is always an object");
        match self {
            Edge::Merged | Edge::Conflicting => {}
            Edge::NewReviews { from, to }
            | Edge::NewReviewThreads { from, to }
            | Edge::NewComments { from, to } => {
                object.insert("previous_max_id".to_string(), json!(from));
                object.insert("current_max_id".to_string(), json!(to));
            }
            Edge::CiFailure { from, newly_failed } => {
                object.insert("previous".to_string(), json!(from));
                object.insert("rollup".to_string(), json!(CiRollup::Failure));
                object.insert("newly_failed".to_string(), json!(newly_failed));
            }
        }
        body
    }
}

/// Fold `obs` into `prior`, returning the new baseline plus the edges that fired.
///
/// The heart of the adapter. Rules:
/// - **merged** fires once, the first poll that observes `MERGED`; the baseline's
///   `merged` flag is monotonic so it never re-fires (a restart resumes with it
///   set).
/// - **mergeable → CONFLICTING** fires only on a genuine transition into
///   conflicting from a known non-conflicting (or never-known) state; a transient
///   `Unknown` keeps the prior known state and fires nothing (UNKNOWN-ignore).
/// - **reviews / threads / comments** fire when their monotonic max-id cursor
///   *strictly increases* (a new item); the cursor never decreases (review item
///   B), so a deletion neither fires nor rewinds the baseline.
/// - **CI** fires only *into* failure, or when new failing checks appear while
///   already failing — never on a transition to pending/success (review item H).
pub fn apply(prior: &Baseline, obs: &Observation) -> (Baseline, Vec<Edge>) {
    let mut edges = Vec::new();

    // Merged: terminal and monotonic — fire once on the first MERGED observation,
    // then latch so a restart from this baseline never re-fires it.
    let merged = prior.merged || obs.state == PrStateObserved::Merged;
    if !prior.merged && obs.state == PrStateObserved::Merged {
        edges.push(Edge::Merged);
    }

    // Mergeable: keep the last known state on UNKNOWN, so a flap does not baseline
    // away the real state nor fire a spurious conflict.
    let mergeable = obs.mergeable.known().or(prior.mergeable);
    if prior.mergeable != Some(Mergeable::Conflicting) && mergeable == Some(Mergeable::Conflicting)
    {
        edges.push(Edge::Conflicting);
    }

    // Monotonic id cursors: fire on a strictly higher max id; never rewind.
    let max_review_id = advance(
        prior.max_review_id,
        obs.max_review_id,
        &mut edges,
        |from, to| Edge::NewReviews { from, to },
    );
    let max_review_thread_id = advance(
        prior.max_review_thread_id,
        obs.max_review_thread_id,
        &mut edges,
        |from, to| Edge::NewReviewThreads { from, to },
    );
    let max_comment_id = advance(
        prior.max_comment_id,
        obs.max_comment_id,
        &mut edges,
        |from, to| Edge::NewComments { from, to },
    );

    if let Some(edge) = ci_edge(prior, obs) {
        edges.push(edge);
    }

    let new = Baseline {
        merged,
        mergeable,
        max_review_id,
        max_review_thread_id,
        max_comment_id,
        ci: obs.ci,
        failed_checks: capped(obs.failed_checks.clone()),
    };
    (new, edges)
}

/// Advance one monotonic id cursor: fire `make_edge` on a strictly higher id, and
/// return the new (never-decreasing) cursor value.
fn advance(
    prior: u64,
    observed: u64,
    edges: &mut Vec<Edge>,
    make_edge: impl Fn(u64, u64) -> Edge,
) -> u64 {
    if observed > prior {
        edges.push(make_edge(prior, observed));
    }
    prior.max(observed)
}

/// Decide whether the CI rollup change is an edge (review item H): only a
/// transition *into* failure, or gaining new failing checks while already failing.
fn ci_edge(prior: &Baseline, obs: &Observation) -> Option<Edge> {
    if obs.ci != CiRollup::Failure {
        // Transitions to pending / success / none are not edges (kill the flap
        // storm): agents care about "CI went red", not every intermediate state.
        return None;
    }
    let newly_failed: Vec<String> = obs
        .failed_checks
        .iter()
        .filter(|name| !prior.failed_checks.contains(name))
        .cloned()
        .collect();
    if prior.ci != CiRollup::Failure {
        // Newly into failure: every failing check is "newly failed".
        Some(Edge::CiFailure {
            from: prior.ci,
            newly_failed: capped(newly_failed),
        })
    } else if !newly_failed.is_empty() {
        // Already failing: fire only if the failing SET gained names — do not
        // re-storm the same failure every poll.
        Some(Edge::CiFailure {
            from: CiRollup::Failure,
            newly_failed: capped(newly_failed),
        })
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn obs(mergeable: MergeableObserved, max_review_id: u64, ci: CiRollup) -> Observation {
        Observation {
            state: PrStateObserved::Open,
            mergeable,
            max_review_id,
            max_review_thread_id: 0,
            max_comment_id: 0,
            ci,
            failed_checks: Vec::new(),
        }
    }

    /// An observation in a given lifecycle state, everything else quiescent.
    fn obs_state(state: PrStateObserved) -> Observation {
        Observation {
            state,
            ..obs(MergeableObserved::Mergeable, 0, CiRollup::None)
        }
    }

    #[test]
    fn merge_fires_once_then_latches() {
        // Baseline open, then a poll sees MERGED → fires once.
        let base = Baseline::from_observation(&obs_state(PrStateObserved::Open));
        assert!(!base.merged);
        let (base, edges) = apply(&base, &obs_state(PrStateObserved::Merged));
        assert_eq!(edges, vec![Edge::Merged]);
        assert!(base.merged, "the baseline latches merged");

        // A subsequent MERGED poll does not re-fire.
        let (base, edges) = apply(&base, &obs_state(PrStateObserved::Merged));
        assert!(edges.is_empty(), "merged is terminal, never re-fired");
        assert!(base.merged);
    }

    #[test]
    fn first_poll_on_an_already_merged_pr_baselines_without_firing() {
        // Watching a PR that is already merged: first poll baselines merged=true
        // and fires nothing (edge-triggered — the merge predates the watch).
        let base = Baseline::from_observation(&obs_state(PrStateObserved::Merged));
        assert!(base.merged);
        let (_b, edges) = apply(&base, &obs_state(PrStateObserved::Merged));
        assert!(edges.is_empty());
    }

    #[test]
    fn closed_without_merge_does_not_fire_merged() {
        let base = Baseline::from_observation(&obs_state(PrStateObserved::Open));
        let (base, edges) = apply(&base, &obs_state(PrStateObserved::Closed));
        assert!(edges.is_empty(), "a plain close is not a merge");
        assert!(!base.merged);
        // If it later merges (reopened → merged), the edge still fires.
        let (_b, edges) = apply(&base, &obs_state(PrStateObserved::Merged));
        assert_eq!(edges, vec![Edge::Merged]);
    }

    #[test]
    fn first_poll_baselines_without_edges() {
        let o = obs(MergeableObserved::Mergeable, 20, CiRollup::Success);
        let base = Baseline::from_observation(&o);
        assert_eq!(base.mergeable, Some(Mergeable::Mergeable));
        assert_eq!(base.max_review_id, 20);
        assert_eq!(base.ci, CiRollup::Success);
    }

    #[test]
    fn first_poll_unknown_baselines_to_none_not_unknown() {
        let base = Baseline::from_observation(&obs(MergeableObserved::Unknown, 0, CiRollup::None));
        assert_eq!(base.mergeable, None, "never baseline to UNKNOWN");
    }

    #[test]
    fn conflict_transition_fires_exactly_once() {
        let base =
            Baseline::from_observation(&obs(MergeableObserved::Mergeable, 0, CiRollup::None));
        let (base, edges) = apply(
            &base,
            &obs(MergeableObserved::Conflicting, 0, CiRollup::None),
        );
        assert_eq!(edges, vec![Edge::Conflicting]);
        let (_b, edges) = apply(
            &base,
            &obs(MergeableObserved::Conflicting, 0, CiRollup::None),
        );
        assert!(edges.is_empty(), "a persistent conflict is not re-fired");
    }

    #[test]
    fn unknown_flapping_produces_zero_events() {
        let mut base =
            Baseline::from_observation(&obs(MergeableObserved::Mergeable, 0, CiRollup::None));
        for state in [
            MergeableObserved::Unknown,
            MergeableObserved::Unknown,
            MergeableObserved::Mergeable,
            MergeableObserved::Unknown,
        ] {
            let (next, edges) = apply(&base, &obs(state, 0, CiRollup::None));
            assert!(edges.is_empty(), "UNKNOWN flapping must fire nothing");
            base = next;
            assert_eq!(base.mergeable, Some(Mergeable::Mergeable));
        }
    }

    /// Review item B: a net-zero add+delete (max id jumps despite unchanged count)
    /// surfaces the new item; a pure decrease fires nothing and does not rewind the
    /// cursor. Covered for all three id signals.
    #[test]
    fn id_cursor_surfaces_new_item_across_net_zero_churn() {
        let prior = Baseline {
            max_review_id: 5,
            max_review_thread_id: 5,
            max_comment_id: 5,
            mergeable: Some(Mergeable::Mergeable),
            ..Baseline::default()
        };

        // reviews: delete id 4, add id 7 → max 5→7 → fires; baseline advances to 7.
        let mut o = obs(MergeableObserved::Mergeable, 7, CiRollup::None);
        o.max_review_thread_id = 5;
        o.max_comment_id = 5;
        let (next, edges) = apply(&prior, &o);
        assert_eq!(edges, vec![Edge::NewReviews { from: 5, to: 7 }]);
        assert_eq!(next.max_review_id, 7);

        // pure decrease (deleted the newest): max 5→3 → no fire, cursor stays 5.
        let mut o = obs(MergeableObserved::Mergeable, 3, CiRollup::None);
        o.max_review_thread_id = 5;
        o.max_comment_id = 5;
        let (next, edges) = apply(&prior, &o);
        assert!(edges.is_empty(), "a pure decrease is not an edge");
        assert_eq!(next.max_review_id, 5, "the cursor never rewinds");

        // threads: new id 9 → fires NewReviewThreads.
        let mut o = obs(MergeableObserved::Mergeable, 5, CiRollup::None);
        o.max_review_thread_id = 9;
        o.max_comment_id = 5;
        let (_n, edges) = apply(&prior, &o);
        assert_eq!(edges, vec![Edge::NewReviewThreads { from: 5, to: 9 }]);

        // comments: new id 6 → fires NewComments.
        let mut o = obs(MergeableObserved::Mergeable, 5, CiRollup::None);
        o.max_review_thread_id = 5;
        o.max_comment_id = 6;
        let (_n, edges) = apply(&prior, &o);
        assert_eq!(edges, vec![Edge::NewComments { from: 5, to: 6 }]);
    }

    #[test]
    fn ci_fires_into_failure_once_with_newly_failed_names() {
        let base = Baseline {
            ci: CiRollup::Pending,
            mergeable: Some(Mergeable::Mergeable),
            ..Baseline::default()
        };
        let failing = Observation {
            ci: CiRollup::Failure,
            failed_checks: vec!["build".to_string()],
            ..obs(MergeableObserved::Mergeable, 0, CiRollup::Failure)
        };
        let (base, edges) = apply(&base, &failing);
        assert_eq!(
            edges,
            vec![Edge::CiFailure {
                from: CiRollup::Pending,
                newly_failed: vec!["build".to_string()],
            }]
        );
        let (_b, edges) = apply(&base, &failing);
        assert!(edges.is_empty(), "a persistent failure is not re-fired");
    }

    #[test]
    fn ci_does_not_fire_on_success_or_pending() {
        // Review item H: a Success↔Pending flap fires nothing.
        let base = Baseline {
            ci: CiRollup::Success,
            mergeable: Some(Mergeable::Mergeable),
            ..Baseline::default()
        };
        let (base, edges) = apply(
            &base,
            &obs(MergeableObserved::Mergeable, 0, CiRollup::Pending),
        );
        assert!(edges.is_empty(), "success→pending is not an edge");
        let (_b, edges) = apply(
            &base,
            &obs(MergeableObserved::Mergeable, 0, CiRollup::Success),
        );
        assert!(edges.is_empty(), "pending→success is not an edge");
    }

    #[test]
    fn ci_refires_only_when_failing_set_gains_new_checks() {
        let base = Baseline {
            ci: CiRollup::Failure,
            failed_checks: vec!["build".to_string()],
            mergeable: Some(Mergeable::Mergeable),
            ..Baseline::default()
        };
        let same = Observation {
            ci: CiRollup::Failure,
            failed_checks: vec!["build".to_string()],
            ..obs(MergeableObserved::Mergeable, 0, CiRollup::Failure)
        };
        let (base, edges) = apply(&base, &same);
        assert!(edges.is_empty());
        let more = Observation {
            ci: CiRollup::Failure,
            failed_checks: vec!["build".to_string(), "test".to_string()],
            ..obs(MergeableObserved::Mergeable, 0, CiRollup::Failure)
        };
        let (_b, edges) = apply(&base, &more);
        assert_eq!(
            edges,
            vec![Edge::CiFailure {
                from: CiRollup::Failure,
                newly_failed: vec!["test".to_string()],
            }]
        );
    }

    #[test]
    fn failed_checks_are_capped_in_the_baseline() {
        let many: Vec<String> = (0..(MAX_FAILED_CHECKS + 50))
            .map(|i| format!("check-{i}"))
            .collect();
        let o = Observation {
            ci: CiRollup::Failure,
            failed_checks: many,
            ..obs(MergeableObserved::Mergeable, 0, CiRollup::Failure)
        };
        let base = Baseline::from_observation(&o);
        assert_eq!(base.failed_checks.len(), MAX_FAILED_CHECKS);
    }

    #[test]
    fn baseline_serde_round_trips() {
        let base = Baseline {
            merged: true,
            mergeable: Some(Mergeable::Conflicting),
            max_review_id: 40,
            max_review_thread_id: 12,
            max_comment_id: 77,
            ci: CiRollup::Failure,
            failed_checks: vec!["build".to_string(), "lint".to_string()],
        };
        let json = serde_json::to_value(&base).unwrap();
        let back: Baseline = serde_json::from_value(json).unwrap();
        assert_eq!(base, back);
    }

    /// A baseline persisted by an older adapter (no `merged` field) still
    /// deserializes — the missing flag reads as `false`, and a later MERGED poll
    /// fires the edge once. Guards the `#[serde(default)]` forward-compat contract.
    #[test]
    fn baseline_without_merged_field_defaults_false_and_can_still_fire() {
        let legacy = serde_json::json!({
            "mergeable": "mergeable",
            "max_review_id": 5,
            "ci": "success"
        });
        let base: Baseline = serde_json::from_value(legacy).unwrap();
        assert!(!base.merged, "a missing merged field defaults to false");
        let (_b, edges) = apply(&base, &obs_state(PrStateObserved::Merged));
        assert_eq!(edges, vec![Edge::Merged]);
    }
}
