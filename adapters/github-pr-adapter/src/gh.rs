//! The `gh` seam: shell out to the GitHub CLI and parse its JSON into an
//! [`Observation`].
//!
//! # Why a subprocess (and why injectable)
//!
//! The adapter reaches GitHub through the user's already-authenticated `gh` CLI
//! rather than reimplementing GitHub auth/HTTP — parity with the old
//! `agent-ipc-github` skill, and it inherits the user's token, enterprise host,
//! and proxy config for free. The binary is overridable via [`ENV_GH_BIN`] so
//! tests point at a FAKE `gh` that emits recorded JSON — the whole poll→diff→
//! publish loop then runs with no network or auth (card-10 decision 2).
//!
//! # Calls per poll
//!
//! - `gh pr view` for mergeability + the whole-PR CI rollup.
//! - `gh api …/pulls/N/reviews`, `…/issues/N/comments`, `…/pulls/N/comments` for
//!   the three review-ish signals. These REST endpoints return **integer ids**, so
//!   the diff can use a monotonic highest-id cursor (review item B) instead of a
//!   non-monotone count.
//!
//! # Strict, fail-CLOSED parsing (review item A)
//!
//! A structurally-valid-but-incomplete gh response (a missing/null `mergeable`,
//! `statusCheckRollup`, or a non-array list) is NOT treated as an empty state —
//! that would fire spurious edges and rewrite the baseline to zeros, causing a
//! full false re-fire storm on the next healthy poll. Instead such a response is a
//! [`GhError::Parse`], which the run loop treats as TRANSIENT (skip the poll, keep
//! the baseline, retry) — distinct from a present-but-empty `[]`, which is a real
//! "nothing here yet".
//!
//! # Error classification (the failure modes design/01 calls out)
//!
//! A non-zero `gh` is classified from its stderr into a [`GhError`] the run loop
//! reacts to differently: [`GhError::AuthMissing`] is fatal (exit non-zero so the
//! supervisor records + surfaces it — no silent spin), [`GhError::RateLimited`]
//! triggers an in-adapter backoff-and-retry (bounded — review item G), and
//! anything else is a fatal [`GhError::Failed`].

use serde_json::Value;

use crate::snapshot::{CiRollup, FailedCheck, MergeableObserved, Observation, PrStateObserved};

/// Env var overriding the `gh` binary (card-10 decision 2: injectable for tests).
pub const ENV_GH_BIN: &str = "MAILBOX_GH_BIN";

/// Default binary: the real `gh` on `PATH`.
const DEFAULT_GH_BIN: &str = "gh";

/// A failure invoking or parsing `gh`. Business errors are values, never panics
/// (type-driven design). The variant drives how the run loop reacts.
#[derive(Debug, thiserror::Error)]
pub enum GhError {
    /// `gh` is not authenticated (or the token expired). Fatal: the adapter exits
    /// non-zero so the supervisor records + surfaces it via its give-up path
    /// (design/01 failure mode: "gh auth missing → exit non-zero").
    #[error("gh authentication is missing or expired: {0}")]
    AuthMissing(String),
    /// `gh` hit a GitHub rate limit. Not immediately fatal: the run loop backs off
    /// and retries inside the adapter (design/01), up to a bounded budget.
    #[error("gh reported a GitHub rate limit: {0}")]
    RateLimited(String),
    /// `gh` failed for some other reason (bad repo, network, unexpected output).
    /// Fatal, so a genuinely broken watch surfaces rather than spinning.
    #[error("gh command failed: {0}")]
    Failed(String),
    /// The `gh` binary could not be spawned at all (not on PATH / bad override).
    #[error("could not run gh binary {bin:?}: {source}")]
    Spawn {
        bin: String,
        #[source]
        source: std::io::Error,
    },
    /// `gh` exited 0 but its JSON was malformed or structurally incomplete (a
    /// missing/null required field). TRANSIENT — the run loop skips the poll and
    /// keeps the baseline (never fail-open to zeros — review item A).
    #[error("gh returned an unexpected/incomplete response: {0}")]
    Parse(String),
}

/// A poll client bound to one PR, shelling out to `gh` under the given identity.
pub struct GhClient {
    bin: String,
    owner: String,
    repo: String,
    number: u64,
}

impl GhClient {
    /// Build a client for `owner/repo#number`, resolving the `gh` binary from
    /// [`ENV_GH_BIN`] (else the real `gh` on PATH).
    pub fn new(owner: impl Into<String>, repo: impl Into<String>, number: u64) -> Self {
        let bin = std::env::var(ENV_GH_BIN)
            .ok()
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| DEFAULT_GH_BIN.to_string());
        Self {
            bin,
            owner: owner.into(),
            repo: repo.into(),
            number,
        }
    }

    /// `owner/repo`, the slug `gh --repo` wants.
    fn slug(&self) -> String {
        format!("{}/{}", self.owner, self.repo)
    }

    /// Run `gh` with `args`, returning its stdout or a classified [`GhError`].
    async fn run(&self, args: &[String]) -> Result<String, GhError> {
        let output = tokio::process::Command::new(&self.bin)
            .args(args)
            .output()
            .await
            .map_err(|source| GhError::Spawn {
                bin: self.bin.clone(),
                source,
            })?;

        if output.status.success() {
            return String::from_utf8(output.stdout)
                .map_err(|err| GhError::Parse(format!("gh stdout was not UTF-8: {err}")));
        }

        let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
        Err(classify_failure(&stderr))
    }

    /// Poll GitHub once and build an [`Observation`]. Four `gh` calls (pr view +
    /// three REST lists). A rate limit or parse failure on *any* call surfaces so
    /// the caller backs off/skips the whole poll.
    pub async fn observe(&self) -> Result<Observation, GhError> {
        let view = self
            .run(&[
                "pr".to_string(),
                "view".to_string(),
                self.number.to_string(),
                "--repo".to_string(),
                self.slug(),
                "--json".to_string(),
                // `url` rides along on a call we already make: it costs nothing and
                // it is the only correct way to link to a PR on a host that may not
                // be github.com.
                "state,mergeable,statusCheckRollup,url".to_string(),
            ])
            .await?;
        let pr = parse_pr_view(&view)?;

        let max_review_id = self.max_id("reviews", "pulls", "reviews").await?;
        let max_comment_id = self.max_id("comments", "issues", "comments").await?;
        let max_review_thread_id = self.max_id("review threads", "pulls", "comments").await?;

        Ok(Observation {
            state: pr.state,
            mergeable: pr.mergeable,
            max_review_id,
            max_review_thread_id,
            max_comment_id,
            ci: pr.ci,
            failed_checks: pr.failed_checks,
            pr_url: pr.url,
        })
    }

    /// Fetch a REST list endpoint (`repos/{o}/{r}/{parent}/{n}/{leaf}`) and return
    /// the highest integer `id` in it (0 for a real empty list).
    async fn max_id(&self, label: &str, parent: &str, leaf: &str) -> Result<u64, GhError> {
        let path = format!(
            "repos/{}/{}/{}/{}/{}",
            self.owner, self.repo, parent, self.number, leaf
        );
        let body = self.run(&["api".to_string(), path]).await?;
        parse_max_id(label, &body)
    }
}

/// Classify a failed `gh` invocation from its stderr. Rate-limit is checked first
/// (a rate-limited auth-looking message should still back off, not exit).
fn classify_failure(stderr: &str) -> GhError {
    let lower = stderr.to_ascii_lowercase();
    let detail = first_line(stderr);
    if lower.contains("rate limit") {
        GhError::RateLimited(detail)
    } else if lower.contains("gh auth login")
        || lower.contains("authentication")
        || lower.contains("not logged in")
        || lower.contains("to get started with github cli")
    {
        GhError::AuthMissing(detail)
    } else {
        GhError::Failed(detail)
    }
}

/// The first non-empty line of `text`, trimmed — a short detail for logs/errors
/// that never dumps a multi-line gh error.
fn first_line(text: &str) -> String {
    text.lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .unwrap_or("")
        .to_string()
}

/// The signals one `gh pr view` call yields. Named (not a positional tuple) so
/// `observe` reads each field at the boundary where it is produced.
#[derive(Debug)]
struct PrView {
    state: PrStateObserved,
    mergeable: MergeableObserved,
    ci: CiRollup,
    failed_checks: Vec<FailedCheck>,
    /// GitHub's own URL for this PR, when it reported one.
    url: Option<String>,
}

/// Parse the `gh pr view` payload into lifecycle state, mergeability, the CI
/// rollup, and the failed-check names. Fail-CLOSED (review item A): a missing/null
/// `state`, `mergeable`, or `statusCheckRollup` is a [`GhError::Parse`], not a
/// silent default.
fn parse_pr_view(view: &str) -> Result<PrView, GhError> {
    let view: Value = serde_json::from_str(view)
        .map_err(|err| GhError::Parse(format!("pr view was not JSON: {err}")))?;

    // A present, non-null string is required; missing/null is an unexpected shape.
    let state = match required(&view, "state")? {
        Value::String(s) => parse_state(s),
        other => {
            return Err(GhError::Parse(format!(
                "pr view `state` was not a string: {other}"
            )));
        }
    };

    let mergeable = match required(&view, "mergeable")? {
        Value::String(s) => parse_mergeable(s),
        other => {
            return Err(GhError::Parse(format!(
                "pr view `mergeable` was not a string: {other}"
            )));
        }
    };

    let rollup = required(&view, "statusCheckRollup")?;
    let checks = rollup.as_array().ok_or_else(|| {
        GhError::Parse("pr view `statusCheckRollup` was not an array".to_string())
    })?;
    let (ci, failed_checks) = reduce_ci(checks);

    // NOT `required`: the URL only makes an event more legible, so a response
    // without one is a subject with no link — never a skipped poll, and never a
    // suppressed edge (review item A is about signals that DRIVE edges).
    let url = view
        .get("url")
        .and_then(Value::as_str)
        .map(str::to_string)
        .filter(|url| !url.is_empty());

    Ok(PrView {
        state,
        mergeable,
        ci,
        failed_checks,
        url,
    })
}

/// Fetch a required, non-null field, or a [`GhError::Parse`] (review item A: a
/// missing/null required field is an unexpected shape, not an empty state).
fn required<'a>(object: &'a Value, field: &str) -> Result<&'a Value, GhError> {
    match object.get(field) {
        Some(Value::Null) | None => Err(GhError::Parse(format!(
            "required field `{field}` was missing or null"
        ))),
        Some(value) => Ok(value),
    }
}

/// GitHub's `state` string → the observed lifecycle state. `OPEN`/`CLOSED`/
/// `MERGED` are the states `gh` returns; anything else folds to `Other`, which
/// never fires a merge edge — the same fail-safe stance as [`parse_mergeable`]'s
/// UNKNOWN, so an unmodelled future value cannot spuriously report a merge.
fn parse_state(value: &str) -> PrStateObserved {
    match value {
        "OPEN" => PrStateObserved::Open,
        "CLOSED" => PrStateObserved::Closed,
        "MERGED" => PrStateObserved::Merged,
        _ => PrStateObserved::Other,
    }
}

/// GitHub's `mergeable` string → the three-state observed value. `MERGEABLE`/
/// `CONFLICTING` are the known states; anything else (`UNKNOWN`, or a value a
/// future GitHub adds) folds to `Unknown`, which never fires — fail-safe.
fn parse_mergeable(value: &str) -> MergeableObserved {
    match value {
        "MERGEABLE" => MergeableObserved::Mergeable,
        "CONFLICTING" => MergeableObserved::Conflicting,
        _ => MergeableObserved::Unknown,
    }
}

/// Parse a REST list body and return its highest integer `id`. A present, empty
/// array `[]` is a real "nothing yet" → `0`. A non-array body, or an element
/// without an integer `id`, is an unexpected shape → [`GhError::Parse`] (review
/// item A: never fail-open).
fn parse_max_id(label: &str, body: &str) -> Result<u64, GhError> {
    let value: Value = serde_json::from_str(body)
        .map_err(|err| GhError::Parse(format!("{label} list was not JSON: {err}")))?;
    let items = value
        .as_array()
        .ok_or_else(|| GhError::Parse(format!("{label} list was not a JSON array")))?;
    let mut max = 0u64;
    for item in items {
        let id = item
            .get("id")
            .and_then(Value::as_u64)
            .ok_or_else(|| GhError::Parse(format!("{label} item had no integer `id`")))?;
        max = max.max(id);
    }
    Ok(max)
}

/// Reduce gh's `statusCheckRollup` array to a whole-PR rollup + the checks that are
/// currently failing (card-10 decision 3).
fn reduce_ci(checks: &[Value]) -> (CiRollup, Vec<FailedCheck>) {
    if checks.is_empty() {
        return (CiRollup::None, Vec::new());
    }
    let mut any_pending = false;
    let mut failed = Vec::new();
    for check in checks {
        match classify_check(check) {
            CheckOutcome::Failing => failed.push(FailedCheck {
                name: check_name(check),
                url: check_url(check),
            }),
            CheckOutcome::Pending => any_pending = true,
            CheckOutcome::Success => {}
        }
    }
    let rollup = if !failed.is_empty() {
        CiRollup::Failure
    } else if any_pending {
        CiRollup::Pending
    } else {
        CiRollup::Success
    };
    (rollup, failed)
}

/// Per-check outcome we fold into the rollup.
enum CheckOutcome {
    Failing,
    Pending,
    Success,
}

/// Classify one `statusCheckRollup` entry. Handles both shapes gh returns: a
/// CheckRun (`status` + `conclusion`) and a StatusContext (`state`).
fn classify_check(check: &Value) -> CheckOutcome {
    if let Some(conclusion) = check.get("conclusion").and_then(Value::as_str)
        && !conclusion.is_empty()
    {
        return match conclusion {
            "SUCCESS" | "NEUTRAL" | "SKIPPED" => CheckOutcome::Success,
            "FAILURE" | "TIMED_OUT" | "CANCELLED" | "ACTION_REQUIRED" | "STARTUP_FAILURE"
            | "STALE" => CheckOutcome::Failing,
            _ => CheckOutcome::Pending,
        };
    }
    if let Some(status) = check.get("status").and_then(Value::as_str) {
        // A CheckRun that has not COMPLETED (or completed without a conclusion,
        // which is anomalous) is pending — fail safe, never counted as failing.
        if status != "COMPLETED" {
            return CheckOutcome::Pending;
        }
        return CheckOutcome::Pending;
    }
    match check.get("state").and_then(Value::as_str) {
        Some("SUCCESS") => CheckOutcome::Success,
        Some("FAILURE") | Some("ERROR") => CheckOutcome::Failing,
        _ => CheckOutcome::Pending,
    }
}

/// A check's display name: CheckRun `name`, else StatusContext `context`, else a
/// stable placeholder so a failed-but-unnamed check still surfaces.
fn check_name(check: &Value) -> String {
    check
        .get("name")
        .and_then(Value::as_str)
        .or_else(|| check.get("context").and_then(Value::as_str))
        .unwrap_or("unknown-check")
        .to_string()
}

/// Where a check reports: `detailsUrl` on a CheckRun, `targetUrl` on a
/// StatusContext, and `None` when neither is present or either is blank.
///
/// This is the link an agent follows to the *actual* failure — the run's own log
/// page — rather than back to the PR it was already looking at.
fn check_url(check: &Value) -> Option<String> {
    ["detailsUrl", "targetUrl"]
        .iter()
        .find_map(|field| check.get(field).and_then(Value::as_str))
        .filter(|url| !url.is_empty())
        .map(str::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_rate_limit_before_anything_else() {
        let err = classify_failure("gh: API rate limit exceeded for user ID 1.");
        assert!(matches!(err, GhError::RateLimited(_)));
    }

    #[test]
    fn classifies_auth_missing() {
        let err = classify_failure(
            "gh: To get started with GitHub CLI, please run: gh auth login\nexit status 4",
        );
        assert!(matches!(err, GhError::AuthMissing(_)));
    }

    #[test]
    fn classifies_other_failure() {
        let err = classify_failure("gh: could not resolve to a Repository with the name 'x/y'.");
        assert!(matches!(err, GhError::Failed(_)));
    }

    #[test]
    fn parses_a_clean_mergeable_pr() {
        let view = r#"{
            "state": "OPEN",
            "mergeable": "MERGEABLE",
            "url": "https://github.com/acme/web/pull/42",
            "statusCheckRollup": [
                {"__typename": "CheckRun", "name": "build", "status": "COMPLETED", "conclusion": "SUCCESS"}
            ]
        }"#;
        let pr = parse_pr_view(view).unwrap();
        assert_eq!(pr.state, PrStateObserved::Open);
        assert_eq!(pr.mergeable, MergeableObserved::Mergeable);
        assert_eq!(pr.ci, CiRollup::Success);
        assert!(pr.failed_checks.is_empty());
        assert_eq!(
            pr.url.as_deref(),
            Some("https://github.com/acme/web/pull/42")
        );
    }

    /// A failing check carries where it reports, so the event can point at the run
    /// rather than back at the PR.
    #[test]
    fn parses_conflicting_with_failing_and_pending_checks() {
        let view = r#"{
            "state": "OPEN",
            "mergeable": "CONFLICTING",
            "statusCheckRollup": [
                {"__typename": "CheckRun", "name": "build", "status": "COMPLETED", "conclusion": "FAILURE",
                 "detailsUrl": "https://github.com/acme/web/actions/runs/9/job/2"},
                {"__typename": "CheckRun", "name": "test", "status": "IN_PROGRESS", "conclusion": ""},
                {"__typename": "StatusContext", "context": "ci/legacy", "state": "SUCCESS"}
            ]
        }"#;
        let pr = parse_pr_view(view).unwrap();
        assert_eq!(pr.mergeable, MergeableObserved::Conflicting);
        assert_eq!(pr.ci, CiRollup::Failure);
        assert_eq!(
            pr.failed_checks,
            vec![FailedCheck {
                name: "build".to_string(),
                url: Some("https://github.com/acme/web/actions/runs/9/job/2".to_string()),
            }]
        );
    }

    /// The two shapes gh returns report their URL under different keys, and a check
    /// that reports neither is still a failing check — just one with nowhere to send
    /// the reader.
    #[test]
    fn a_failing_check_takes_its_url_from_either_shape_or_none() {
        let view = r#"{
            "state": "OPEN",
            "mergeable": "MERGEABLE",
            "statusCheckRollup": [
                {"__typename": "StatusContext", "context": "ci/legacy", "state": "FAILURE",
                 "targetUrl": "https://ci.example.com/build/9"},
                {"__typename": "CheckRun", "name": "lint", "status": "COMPLETED",
                 "conclusion": "FAILURE", "detailsUrl": ""}
            ]
        }"#;
        let pr = parse_pr_view(view).unwrap();
        assert_eq!(pr.ci, CiRollup::Failure);
        assert_eq!(
            pr.failed_checks,
            vec![
                FailedCheck {
                    name: "ci/legacy".to_string(),
                    url: Some("https://ci.example.com/build/9".to_string()),
                },
                FailedCheck::new("lint"),
            ]
        );
    }

    /// **A missing url must never cost an edge.** It is decoration on the event, not
    /// a signal that drives one — so unlike `state`/`mergeable`/`statusCheckRollup`
    /// (review item A) its absence parses cleanly and simply yields no link.
    #[test]
    fn a_missing_url_is_not_a_parse_error() {
        let pr =
            parse_pr_view(r#"{"state":"OPEN","mergeable":"MERGEABLE","statusCheckRollup":[]}"#)
                .unwrap();
        assert_eq!(pr.url, None);

        let pr = parse_pr_view(
            r#"{"state":"OPEN","mergeable":"MERGEABLE","statusCheckRollup":[],"url":""}"#,
        )
        .unwrap();
        assert_eq!(pr.url, None, "a blank url is no url");
    }

    #[test]
    fn empty_rollup_is_ci_none() {
        let view = r#"{"state":"OPEN","mergeable":"MERGEABLE","statusCheckRollup":[]}"#;
        assert_eq!(parse_pr_view(view).unwrap().ci, CiRollup::None);
    }

    /// A merged PR: `state` is MERGED, and `mergeable` is typically UNKNOWN once
    /// GitHub stops recomputing it — the merge signal must not depend on
    /// mergeability still being known.
    #[test]
    fn parses_a_merged_pr_even_with_unknown_mergeable() {
        let view = r#"{
            "state": "MERGED",
            "mergeable": "UNKNOWN",
            "statusCheckRollup": []
        }"#;
        let pr = parse_pr_view(view).unwrap();
        assert_eq!(pr.state, PrStateObserved::Merged);
        assert_eq!(pr.mergeable, MergeableObserved::Unknown);
    }

    #[test]
    fn parses_state_variants_and_folds_unknown_to_other() {
        assert_eq!(parse_state("OPEN"), PrStateObserved::Open);
        assert_eq!(parse_state("CLOSED"), PrStateObserved::Closed);
        assert_eq!(parse_state("MERGED"), PrStateObserved::Merged);
        assert_eq!(parse_state("SOMETHING_NEW"), PrStateObserved::Other);
    }

    /// Review item A applies to `state` too: a missing/null state is a transient
    /// Parse error, never a silent default (which could mask a merge either way).
    #[test]
    fn missing_state_is_a_parse_error() {
        let err = parse_pr_view(r#"{"mergeable":"MERGEABLE","statusCheckRollup":[]}"#).unwrap_err();
        assert!(matches!(err, GhError::Parse(_)), "missing state");
        let err = parse_pr_view(r#"{"state":null,"mergeable":"MERGEABLE","statusCheckRollup":[]}"#)
            .unwrap_err();
        assert!(matches!(err, GhError::Parse(_)), "null state");
    }

    /// Review item A: a missing/null required field is a Parse error (transient),
    /// NOT a silent fail-open to a default.
    #[test]
    fn missing_mergeable_is_a_parse_error() {
        let err = parse_pr_view(r#"{"state":"OPEN","statusCheckRollup":[]}"#).unwrap_err();
        assert!(matches!(err, GhError::Parse(_)), "missing mergeable");
        let err = parse_pr_view(r#"{"state":"OPEN","mergeable":null,"statusCheckRollup":[]}"#)
            .unwrap_err();
        assert!(matches!(err, GhError::Parse(_)), "null mergeable");
    }

    #[test]
    fn missing_or_null_rollup_is_a_parse_error() {
        let err = parse_pr_view(r#"{"state":"OPEN","mergeable":"MERGEABLE"}"#).unwrap_err();
        assert!(matches!(err, GhError::Parse(_)), "missing rollup");
        let err =
            parse_pr_view(r#"{"state":"OPEN","mergeable":"MERGEABLE","statusCheckRollup":null}"#)
                .unwrap_err();
        assert!(matches!(err, GhError::Parse(_)), "null rollup");
        let err =
            parse_pr_view(r#"{"state":"OPEN","mergeable":"MERGEABLE","statusCheckRollup":{}}"#)
                .unwrap_err();
        assert!(matches!(err, GhError::Parse(_)), "non-array rollup");
    }

    #[test]
    fn empty_list_is_zero_but_non_array_is_a_parse_error() {
        assert_eq!(parse_max_id("reviews", "[]").unwrap(), 0);
        assert_eq!(
            parse_max_id("reviews", "[{\"id\":3},{\"id\":9}]").unwrap(),
            9
        );
        assert!(matches!(
            parse_max_id("reviews", "null").unwrap_err(),
            GhError::Parse(_)
        ));
        assert!(matches!(
            parse_max_id("reviews", "{\"message\":\"Not Found\"}").unwrap_err(),
            GhError::Parse(_)
        ));
        assert!(
            matches!(
                parse_max_id("reviews", "[{\"body\":\"no id\"}]").unwrap_err(),
                GhError::Parse(_)
            ),
            "an item without an integer id is an unexpected shape"
        );
    }
}
