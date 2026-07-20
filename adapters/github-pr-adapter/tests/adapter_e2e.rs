//! End-to-end acceptance tests for the github-pr adapter (card 10), driving the
//! REAL built binary against a FAKE `gh` that emits recorded fixtures for a
//! sequence of polls (card-10 decision 2: `gh` is injectable via `MAILBOX_GH_BIN`
//! so the whole poll→diff→publish loop runs with no network or auth).
//!
//! Proves the acceptance criteria at the adapter boundary:
//! - ac-10-1: baseline on the first poll (no publishes); a conflict, a review,
//!   and a CI transition each publish EXACTLY ONCE; a stable state re-fires nothing.
//! - ac-10-2: restart with the persisted baseline injected → NO duplicate events.
//! - ac-10-3: UNKNOWN mergeable flapping → ZERO events.
//! - ac-10-4: a rate-limit response triggers backoff + retry, NOT a tight loop.
//! - review item A: a partial/garbage gh body (exit 0) → zero false edges, the
//!   injected baseline is NOT reset, the poll is skipped (not fatal); a persistent
//!   partial → eventual non-zero exit.
//! - review item G: a persistent rate limit → eventual non-zero exit (surfaced).
//! - gh auth missing → the adapter exits NON-ZERO.
//!
//! Each poll makes four `gh` calls in order — `pr view` (mergeability + CI) then
//! three REST list endpoints (`reviews`, issue `comments`, review-thread
//! `comments`), which return integer ids for the monotonic-cursor diff. The fake
//! dispatches on the subcommand + path and keeps a per-sequence counter, clamping
//! to the last fixture so an extra poll repeats the tail rather than erroring.
//!
//! Determinism (no flaky sleeps): every run is bounded by `max_polls`, so the
//! adapter exits on its own and we read its full stdout.

use std::io::{Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Instant;

use serde_json::{Value, json};
use tempfile::TempDir;

/// The freshly built adapter binary under test.
fn adapter_bin() -> &'static str {
    env!("CARGO_BIN_EXE_mailbox-github-pr-adapter")
}

/// A fake `gh` that dispatches on the subcommand (`pr` vs `api`, and for `api` on
/// the endpoint path), advances a per-sequence counter, clamps to the last
/// fixture, and honours `@AUTH` / `@RATE` directive fixtures. Fixtures live in
/// `$MAILBOX_FAKE_GH_DIR` named `<seq>.<i>` where seq ∈ {pr, reviews,
/// issue_comments, thread_comments}.
const FAKE_GH: &str = r#"#!/usr/bin/env bash
set -euo pipefail
dir="${MAILBOX_FAKE_GH_DIR:?MAILBOX_FAKE_GH_DIR must be set}"
case "${1:-}" in
  pr) key="pr" ;;
  api)
    case "${2:-}" in
      */pulls/*/reviews) key="reviews" ;;
      */issues/*/comments) key="issue_comments" ;;
      */pulls/*/comments) key="thread_comments" ;;
      *) echo "fake-gh: unknown api path ${2:-}" >&2; exit 2 ;;
    esac ;;
  *) echo "fake-gh: unsupported invocation: $*" >&2; exit 2 ;;
esac
ctr="$dir/counter.$key"
i=0
if [ -f "$ctr" ]; then i="$(cat "$ctr")"; fi
max=0
for f in "$dir/$key".*; do
  [ -e "$f" ] || continue
  n="${f##*.}"
  case "$n" in (*[!0-9]*) continue ;; esac
  if [ "$n" -gt "$max" ]; then max="$n"; fi
done
use="$i"
if [ "$i" -gt "$max" ]; then use="$max"; fi
echo $((i + 1)) > "$ctr"
f="$dir/$key.$use"
if [ ! -f "$f" ]; then echo "fake-gh: missing fixture $f" >&2; exit 3; fi
directive="$(head -n1 "$f")"
case "$directive" in
  @AUTH) echo "gh: To get started with GitHub CLI, please run: gh auth login" >&2; exit 4 ;;
  @RATE) echo "gh: API rate limit exceeded for user ID 1" >&2; exit 1 ;;
  *) cat "$f" ;;
esac
"#;

/// A test harness: a fixture dir plus the fake `gh` script, both in a tempdir.
struct FakeGh {
    dir: TempDir,
    script: PathBuf,
}

impl FakeGh {
    fn new() -> Self {
        let dir = TempDir::new().expect("tempdir");
        let script = dir.path().join("fake-gh.sh");
        std::fs::write(&script, FAKE_GH).expect("write fake gh");
        let mut perms = std::fs::metadata(&script).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&script, perms).expect("chmod fake gh");
        // Default: every review/comment/thread list is empty; the clamp repeats it
        // for all polls, so tests only script the sequences they care about.
        for seq in ["reviews", "issue_comments", "thread_comments"] {
            std::fs::write(dir.path().join(format!("{seq}.0")), "[]").expect("write default list");
        }
        FakeGh { dir, script }
    }

    /// Write a fixture for sequence `seq` at poll index `i`.
    fn fixture(&self, seq: &str, i: usize, body: &str) -> &Self {
        std::fs::write(self.dir.path().join(format!("{seq}.{i}")), body).expect("write fixture");
        self
    }

    fn pr(&self, i: usize, body: &str) -> &Self {
        self.fixture("pr", i, body)
    }

    fn reviews(&self, i: usize, body: &str) -> &Self {
        self.fixture("reviews", i, body)
    }

    /// Write a `pr` directive fixture (`@AUTH` / `@RATE`) for poll index `i`.
    fn pr_directive(&self, i: usize, directive: &str) -> &Self {
        self.fixture("pr", i, directive)
    }

    /// How many times the fake's `pr` subcommand was invoked (for the rate-limit
    /// "did it retry, not spin" assertion).
    fn pr_call_count(&self) -> u64 {
        std::fs::read_to_string(self.dir.path().join("counter.pr"))
            .ok()
            .and_then(|s| s.trim().parse().ok())
            .unwrap_or(0)
    }

    fn script(&self) -> &Path {
        &self.script
    }

    fn dir(&self) -> &Path {
        self.dir.path()
    }
}

/// A completed adapter run: exit code + parsed stdout lines.
struct RunResult {
    code: Option<i32>,
    stdout: String,
    stderr: String,
}

impl RunResult {
    fn messages(&self) -> Vec<Value> {
        self.stdout
            .lines()
            .filter(|l| !l.trim().is_empty())
            .map(|l| {
                serde_json::from_str(l).unwrap_or_else(|e| panic!("bad stdout line {l:?}: {e}"))
            })
            .collect()
    }

    fn edge_count(&self, edge: &str) -> usize {
        self.messages()
            .iter()
            .filter(|m| m["type"] == "publish" && m["body"]["edge"] == edge)
            .count()
    }

    fn publish_count(&self) -> usize {
        self.messages()
            .iter()
            .filter(|m| m["type"] == "publish")
            .count()
    }

    fn baseline_count(&self) -> usize {
        self.messages()
            .iter()
            .filter(|m| m["type"] == "baseline")
            .count()
    }

    fn last_baseline(&self) -> Option<Value> {
        self.messages()
            .into_iter()
            .rfind(|m| m["type"] == "baseline")
            .map(|m| m["value"].clone())
    }
}

/// Run the adapter binary against `fake` with a given config, and return its exit
/// + output. Extra env vars are applied last.
fn run_adapter(fake: &FakeGh, config: Value, extra_env: &[(&str, &str)]) -> RunResult {
    let mut child = Command::new(adapter_bin())
        .env("MAILBOX_GH_BIN", fake.script())
        .env("MAILBOX_FAKE_GH_DIR", fake.dir())
        .env("RUST_LOG", "error")
        .envs(extra_env.iter().copied())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn adapter");

    let line = format!("{}\n", serde_json::to_string(&config).unwrap());
    child
        .stdin
        .take()
        .expect("adapter stdin")
        .write_all(line.as_bytes())
        .expect("write config");

    let mut stdout = String::new();
    child
        .stdout
        .take()
        .expect("adapter stdout")
        .read_to_string(&mut stdout)
        .expect("read stdout");
    let mut stderr = String::new();
    child
        .stderr
        .take()
        .expect("adapter stderr")
        .read_to_string(&mut stderr)
        .expect("read stderr");

    let status = child.wait().expect("wait adapter");
    RunResult {
        code: status.code(),
        stdout,
        stderr,
    }
}

/// A base config for `octocat/hello-world#42` polling fast.
fn config(max_polls: u64, baseline: Value) -> Value {
    json!({
        "topic": "github.pr.octocat/hello-world#42",
        "owner": "octocat",
        "repo": "hello-world",
        "number": 42,
        "interval_ms": 5,
        "baseline": baseline,
        "max_polls": max_polls,
    })
}

// pr-view fixtures (state + mergeable + statusCheckRollup — reviews/comments come
// from the REST endpoints now). All OPEN unless a test drives a merge.
fn pr_mergeable_ci_success() -> &'static str {
    r#"{"state":"OPEN","mergeable":"MERGEABLE","statusCheckRollup":[{"__typename":"CheckRun","name":"build","status":"COMPLETED","conclusion":"SUCCESS"}]}"#
}
fn pr_conflicting_ci_success() -> &'static str {
    r#"{"state":"OPEN","mergeable":"CONFLICTING","statusCheckRollup":[{"__typename":"CheckRun","name":"build","status":"COMPLETED","conclusion":"SUCCESS"}]}"#
}
fn pr_conflicting_ci_failure() -> &'static str {
    r#"{"state":"OPEN","mergeable":"CONFLICTING","statusCheckRollup":[{"__typename":"CheckRun","name":"build","status":"COMPLETED","conclusion":"FAILURE"}]}"#
}
/// A merged PR as `gh` reports it: `state` MERGED, `mergeable` gone UNKNOWN.
fn pr_merged() -> &'static str {
    r#"{"state":"MERGED","mergeable":"UNKNOWN","statusCheckRollup":[{"__typename":"CheckRun","name":"build","status":"COMPLETED","conclusion":"SUCCESS"}]}"#
}

// ==== ac-10-1: baseline first, then conflict / review / CI each fire once =======

#[test]
fn ac10_1_baseline_then_each_transition_fires_exactly_once() {
    let fake = FakeGh::new();
    // pr: clean → conflicting → conflicting → conflicting+CI-failure (poll4 clamps
    // to poll3 = a stable failure state → no further edge).
    fake.pr(0, pr_mergeable_ci_success());
    fake.pr(1, pr_conflicting_ci_success());
    fake.pr(2, pr_conflicting_ci_success());
    fake.pr(3, pr_conflicting_ci_failure());
    // reviews: empty until poll2 introduces a new review (id 100); clamp repeats.
    fake.reviews(0, "[]");
    fake.reviews(1, "[]");
    fake.reviews(2, r#"[{"id":100}]"#);

    let result = run_adapter(&fake, config(5, Value::Null), &[]);
    assert_eq!(
        result.code,
        Some(0),
        "clean exit; stderr: {}",
        result.stderr
    );

    assert_eq!(
        result.edge_count("mergeable_conflicting"),
        1,
        "conflict fires exactly once"
    );
    assert_eq!(
        result.edge_count("new_reviews"),
        1,
        "a review transition fires exactly once"
    );
    assert_eq!(
        result.edge_count("ci_failure"),
        1,
        "a CI transition into failure fires exactly once"
    );
    assert_eq!(
        result.publish_count(),
        3,
        "exactly three edges total (first poll baselined, last poll was stable)"
    );

    // The CI edge carries the newly-failed check name + the rollup (item J data).
    let ci = result
        .messages()
        .into_iter()
        .find(|m| m["body"]["edge"] == "ci_failure")
        .expect("a ci_failure event");
    assert_eq!(ci["body"]["rollup"], "failure");
    assert_eq!(ci["body"]["previous"], "success");
    assert_eq!(ci["body"]["newly_failed"], json!(["build"]));

    // The new-review edge carries the id delta.
    let review = result
        .messages()
        .into_iter()
        .find(|m| m["body"]["edge"] == "new_reviews")
        .expect("a new_reviews event");
    assert_eq!(review["body"]["previous_max_id"], 0);
    assert_eq!(review["body"]["current_max_id"], 100);

    let baseline = result.last_baseline().expect("a persisted baseline");
    assert_eq!(baseline["mergeable"], "conflicting");
    assert_eq!(baseline["ci"], "failure");
    assert_eq!(baseline["max_review_id"], 100);
}

// ==== ac-10-2: restart with the persisted baseline → no duplicate events ========

#[test]
fn ac10_2_restart_with_persisted_baseline_refires_nothing() {
    let first = FakeGh::new();
    first.pr(0, pr_mergeable_ci_success());
    first.pr(1, pr_conflicting_ci_success());
    let run1 = run_adapter(&first, config(2, Value::Null), &[]);
    assert_eq!(run1.code, Some(0));
    assert_eq!(
        run1.edge_count("mergeable_conflicting"),
        1,
        "run #1 fires the conflict once"
    );
    let persisted = run1
        .last_baseline()
        .expect("run #1 persisted a baseline via protocol");
    assert_eq!(persisted["mergeable"], "conflicting");

    // Run #2 (the "restart"): a FRESH fake whose very first poll is the SAME
    // conflicting state, with run #1's baseline injected. Re-fires NOTHING.
    let second = FakeGh::new();
    second.pr(0, pr_conflicting_ci_success());
    second.pr(1, pr_conflicting_ci_success());
    let run2 = run_adapter(&second, config(2, persisted), &[]);
    assert_eq!(run2.code, Some(0), "stderr: {}", run2.stderr);
    assert_eq!(
        run2.publish_count(),
        0,
        "a restart from the persisted baseline must not re-fire already-baselined edges"
    );
}

// ==== merge: fires exactly once, and a restart re-fires nothing =================

#[test]
fn merge_fires_once_and_does_not_refire_on_restart() {
    let fake = FakeGh::new();
    // open → open → merged (poll3+ clamp to the merged state → no further edge).
    fake.pr(0, pr_mergeable_ci_success());
    fake.pr(1, pr_mergeable_ci_success());
    fake.pr(2, pr_merged());

    let result = run_adapter(&fake, config(4, Value::Null), &[]);
    assert_eq!(
        result.code,
        Some(0),
        "clean exit; stderr: {}",
        result.stderr
    );
    assert_eq!(
        result.edge_count("pr_merged"),
        1,
        "a merge fires exactly once across repeated merged polls"
    );

    let merged = result
        .messages()
        .into_iter()
        .find(|m| m["body"]["edge"] == "pr_merged")
        .expect("a pr_merged event");
    assert_eq!(merged["body"]["source"], "github-pr");
    assert_eq!(merged["body"]["pr"], 42);

    let baseline = result.last_baseline().expect("a persisted baseline");
    assert_eq!(baseline["merged"], true, "the baseline latches merged");

    // Restart: a fresh fake whose first poll is already merged, with the persisted
    // baseline injected. The latched `merged` flag suppresses a duplicate.
    let second = FakeGh::new();
    second.pr(0, pr_merged());
    second.pr(1, pr_merged());
    let run2 = run_adapter(&second, config(2, baseline), &[]);
    assert_eq!(run2.code, Some(0), "stderr: {}", run2.stderr);
    assert_eq!(
        run2.edge_count("pr_merged"),
        0,
        "a restart from a merged baseline must not re-fire the merge"
    );
}

// ==== ac-10-3: UNKNOWN mergeable flapping → zero events ==========================

#[test]
fn ac10_3_unknown_flapping_produces_zero_events() {
    let fake = FakeGh::new();
    let unknown = r#"{"state":"OPEN","mergeable":"UNKNOWN","statusCheckRollup":[]}"#;
    let mergeable = r#"{"state":"OPEN","mergeable":"MERGEABLE","statusCheckRollup":[]}"#;
    fake.pr(0, mergeable);
    fake.pr(1, unknown);
    fake.pr(2, unknown);
    fake.pr(3, mergeable);
    fake.pr(4, unknown);

    let result = run_adapter(&fake, config(5, Value::Null), &[]);
    assert_eq!(result.code, Some(0), "stderr: {}", result.stderr);
    assert_eq!(
        result.publish_count(),
        0,
        "UNKNOWN flapping must produce zero events"
    );
}

// ==== ac-10-4: a rate-limit response backs off + retries, not a tight loop ======

#[test]
fn ac10_4_rate_limit_backs_off_and_retries() {
    let fake = FakeGh::new();
    fake.pr_directive(0, "@RATE");
    fake.pr(1, pr_mergeable_ci_success());

    let backoff_ms = 200;
    let started = Instant::now();
    let result = run_adapter(
        &fake,
        config(1, Value::Null),
        &[("MAILBOX_GH_RATE_LIMIT_BACKOFF_MS", &backoff_ms.to_string())],
    );
    let elapsed = started.elapsed();

    assert_eq!(result.code, Some(0), "stderr: {}", result.stderr);
    assert_eq!(result.publish_count(), 0, "first poll baselines silently");
    assert!(
        elapsed.as_millis() as u64 >= backoff_ms,
        "the adapter must sleep the backoff before retrying (elapsed {elapsed:?} < {backoff_ms}ms)"
    );
    assert_eq!(
        fake.pr_call_count(),
        2,
        "exactly one retry after the rate limit — not a tight spin"
    );
}

// ==== review item G: a persistent rate limit surfaces (non-zero exit) ===========

#[test]
fn persistent_rate_limit_exits_non_zero() {
    let fake = FakeGh::new();
    // Every pr poll is rate-limited (clamped), so the retry budget is exhausted.
    fake.pr_directive(0, "@RATE");

    let result = run_adapter(
        &fake,
        config(0, Value::Null),
        &[
            ("MAILBOX_GH_RATE_LIMIT_BACKOFF_MS", "10"),
            ("MAILBOX_GH_MAX_RATE_LIMIT_RETRIES", "3"),
        ],
    );
    assert_ne!(
        result.code,
        Some(0),
        "a persistent rate limit must surface via a non-zero exit"
    );
    assert!(
        result.stderr.to_lowercase().contains("rate"),
        "the failure should mention the rate limit; stderr: {}",
        result.stderr
    );
}

// ==== review item A: a partial gh body is transient — no false edges, baseline
//      preserved, poll skipped (not fatal) ======================================

#[test]
fn partial_gh_response_is_transient_and_preserves_baseline() {
    let fake = FakeGh::new();
    // poll0: a structurally-valid-but-INCOMPLETE pr view (missing statusCheckRollup)
    // → strict parse rejects it → transient skip. poll1: a real conflicting state.
    fake.pr(0, r#"{"state":"OPEN","mergeable":"MERGEABLE"}"#);
    fake.pr(1, pr_conflicting_ci_success());
    // A new review is present from the start (id 9), so the successful poll's diff
    // runs against the INJECTED cursor (5), not a reset-to-zero one.
    fake.reviews(0, r#"[{"id":9}]"#);

    // Inject a baseline that already says conflicting + max_review_id 5. If the
    // partial poll had (buggily) reset the baseline, poll1 would re-fire a conflict
    // and a new_reviews from 0; instead it must fire ONLY new_reviews 5→9.
    let injected = json!({ "mergeable": "conflicting", "max_review_id": 5 });
    let result = run_adapter(&fake, config(1, injected), &[]);

    assert_eq!(
        result.code,
        Some(0),
        "one partial response must NOT be fatal; stderr: {}",
        result.stderr
    );
    assert_eq!(
        result.edge_count("mergeable_conflicting"),
        0,
        "the injected conflicting baseline survived the transient poll (no re-fire)"
    );
    assert_eq!(
        result.edge_count("new_reviews"),
        1,
        "the diff ran against the injected cursor (5→9), proving no reset to zero"
    );
}

#[test]
fn persistent_partial_gh_response_eventually_exits_non_zero() {
    let fake = FakeGh::new();
    // Every pr poll is incomplete (clamped) → persistent transient failures.
    fake.pr(0, r#"{"state":"OPEN","mergeable":"MERGEABLE"}"#);

    let result = run_adapter(
        &fake,
        config(0, Value::Null),
        &[("MAILBOX_GH_MAX_TRANSIENT_FAILURES", "3")],
    );
    assert_ne!(
        result.code,
        Some(0),
        "persistent schema drift must surface, not become a silent black hole"
    );
    assert_eq!(result.publish_count(), 0, "no false edges from a bad body");
    assert_eq!(
        result.baseline_count(),
        0,
        "and no zero-baseline was written"
    );
}

// ==== gh auth missing → the adapter exits non-zero ==============================

#[test]
fn gh_auth_missing_exits_non_zero() {
    let fake = FakeGh::new();
    fake.pr_directive(0, "@AUTH");

    let result = run_adapter(&fake, config(1, Value::Null), &[]);
    assert_ne!(
        result.code,
        Some(0),
        "gh auth missing must make the adapter exit non-zero"
    );
    assert!(
        result.stderr.to_lowercase().contains("authentication"),
        "the failure should mention authentication; stderr: {}",
        result.stderr
    );
}
