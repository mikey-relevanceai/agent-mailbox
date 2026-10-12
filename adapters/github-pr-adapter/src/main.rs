//! The **GitHub adapter**: the MVP's real poller. In its `github-pr` mode it polls
//! one GitHub pull request via `gh` and publishes ONLY the transitions agents care
//! about — parity with the old `agent-ipc-github`, plus CI. In its
//! `github-review-requests` mode it polls one login's pending review requests and
//! publishes each PR that newly asks for one (see [`review_requests`], design/03).
//! One program serves both because everything but the observe-and-diff step is
//! the same (ADR-0030); [`watcher::Watcher`] is that step.
//!
//! It imports **only** `mailbox-protocol` — never the `mailbox` bridge crate. An
//! adapter is a separate process that speaks the wire protocol and never touches
//! the bridge's storage or wake logic (ADR-0001); it reaches GitHub by shelling
//! out to `gh` (see [`gh`]).
//!
//! # Edge-triggered, baseline-via-protocol (the load-bearing behaviour)
//!
//! On the FIRST poll the adapter *baselines* — records the current state and
//! publishes nothing. Thereafter it publishes only on *transitions* (see
//! [`snapshot::apply`]): mergeable → CONFLICTING (ignoring transient UNKNOWN),
//! new reviews / review threads / PR comments, and CI rollup transitions — or, in
//! review-request mode, a PR entering the requested set
//! ([`review_requests::apply`]).
//!
//! The baseline must survive a restart so an already-fired edge is not re-fired,
//! and it must NOT live in adapter-side SQLite (ADR-0001). So it round-trips
//! through the bridge (design/01, card-10 decision 1): the supervisor injects the
//! last persisted baseline into this adapter's config at spawn (the `baseline`
//! field), and after each poll that changes the baseline the adapter emits a
//! `mailbox_protocol::Baseline` line, which the host relays to storage. A restart
//! therefore resumes exactly where it left off — no duplicate events.
//!
//! # Config schema (the FIRST NDJSON line on stdin)
//!
//! ```json
//! {
//!   "kind": "github-pr",
//!   "topic": "github.pr.octocat/hello-world#42",
//!   "owner": "octocat", "repo": "hello-world", "number": 42,
//!   "interval_ms": 60000,
//!   "baseline": null,
//!   "max_polls": 0
//! }
//! ```
//!
//! or, for review requests,
//! `{"kind": "github-review-requests", "topic": "github.review-requests.octocat",
//! "login": "octocat", …}`.
//!
//! - `kind` (optional) — `github-pr` (the default, so a pre-ADR-0030 resolver's
//!   config still works) or `github-review-requests`.
//! - `topic` (required) — where to publish.
//! - `owner`/`repo`/`number` (required for `github-pr`) — the PR to poll.
//! - `login` (required for `github-review-requests`) — whose requests to poll.
//! - `interval_ms` (optional) — poll cadence; absent or `0` ⇒ [`DEFAULT_INTERVAL_MS`].
//! - `baseline` (injected by the supervisor) — the persisted snapshot, or `null`
//!   for "never baselined" (⇒ baseline on the first poll).
//! - `max_polls` (optional) — stop cleanly (exit 0) after this many completed
//!   polls; `0`/absent ⇒ poll forever (until SIGTERM). Bounds a by-hand or test
//!   run without needing to signal it.
//!
//! # Failure modes (design/01)
//!
//! - `gh` auth missing → exit NON-ZERO (the supervisor records + surfaces it).
//! - GitHub rate limit → back off inside the adapter and retry (still one process).
//! - On SIGTERM/SIGINT → exit promptly with code 0 (clean supervisor teardown).

mod gh;
mod review_requests;
mod snapshot;
mod watcher;

use std::io::BufRead;
use std::process::ExitCode;
use std::time::Duration;

use serde::Deserialize;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use tokio::io::{AsyncWriteExt, Stdout};
use tokio::signal::unix::{Signal, SignalKind, signal};
use tokio::time::{Instant, MissedTickBehavior};
use tracing::{debug, error, info, warn};

use mailbox_protocol::{
    AdapterId, Baseline as BaselineMsg, GithubLogin, Message, Publish, Topic, encode_line,
};

use gh::{Gh, GhClient, GhError};
use watcher::{PrWatcher, ReviewRequestsWatcher, Watcher};

/// Poll cadence used when config omits `interval_ms` or sets it to `0`. ~60s
/// matches design/01's default; a sane floor so a missing interval is a slow
/// poll, never a busy loop hammering the GitHub API.
const DEFAULT_INTERVAL_MS: u64 = 60_000;

/// Default backoff after a rate-limit response before retrying the poll, in
/// milliseconds. Generous (GitHub rate windows are minutes) but overridable via
/// [`ENV_RATE_LIMIT_BACKOFF_MS`] so tests exercise the backoff path fast.
const DEFAULT_RATE_LIMIT_BACKOFF_MS: u64 = 30_000;

/// Env override for the rate-limit backoff (tests set this small).
const ENV_RATE_LIMIT_BACKOFF_MS: &str = "MAILBOX_GH_RATE_LIMIT_BACKOFF_MS";

/// Consecutive rate-limit retries before giving up and exiting non-zero (review
/// item G): a persistent rate limit must SURFACE via the supervisor's give-up
/// path, not spin forever behind a live process that looks healthy.
const DEFAULT_MAX_RATE_LIMIT_RETRIES: u64 = 12;

/// Env override for the rate-limit retry budget (tests set this small).
const ENV_MAX_RATE_LIMIT_RETRIES: &str = "MAILBOX_GH_MAX_RATE_LIMIT_RETRIES";

/// Consecutive transient (parse/incomplete-response) poll failures before giving
/// up and exiting non-zero (review item A): one gh glitch must not tear down a
/// healthy watch, but persistent schema drift must surface rather than becoming a
/// silent black hole.
const DEFAULT_MAX_TRANSIENT_FAILURES: u64 = 10;

/// Env override for the transient-failure budget (tests set this small).
const ENV_MAX_TRANSIENT_FAILURES: &str = "MAILBOX_GH_MAX_TRANSIENT_FAILURES";

/// Hard cap on the config line read from stdin. Config carries the injected
/// baseline, whose worst case is a full-sized `Baseline` line — the adapter emits
/// baselines through the host's stdout per-line cap (1 MiB). This cap MUST be ≥
/// that emit cap (review item F): any baseline we can PERSIST must be RESTORABLE,
/// else a mid-band baseline is stored but un-injectable and the watch crash-loops.
/// 2 MiB leaves head room above the 1 MiB line cap plus the config envelope.
const MAX_CONFIG_BYTES: u64 = 2 * 1024 * 1024;

/// Provenance this adapter self-reports on the wire. The transport stamps its OWN
/// spawn identity onto the durable event and ignores this — provenance is
/// controlled by who started the adapter, not self-asserted.
const ADAPTER_ID: &str = "github-pr-adapter";

/// The adapter's own config, parsed once from the first stdin line.
#[derive(Debug)]
struct Config {
    /// The topic to publish edges on (the resolver passes `github.pr.<o>/<r>#<n>`
    /// or `github.review-requests.<login>`).
    topic: String,
    /// What to poll: the config's `kind` and the fields that kind needs.
    target: Target,
    /// Poll cadence in ms; `0`/absent ⇒ [`DEFAULT_INTERVAL_MS`].
    interval_ms: u64,
    /// The persisted baseline snapshot injected by the supervisor: `null` for
    /// "never baselined", else the mode's baseline object.
    baseline: Value,
    /// Stop cleanly after this many completed polls; `0`/absent ⇒ forever.
    max_polls: u64,
}

/// The fields every mode shares.
#[derive(Debug, Deserialize)]
struct Common {
    topic: String,
    #[serde(default)]
    interval_ms: u64,
    #[serde(default)]
    baseline: Value,
    #[serde(default)]
    max_polls: u64,
}

/// What this process polls. One program serves both GitHub watch kinds
/// (ADR-0030); the resolver names the kind in the config.
#[derive(Debug, PartialEq, Eq)]
enum Target {
    /// One pull request (`kind: "github-pr"`, card 10).
    GithubPr {
        owner: String,
        repo: String,
        number: u64,
    },
    /// One login's review requests (`kind: "github-review-requests"`, design/03).
    GithubReviewRequests { login: GithubLogin },
}

#[derive(Deserialize)]
struct PrFields {
    owner: String,
    repo: String,
    number: u64,
}

#[derive(Deserialize)]
struct ReviewRequestsFields {
    login: GithubLogin,
}

/// A github-pr adapter failure. Business errors are values in a `Result`, never
/// panics (type-driven design). A clean SIGTERM/`max_polls` exit is `Ok(())`;
/// these make the process exit non-zero.
#[derive(Debug, thiserror::Error)]
enum AdapterError {
    #[error("could not read the config line from stdin: {0}")]
    ReadConfig(#[source] std::io::Error),
    #[error("no config line on stdin (expected one JSON object as the first line)")]
    EmptyConfig,
    #[error("config line exceeds the {0}-byte limit")]
    ConfigTooLarge(u64),
    #[error("could not parse the config line as JSON: {0}")]
    ParseConfig(#[source] serde_json::Error),
    #[error("config kind {0:?} is not one this adapter serves")]
    UnknownKind(String),
    #[error("config topic {topic:?} is not a valid topic: {source}")]
    InvalidTopic {
        topic: String,
        #[source]
        source: mailbox_protocol::TopicError,
    },
    #[error("could not install the {0} signal handler: {1}")]
    Signal(&'static str, #[source] std::io::Error),
    /// A fatal `gh` failure (auth missing, bad repo, unparseable output). This is
    /// the design/01 "gh auth missing → exit non-zero" path.
    #[error(transparent)]
    Gh(#[from] GhError),
    #[error("could not encode a line: {0}")]
    Encode(#[from] mailbox_protocol::FramingError),
    #[error("could not write a line to stdout: {0}")]
    Write(#[source] std::io::Error),
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    init_tracing();
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            // A fatal fault (bad config, gh auth missing, gh failure). Log to
            // stderr (the host drains it into its tracing) and fail non-zero so
            // the supervisor records + surfaces it — no silent spin (design/01).
            error!(error = %err, "github adapter exiting with an error");
            ExitCode::FAILURE
        }
    }
}

/// Read config and run the mode it names.
async fn run() -> Result<(), AdapterError> {
    let config = read_config(std::io::stdin().lock())?;
    let topic = topic_of(&config)?;
    let interval = interval_of(&config);
    let gh = Gh::from_env();
    match config.target {
        Target::GithubPr {
            owner,
            repo,
            number,
        } => {
            let watcher = PrWatcher::new(GhClient::new(gh, &owner, &repo, number), owner, repo);
            poll_loop(&watcher, topic, interval, config.baseline, config.max_polls).await
        }
        Target::GithubReviewRequests { login } => {
            let watcher =
                ReviewRequestsWatcher::new(review_requests::Client::new(gh, login.as_str()));
            poll_loop(&watcher, topic, interval, config.baseline, config.max_polls).await
        }
    }
}

/// Poll → diff → publish → persist-baseline until `max_polls` is reached or a
/// termination signal arrives — both clean (`Ok(())`) exits.
async fn poll_loop<W: Watcher>(
    watcher: &W,
    topic: Topic,
    interval: Duration,
    injected_baseline: Value,
    max_polls: u64,
) -> Result<(), AdapterError> {
    let backoff = rate_limit_backoff();
    let max_rate_limit_retries =
        env_u64(ENV_MAX_RATE_LIMIT_RETRIES, DEFAULT_MAX_RATE_LIMIT_RETRIES);
    let max_transient = env_u64(ENV_MAX_TRANSIENT_FAILURES, DEFAULT_MAX_TRANSIENT_FAILURES);
    let watching = watcher.label();

    let mut sigterm =
        signal(SignalKind::terminate()).map_err(|e| AdapterError::Signal("SIGTERM", e))?;
    let mut sigint =
        signal(SignalKind::interrupt()).map_err(|e| AdapterError::Signal("SIGINT", e))?;
    let mut stdout = tokio::io::stdout();

    // The injected baseline: Some(..) means "resume, diff against it"; None means
    // "first poll baselines and publishes nothing".
    let mut baseline: Option<W::Baseline> = parse_injected_baseline(injected_baseline);

    // First tick fires immediately (fast, deterministic first poll for tests);
    // subsequent ticks honour `interval`. Skip missed ticks rather than bursting.
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

    info!(
        topic = topic.as_str(),
        watching = %watching,
        interval_ms = interval.as_millis() as u64,
        resuming = baseline.is_some(),
        max_polls,
        "github adapter started"
    );

    let mut polls: u64 = 0;
    let mut consecutive_transient: u64 = 0;
    loop {
        if max_polls != 0 && polls >= max_polls {
            info!(polls, "reached configured max_polls; exiting cleanly");
            return Ok(());
        }
        tokio::select! {
            _ = sigterm.recv() => return shutdown(polls, "SIGTERM"),
            _ = sigint.recv() => return shutdown(polls, "SIGINT"),
            _ = ticker.tick() => {}
        }

        // Poll, retrying inside the adapter on a rate limit (one process, no tight
        // loop), and letting a signal win so a slow gh cannot delay shutdown.
        let observation = match poll_with_backoff(
            watcher,
            backoff,
            max_rate_limit_retries,
            &mut sigterm,
            &mut sigint,
        )
        .await
        {
            PollOutcome::Observed(obs) => {
                consecutive_transient = 0;
                obs
            }
            PollOutcome::Signalled(signal) => return shutdown(polls, signal),
            PollOutcome::Fatal(err) => return Err(AdapterError::from(err)),
            PollOutcome::Transient(err) => {
                // A malformed/incomplete gh response is TRANSIENT (review item A):
                // do NOT fire, do NOT touch the baseline — skip and retry next
                // interval. Escalate only after a persistent streak, so schema
                // drift surfaces rather than becoming a silent black hole.
                consecutive_transient += 1;
                warn!(
                    watching = %watching,
                    consecutive_transient,
                    max_transient,
                    error = %err,
                    "skipped poll on a transient gh error; baseline unchanged"
                );
                if consecutive_transient >= max_transient {
                    error!(
                        consecutive_transient,
                        "persistent transient gh failures; surfacing via non-zero exit"
                    );
                    return Err(AdapterError::from(err));
                }
                // Do not count a skipped poll toward max_polls; wait the next tick.
                continue;
            }
        };

        match &baseline {
            None => {
                // First poll: record the baseline, publish NOTHING (edge-triggered).
                let base = watcher.first_baseline(&observation);
                emit_baseline(&mut stdout, &base).await?;
                info!(
                    watching = %watching,
                    "baselined on first poll; published nothing"
                );
                baseline = Some(base);
            }
            Some(prior) => {
                let (next, edges) = watcher.apply(prior, &observation);
                for edge in &edges {
                    publish_edge(&mut stdout, watcher, &topic, edge, &observation).await?;
                }
                let changed = &next != prior;
                if changed {
                    // Persist AFTER publishing the edges, so the persisted baseline
                    // only ever reflects edges already on the bus — a restart from
                    // it re-fires nothing (design/01 exactly-once via baseline).
                    emit_baseline(&mut stdout, &next).await?;
                    baseline = Some(next);
                }
                let fired: Vec<&str> = edges.iter().map(|edge| watcher.edge_kind(edge)).collect();
                info!(
                    watching = %watching,
                    fired = edges.len(),
                    edges = ?fired,
                    baseline_changed = changed,
                    "polled and evaluated transitions"
                );
            }
        }
        polls += 1;
    }
}

/// The result of one (possibly rate-limit-retried) poll attempt.
enum PollOutcome<O> {
    Observed(O),
    /// A signal arrived while polling or backing off; shut down with this name.
    Signalled(&'static str),
    /// A fatal gh failure (auth, bad repo, or an exhausted rate-limit budget) —
    /// the adapter must exit non-zero so the supervisor surfaces it.
    Fatal(GhError),
    /// A transient failure (malformed/incomplete gh response): skip this poll and
    /// keep the baseline; the run loop escalates only after a persistent streak.
    Transient(GhError),
}

/// Poll once, backing off and retrying on a rate limit (bounded by
/// `max_rate_limit_retries` — review item G), racing signals so a slow gh or a
/// long backoff never delays a clean shutdown.
async fn poll_with_backoff<W: Watcher>(
    watcher: &W,
    backoff: Duration,
    max_rate_limit_retries: u64,
    sigterm: &mut Signal,
    sigint: &mut Signal,
) -> PollOutcome<W::Observation> {
    let mut rate_limit_retries: u64 = 0;
    loop {
        let result = tokio::select! {
            _ = sigterm.recv() => return PollOutcome::Signalled("SIGTERM"),
            _ = sigint.recv() => return PollOutcome::Signalled("SIGINT"),
            result = watcher.observe() => result,
        };
        match result {
            Ok(observation) => return PollOutcome::Observed(observation),
            // A malformed/incomplete response is transient — surface it to the run
            // loop, which skips the poll and keeps the baseline (never fail-open).
            Err(err @ GhError::Parse(_)) => return PollOutcome::Transient(err),
            Err(GhError::RateLimited(detail)) => {
                rate_limit_retries += 1;
                if rate_limit_retries > max_rate_limit_retries {
                    // A persistent rate limit must SURFACE (review item G), not spin
                    // behind a live-but-stuck process.
                    return PollOutcome::Fatal(GhError::RateLimited(format!(
                        "exhausted {max_rate_limit_retries} rate-limit retries: {detail}"
                    )));
                }
                warn!(
                    backoff_ms = backoff.as_millis() as u64,
                    retry = rate_limit_retries,
                    max = max_rate_limit_retries,
                    detail = %detail,
                    "gh reported a rate limit; backing off before retry (not spinning)"
                );
                let slept_at = Instant::now();
                tokio::select! {
                    _ = sigterm.recv() => return PollOutcome::Signalled("SIGTERM"),
                    _ = sigint.recv() => return PollOutcome::Signalled("SIGINT"),
                    _ = tokio::time::sleep(backoff) => {}
                }
                debug!(
                    waited_ms = slept_at.elapsed().as_millis() as u64,
                    "rate-limit backoff elapsed; retrying poll"
                );
                // loop → retry the whole poll
            }
            // Auth missing / bad repo / spawn failure → fatal (exit non-zero).
            Err(other) => return PollOutcome::Fatal(other),
        }
    }
}

/// A clean, signalled shutdown: log which signal and exit `Ok`.
fn shutdown(polls: u64, signal: &str) -> Result<(), AdapterError> {
    info!(polls, signal, "received signal; exiting cleanly");
    Ok(())
}

/// Parse the supervisor-injected baseline. `null` (or absent) ⇒ `None` (first
/// poll baselines). A snapshot that fails to parse is treated as `None` with a
/// warning — safer to re-baseline (at most one re-fired edge) than to crash.
fn parse_injected_baseline<B: DeserializeOwned>(value: Value) -> Option<B> {
    if value.is_null() {
        return None;
    }
    match serde_json::from_value::<B>(value) {
        Ok(baseline) => Some(baseline),
        Err(err) => {
            warn!(
                error = %err,
                "could not parse the injected baseline; re-baselining on the first poll"
            );
            None
        }
    }
}

/// The poll interval, flooring a zero/omitted value to [`DEFAULT_INTERVAL_MS`].
fn interval_of(config: &Config) -> Duration {
    let ms = if config.interval_ms == 0 {
        DEFAULT_INTERVAL_MS
    } else {
        config.interval_ms
    };
    Duration::from_millis(ms)
}

/// The rate-limit backoff, from [`ENV_RATE_LIMIT_BACKOFF_MS`] else the default.
fn rate_limit_backoff() -> Duration {
    Duration::from_millis(env_u64(
        ENV_RATE_LIMIT_BACKOFF_MS,
        DEFAULT_RATE_LIMIT_BACKOFF_MS,
    ))
}

/// Read a `u64` env override, falling back to `default` when unset/unparseable.
fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

/// Parse and validate the config's topic. Split out so the invalid-topic path is
/// unit-testable without the whole loop.
fn topic_of(config: &Config) -> Result<Topic, AdapterError> {
    Topic::parse(&config.topic).map_err(|source| AdapterError::InvalidTopic {
        topic: config.topic.clone(),
        source,
    })
}

/// Read and parse the first line of `reader` as [`Config`], bounded by
/// [`MAX_CONFIG_BYTES`]. Mirrors the stub adapter's bounded single-line read.
///
/// A config with no `kind` is a PR watch: that is what every resolver before
/// ADR-0030 sent, and a daemon still running one may spawn this binary after it
/// is upgraded and before the daemon is restarted.
fn read_config(reader: impl BufRead) -> Result<Config, AdapterError> {
    let mut limited = reader.take(MAX_CONFIG_BYTES + 1);
    let mut line = String::new();
    let read = limited
        .read_line(&mut line)
        .map_err(AdapterError::ReadConfig)?;
    if read as u64 > MAX_CONFIG_BYTES {
        return Err(AdapterError::ConfigTooLarge(MAX_CONFIG_BYTES));
    }
    if read == 0 || line.trim().is_empty() {
        return Err(AdapterError::EmptyConfig);
    }
    let value: Value = serde_json::from_str(line.trim()).map_err(AdapterError::ParseConfig)?;
    let kind = match value.get("kind") {
        None | Some(Value::Null) => "github-pr".to_string(),
        Some(Value::String(kind)) => kind.clone(),
        Some(other) => return Err(AdapterError::UnknownKind(other.to_string())),
    };
    let target = match kind.as_str() {
        "github-pr" => {
            let PrFields {
                owner,
                repo,
                number,
            } = PrFields::deserialize(&value).map_err(AdapterError::ParseConfig)?;
            Target::GithubPr {
                owner,
                repo,
                number,
            }
        }
        "github-review-requests" => {
            let ReviewRequestsFields { login } =
                ReviewRequestsFields::deserialize(&value).map_err(AdapterError::ParseConfig)?;
            Target::GithubReviewRequests { login }
        }
        _ => return Err(AdapterError::UnknownKind(kind)),
    };
    let Common {
        topic,
        interval_ms,
        baseline,
        max_polls,
    } = Common::deserialize(value).map_err(AdapterError::ParseConfig)?;
    Ok(Config {
        topic,
        target,
        interval_ms,
        baseline,
        max_polls,
    })
}

/// Write one edge as a `Publish` NDJSON line. The body is small opaque content
/// (never a gh dump — ADR-0001); the subject is the one line of it the bridge will
/// show on the wake wire (ADR-0022).
async fn publish_edge<W: Watcher>(
    stdout: &mut Stdout,
    watcher: &W,
    topic: &Topic,
    edge: &W::Edge,
    observation: &W::Observation,
) -> Result<(), AdapterError> {
    let message = Message::Publish(Publish {
        topic: topic.clone(),
        adapter: AdapterId(ADAPTER_ID.to_string()),
        body: watcher.body(edge),
        subject: watcher.subject(edge, observation),
    });
    write_message(stdout, &message).await?;
    // The highest-value line: log the fired edge with its decision data at info
    // (review item J) — the kind plus a concise description (id deltas / CI
    // newly-failed names), never the raw gh body.
    info!(
        edge = watcher.edge_kind(edge),
        detail = %watcher.describe(edge),
        topic = topic.as_str(),
        "published edge"
    );
    Ok(())
}

/// Write the current baseline as a `Baseline` NDJSON line, for the host to relay
/// to storage (baseline-via-protocol).
async fn emit_baseline<B: Serialize>(
    stdout: &mut Stdout,
    baseline: &B,
) -> Result<(), AdapterError> {
    let value = serde_json::to_value(baseline)
        .expect("a Baseline always serializes to JSON (only owned primitives/strings/sets)");
    write_message(stdout, &Message::Baseline(BaselineMsg { value })).await
}

/// Encode `message` as one NDJSON line and flush. stdout is block-buffered when
/// piped, so flush every line to keep ordering deterministic and lines promptly
/// visible to the transport.
async fn write_message(stdout: &mut Stdout, message: &Message) -> Result<(), AdapterError> {
    let line = encode_line(message)?;
    stdout
        .write_all(line.as_bytes())
        .await
        .map_err(AdapterError::Write)?;
    stdout.write_all(b"\n").await.map_err(AdapterError::Write)?;
    stdout.flush().await.map_err(AdapterError::Write)?;
    Ok(())
}

/// Route this adapter's own `tracing` to stderr, filtered by `RUST_LOG` (quiet by
/// default). The host drains our stderr into its tracing, so these surface in the
/// bridge logs. We never log the full gh JSON body — only decisions.
fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .try_init();
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pr_target() -> Target {
        Target::GithubPr {
            owner: "o".to_string(),
            repo: "r".to_string(),
            number: 1,
        }
    }

    #[test]
    fn read_config_parses_a_valid_line() {
        let input =
            br#"{"topic":"github.pr.o/r#1","owner":"o","repo":"r","number":1,"interval_ms":5000}"#;
        let config = read_config(&input[..]).unwrap();
        assert_eq!(
            config.target,
            Target::GithubPr {
                owner: "o".to_string(),
                repo: "r".to_string(),
                number: 1
            },
            "a config with no kind is a PR watch, as every resolver before ADR-0030 sent"
        );
        assert_eq!(config.interval_ms, 5000);
        assert_eq!(config.max_polls, 0);
        assert!(
            config.baseline.is_null(),
            "absent baseline defaults to null"
        );
    }

    #[test]
    fn read_config_rejects_empty_and_garbage() {
        assert!(matches!(
            read_config(&b""[..]),
            Err(AdapterError::EmptyConfig)
        ));
        assert!(matches!(
            read_config(&b"not json\n"[..]),
            Err(AdapterError::ParseConfig(_))
        ));
    }

    #[test]
    fn read_config_parses_a_review_request_watch() {
        let input = br#"{"kind":"github-review-requests","topic":"github.review-requests.octocat","login":"Octocat"}"#;
        let config = read_config(&input[..]).unwrap();
        assert_eq!(
            config.target,
            Target::GithubReviewRequests {
                login: GithubLogin::parse("octocat").unwrap()
            }
        );
    }

    /// The login is spliced into a search query, so the adapter re-parses it
    /// rather than trusting whoever wrote its config.
    #[test]
    fn read_config_refuses_a_login_that_could_change_the_query_and_an_unknown_kind() {
        let bad_login =
            br#"{"kind":"github-review-requests","topic":"t","login":"a review-requested:b"}"#;
        assert!(matches!(
            read_config(&bad_login[..]),
            Err(AdapterError::ParseConfig(_))
        ));
        let unknown = br#"{"kind":"gitlab-mr","topic":"t"}"#;
        assert!(matches!(
            read_config(&unknown[..]),
            Err(AdapterError::UnknownKind(_))
        ));
    }

    #[test]
    fn interval_floors_zero_to_default() {
        let config = Config {
            topic: "t".to_string(),
            target: pr_target(),
            interval_ms: 0,
            baseline: Value::Null,
            max_polls: 0,
        };
        assert_eq!(interval_of(&config).as_millis() as u64, DEFAULT_INTERVAL_MS);
    }

    #[test]
    fn parse_injected_baseline_null_is_none() {
        assert!(parse_injected_baseline::<snapshot::Baseline>(Value::Null).is_none());
    }

    #[test]
    fn parse_injected_baseline_round_trips_a_snapshot() {
        let snapshot = serde_json::json!({
            "mergeable": "conflicting",
            "max_review_id": 2,
            "ci": "failure",
            "failed_checks": ["build"]
        });
        let base = parse_injected_baseline::<snapshot::Baseline>(snapshot).unwrap();
        assert_eq!(base.max_review_id, 2);
        assert_eq!(base.ci, snapshot::CiRollup::Failure);
    }

    /// Review item F: any baseline the adapter can EMIT (bounded to the host's
    /// per-line cap) must be RESTORABLE through the config read cap. A mid-band
    /// baseline (larger than the old 256 KiB config cap) round-trips
    /// emit→persist→inject→read_config without a ConfigTooLarge crash-loop.
    #[test]
    fn mid_band_baseline_round_trips_through_config_read() {
        // A baseline whose serialized size lands between the old 256 KiB config cap
        // and the 1 MiB emit cap (well within MAX_FAILED_CHECKS, ~500 KiB of names).
        let checks: Vec<String> = (0..snapshot::MAX_FAILED_CHECKS)
            .map(|i| format!("{:0>1900}-check-{i}", i))
            .collect();
        let baseline = snapshot::Baseline {
            ci: snapshot::CiRollup::Failure,
            failed_checks: checks,
            ..snapshot::Baseline::default()
        };
        let baseline_json = serde_json::to_value(&baseline).unwrap();
        let baseline_line = serde_json::to_string(&baseline_json).unwrap();
        assert!(
            baseline_line.len() > 256 * 1024,
            "the fixture must exceed the OLD config cap to be a real regression guard: {}",
            baseline_line.len()
        );
        assert!(
            (baseline_line.len() as u64) < MAX_CONFIG_BYTES,
            "and stay under the new config cap"
        );

        let config = serde_json::json!({
            "topic": "github.pr.o/r#1", "owner": "o", "repo": "r", "number": 1,
            "baseline": baseline_json,
        });
        let line = format!("{}\n", serde_json::to_string(&config).unwrap());
        let parsed = read_config(line.as_bytes()).expect("mid-band config must be readable");
        let restored = parse_injected_baseline::<snapshot::Baseline>(parsed.baseline)
            .expect("baseline restores");
        assert_eq!(
            restored, baseline,
            "the mid-band baseline round-trips exactly"
        );
    }

    #[test]
    fn invalid_topic_is_a_typed_error() {
        let config = Config {
            topic: "has space".to_string(),
            target: pr_target(),
            interval_ms: 0,
            baseline: Value::Null,
            max_polls: 0,
        };
        assert!(matches!(
            topic_of(&config),
            Err(AdapterError::InvalidTopic { .. })
        ));
    }
}
