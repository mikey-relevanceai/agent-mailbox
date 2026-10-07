//! The **Slack adapter**: polls one Slack channel's top level, or one thread, and
//! publishes each new message (design/02).
//!
//! It imports **only** `mailbox-protocol`, never the `mailbox` bridge crate
//! (ADR-0001). It reaches Slack through `curl` ([`api`]) with a token it reads
//! from the macOS Keychain itself ([`token`], ADR-0027); the bridge never sees
//! the token.
//!
//! # Edge-triggered, baseline-via-protocol
//!
//! The same contract as `github-pr`: with no injected baseline the first poll
//! records the newest message and publishes nothing; after that each poll
//! publishes the messages posted since the cursor, THEN emits the advanced
//! cursor as a `Baseline` line. Publishing first means a crash between the two
//! re-fires a message rather than dropping it.
//!
//! # Config schema (the FIRST NDJSON line on stdin)
//!
//! ```json
//! {
//!   "topic": "slack.thread.C0C83CXLUL8/1791349480.652779",
//!   "channel": "C0C83CXLUL8",
//!   "thread_ts": "1791349480.652779",
//!   "interval_ms": 60000,
//!   "baseline": null,
//!   "max_polls": 0
//! }
//! ```
//!
//! - `thread_ts` absent or `null` ⇒ a channel watch.
//! - `interval_ms` absent or `0` ⇒ [`DEFAULT_INTERVAL_MS`].
//! - `max_polls` stops cleanly after that many polls; `0` ⇒ until SIGTERM.
//!
//! # Failure modes
//!
//! - No token, a rejected token, or a channel the bot cannot read → exit
//!   non-zero; the supervisor backs off, gives up and says so once (ADR-0023).
//! - Rate limited → wait Slack's `Retry-After` and retry, a bounded number of times.
//! - Offline or a Slack 5xx → skip the poll; only a long streak exits non-zero.

mod api;
mod message;
mod token;
mod watcher;

use std::io::BufRead;
use std::process::ExitCode;
use std::time::Duration;

use serde::Deserialize;
use serde_json::Value;
use tokio::io::{AsyncWriteExt, Stdout};
use tokio::signal::unix::{Signal, SignalKind, signal};
use tokio::time::MissedTickBehavior;
use tracing::{Instrument, error, info, info_span, warn};

use mailbox_protocol::{
    AdapterId, Baseline as BaselineMsg, Message, Publish, SlackChannelId, SlackTargetError,
    SlackTs, SlackWatch, Topic, encode_line,
};

use api::{CurlSlack, SlackApi, SlackError};
use watcher::{Baseline, NewMessage, Watcher};

/// One poll a minute is about 2% of Slack's Tier 3 budget for an internal app
/// (design/02), and soon enough for agents chatting in a channel.
const DEFAULT_INTERVAL_MS: u64 = 60_000;

/// Wait after a rate limit that came with no `Retry-After`.
const DEFAULT_RATE_LIMIT_BACKOFF_MS: u64 = 30_000;
const ENV_RATE_LIMIT_BACKOFF_MS: &str = "MAILBOX_SLACK_RATE_LIMIT_BACKOFF_MS";

/// Consecutive rate limits before exiting non-zero, so a persistent limit
/// surfaces through the supervisor instead of hiding behind a live process.
const DEFAULT_MAX_RATE_LIMIT_RETRIES: u64 = 12;
const ENV_MAX_RATE_LIMIT_RETRIES: &str = "MAILBOX_SLACK_MAX_RATE_LIMIT_RETRIES";

/// Consecutive skipped polls before exiting non-zero. Ten minutes offline at the
/// default interval is a lid closed on a train, not a broken watch; longer than
/// that and the supervisor's give-up notice is the more useful signal.
const DEFAULT_MAX_TRANSIENT_FAILURES: u64 = 10;
const ENV_MAX_TRANSIENT_FAILURES: &str = "MAILBOX_SLACK_MAX_TRANSIENT_FAILURES";

/// The config line is small; this only bounds a hostile or broken one.
const MAX_CONFIG_BYTES: u64 = 64 * 1024;

/// Self-reported provenance. The host stamps its own spawn identity instead.
const ADAPTER_ID: &str = "slack-adapter";

#[derive(Debug, Deserialize)]
struct Config {
    topic: String,
    channel: String,
    #[serde(default)]
    thread_ts: Option<String>,
    #[serde(default)]
    interval_ms: u64,
    #[serde(default)]
    baseline: Value,
    #[serde(default)]
    max_polls: u64,
}

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
    #[error("config names an invalid Slack target: {0}")]
    InvalidTarget(#[from] SlackTargetError),
    // The resolver derives both from the same watch row; disagreement means the
    // bridge and this adapter have drifted, and publishing would be rejected.
    #[error("config topic {topic:?} does not match the Slack target's topic {expected:?}")]
    TopicMismatch { topic: String, expected: String },
    #[error("could not install the {0} signal handler: {1}")]
    Signal(&'static str, #[source] std::io::Error),
    #[error(transparent)]
    Token(#[from] token::TokenError),
    #[error(transparent)]
    Slack(#[from] SlackError),
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
            error!(error = %err, "slack adapter exited with an error");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<(), AdapterError> {
    let config = read_config(std::io::stdin().lock())?;
    let watch = watch_of(&config)?;
    let topic = watch.topic();
    // Every line from here on carries the watch's topic, so a warning in the
    // bridge's log (where several adapters' stderr meet) names its watch.
    let span = info_span!("slack_watch", topic = topic.as_str());
    watch_loop(config, watch, topic).instrument(span).await
}

async fn watch_loop(config: Config, watch: SlackWatch, topic: Topic) -> Result<(), AdapterError> {
    let interval = Duration::from_millis(if config.interval_ms == 0 {
        DEFAULT_INTERVAL_MS
    } else {
        config.interval_ms
    });
    let limits = Limits::from_env();

    let mut sigterm =
        signal(SignalKind::terminate()).map_err(|e| AdapterError::Signal("SIGTERM", e))?;
    let mut sigint =
        signal(SignalKind::interrupt()).map_err(|e| AdapterError::Signal("SIGINT", e))?;
    let mut stdout = tokio::io::stdout();

    let token = token::load().await?;
    let mut watcher = Watcher::new(CurlSlack::new(token), watch);
    let mut baseline = injected_baseline(config.baseline);

    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    info!(
        interval_ms = interval.as_millis() as u64,
        resuming = baseline.is_some(),
        max_polls = config.max_polls,
        "slack adapter started"
    );

    let mut polls: u64 = 0;
    let mut consecutive_transient: u64 = 0;
    loop {
        if config.max_polls != 0 && polls >= config.max_polls {
            info!(polls, "reached configured max_polls; exiting cleanly");
            return Ok(());
        }
        tokio::select! {
            _ = sigterm.recv() => return shutdown(polls, "SIGTERM"),
            _ = sigint.recv() => return shutdown(polls, "SIGINT"),
            _ = ticker.tick() => {}
        }

        let step = match step_with_backoff(
            &mut watcher,
            baseline.as_ref(),
            &limits,
            &mut sigterm,
            &mut sigint,
        )
        .await
        {
            Outcome::Done(step) => {
                consecutive_transient = 0;
                step
            }
            Outcome::Signalled(name) => return shutdown(polls, name),
            Outcome::Transient(err) => {
                consecutive_transient += 1;
                warn!(
                    consecutive_transient,
                    max = limits.max_transient,
                    error = %err,
                    "skipped a poll on a transient Slack failure; cursor unchanged"
                );
                if consecutive_transient >= limits.max_transient {
                    error!(
                        consecutive_transient,
                        polls,
                        "persistent transient Slack failures; exiting so the supervisor surfaces it"
                    );
                    return Err(err.into());
                }
                continue;
            }
            Outcome::Fatal(err) => return Err(err.into()),
        };

        match step {
            Step::Baselined(base) => {
                emit_baseline(&mut stdout, &base).await?;
                info!(last_ts = %base.last_ts, "baselined on first poll; published nothing");
                baseline = Some(base);
            }
            Step::Polled(next, messages) => {
                let label = watcher.channel_label();
                for message in &messages {
                    publish(&mut stdout, &topic, watcher.watch(), &label, message).await?;
                }
                // After the publishes, so the persisted cursor never runs ahead of
                // what is already on the bus.
                let changed = baseline.as_ref() != Some(&next);
                if changed {
                    emit_baseline(&mut stdout, &next).await?;
                    baseline = Some(next);
                }
                info!(
                    published = messages.len(),
                    cursor_moved = changed,
                    "polled Slack"
                );
            }
        }
        polls += 1;
    }
}

/// Retry budgets, from the environment so tests can shrink them.
struct Limits {
    rate_limit_backoff: Duration,
    max_rate_limit_retries: u64,
    max_transient: u64,
}

impl Limits {
    fn from_env() -> Self {
        Self {
            rate_limit_backoff: Duration::from_millis(env_u64(
                ENV_RATE_LIMIT_BACKOFF_MS,
                DEFAULT_RATE_LIMIT_BACKOFF_MS,
            )),
            max_rate_limit_retries: env_u64(
                ENV_MAX_RATE_LIMIT_RETRIES,
                DEFAULT_MAX_RATE_LIMIT_RETRIES,
            ),
            max_transient: env_u64(ENV_MAX_TRANSIENT_FAILURES, DEFAULT_MAX_TRANSIENT_FAILURES),
        }
    }
}

/// What one successful poll produced.
enum Step {
    /// No cursor yet: this is it, and nothing is published.
    Baselined(Baseline),
    /// The advanced cursor and the messages that wake.
    Polled(Baseline, Vec<NewMessage>),
}

enum Outcome {
    Done(Step),
    Signalled(&'static str),
    Transient(SlackError),
    Fatal(SlackError),
}

async fn step<A: SlackApi>(
    watcher: &mut Watcher<A>,
    baseline: Option<&Baseline>,
) -> Result<Step, SlackError> {
    match baseline {
        None => watcher.baseline().await.map(Step::Baselined),
        Some(prior) => watcher
            .poll(prior)
            .await
            .map(|(next, messages)| Step::Polled(next, messages)),
    }
}

/// Run one step, waiting out rate limits (bounded), racing signals so a slow
/// Slack or a long backoff never delays shutdown.
async fn step_with_backoff<A: SlackApi>(
    watcher: &mut Watcher<A>,
    baseline: Option<&Baseline>,
    limits: &Limits,
    sigterm: &mut Signal,
    sigint: &mut Signal,
) -> Outcome {
    let mut retries: u64 = 0;
    loop {
        let result = tokio::select! {
            _ = sigterm.recv() => return Outcome::Signalled("SIGTERM"),
            _ = sigint.recv() => return Outcome::Signalled("SIGINT"),
            result = step(watcher, baseline) => result,
        };
        let retry_after = match result {
            Ok(step) => return Outcome::Done(step),
            Err(SlackError::RateLimited { retry_after }) => retry_after,
            Err(err @ SlackError::Transient(_)) => return Outcome::Transient(err),
            // Listed, not wildcarded: a new error class must decide here whether
            // it is retried, skipped or fatal.
            Err(
                err @ (SlackError::Auth(_)
                | SlackError::NotInChannel
                | SlackError::Access(_)
                | SlackError::Failed(_)
                | SlackError::Spawn { .. }),
            ) => return Outcome::Fatal(err),
        };
        retries += 1;
        if retries > limits.max_rate_limit_retries {
            return Outcome::Fatal(SlackError::Failed(format!(
                "still rate-limited after {} retries",
                limits.max_rate_limit_retries
            )));
        }
        let wait = retry_after.unwrap_or(limits.rate_limit_backoff);
        warn!(
            wait_ms = wait.as_millis() as u64,
            retry = retries,
            from_slack = retry_after.is_some(),
            "Slack rate-limited the poll; waiting before retrying"
        );
        tokio::select! {
            _ = sigterm.recv() => return Outcome::Signalled("SIGTERM"),
            _ = sigint.recv() => return Outcome::Signalled("SIGINT"),
            _ = tokio::time::sleep(wait) => {}
        }
    }
}

fn shutdown(polls: u64, signal: &str) -> Result<(), AdapterError> {
    info!(polls, signal, "received signal; exiting cleanly");
    Ok(())
}

/// The supervisor-injected cursor. One this adapter cannot read is dropped with a
/// warning: re-baselining loses at most the messages posted while it was down,
/// where crashing on it would lose the watch.
fn injected_baseline(value: Value) -> Option<Baseline> {
    if value.is_null() {
        return None;
    }
    let parsed = Baseline::from_json(value);
    if parsed.is_none() {
        warn!("could not parse the injected baseline; re-baselining on the first poll");
    }
    parsed
}

fn watch_of(config: &Config) -> Result<SlackWatch, AdapterError> {
    let channel = SlackChannelId::parse(&config.channel)?;
    let watch = match &config.thread_ts {
        None => SlackWatch::Channel { channel },
        Some(ts) => SlackWatch::Thread {
            channel,
            thread_ts: SlackTs::parse(ts)?,
        },
    };
    let expected = watch.topic();
    if config.topic != expected.as_str() {
        return Err(AdapterError::TopicMismatch {
            topic: config.topic.clone(),
            expected: expected.as_str().to_string(),
        });
    }
    Ok(watch)
}

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

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
    serde_json::from_str(line.trim()).map_err(AdapterError::ParseConfig)
}

async fn publish(
    stdout: &mut Stdout,
    topic: &Topic,
    watch: &SlackWatch,
    channel_label: &str,
    message: &NewMessage,
) -> Result<(), AdapterError> {
    let line = Message::Publish(Publish {
        topic: topic.clone(),
        adapter: AdapterId(ADAPTER_ID.to_string()),
        body: message.body(watch),
        subject: message.subject(watch, channel_label),
    });
    write_message(stdout, &line).await?;
    info!(
        ts = %message.ts,
        user = message.user.as_ref().map_or("", |user| user.as_str()),
        bot_id = message.bot_id.as_deref().unwrap_or(""),
        subtype = message.subtype.as_ref().map_or("", |subtype| subtype.as_str()),
        "published a new Slack message"
    );
    Ok(())
}

async fn emit_baseline(stdout: &mut Stdout, baseline: &Baseline) -> Result<(), AdapterError> {
    write_message(
        stdout,
        &Message::Baseline(BaselineMsg {
            value: baseline.to_json(),
        }),
    )
    .await
}

/// One NDJSON line, flushed: stdout is block-buffered when piped, and the host
/// must see each line promptly and in order.
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

/// Tracing to stderr, filtered by `RUST_LOG`; the host drains it into its own
/// logs. Message text is never logged.
fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .try_init();
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(line: &str) -> Config {
        read_config(line.as_bytes()).unwrap()
    }

    #[test]
    fn a_config_without_thread_ts_is_a_channel_watch() {
        let watch = watch_of(&config(
            r#"{"topic":"slack.channel.C0C83CXLUL8","channel":"C0C83CXLUL8"}"#,
        ))
        .unwrap();
        assert!(matches!(watch, SlackWatch::Channel { .. }));
    }

    #[test]
    fn a_config_with_thread_ts_is_a_thread_watch() {
        let watch = watch_of(&config(
            r#"{"topic":"slack.thread.C0C83CXLUL8/1791349480.652779","channel":"C0C83CXLUL8","thread_ts":"1791349480.652779"}"#,
        ))
        .unwrap();
        assert_eq!(watch.key(), "C0C83CXLUL8/1791349480.652779");
    }

    #[test]
    fn a_topic_that_disagrees_with_the_target_is_refused() {
        let err = watch_of(&config(
            r#"{"topic":"slack.channel.C999","channel":"C0C83CXLUL8"}"#,
        ))
        .unwrap_err();
        assert!(matches!(err, AdapterError::TopicMismatch { .. }));
    }

    #[test]
    fn empty_and_garbage_config_are_typed_errors() {
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
    fn an_unreadable_injected_baseline_rebaselines() {
        assert!(injected_baseline(Value::Null).is_none());
        assert!(injected_baseline(serde_json::json!({"last_ts": 5})).is_none());
        assert!(injected_baseline(serde_json::json!({"last_ts": "1.000001"})).is_some());
    }
}
