//! The reference **stub adapter**: a trivial, real, spawnable program that
//! publishes a synthetic event on a fixed interval.
//!
//! # Why it exists
//!
//! It proves the whole adapter path — the card-02 protocol + framing, the
//! card-07 transport contract (config on stdin, `Publish` on stdout), the
//! card-08 supervision, wake, cursors, and multi-subscriber fan-out — WITHOUT
//! any real poller (GitHub etc.) existing yet. It is the canonical happy-path
//! adapter the supervision tests drive, and a hand-runnable toy for experiments.
//!
//! It imports **only** `mailbox-protocol` — never the `mailbox` bridge crate. An
//! adapter is a separate process that speaks the wire protocol and never touches
//! the bridge's storage or wake logic (ADR-0001).
//!
//! # Config schema (the FIRST NDJSON line on stdin)
//!
//! The transport (card 07) delivers this adapter's config as one JSON object on
//! the first line of stdin, then closes stdin (EOF). The object is opaque to the
//! host — this adapter defines its own schema:
//!
//! ```json
//! { "topic": "stub.demo", "interval_ms": 500, "count": 10 }
//! ```
//!
//! - `topic` (required) — the [`Topic`] to publish on. The bridge's stub resolver
//!   passes `stub.<label>`; a by-hand run may pass any valid topic.
//! - `interval_ms` (optional) — delay between publishes. Absent or `0` means
//!   [`DEFAULT_INTERVAL_MS`] — never a busy loop, even if misconfigured.
//! - `count` (optional) — how many events to publish. Absent or `0` means publish
//!   **forever** (until SIGTERM). A finite count exits cleanly (0) when reached.
//!
//! It is NOT wrapped in a `mailbox-protocol` version frame: the host does not
//! interpret the config, so it is the adapter's own plain JSON (opaque-body
//! principle, ADR-0001).
//!
//! # Output & lifecycle
//!
//! Every `interval_ms` it writes one `mailbox_protocol::Publish` as an NDJSON
//! line to stdout (the transport forwards it onto the durable bus). Bodies are
//! deliberately tiny (`{"source":"stub","seq":N}`) — an adapter body is opaque
//! content and must never be large (ADR-0001, "never log huge bodies").
//!
//! On **SIGTERM** (bridge teardown) or **SIGINT** (a by-hand Ctrl-C) it exits
//! promptly with code 0, so the supervisor's group teardown (SIGTERM → grace →
//! SIGKILL) is clean and an interactive run exits cleanly rather than dying by
//! signal. tokio installs the handlers, so no `unsafe` is needed (the workspace
//! denies it), and the stdout write is async + raced against the signals so host
//! backpressure can never wedge a clean shutdown. The config read is byte-capped
//! so a malformed producer cannot drive an unbounded allocation.
//!
//! # By hand (for experiments)
//!
//! ```text
//! echo '{"topic":"stub.demo","interval_ms":500,"count":3}' | mailbox-stub-adapter
//! ```
//!
//! prints three `Publish` lines, 500 ms apart, then exits 0. Pipe it into
//! `jq` to inspect, or run it under a `mailbox watch stub` supervised by the
//! bridge (see `adapters/README.md`).

use std::io::BufRead;
use std::process::ExitCode;
use std::time::Duration;

use serde::Deserialize;
use tokio::io::{AsyncWriteExt, Stdout};
use tokio::signal::unix::{SignalKind, signal};
use tracing::{debug, error, info};

use mailbox_protocol::{AdapterId, FramingError, Message, Publish, Topic, TopicError, encode_line};

/// Interval used when config omits `interval_ms` or sets it to `0`. A sane floor
/// so a missing/zero interval is a slow heartbeat, never a 100%-CPU busy loop.
const DEFAULT_INTERVAL_MS: u64 = 1000;

/// Hard cap on the config line read from stdin. Config is a small JSON object
/// (topic + two integers); anything past this is a malformed or hostile producer,
/// so we refuse rather than let an uncapped read allocate unbounded memory.
const MAX_CONFIG_BYTES: u64 = 64 * 1024;

/// Provenance label this adapter self-reports on the wire. The transport stamps
/// its OWN spawn identity onto the durable event and ignores this (provenance is
/// controlled by who started the adapter, not self-asserted) — it is here only so
/// a hand-run `Publish` line is well formed and legible.
const ADAPTER_ID: &str = "stub-adapter";

/// The adapter's own config, parsed once from the first stdin line via serde.
///
/// A typed value (not ad-hoc `Value` poking) so a malformed config is a clean
/// typed error at the edge, not a panic deep in the loop.
#[derive(Debug, Deserialize)]
struct StubConfig {
    /// The topic to publish on (required).
    topic: String,
    /// Delay between publishes in milliseconds; `0`/absent ⇒ [`DEFAULT_INTERVAL_MS`].
    #[serde(default)]
    interval_ms: u64,
    /// How many events to publish; `0`/absent ⇒ forever.
    #[serde(default)]
    count: u64,
}

/// A stub adapter failure. Business errors are values in a `Result`, never
/// panics (type-driven design). A clean SIGTERM/`count` exit is `Ok(())`; these
/// are the genuine faults that make the process exit non-zero.
#[derive(Debug, thiserror::Error)]
enum StubError {
    #[error("could not read the config line from stdin: {0}")]
    ReadConfig(#[source] std::io::Error),

    #[error("no config line on stdin (expected one JSON object as the first line)")]
    EmptyConfig,

    #[error("config line exceeds the {0}-byte limit")]
    ConfigTooLarge(u64),

    #[error("could not parse the config line as JSON: {0}")]
    ParseConfig(#[source] serde_json::Error),

    #[error("config topic {topic:?} is not a valid topic: {source}")]
    InvalidTopic {
        topic: String,
        #[source]
        source: TopicError,
    },

    #[error("could not install the SIGTERM handler: {0}")]
    Signal(#[source] std::io::Error),

    #[error("could not encode a publish line: {0}")]
    Encode(#[from] FramingError),

    #[error("could not write a publish line to stdout: {0}")]
    Write(#[source] std::io::Error),
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    init_tracing();
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            // A start-time/runtime fault (bad config, stdout gone). Log it to
            // stderr (the host drains stderr into its tracing) and fail non-zero.
            error!(error = %err, "stub adapter exiting with an error");
            ExitCode::FAILURE
        }
    }
}

/// Read config, then publish on the interval until `count` is reached or a
/// termination signal (SIGTERM/SIGINT) arrives — whichever comes first. All of
/// those are clean (`Ok(())`) exits.
async fn run() -> Result<(), StubError> {
    let config = read_config(std::io::stdin().lock())?;
    let topic = topic_of(&config)?;
    // Floor the interval so a zero/omitted value is a slow heartbeat, not a busy
    // loop. `count == 0` is the sentinel for "publish forever".
    let interval_ms = if config.interval_ms == 0 {
        DEFAULT_INTERVAL_MS
    } else {
        config.interval_ms
    };
    let count = config.count;

    // Install the signal handlers and the ticker BEFORE announcing readiness, so
    // "started" only fires once init genuinely completed (and SIGTERM is already
    // caught — no window where an early signal kills us by default disposition).
    let mut sigterm = signal(SignalKind::terminate()).map_err(StubError::Signal)?;
    let mut sigint = signal(SignalKind::interrupt()).map_err(StubError::Signal)?;
    // `interval`'s first tick fires immediately, so the first event is published
    // right away (fast, deterministic first delivery for tests); subsequent ticks
    // honour `interval_ms`.
    let mut ticker = tokio::time::interval(Duration::from_millis(interval_ms));
    let mut stdout = tokio::io::stdout();

    info!(
        topic = topic.as_str(),
        interval_ms,
        count,
        forever = count == 0,
        "stub adapter started"
    );

    let mut seq: u64 = 0;
    loop {
        // Wait for the next tick, but let a signal win immediately.
        tokio::select! {
            _ = sigterm.recv() => return shutdown(seq, "SIGTERM"),
            _ = sigint.recv() => return shutdown(seq, "SIGINT"),
            _ = ticker.tick() => {}
        }
        // Publish, but RACE the write against the signals: a host that stops
        // reading stdout backpressures the write, and we must still shut down
        // promptly rather than wedge inside a pending `write_all`.
        tokio::select! {
            _ = sigterm.recv() => return shutdown(seq, "SIGTERM"),
            _ = sigint.recv() => return shutdown(seq, "SIGINT"),
            result = publish(&mut stdout, &topic, seq) => {
                result?;
                seq += 1;
                if count != 0 && seq >= count {
                    info!(published = seq, "reached configured count; exiting");
                    return Ok(());
                }
            }
        }
    }
}

/// A clean, signalled shutdown: log which signal and exit `Ok`. Extracted so the
/// four signal arms in [`run`] share one prompt, clean exit (so the supervisor's
/// group teardown is clean).
fn shutdown(published: u64, signal: &str) -> Result<(), StubError> {
    info!(published, signal, "received signal; exiting cleanly");
    Ok(())
}

/// Parse and validate the config's topic. Split out from [`run`] so the
/// invalid-topic path is unit-testable without spawning the whole loop.
fn topic_of(config: &StubConfig) -> Result<Topic, StubError> {
    Topic::parse(&config.topic).map_err(|source| StubError::InvalidTopic {
        topic: config.topic.clone(),
        source,
    })
}

/// Read and parse the first line of `reader` as the opaque [`StubConfig`],
/// bounded by [`MAX_CONFIG_BYTES`]. The host writes exactly one line then closes
/// stdin, so a single (capped) `read_line` is enough. Taking `impl BufRead` keeps
/// this pure and unit-testable off a byte slice.
fn read_config(reader: impl BufRead) -> Result<StubConfig, StubError> {
    // `take(cap + 1)` so a line at exactly the cap is accepted but anything longer
    // is detected (n > cap) rather than silently truncated — never an uncapped
    // allocation for a hostile producer that streams bytes without a newline.
    let mut limited = reader.take(MAX_CONFIG_BYTES + 1);
    let mut line = String::new();
    let read = limited
        .read_line(&mut line)
        .map_err(StubError::ReadConfig)?;
    if read as u64 > MAX_CONFIG_BYTES {
        return Err(StubError::ConfigTooLarge(MAX_CONFIG_BYTES));
    }
    if read == 0 || line.trim().is_empty() {
        return Err(StubError::EmptyConfig);
    }
    serde_json::from_str(line.trim()).map_err(StubError::ParseConfig)
}

/// Write one synthetic `Publish` as an NDJSON line to `stdout` and flush.
///
/// Async so a host that stops reading (backpressure) pends this future instead of
/// blocking the runtime thread — the caller races it against the shutdown signals
/// so a stuck write can never delay a clean exit. The body is intentionally
/// minimal opaque content (`source` + a monotonic `seq`) — enough to prove
/// fan-out and cursor advance, never a "huge body" the host would truncate.
async fn publish(stdout: &mut Stdout, topic: &Topic, seq: u64) -> Result<(), StubError> {
    let message = Message::Publish(Publish {
        topic: topic.clone(),
        adapter: AdapterId(ADAPTER_ID.to_string()),
        body: serde_json::json!({ "source": "stub", "seq": seq }),
    });
    let line = encode_line(&message)?;
    // stdout is block-buffered when piped, so flush every line to keep ordering
    // deterministic and events promptly visible to the transport.
    stdout
        .write_all(line.as_bytes())
        .await
        .map_err(StubError::Write)?;
    stdout.write_all(b"\n").await.map_err(StubError::Write)?;
    stdout.flush().await.map_err(StubError::Write)?;
    debug!(
        seq,
        topic = topic.as_str(),
        "published synthetic stub event"
    );
    Ok(())
}

/// Route this adapter's own `tracing` events to stderr, filtered by `RUST_LOG`
/// (quiet by default, like the bridge). The host drains our stderr into its own
/// tracing, so these lines surface in the bridge's logs. `try_init` so a repeated
/// init (shouldn't happen, but harmless) is not fatal.
fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .try_init();
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A well-formed config line parses into its typed fields (defaults applied).
    #[test]
    fn read_config_parses_a_valid_line() {
        let input = b"{\"topic\":\"stub.demo\",\"interval_ms\":250,\"count\":3}\n";
        let config = read_config(&input[..]).unwrap();
        assert_eq!(config.topic, "stub.demo");
        assert_eq!(config.interval_ms, 250);
        assert_eq!(config.count, 3);
    }

    /// Optional fields default to 0 (⇒ default interval / publish forever).
    #[test]
    fn read_config_defaults_optional_fields() {
        let config = read_config(&b"{\"topic\":\"stub.min\"}\n"[..]).unwrap();
        assert_eq!(config.interval_ms, 0);
        assert_eq!(config.count, 0);
    }

    /// Empty stdin (EOF with nothing) is a typed `EmptyConfig`, not a panic.
    #[test]
    fn read_config_rejects_empty_input() {
        assert!(matches!(read_config(&b""[..]), Err(StubError::EmptyConfig)));
    }

    /// Garbage that is not JSON is a typed `ParseConfig`.
    #[test]
    fn read_config_rejects_garbage() {
        assert!(matches!(
            read_config(&b"not json at all\n"[..]),
            Err(StubError::ParseConfig(_))
        ));
    }

    /// A line longer than the cap is refused rather than allocated unbounded.
    #[test]
    fn read_config_rejects_oversized_line() {
        // MAX_CONFIG_BYTES + 1 non-newline bytes: over the cap.
        let big = vec![b'x'; (MAX_CONFIG_BYTES + 1) as usize];
        assert!(matches!(
            read_config(&big[..]),
            Err(StubError::ConfigTooLarge(_))
        ));
    }

    /// An invalid topic in an otherwise-valid config is a typed `InvalidTopic`.
    #[test]
    fn topic_of_rejects_invalid_topic() {
        let config = read_config(&b"{\"topic\":\"has space\"}\n"[..]).unwrap();
        assert!(matches!(
            topic_of(&config),
            Err(StubError::InvalidTopic { .. })
        ));
    }
}
