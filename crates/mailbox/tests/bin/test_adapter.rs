//! TEST-ONLY adapter fixture for the card-07 adapter-host tests.
//!
//! This is deliberately a real, spawnable program: the host (`SubprocessTransport`)
//! must be exercised against a genuine child process, so the acceptance tests
//! launch this binary via `CARGO_BIN_EXE_test_adapter`. It is NOT the shipped CLI
//! and NOT the real reference stub adapter (that is a later card).
//!
//! # Roles (argv, checked before reading config)
//!
//! - `--grandchild <marker>` — the process a `spawn_grandchild` run re-execs. It
//!   inherits its parent's stdout (the host's read pipe) and sleeps forever
//!   (default SIGTERM disposition), so it keeps that pipe open until the host's
//!   process-group teardown kills it. The `<marker>` is unique so a test can
//!   `pgrep -f` it.
//! - `--deaf` — never reads stdin and sleeps forever, so a config write larger
//!   than the pipe buffer blocks; the host's config-write timeout must fire.
//!
//! # Modes (the `mode` field of the config read from the first stdin line)
//!
//! - `publish` — write `count` `Publish` NDJSON lines; if `exit_code` is set,
//!   exit with it, else exit 0.
//! - `malformed` — good publish, one malformed line, good publish, then exit 0.
//! - `oversized` — one line of `size` bytes (over the host cap), then a good
//!   publish, then exit 0.
//! - `crash` — write `count` publishes then `abort()` (dies to SIGABRT).
//! - `stderr_flood` — write `stderr_lines` lines to stderr, then one publish.
//! - `spawn_grandchild` — re-exec self as `--grandchild <marker>`, then either
//!   `then: "exit"` (exit 0, grandchild keeps stdout open) or `then: "sleep"`.
//! - `sleep` — block forever, default SIGTERM disposition (graceful stop path).
//! - `interval` — publish an incrementing counter every `interval_ms` (default
//!   50) forever, default SIGTERM disposition. The long-running supervised poller
//!   the card-08 supervision tests drive (both sessions receive events; a kill
//!   triggers a restart; last-interest/stop tears it down).
//! - `ignore_sigterm` — install a SIGTERM handler and ignore it (forceful path).
//!
//! Every `Publish` self-reports a bogus adapter id, so a test can prove the host
//! stamps its OWN identity and ignores what the child claims.

use std::io::{BufRead, Write};

use mailbox_protocol::{AdapterId, GithubPr, Message, Publish, Topic, encode_line};
use serde_json::Value;

/// The identity the fixture puts on the wire. The host is expected to IGNORE
/// this and stamp its own spawn identity instead.
const SELF_REPORTED_ADAPTER: &str = "fixture-self-reported";

fn main() {
    let args: Vec<String> = std::env::args().collect();

    // Grandchild role: identified by a marker in argv so a test can `pgrep -f`
    // it. It inherits the direct child's stdout, so it holds the host's read pipe
    // open — the scenario the process-group teardown must handle.
    if args.iter().any(|a| a == "--grandchild") {
        sleep_forever_respecting_sigterm();
    }
    // Deaf role: never read stdin, so a config write that exceeds the pipe buffer
    // blocks — the host's config-write timeout must fire.
    if args.iter().any(|a| a == "--deaf") {
        sleep_forever_respecting_sigterm();
    }

    let config = read_config_line();

    let mode = config
        .get("mode")
        .and_then(Value::as_str)
        .unwrap_or("publish")
        .to_string();
    let count = config.get("count").and_then(Value::as_u64).unwrap_or(0);
    let topic = config
        .get("topic")
        .and_then(Value::as_str)
        .map(str::to_string)
        .unwrap_or_else(default_topic);
    let topic = Topic::parse(&topic).expect("test fixture given a valid topic");

    if let Some(message) = config.get("stderr").and_then(Value::as_str) {
        eprintln!("{message}");
    }

    match mode.as_str() {
        "publish" => {
            for i in 0..count {
                emit_publish(&topic, i);
            }
            if let Some(code) = config.get("exit_code").and_then(Value::as_i64) {
                std::process::exit(code as i32);
            }
        }
        "malformed" => {
            emit_publish(&topic, 0);
            emit_raw("this is not valid json");
            emit_publish(&topic, 1);
        }
        "oversized" => {
            let size = config
                .get("size")
                .and_then(Value::as_u64)
                .unwrap_or(100_000) as usize;
            emit_raw(&"a".repeat(size));
            emit_publish(&topic, 0);
        }
        "crash" => {
            for i in 0..count {
                emit_publish(&topic, i);
            }
            // abort() delivers SIGABRT (signal 6) to ourselves — a crash the host
            // must reap cleanly. No `unsafe` needed (the workspace denies it).
            std::process::abort();
        }
        "stderr_flood" => {
            let lines = config
                .get("stderr_lines")
                .and_then(Value::as_u64)
                .unwrap_or(50_000);
            for i in 0..lines {
                eprintln!("flood line {i}");
            }
            emit_publish(&topic, 0);
        }
        "spawn_grandchild" => {
            let marker = config
                .get("marker")
                .and_then(Value::as_str)
                .unwrap_or("grandchild-marker");
            spawn_grandchild(marker);
            match config.get("then").and_then(Value::as_str).unwrap_or("exit") {
                "sleep" => sleep_forever_respecting_sigterm(),
                _ => { /* return → exit 0; the grandchild keeps stdout open */ }
            }
        }
        "sleep" => sleep_forever_respecting_sigterm(),
        "interval" => {
            // A long-running poller: publish forever on a fixed cadence, dying on
            // the default SIGTERM disposition (no handler installed) so the host's
            // graceful stop terminates it.
            let period = config
                .get("interval_ms")
                .and_then(Value::as_u64)
                .unwrap_or(50);
            let mut i = 0u64;
            loop {
                emit_publish(&topic, i);
                i += 1;
                std::thread::sleep(std::time::Duration::from_millis(period));
            }
        }
        "ignore_sigterm" => sleep_forever_ignoring_sigterm(&topic),
        other => {
            eprintln!("test_adapter: unknown mode {other:?}");
            std::process::exit(64);
        }
    }
}

/// Read and parse the first stdin line as the opaque config object.
fn read_config_line() -> Value {
    let mut line = String::new();
    std::io::stdin()
        .lock()
        .read_line(&mut line)
        .expect("read config line from stdin");
    serde_json::from_str(line.trim()).expect("config line is valid JSON")
}

fn default_topic() -> String {
    GithubPr::new("octocat", "hello-world", 1)
        .unwrap()
        .topic()
        .as_str()
        .to_string()
}

/// Re-exec this binary as a grandchild that sleeps forever. Default stdio is
/// inherited, so the grandchild inherits our stdout (the host's read pipe) and
/// keeps it open past our own exit — exercising the host's process-group
/// teardown. The `marker` lands in the grandchild's argv for `pgrep -f`.
// The grandchild is DELIBERATELY not waited on: it must outlive this direct
// child so it keeps the host's stdout pipe open, which is the whole point of the
// process-group-teardown test. It is reaped either by the host's group signal or
// by init after reparenting — never by us.
#[allow(clippy::zombie_processes)]
fn spawn_grandchild(marker: &str) {
    let exe = std::env::current_exe().expect("current exe");
    std::process::Command::new(exe)
        .arg("--grandchild")
        .arg(marker)
        .spawn()
        .expect("spawn grandchild");
}

/// Write one well-formed `Publish` NDJSON line and flush (stdout is block
/// buffered when piped, so an explicit flush keeps line ordering deterministic).
fn emit_publish(topic: &Topic, i: u64) {
    let message = Message::Publish(Publish {
        topic: topic.clone(),
        adapter: AdapterId(SELF_REPORTED_ADAPTER.to_string()),
        body: serde_json::json!({ "i": i }),
    });
    emit_raw(&encode_line(&message).expect("encode publish"));
}

/// Write a raw line verbatim (used for the malformed/oversized cases too).
fn emit_raw(line: &str) {
    let mut out = std::io::stdout().lock();
    writeln!(out, "{line}").expect("write stdout line");
    out.flush().expect("flush stdout");
}

/// Block forever without touching signal dispositions, so the default SIGTERM
/// action (terminate) applies — the host's graceful stop kills us.
fn sleep_forever_respecting_sigterm() -> ! {
    loop {
        std::thread::sleep(std::time::Duration::from_secs(3600));
    }
}

/// Install a SIGTERM handler and swallow it forever, so SIGTERM no longer
/// terminates us and only the host's SIGKILL can. Uses tokio's signal handling
/// so no `unsafe` is needed (the workspace denies it).
///
/// After the handler is installed we publish one "ready" event, so the test has
/// a deterministic readiness signal (host health `forwarded >= 1`) proving the
/// ignore is in force before it sends SIGTERM — no timing races.
fn sleep_forever_ignoring_sigterm(topic: &Topic) -> ! {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime");
    runtime.block_on(async {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("register SIGTERM handler");
        // Handler installed → the default SIGTERM disposition is replaced.
        // Announce readiness so the test can wait for it before stopping us.
        emit_publish(topic, 0);
        loop {
            // Draining and ignoring: SIGTERM will not kill the process now.
            term.recv().await;
        }
    });
    unreachable!("the ignore-SIGTERM loop only ends via SIGKILL")
}
