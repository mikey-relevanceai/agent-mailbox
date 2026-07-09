//! The `mailbox` bridge CLI binary.
//!
//! For now this is a thin scaffold plus a single PROVISIONAL subcommand,
//! `mailbox wait`, used to exercise the wake path end-to-end (card 05). The
//! full CLI surface — command names, argument shapes, output format — is settled
//! in card 06 and WILL be renamed/restructured; nothing here is a stable
//! interface. The real, reusable logic lives in [`mailbox::wake`]; this file is
//! only argument plumbing and process exit-code mapping.

use std::process::ExitCode;

use mailbox::storage::StorageConfig;
use mailbox::wake::{Waiter, WakeOutcome};
use mailbox_harness::protocol_version;
use mailbox_protocol::PROTOCOL_VERSION;
use tracing_subscriber::EnvFilter;

/// Opt-in env var: when set to `1`, the waiter appends its [`WakeReason`] to
/// stderr as a second line. This is a TEST/diagnostic hook only — the default
/// reminder the harness surfaces stays a clean payload-free "mail on topic …".
///
/// [`WakeReason`]: mailbox::wake::WakeReason
const WAIT_DEBUG_ENV: &str = "MAILBOX_WAIT_DEBUG";

fn main() -> ExitCode {
    // Route library `tracing` events (kicked N sessions, waiter woke, …) to
    // stderr, filtered by RUST_LOG. With RUST_LOG unset the default filter is
    // quiet (error only), so the waiter's payload-free reminder stays clean;
    // operators opt into diagnostics with `RUST_LOG=mailbox=debug`.
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();

    // Skip argv[0].
    let args: Vec<String> = std::env::args().skip(1).collect();

    match args.first().map(String::as_str) {
        // PROVISIONAL (card 06 will rename): block until this session has mail,
        // then exit 2 with the topic name(s) on stderr — the Claude Code
        // `asyncRewake` contract (docs/01-wake-and-rearm.md).
        Some("wait") => run_wait(&args[1..]),
        _ => {
            // Keep the binary linked to both workspace crates from day one so the
            // scaffold fails loudly if the workspace graph breaks.
            assert_eq!(protocol_version(), PROTOCOL_VERSION);
            println!("agent-mailbox {PROTOCOL_VERSION}");
            ExitCode::SUCCESS
        }
    }
}

/// Parse `--session <id>` and run the waiter. Exit codes:
/// - `2`: the session has mail — wake it (reminder on stderr).
/// - `1`: a waiter error (could not set up the FIFO, read the store, …).
/// - `64`: usage error (missing/garbled arguments).
fn run_wait(args: &[String]) -> ExitCode {
    let Some(session_id) = parse_session_flag(args) else {
        eprintln!("usage: mailbox wait --session <session-id>  [PROVISIONAL, card 06]");
        return ExitCode::from(64);
    };

    let config = match StorageConfig::from_env() {
        Ok(config) => config,
        Err(err) => {
            eprintln!("mailbox wait: {err}");
            return ExitCode::FAILURE;
        }
    };

    let waiter = Waiter::new(
        config.waiters_dir(),
        config.path().to_path_buf(),
        mailbox::storage::SessionId::new(session_id),
    );

    match waiter.wait() {
        Ok(outcome) => {
            // The payload-free reminder is the contract the harness surfaces:
            // "mail on topic …", topic names only, never a body.
            eprintln!("{}", outcome.reminder());
            // Opt-in diagnostic ONLY: reveal which wake path fired so tests can
            // assert it. Never part of the default reminder.
            if debug_enabled() {
                eprintln!("wake reason: {}", outcome.reason().as_str());
            }
            ExitCode::from(WakeOutcome::EXIT_CODE)
        }
        Err(err) => {
            eprintln!("mailbox wait: {err}");
            ExitCode::FAILURE
        }
    }
}

/// Whether the opt-in [`WAIT_DEBUG_ENV`] diagnostic is enabled.
fn debug_enabled() -> bool {
    std::env::var(WAIT_DEBUG_ENV).is_ok_and(|v| v == "1")
}

/// Extract the value of `--session <id>` (or `--session=<id>`) from `args`.
fn parse_session_flag(args: &[String]) -> Option<String> {
    let mut iter = args.iter();
    while let Some(arg) = iter.next() {
        if let Some(value) = arg.strip_prefix("--session=") {
            return (!value.is_empty()).then(|| value.to_string());
        }
        if arg == "--session" {
            return iter.next().filter(|v| !v.is_empty()).cloned();
        }
    }
    None
}
