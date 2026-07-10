//! The `mailbox` bridge CLI binary — the single user/agent/adapter entry point.
//!
//! This file is argument plumbing and exit-code mapping only; the real logic
//! lives in the module split settled by card 06:
//!
//! - [`serve`] — the long-lived daemon that owns the single [`Storage`] writer,
//!   the [`Waker`], and the user-scoped Unix socket (ADR-0004);
//! - [`client`] — the one-shot socket client every mutating/reading command uses;
//! - [`control`] — the request/response types client and server share;
//! - [`cli`] — the clap command layer, argument parsing, and output formatting.
//!
//! Two commands do not use the socket. `serve` *is* the daemon. `wait` is the
//! sole read-only exception (ADR-0003): it opens the store read-only and blocks
//! on its wake FIFO, so it runs synchronously with no tokio runtime — its
//! blocking `poll` would otherwise idle a runtime worker for no benefit.
//!
//! [`Storage`]: mailbox::storage::Storage
//! [`Waker`]: mailbox::wake::Waker

mod cli;
mod client;
mod control;
mod serve;

use std::process::ExitCode;

use clap::Parser;
use tracing_subscriber::EnvFilter;

use cli::{Cli, Command};

fn main() -> ExitCode {
    // Route library `tracing` events to STDERR, filtered by RUST_LOG. Keeping
    // logs off stdout is what lets `--json` output stay clean and parseable; the
    // default filter is quiet (error only) so ordinary CLI use is silent unless
    // an operator opts in with `RUST_LOG=mailbox=debug`.
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();

    // clap handles --help/--version and prints usage errors itself (exit 2).
    let cli = Cli::parse();

    match cli.command {
        // `wait` runs synchronously (no runtime) and owns its own exit codes:
        // 2 = mail (wake the session), 1 = waiter error.
        Command::Wait(args) => cli::run_wait(&args),
        // Everything else is async (socket client, or the serve daemon).
        command => {
            let runtime = match tokio::runtime::Runtime::new() {
                Ok(runtime) => runtime,
                Err(err) => {
                    eprintln!("mailbox: could not start async runtime: {err}");
                    return ExitCode::FAILURE;
                }
            };
            match runtime.block_on(cli::run(cli::output_format(cli.json), command)) {
                Ok(()) => ExitCode::SUCCESS,
                Err(err) => {
                    // `{err:#}` includes the anyhow context chain. Goes to stderr,
                    // so `--json` stdout is unaffected.
                    eprintln!("mailbox: {err:#}");
                    ExitCode::FAILURE
                }
            }
        }
    }
}
