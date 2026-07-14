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
use tracing_subscriber::filter::LevelFilter;

use cli::{Cli, Command, HarnessCommand};

fn main() -> ExitCode {
    // clap handles --help/--version and prints usage errors itself (exit 2).
    let cli = Cli::parse();

    // Route `tracing` events. For `wait` and `harness arm`, the process stderr is
    // a WIRE channel: on exit 2 Claude Code surfaces it to the agent as the "mail
    // on topic X" reminder (payload-free wake). A `RUST_LOG=info` tracing line on
    // that stderr would pollute the reminder, so those two commands send tracing to
    // a log file under the mailbox dir (or suppress it) — the reminder is written
    // with a bare `eprintln!`, keeping the wire clean regardless of `RUST_LOG`.
    // Every other command keeps logs on stderr (stdout stays clean for `--json`).
    init_tracing(&cli.command);

    match cli.command {
        // `wait` runs synchronously (no runtime) and owns its own exit codes:
        // 2 = mail (wake the session), 1 = waiter error. On its self-respawn
        // boundary it re-execs itself, so it never returns to `main` in that case.
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

/// Whether this command's stderr is a wake wire channel (`wait` and `harness
/// arm`), so its tracing must be kept OFF stderr.
fn is_wire_stderr(command: &Command) -> bool {
    matches!(
        command,
        Command::Wait(_)
            | Command::Harness(cli::HarnessArgs {
                command: HarnessCommand::Arm(_)
            })
    )
}

/// Initialise the `tracing` subscriber. Wire-stderr commands (`wait`, `harness
/// arm`) log to `<db-dir>/harness.log` (append) so their stderr stays a clean wake
/// channel; if that file cannot be opened, or for any other command, tracing goes
/// to stderr as usual. Filtered by `RUST_LOG`.
///
/// The two sinks default differently ON PURPOSE (card 16 / FIX 3). Other commands'
/// stderr stays quiet — ERROR only — so an agent's terminal is not flooded. The
/// `harness.log` file defaults to WARN, because it is the *only* place a hook
/// leaves a trace: a silently-unregistered inbox (→ a permanently unreachable
/// agent) is logged by `register_inbox` at warn/error, and at ERROR-only those
/// warns would vanish, leaving zero visible signal. `RUST_LOG` overrides either.
fn init_tracing(command: &Command) {
    if is_wire_stderr(command)
        && let Some(file) = wire_log_file()
    {
        // Default to WARN (not ERROR) for the harness log so the transient
        // "bridge unreachable/errored while registering the inbox" lines are
        // visible; a set `RUST_LOG` still wins. This raises verbosity ONLY on the
        // harness.log sink — no other command's stderr is affected.
        let filter = EnvFilter::builder()
            .with_default_directive(LevelFilter::WARN.into())
            .from_env_lossy();
        // `with_writer` takes a MakeWriter; a closure returning a fresh handle each
        // time satisfies it, and appends interleave safely on our targets.
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_ansi(false)
            .with_writer(move || file.try_clone().unwrap_or_else(|_| open_null()))
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(EnvFilter::from_default_env())
            .with_writer(std::io::stderr)
            .init();
    }
}

/// Open (append) the harness log file beside the database, if the storage path
/// resolves. `None` falls back to stderr for that run — losing a few log lines is
/// preferable to failing the hook.
fn wire_log_file() -> Option<std::fs::File> {
    let config = mailbox::storage::StorageConfig::from_env().ok()?;
    let dir = config.dir();
    std::fs::create_dir_all(&dir).ok()?;
    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(dir.join("harness.log"))
        .ok()
}

/// `/dev/null` as a last-resort writer (a failed `try_clone`); dropping logs is
/// fine here — never polluting the wake wire is the priority.
fn open_null() -> std::fs::File {
    std::fs::OpenOptions::new()
        .write(true)
        .open("/dev/null")
        .expect("open /dev/null")
}
