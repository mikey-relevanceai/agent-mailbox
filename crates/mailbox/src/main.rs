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
//! Three commands do not use the socket. `serve` *is* the daemon. `harness wake` is
//! the `FileChanged` hook: a read-only peek at the store, so it runs synchronously
//! with no tokio runtime. `doctor` is the other read-only reader (ADR-0016): a health
//! check has to work when the daemon is the thing that is broken, which is exactly
//! when a socket client cannot. Both read-only opens are permitted by ADR-0003 and
//! neither can mutate.
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

    // Route `tracing` events. For the hooks, the process stderr is a WIRE channel:
    // on exit 2 Claude Code surfaces it to the agent as the "mail on topic X"
    // reminder (payload-free wake). A `RUST_LOG=info` tracing line on that stderr
    // would pollute the reminder, so those commands send tracing to a log file under
    // the mailbox dir (or suppress it) — the reminder is written with a bare
    // `eprintln!`, keeping the wire clean regardless of `RUST_LOG`. Every other
    // command keeps logs on stderr (stdout stays clean for `--json`).
    init_tracing(&cli.command);

    match cli.command {
        // `doctor` is socket-free by design: it reads Claude Code's own session
        // registry and the process table, so a health check still works when the
        // daemon is the thing that is broken. It needs no runtime.
        Command::Doctor(args) => cli::run_doctor(cli::output_format(cli.json), &args),
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
                // The command owns its exit code: almost always SUCCESS, but a REFUSED
                // publish ("you have unread mail; read first") has its own, so a
                // scripted publisher can tell "retry after reading" from a real error.
                Ok(code) => code,
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

/// Whether this command's tracing must be kept OFF stderr and sent to
/// `harness.log` instead. Two reasons a harness command qualifies:
///
/// - its stderr is a WAKE WIRE — the `harness wake` hook writes the payload-free
///   reminder there on exit 2, so a tracing line would pollute it;
/// - its stdout is a HOOK CONTRACT — `harness session-start` prints the `watchPaths`
///   JSON there, and it wants its lifecycle in `harness.log` rather than on a channel
///   Claude Code parses.
fn is_wire_stderr(command: &Command) -> bool {
    matches!(
        command,
        Command::Harness(cli::HarnessArgs {
            command: HarnessCommand::SessionStart | HarnessCommand::Cleanup
        })
    )
}

/// Initialise the `tracing` subscriber. Wire-stderr commands (the hooks) log to
/// `<db-dir>/harness.log` (append) so their stderr stays a clean wake channel; if
/// that file cannot be opened, or for any other command, tracing goes to stderr as
/// usual. Filtered by `RUST_LOG`.
///
/// The two sinks default differently ON PURPOSE (card 16 / FIX 3). Other commands'
/// stderr stays quiet — ERROR only — so an agent's terminal is not flooded. The
/// `harness.log` file defaults to **INFO**, because it is the *only* place the hook
/// side of the wake loop leaves a trace, and the lines that answer "why didn't my
/// agent wake?" — armed / woke-with-unread / re-triggered-at-the-turn-boundary — are
/// all `info!`. At WARN they were discarded, which is precisely why a session that
/// silently stopped being wakeable was unfalsifiable from outside the process.
/// `RUST_LOG` overrides either sink.
fn init_tracing(command: &Command) {
    if is_wire_stderr(command)
        && let Some(file) = wire_log_file()
    {
        // Default to INFO for the harness log: the hooks' decisions must be visible
        // at the DEFAULT level, or the next wake bug is again invisible. A
        // set `RUST_LOG` still wins. This raises verbosity ONLY on the harness.log
        // sink — no other command's stderr is affected, and the exit-2 stderr wire
        // stays payload-free regardless (tracing never goes there).
        let filter = EnvFilter::builder()
            .with_default_directive(LevelFilter::INFO.into())
            .from_env_lossy();
        // `with_writer` takes a MakeWriter; a closure returning a fresh handle each
        // time satisfies it, and appends interleave safely on our targets.
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_ansi(false)
            .with_writer(move || file.try_clone().unwrap_or_else(|_| open_null()))
            .init();
    } else {
        // The daemon owns the OTHER half of the "why didn't my agent wake?" trail:
        // `wake_all` logs, per publish, how many subscribers' sentinels it actually
        // bumped. That is an `info!`, and at the ERROR default it was discarded — so
        // the publisher's side of a missed wake was as invisible as the hook's.
        // `serve` runs in its own terminal (or a redirected log), so INFO there floods
        // nobody. Every other command keeps the quiet ERROR default: their stderr is
        // the agent's terminal.
        let default = match command {
            Command::Serve => LevelFilter::INFO,
            _ => LevelFilter::ERROR,
        };
        let filter = EnvFilter::builder()
            .with_default_directive(default.into())
            .from_env_lossy();
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_writer(std::io::stderr)
            .init();
    }
}

/// Open (append) the harness log file beside the database, if the storage path
/// resolves. `None` falls back to stderr for that run — losing a few log lines is
/// preferable to failing the hook.
fn wire_log_file() -> Option<std::fs::File> {
    let config = mailbox::storage::StorageConfig::from_env().ok()?;
    std::fs::create_dir_all(config.dir()).ok()?;
    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(config.harness_log_path())
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
