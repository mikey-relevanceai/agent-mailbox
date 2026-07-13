//! The wake kick: signalling a blocked waiter that mail has arrived.
//!
//! # What crosses the boundary (and what does not)
//!
//! Wake is **payload-free** (ADR-0001, docs/01-wake-and-rearm.md): the kick
//! carries a single meaningless byte, and the woken waiter reports at most the
//! *topic name(s)* with unread mail on its stderr. The event body never crosses
//! this boundary — it stays in the durable log and is read later by the agent's
//! `read`. Wake is ingress, not authority.
//!
//! # Chosen primitive: a per-session FIFO
//!
//! Layout: `<waiters-dir>/<session>.fifo`, where `<waiters-dir>` sits beside the
//! database file (see [`crate::storage::StorageConfig::waiters_dir`]). A FIFO was
//! chosen over the alternatives (orchestrator DECISION, card 05) because it is
//! self-contained: no daemon or socket server exists yet (that lands in later
//! cards), a waiter is just a child process a hook can launch, and both targets
//! (macOS + Linux, per AGENTS.md) support FIFOs. Everything here is deliberately
//! behind [`Waker`] / [`Waiter`] so the primitive can be swapped for a Unix
//! socket later without touching callers (the bus, the CLI).
//!
//! # Single waiter per session
//!
//! Exactly one waiter may be live per session. Two waiters sharing one FIFO would
//! *steal* each other's kick bytes — a single kick reaches only one reader, so
//! the other (perhaps the harness's tracked waiter) would be stranded and its
//! wake lost. [`Waiter::wait`] therefore takes an exclusive advisory lock on a
//! per-session lockfile before doing anything else; a second waiter fails fast
//! with [`WakeError::AlreadyWaiting`] rather than silently attaching to the FIFO.
//!
//! # The missed-kick race, and why the ordering closes it
//!
//! A waiter that starts *after* a publish must still fire. The waiter therefore
//! does **open FIFO → check unread → block**, never check-then-open:
//!
//! - A kick that arrives *after* the waiter opened the FIFO is buffered in the
//!   pipe, so the subsequent blocking wait returns immediately.
//! - A publish that landed *before* the waiter opened is caught by the unread
//!   check (the event is durable), so the waiter exits without ever blocking.
//!
//! Those two windows are exhaustive and overlap: because a publish appends to
//! the durable log *before* it kicks, there is no interleaving in which the kick
//! is lost *and* the unread check misses the event. See [`Waiter::wait`] for the
//! step-by-step argument.
//!
//! The waiter opens the FIFO **read-write, non-blocking** (`O_RDWR | O_NONBLOCK`)
//! and blocks with `poll`, then drains with non-blocking reads. `O_RDWR` keeps a
//! writer attached (itself), so `poll` never reports a spurious hang-up and a
//! read never returns a phantom EOF the moment no external writer is attached.
//! `O_RDWR` on a FIFO is not POSIX-specified, but behaves this way on both of our
//! targets (Linux, macOS).

use std::io::{self, Read};
use std::os::fd::AsFd;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use tracing::{debug, info, trace, warn};

// nix gives us SAFE wrappers for the Unix primitives wake needs (`mkfifo`,
// `flock`, `poll`, plus the `O_NONBLOCK`/errno constants it re-exports from
// libc). The workspace `unsafe_code = "deny"` lint forbids calling the raw
// `libc` equivalents directly, so nix is the way to reach these (ADR-0002).
use nix::errno::Errno;
use nix::fcntl::{Flock, FlockArg};
use nix::poll::{PollFd, PollFlags, PollTimeout, poll};
use nix::sys::stat::{Mode, SFlag};

use mailbox_protocol::Topic;

use crate::storage::{ReadOnlyStore, SessionId, StorageError};

/// The lone byte a kick writes into a FIFO.
///
/// Its *value* is meaningless — only its arrival matters. Making the wake a
/// single fixed byte is what enforces payload-free wake structurally: there is
/// no field in which a body could ever be smuggled across the kick. Exposed so
/// tests can assert the channel carries exactly this and nothing else.
pub const WAKE_BYTE: u8 = 1;

/// Something went wrong setting up or operating a wake FIFO.
///
/// Note that a *kick* never surfaces these to the publisher — [`Waker::kick`]
/// reports a [`KickOutcome`] instead and never fails a publish (a missing reader
/// is normal, not an error). These are for the waiter side, where a failure
/// means the process cannot do its job and should exit loudly (non-zero).
#[derive(Debug, thiserror::Error)]
pub enum WakeError {
    /// The waiter directory could not be created.
    #[error("could not create waiter directory {path}: {source}")]
    CreateDir {
        path: PathBuf,
        #[source]
        source: io::Error,
    },

    /// The FIFO node could not be created (and did not already exist).
    #[error("could not create wake FIFO {path}: {source}")]
    Mkfifo {
        path: PathBuf,
        #[source]
        source: Errno,
    },

    /// A node exists at the FIFO path but it is NOT a FIFO (e.g. a regular file
    /// left there). Opening and reading it would busy-spin on phantom EOF, so we
    /// refuse loudly instead of degrading into a hot loop.
    #[error("path {path} exists but is not a FIFO; refusing to wait on it")]
    NotAFifo { path: PathBuf },

    /// Another waiter already holds this session's lock. Only one waiter may be
    /// live per session (see the module docs); a second must not attach.
    #[error("another waiter is already running for this session (lock held at {path})")]
    AlreadyWaiting { path: PathBuf },

    /// The per-session lockfile could not be opened or locked.
    #[error("could not acquire waiter lock {path}: {source}")]
    Lock {
        path: PathBuf,
        #[source]
        source: io::Error,
    },

    /// The FIFO could not be opened.
    #[error("could not open wake FIFO {path}: {source}")]
    OpenFifo {
        path: PathBuf,
        #[source]
        source: io::Error,
    },

    /// A poll/read on the FIFO failed.
    #[error("could not read wake FIFO {path}: {source}")]
    ReadFifo {
        path: PathBuf,
        #[source]
        source: io::Error,
    },

    /// The waiter pidfile could not be written (the waiter cannot make itself
    /// reap-able by `cleanup`, so this is a real error rather than best-effort).
    #[error("could not write waiter pidfile {path}: {source}")]
    Pidfile {
        path: PathBuf,
        #[source]
        source: io::Error,
    },

    /// The read-only unread check against the durable store failed.
    #[error(transparent)]
    Storage(#[from] StorageError),
}

/// The result of one [`Waker::kick`]: did the wake byte reach a listening waiter?
///
/// Returned (rather than discarded) so [`Waker::kick_all`] can report real
/// delivered/no-reader/error counts — "why didn't my agent wake?" is answerable
/// from the logs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KickOutcome {
    /// The wake byte was written to a waiter that had the FIFO open for reading.
    Delivered,
    /// No live waiter: either no FIFO node exists, or none is open for reading
    /// (`ENXIO`). Normal — the waiter's unread check covers this case.
    NoReader,
    /// The kick failed for some other reason (logged). Never fails the publish.
    Error,
}

/// Why a waiter woke. Both outcomes mean the same thing to the harness (exit 2
/// → wake the idle session); the distinction exists only so the reason can be
/// logged honestly, and surfaced to tests behind an opt-in debug flag.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WakeReason {
    /// Unread mail already existed when the waiter checked, before it ever
    /// blocked (the missed-kick safety path, AC2).
    ExistingUnread,
    /// The waiter was blocked and a kick arrived (the steady-state path, AC1).
    Kicked,
}

impl WakeReason {
    /// Stable label for logs and the opt-in debug line.
    pub fn as_str(self) -> &'static str {
        match self {
            WakeReason::ExistingUnread => "existing-unread",
            WakeReason::Kicked => "kicked",
        }
    }
}

/// Which pass of the check-then-block loop we are on. A two-state enum rather
/// than a bare `bool` so the meaning is legible at the branch (enum-over-bool
/// discipline, AGENTS.md).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Pass {
    /// The very first unread check, before the waiter has ever blocked.
    FirstCheck,
    /// A check performed after waking from a kick.
    AfterKick,
}

/// Why one `poll` on the FIFO returned: a kick byte arrived (drain + re-check),
/// or the bounded block elapsed with no kick (the self-respawn boundary, card 11).
/// A named enum, not a `bool`, so the branch reads plainly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Blocked {
    /// The FIFO became readable — a kick arrived and was drained.
    Kicked,
    /// The `max_block` budget elapsed before any kick (bounded waits only).
    TimedOut,
}

/// The result of a [`Waiter::wait`]: how a blocking wait ended.
///
/// A three-way outcome, so the harness's arm-iff-subscribed and self-respawn
/// coordination are both representable and exhaustively handled at the call site
/// (no `unreachable!`): see docs/01-wake-and-rearm.md and ADR-0006.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WaitOutcome {
    /// The session has unread mail; exit 2 and surface the reminder.
    Woken(WakeOutcome),
    /// No mail within `budget`; the caller should re-exec a fresh waiter. Only
    /// ever returned when a `max_block` was supplied (an unbounded wait blocks
    /// forever). `budget` is carried so the caller re-arms with the same bound
    /// without re-reading it from elsewhere.
    TimedOut { budget: Duration },
    /// The session has NO subscriptions, so there is nothing to be woken about —
    /// exit cleanly WITHOUT waking. This is the waiter-side half of
    /// arm-iff-subscribed: an `arm` that raced a `SessionEnd`/unsubscribe (interest
    /// already dropped) starts a waiter that finds nothing and self-exits, so no
    /// orphan waiter survives (card 11 HIGH#2 fix).
    Unsubscribed,
}

/// The result of a completed [`Waiter::wait`]: the session has mail and should
/// be woken.
///
/// A dedicated type (rather than a bare `Vec<Topic>`) names the protocol — the
/// waiter's contract is "exit 2, topic names on stderr" — and keeps the
/// exit-code and reminder-line construction in one place so callers cannot get
/// them subtly wrong.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WakeOutcome {
    topics: Vec<Topic>,
    reason: WakeReason,
}

impl WakeOutcome {
    /// The process exit code that asks the Claude Code harness to wake the idle
    /// session (the `asyncRewake` contract, docs/01-wake-and-rearm.md). A `u8` so
    /// the sole consumer maps it to a `std::process::ExitCode` without a lossy
    /// cast.
    pub const EXIT_CODE: u8 = 2;

    /// The subscribed topics that currently have unread mail.
    pub fn topics(&self) -> &[Topic] {
        &self.topics
    }

    /// Why the waiter woke.
    pub fn reason(&self) -> WakeReason {
        self.reason
    }

    /// The payload-free stderr reminder line. Topic names only — never a body —
    /// so it is safe to surface verbatim as a system reminder.
    pub fn reminder(&self) -> String {
        let names: Vec<&str> = self.topics.iter().map(Topic::as_str).collect();
        format!("mail on topic {}", names.join(", "))
    }
}

/// The path of a session's FIFO under `waiters_dir`.
///
/// The filename stem is [`SessionId::encode_filename`] — the ONE shared encoder
/// (in `mailbox-protocol`), so the FIFO, the lock, and the harness's pidfile all
/// key a session identically (card 11 coordination).
fn fifo_path(waiters_dir: &Path, session: &SessionId) -> PathBuf {
    waiters_dir.join(format!("{}.fifo", session.encode_filename()))
}

/// The path of a session's advisory lockfile under `waiters_dir`.
fn lock_path(waiters_dir: &Path, session: &SessionId) -> PathBuf {
    waiters_dir.join(format!("{}.lock", session.encode_filename()))
}

/// The path of a session's waiter pidfile under `waiters_dir`.
///
/// The waiter (not `arm`) owns this: it is written only AFTER the single-waiter
/// lock is acquired, so the pidfile always names the one live lock-holding waiter
/// — the source of truth `cleanup` reaps (card 11, ADR-0006).
pub fn pidfile_path(waiters_dir: &Path, session: &SessionId) -> PathBuf {
    waiters_dir.join(format!("{}.waiter.pid", session.encode_filename()))
}

/// Whether a session currently has a LIVE waiter blocked on its FIFO.
///
/// The honest definition, because `mailbox agents` reports it and must not
/// overclaim: this reads the session's pidfile and probes that PID with
/// `kill(pid, 0)`. Post-ADR-0006 the pidfile is written by the waiter itself,
/// only after it takes the single-waiter lock, so it reliably names the one
/// lock-holding waiter for the session. A live PID therefore means "this agent is
/// idle and listening — a publish to its topics will wake it now".
///
/// What it is NOT: a heartbeat, or proof the *agent* is healthy. `false` only
/// means no waiter is blocked at this instant — typically because the session is
/// mid-turn (busy), or because it never armed. A message published to a
/// subscribed session with no live waiter is still durably delivered; it surfaces
/// on that session's next read/arm. There is no liveness signal beyond this, and
/// we deliberately do not invent one.
pub fn waiter_alive(waiters_dir: &Path, session: &SessionId) -> bool {
    let Ok(text) = std::fs::read_to_string(pidfile_path(waiters_dir, session)) else {
        return false;
    };
    let Ok(pid) = text.trim().parse::<i32>() else {
        // A half-written or garbage pidfile names no waiter (same tolerance as
        // `mailbox_harness::arm::read_pidfile`).
        return false;
    };
    // Signal 0 probes liveness without delivering anything: Ok => alive, ESRCH => gone.
    matches!(
        nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid), None),
        Ok(())
    )
}

/// The kick side of wake, held by whoever publishes (the bridge / bus).
///
/// Cheap to clone; it holds only the waiters directory path. It never opens the
/// database — it is handed the list of sessions to kick.
#[derive(Debug, Clone)]
pub struct Waker {
    waiters_dir: PathBuf,
}

impl Waker {
    /// Build a waker that signals FIFOs under `waiters_dir`.
    pub fn new(waiters_dir: impl Into<PathBuf>) -> Self {
        Self {
            waiters_dir: waiters_dir.into(),
        }
    }

    /// Kick every session in `sessions` that is subscribed to `topic`, then log
    /// the aggregate outcome.
    ///
    /// Payload-free: `topic` is used only for the log line, never written into
    /// any FIFO. With zero sessions this logs `delivered=0 no_reader=0` — an
    /// honest "nothing to wake", not a misleading "kicked" claim.
    pub fn kick_all(&self, sessions: &[SessionId], topic: &Topic) {
        let mut delivered = 0usize;
        let mut no_reader = 0usize;
        let mut errors = 0usize;
        for session in sessions {
            match self.kick(session) {
                KickOutcome::Delivered => delivered += 1,
                KickOutcome::NoReader => no_reader += 1,
                KickOutcome::Error => errors += 1,
            }
        }
        // Log after the decision point: which topic drove the kicks, and how many
        // subscribers we actually woke vs had no live waiter. Never the body.
        info!(
            topic = topic.as_str(),
            delivered, no_reader, errors, "kicked subscribed sessions after publish"
        );
    }

    /// Kick one session's FIFO. Best-effort by design:
    ///
    /// - The FIFO is opened **write-only, non-blocking**, so the publisher never
    ///   blocks waiting for a reader to appear.
    /// - If no waiter currently holds the FIFO open for reading, the OS reports
    ///   `ENXIO`; if no FIFO node exists at all, `NotFound`. Both are normal (a
    ///   session may have no live waiter) and yield [`KickOutcome::NoReader`]. The
    ///   waiter's open→check→block ordering covers the "published before I
    ///   blocked" case, so a dropped kick is never a lost wake.
    /// - Any other failure yields [`KickOutcome::Error`] and is logged: a kick
    ///   must never fail a publish (the event is already durable).
    pub fn kick(&self, session: &SessionId) -> KickOutcome {
        let path = fifo_path(&self.waiters_dir, session);
        // O_WRONLY | O_NONBLOCK: returns immediately, with ENXIO if no reader.
        let open = std::fs::OpenOptions::new()
            .write(true)
            .custom_flags(nix::libc::O_NONBLOCK)
            .open(&path);

        let mut file = match open {
            Ok(file) => file,
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                debug!(
                    session = session.as_str(),
                    "no waiter FIFO; kick is a no-op"
                );
                return KickOutcome::NoReader;
            }
            Err(err) if err.raw_os_error() == Some(nix::libc::ENXIO) => {
                debug!(
                    session = session.as_str(),
                    "no waiter listening on FIFO; kick is a no-op"
                );
                return KickOutcome::NoReader;
            }
            Err(err) => {
                warn!(
                    session = session.as_str(),
                    error = %err,
                    "could not open waiter FIFO to kick; skipping (publish already durable)"
                );
                return KickOutcome::Error;
            }
        };

        use io::Write;
        match file.write_all(&[WAKE_BYTE]) {
            Ok(()) => {
                debug!(session = session.as_str(), "delivered wake byte");
                KickOutcome::Delivered
            }
            Err(err) => {
                // A reader that just vanished (EPIPE) or a full pipe (EAGAIN) is
                // harmless: coalescing means one delivered byte is enough, and a
                // vanished reader has nothing to wake.
                debug!(
                    session = session.as_str(),
                    error = %err,
                    "wake byte not written (pipe full or reader gone)"
                );
                KickOutcome::Error
            }
        }
    }
}

/// The block side of wake, run by the waiter process.
///
/// Holds the FIFO path, the per-session lock path, and the database path (opened
/// read-only inside [`Waiter::wait`]).
///
/// # Residual risk (documented, out of scope here)
///
/// The unread check reads the database read-only across processes; that relies
/// on the bridge/writer process being live so WAL is present. The "bridge down"
/// window — where the read-only open would fail — is owned by later
/// process-lifecycle cards, not this one.
#[derive(Debug)]
pub struct Waiter {
    session: SessionId,
    fifo_path: PathBuf,
    lock_path: PathBuf,
    pidfile_path: PathBuf,
    db_path: PathBuf,
}

impl Waiter {
    /// Build a waiter for `session`, using FIFOs/locks/pidfiles under
    /// `waiters_dir` and the read-only store at `db_path`.
    pub fn new(
        waiters_dir: impl AsRef<Path>,
        db_path: impl Into<PathBuf>,
        session: SessionId,
    ) -> Self {
        let waiters_dir = waiters_dir.as_ref();
        let fifo_path = fifo_path(waiters_dir, &session);
        let lock_path = lock_path(waiters_dir, &session);
        let pidfile_path = pidfile_path(waiters_dir, &session);
        Self {
            session,
            fifo_path,
            lock_path,
            pidfile_path,
            db_path: db_path.into(),
        }
    }

    /// The resolved FIFO path (exposed for tests and diagnostics).
    pub fn fifo_path(&self) -> &Path {
        &self.fifo_path
    }

    /// The resolved waiter pidfile path (exposed for tests and diagnostics).
    pub fn pidfile_path(&self) -> &Path {
        &self.pidfile_path
    }

    /// Block until this session has unread mail, then return the outcome.
    ///
    /// The ordering is load-bearing (see the module docs and ADR-0006):
    ///
    /// 0. Acquire the per-session exclusive lock (single-waiter-per-session); a
    ///    second waiter fails fast with [`WakeError::AlreadyWaiting`] and — never
    ///    having reached the pidfile step — leaves the pidfile untouched, so it
    ///    keeps naming the LIVE lock-holding waiter (card 11 HIGH#1 fix).
    /// 1. Write the pidfile (our pid) — the waiter, not `arm`, owns it, written
    ///    only AFTER the lock so the pidfile is always the one live waiter's.
    /// 2. Ensure + open the FIFO (`O_RDWR | O_NONBLOCK`) — from here any kick is
    ///    either buffered in this pipe or already reflected in the durable log.
    /// 3. Open the read-only store and **re-check the session still has a
    ///    subscription** — an `arm` that raced `SessionEnd` finds none and exits
    ///    [`WaitOutcome::Unsubscribed`] (no orphan; card 11 HIGH#2 fix).
    /// 4. Loop: check unread. If there is mail, return `Woken`. Otherwise `poll`
    ///    until a kick (drain + re-check) or, with a `max_block`, the budget
    ///    elapses → `TimedOut` (the caller re-execs a fresh waiter).
    ///
    /// `max_block = None` blocks indefinitely (the original card-05 contract, so a
    /// bare `mailbox wait` never times out). `Some(budget)` is a per-block bound:
    /// the self-respawn primitive that keeps a long idle armed without the
    /// harness's per-hook timeout ever landing on a live wait (see
    /// docs/01-wake-and-rearm.md). The same open→check→block ordering runs on every
    /// fresh waiter, so a publish during a re-exec gap is caught, not missed.
    ///
    /// The pidfile is removed only on the `Unsubscribed` exit (while the lock is
    /// still held, so no concurrent waiter can have written a fresh one). On
    /// `Woken` and `TimedOut` it is deliberately LEFT in place: on `TimedOut` the
    /// same-pid re-exec re-writes it (and a `cleanup` racing the exec gap can still
    /// reap our live pid); on `Woken` the process is exiting and the next `arm`'s
    /// waiter overwrites it under lock.
    pub fn wait(&self, max_block: Option<Duration>) -> Result<WaitOutcome, WakeError> {
        // Step 0: single-waiter lock. Held for the whole call; released on drop
        // (any return path). `_lock` must stay bound so it is not dropped early.
        let _lock = self.acquire_lock()?;

        // Step 1: record ourselves as the live waiter, now that we hold the lock.
        self.write_pidfile()?;

        // Step 2: open the FIFO FIRST. O_RDWR|O_NONBLOCK keeps the open and the
        // reads non-blocking; we block explicitly with poll below.
        self.ensure_fifo()?;
        let mut fifo = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(nix::libc::O_NONBLOCK)
            .open(&self.fifo_path)
            .map_err(|source| WakeError::OpenFifo {
                path: self.fifo_path.clone(),
                source,
            })?;

        // Step 3: read-only view of the durable store, and the arm-iff-subscribed
        // re-check. If the session unsubscribed (or a SessionEnd raced us), there
        // is nothing to wake about: drop the pidfile (under lock) and self-exit.
        let store = ReadOnlyStore::open(&self.db_path)?;
        if !store.has_subscription(&self.session)? {
            self.remove_pidfile();
            info!(
                session = self.session.as_str(),
                "waiter found no subscriptions; exiting without waking"
            );
            return Ok(WaitOutcome::Unsubscribed);
        }

        // Step 4: check-then-block loop.
        let mut pass = Pass::FirstCheck;
        loop {
            let topics = store.topics_with_unread(&self.session)?;
            if !topics.is_empty() {
                let reason = match pass {
                    Pass::FirstCheck => WakeReason::ExistingUnread,
                    Pass::AfterKick => WakeReason::Kicked,
                };
                let names: Vec<&str> = topics.iter().map(Topic::as_str).collect();
                info!(
                    session = self.session.as_str(),
                    reason = reason.as_str(),
                    // Topic names are payload-free (they cross to stderr anyway),
                    // so logging them is safe and makes "why did it wake" concrete.
                    topics = names.join(","),
                    "waiter woke; session has unread mail (exiting 2)"
                );
                return Ok(WaitOutcome::Woken(WakeOutcome { topics, reason }));
            }
            pass = Pass::AfterKick;

            // No unread yet: block until a kick byte arrives (or the budget
            // elapses), then drain and re-check.
            match self.block_for_kick(&mut fifo, max_block)? {
                Blocked::Kicked => continue,
                Blocked::TimedOut => {
                    // Re-check once before giving up: a publish may have landed
                    // during this block without a delivered kick (the same durable
                    // safety the first-pass check relies on). If still nothing, the
                    // caller re-arms a fresh waiter.
                    if !store.topics_with_unread(&self.session)?.is_empty() {
                        continue;
                    }
                    // `budget` is the max_block that produced this timeout; a
                    // `None` (unbounded) wait never reaches here (its poll blocks
                    // forever), so this is only ever the bounded case.
                    let budget = max_block.unwrap_or_default();
                    info!(
                        session = self.session.as_str(),
                        max_block_ms = budget.as_millis(),
                        "waiter reached its max-block with no mail; yielding for re-arm"
                    );
                    return Ok(WaitOutcome::TimedOut { budget });
                }
            }
        }
    }

    /// Record our own pid in the pidfile (called after the lock is acquired).
    /// Creates the waiters dir if needed; a write failure is a real error (the
    /// waiter cannot make itself reap-able) so it surfaces rather than being
    /// swallowed.
    fn write_pidfile(&self) -> Result<(), WakeError> {
        self.ensure_dir()?;
        std::fs::write(&self.pidfile_path, std::process::id().to_string()).map_err(|source| {
            WakeError::Pidfile {
                path: self.pidfile_path.clone(),
                source,
            }
        })
    }

    /// Remove the pidfile, ignoring a missing file. Best-effort: only called on
    /// the clean `Unsubscribed` exit while still holding the lock.
    fn remove_pidfile(&self) {
        if let Err(err) = std::fs::remove_file(&self.pidfile_path)
            && err.kind() != io::ErrorKind::NotFound
        {
            warn!(
                session = self.session.as_str(),
                path = %self.pidfile_path.display(),
                error = %err,
                "could not remove waiter pidfile on clean exit"
            );
        }
    }

    /// Block (via `poll`) until the FIFO is readable or `max_block` elapses, then
    /// drain every buffered byte with non-blocking reads.
    ///
    /// Draining fully means coalesced kicks leave nothing behind for a later
    /// iteration to misread. A read of `Ok(0)` is EOF — impossible for a live
    /// FIFO we hold `O_RDWR` on, so it means the node is not (or no longer) a
    /// working FIFO — and is surfaced as an error rather than looped on, which is
    /// the second guard against the hot-spin failure mode.
    ///
    /// With `max_block = None` the poll blocks indefinitely (card-05 behaviour);
    /// with `Some(budget)` it returns [`Blocked::TimedOut`] when the budget
    /// elapses before any kick — the self-respawn boundary (card 11).
    fn block_for_kick(
        &self,
        fifo: &mut std::fs::File,
        max_block: Option<Duration>,
    ) -> Result<Blocked, WakeError> {
        // Scope the poll so the BorrowedFd it holds is released before the drain
        // loop needs `fifo` mutably.
        let ready = {
            let mut poll_fds = [PollFd::new(fifo.as_fd(), PollFlags::POLLIN)];
            // NONE = block indefinitely; a bounded timeout returns 0 ready fds when
            // it elapses. A kick or existing buffered byte makes the fd readable.
            let timeout = match max_block {
                None => PollTimeout::NONE,
                Some(budget) => PollTimeout::try_from(budget).unwrap_or(PollTimeout::MAX),
            };
            poll(&mut poll_fds, timeout).map_err(|errno| WakeError::ReadFifo {
                path: self.fifo_path.clone(),
                source: io::Error::from_raw_os_error(errno as i32),
            })?
        };

        // `poll` returned 0 ready fds => the timeout elapsed with no kick.
        if ready == 0 {
            return Ok(Blocked::TimedOut);
        }

        // Drain all currently-available bytes; their count and value are
        // irrelevant (payload-free), we only care that a kick arrived.
        let mut buf = [0u8; 64];
        loop {
            match fifo.read(&mut buf) {
                Ok(0) => {
                    return Err(WakeError::NotAFifo {
                        path: self.fifo_path.clone(),
                    });
                }
                Ok(_) => continue,
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => break,
                Err(source) => {
                    return Err(WakeError::ReadFifo {
                        path: self.fifo_path.clone(),
                        source,
                    });
                }
            }
        }
        trace!(session = self.session.as_str(), "waiter drained a kick");
        Ok(Blocked::Kicked)
    }

    /// Acquire the per-session exclusive advisory lock. Non-blocking: if another
    /// waiter holds it, return [`WakeError::AlreadyWaiting`] immediately rather
    /// than queueing. The returned [`Flock`] releases the lock on drop.
    fn acquire_lock(&self) -> Result<Flock<std::fs::File>, WakeError> {
        self.ensure_dir()?;
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&self.lock_path)
            .map_err(|source| WakeError::Lock {
                path: self.lock_path.clone(),
                source,
            })?;

        match Flock::lock(file, FlockArg::LockExclusiveNonblock) {
            Ok(flock) => Ok(flock),
            Err((_file, errno)) if errno == Errno::EAGAIN || errno == Errno::EWOULDBLOCK => {
                Err(WakeError::AlreadyWaiting {
                    path: self.lock_path.clone(),
                })
            }
            Err((_file, errno)) => Err(WakeError::Lock {
                path: self.lock_path.clone(),
                source: io::Error::from_raw_os_error(errno as i32),
            }),
        }
    }

    /// Create the waiter directory and the FIFO node if they do not already
    /// exist, and verify an existing node is genuinely a FIFO.
    ///
    /// `mkfifo` returning `EEXIST` only tells us *a* node exists, not that it is a
    /// FIFO. A regular file left at the path would make `open(O_RDWR).read()`
    /// return `Ok(0)` (EOF) forever, hot-spinning the wait loop — so we `stat`
    /// and reject a non-FIFO node loudly ([`WakeError::NotAFifo`]).
    fn ensure_fifo(&self) -> Result<(), WakeError> {
        self.ensure_dir()?;
        // 0600: a wake FIFO is a user-scoped resource; no other user need signal
        // or observe it.
        let mode = Mode::S_IRUSR | Mode::S_IWUSR;
        match nix::unistd::mkfifo(&self.fifo_path, mode) {
            Ok(()) => Ok(()),
            Err(Errno::EEXIST) => self.verify_is_fifo(),
            Err(source) => Err(WakeError::Mkfifo {
                path: self.fifo_path.clone(),
                source,
            }),
        }
    }

    /// `stat` the existing FIFO-path node and confirm it is a FIFO.
    fn verify_is_fifo(&self) -> Result<(), WakeError> {
        let stat = nix::sys::stat::stat(&self.fifo_path).map_err(|source| WakeError::Mkfifo {
            path: self.fifo_path.clone(),
            source,
        })?;
        let file_type = SFlag::from_bits_truncate(stat.st_mode) & SFlag::S_IFMT;
        if file_type == SFlag::S_IFIFO {
            Ok(())
        } else {
            Err(WakeError::NotAFifo {
                path: self.fifo_path.clone(),
            })
        }
    }

    /// Create the waiters directory (idempotent). Shared by the lock and FIFO
    /// setup so either can be the first to run.
    fn ensure_dir(&self) -> Result<(), WakeError> {
        if let Some(parent) = self.fifo_path.parent() {
            std::fs::create_dir_all(parent).map_err(|source| WakeError::CreateDir {
                path: parent.to_path_buf(),
                source,
            })?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // The session-id filename encoding is now shared and authoritatively tested in
    // `mailbox_protocol::session`; here we only assert the per-session file paths
    // are built from it and sit under the waiters dir.
    #[test]
    fn per_session_paths_share_one_encoded_stem_under_waiters_dir() {
        let dir = Path::new("/tmp/mb/waiters");
        let s = SessionId::new("s1");
        assert_eq!(fifo_path(dir, &s), PathBuf::from("/tmp/mb/waiters/s1.fifo"));
        assert_eq!(lock_path(dir, &s), PathBuf::from("/tmp/mb/waiters/s1.lock"));
        assert_eq!(
            pidfile_path(dir, &s),
            PathBuf::from("/tmp/mb/waiters/s1.waiter.pid")
        );
        // An unsafe id keys all three files identically (the FIFO/lock/pidfile
        // coordination the wake loop relies on).
        let unsafe_id = SessionId::new("a/b");
        assert_eq!(
            fifo_path(dir, &unsafe_id)
                .file_stem()
                .unwrap()
                .to_str()
                .unwrap(),
            "a%2Fb"
        );
    }

    #[test]
    fn reminder_lists_only_topic_names() {
        let outcome = WakeOutcome {
            topics: vec![
                Topic::parse("github.pr.o/r#1").unwrap(),
                Topic::parse("github.pr.o/r#2").unwrap(),
            ],
            reason: WakeReason::Kicked,
        };
        assert_eq!(
            outcome.reminder(),
            "mail on topic github.pr.o/r#1, github.pr.o/r#2"
        );
    }
}
