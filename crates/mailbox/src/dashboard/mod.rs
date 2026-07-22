//! `mailbox dashboard`: a live, fleet-wide view of whether the bus is actually
//! working.
//!
//! Every other read-only view answers "what does this session have?". This one
//! answers "is the wake path doing its job, for anyone?" — the question that has no
//! other home, because the interesting failure is invisible from inside a single
//! session. A deaf session reports a registered inbox, a live watcher and a moving
//! sentinel; `mailbox status` shows it as healthy right up until it misses its mail.
//!
//! So the view is organised around evidence rather than configuration:
//! [`wake_health`] reconstructs, per session, whether the harness has ever actually
//! run the wake hook, and [`snapshot`] ranks the fleet worst-first so the sessions
//! that cannot be woken are the first thing on screen.
//!
//! It reads the store READ-ONLY rather than through the daemon socket (ADR-0015), so
//! it still renders when the bridge is down — which is precisely when someone wants
//! to look at it.

pub mod snapshot;
pub mod ui;
pub mod wake_health;

pub use snapshot::{DaemonState, SessionRow, Snapshot};
pub use wake_health::{WakeHealth, WakeSummary};

/// Why the dashboard could not run.
///
/// A terminal failure is deliberately NOT folded into `StorageError`: driving the
/// terminal is this module's own concern, and someone who sees one should not have to
/// wonder whether their database is broken.
#[derive(Debug, thiserror::Error)]
pub enum DashboardError {
    /// The durable store could not be read — the one failure that genuinely prevents
    /// a snapshot.
    #[error(transparent)]
    Store(#[from] crate::storage::StorageError),
    /// The terminal could not be driven (not a TTY, or raw mode was refused).
    #[error("could not drive the terminal: {0}")]
    Terminal(#[from] std::io::Error),
}
