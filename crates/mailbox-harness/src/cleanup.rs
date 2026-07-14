//! Reaping the waiter on `SessionEnd`.
//!
//! The bridge half of cleanup (dropping the session's subscriptions + interests,
//! which stops now-orphaned adapters) travels over the socket from the `mailbox`
//! binary. This module owns the *process* half: terminate the session's waiter so
//! it does not outlive the session, and remove its pidfile.
//!
//! The waiter is one process for the life of its hook (the hook exec'd into it —
//! see [`crate::arm`]), and its pidfile names that PID, so reaping is: read the
//! pidfile, `SIGTERM` that PID if it is still alive, and delete the pidfile. A PID
//! that is already gone is normal (the waiter may have yielded for a re-arm, or been
//! killed at the hook timeout) and reads as `AlreadyGone`.

use std::path::Path;

use mailbox_protocol::SessionId;
use nix::errno::Errno;
use nix::sys::signal::{Signal, kill};
use nix::unistd::Pid;

use crate::arm::{pidfile_path, read_pidfile};

/// What reaping a session's waiter did — for an honest teardown log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReapOutcome {
    /// A live waiter was found and sent `SIGTERM`.
    Terminated { pid: u32 },
    /// A pidfile existed but its process was already gone (stale); we just cleaned
    /// the file up.
    AlreadyGone { pid: u32 },
    /// No pidfile — nothing was armed for this session.
    NoWaiter,
}

impl ReapOutcome {
    /// Stable label for the structured log line.
    pub fn as_str(self) -> &'static str {
        match self {
            ReapOutcome::Terminated { .. } => "terminated",
            ReapOutcome::AlreadyGone { .. } => "already-gone",
            ReapOutcome::NoWaiter => "no-waiter",
        }
    }
}

/// Terminate the session's waiter (if any) and remove its pidfile.
///
/// Best-effort and idempotent: a missing pidfile, an already-dead PID, or a
/// missing file on removal are all normal `SessionEnd` states, not errors. We do
/// not verify the PID still belongs to *our* waiter beyond liveness — the pidfile
/// lives in the owner-only waiters dir and holds a single stable PID, so a
/// mismatch would require PID reuse inside that window, which we accept as
/// negligible for a local dev bus.
pub fn reap_waiter(waiters_dir: &Path, session: &SessionId) -> ReapOutcome {
    let outcome = match read_pidfile(waiters_dir, session) {
        None => ReapOutcome::NoWaiter,
        Some(pid) => {
            let target = Pid::from_raw(pid as i32);
            // signal 0 probes liveness without delivering anything: ESRCH => gone.
            match kill(target, None) {
                Err(Errno::ESRCH) => ReapOutcome::AlreadyGone { pid },
                _ => {
                    let _ = kill(target, Signal::SIGTERM);
                    ReapOutcome::Terminated { pid }
                }
            }
        }
    };
    // Remove the pidfile regardless (NoWaiter leaves nothing to remove).
    let _ = std::fs::remove_file(pidfile_path(waiters_dir, session));
    outcome
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arm::write_pidfile_for_test;

    fn sess() -> SessionId {
        SessionId::new("s")
    }

    #[test]
    fn no_pidfile_is_no_waiter() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(reap_waiter(dir.path(), &sess()), ReapOutcome::NoWaiter);
    }

    #[test]
    fn stale_pidfile_is_already_gone_and_removed() {
        let dir = tempfile::tempdir().unwrap();
        // PID 2^31-1 is not a live process on our targets, so it reads as gone.
        let dead = i32::MAX as u32;
        write_pidfile_for_test(dir.path(), &sess(), dead).unwrap();
        assert_eq!(
            reap_waiter(dir.path(), &sess()),
            ReapOutcome::AlreadyGone { pid: dead }
        );
        // The pidfile is cleaned up so a later arm starts fresh.
        assert_eq!(read_pidfile(dir.path(), &sess()), None);
    }

    #[test]
    fn terminates_a_live_child_and_removes_the_pidfile() {
        let dir = tempfile::tempdir().unwrap();
        // A real, reap-able child that sleeps long enough to be signalled.
        let mut child = std::process::Command::new("sleep")
            .arg("30")
            .spawn()
            .expect("spawn sleep");
        write_pidfile_for_test(dir.path(), &sess(), child.id()).unwrap();

        let outcome = reap_waiter(dir.path(), &sess());
        assert_eq!(outcome, ReapOutcome::Terminated { pid: child.id() });

        // SIGTERM ends `sleep`; the child must actually exit (bounded wait).
        let status = wait_bounded(&mut child, std::time::Duration::from_secs(5));
        assert!(!status.success(), "sleep should die from SIGTERM");
        assert_eq!(read_pidfile(dir.path(), &sess()), None);
    }

    /// Reap `child` within `timeout`, killing it if it overruns so no stray sleep
    /// survives the test.
    fn wait_bounded(
        child: &mut std::process::Child,
        timeout: std::time::Duration,
    ) -> std::process::ExitStatus {
        let deadline = std::time::Instant::now() + timeout;
        loop {
            if let Some(status) = child.try_wait().expect("try_wait") {
                return status;
            }
            if std::time::Instant::now() >= deadline {
                let _ = child.kill();
                return child.wait().expect("wait");
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
    }
}
