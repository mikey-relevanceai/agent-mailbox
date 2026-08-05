//! The per-session waiter pidfile, reader side.
//!
//! The detached watcher (`mailbox harness watch`) writes `<session>.waiter.pid`
//! AFTER it takes the single-waiter lock, so the file's presence means "blocked and
//! listening", not merely "process spawned". Everything that needs to know whether a
//! session is still wakeable reads it from here: `mailbox agents` for liveness, the
//! daemon's sweep for interest refresh (ADR-0009), and `cleanup` to reap the watcher
//! at `SessionEnd`.
//!
//! This module used to also hold the `arm` decision and the exec-into-waiter path
//! for the ADR-0006 re-arm loop. That loop is gone (ADR-0008 replaced it with the
//! watcher + `FileChanged` wake), and so is `mailbox harness arm` — what is left is
//! the pidfile itself.

use std::path::{Path, PathBuf};

use mailbox_protocol::SessionId;

/// The pidfile that records the live waiter's PID for a session.
///
/// It sits beside the waiter FIFO/lock under `waiters_dir`, named with the SAME
/// collision-free session encoding ([`SessionId::encode_filename`]) and the SAME
/// `.waiter.pid` suffix the waiter (`mailbox::wake`) writes — the two MUST agree
/// or `cleanup` would look for the pidfile under the wrong name.
pub fn pidfile_path(waiters_dir: &Path, session: &SessionId) -> PathBuf {
    waiters_dir.join(format!("{}.waiter.pid", session.encode_filename()))
}

/// Read the session's recorded waiter PID, or `None` if there is no pidfile (or it
/// is empty/garbage — treated as "no live waiter recorded" rather than an error,
/// since a stale/half-written file must not wedge cleanup).
pub fn read_pidfile(waiters_dir: &Path, session: &SessionId) -> Option<u32> {
    let text = std::fs::read_to_string(pidfile_path(waiters_dir, session)).ok()?;
    text.trim().parse().ok()
}

/// Test-only helper to plant a pidfile (production writes it inside the waiter,
/// `mailbox::wake`). Lets the cleanup/reap tests simulate a recorded waiter.
#[cfg(test)]
pub(crate) fn write_pidfile_for_test(
    waiters_dir: &Path,
    session: &SessionId,
    pid: u32,
) -> std::io::Result<()> {
    std::fs::create_dir_all(waiters_dir)?;
    std::fs::write(pidfile_path(waiters_dir, session), pid.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The ONLY thing that must not arm is a clean "you subscribe to nothing". A
    /// failed probe arms anyway (fail-open): skipping it left an idle session with no
    /// waiter, and an idle session fires no further `Stop`, so nothing ever retried —
    /// one bridge blip and the session was deaf forever.
    #[test]
    fn pidfile_round_trips_and_sits_under_waiters_dir() {
        let dir = tempfile::tempdir().unwrap();
        let s = SessionId::new("s1");
        assert_eq!(read_pidfile(dir.path(), &s), None);
        write_pidfile_for_test(dir.path(), &s, 4321).unwrap();
        assert_eq!(read_pidfile(dir.path(), &s), Some(4321));
        assert_eq!(
            pidfile_path(dir.path(), &s),
            dir.path().join("s1.waiter.pid")
        );
    }

    #[test]
    fn pidfile_name_encodes_unsafe_session_ids() {
        // A slash in the session id must not create a subdirectory.
        assert_eq!(
            pidfile_path(Path::new("/w"), &SessionId::new("a/b")),
            Path::new("/w/a%2Fb.waiter.pid")
        );
    }
}
