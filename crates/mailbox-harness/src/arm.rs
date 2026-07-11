//! The arm decision, the waiter pidfile (reader side), and exec-into-waiter.
//!
//! `arm` is the `SessionStart` / `Stop` hook target. It launches a waiter only
//! when the session has subscriptions — otherwise there is nothing to be woken
//! about, so waking would be noise. The decision is modelled as a type so a
//! caller cannot forget the fail-safe: a bridge that is down or a session with no
//! subscriptions both resolve to [`ArmDecision::Skip`], never to a wake.
//!
//! When it does arm, the hook process **execs** the card-05 waiter (`mailbox
//! wait`), replacing its own image. exec preserves the PID, so the pidfile the
//! *waiter* writes for itself (after it takes the single-waiter lock) keeps
//! identifying the live waiter across the whole idle — including across the
//! waiter's self-respawn (`mailbox wait` re-execs itself on its max-block, same
//! PID). That single stable PID is what [`crate::cleanup`] reaps on `SessionEnd`.
//!
//! `arm` no longer writes the pidfile: the waiter owns it, written only after the
//! lock is held, so a doomed second arm (a lock loser) can never overwrite the
//! live waiter's pidfile with its own dead pid (card 11 HIGH#1 / ADR-0006). This
//! module keeps only the pidfile *path* + *reader*, which `cleanup` uses to reap.

use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::Command;

use mailbox_protocol::SessionId;

/// What the bridge said about a session's subscriptions when `arm` asked.
///
/// A four-way answer, not a `bool`, so "the bridge errored" and "the bridge was
/// unreachable" are their own cases (both skip arming, but they are logged apart
/// for honest diagnostics).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubscriptionProbe {
    /// The session has at least one subscription — arm a waiter.
    Subscribed,
    /// The bridge answered and the session has no subscriptions.
    NotSubscribed,
    /// The bridge was reachable but returned an error / unexpected reply. Fail
    /// SAFE: do not wake.
    BridgeError,
    /// The bridge could not be reached (down, timed out). Fail SAFE: do not wake.
    BridgeUnreachable,
}

/// Whether `arm` should launch a waiter, and if not, why.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArmDecision {
    /// Launch the waiter for this session.
    Arm,
    /// Do not launch a waiter; the reason is for an honest log line.
    Skip(SkipReason),
}

/// Why `arm` declined to launch a waiter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SkipReason {
    /// The session subscribes to nothing, so there is nothing to wake about.
    NotSubscribed,
    /// The bridge returned an error/unexpected reply; arming fail-safe → no wake.
    BridgeError,
    /// The bridge was unreachable; arming fail-safe → no wake.
    BridgeUnreachable,
}

impl SkipReason {
    /// Stable label for the structured log line.
    pub fn as_str(self) -> &'static str {
        match self {
            SkipReason::NotSubscribed => "not-subscribed",
            SkipReason::BridgeError => "bridge-error",
            SkipReason::BridgeUnreachable => "bridge-unreachable",
        }
    }
}

/// Map a subscription probe to an arm decision. The whole "arm iff subscribed,
/// fail-safe on a down/erroring bridge" rule, in one exhaustive match.
pub fn decide(probe: SubscriptionProbe) -> ArmDecision {
    match probe {
        SubscriptionProbe::Subscribed => ArmDecision::Arm,
        SubscriptionProbe::NotSubscribed => ArmDecision::Skip(SkipReason::NotSubscribed),
        SubscriptionProbe::BridgeError => ArmDecision::Skip(SkipReason::BridgeError),
        SubscriptionProbe::BridgeUnreachable => ArmDecision::Skip(SkipReason::BridgeUnreachable),
    }
}

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

/// Exec `mailbox wait --session <session> --max-block-ms <max_block_ms>`,
/// replacing the current process.
///
/// On success this never returns (the image is replaced); it returns the
/// [`std::io::Error`] only if the exec itself fails (e.g. the binary is missing).
/// Used both by `arm` to become the waiter and by the waiter to self-respawn, so
/// the same argv shape is built in exactly one place.
pub fn exec_waiter(mailbox_bin: &Path, session: &str, max_block_ms: u64) -> std::io::Error {
    Command::new(mailbox_bin)
        .arg("wait")
        .arg("--session")
        .arg(session)
        .arg("--max-block-ms")
        .arg(max_block_ms.to_string())
        .exec()
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

    #[test]
    fn subscribed_arms_everything_else_skips() {
        assert_eq!(decide(SubscriptionProbe::Subscribed), ArmDecision::Arm);
        assert_eq!(
            decide(SubscriptionProbe::NotSubscribed),
            ArmDecision::Skip(SkipReason::NotSubscribed)
        );
        assert_eq!(
            decide(SubscriptionProbe::BridgeError),
            ArmDecision::Skip(SkipReason::BridgeError)
        );
        assert_eq!(
            decide(SubscriptionProbe::BridgeUnreachable),
            ArmDecision::Skip(SkipReason::BridgeUnreachable)
        );
    }

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
