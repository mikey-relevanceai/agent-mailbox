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
//! *waiter* writes for itself (after it takes the single-waiter lock) identifies
//! the live waiter for as long as that hook process lives. That PID is what
//! [`crate::cleanup`] reaps on `SessionEnd`.
//!
//! The waiter does NOT try to outlive its hook process. Claude Code kills a hook at
//! its `timeout`, and that deadline is per-process: an `execv` in place (same PID)
//! does not reset it, so a waiter cannot self-respawn its way past the kill. It
//! therefore yields *before* the deadline — exit 2, the benign re-arm notice — and
//! the resulting `Stop` runs `arm` again, producing a FRESH hook process with a
//! FRESH timeout (ADR-0006).
//!
//! `arm` no longer writes the pidfile: the waiter owns it, written only after the
//! lock is held, so a doomed second arm (a lock loser) can never overwrite the
//! live waiter's pidfile with its own dead pid (card 11 HIGH#1 / ADR-0006). This
//! module keeps the pidfile *path* + *reader* (which `cleanup` uses to reap) and
//! the stale-pidfile reaper `arm` runs before it execs.

use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::Command;

use mailbox_protocol::SessionId;
use nix::errno::Errno;
use nix::sys::signal::kill;
use nix::unistd::Pid;

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

/// What [`reap_stale_pidfile`] found (and did) for a session's waiter pidfile.
///
/// A named outcome rather than a bare `bool`/`Option` so the caller logs the honest
/// reason: "there is a live waiter" and "I removed a dead one" are opposite facts,
/// and conflating them is what let a killed waiter keep passing for a live one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StalePidfile {
    /// No pidfile (or an unparseable one): nothing to reap.
    None,
    /// The pidfile names a process that is still alive — the live waiter (or a
    /// SessionStart-vs-Stop arm race whose waiter holds the lock). Left ALONE: the
    /// incoming waiter will lose the single-waiter lock and exit without touching
    /// it, which is the invariant working correctly.
    Live { pid: u32 },
    /// The pidfile named a DEAD pid and was removed. That is the fingerprint of a
    /// waiter the harness killed at its hook `timeout`: the process is gone but its
    /// pidfile survives, so `mailbox agents` / `status` keep reporting a live waiter
    /// that does not exist. Reaped here, on the way to arming a real one.
    Reaped { pid: u32 },
}

impl StalePidfile {
    /// Stable label for the structured log line.
    pub fn as_str(self) -> &'static str {
        match self {
            StalePidfile::None => "none",
            StalePidfile::Live { .. } => "live",
            StalePidfile::Reaped { .. } => "reaped",
        }
    }
}

/// Remove a pidfile that names a DEAD pid, so `arm` never leaves one behind to
/// impersonate a waiter that no longer exists.
///
/// A live pid is left untouched — see [`StalePidfile::Live`]. Best-effort: a
/// removal failure is reported as `None` rather than failing the hook (the incoming
/// waiter overwrites the pidfile under lock anyway).
///
/// Note the residual PID-reuse hazard, unchanged from `waiter_alive`: `kill(pid, 0)`
/// only proves *some* process holds that pid. We accept it for a local dev bus.
pub fn reap_stale_pidfile(waiters_dir: &Path, session: &SessionId) -> StalePidfile {
    let Some(pid) = read_pidfile(waiters_dir, session) else {
        return StalePidfile::None;
    };
    // Signal 0 probes liveness without delivering anything: ESRCH => the pid is gone.
    if kill(Pid::from_raw(pid as i32), None) != Err(Errno::ESRCH) {
        return StalePidfile::Live { pid };
    }
    match std::fs::remove_file(pidfile_path(waiters_dir, session)) {
        Ok(()) => StalePidfile::Reaped { pid },
        Err(_) => StalePidfile::None,
    }
}

/// Exec `mailbox wait --session <session> --max-block-ms <max_block_ms>`,
/// replacing the current process.
///
/// On success this never returns (the image is replaced); it returns the
/// [`std::io::Error`] only if the exec itself fails (e.g. the binary is missing).
/// The ONE caller is `arm`, which becomes the waiter this way — so the hook process
/// *is* the waiter, and Claude Code's per-hook `timeout` bounds its whole life. (It
/// used to be called a second time by the waiter itself, to self-respawn at its
/// max-block; that was removed once we established that `execv` does not reset the
/// hook timeout, which made the respawn pointless — ADR-0006.)
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
    fn reap_stale_pidfile_removes_a_dead_pid_but_never_a_live_one() {
        let dir = tempfile::tempdir().unwrap();
        let s = SessionId::new("s1");

        // Nothing armed yet.
        assert_eq!(reap_stale_pidfile(dir.path(), &s), StalePidfile::None);

        // The killed-waiter fingerprint: a pidfile naming a pid that is gone. It
        // must be removed, or `agents`/`status` keep reporting a phantom waiter.
        // 2_000_000_000 is above any platform PID_MAX, so the probe is never a
        // false positive.
        let dead = 2_000_000_000u32;
        write_pidfile_for_test(dir.path(), &s, dead).unwrap();
        assert_eq!(
            reap_stale_pidfile(dir.path(), &s),
            StalePidfile::Reaped { pid: dead }
        );
        assert_eq!(read_pidfile(dir.path(), &s), None);

        // A LIVE waiter's pidfile is left alone (the SessionStart-vs-Stop arm race:
        // the loser must not disturb the winner's record).
        let live = std::process::id();
        write_pidfile_for_test(dir.path(), &s, live).unwrap();
        assert_eq!(
            reap_stale_pidfile(dir.path(), &s),
            StalePidfile::Live { pid: live }
        );
        assert_eq!(read_pidfile(dir.path(), &s), Some(live));
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
