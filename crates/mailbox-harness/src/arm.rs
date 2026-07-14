//! The arm decision, the waiter pidfile (reader side), and exec-into-waiter.
//!
//! `arm` is the `SessionStart` / `Stop` hook target. It launches a waiter unless the
//! bridge says, cleanly, that the session subscribes to nothing — then there is
//! nothing to be woken about, so waking would be noise.
//!
//! # Why a failed probe ARMS ANYWAY (fail-OPEN)
//!
//! It used to skip: a bridge that was down or erroring resolved to `Skip`, "failing
//! safe" by not waking. That was backwards, and it was a permanent-deafness bug.
//! `arm` runs on `Stop`, and the re-arm loop now depends on it running successfully
//! *every* `max_block`, forever (ADR-0006). A momentary bridge blip at one of those
//! re-arm `Stop`s left the session with NO waiter — and an idle session fires no
//! further `Stop`, so nothing ever retried. One blip, deaf forever.
//!
//! Arming without a confirmed subscription is safe because **the waiter validates
//! itself**: `mailbox wait` needs no daemon socket (it opens the store read-only) and
//! re-checks `has_subscription` *after* taking the single-waiter lock. A session that
//! genuinely subscribes to nothing therefore gets a waiter that immediately self-exits
//! 0 and removes its own pidfile — the same outcome `Skip` would have produced, minus
//! the deafness. And if the store cannot be opened at all, the waiter exits cleanly
//! too.
//!
//! Exiting 2 on a probe failure would be the other way to keep the loop alive, and it
//! is WRONG: it would hot-loop wakes for as long as the bridge is down. Blocking is
//! correct — when the daemon comes back and publishes, the kick reaches the waiter
//! that is already blocked on the FIFO.
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
/// unreachable" are their own cases: both arm anyway, but they are logged apart for
/// honest diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubscriptionProbe {
    /// The session has at least one subscription — arm a waiter.
    Subscribed,
    /// The bridge answered and the session has no subscriptions.
    NotSubscribed,
    /// The bridge was reachable but returned an error / unexpected reply.
    BridgeError,
    /// The bridge could not be reached (down, timed out).
    BridgeUnreachable,
}

/// Why the bridge could not tell us whether the session is subscribed.
///
/// Kept as its own type (rather than folded into "skip") because the *decision* it
/// leads to is now the opposite of a skip: we arm regardless, and this is only the
/// label on the `warn!` that says so.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProbeFailure {
    /// The bridge returned an error / unexpected reply.
    BridgeError,
    /// The bridge was unreachable (down, timed out).
    BridgeUnreachable,
}

impl ProbeFailure {
    /// Stable label for the structured log line.
    pub fn as_str(self) -> &'static str {
        match self {
            ProbeFailure::BridgeError => "bridge-error",
            ProbeFailure::BridgeUnreachable => "bridge-unreachable",
        }
    }
}

/// Whether `arm` should launch a waiter — and, when it does so without a confirmed
/// subscription, why.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArmDecision {
    /// The bridge confirmed at least one subscription: launch the waiter.
    Arm,
    /// The probe FAILED, so we do not know. Launch the waiter anyway (fail-open — see
    /// the module docs): the waiter re-checks `has_subscription` itself after taking
    /// its lock, so an unsubscribed session self-exits cleanly, whereas NOT arming
    /// would leave an idle session permanently unwakeable.
    ArmUnverified(ProbeFailure),
    /// The bridge answered cleanly: this session subscribes to nothing, so there is
    /// nothing to wake it about. The ONLY case that does not arm.
    Skip,
}

/// Map a subscription probe to an arm decision: arm unless the bridge said, clearly,
/// that there are no subscriptions — the whole rule, in one exhaustive match.
pub fn decide(probe: SubscriptionProbe) -> ArmDecision {
    match probe {
        SubscriptionProbe::Subscribed => ArmDecision::Arm,
        SubscriptionProbe::NotSubscribed => ArmDecision::Skip,
        // Fail-OPEN. A bridge blip at a re-arm `Stop` used to skip the waiter, and an
        // idle session fires no further `Stop` to retry — so one blip deafened the
        // session forever. The waiter's own post-lock re-check is what makes arming
        // on an unknown subscription state safe.
        SubscriptionProbe::BridgeError => ArmDecision::ArmUnverified(ProbeFailure::BridgeError),
        SubscriptionProbe::BridgeUnreachable => {
            ArmDecision::ArmUnverified(ProbeFailure::BridgeUnreachable)
        }
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
    /// The pidfile changed under us between the liveness probe and the delete: a
    /// concurrent arm's waiter took the lock and wrote its own pid. Left ALONE — the
    /// whole point of the compare-and-delete (see [`reap_stale_pidfile`]).
    Raced { pid: u32 },
}

impl StalePidfile {
    /// Stable label for the structured log line.
    pub fn as_str(self) -> &'static str {
        match self {
            StalePidfile::None => "none",
            StalePidfile::Live { .. } => "live",
            StalePidfile::Reaped { .. } => "reaped",
            StalePidfile::Raced { .. } => "raced",
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
/// # Compare-and-delete, not delete-by-path
///
/// The delete is guarded by a re-read: we unlink ONLY if the file still names the
/// same dead pid we probed. Deleting by path alone was a TOCTOU bug — between the
/// `kill(P, 0)` that proved P dead and the `remove_file`, a concurrent `arm`'s waiter
/// can take the single-waiter lock and write its own LIVE pid into that very file, and
/// we would then delete a live waiter's pidfile. The damage is real and lasting: the
/// waiter keeps running and keeps the lock (so no later waiter replaces the file), but
/// `cleanup` has nothing to reap on `SessionEnd` (an orphan surviving the session) and
/// `agents` / `status` under-report it.
///
/// Residual, stated honestly: the re-read narrows the window to the microseconds
/// between it and the `unlink` — POSIX has no atomic compare-and-unlink, and taking
/// the waiter lock here would be worse (a concurrent waiter would see the lock held
/// and exit as a "loser", which is exactly the un-armed session we are trying to
/// prevent). PID reuse is the same accepted hazard as `waiter_alive`: `kill(pid, 0)`
/// only proves *some* process holds that pid.
pub fn reap_stale_pidfile(waiters_dir: &Path, session: &SessionId) -> StalePidfile {
    reap_stale_pidfile_racing(waiters_dir, session, || {})
}

/// [`reap_stale_pidfile`] with a seam for the ONE race that matters: `interleave` runs
/// between the liveness probe and the compare-and-delete, so a test can deterministically
/// plant a concurrent waiter's live pid in that window instead of hoping to hit it.
/// Production passes a no-op.
fn reap_stale_pidfile_racing(
    waiters_dir: &Path,
    session: &SessionId,
    interleave: impl FnOnce(),
) -> StalePidfile {
    let Some(pid) = read_pidfile(waiters_dir, session) else {
        return StalePidfile::None;
    };
    // Signal 0 probes liveness without delivering anything: ESRCH => the pid is gone.
    if kill(Pid::from_raw(pid as i32), None) != Err(Errno::ESRCH) {
        return StalePidfile::Live { pid };
    }
    interleave();
    // Compare-and-delete: whoever owns the pidfile NOW must still be the dead pid we
    // just probed. If a concurrent waiter has rewritten it with its own live pid, that
    // file is not ours to remove.
    match read_pidfile(waiters_dir, session) {
        Some(current) if current == pid => {
            match std::fs::remove_file(pidfile_path(waiters_dir, session)) {
                Ok(()) => StalePidfile::Reaped { pid },
                Err(_) => StalePidfile::None,
            }
        }
        // Rewritten under us: a concurrent arm's waiter claimed the session while we
        // were probing. Not ours to remove.
        Some(current) => StalePidfile::Raced { pid: current },
        // Removed under us (a racing `cleanup`): nothing left to reap.
        None => StalePidfile::None,
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

    /// The ONLY thing that must not arm is a clean "you subscribe to nothing". A
    /// failed probe arms anyway (fail-open): skipping it left an idle session with no
    /// waiter, and an idle session fires no further `Stop`, so nothing ever retried —
    /// one bridge blip and the session was deaf forever.
    #[test]
    fn only_a_clean_not_subscribed_skips_a_failed_probe_arms_anyway() {
        assert_eq!(decide(SubscriptionProbe::Subscribed), ArmDecision::Arm);
        assert_eq!(decide(SubscriptionProbe::NotSubscribed), ArmDecision::Skip);
        assert_eq!(
            decide(SubscriptionProbe::BridgeError),
            ArmDecision::ArmUnverified(ProbeFailure::BridgeError)
        );
        assert_eq!(
            decide(SubscriptionProbe::BridgeUnreachable),
            ArmDecision::ArmUnverified(ProbeFailure::BridgeUnreachable)
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

    /// The TOCTOU that delete-by-path had: between "pid P is dead" and the unlink, a
    /// concurrent arm's waiter takes the single-waiter lock and writes its own LIVE pid
    /// into the same file. Deleting by path then destroys a LIVE waiter's pidfile — the
    /// waiter keeps running and keeps the lock (so nothing rewrites the file), leaving
    /// `cleanup` nothing to reap on `SessionEnd` and `agents`/`status` under-reporting it.
    /// The compare-and-delete must see the new pid and leave the file alone.
    #[test]
    fn a_concurrent_waiters_live_pidfile_is_never_deleted_by_the_reaper() {
        let dir = tempfile::tempdir().unwrap();
        let s = SessionId::new("s1");
        let dead = 2_000_000_000u32; // above any platform PID_MAX: never a live pid.
        let live = std::process::id();

        write_pidfile_for_test(dir.path(), &s, dead).unwrap();

        // The race, deterministically: the concurrent waiter wins the lock and records
        // itself in the window between our liveness probe and our delete.
        let outcome = reap_stale_pidfile_racing(dir.path(), &s, || {
            write_pidfile_for_test(dir.path(), &s, live).unwrap();
        });

        assert_eq!(outcome, StalePidfile::Raced { pid: live });
        assert_eq!(
            read_pidfile(dir.path(), &s),
            Some(live),
            "the live waiter's pidfile must survive the reap (else cleanup cannot reap it)"
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
