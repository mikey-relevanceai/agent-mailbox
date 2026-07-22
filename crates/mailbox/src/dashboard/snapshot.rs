//! One instant's view of the fleet, assembled from the four places the truth lives.
//!
//! No single source knows whether the bus is working:
//!
//! - the **store** knows who subscribes to what and who is behind;
//! - the **waiters directory** knows which sessions have a live watcher process;
//! - the **harness log** knows whether the harness has ever actually run a wake hook
//!   for a session ([`super::wake_health`]) — the only evidence that the last hop
//!   exists at all;
//! - the **socket** says whether the daemon is up.
//!
//! Joining them is the point. A session is only healthy if all of them agree, and
//! every real wake failure so far has been a case where three of them looked fine.

use std::path::Path;

use crate::dashboard::wake_health::{WakeHealth, WakeSummary};
use crate::storage::{Fleet, FleetWatch, ReadOnlyStore, SessionId, StorageConfig, StorageError};
use crate::wake::waiter_alive;

/// Whether the `serve` daemon is accepting connections.
///
/// Reported rather than required: the dashboard is most useful when things are
/// broken, and "the bridge is down" is a headline, not a reason to render nothing
/// (ADR-0015).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DaemonState {
    Up,
    Down,
}

impl DaemonState {
    /// Probe the daemon by connecting to its socket. A refused or missing socket is
    /// [`DaemonState::Down`]; nothing here can fail the snapshot.
    pub fn probe(socket: &Path) -> Self {
        match std::os::unix::net::UnixStream::connect(socket) {
            Ok(_) => DaemonState::Up,
            Err(_) => DaemonState::Down,
        }
    }
}

/// One session as the dashboard shows it.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct SessionRow {
    pub session: String,
    /// Whether peers can `send` to it at all.
    pub inbox_registered: bool,
    /// Whether a watcher process is alive for it right now.
    pub live_waiter: bool,
    pub unread: u64,
    pub subscriptions: u64,
    pub watch_interest: u64,
    /// What the harness log says about its ability to be woken.
    pub health: WakeHealth,
}

impl SessionRow {
    /// How alarming this row is, highest first. Drives the default ordering, so the
    /// sessions that are actually failing are on screen without scrolling.
    ///
    /// The ranking is deliberately about CONSEQUENCE, not just health: a session with
    /// no observed wake AND unread mail is actively missing messages right now, which
    /// is strictly worse than one that merely looks deaf while idle. A dashboard that
    /// sorted by session id would bury the only row that matters among forty that do
    /// not.
    pub fn severity(&self) -> u8 {
        match (&self.health, self.unread > 0) {
            // Sitting on mail it cannot be woken for: the failure, in progress.
            (WakeHealth::NoWakeObserved { .. }, true) => 5,
            // No wake ever observed. Idle for now, but it will miss the next message.
            (WakeHealth::NoWakeObserved { .. }, false) => 4,
            // Unread with no watcher: nothing is even listening for a kick.
            _ if self.unread > 0 && !self.live_waiter => 3,
            // Unread on a session we have no wake evidence for either way.
            (WakeHealth::Unproven, true) => 2,
            // Unread, but the wake path is proven — it will be delivered.
            (_, true) => 1,
            _ => 0,
        }
    }
}

/// The whole fleet at one instant.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct Snapshot {
    pub daemon: DaemonState,
    /// Sessions, worst-first (see [`SessionRow::severity`]).
    pub rows: Vec<SessionRow>,
    pub watches: Vec<FleetWatch>,
    pub events: u64,
    /// Sessions whose wake path is proven to work.
    pub verified: usize,
    /// Sessions that have been bumped and never woken.
    pub suspect: usize,
    /// Whether the wake evidence is based on only the tail of the log, so "never"
    /// means "not within the window".
    pub wake_truncated: bool,
}

impl Snapshot {
    /// Gather a snapshot: read the store, probe the daemon, check each session's
    /// watcher, and classify wake health from the log.
    ///
    /// Only the STORE read can fail the snapshot. A missing log, an unreadable
    /// waiters directory or a dead daemon all degrade to "less is known", never to an
    /// error — the view has to survive exactly the conditions it is used to diagnose.
    pub fn gather(config: &StorageConfig) -> Result<Self, StorageError> {
        let fleet = ReadOnlyStore::open(config.path())?.fleet()?;
        let wake = WakeSummary::from_log(&config.harness_log_path());
        let daemon = DaemonState::probe(&config.socket_path());
        let waiters = config.waiters_dir();
        Ok(Self::assemble(fleet, wake, daemon, &|session| {
            waiter_alive(&waiters, &SessionId::new(session))
        }))
    }

    /// The pure assembly step: join the parts and rank them.
    ///
    /// `live_waiter` is injected so this — including the ordering that decides what a
    /// human sees first — is testable from plain values, with no store, no log file
    /// and no processes to fake.
    pub fn assemble(
        fleet: Fleet,
        wake: WakeSummary,
        daemon: DaemonState,
        live_waiter: &dyn Fn(&str) -> bool,
    ) -> Self {
        let (verified, suspect) = wake.tally();
        let mut rows: Vec<SessionRow> = fleet
            .sessions
            .into_iter()
            .map(|s| SessionRow {
                health: wake.get(&s.session),
                live_waiter: live_waiter(&s.session),
                session: s.session,
                inbox_registered: s.inbox_registered,
                unread: s.unread,
                subscriptions: s.subscriptions,
                watch_interest: s.watch_interest,
            })
            .collect();

        // Worst first, then most-behind first. Session id last so the order is total
        // and the view does not shuffle between refreshes that changed nothing.
        rows.sort_by(|a, b| {
            b.severity()
                .cmp(&a.severity())
                .then(b.unread.cmp(&a.unread))
                .then(a.session.cmp(&b.session))
        });

        Self {
            daemon,
            rows,
            watches: fleet.watches,
            events: fleet.events,
            verified,
            suspect,
            wake_truncated: wake.truncated,
        }
    }

    /// The rows a `--deaf-only` view keeps: sessions with no observed wake.
    pub fn suspect_rows(&self) -> impl Iterator<Item = &SessionRow> {
        self.rows.iter().filter(|r| r.health.is_suspect())
    }

    /// How many of these sessions have a live watcher — the fleet that actually
    /// exists right now.
    ///
    /// Subscriptions outlive their session when `SessionEnd` never ran (a crash, a
    /// `kill -9`), so the store knows about far more sessions than are running. Those
    /// rows are real but inert: a dead session cannot be woken and is not a fault.
    /// They are counted rather than dropped, because "306 known, 45 live" is itself
    /// worth seeing — it is uncollected garbage — while showing all 306 by default
    /// would bury the handful that matter.
    pub fn live_count(&self) -> usize {
        self.rows.iter().filter(|r| r.live_waiter).count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::FleetSession;

    fn session(id: &str, unread: u64) -> FleetSession {
        FleetSession {
            session: id.to_string(),
            subscriptions: 1,
            inbox_registered: true,
            unread,
            watch_interest: 0,
        }
    }

    fn fleet(sessions: Vec<FleetSession>) -> Fleet {
        Fleet {
            sessions,
            watches: vec![],
            events: 0,
        }
    }

    fn wake_from(lines: &[&str]) -> WakeSummary {
        WakeSummary::from_lines(lines.iter().copied())
    }

    /// The ordering IS the feature. A session sitting on mail it cannot be woken for
    /// must outrank everything, including a session with far more unread whose wake
    /// works — because the second one will get its mail and the first will not.
    #[test]
    fn a_deaf_session_with_mail_outranks_a_healthy_session_with_more_mail() {
        let snapshot = Snapshot::assemble(
            fleet(vec![session("healthy", 99), session("deaf", 1)]),
            wake_from(&[
                "INFO FileChanged wake: genuine unread mail session=healthy",
                r#"INFO wake: watcher wrote the wake sentinel session="deaf" topics="t""#,
            ]),
            DaemonState::Up,
            &|_| true,
        );

        assert_eq!(
            snapshot.rows.first().map(|r| r.session.as_str()),
            Some("deaf"),
            "the session that cannot receive its mail must be the first thing on screen"
        );
        assert_eq!(snapshot.verified, 1);
        assert_eq!(snapshot.suspect, 1);
    }

    /// A deaf session with no mail still outranks a healthy one that is merely behind:
    /// it is broken, the other is just busy.
    #[test]
    fn a_deaf_idle_session_still_outranks_a_healthy_session_with_unread() {
        let snapshot = Snapshot::assemble(
            fleet(vec![session("healthy", 5), session("deaf", 0)]),
            wake_from(&[
                "INFO FileChanged wake: nothing unread session=healthy",
                r#"INFO wake: watcher wrote the wake sentinel session="deaf" topics="t""#,
            ]),
            DaemonState::Up,
            &|_| true,
        );
        assert_eq!(
            snapshot.rows.first().map(|r| r.session.as_str()),
            Some("deaf")
        );
    }

    /// Unread with no live watcher is its own failure — nothing is listening for the
    /// kick — and must rank above an ordinary behind-but-healthy session.
    #[test]
    fn unread_with_no_live_watcher_ranks_above_unread_with_one() {
        let snapshot = Snapshot::assemble(
            fleet(vec![session("watched", 3), session("orphan", 3)]),
            wake_from(&[]),
            DaemonState::Up,
            &|s| s == "watched",
        );
        assert_eq!(
            snapshot.rows.first().map(|r| r.session.as_str()),
            Some("orphan"),
            "a session with mail and no watcher has nobody to deliver it"
        );
    }

    /// Refreshing must not reshuffle rows that did not change, or the view is unusable
    /// on a timer. Equal severity and equal unread fall back to session id.
    #[test]
    fn equal_rows_are_ordered_deterministically_by_session_id() {
        let build = || {
            Snapshot::assemble(
                fleet(vec![
                    session("ccc", 1),
                    session("aaa", 1),
                    session("bbb", 1),
                ]),
                wake_from(&[]),
                DaemonState::Up,
                &|_| true,
            )
        };
        let ids: Vec<_> = build().rows.iter().map(|r| r.session.clone()).collect();
        assert_eq!(ids, vec!["aaa", "bbb", "ccc"]);
        assert_eq!(
            build(),
            build(),
            "the same inputs must render the same order"
        );
    }

    /// A dead daemon is reported, not fatal: every row still renders. This is the
    /// ADR-0015 property — the view survives the failure it is there to show.
    #[test]
    fn a_down_daemon_still_produces_a_full_snapshot() {
        let snapshot = Snapshot::assemble(
            fleet(vec![session("s1", 2)]),
            wake_from(&[]),
            DaemonState::Down,
            &|_| false,
        );
        assert_eq!(snapshot.daemon, DaemonState::Down);
        assert_eq!(snapshot.rows.len(), 1, "rows must survive a down daemon");
    }

    #[test]
    fn deaf_only_keeps_exactly_the_sessions_with_no_observed_wake() {
        let snapshot = Snapshot::assemble(
            fleet(vec![
                session("ok", 0),
                session("deaf", 0),
                session("quiet", 0),
            ]),
            wake_from(&[
                "INFO FileChanged wake: nothing unread session=ok",
                r#"INFO wake: watcher wrote the wake sentinel session="deaf" topics="t""#,
            ]),
            DaemonState::Up,
            &|_| true,
        );
        let kept: Vec<_> = snapshot.suspect_rows().map(|r| &r.session).collect();
        assert_eq!(
            kept,
            vec!["deaf"],
            "'quiet' has no evidence, so it is not a failure"
        );
    }
}
