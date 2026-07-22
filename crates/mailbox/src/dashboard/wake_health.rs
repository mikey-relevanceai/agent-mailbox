//! Whether a session's wake wire actually works, reconstructed from `harness.log`.
//!
//! # Why this has to be inferred rather than asked
//!
//! The wake path ends outside our process. `mailbox` bumps a sentinel file and the
//! HARNESS (Claude Code) is supposed to notice, match its `FileChanged` matcher, and
//! run `mailbox harness wake`. We cannot query whether that watch was established —
//! there is no API for it, and a `watchPaths` registration that never took effect
//! looks *identical from our side* to one that did: we write the file either way.
//!
//! The one observable difference is whether `mailbox harness wake` ever RAN. That
//! process logs on every path it can take, so its presence in `harness.log` is proof
//! the harness is watching, and its absence — across sentinel bumps that should have
//! triggered it — is evidence that nothing is.
//!
//! That asymmetry is the whole point of this module. A session whose inbox is
//! registered, whose watcher is alive, and whose sentinel is being bumped can still
//! be completely unwakeable, and every other view reports it as healthy. This is the
//! only place that distinguishes the two.
//!
//! # Evidence, not certainty
//!
//! [`WakeHealth`] is deliberately three-valued and named for the evidence rather than
//! the conclusion. Absence of a wake line is not proof of deafness — the log may have
//! been rotated or truncated, or the bumps may all have landed while the session was
//! mid-turn, which is a lost edge rather than a missing watch (ADR-0012). So a session
//! with bumps and no wakes is reported as [`WakeHealth::NoWakeObserved`], with the bump
//! count that makes the claim checkable, and one that has never been bumped at all is
//! [`WakeHealth::Unproven`] rather than being lumped in with the failures. Overstating
//! this would make the dashboard exactly as misleading as the thing it exists to catch.

use std::collections::HashMap;

/// The substring identifying a line written by the `FileChanged` wake hook
/// (`mailbox harness wake`). Every branch of that handler logs, including the two
/// no-op ones, so ANY line matching this proves the harness ran the hook — which is
/// the only thing that proves the watch exists.
///
/// Matching a substring rather than the full message is deliberate: the wording of
/// the individual outcomes is free to change, but "a wake hook ran for this session"
/// must keep being recognisable. `dashboard_classifies_a_real_wake_as_verified` in
/// `tests/dashboard.rs` drives a genuine wake end to end and fails if this drifts.
const WAKE_HOOK_MARKER: &str = "FileChanged wake:";

/// The substring identifying a sentinel bump — the watcher telling the harness there
/// is something to wake for. Emitted by `wake::Waiter` on every kick and by the
/// ADR-0012 turn-boundary re-trigger.
const SENTINEL_BUMP_MARKER: &str = "wrote the wake sentinel";

/// The substring identifying the ADR-0012 turn-boundary re-bump, which is a bump like
/// any other but worth counting separately: it is the level-triggered rescue, and if
/// those never produce a wake either, the rescue is not working.
const RETRIGGER_MARKER: &str = "re-bumped the wake sentinel";

/// How much of the tail of `harness.log` to read.
///
/// The log is append-only and unbounded, and a dashboard that refreshes on a timer
/// must not degrade as it grows. 8 MiB is far more than a day of a busy fleet, and
/// reading the TAIL means the most recent (most relevant) history always wins. The
/// cost is that "never woken" is really "never woken within the last 8 MiB of log" —
/// which [`WakeSummary::truncated`] reports rather than hides, because a dashboard
/// that silently drops evidence is the failure mode this whole module exists to catch.
const MAX_LOG_TAIL_BYTES: u64 = 8 * 1024 * 1024;

/// What the log says about one session's ability to be woken.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum WakeHealth {
    /// A wake hook has run for this session: the harness IS watching its sentinel.
    /// This is positive proof — the hook cannot run unless the watch exists.
    Verified {
        /// How many times the wake hook ran (all outcomes, not just exit-2 wakes).
        hook_runs: u64,
        /// How many of those actually woke the session (found genuine unread mail).
        wakes: u64,
    },
    /// The sentinel has been bumped, sometimes repeatedly, and no wake hook has ever
    /// run. The session is very likely unwakeable — but see the module docs: this is
    /// named for the evidence, not the conclusion.
    NoWakeObserved {
        /// Bumps that should each have triggered the hook.
        bumps: u64,
        /// How many of those were ADR-0012 turn-boundary re-triggers.
        retriggers: u64,
    },
    /// Nothing has ever bumped this session's sentinel, so the log says nothing
    /// either way. A brand-new session sits here until its first bump.
    Unproven,
}

impl WakeHealth {
    /// Whether this session is known to be wakeable. Only positive proof counts.
    pub fn is_verified(&self) -> bool {
        matches!(self, WakeHealth::Verified { .. })
    }

    /// Whether the evidence points at a session that cannot be woken — the rows a
    /// human wants pulled to the top, and what `--deaf-only` filters to.
    pub fn is_suspect(&self) -> bool {
        matches!(self, WakeHealth::NoWakeObserved { .. })
    }
}

/// Per-session wake health for the whole fleet, plus how much of the log it is based
/// on.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WakeSummary {
    by_session: HashMap<String, WakeHealth>,
    /// True if the log was longer than [`MAX_LOG_TAIL_BYTES`] and only its tail was
    /// read, so "never" claims are bounded by that window.
    pub truncated: bool,
}

impl WakeSummary {
    /// This session's health. A session the log has never mentioned is
    /// [`WakeHealth::Unproven`], not absent — the caller should never have to
    /// distinguish "no evidence" from "no entry".
    pub fn get(&self, session: &str) -> WakeHealth {
        self.by_session
            .get(session)
            .cloned()
            .unwrap_or(WakeHealth::Unproven)
    }

    /// How many sessions are proven wakeable, and how many look deaf. Sessions the
    /// log knows nothing about are in neither bucket.
    pub fn tally(&self) -> (usize, usize) {
        let verified = self.by_session.values().filter(|h| h.is_verified()).count();
        let suspect = self.by_session.values().filter(|h| h.is_suspect()).count();
        (verified, suspect)
    }

    /// Read the tail of the harness log and classify every session in it.
    ///
    /// A missing or unreadable log is an EMPTY summary, not an error: the dashboard
    /// must still render (every session simply shows as `Unproven`). A log we cannot
    /// read is a reason to say "no evidence", never a reason to fail.
    pub fn from_log(path: &std::path::Path) -> Self {
        Self::from_log_tail(path, MAX_LOG_TAIL_BYTES)
    }

    /// [`WakeSummary::from_log`] with an explicit tail size, so the truncation path can
    /// be exercised against a small file instead of fabricating an 8 MiB one.
    pub fn from_log_tail(path: &std::path::Path, limit: u64) -> Self {
        let Some((text, truncated)) = read_tail(path, limit) else {
            return Self::default();
        };
        let mut summary = Self::from_lines(text.lines());
        summary.truncated = truncated;
        summary
    }

    /// The pure classifier, over any iterator of log lines.
    ///
    /// Split from the file reading so the rules can be unit-tested against handwritten
    /// lines in microseconds, with no tempdir and no log to fabricate on disk.
    pub fn from_lines<'a>(lines: impl Iterator<Item = &'a str>) -> Self {
        let mut counts: HashMap<String, Counts> = HashMap::new();
        for line in lines {
            let Some(session) = session_of(line) else {
                continue;
            };
            let entry = counts.entry(session).or_default();
            if line.contains(WAKE_HOOK_MARKER) {
                entry.hook_runs += 1;
                // The hook logs on every branch; only this one is an actual wake.
                if line.contains("genuine unread mail") {
                    entry.wakes += 1;
                }
            } else if line.contains(RETRIGGER_MARKER) {
                entry.bumps += 1;
                entry.retriggers += 1;
            } else if line.contains(SENTINEL_BUMP_MARKER) {
                entry.bumps += 1;
            }
        }

        Self {
            by_session: counts
                .into_iter()
                .map(|(session, c)| (session, c.classify()))
                .collect(),
            truncated: false,
        }
    }
}

/// Running tallies for one session while scanning the log.
#[derive(Debug, Default)]
struct Counts {
    hook_runs: u64,
    wakes: u64,
    bumps: u64,
    retriggers: u64,
}

impl Counts {
    /// Positive proof first: a hook that ran outranks any number of bumps that did
    /// not produce one, because it settles the question the bumps only hint at.
    fn classify(self) -> WakeHealth {
        if self.hook_runs > 0 {
            WakeHealth::Verified {
                hook_runs: self.hook_runs,
                wakes: self.wakes,
            }
        } else if self.bumps > 0 {
            WakeHealth::NoWakeObserved {
                bumps: self.bumps,
                retriggers: self.retriggers,
            }
        } else {
            WakeHealth::Unproven
        }
    }
}

/// Pull the session id out of a log line, from either shape the harness emits:
/// `session=<id>` (the hook handlers in `cli`) or `session="<id>"` (the `wake`
/// module, which logs it as a quoted string).
///
/// Handling both here rather than at the call sites is the point: the two shapes are
/// an artifact of how each site happens to pass the field to `tracing`, and a parser
/// that silently understood only one would under-count exactly the sessions whose
/// evidence lives in the other — reporting a healthy session as deaf, or worse.
fn session_of(line: &str) -> Option<String> {
    let rest = line.split("session=").nth(1)?;
    let rest = rest.strip_prefix('"').unwrap_or(rest);
    let id: String = rest
        .chars()
        .take_while(|c| c.is_ascii_alphanumeric() || *c == '-')
        .collect();
    (!id.is_empty()).then_some(id)
}

/// Read at most `limit` bytes from the END of `path`, returning the text and whether
/// anything was skipped. `None` if the file cannot be read at all.
///
/// The first line of a truncated read is very likely cut mid-way, so it is dropped
/// rather than parsed — a half line cannot be classified correctly and could attach a
/// count to the wrong session.
fn read_tail(path: &std::path::Path, limit: u64) -> Option<(String, bool)> {
    use std::io::{Read, Seek, SeekFrom};

    let mut file = std::fs::File::open(path).ok()?;
    let len = file.metadata().ok()?.len();
    let truncated = len > limit;
    if truncated {
        file.seek(SeekFrom::Start(len - limit)).ok()?;
    }
    let mut buf = Vec::with_capacity(limit.min(len) as usize);
    file.read_to_end(&mut buf).ok()?;

    let text = String::from_utf8_lossy(&buf).into_owned();
    let text = if truncated {
        // Drop the partial first line.
        text.split_once('\n').map(|(_, rest)| rest.to_string())?
    } else {
        text
    };
    Some((text, truncated))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The two field shapes the harness actually emits. If either stops parsing, a
    /// whole class of evidence goes missing and healthy sessions read as deaf.
    #[test]
    fn a_session_id_is_read_from_both_quoted_and_bare_field_shapes() {
        assert_eq!(
            session_of("... wake: nothing unread session=abc-123 topics=x").as_deref(),
            Some("abc-123")
        );
        assert_eq!(
            session_of(r#"... wrote the wake sentinel session="abc-123" topics="t""#).as_deref(),
            Some("abc-123")
        );
        assert_eq!(session_of("a line with no session field"), None);
    }

    /// A wake hook that RAN is positive proof the harness is watching, even when that
    /// particular run found nothing to wake for (the common "stray sentinel" no-op).
    /// Classifying only exit-2 wakes as healthy would report a perfectly wakeable idle
    /// session as deaf.
    #[test]
    fn any_wake_hook_run_verifies_the_watch_even_when_it_did_not_wake() {
        let summary = WakeSummary::from_lines(
            ["INFO FileChanged wake: nothing unread (stray sentinel change) session=s1"]
                .into_iter(),
        );
        assert_eq!(
            summary.get("s1"),
            WakeHealth::Verified {
                hook_runs: 1,
                wakes: 0
            }
        );
        assert!(summary.get("s1").is_verified());
        assert!(!summary.get("s1").is_suspect());
    }

    /// The headline case: bumps with no hook run. This is the session that looks
    /// healthy everywhere else — registered inbox, live watcher, sentinel moving — and
    /// cannot be woken.
    #[test]
    fn bumps_with_no_hook_run_are_reported_as_no_wake_observed() {
        let summary = WakeSummary::from_lines(
            [
                r#"INFO wake: watcher wrote the wake sentinel session="s2" topics="t.a""#,
                r#"INFO wake: watcher wrote the wake sentinel session="s2" topics="t.a""#,
                "INFO cli: turn boundary: unread mail arrived while busy; re-bumped the wake sentinel session=s2 topics=t.a",
            ]
            .into_iter(),
        );
        assert_eq!(
            summary.get("s2"),
            WakeHealth::NoWakeObserved {
                bumps: 3,
                retriggers: 1
            }
        );
        assert!(summary.get("s2").is_suspect());
    }

    /// Proof outranks suspicion regardless of order: a session with many silent bumps
    /// and one hook run IS wakeable, and the bumps were lost edges (ADR-0012), not a
    /// missing watch. Asserting both orders keeps the rule from depending on log order.
    #[test]
    fn one_hook_run_outranks_any_number_of_silent_bumps_in_either_order() {
        let bump = r#"INFO wake: watcher wrote the wake sentinel session="s3" topics="t""#;
        let hook = "INFO FileChanged wake: genuine unread mail session=s3";

        for lines in [vec![bump, bump, hook], vec![hook, bump, bump]] {
            assert_eq!(
                WakeSummary::from_lines(lines.into_iter()).get("s3"),
                WakeHealth::Verified {
                    hook_runs: 1,
                    wakes: 1
                },
                "positive proof must win whichever side of the bumps it lands on"
            );
        }
    }

    /// A session the log has never mentioned must not be confused with one that has
    /// been bumped and stayed silent — that difference is the whole reason the type
    /// has three variants and not two.
    #[test]
    fn a_session_the_log_never_mentions_is_unproven_not_deaf() {
        let summary = WakeSummary::from_lines(std::iter::empty());
        assert_eq!(summary.get("never-seen"), WakeHealth::Unproven);
        assert!(!summary.get("never-seen").is_suspect());
        assert_eq!(summary.tally(), (0, 0));
    }

    /// Unrelated harness chatter (arming, registration, ensure-watcher) carries a
    /// session field too. It must not be mistaken for bump evidence, or every session
    /// would look deaf the moment it started.
    #[test]
    fn unrelated_harness_lines_are_not_evidence_either_way() {
        let summary = WakeSummary::from_lines(
            [
                "INFO cli: registered the session's agent inbox session=s4 topic=agent.s4",
                "INFO cli: ensure-watcher: a live watcher already holds the lock session=s4",
                "INFO cli: turn boundary: session is caught up; nothing to re-trigger session=s4",
            ]
            .into_iter(),
        );
        assert_eq!(summary.get("s4"), WakeHealth::Unproven);
    }

    #[test]
    fn a_tally_counts_verified_and_suspect_sessions_separately() {
        let summary = WakeSummary::from_lines(
            [
                "INFO FileChanged wake: genuine unread mail session=ok1",
                "INFO FileChanged wake: nothing unread session=ok2",
                r#"INFO wake: watcher wrote the wake sentinel session="deaf1" topics="t""#,
                r#"INFO wake: watcher wrote the wake sentinel session="deaf2" topics="t""#,
                "INFO cli: registered the session's agent inbox session=quiet",
            ]
            .into_iter(),
        );
        assert_eq!(summary.tally(), (2, 2));
        assert_eq!(summary.get("quiet"), WakeHealth::Unproven);
    }

    /// A missing log must render an empty dashboard, never fail one. Losing the
    /// evidence is a reason to say "unproven", not to take the view away.
    #[test]
    fn a_missing_log_is_an_empty_summary_not_an_error() {
        let summary = WakeSummary::from_log(std::path::Path::new("/nonexistent/harness.log"));
        assert_eq!(summary.tally(), (0, 0));
        assert!(!summary.truncated);
        assert_eq!(summary.get("anything"), WakeHealth::Unproven);
    }

    /// Reading only the tail must drop the partial first line rather than classify it,
    /// and must say that it truncated — a bounded read that claims to be complete is
    /// how a dashboard starts lying about "never".
    #[test]
    fn a_truncated_read_drops_the_partial_line_and_reports_itself() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("harness.log");

        // A long run of filler, then the evidence, so the tail read starts mid-filler.
        // Each filler line is a fixed width that does not divide the read limit, so the
        // cut is guaranteed to land inside one rather than on a boundary by luck.
        let mut text = String::new();
        for i in 0..2000 {
            text.push_str(&format!("INFO filler line {i:07} with no session field\n"));
        }
        text.push_str("INFO FileChanged wake: genuine unread mail session=tail\n");
        std::fs::write(&path, &text).unwrap();

        let (read, truncated) = read_tail(&path, 4096).unwrap();
        assert!(truncated, "a file past the limit must report truncation");
        // The invariant is that the kept text begins at a LINE BOUNDARY: every line is
        // whole, so none can be misparsed. A surviving partial line would be a suffix
        // of a filler line, and so would not start with the record prefix.
        assert!(
            read.lines().all(|l| l.starts_with("INFO ")),
            "every retained line must be whole; got first line {:?}",
            read.lines().next()
        );

        let summary = WakeSummary::from_log_tail(&path, 4096);
        assert!(
            summary.truncated,
            "a bounded read must report that it was bounded"
        );
        assert_eq!(
            summary.get("tail"),
            WakeHealth::Verified {
                hook_runs: 1,
                wakes: 1
            },
            "evidence in the tail must survive truncation"
        );
    }
}
