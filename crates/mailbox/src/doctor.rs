//! Active wakeability probing: prove a session can be woken, rather than infer it
//! (ADR-0016).
//!
//! # Why inference was not enough
//!
//! [`crate::dashboard::wake_health`] reads `harness.log` and asks "has a wake hook
//! ever run for this session?". That is real evidence, but it answers the wrong
//! question in two directions, both measured on a live fleet of 19 idle sessions:
//!
//! - **False alarm (6 of 9).** A session that has simply never needed waking looks
//!   exactly like one whose watch is dead. Six sessions the log called suspect
//!   answered a probe in under five seconds.
//! - **False reassurance (3 of 10).** A session that woke last week looks healthy
//!   today. Three sessions the log called verified were, when asked, deaf — so
//!   wakeability is **perishable**, not a fixed property of a session. Any agent can
//!   silently stop being reachable while its history still says it is fine.
//!
//! History cannot answer "can this agent be woken *now*", and for a fleet that hands
//! work between agents that is the only question that matters. So we ask directly.
//!
//! # The probe
//!
//! 1. Read the session's hook-ran stamp ([`Sentinel::hook_ran`]).
//! 2. Bump its sentinel **content-preservingly** ([`Sentinel::bump_in_place`]) — the
//!    same file change a real kick makes, with no effect on what the agent will be
//!    told is unread.
//! 3. Wait for the stamp to change. The `FileChanged` hook records it on every exit
//!    path, so a changed stamp is positive proof that Claude Code delivered the
//!    event; an unchanged one after the budget is the absence of that proof.
//!
//! Every session is bumped before any is polled, so the whole fleet is measured in
//! ONE window. That matters: it is what distinguishes "this session is deaf" from
//! "something global was wrong for ten seconds" — a distinction a session-at-a-time
//! loop cannot make, and the confound that cost an earlier investigation eleven
//! refuted theories.
//!
//! # Why a live-process check is part of the probe, not a nicety
//!
//! A session whose Claude Code process has exited also fails to answer, and the
//! mailbox side looks perfect for it: its watcher survives the process and keeps
//! bumping a sentinel nobody is watching. Reporting that as "deaf" is what made a
//! real bug look ten times bigger than it was. [`Reachability::Gone`] is therefore
//! a first-class verdict, and it is NOT a fault.

use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use mailbox_protocol::SessionId;

use crate::sentinel::{BumpOutcome, HookRunRecord, Sentinel, SentinelError, TurnState};

/// How long to wait for the wake hook to answer before calling a session deaf.
///
/// Measured: every session that answers at all answers in under five seconds
/// (13 of 13, in one window), while the deaf ones stayed silent for a full three
/// minutes. Ten seconds is comfortably past the observed answer time without making
/// a fleet sweep tedious, and [`Probe::budget`] can raise it on a loaded machine.
pub const DEFAULT_BUDGET: Duration = Duration::from_secs(10);

/// How often to re-read the hook-ran stamps while waiting.
const POLL_INTERVAL: Duration = Duration::from_millis(250);

/// What a probe learned about one session's ability to be woken *right now*.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reachability {
    /// The wake hook answered: Claude Code is watching this session's sentinel.
    /// Positive proof — the hook cannot run unless the watch exists.
    Wakeable {
        /// How long the answer took. Worth reporting: a session answering near the
        /// budget is healthy but worth a second look.
        took: Duration,
    },
    /// A live Claude Code process, a sentinel that was bumped, and no answer within
    /// the budget — while the session was **idle**, so the silence is meaningful.
    /// **This is the fault**: mail for this agent will not reach it.
    Deaf,
    /// The session was mid-turn, so it could not have run the hook and its silence
    /// proves nothing. NOT a fault: a busy session picks its mail up at the turn
    /// boundary (ADR-0012). Re-probe once it is idle to learn anything about it.
    Busy,
    /// No live Claude Code process owns this session, so there is nothing to wake.
    /// Expected and harmless — sessions outlive their processes because `SessionEnd`
    /// does not run when a terminal is closed or a process is killed.
    Gone,
    /// The session has a live process but no sentinel has ever been written, so
    /// there is no watched file to bump and nothing to prove yet.
    NeverArmed,
    /// The probe could not be carried out (the sentinel could not be read or
    /// rewritten). Reported rather than silently folded into `Deaf`: "I could not
    /// measure" and "I measured a failure" are different facts.
    Undetermined {
        /// What went wrong, for the operator to act on.
        reason: String,
    },
}

impl Reachability {
    /// Whether this verdict is a fault an operator must act on. Only [`Self::Deaf`]
    /// is: a session with no process is expected, and an unmeasurable one is a
    /// separate (louder) problem but not evidence of a broken watch.
    pub fn is_fault(&self) -> bool {
        matches!(self, Self::Deaf)
    }

    /// A short, stable label for machine-readable output.
    pub fn label(&self) -> &'static str {
        match self {
            Self::Wakeable { .. } => "wakeable",
            Self::Deaf => "deaf",
            Self::Busy => "busy",
            Self::Gone => "gone",
            Self::NeverArmed => "never_armed",
            Self::Undetermined { .. } => "undetermined",
        }
    }
}

/// One session's probe result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionReport {
    /// Which session this is about.
    pub session: SessionId,
    /// What the probe found.
    pub reachability: Reachability,
}

/// The whole fleet's probe result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FleetReport {
    /// Per-session results, in the order probed.
    pub sessions: Vec<SessionReport>,
    /// The budget the probe allowed each session to answer within.
    pub budget: Duration,
}

impl FleetReport {
    /// How many sessions are a genuine fault (live, armed, and not answering).
    pub fn deaf(&self) -> usize {
        self.sessions
            .iter()
            .filter(|r| r.reachability.is_fault())
            .count()
    }

    /// How many sessions answered.
    pub fn wakeable(&self) -> usize {
        self.sessions
            .iter()
            .filter(|r| matches!(r.reachability, Reachability::Wakeable { .. }))
            .count()
    }

    /// Whether this result is better explained by an out-of-date installed binary
    /// than by a genuinely deaf fleet.
    ///
    /// The ack is written by whichever `mailbox` binary the `FileChanged` hook
    /// actually invokes. If that binary predates the ack, every live session answers
    /// nothing and the probe reports a total blackout — which is alarming, wrong, and
    /// exactly the kind of confident-but-false health signal this command exists to
    /// replace. A fleet where NOTHING answers is far more likely to be a stale
    /// install than every agent breaking at once, so we say so rather than let the
    /// operator draw the terrifying conclusion.
    ///
    /// It takes a MINIMUM SAMPLE to say that. "Nothing answered" is only surprising
    /// when enough sessions were asked; on a single-session probe it is trivially
    /// true of any genuine fault, and the first version of this fired on exactly
    /// that — telling an operator to go check a correct install while looking
    /// straight at a real deaf agent. Blaming the tooling for a true positive is the
    /// same class of confident-but-false signal as the one this command replaces, so
    /// the hint stays silent unless the blackout is fleet-shaped.
    pub fn looks_like_a_stale_install(&self) -> bool {
        /// Below this many silent sessions, silence is a fault report, not evidence
        /// about the install.
        const MIN_SAMPLE: usize = 3;
        self.wakeable() == 0 && self.deaf() >= MIN_SAMPLE
    }
}

/// A configured probe.
pub struct Probe {
    /// How long a session has to answer before it is called deaf.
    pub budget: Duration,
}

impl Default for Probe {
    fn default() -> Self {
        Self {
            budget: DEFAULT_BUDGET,
        }
    }
}

/// One session mid-probe: what we knew before the bump, and whether we bumped.
struct InFlight {
    session: SessionId,
    sentinel: Sentinel,
    before: HookRunRecord,
    /// `Some` once the session has reached a verdict without needing to be polled
    /// (no process, never armed, or the bump itself failed).
    settled: Option<Reachability>,
}

impl Probe {
    /// Probe every session in `sessions`, bumping them all before polling any, so the
    /// whole fleet is measured in one window.
    ///
    /// `live` is the set of session ids that currently have a Claude Code process;
    /// see [`live_claude_sessions`]. Passing it in (rather than scanning inside)
    /// keeps this function testable and lets a caller probe a fleet it has already
    /// enumerated.
    pub fn run(&self, sessions: &[SessionId], live: &BTreeSet<String>) -> FleetReport {
        self.probe_all(sessions, live, Sentinel::for_session)
    }

    /// [`Self::run`] against an explicit sentinel root.
    ///
    /// The env-reading resolution in [`Sentinel::for_session`] is process-global, so
    /// a test that wanted to exercise the probe would have to mutate the environment
    /// out from under every other test. This mirrors [`Sentinel::under_root`]: the
    /// path scheme is the same, the source of the root is the caller's business.
    pub fn run_under_root(
        &self,
        root: &std::path::Path,
        sessions: &[SessionId],
        live: &BTreeSet<String>,
    ) -> FleetReport {
        self.probe_all(sessions, live, |session| {
            Ok(Sentinel::under_root(root, session))
        })
    }

    fn probe_all(
        &self,
        sessions: &[SessionId],
        live: &BTreeSet<String>,
        resolve: impl Fn(&SessionId) -> Result<Sentinel, SentinelError>,
    ) -> FleetReport {
        let mut in_flight: Vec<InFlight> = sessions
            .iter()
            .map(|session| self.begin(session.clone(), live, &resolve))
            .collect();

        let deadline = Instant::now() + self.budget;
        let started = Instant::now();
        loop {
            let mut pending = false;
            for entry in in_flight.iter_mut() {
                if entry.settled.is_some() {
                    continue;
                }
                if answered(entry.before, entry.sentinel.hook_ran()) {
                    entry.settled = Some(Reachability::Wakeable {
                        took: started.elapsed(),
                    });
                } else {
                    pending = true;
                }
            }
            if !pending || Instant::now() >= deadline {
                break;
            }
            std::thread::sleep(POLL_INTERVAL);
        }

        FleetReport {
            sessions: in_flight
                .into_iter()
                .map(|entry| {
                    // Anything still unsettled had a live process, a real sentinel, and
                    // a bump — and did not answer. That is only a fault if the session
                    // was IDLE and could therefore have answered; a session executing a
                    // turn is silent for an entirely ordinary reason, and calling that
                    // deaf libels every busy agent in the fleet.
                    let unanswered = match entry.sentinel.turn_state() {
                        TurnState::Busy => Reachability::Busy,
                        TurnState::Idle => Reachability::Deaf,
                    };
                    SessionReport {
                        session: entry.session,
                        reachability: entry.settled.unwrap_or(unanswered),
                    }
                })
                .collect(),
            budget: self.budget,
        }
    }

    /// Resolve one session's sentinel, decide whether it is even probeable, and (if
    /// it is) bump it.
    fn begin(
        &self,
        session: SessionId,
        live: &BTreeSet<String>,
        resolve: &impl Fn(&SessionId) -> Result<Sentinel, SentinelError>,
    ) -> InFlight {
        let sentinel = match resolve(&session) {
            Ok(sentinel) => sentinel,
            Err(err) => return settled_without_sentinel(session, err),
        };
        // A session with no process cannot answer, and saying "deaf" about it is the
        // misreading this verdict exists to prevent. Check before bumping: there is
        // no point changing a file nobody is watching.
        if !live.contains(session.as_str()) {
            return InFlight {
                session,
                sentinel,
                before: HookRunRecord::Never,
                settled: Some(Reachability::Gone),
            };
        }
        let before = sentinel.hook_ran();
        let settled = match sentinel.bump_in_place() {
            Ok(BumpOutcome::Bumped) => None,
            Ok(BumpOutcome::NothingToBump) => Some(Reachability::NeverArmed),
            Err(err) => Some(Reachability::Undetermined {
                reason: err.to_string(),
            }),
        };
        InFlight {
            session,
            sentinel,
            before,
            settled,
        }
    }
}

/// A session whose sentinel path could not even be resolved: report it, do not
/// pretend it was measured.
fn settled_without_sentinel(session: SessionId, err: SentinelError) -> InFlight {
    // `Sentinel::under_root` on a path we know is unusable would be a lie; but
    // `InFlight` needs one, so resolve against a path that cannot exist and mark the
    // entry settled so it is never touched again.
    let sentinel = Sentinel::under_root(std::path::Path::new("/nonexistent"), &session);
    InFlight {
        session,
        sentinel,
        before: HookRunRecord::Never,
        settled: Some(Reachability::Undetermined {
            reason: err.to_string(),
        }),
    }
}

/// Whether the hook-ran stamp changed in a way that proves a FRESH hook run.
///
/// Only a `Never -> At` transition or a differing `At` counts. An `Unreadable`
/// record proves nothing in either direction, and a record that is unreadable both
/// before and after must NOT be read as an answer — that would turn a broken
/// bookkeeping file into a clean bill of health.
fn answered(before: HookRunRecord, after: HookRunRecord) -> bool {
    match (before, after) {
        (_, HookRunRecord::Never | HookRunRecord::Unreadable) => false,
        (HookRunRecord::Never | HookRunRecord::Unreadable, HookRunRecord::At(_)) => true,
        (HookRunRecord::At(before), HookRunRecord::At(after)) => before != after,
    }
}

/// The session ids that currently have a live Claude Code process, read from the
/// process table.
///
/// Claude Code puts the session id in its own argv (`--session-id <uuid>` when a
/// session is created, `--resume <uuid>` when one is reopened), which makes the
/// process table the one place where "is this agent still running?" can be answered
/// from outside. Nothing in the mailbox's own state can answer it: a session's
/// watcher, subscriptions, and sentinel all outlive the process they belong to.
///
/// Best-effort by design. If `ps` cannot be run we return `None` and the caller
/// degrades to not distinguishing [`Reachability::Gone`] — better than inventing a
/// liveness answer.
pub fn live_claude_sessions() -> Option<BTreeSet<String>> {
    let output = std::process::Command::new("ps")
        .args(["ax", "-o", "args="])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    Some(parse_live_sessions(&String::from_utf8_lossy(
        &output.stdout,
    )))
}

/// The pure half of [`live_claude_sessions`]: extract session ids from `ps` output.
///
/// Split out so the parsing rules are unit-testable without a process table, and so
/// the awkward cases (the mailbox's own `--session` flag, a session id inside a
/// prompt) are pinned by tests rather than by hope.
pub fn parse_live_sessions(ps_output: &str) -> BTreeSet<String> {
    let mut found = BTreeSet::new();
    for line in ps_output.lines() {
        // Only Claude Code's own argv counts. `mailbox harness watch --session <id>`
        // also carries a session id, and counting it would make every orphaned
        // watcher look like a live agent — precisely the false liveness signal this
        // function exists to replace.
        if !is_claude_command(line) {
            continue;
        }
        for flag in ["--session-id", "--resume"] {
            if let Some(id) = flag_value(line, flag) {
                found.insert(id);
            }
        }
    }
    found
}

/// Whether this `ps` line is a Claude Code process rather than something else that
/// merely mentions a session id.
fn is_claude_command(line: &str) -> bool {
    let program = line.split_whitespace().next().unwrap_or_default();
    let name = program.rsplit('/').next().unwrap_or_default();
    // The wrapper scripts exec the real binary, so both appear; either is proof the
    // session is running. `mailbox` never matches, which is the point.
    name == "claude" || (name == "node" && line.contains("/claude"))
}

/// The value following `flag` in a command line, if it looks like a session id.
///
/// Requires the value to be a plausible id (hex, dashes) so that a `--resume` inside
/// a quoted prompt cannot inject an arbitrary token into the live set.
fn flag_value(line: &str, flag: &str) -> Option<String> {
    let mut parts = line.split_whitespace();
    while let Some(part) = parts.next() {
        if part != flag {
            continue;
        }
        let value = parts.next()?;
        if looks_like_session_id(value) {
            return Some(value.to_string());
        }
    }
    None
}

/// Whether a token is shaped like a session id: non-empty, and only the characters
/// a UUID uses. Deliberately not a strict UUID parse — the id is opaque to us and a
/// future format change must not silently empty the live set.
fn looks_like_session_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() >= 8
        && value
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}

/// Every session that has a per-session sentinel directory under `root`.
///
/// The sentinel root is the one place that knows about sessions without needing the
/// daemon, which keeps `doctor` working when the bridge is down — the same reasoning
/// that made the dashboard a read-only store reader (ADR-0015).
///
/// Directory names are filename-ENCODED session ids. Rather than write a decoder
/// (a second encoding rule that could drift from the first), we accept only names
/// that round-trip through the existing encoder. A name that does not is not a
/// session we could address anyway.
pub fn sessions_with_sentinels(root: &std::path::Path) -> Vec<SessionId> {
    let by_agent = root.join("by-agent");
    let Ok(entries) = std::fs::read_dir(&by_agent) else {
        return Vec::new();
    };
    let mut sessions: Vec<SessionId> = entries
        .flatten()
        .filter(|entry| entry.path().is_dir())
        .filter_map(|entry| entry.file_name().into_string().ok())
        .filter_map(|name| {
            let session = SessionId::new(&name);
            (session.encode_filename() == name).then_some(session)
        })
        .collect();
    sessions.sort_by(|a, b| a.as_str().cmp(b.as_str()));
    sessions
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sentinel::HookRun;

    const CLAUDE: &str = "/Users/x/.local/bin/claude";

    #[test]
    fn a_fresh_stamp_counts_as_an_answer_and_an_unchanged_one_does_not() {
        let a = HookRun::for_test(1);
        let b = HookRun::for_test(2);
        assert!(answered(HookRunRecord::Never, HookRunRecord::At(a)));
        assert!(answered(HookRunRecord::At(a), HookRunRecord::At(b)));
        assert!(!answered(HookRunRecord::At(a), HookRunRecord::At(a)));
        assert!(!answered(HookRunRecord::At(a), HookRunRecord::Never));
    }

    /// A record we cannot read proves nothing. Treating "unreadable before,
    /// unreadable after" as an answer would report a broken bookkeeping file as a
    /// healthy session — the exact silent-success failure this module exists to end.
    #[test]
    fn an_unreadable_record_is_never_an_answer() {
        assert!(!answered(
            HookRunRecord::Unreadable,
            HookRunRecord::Unreadable
        ));
        assert!(!answered(HookRunRecord::Never, HookRunRecord::Unreadable));
        assert!(!answered(
            HookRunRecord::At(HookRun::for_test(1)),
            HookRunRecord::Unreadable
        ));
        // But an unreadable record replaced by a real stamp IS a fresh run.
        assert!(answered(
            HookRunRecord::Unreadable,
            HookRunRecord::At(HookRun::for_test(1))
        ));
    }

    #[test]
    fn live_sessions_come_from_claude_argv_in_both_launch_forms() {
        let ps = format!(
            "{CLAUDE} --dangerously-skip-permissions --session-id 8ce450e7-ca56-447d-8208-a58d53ac2d6e\n\
             {CLAUDE} --resume 16d3f539-1bed-46f5-8bc4-41b22cdd8e0c\n"
        );
        let live = parse_live_sessions(&ps);
        assert!(live.contains("8ce450e7-ca56-447d-8208-a58d53ac2d6e"));
        assert!(live.contains("16d3f539-1bed-46f5-8bc4-41b22cdd8e0c"));
        assert_eq!(live.len(), 2);
    }

    /// The bug this whole module is a response to: an orphaned watcher outlives its
    /// agent and carries the same session id on its command line. Counting it would
    /// reinstate exactly the false liveness signal we are replacing.
    #[test]
    fn a_mailbox_watcher_is_not_a_live_agent() {
        let ps = "/Users/x/.local/bin/mailbox harness watch --session \
                  38a248a5-5c0b-40e2-ba92-7011cd181140\n";
        assert!(parse_live_sessions(ps).is_empty());
    }

    #[test]
    fn a_session_id_mentioned_in_a_prompt_is_not_a_live_agent() {
        let ps = "/bin/zsh -c echo please --resume that-work-later\n";
        assert!(parse_live_sessions(ps).is_empty());
    }

    #[test]
    fn a_flag_with_no_value_does_not_panic_or_invent_a_session() {
        assert!(parse_live_sessions(&format!("{CLAUDE} --resume\n")).is_empty());
    }

    #[test]
    fn only_round_trippable_directory_names_are_treated_as_sessions() {
        let dir = tempfile::TempDir::new().unwrap();
        let by_agent = dir.path().join("by-agent");
        std::fs::create_dir_all(by_agent.join("8ce450e7-ca56-447d-8208-a58d53ac2d6e")).unwrap();
        // An encoded name does NOT round-trip (`%42` re-encodes to `%2542`), so it is
        // skipped rather than decoded by a second, drift-prone rule.
        std::fs::create_dir_all(by_agent.join("a%42")).unwrap();
        std::fs::write(by_agent.join("not-a-dir"), "x").unwrap();

        let sessions = sessions_with_sentinels(dir.path());
        assert_eq!(
            sessions.iter().map(SessionId::as_str).collect::<Vec<_>>(),
            vec!["8ce450e7-ca56-447d-8208-a58d53ac2d6e"],
            "only the round-trippable directory is a session; the encoded name and \
             the plain file are skipped"
        );
    }

    #[test]
    fn a_missing_sentinel_root_yields_no_sessions_rather_than_an_error() {
        assert!(sessions_with_sentinels(std::path::Path::new("/nonexistent")).is_empty());
    }

    /// Arm a session the way a watcher would: a per-session dir with a sentinel.
    fn arm(root: &std::path::Path, session: &SessionId) -> Sentinel {
        let sentinel = Sentinel::under_root(root, session);
        sentinel
            .write_topics(&[mailbox_protocol::Topic::parse("agent.x").unwrap()])
            .unwrap();
        sentinel
    }

    /// The whole point, end to end: a session whose hook answers is `Wakeable`, and
    /// one whose hook stays silent is `Deaf` — measured in the SAME window, which is
    /// what makes the difference attributable to the session rather than to the
    /// moment.
    #[test]
    fn a_session_whose_hook_answers_is_wakeable_and_a_silent_one_is_deaf() {
        let dir = tempfile::TempDir::new().unwrap();
        let answering = SessionId::new("answers-1234");
        let silent = SessionId::new("silent-1234");
        let answering_sentinel = arm(dir.path(), &answering);
        arm(dir.path(), &silent);

        // Stand in for Claude Code delivering the FileChanged event to one of them.
        let hook = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(50));
            answering_sentinel
                .record_hook_ran(std::time::SystemTime::now())
                .unwrap();
        });

        let live = BTreeSet::from([answering.as_str().to_string(), silent.as_str().to_string()]);
        let probe = Probe {
            budget: Duration::from_secs(2),
        };
        let report = probe.run_under_root(dir.path(), &[answering, silent], &live);
        hook.join().unwrap();

        assert!(matches!(
            report.sessions[0].reachability,
            Reachability::Wakeable { .. }
        ));
        assert_eq!(report.sessions[1].reachability, Reachability::Deaf);
        assert_eq!(report.deaf(), 1);
        assert_eq!(report.wakeable(), 1);
    }

    /// The misreading that made a real bug look ten times bigger than it was: a
    /// session with no live process is silent for a completely mundane reason and
    /// must never be reported as a fault.
    #[test]
    fn a_session_with_no_live_process_is_gone_not_deaf() {
        let dir = tempfile::TempDir::new().unwrap();
        let session = SessionId::new("departed-1234");
        arm(dir.path(), &session);

        let report = Probe {
            budget: Duration::from_millis(200),
        }
        .run_under_root(dir.path(), &[session], &BTreeSet::new());

        assert_eq!(report.sessions[0].reachability, Reachability::Gone);
        assert_eq!(report.deaf(), 0, "a departed session is not a fault");
    }

    /// A probe must not be able to change what the agent will be told is unread —
    /// otherwise checking the fleet's health could alter it.
    #[test]
    fn probing_bumps_the_sentinel_without_changing_what_it_says() {
        let dir = tempfile::TempDir::new().unwrap();
        let session = SessionId::new("preserved-1234");
        let sentinel = arm(dir.path(), &session);
        let before = sentinel.read_topics();

        Probe {
            budget: Duration::from_millis(200),
        }
        .run_under_root(
            dir.path(),
            std::slice::from_ref(&session),
            &BTreeSet::from([session.as_str().to_string()]),
        );

        assert_eq!(
            sentinel.read_topics(),
            before,
            "the probe must preserve the topic set exactly"
        );
    }

    /// A live session that has never been armed has no watched file to bump, so
    /// there is nothing to prove either way. Reporting it as deaf would be a lie.
    #[test]
    fn a_live_session_with_no_sentinel_is_never_armed_not_deaf() {
        let dir = tempfile::TempDir::new().unwrap();
        let session = SessionId::new("unarmed-1234");

        let report = Probe {
            budget: Duration::from_millis(200),
        }
        .run_under_root(
            dir.path(),
            std::slice::from_ref(&session),
            &BTreeSet::from([session.as_str().to_string()]),
        );

        assert_eq!(report.sessions[0].reachability, Reachability::NeverArmed);
        assert_eq!(report.deaf(), 0);
    }

    /// The correction that motivated `Busy` existing at all: a session executing a
    /// turn cannot run its `FileChanged` hook, so it is silent for a wholly ordinary
    /// reason. The first version of this probe called that deaf and libelled four
    /// busy agents — including the one running the probe.
    #[test]
    fn a_session_mid_turn_is_busy_not_deaf() {
        let dir = tempfile::TempDir::new().unwrap();
        let session = SessionId::new("mid-turn-1234");
        let sentinel = arm(dir.path(), &session);

        // A turn opened and has not closed: exactly what UserPromptSubmit + Stop record.
        let t0 = std::time::UNIX_EPOCH + Duration::from_secs(1000);
        sentinel.record_turn_ended(t0).unwrap();
        sentinel
            .record_turn_started(t0 + Duration::from_secs(1))
            .unwrap();

        let report = Probe {
            budget: Duration::from_millis(200),
        }
        .run_under_root(
            dir.path(),
            std::slice::from_ref(&session),
            &BTreeSet::from([session.as_str().to_string()]),
        );

        assert_eq!(report.sessions[0].reachability, Reachability::Busy);
        assert_eq!(
            report.deaf(),
            0,
            "a busy session is not a fault: it collects its mail at the turn boundary"
        );
    }

    /// The other half of the pair: once the turn has closed, silence means something
    /// again. Without this, adding `Busy` would simply have hidden the real fault.
    #[test]
    fn a_session_whose_turn_has_ended_is_deaf_again_when_silent() {
        let dir = tempfile::TempDir::new().unwrap();
        let session = SessionId::new("turn-done-1234");
        let sentinel = arm(dir.path(), &session);

        let t0 = std::time::UNIX_EPOCH + Duration::from_secs(1000);
        sentinel.record_turn_started(t0).unwrap();
        sentinel
            .record_turn_ended(t0 + Duration::from_secs(1))
            .unwrap();

        let report = Probe {
            budget: Duration::from_millis(200),
        }
        .run_under_root(
            dir.path(),
            std::slice::from_ref(&session),
            &BTreeSet::from([session.as_str().to_string()]),
        );

        assert_eq!(report.sessions[0].reachability, Reachability::Deaf);
        assert_eq!(report.deaf(), 1);
    }

    /// A session that has never taken a turn is sitting at the prompt — idle, and
    /// genuinely probeable. Treating "no stamps" as busy would make every fresh
    /// session permanently unmeasurable.
    #[test]
    fn a_session_that_has_never_taken_a_turn_is_treated_as_idle() {
        let dir = tempfile::TempDir::new().unwrap();
        let session = SessionId::new("never-prompted-1234");
        arm(dir.path(), &session);

        let report = Probe {
            budget: Duration::from_millis(200),
        }
        .run_under_root(
            dir.path(),
            std::slice::from_ref(&session),
            &BTreeSet::from([session.as_str().to_string()]),
        );

        assert_eq!(report.sessions[0].reachability, Reachability::Deaf);
    }

    /// A stale stamp from an earlier wake must not be mistaken for an answer to THIS
    /// probe — that is precisely the false reassurance the log-based signal gave.
    #[test]
    fn an_old_hook_stamp_does_not_count_as_answering_this_probe() {
        let dir = tempfile::TempDir::new().unwrap();
        let session = SessionId::new("stale-1234");
        let sentinel = arm(dir.path(), &session);
        sentinel
            .record_hook_ran(std::time::SystemTime::now())
            .unwrap();

        let report = Probe {
            budget: Duration::from_millis(200),
        }
        .run_under_root(
            dir.path(),
            std::slice::from_ref(&session),
            &BTreeSet::from([session.as_str().to_string()]),
        );

        assert_eq!(
            report.sessions[0].reachability,
            Reachability::Deaf,
            "history of having woken once is not evidence of being wakeable now"
        );
    }

    fn report(states: Vec<Reachability>) -> FleetReport {
        FleetReport {
            sessions: states
                .into_iter()
                .enumerate()
                .map(|(i, reachability)| SessionReport {
                    session: SessionId::new(&format!("s{i}-abcdefgh")),
                    reachability,
                })
                .collect(),
            budget: Duration::from_secs(1),
        }
    }

    /// Probing ONE session and finding it deaf is a fault report, not evidence that
    /// the install is stale. The first version told the operator to go check a
    /// perfectly current install while staring at a real deaf agent.
    #[test]
    fn a_single_deaf_session_is_not_blamed_on_the_install() {
        assert!(!report(vec![Reachability::Deaf]).looks_like_a_stale_install());
        assert!(!report(vec![Reachability::Deaf, Reachability::Deaf]).looks_like_a_stale_install());
    }

    /// A fleet-shaped blackout still is.
    #[test]
    fn a_fleet_wide_blackout_still_points_at_the_install() {
        assert!(
            report(vec![
                Reachability::Deaf,
                Reachability::Deaf,
                Reachability::Deaf
            ])
            .looks_like_a_stale_install()
        );
    }

    /// One session answering proves the ack mechanism works, so the install is
    /// exonerated no matter how many others are deaf.
    #[test]
    fn one_answer_exonerates_the_install() {
        assert!(
            !report(vec![
                Reachability::Deaf,
                Reachability::Deaf,
                Reachability::Deaf,
                Reachability::Wakeable {
                    took: Duration::ZERO
                },
            ])
            .looks_like_a_stale_install()
        );
    }

    #[test]
    fn only_deaf_is_reported_as_a_fault() {
        assert!(Reachability::Deaf.is_fault());
        assert!(!Reachability::Gone.is_fault());
        assert!(!Reachability::NeverArmed.is_fault());
        assert!(
            !Reachability::Wakeable {
                took: Duration::ZERO
            }
            .is_fault()
        );
        assert!(!Reachability::Undetermined { reason: "x".into() }.is_fault());
    }
}
