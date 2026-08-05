//! The wake path: telling an idle Claude Code session that mail has arrived.
//!
//! # The whole mechanism, in one line
//!
//! `publish` → the `serve` daemon writes the subscriber's sentinel file → Claude
//! Code's `FileChanged` hook fires → `mailbox harness wake` exits 2 → the idle
//! session takes a turn.
//!
//! That is two live components (the daemon and Claude Code) and one file. It used
//! to be five: a per-session FIFO, a detached watcher process blocked on it, a
//! single-waiter advisory lock, a waiter pidfile, and the sentinel. The watcher
//! existed only to turn a kick into a file write — work the daemon can do itself,
//! in the same process that just committed the event — and every one of those
//! pieces could fail silently, leaving the agent deaf with nothing reporting it
//! (ADR-0017).
//!
//! # What crosses the boundary (and what does not)
//!
//! Wake is **payload-free** (ADR-0001, docs/01-wake-and-rearm.md): the sentinel
//! carries topic NAMES only, and the woken hook reports at most those names on its
//! stderr. The event body never crosses this boundary — it stays in the durable log
//! and is read later by the agent's `read`. Wake is ingress, not authority.
//!
//! # Two sides, two types
//!
//! - [`Waker`] is the DAEMON side: given a session and its currently-unread topics,
//!   write them into that session's sentinel. Held by the [`crate::bus`], used on
//!   every publish.
//! - [`SessionMail`] is the HOOK side: a read-only view of one session's unread
//!   mail, used by the `FileChanged` wake hook to decide whether to wake (exit 2),
//!   by `SessionStart` to arm the sentinel, and by the `Stop` hook for the ADR-0012
//!   turn-boundary re-trigger.
//!
//! They never talk to each other; the durable store and the sentinel file are the
//! only things between them.

use std::path::PathBuf;

use tracing::{info, warn};

use mailbox_protocol::Topic;

use crate::sentinel::{RetriggerRecord, Sentinel, SentinelError};
use crate::storage::{ReadOnlyStore, SessionId, StorageError, Unread, WakeWatermark};

/// The process exit code that asks the Claude Code harness to wake the idle session
/// (the `asyncRewake` contract, docs/01-wake-and-rearm.md). A `u8` so the sole
/// consumer maps it to a `std::process::ExitCode` without a lossy cast.
pub const WAKE_EXIT_CODE: u8 = 2;

/// The payload-free stderr reminder the `FileChanged` wake hook writes on exit 2.
///
/// Topic NAMES only — never a body — so it is safe to surface verbatim as a system
/// reminder. It is a function rather than a format string at the call site so there
/// is ONE definition of the wake wire's content, and one place for the test that
/// pins it payload-free.
pub fn reminder(topics: &[Topic]) -> String {
    let names: Vec<&str> = topics.iter().map(Topic::as_str).collect();
    format!("mail on topic {}", names.join(", "))
}

/// Something went wrong reading a session's unread mail.
///
/// Only the READ side has an error type: writing a sentinel yields a
/// [`SentinelError`], which every caller reports rather than propagates — a hook
/// that failed loudly, or slowly, would cost a model turn on every session.
#[derive(Debug, thiserror::Error)]
pub enum WakeError {
    /// The read-only unread check against the durable store failed.
    #[error(transparent)]
    Storage(#[from] StorageError),
}

/// How one [`SessionMail::retrigger_if_unread`] ended (ADR-0012).
///
/// Returned rather than logged in place so the `Stop` hook can report it in one
/// exhaustive `match` — and so a test can assert the ANTI-LOOP branch directly
/// instead of inferring it from a file mtime. Every variant carries what a reader
/// needs to explain the decision.
#[derive(Debug)]
pub enum RetriggerOutcome {
    /// Nothing unread: the ordinary quiet turn. The sentinel was not touched.
    CaughtUp,
    /// This mail already earned its one nudge — the anti-loop bound holding.
    AlreadyRetriggered {
        last: RetriggerRecord,
        high_water: WakeWatermark,
    },
    /// The sentinel was re-bumped and the watermark recorded: a `FileChanged` will
    /// fire against the now-idle session.
    Retriggered {
        topics: Vec<Topic>,
        high_water: WakeWatermark,
    },
    /// The sentinel was re-bumped but the watermark could not be recorded. The wake
    /// still fires (the bump landed); a later turn may simply nudge again.
    RetriggeredUnrecorded {
        topics: Vec<Topic>,
        high_water: WakeWatermark,
        error: SentinelError,
    },
    /// The sentinel could not be written, so no wake will fire from this turn. The
    /// mail stays durable and surfaces on the next publish.
    BumpFailed { error: SentinelError },
}

/// Whether mail at `high_water` has earned a turn-boundary nudge, given what the
/// re-trigger record says.
///
/// The whole anti-loop rule, isolated as a pure function so its boundaries can be
/// unit-tested in microseconds rather than inferred from file mtimes in an
/// integration test. A missing OR unreadable record re-triggers: that is the
/// fail-safe direction — a redundant wake, never a swallowed one.
pub fn needs_retrigger(record: RetriggerRecord, high_water: WakeWatermark) -> bool {
    match record {
        RetriggerRecord::Missing | RetriggerRecord::Unreadable => true,
        // Strictly newer only. Equal means "this exact mail already earned its nudge",
        // which is what stops an agent that never reads from being nudged forever.
        RetriggerRecord::At(last) => last < high_water,
    }
}

/// The DAEMON side of wake: writes a session's unread topic set into its sentinel,
/// which is what makes that session's `FileChanged` hook fire.
///
/// Cheap to clone; it holds only the sentinel root. It never opens the database —
/// it is handed the sessions to wake and what they have unread.
#[derive(Debug, Clone)]
pub struct Waker {
    sentinel_root: PathBuf,
}

impl Waker {
    /// Build a waker over the resolved sentinel root (`~/.mailbox` by default; see
    /// [`crate::sentinel`]). The root is resolved ONCE, by the daemon at startup,
    /// rather than re-read from the environment per publish.
    pub fn new(sentinel_root: impl Into<PathBuf>) -> Self {
        Self {
            sentinel_root: sentinel_root.into(),
        }
    }

    /// Write `unread` into `session`'s sentinel, bumping its mtime so the session's
    /// `FileChanged` hook fires.
    ///
    /// **Unconditional**: there is deliberately no coalescing (ADR-0008, revised).
    /// We do not compare the topic set to what the sentinel already holds, so every
    /// publish to a subscribed topic advances the mtime and no message can be
    /// silently dropped. The three lost-wake bugs this design retired all lived in
    /// the coalescing logic; there is now none that CAN be wrong.
    ///
    /// It creates the per-session directory if needed. That is exactly what the
    /// detached watcher did, and it means a session that subscribed without ever
    /// running `SessionStart` still gets a sentinel — one Claude Code is not
    /// watching, but which `mailbox doctor` can then see and report on.
    pub fn wake(&self, session: &SessionId, unread: &[Topic]) -> Result<(), SentinelError> {
        Sentinel::under_root(&self.sentinel_root, session).write_topics(unread)
    }

    /// Wake every session in `unread_by_session`, then log the aggregate outcome.
    ///
    /// Payload-free: `topic` is used only for the log line. With zero sessions this
    /// logs `bumped=0` — an honest "nothing to wake", not a misleading claim.
    ///
    /// A failure to write one session's sentinel is logged and skipped: the event is
    /// already durable, so it must never fail a publish, and the other subscribers
    /// still get their wake.
    pub fn wake_all(&self, unread_by_session: &[(SessionId, Vec<Topic>)], topic: &Topic) {
        let mut bumped = 0usize;
        let mut failed = 0usize;
        for (session, unread) in unread_by_session {
            match self.wake(session, unread) {
                Ok(()) => bumped += 1,
                Err(err) => {
                    failed += 1;
                    warn!(
                        session = session.as_str(),
                        error = %err,
                        "could not write a subscriber's wake sentinel; it will not wake for \
                         this event (the event is durable and surfaces on its next read)"
                    );
                }
            }
        }
        // Log after the decision point: which topic drove the writes, and how many
        // subscribers we actually bumped. Never the body.
        info!(
            topic = topic.as_str(),
            bumped, failed, "bumped subscribers' wake sentinels after publish"
        );
    }
}

/// The HOOK side of wake: a read-only view of one session's unread mail.
///
/// Holds the session id and the database path (opened read-only per call). Every
/// read here is non-destructive — consuming mail is the exclusive job of the agent's
/// `read` through the single writer, and a wake must never advance a cursor.
#[derive(Debug)]
pub struct SessionMail {
    session: SessionId,
    db_path: PathBuf,
}

impl SessionMail {
    /// Build a view of `session`'s mail in the store at `db_path`.
    pub fn new(db_path: impl Into<PathBuf>, session: SessionId) -> Self {
        Self {
            session,
            db_path: db_path.into(),
        }
    }

    /// Peek at the session's currently-unread topics WITHOUT consuming anything —
    /// the read the `FileChanged` wake hook makes to decide whether a sentinel change
    /// reflects genuine mail (exit 2) or a stray touch (exit 0). Opens the store
    /// read-only, so it needs no daemon socket; a missing or unreadable store surfaces
    /// as an error the caller treats as "nothing to wake about" (the anti-loop-safe
    /// default).
    pub fn peek_unread(&self) -> Result<Vec<Topic>, WakeError> {
        let store = ReadOnlyStore::open(&self.db_path)?;
        Ok(store.topics_with_unread(&self.session)?)
    }

    /// The same peek, carrying the [`WakeWatermark`] as well — the read the ADR-0012
    /// turn-boundary re-trigger makes to decide whether the session is sitting on mail
    /// it has NOT yet been re-triggered for.
    ///
    /// Kept beside [`SessionMail::peek_unread`] because they must never disagree: both
    /// are the one read-only unread predicate, asked with and without the watermark.
    pub fn peek_unread_state(&self) -> Result<Unread, WakeError> {
        let store = ReadOnlyStore::open(&self.db_path)?;
        Ok(store.unread(&self.session)?)
    }

    /// ARM the session's wake sentinel: write its currently-unread topic set,
    /// creating the file if it does not exist. Returns what was written.
    ///
    /// # Why arming is a separate step from the daemon's bump
    ///
    /// Claude Code watches a PATH. The daemon rewrites that file on every publish,
    /// which is a MODIFY — but the first write for a session that has never been armed
    /// is a CREATE, and a create is a different event the watch may not deliver at
    /// all. So the `SessionStart` hook writes the file itself, before printing the
    /// `watchPaths` registration that puts a watch on it.
    ///
    /// It doubles as the level-triggered arm the wake path requires (AGENTS.md): mail
    /// that landed while no process owned this session is written into the sentinel
    /// here, so a session that starts (or resumes) on top of unread mail wakes for it
    /// instead of waiting for the next publish.
    ///
    /// A store it cannot read does NOT leave the session unarmed: it arms with an
    /// empty topic set, because an existing-but-empty sentinel is watchable and a
    /// missing one is not.
    pub fn arm(&self, sentinel: &Sentinel) -> Result<Vec<Topic>, SentinelError> {
        let topics = match self.peek_unread() {
            Ok(topics) => topics,
            Err(err) => {
                warn!(
                    session = self.session.as_str(),
                    error = %err,
                    "could not read unread mail while arming the wake sentinel; arming with an \
                     empty topic set (the file must EXIST to be watchable, and the daemon \
                     rewrites it on the next publish)"
                );
                Vec::new()
            }
        };
        sentinel.write_topics(&topics).map(|()| topics)
    }

    /// The ADR-0012 TURN-BOUNDARY re-trigger: re-bump `sentinel` if this session is
    /// sitting on mail it has not already been re-triggered for.
    ///
    /// # Why this exists
    ///
    /// The steady-state wake is an EDGE (publish → the daemon writes the sentinel →
    /// `FileChanged` → the wake hook exits 2), and an edge only reaches an IDLE
    /// session. Mail published while the agent is mid-turn bumps the sentinel to no
    /// effect — the hook does not even run — and nothing bumps it again, so the
    /// session goes idle DEAF on top of unread mail. This is the level check that
    /// closes that window, run from the `Stop` hook because a turn boundary is the
    /// only moment the harness learns a turn ended.
    ///
    /// It lives here, next to [`SessionMail::arm`], because "read the unread state,
    /// then write the sentinel" is this type's job — the hook layer should only decide
    /// WHEN to ask and how to report the answer.
    ///
    /// # Bounded, so it cannot loop
    ///
    /// An agent that wakes and does not read would otherwise be nudged every turn
    /// forever. So the bump is gated on [`needs_retrigger`]: only mail strictly newer
    /// than the recorded watermark earns one, i.e. **at most one turn-boundary wake
    /// per message**.
    ///
    /// Ordering is load-bearing: **bump first, record second.** A crash between them
    /// costs a duplicate nudge (harmless — the wake hook re-checks the store), while
    /// recording first would lose the wake outright. That is also why a failed record
    /// is its own outcome rather than an error: the bump already landed, so the wake
    /// still fires.
    ///
    /// Returns the decision rather than logging it, so the caller can report it and a
    /// test can assert it. Only the store read is an `Err` — a sentinel failure is a
    /// reported outcome, never fatal to a per-turn hook.
    pub fn retrigger_if_unread(&self, sentinel: &Sentinel) -> Result<RetriggerOutcome, WakeError> {
        let Unread::Pending(mail) = self.peek_unread_state()? else {
            return Ok(RetriggerOutcome::CaughtUp);
        };
        let high_water = mail.high_water();

        let record = sentinel.last_retriggered();
        if !needs_retrigger(record, high_water) {
            return Ok(RetriggerOutcome::AlreadyRetriggered {
                last: record,
                high_water,
            });
        }

        let topics = mail.topics().to_vec();
        if let Err(error) = sentinel.write_topics(&topics) {
            return Ok(RetriggerOutcome::BumpFailed { error });
        }
        // The bump landed: from here the wake WILL fire regardless of what follows.
        match sentinel.record_retriggered(high_water) {
            Ok(()) => Ok(RetriggerOutcome::Retriggered { topics, high_water }),
            Err(error) => Ok(RetriggerOutcome::RetriggeredUnrecorded {
                topics,
                high_water,
                error,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The ADR-0012 anti-loop rule, at its boundaries. This is the whole bound on
    /// "how many times can one message wake an agent", so every case is pinned here
    /// rather than inferred from mtimes in a slow integration test.
    #[test]
    fn the_anti_loop_re_triggers_only_for_strictly_newer_mail() {
        let mark = |n: i64| WakeWatermark::parse(&n.to_string()).unwrap();

        // Never re-triggered: the first turn boundary after mail arrives must nudge.
        assert!(needs_retrigger(RetriggerRecord::Missing, mark(5)));

        // Already nudged for exactly this mail: the loop stops here. This is the case
        // that keeps an agent which wakes and never reads from being nudged forever.
        assert!(!needs_retrigger(RetriggerRecord::At(mark(5)), mark(5)));

        // Older mail than we have already nudged for cannot re-nudge either.
        assert!(!needs_retrigger(RetriggerRecord::At(mark(9)), mark(5)));

        // But NEWER mail always earns its own nudge — the bound is per-message, not a
        // permanent silence.
        assert!(needs_retrigger(RetriggerRecord::At(mark(5)), mark(6)));

        // A broken record fails SAFE: re-trigger (a redundant wake) rather than
        // assume delivery (a swallowed one).
        assert!(needs_retrigger(RetriggerRecord::Unreadable, mark(1)));
    }

    #[test]
    fn reminder_lists_only_topic_names() {
        let topics = [
            Topic::parse("github.pr.o/r#1").unwrap(),
            Topic::parse("github.pr.o/r#2").unwrap(),
        ];
        let line = reminder(&topics);
        assert_eq!(line, "mail on topic github.pr.o/r#1, github.pr.o/r#2");
        // The wake wire is payload-free by construction: there is no field in which a
        // body could be smuggled, and this pins that the line carries none.
        assert!(!line.contains('{'), "the reminder must be payload-free");
    }

    /// The daemon's bump writes topic NAMES into the session's OWN sentinel — the
    /// per-session isolation the `watchPaths` registration depends on (the
    /// `FileChanged` matcher is the shared basename, so nothing but the path keeps one
    /// session's bump out of another session's hook).
    #[test]
    fn waking_writes_topic_names_into_that_session_and_no_other() {
        let dir = tempfile::TempDir::new().unwrap();
        let waker = Waker::new(dir.path());
        let (a, b) = (SessionId::new("s-a"), SessionId::new("s-b"));
        let topic = Topic::parse("agent.s-a").unwrap();

        waker.wake(&a, std::slice::from_ref(&topic)).unwrap();

        let sentinel_a = Sentinel::under_root(dir.path(), &a);
        assert_eq!(sentinel_a.read_topics(), vec!["agent.s-a".to_string()]);
        let raw = std::fs::read_to_string(sentinel_a.path()).unwrap();
        assert!(
            !raw.contains('{'),
            "the sentinel must be payload-free: {raw:?}"
        );
        assert!(
            !Sentinel::under_root(dir.path(), &b).path().exists(),
            "waking A must not touch B's sentinel"
        );
    }

    /// Every wake rewrites the file even when the topic set is identical, so a second
    /// message on an already-unread topic still advances the mtime. This is the whole
    /// no-coalescing guarantee: a lost wake is structurally impossible.
    #[test]
    fn waking_twice_with_the_same_topics_still_advances_the_mtime() {
        let dir = tempfile::TempDir::new().unwrap();
        let waker = Waker::new(dir.path());
        let session = SessionId::new("s-a");
        let topics = [Topic::parse("t.a").unwrap()];
        let sentinel = Sentinel::under_root(dir.path(), &session);
        let mtime = || {
            std::fs::metadata(sentinel.path())
                .unwrap()
                .modified()
                .unwrap()
        };

        waker.wake(&session, &topics).unwrap();
        let first = mtime();

        std::thread::sleep(std::time::Duration::from_millis(10));
        waker.wake(&session, &topics).unwrap();

        assert!(
            mtime() > first,
            "an unconditional write must always bump the mtime, even for an identical set"
        );
    }

    /// Arming must CREATE the sentinel even with nothing unread — and even with no
    /// store at all. A file that does not exist is not watchable, and the whole point
    /// of arming is to give `watchPaths` something to register before the daemon's
    /// first bump.
    #[test]
    fn arming_creates_the_sentinel_even_with_no_store() {
        let dir = tempfile::TempDir::new().unwrap();
        let session = SessionId::new("s-unarmed");
        let sentinel = Sentinel::under_root(dir.path(), &session);
        let mail = SessionMail::new(dir.path().join("nonexistent.db"), session);

        let topics = mail.arm(&sentinel).unwrap();

        assert!(
            topics.is_empty(),
            "no store means nothing is known to be unread"
        );
        assert!(
            sentinel.path().exists(),
            "arming must leave a watchable file behind, store or no store"
        );
    }
}
