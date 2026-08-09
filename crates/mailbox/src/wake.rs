//! The wake path: telling an idle Claude Code session that mail has arrived.
//!
//! # Two channels, tried in order (ADR-0020)
//!
//! ```text
//! publish → has this subscriber a bound Claude Code inbox socket?
//!           ├─ yes → write the socket; the idle session takes a turn
//!           └─ no  → write its sentinel → `FileChanged` → `mailbox harness wake`
//!                    exits 2 → the idle session takes a turn
//! ```
//!
//! The **peer channel** is one hop and is preferred. The **sentinel channel** is
//! everything ADR-0008 and ADR-0017 describe, and it stays because Claude Code's
//! `agents_cross_session_inbox` gate decides which sessions bind a socket and
//! **cannot be turned on from outside Claude Code** — on the machine this was
//! designed against, 2 of 19 live sessions had one, across identical versions.
//!
//! The sentinel path used to be five moving parts: a per-session FIFO, a detached
//! watcher process blocked on it, a single-waiter advisory lock, a waiter pidfile,
//! and the sentinel. Every one could fail silently, leaving the agent deaf with
//! nothing reporting it (ADR-0017).
//!
//! # The two channels are not equally forgiving
//!
//! On the sentinel channel the write is only a TRIGGER: the woken hook re-reads the
//! store and exits 0 if there is nothing unread, so a spurious bump costs a hook
//! process and no model turn. On the peer channel **delivery IS the wake** — there
//! is no second opinion between the socket write and the agent taking a turn. So a
//! peer message is sent only when there is genuinely something to report; see
//! [`Waker::deliver`].
//!
//! # What crosses the boundary (and what does not)
//!
//! Wake is **payload-free** (ADR-0001, docs/01-wake.md): the sentinel
//! carries topic NAMES only, and the woken hook reports at most those names on its
//! stderr. The event body never crosses this boundary — it stays in the durable log
//! and is read later by the agent's `read`. Wake is ingress, not authority.
//!
//! # Two sides, two types
//!
//! - [`Waker`] is the DAEMON side: given a session and its currently-unread topics,
//!   deliver on whichever channel that session supports. Held by the [`crate::bus`],
//!   used on every publish.
//! - [`SessionMail`] is the HOOK side: a read-only view of one session's unread
//!   mail, used by the `FileChanged` wake hook to decide whether to wake (exit 2),
//!   by `SessionStart` to arm the sentinel, and by the `Stop` hook for the ADR-0012
//!   turn-boundary re-trigger.
//!
//! They never talk to each other; the durable store and the sentinel file are the
//! only things between them.

use std::path::{Path, PathBuf};

use tracing::{info, warn};

use mailbox_protocol::Topic;

use crate::claude_registry::ClaudeRegistry;
use crate::peer::{self, PeerDeliveryError};
use crate::sentinel::{RetriggerRecord, Sentinel, SentinelError};
use crate::storage::{ReadOnlyStore, SessionId, StorageError, Unread, WakeWatermark};

/// The process exit code that asks the Claude Code harness to wake the idle session
/// (the `asyncRewake` contract, docs/01-wake.md). A `u8` so the sole
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

/// Which channel carried one session's wake, and what went wrong on the way
/// (ADR-0020).
///
/// Returned rather than logged in place so [`Waker::wake_all`] can aggregate, and so
/// a test can assert the FALLBACK branch directly instead of inferring it from a file
/// mtime. Every variant carries what a reader needs to explain the outcome.
#[derive(Debug)]
pub enum WakeOutcome {
    /// Delivered on the session's inbox socket. One hop; no hook involved.
    Peer,

    /// The session has no usable socket, so the sentinel was written and the
    /// `FileChanged` path takes over. The ordinary case wherever the
    /// `agents_cross_session_inbox` gate has not reached.
    Sentinel,

    /// The socket was registered but would not take the frame, and the sentinel
    /// carried the wake instead. Expected transiently: a session can exit between
    /// the registry read and the write.
    SentinelAfterPeerFailure { peer_error: PeerDeliveryError },

    /// Nothing landed on either channel. The event is still durable and surfaces on
    /// the session's next `read`, `SessionStart` arm, or turn-boundary re-trigger.
    Undelivered {
        peer_error: Option<PeerDeliveryError>,
        sentinel_error: SentinelError,
    },
}

/// The DAEMON side of wake: delivers a session's unread topic set on whichever
/// channel that session supports — its inbox socket, or its sentinel.
///
/// Cheap to clone; it holds only two directory paths. It never opens the database —
/// it is handed the sessions to wake and what they have unread.
#[derive(Debug, Clone)]
pub struct Waker {
    sentinel_root: PathBuf,
    sessions_dir: PathBuf,
}

impl Waker {
    /// Build a waker over the resolved sentinel root (`~/.mailbox` by default; see
    /// [`crate::sentinel`]) and Claude Code's sessions directory (`~/.claude/sessions`
    /// by default; see [`crate::claude_registry`]).
    ///
    /// Both are resolved ONCE, by the daemon at startup, rather than re-read from the
    /// environment per publish — which also means a test can point them at a tempdir
    /// and never touch the developer's real `~/.claude` or `~/.mailbox`.
    ///
    /// The sessions directory is a PATH, not a registry: its *contents* are re-read on
    /// every publish, because sessions start, stop and resume constantly.
    pub fn new(sentinel_root: impl Into<PathBuf>, sessions_dir: impl Into<PathBuf>) -> Self {
        Self {
            sentinel_root: sentinel_root.into(),
            sessions_dir: sessions_dir.into(),
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

    /// Deliver one session's wake on the best channel available to it.
    ///
    /// `socket` is that session's inbox socket if Claude Code bound one — see
    /// [`ClaudeRegistry::inbox_socket`]. Passing it in (rather than looking it up
    /// here) keeps the channel decision a pure function of its inputs, so every
    /// branch below is testable without a registry on disk.
    ///
    /// # Why an empty topic set never goes on the peer channel
    ///
    /// The sentinel is a TRIGGER — the woken hook re-reads the store and exits 0 when
    /// there is nothing unread, so writing an empty set is harmless and is what keeps
    /// the file's contents agreeing with reality. The peer channel has no such second
    /// opinion: the message IS the turn. Sending "you have mail" when the session has
    /// none would spend a model turn to say nothing, which is the exact cost this
    /// whole subsystem exists to avoid. So an empty set takes the sentinel path only.
    pub fn deliver(
        &self,
        session: &SessionId,
        unread: &[Topic],
        socket: Option<&Path>,
    ) -> WakeOutcome {
        if let Some(socket) = socket.filter(|_| !unread.is_empty()) {
            match peer::deliver(socket, &reminder(unread)) {
                Ok(()) => return WakeOutcome::Peer,
                Err(peer_error) => {
                    // Fall through to the sentinel: a registered socket that will not
                    // take the frame is a session that went away between the registry
                    // read and now, and the fallback still reaches it if its hooks are
                    // installed.
                    return match self.wake(session, unread) {
                        Ok(()) => WakeOutcome::SentinelAfterPeerFailure { peer_error },
                        Err(sentinel_error) => WakeOutcome::Undelivered {
                            peer_error: Some(peer_error),
                            sentinel_error,
                        },
                    };
                }
            }
        }

        match self.wake(session, unread) {
            Ok(()) => WakeOutcome::Sentinel,
            Err(sentinel_error) => WakeOutcome::Undelivered {
                peer_error: None,
                sentinel_error,
            },
        }
    }

    /// Wake every session in `unread_by_session`, then log the aggregate outcome.
    ///
    /// Reads Claude Code's session registry ONCE per publish and never caches it:
    /// sessions start, stop and resume constantly, and a wake delivered to a socket
    /// that closed a minute ago is a lost wake. A directory that does not exist (no
    /// Claude Code on this machine) yields an empty registry, which puts every
    /// subscriber on the sentinel channel — the pre-ADR-0020 behaviour.
    pub fn wake_all(&self, unread_by_session: &[(SessionId, Vec<Topic>)], topic: &Topic) {
        let registry = ClaudeRegistry::read_dir(&self.sessions_dir);
        self.wake_all_with_registry(unread_by_session, topic, &registry);
    }

    /// The injectable core of [`Waker::wake_all`], taking the registry rather than
    /// reading it, so the two-channel behaviour is testable against a fixture.
    ///
    /// Payload-free: `topic` is used only for the log line. With zero sessions this
    /// logs all-zero counts — an honest "nothing to wake", not a misleading claim.
    ///
    /// A failure for one session is logged and skipped: the event is already durable,
    /// so delivery must never fail a publish, and the other subscribers still get
    /// their wake.
    pub fn wake_all_with_registry(
        &self,
        unread_by_session: &[(SessionId, Vec<Topic>)],
        topic: &Topic,
        registry: &ClaudeRegistry,
    ) {
        let (mut peer, mut sentinel, mut fell_back, mut failed) = (0usize, 0usize, 0usize, 0usize);

        for (session, unread) in unread_by_session {
            match self.deliver(session, unread, registry.inbox_socket(session)) {
                WakeOutcome::Peer => peer += 1,
                WakeOutcome::Sentinel => sentinel += 1,
                WakeOutcome::SentinelAfterPeerFailure { peer_error } => {
                    fell_back += 1;
                    warn!(
                        session = session.as_str(),
                        error = %peer_error,
                        "a subscriber's inbox socket refused the wake; fell back to its sentinel"
                    );
                }
                WakeOutcome::Undelivered {
                    peer_error,
                    sentinel_error,
                } => {
                    failed += 1;
                    warn!(
                        session = session.as_str(),
                        peer_error = peer_error.map(|e| e.to_string()).unwrap_or_default(),
                        error = %sentinel_error,
                        "could not wake a subscriber on either channel; it will not wake for \
                         this event (the event is durable and surfaces on its next read)"
                    );
                }
            }
        }

        // Log after the decision point: which topic drove the delivery, and how each
        // subscriber was actually reached. Never the body.
        info!(
            topic = topic.as_str(),
            peer, sentinel, fell_back, failed, "woke subscribers after publish"
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
        let waker = Waker::new(dir.path(), dir.path());
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
        let waker = Waker::new(dir.path(), dir.path());
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
    /// Accept one connection on `socket` and hand back whatever line was written.
    fn listen_once(socket: &std::path::Path) -> std::sync::mpsc::Receiver<String> {
        use std::io::{BufRead, BufReader};
        let listener = std::os::unix::net::UnixListener::bind(socket).unwrap();
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            if let Ok((stream, _)) = listener.accept() {
                let mut line = String::new();
                let _ = BufReader::new(stream).read_line(&mut line);
                let _ = tx.send(line);
            }
        });
        rx
    }

    /// The happy path of ADR-0020: a session with a bound socket is woken in ONE hop,
    /// and its sentinel is not written at all. The sentinel assertion is the point —
    /// writing both would leave the `FileChanged` hook firing redundantly for a
    /// session that has already taken its turn.
    #[test]
    fn delivers_on_the_peer_channel_and_leaves_the_sentinel_alone() {
        let dir = tempfile::TempDir::new().unwrap();
        let socket = dir.path().join("inbox.sock");
        let rx = listen_once(&socket);
        let waker = Waker::new(dir.path(), dir.path());
        let session = SessionId::new("s-peer");
        let topics = [Topic::parse("t.a").unwrap()];

        let outcome = waker.deliver(&session, &topics, Some(socket.as_path()));

        assert!(matches!(outcome, WakeOutcome::Peer), "{outcome:?}");
        let line = rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("the session should have received a frame");
        let parsed: serde_json::Value = serde_json::from_str(line.trim_end()).unwrap();
        assert_eq!(parsed["message"]["content"], "mail on topic t.a");
        assert!(
            !Sentinel::under_root(dir.path(), &session).path().exists(),
            "a peer delivery must not also write the sentinel"
        );
    }

    /// The gate leaves most sessions without a socket, and they must keep waking
    /// exactly as they did before ADR-0020.
    #[test]
    fn falls_back_to_the_sentinel_when_the_session_has_no_socket() {
        let dir = tempfile::TempDir::new().unwrap();
        let waker = Waker::new(dir.path(), dir.path());
        let session = SessionId::new("s-nosock");
        let topics = [Topic::parse("t.a").unwrap()];

        let outcome = waker.deliver(&session, &topics, None);

        assert!(matches!(outcome, WakeOutcome::Sentinel), "{outcome:?}");
        assert_eq!(
            Sentinel::under_root(dir.path(), &session).read_topics(),
            vec!["t.a".to_string()]
        );
    }

    /// A session can exit between the registry read and the write. That must degrade
    /// to the fallback, not lose the wake.
    #[test]
    fn falls_back_to_the_sentinel_when_the_socket_refuses() {
        let dir = tempfile::TempDir::new().unwrap();
        let waker = Waker::new(dir.path(), dir.path());
        let session = SessionId::new("s-dead");
        let topics = [Topic::parse("t.a").unwrap()];

        let outcome = waker.deliver(&session, &topics, Some(&dir.path().join("gone.sock")));

        assert!(
            matches!(outcome, WakeOutcome::SentinelAfterPeerFailure { .. }),
            "{outcome:?}"
        );
        assert_eq!(
            Sentinel::under_root(dir.path(), &session).read_topics(),
            vec!["t.a".to_string()],
            "the sentinel must carry the wake the socket refused"
        );
    }

    /// The asymmetry between the channels, pinned. An empty sentinel write is benign
    /// (the hook re-checks the store and exits 0), but an empty PEER message would
    /// spend a full model turn to announce nothing — the exact cost this subsystem
    /// exists to avoid. There is no anti-loop on the peer channel, so the guard has
    /// to be here.
    #[test]
    fn never_sends_an_empty_wake_on_the_peer_channel() {
        let dir = tempfile::TempDir::new().unwrap();
        let socket = dir.path().join("inbox.sock");
        let rx = listen_once(&socket);
        let waker = Waker::new(dir.path(), dir.path());
        let session = SessionId::new("s-empty");

        let outcome = waker.deliver(&session, &[], Some(socket.as_path()));

        assert!(
            matches!(outcome, WakeOutcome::Sentinel),
            "an empty unread set belongs on the sentinel channel only: {outcome:?}"
        );
        assert!(
            rx.recv_timeout(std::time::Duration::from_millis(200))
                .is_err(),
            "nothing may be written to the socket when there is nothing unread"
        );
    }

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
