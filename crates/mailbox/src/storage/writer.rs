//! The single writer: the one place that owns the `rusqlite::Connection`.
//!
//! # Why a single writer (ADR-0003)
//!
//! SQLite serves concurrent readers well but wants one writer. Rather than let
//! many callers race for the write lock (busy loops, `SQLITE_BUSY`, subtle
//! corruption), the whole database lives on **one dedicated OS thread**. That
//! thread owns the `Connection` outright — it is never shared — and the only
//! thing the rest of the process holds is a channel [`Sender`]. Every mutation
//! *and* every read is a [`Command`] posted to that channel and answered on a
//! `oneshot`, so there is structurally no second path to the DB.
//!
//! A happy side effect: because exactly one thread ever touches the connection,
//! per-topic offset assignment (`MAX(offset)+1`) cannot race, and no caller
//! *within this bridge process* can observe `SQLITE_BUSY`. Cross-process
//! exclusion is deliberately out of scope for this module: other processes (the
//! CLI, adapters) never open the database directly — they speak the protocol to
//! the bridge (ADR-0003), and that daemon/socket lifecycle is owned by later
//! cards (06–08), not here. No cross-process file locking lives in this card.

use rusqlite::{Connection, OptionalExtension, params};
use serde_json::Value;
use tokio::sync::oneshot;
use tracing::{error, info, warn};

use mailbox_protocol::{AdapterId, Cursor, Event, EventId, Offset, Timestamp, Topic, inbox_topic};

use super::error::StorageError;
use super::model::{
    EndSessionOutcome, Pid, PublishAttempt, ReadPage, SessionId, SubscribeKind, SubscribeOutcome,
    TopicSummary, Watch, WatchId, WatchKind, WatchSpec, WatchState, WatchTarget,
};

/// How long after a session ends its tombstone refuses a re-subscription of the
/// same id (milliseconds). See [`do_subscribe_and_baseline`] for the full
/// argument; in brief: the arm-Subscribe-vs-cleanup-EndSession race (ADR-0007) is
/// sub-second, whereas a genuine resume of the same session id happens far later,
/// so a 10s window covers the race with vast head room while still auto-healing a
/// real restart (which simply re-registers on its next arm once the guard lapses).
/// This deliberately does not depend on Claude Code's SessionStart-on-resume
/// matcher behaviour, which we cannot verify.
const SUBSCRIBE_TOMBSTONE_GUARD_MS: i64 = 10_000;

/// Default page size when a reader does not specify a limit. Bounds memory for
/// a catch-up read without forcing every caller to pick a number.
const DEFAULT_READ_LIMIT: u32 = 256;

/// Hard cap on the effective read page size, whatever a caller asks for. A read
/// materializes the whole page into a `Vec` on the single writer thread, so an
/// unbounded `limit` (the `u32` max is ~4.3B) could stall the writer and blow
/// memory. 1000 events is a generous single catch-up page; larger backlogs page
/// via the returned `next` cursor. (Reads currently share the writer's
/// serialization; a read-only side connection is a deliberately-deferred perf
/// optimisation per ADR-0003 — see the module docs.)
const MAX_READ_LIMIT: u32 = 1000;

/// Convert a wire [`Offset`] (u64) into the `i64` SQLite stores.
///
/// This is the single checked crossing between the two integer worlds. Offsets
/// above `i64::MAX` cannot exist in an i64-keyed column, so a cursor beyond it
/// is garbage/overflow; catching it here stops it from wrapping negative and
/// turning `WHERE offset > ?` into "match the whole log".
fn offset_to_sqlite(offset: Offset) -> Result<i64, StorageError> {
    i64::try_from(offset.0).map_err(|_| StorageError::OffsetOutOfRange { offset: offset.0 })
}

/// Convert an `i64` offset read back from SQLite into a wire [`Offset`]. A
/// negative value is impossible for a value we wrote, so it is corrupt data.
fn sqlite_to_offset(value: i64) -> Result<Offset, StorageError> {
    u64::try_from(value)
        .map(Offset)
        .map_err(|_| StorageError::Corrupt {
            detail: format!("negative offset {value} stored"),
        })
}

/// A unit of work for the writer thread. Each variant carries its arguments and
/// a `oneshot` sender for the reply, so the caller awaits exactly its own result.
///
/// Private to storage: callers never build these directly, they go through the
/// typed [`super::Storage`] methods.
pub(crate) enum Command {
    Publish {
        topic: Topic,
        adapter: AdapterId,
        timestamp: Timestamp,
        body: Value,
        reply: oneshot::Sender<Result<Event, StorageError>>,
    },
    /// Publish on behalf of a SESSION (an agent), rather than an adapter: check the
    /// publisher is caught up on the topic, append, and advance its own cursor past
    /// its own event — all in ONE transaction, so the "caught up" check cannot go
    /// stale between the check and the append. See [`PublishAttempt`] and
    /// `do_publish_as_session`.
    PublishAsSession {
        topic: Topic,
        adapter: AdapterId,
        timestamp: Timestamp,
        body: Value,
        publisher: SessionId,
        reply: oneshot::Sender<Result<PublishAttempt, StorageError>>,
    },
    ReadEvents {
        topic: Topic,
        cursor: Cursor,
        limit: Option<u32>,
        reply: oneshot::Sender<Result<ReadPage, StorageError>>,
    },
    Unsubscribe {
        session: SessionId,
        topic: Topic,
        reply: oneshot::Sender<Result<(), StorageError>>,
    },
    /// Subscribe (idempotent) AND baseline the delivery cursor to the topic head
    /// in ONE transaction. Composed here — not from two awaited handle calls — so
    /// there is no window in which a concurrent publish lands between "recorded
    /// the subscription" and "set the baseline" and is either missed or replayed.
    SubscribeAndBaseline {
        session: SessionId,
        topic: Topic,
        /// Wall-clock now (Unix millis), for the tombstone guard's age check. The
        /// caller stamps it (like `AddInterest.last_seen`) so the writer never
        /// reaches for the clock itself and tests can inject an instant.
        now_ms: i64,
        /// Which caller path this is — the axis the tombstone guard branches on.
        /// Threaded like `now_ms` (never inferred inside the writer) so only the
        /// automatic inbox re-registration is guarded (ADR-0007).
        kind: SubscribeKind,
        reply: oneshot::Sender<Result<SubscribeOutcome, StorageError>>,
    },
    /// Read a session's unread events across ALL its subscribed topics AND advance
    /// each topic's cursor to the last returned offset, in ONE transaction. This
    /// atomic read-then-advance is the load-bearing exactly-once operation: an
    /// event is unread until it has been returned by exactly one such command, and
    /// the cursor advance that "consumes" it commits with the read that produced
    /// it, so no interleaving can deliver it twice or drop it.
    ReadUnread {
        session: SessionId,
        limit: Option<u32>,
        reply: oneshot::Sender<Result<Vec<Event>, StorageError>>,
    },
    GetCursor {
        session: SessionId,
        topic: Topic,
        reply: oneshot::Sender<Result<Option<Offset>, StorageError>>,
    },
    /// List the sessions subscribed to a topic (the kick side of wake). A read,
    /// but routed through the writer channel like every other op so it never
    /// opens a second connection.
    SessionsSubscribed {
        topic: Topic,
        reply: oneshot::Sender<Result<Vec<SessionId>, StorageError>>,
    },
    /// List the topics a session subscribes to (the "arm-iff-subscribed" read,
    /// card 11). A read routed through the writer channel like every other op.
    SessionSubscriptions {
        session: SessionId,
        reply: oneshot::Sender<Result<Vec<Topic>, StorageError>>,
    },
    /// Drop all of a session's subscriptions AND interests in one transaction,
    /// returning what was removed and which watches reached zero interest (the
    /// SessionEnd teardown, card 11).
    EndSession {
        session: SessionId,
        /// Wall-clock now (Unix millis), recorded as the session's tombstone
        /// `ended_at_ms` in the same transaction. Caller-stamped like
        /// `SubscribeAndBaseline.now_ms`.
        now_ms: i64,
        reply: oneshot::Sender<Result<EndSessionOutcome, StorageError>>,
    },
    UpsertWatch {
        spec: WatchSpec,
        reply: oneshot::Sender<Result<WatchId, StorageError>>,
    },
    SetWatchState {
        id: WatchId,
        state: WatchState,
        reply: oneshot::Sender<Result<(), StorageError>>,
    },
    GetWatch {
        id: WatchId,
        reply: oneshot::Sender<Result<Option<Watch>, StorageError>>,
    },
    AddInterest {
        watch: WatchId,
        session: SessionId,
        /// Unix-millis last-seen stamp for the TTL sweeper (card 08).
        last_seen: i64,
        reply: oneshot::Sender<Result<u64, StorageError>>,
    },
    /// Refresh one interest's `last_seen`. A no-op if the interest row does not
    /// exist.
    TouchInterest {
        watch: WatchId,
        session: SessionId,
        last_seen: i64,
        reply: oneshot::Sender<Result<(), StorageError>>,
    },
    /// Refresh every interest held by `session` (the daemon-side liveness
    /// heartbeat, ADR-0009). Returns the number of interests refreshed.
    TouchSessionInterests {
        session: SessionId,
        last_seen: i64,
        reply: oneshot::Sender<Result<u64, StorageError>>,
    },
    /// Every distinct session holding at least one interest — the sweeper's
    /// liveness-probe candidates.
    ListInterestSessions {
        reply: oneshot::Sender<Result<Vec<SessionId>, StorageError>>,
    },
    /// The sessions holding an interest in ONE watch — the startup reconcile's
    /// liveness-probe candidates for that watch (design/01 rule 6).
    ListWatchInterestSessions {
        watch: WatchId,
        reply: oneshot::Sender<Result<Vec<SessionId>, StorageError>>,
    },
    /// Drop every interest whose `last_seen` is strictly older than `cutoff`,
    /// returning the watches whose interest thereby reached zero (the sweeper
    /// stops those adapters).
    SweepStaleInterests {
        cutoff: i64,
        reply: oneshot::Sender<Result<Vec<WatchId>, StorageError>>,
    },
    RemoveInterest {
        watch: WatchId,
        session: SessionId,
        reply: oneshot::Sender<Result<u64, StorageError>>,
    },
    InterestCount {
        watch: WatchId,
        reply: oneshot::Sender<Result<u64, StorageError>>,
    },
    /// Enumerate all watches (card-06 `status` / `unwatch`). A read routed through
    /// the writer channel like every other op.
    ListWatches {
        reply: oneshot::Sender<Result<Vec<Watch>, StorageError>>,
    },
    /// Per-topic unread counts for a session (card-06 `status`). A non-advancing
    /// read: it reports what a read *would* deliver without consuming it.
    UnreadCounts {
        session: SessionId,
        reply: oneshot::Sender<Result<Vec<(Topic, u64)>, StorageError>>,
    },
    /// The sessions with a registered agent inbox (card-16 `agents`). A read
    /// routed through the writer channel like every other op.
    ListAgentInboxes {
        reply: oneshot::Sender<Result<Vec<SessionId>, StorageError>>,
    },
    /// Every known topic with its subscriber/event counts (card-16 `topics`).
    ListTopics {
        prefix: Option<String>,
        reply: oneshot::Sender<Result<Vec<TopicSummary>, StorageError>>,
    },
    GetBaseline {
        watch: WatchId,
        reply: oneshot::Sender<Result<Option<Value>, StorageError>>,
    },
    SetBaseline {
        watch: WatchId,
        baseline: Value,
        reply: oneshot::Sender<Result<(), StorageError>>,
    },
    /// Run `PRAGMA integrity_check`. Exposed for operational/verification use.
    IntegrityCheck {
        reply: oneshot::Sender<Result<(), StorageError>>,
    },
}

/// Open the connection, apply pragmas, and migrate. Runs on the writer thread.
///
/// WAL mode lets readers proceed during a write and makes a mid-write crash
/// recoverable on reopen; `busy_timeout` is a defensive backstop (with a single
/// writer it should never fire); `synchronous = NORMAL` is the WAL-recommended
/// level — committed transactions survive a **process crash** (`kill -9`),
/// which is the failure the crash-recovery test exercises. It does NOT
/// guarantee durability across an OS crash or power loss (that needs
/// `synchronous = FULL`). FULL is deliberately not chosen: for a wake bus,
/// losing the very tail of the log on power loss is acceptable — a missed event
/// beats paying an fsync on every single commit.
pub(crate) fn open_connection(path: &std::path::Path) -> Result<Connection, StorageError> {
    let conn = Connection::open(path).map_err(|source| StorageError::Open {
        path: path.to_path_buf(),
        source,
    })?;

    // journal_mode returns the resulting mode as a row, so consume it (a plain
    // pragma_update would error on the returned row).
    let mode: String = conn.query_row("PRAGMA journal_mode = WAL", [], |row| row.get(0))?;
    debug_assert_eq!(mode.to_ascii_lowercase(), "wal");

    conn.busy_timeout(std::time::Duration::from_secs(5))?;
    conn.pragma_update(None, "synchronous", "NORMAL")?;
    conn.pragma_update(None, "foreign_keys", "ON")?;

    super::schema::migrate(&conn)?;
    Ok(conn)
}

/// The writer thread body: open, signal readiness, then service commands until
/// the channel closes (all handles dropped).
pub(crate) fn run(
    path: std::path::PathBuf,
    mut rx: tokio::sync::mpsc::Receiver<Command>,
    init: oneshot::Sender<Result<(), StorageError>>,
) {
    let mut conn = match open_connection(&path) {
        Ok(conn) => conn,
        Err(err) => {
            // Report the open/migration failure back to `open()`; do not run.
            error!(error = %err, "storage writer failed to start");
            let _ = init.send(Err(err));
            return;
        }
    };

    if init.send(Ok(())).is_err() {
        // The opener gave up before we finished; nothing to serve.
        return;
    }
    // Path is safe to log; event bodies are never logged (untrusted/large).
    info!(path = %path.display(), "storage writer started");

    while let Some(cmd) = rx.blocking_recv() {
        handle(&mut conn, cmd);
    }

    info!("storage writer stopped (all handles dropped)");
}

/// If a handler failed, log it with the operation name and identifying context
/// (never the body/baseline) so a `Sqlite`/`Json`/`Corrupt` failure is
/// attributable to a specific command. The context closure runs only on error.
fn log_on_err<T>(result: &Result<T, StorageError>, op: &str, context: impl FnOnce() -> String) {
    if let Err(err) = result {
        error!(op, context = %context(), error = %err, "storage command failed");
    }
}

/// Dispatch one command. Each arm runs its handler, logs any failure with
/// context, then sends the `Result` back; a dropped receiver (caller went away)
/// is ignored.
fn handle(conn: &mut Connection, cmd: Command) {
    match cmd {
        Command::Publish {
            topic,
            adapter,
            timestamp,
            body,
            reply,
        } => {
            let result = do_publish(conn, &topic, &adapter, timestamp, body);
            log_on_err(&result, "publish", || format!("topic={}", topic.as_str()));
            let _ = reply.send(result);
        }
        Command::PublishAsSession {
            topic,
            adapter,
            timestamp,
            body,
            publisher,
            reply,
        } => {
            let result = do_publish_as_session(conn, &topic, &adapter, timestamp, body, &publisher);
            log_on_err(&result, "publish_as_session", || {
                format!("topic={} session={}", topic.as_str(), publisher.as_str())
            });
            let _ = reply.send(result);
        }
        Command::ReadEvents {
            topic,
            cursor,
            limit,
            reply,
        } => {
            let result = do_read_events(conn, &topic, cursor, limit);
            log_on_err(&result, "read_events", || {
                format!("topic={}", topic.as_str())
            });
            let _ = reply.send(result);
        }
        Command::Unsubscribe {
            session,
            topic,
            reply,
        } => {
            let result = do_unsubscribe(conn, &session, &topic);
            log_on_err(&result, "unsubscribe", || {
                format!("session={} topic={}", session.as_str(), topic.as_str())
            });
            let _ = reply.send(result);
        }
        Command::GetCursor {
            session,
            topic,
            reply,
        } => {
            let result = do_get_cursor(conn, &session, &topic);
            log_on_err(&result, "get_cursor", || {
                format!("session={} topic={}", session.as_str(), topic.as_str())
            });
            let _ = reply.send(result);
        }
        Command::SessionsSubscribed { topic, reply } => {
            let result = do_sessions_subscribed(conn, &topic);
            log_on_err(&result, "sessions_subscribed", || {
                format!("topic={}", topic.as_str())
            });
            let _ = reply.send(result);
        }
        Command::SessionSubscriptions { session, reply } => {
            let result = do_session_subscriptions(conn, &session);
            log_on_err(&result, "session_subscriptions", || {
                format!("session={}", session.as_str())
            });
            let _ = reply.send(result);
        }
        Command::EndSession {
            session,
            now_ms,
            reply,
        } => {
            let result = do_end_session(conn, &session, now_ms);
            log_on_err(&result, "end_session", || {
                format!("session={}", session.as_str())
            });
            let _ = reply.send(result);
        }
        Command::SubscribeAndBaseline {
            session,
            topic,
            now_ms,
            kind,
            reply,
        } => {
            let result = do_subscribe_and_baseline(conn, &session, &topic, now_ms, kind);
            log_on_err(&result, "subscribe_and_baseline", || {
                format!("session={} topic={}", session.as_str(), topic.as_str())
            });
            let _ = reply.send(result);
        }
        Command::ReadUnread {
            session,
            limit,
            reply,
        } => {
            let result = do_read_unread(conn, &session, limit);
            log_on_err(&result, "read_unread", || {
                format!("session={}", session.as_str())
            });
            let _ = reply.send(result);
        }
        Command::UpsertWatch { spec, reply } => {
            let result = do_upsert_watch(conn, &spec);
            log_on_err(&result, "upsert_watch", || {
                format!(
                    "kind={} repo={} pr={}",
                    spec.target.kind().as_str(),
                    spec.target.repo_column(),
                    spec.target.pr_column()
                )
            });
            let _ = reply.send(result);
        }
        Command::SetWatchState { id, state, reply } => {
            let result = do_set_watch_state(conn, id, state);
            log_on_err(&result, "set_watch_state", || format!("watch={}", id.get()));
            let _ = reply.send(result);
        }
        Command::GetWatch { id, reply } => {
            let result = do_get_watch(conn, id);
            log_on_err(&result, "get_watch", || format!("watch={}", id.get()));
            let _ = reply.send(result);
        }
        Command::AddInterest {
            watch,
            session,
            last_seen,
            reply,
        } => {
            let result = do_add_interest(conn, watch, &session, last_seen);
            log_on_err(&result, "add_interest", || {
                format!("watch={} session={}", watch.get(), session.as_str())
            });
            let _ = reply.send(result);
        }
        Command::TouchInterest {
            watch,
            session,
            last_seen,
            reply,
        } => {
            let result = do_touch_interest(conn, watch, &session, last_seen);
            log_on_err(&result, "touch_interest", || {
                format!("watch={} session={}", watch.get(), session.as_str())
            });
            let _ = reply.send(result);
        }
        Command::TouchSessionInterests {
            session,
            last_seen,
            reply,
        } => {
            let result = do_touch_session_interests(conn, &session, last_seen);
            log_on_err(&result, "touch_session_interests", || {
                format!("session={}", session.as_str())
            });
            let _ = reply.send(result);
        }
        Command::ListInterestSessions { reply } => {
            let result = do_list_interest_sessions(conn);
            log_on_err(&result, "list_interest_sessions", String::new);
            let _ = reply.send(result);
        }
        Command::ListWatchInterestSessions { watch, reply } => {
            let result = do_list_watch_interest_sessions(conn, watch);
            log_on_err(&result, "list_watch_interest_sessions", || {
                watch.get().to_string()
            });
            let _ = reply.send(result);
        }
        Command::SweepStaleInterests { cutoff, reply } => {
            let result = do_sweep_stale_interests(conn, cutoff);
            log_on_err(&result, "sweep_stale_interests", || {
                format!("cutoff={cutoff}")
            });
            let _ = reply.send(result);
        }
        Command::RemoveInterest {
            watch,
            session,
            reply,
        } => {
            let result = do_remove_interest(conn, watch, &session);
            log_on_err(&result, "remove_interest", || {
                format!("watch={} session={}", watch.get(), session.as_str())
            });
            let _ = reply.send(result);
        }
        Command::InterestCount { watch, reply } => {
            let result = do_interest_count(conn, watch);
            log_on_err(&result, "interest_count", || {
                format!("watch={}", watch.get())
            });
            let _ = reply.send(result);
        }
        Command::ListWatches { reply } => {
            let result = do_list_watches(conn);
            log_on_err(&result, "list_watches", String::new);
            let _ = reply.send(result);
        }
        Command::UnreadCounts { session, reply } => {
            let result = do_unread_counts(conn, &session);
            log_on_err(&result, "unread_counts", || {
                format!("session={}", session.as_str())
            });
            let _ = reply.send(result);
        }
        Command::ListAgentInboxes { reply } => {
            let result = do_list_agent_inboxes(conn);
            log_on_err(&result, "list_agent_inboxes", String::new);
            let _ = reply.send(result);
        }
        Command::ListTopics { prefix, reply } => {
            let result = do_list_topics(conn, prefix.as_deref());
            log_on_err(&result, "list_topics", || {
                format!("prefix={}", prefix.as_deref().unwrap_or("-"))
            });
            let _ = reply.send(result);
        }
        Command::GetBaseline { watch, reply } => {
            let result = do_get_baseline(conn, watch);
            log_on_err(&result, "get_baseline", || format!("watch={}", watch.get()));
            let _ = reply.send(result);
        }
        Command::SetBaseline {
            watch,
            baseline,
            reply,
        } => {
            let result = do_set_baseline(conn, watch, baseline);
            log_on_err(&result, "set_baseline", || format!("watch={}", watch.get()));
            let _ = reply.send(result);
        }
        Command::IntegrityCheck { reply } => {
            let result = do_integrity_check(conn);
            log_on_err(&result, "integrity_check", String::new);
            let _ = reply.send(result);
        }
    }
}

fn do_publish(
    conn: &mut Connection,
    topic: &Topic,
    adapter: &AdapterId,
    timestamp: Timestamp,
    body: Value,
) -> Result<Event, StorageError> {
    let tx = conn.transaction()?;
    // No authoring session: an adapter (or an explicit `publish --no-session`) is
    // nobody's message, so it is unread — and wakeable — for EVERY subscriber.
    let event = append_event_tx(&tx, topic, adapter, timestamp, body, None)?;
    tx.commit()?;
    Ok(event)
}

/// Publish on behalf of a session (an agent), enforcing the caller-aware rule in ONE
/// transaction with the append (see [`PublishAttempt`]).
///
/// **Be caught up to speak.** If the publisher is subscribed to the topic and has
/// unread events on it **that it did not write itself**, REFUSE — nothing is
/// written. An agent that speaks over mail it has not read is talking past its peers,
/// and the durable log would interleave a reply to a message it never saw.
///
/// The "that it did not write itself" clause is what keeps the rule from deadlocking
/// the publisher on its own last message, and it is why the author is stamped on the
/// row (`event.author_session`, schema v5). The obvious alternative — advancing the
/// publisher's own cursor past its own event — was WRONG and is gone: the publisher
/// is *inferred* from the ambient `$CLAUDE_CODE_SESSION_ID`, which Claude Code
/// exports into every process the agent spawns, so a build script / git hook /
/// subagent publishing under that id had its event silently marked read for the
/// agent. That is silent mail loss. Nothing may skip an event for a session, so no
/// cursor is touched here: the event stays unread and readable for everyone,
/// including its (possibly mis-attributed) author — it simply does not *block* or
/// *wake* the session it is attributed to.
///
/// The rule only applies while subscribed: a session publishing to a topic it does
/// not subscribe to has no cursor and no unread, and is unaffected. The atomicity is
/// load-bearing — a check in one transaction and an append in another could be
/// separated by a concurrent publish, which would decide the rule on a log that no
/// longer exists by the time the event lands.
fn do_publish_as_session(
    conn: &mut Connection,
    topic: &Topic,
    adapter: &AdapterId,
    timestamp: Timestamp,
    body: Value,
    publisher: &SessionId,
) -> Result<PublishAttempt, StorageError> {
    let tx = conn.transaction()?;

    let subscribed: bool = tx
        .query_row(
            "SELECT 1 FROM subscription WHERE session_id = ?1 AND topic = ?2",
            params![publisher.as_str(), topic.as_str()],
            |_| Ok(()),
        )
        .optional()?
        .is_some();

    if subscribed {
        // The unread predicate of `do_unread_counts` / `read_topic_unread` (strictly
        // beyond the delivery cursor; absent cursor => -1 => everything), narrowed to
        // events this session did NOT author. Your own message is still unread — it
        // shows in `read` and in `status` — it just is not mail you owe anyone a read
        // of before you may speak again.
        let unread: i64 = tx.query_row(
            "SELECT COUNT(*) FROM event
             WHERE topic = ?1
               AND (author_session IS NULL OR author_session <> ?2)
               AND offset > COALESCE(
                   (SELECT offset FROM delivery_cursor
                    WHERE session_id = ?2 AND topic = ?1),
                   -1)",
            params![topic.as_str(), publisher.as_str()],
            |row| row.get(0),
        )?;
        if unread > 0 {
            // Refuse: the tx drops (rolls back) with nothing written.
            return Ok(PublishAttempt::RefusedUnread {
                unread: unread.max(0) as u64,
            });
        }
    }

    let event = append_event_tx(&tx, topic, adapter, timestamp, body, Some(publisher))?;

    tx.commit()?;
    Ok(PublishAttempt::Published(event))
}

/// Append one event to a topic's log inside an open transaction, assigning the next
/// per-topic offset and the opaque event id. Shared by the adapter publish and the
/// session publish so the two can never drift in how an event is minted.
///
/// `author` is the session that published it, or `None` for an adapter / anonymous
/// publish. It is provenance for the two caller-aware rules ONLY (be-caught-up-to-
/// speak, and no-self-wake); it grants no authority and the body stays opaque.
fn append_event_tx(
    tx: &rusqlite::Transaction<'_>,
    topic: &Topic,
    adapter: &AdapterId,
    timestamp: Timestamp,
    body: Value,
    author: Option<&SessionId>,
) -> Result<Event, StorageError> {
    let body_text = serde_json::to_string(&body)?;

    // Per-topic offset: strictly the next value after the current max for THIS
    // topic. Safe without locking because the single writer is the only place
    // that assigns offsets; two publishes can never read the same max. The
    // UNIQUE(topic, offset) constraint turns any violation of that assumption
    // into a hard error instead of a silent gap/duplicate.
    let offset: i64 = tx.query_row(
        "SELECT COALESCE(MAX(offset), -1) + 1 FROM event WHERE topic = ?1",
        params![topic.as_str()],
        |row| row.get(0),
    )?;

    // Insert with a temporary, provably-unique event_id, then rewrite it to the
    // global row id. Two steps because the opaque EventId is derived from the
    // row id, which is only known after insert; the temp is unique because
    // (topic, offset) is unique, so it never collides with a committed row.
    let temp_id = format!("pending:{}:{offset}", topic.as_str());
    tx.execute(
        "INSERT INTO event (topic, offset, event_id, adapter, timestamp, body, author_session)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            topic.as_str(),
            offset,
            temp_id,
            adapter.0,
            timestamp.0,
            body_text,
            author.map(SessionId::as_str),
        ],
    )?;
    let row_id = tx.last_insert_rowid();
    let event_id = format!("evt-{row_id}");
    tx.execute(
        "UPDATE event SET event_id = ?1 WHERE event_row_id = ?2",
        params![event_id, row_id],
    )?;

    Ok(Event {
        id: EventId(event_id),
        offset: sqlite_to_offset(offset)?,
        topic: topic.clone(),
        timestamp,
        body,
    })
}

fn do_read_events(
    conn: &Connection,
    topic: &Topic,
    cursor: Cursor,
    limit: Option<u32>,
) -> Result<ReadPage, StorageError> {
    // Both cursor forms reduce to "offset strictly greater than N": Oldest is
    // "greater than -1" (offsets start at 0). A cursor past i64::MAX has no
    // possible successor in the log, so it yields an empty page — NOT a wrapped
    // negative that would replay everything.
    let after: i64 = match cursor {
        Cursor::Oldest => -1,
        Cursor::After { offset } => match offset_to_sqlite(offset) {
            Ok(after) => after,
            Err(_) => {
                return Ok(ReadPage {
                    events: Vec::new(),
                    next: cursor,
                });
            }
        },
    };
    // Clamp the page size to a hard maximum regardless of what the caller asked.
    let limit = limit.unwrap_or(DEFAULT_READ_LIMIT).min(MAX_READ_LIMIT) as i64;

    let mut stmt = conn.prepare(
        "SELECT offset, event_id, timestamp, body
         FROM event
         WHERE topic = ?1 AND offset > ?2
         ORDER BY offset ASC
         LIMIT ?3",
    )?;
    let rows = stmt.query_map(params![topic.as_str(), after, limit], |row| {
        let offset: i64 = row.get(0)?;
        let event_id: String = row.get(1)?;
        let timestamp: i64 = row.get(2)?;
        let body_text: String = row.get(3)?;
        Ok((offset, event_id, timestamp, body_text))
    })?;

    let mut events = Vec::new();
    for row in rows {
        let (offset, event_id, timestamp, body_text) = row?;
        let body: Value = serde_json::from_str(&body_text)?;
        events.push(Event {
            id: EventId(event_id),
            offset: sqlite_to_offset(offset)?,
            topic: topic.clone(),
            timestamp: Timestamp(timestamp),
            body,
        });
    }

    // Next cursor points just past the last row returned; on an empty page we
    // hand back the same cursor so the caller can poll from where it asked.
    let next = match events.last() {
        Some(last) => Cursor::After {
            offset: last.offset,
        },
        None => cursor,
    };
    Ok(ReadPage { events, next })
}

fn do_unsubscribe(
    conn: &Connection,
    session: &SessionId,
    topic: &Topic,
) -> Result<(), StorageError> {
    conn.execute(
        "DELETE FROM subscription WHERE session_id = ?1 AND topic = ?2",
        params![session.as_str(), topic.as_str()],
    )?;
    Ok(())
}

fn do_get_cursor(
    conn: &Connection,
    session: &SessionId,
    topic: &Topic,
) -> Result<Option<Offset>, StorageError> {
    let offset: Option<i64> = conn
        .query_row(
            "SELECT offset FROM delivery_cursor WHERE session_id = ?1 AND topic = ?2",
            params![session.as_str(), topic.as_str()],
            |row| row.get(0),
        )
        .optional()?;
    match offset {
        Some(value) => Ok(Some(sqlite_to_offset(value)?)),
        None => Ok(None),
    }
}

/// The current head (highest assigned offset) of `topic`, or `None` if the
/// topic has no events yet. `MAX(offset)` over an empty set is SQL `NULL`, which
/// maps to `None` — the "no head to baseline to" case.
/// The sessions subscribed to `topic`. Order is not significant (the caller
/// kicks each independently), so no `ORDER BY`.
fn do_sessions_subscribed(
    conn: &Connection,
    topic: &Topic,
) -> Result<Vec<SessionId>, StorageError> {
    let mut stmt = conn.prepare("SELECT session_id FROM subscription WHERE topic = ?1")?;
    let rows = stmt.query_map(params![topic.as_str()], |row| row.get::<_, String>(0))?;
    let mut sessions = Vec::new();
    for row in rows {
        sessions.push(SessionId::new(row?));
    }
    Ok(sessions)
}

/// The topics `session` subscribes to, ascending (deterministic for the arm
/// decision + logs). A subscription row can only hold a topic the bridge
/// accepted, so a value that now fails the grammar is corrupt storage, not user
/// input (mirrors `do_unread_counts`).
fn do_session_subscriptions(
    conn: &Connection,
    session: &SessionId,
) -> Result<Vec<Topic>, StorageError> {
    let mut stmt =
        conn.prepare("SELECT topic FROM subscription WHERE session_id = ?1 ORDER BY topic ASC")?;
    let rows = stmt.query_map(params![session.as_str()], |row| row.get::<_, String>(0))?;
    let mut topics = Vec::new();
    for row in rows {
        let topic_str = row?;
        let topic = Topic::parse(&topic_str).map_err(|_| StorageError::Corrupt {
            detail: format!("invalid topic {topic_str:?} stored in subscription"),
        })?;
        topics.push(topic);
    }
    Ok(topics)
}

/// Drop every subscription and every watch interest held by `session`, and report
/// which watches thereby reached zero interest — all in ONE transaction so the
/// "who reached zero" answer is consistent with the deletion that caused it
/// (mirrors [`do_sweep_stale_interests`], but keyed by session rather than age).
///
/// The delivery cursors are intentionally left untouched: they are harmless
/// orphans once the subscriptions are gone (nothing reads them), and preserving
/// them means a session id that is ever reused does not silently replay history.
///
/// # The tombstone (ADR-0007, resurrection guard)
///
/// In the SAME transaction as the deletions, this records `now_ms` as the
/// session's tombstone `ended_at_ms`. That is what lets the `Subscribe` writer
/// path refuse a re-registration racing this end (see
/// [`do_subscribe_and_baseline`]): both land on the single writer in some order,
/// and without the tombstone the ordering [delete] → [subscribe] would resurrect
/// the dead session's inbox permanently (subscriptions have no TTL). Writing it
/// atomically with the delete means the guard can never observe a half-ended
/// session.
fn do_end_session(
    conn: &mut Connection,
    session: &SessionId,
    now_ms: i64,
) -> Result<EndSessionOutcome, StorageError> {
    let tx = conn.transaction()?;

    // The watches this session had an interest in — only these can have reached
    // zero, so we recount just them after the delete rather than every watch.
    let affected: Vec<i64> = {
        let mut stmt = tx.prepare("SELECT watch_id FROM watch_interest WHERE session_id = ?1")?;
        let rows = stmt.query_map(params![session.as_str()], |row| row.get::<_, i64>(0))?;
        rows.collect::<Result<Vec<_>, _>>()?
    };

    let interests_removed = tx.execute(
        "DELETE FROM watch_interest WHERE session_id = ?1",
        params![session.as_str()],
    )? as u64;
    let subscriptions_removed = tx.execute(
        "DELETE FROM subscription WHERE session_id = ?1",
        params![session.as_str()],
    )? as u64;

    let mut emptied_watches = Vec::new();
    for watch_id in affected {
        let count: i64 = tx.query_row(
            "SELECT COUNT(*) FROM watch_interest WHERE watch_id = ?1",
            params![watch_id],
            |row| row.get(0),
        )?;
        if count == 0 {
            emptied_watches.push(WatchId::new(watch_id));
        }
    }

    // Tombstone the session (ADR-0007) atomically with the delete above, so the
    // Subscribe path can refuse a re-registration that raced this end. INSERT OR
    // REPLACE keeps the newest end instant if the same id somehow ends twice.
    tx.execute(
        "INSERT OR REPLACE INTO session_tombstone (session_id, ended_at_ms) VALUES (?1, ?2)",
        params![session.as_str(), now_ms],
    )?;
    tx.commit()?;

    info!(
        session = session.as_str(),
        subscriptions_removed,
        interests_removed,
        emptied = emptied_watches.len(),
        "ended session (dropped its subscriptions and interests, tombstoned the id)"
    );
    Ok(EndSessionOutcome {
        subscriptions_removed,
        interests_removed,
        emptied_watches,
    })
}

/// The `ended_at_ms` of `session`'s tombstone, or `None` if it has none. The
/// resurrection-guard lookup for [`do_subscribe_and_baseline`], read inside the
/// caller's transaction so it is consistent with the write that follows.
fn tombstone_ended_at(
    tx: &rusqlite::Transaction,
    session: &SessionId,
) -> Result<Option<i64>, StorageError> {
    let ended_at: Option<i64> = tx
        .query_row(
            "SELECT ended_at_ms FROM session_tombstone WHERE session_id = ?1",
            params![session.as_str()],
            |row| row.get(0),
        )
        .optional()?;
    Ok(ended_at)
}

fn topic_head(tx: &rusqlite::Transaction, topic: &Topic) -> Result<Option<i64>, StorageError> {
    let head: Option<i64> = tx.query_row(
        "SELECT MAX(offset) FROM event WHERE topic = ?1",
        params![topic.as_str()],
        |row| row.get(0),
    )?;
    Ok(head)
}

/// Move `session`'s cursor on `topic` forward to `offset` inside `tx`.
///
/// # Exclusive ownership of the delivery cursor
///
/// For a bus subscriber the `delivery_cursor` row is owned SOLELY by the
/// read-and-advance path (`do_read_unread`) and the baseline set at subscribe
/// (`do_subscribe_and_baseline`). There is deliberately no public/general
/// `advance_cursor`: an external writer nudging the cursor past the head would
/// make `do_read_unread` silently skip the gap — a permanent lost delivery. The
/// `MAX(offset, excluded.offset)` upsert keeps the cursor monotonic (a late or
/// duplicate advance can never rewind delivery); this helper is `tx`-scoped so
/// the advance always commits atomically with the operation that produced `offset`.
///
/// Exactly TWO callers may advance a cursor, and each commits the advance with the
/// thing that justifies it: the read that delivered the events
/// (`read_topic_unread`), and the fresh subscription that baselines to the head
/// (`do_subscribe_and_baseline` — you are not shown mail sent before you existed).
///
/// A publish deliberately does NOT advance the publisher's cursor, even over its own
/// event. It used to, and that was silent mail loss: the "publisher" is inferred from
/// an ambient env var Claude Code exports into every process an agent spawns, so a
/// mis-attributed publish silently marked itself read for the agent (see
/// `do_publish_as_session`). Marking an event read is now something only a `read`
/// (or a baseline at subscribe time) may do.
fn advance_cursor_tx(
    tx: &rusqlite::Transaction,
    session: &SessionId,
    topic: &Topic,
    offset: i64,
) -> Result<(), StorageError> {
    tx.execute(
        "INSERT INTO delivery_cursor (session_id, topic, offset) VALUES (?1, ?2, ?3)
         ON CONFLICT(session_id, topic)
         DO UPDATE SET offset = MAX(offset, excluded.offset)",
        params![session.as_str(), topic.as_str(), offset],
    )?;
    Ok(())
}

/// Subscribe idempotently, and — only when the subscription is genuinely new —
/// baseline the delivery cursor to the topic head, all in one transaction.
///
/// # Why baseline only on a fresh subscription
///
/// Baselining unconditionally would break idempotency: a second `subscribe`
/// while already subscribed would jump the cursor to the head and silently skip
/// events the session had not yet read. So we baseline exactly when the
/// `INSERT OR IGNORE` actually inserts a row (`changed == 1`). The two cursorless
/// states are kept distinct in [`SubscribeOutcome`] for honest logging.
///
/// # Why to the head, and why that is always forward
///
/// A new (or re-subscribing) session must not replay history — it only cares
/// about events published after it declared interest (docs/01, baseline-on-
/// subscribe). The head only ever grows, so setting the cursor to it is always a
/// forward move; reusing the monotonic advance keeps that guarantee even against
/// a stale cursor left behind by an earlier unsubscribe.
///
/// # The tombstone guard, scoped to the auto-inbox path (ADR-0007, resurrection race)
///
/// Before creating the subscription we consult the session's tombstone (written
/// by [`do_end_session`]) — but ONLY on the [`SubscribeKind::AutoInbox`] path. The
/// race this closes: `harness arm` re-registers the inbox on every `Stop`, including
/// the final one, so an arm's auto-registration `Subscribe` can reach the single
/// writer *just after* `SessionEnd`'s delete — and, because subscriptions have no
/// TTL, would resurrect the dead session's inbox forever (an orphan waiter, `agents`
/// listing a corpse, `send` succeeding into a void). So for `AutoInbox`: a tombstone
/// younger than [`SUBSCRIBE_TOMBSTONE_GUARD_MS`] ⇒ REFUSE (create nothing, report
/// [`SubscribeOutcome::RefusedSessionRecentlyEnded`]); a tombstone OLDER than the
/// guard is a genuine restart long after the end ⇒ delete the stale tombstone and
/// proceed; no tombstone ⇒ proceed.
///
/// # Why Explicit is exempt (and safe to exempt)
///
/// An [`SubscribeKind::Explicit`] subscribe/watch is issued SYNCHRONOUSLY from a
/// live turn, which by construction completes before that turn's `SessionEnd` hook
/// fires. It therefore CANNOT be the doomed post-teardown async arm the guard
/// defends against — that culprit is exclusively `register_inbox` (AutoInbox). An
/// explicit subscribe reaching this writer after a tombstone was written is thus a
/// genuinely-resumed session proving it is alive, so it must PROCEED and CLEAR the
/// tombstone (else the resumed session would silently receive zero deliveries: a
/// running poller + interest row but no subscription). The resurrection guarantee
/// is untouched — the only path the racing arm uses is still fully guarded.
///
/// All branches run inside this one transaction so the check and the write commit
/// together.
fn do_subscribe_and_baseline(
    conn: &mut Connection,
    session: &SessionId,
    topic: &Topic,
    now_ms: i64,
    kind: SubscribeKind,
) -> Result<SubscribeOutcome, StorageError> {
    // Silent on success (the bus layer owns the subscribe log, like unsubscribe);
    // storage only logs failures via `log_on_err`.
    let tx = conn.transaction()?;

    match kind {
        // Explicit proof-of-life: clear any tombstone unconditionally and proceed.
        // Safe because a synchronous live-turn subscribe cannot be the racing arm
        // (see the fn docs); a no-op DELETE when there is no tombstone.
        SubscribeKind::Explicit => {
            tx.execute(
                "DELETE FROM session_tombstone WHERE session_id = ?1",
                params![session.as_str()],
            )?;
        }
        // Auto-inbox re-registration: the guarded path.
        SubscribeKind::AutoInbox => {
            if let Some(ended_at_ms) = tombstone_ended_at(&tx, session)? {
                if now_ms.saturating_sub(ended_at_ms) < SUBSCRIBE_TOMBSTONE_GUARD_MS {
                    // Within the race window: refuse. The tx drops (rolls back) with
                    // nothing written — no subscription row, tombstone left in place.
                    return Ok(SubscribeOutcome::RefusedSessionRecentlyEnded);
                }
                // Aged tombstone: a real restart well after the end. Clear it so this
                // and future subscribes proceed normally (self-healing).
                tx.execute(
                    "DELETE FROM session_tombstone WHERE session_id = ?1",
                    params![session.as_str()],
                )?;
            }
        }
    }

    let changed = tx.execute(
        "INSERT OR IGNORE INTO subscription (session_id, topic) VALUES (?1, ?2)",
        params![session.as_str(), topic.as_str()],
    )?;
    if changed == 0 {
        // Idempotent no-op: leave the cursor exactly where it was.
        tx.commit()?;
        return Ok(SubscribeOutcome::AlreadySubscribed);
    }

    let baseline = match topic_head(&tx, topic)? {
        Some(head) => {
            advance_cursor_tx(&tx, session, topic, head)?;
            Some(sqlite_to_offset(head)?)
        }
        // Empty topic: no head yet. Leaving the cursor unset means the next read
        // starts at Oldest and the first post-subscribe publish (offset 0) is
        // delivered — which is exactly a "published after I subscribed" event.
        //
        // SAFETY (baseline invariant): an absent cursor reading from the oldest
        // event is correct ONLY because the log is append-only. If events could
        // ever be deleted/compacted, "oldest surviving event" would no longer
        // equal "first event after subscribe", and this would replay history to a
        // fresh subscriber. Any future retention/compaction must write an explicit
        // baseline cursor here instead of relying on absence.
        None => None,
    };
    tx.commit()?;

    Ok(SubscribeOutcome::Subscribed { baseline })
}

/// Read a session's unread events across every topic it subscribes to, advancing
/// each topic's cursor to the last event returned.
///
/// # Advance-on-read, no explicit ack (docs/01)
///
/// The agent-facing loop is subscribe → idle → wake → read → react; there is no
/// separate ack step. So this command both returns the unread page AND advances
/// the cursor: an event is "delivered" precisely once, at the moment it is
/// returned. A publish that arrives after this read has committed sits beyond the
/// cursor and is therefore surfaced on the session's NEXT read — not lost, not
/// delivered twice. The delivery cursor is owned exclusively by this path (and
/// the subscribe baseline); see [`advance_cursor_tx`].
///
/// # Per-topic transaction isolation (blast-radius containment)
///
/// Each subscribed topic's read+advance runs in its OWN transaction. That still
/// gives exactly-once — the read and the advance for a topic commit or roll back
/// together — while containing failures: one topic with a corrupt row (an
/// un-parseable topic string, a non-JSON body, a negative offset) does not roll
/// back or starve the session's OTHER topics. A failing topic is logged and
/// skipped; the events successfully read from healthy topics are still returned.
/// Topics are visited in ascending order for a deterministic cross-topic order.
fn do_read_unread(
    conn: &mut Connection,
    session: &SessionId,
    limit: Option<u32>,
) -> Result<Vec<Event>, StorageError> {
    // Clamp the per-topic page to the same hard cap as `do_read_events`; anything
    // beyond it surfaces on the next read via the advanced cursor.
    let limit = limit.unwrap_or(DEFAULT_READ_LIMIT).min(MAX_READ_LIMIT) as i64;

    // Snapshot the subscribed topics up front (ORDER BY for a deterministic
    // cross-topic delivery order). A plain read on the connection; each topic
    // then gets its own transaction below.
    let topics: Vec<String> = {
        let mut stmt = conn
            .prepare("SELECT topic FROM subscription WHERE session_id = ?1 ORDER BY topic ASC")?;
        let rows = stmt.query_map(params![session.as_str()], |row| row.get::<_, String>(0))?;
        rows.collect::<Result<Vec<_>, _>>()?
    };

    let mut delivered: Vec<Event> = Vec::new();
    for topic_str in &topics {
        match read_topic_unread(conn, session, topic_str, limit) {
            Ok(mut events) => delivered.append(&mut events),
            // Contain the blast radius: a single bad topic must not fail the whole
            // read. Log the reason (topic + error, NEVER the body) and move on.
            Err(err) => {
                error!(
                    session = session.as_str(),
                    topic = topic_str.as_str(),
                    error = %err,
                    "skipped a topic during read (its transaction rolled back); other topics unaffected"
                );
            }
        }
    }

    Ok(delivered)
}

/// Read + advance one topic for `session` in a single transaction, returning the
/// events delivered (possibly empty). Isolated per topic so a failure here rolls
/// back only this topic (see [`do_read_unread`]).
fn read_topic_unread(
    conn: &mut Connection,
    session: &SessionId,
    topic_str: &str,
    limit: i64,
) -> Result<Vec<Event>, StorageError> {
    // A subscription row can only hold a topic this bridge accepted, so a value
    // that fails the grammar now is corrupt storage, not user input.
    let topic = Topic::parse(topic_str).map_err(|_| StorageError::Corrupt {
        detail: format!("invalid topic {topic_str:?} stored in subscription"),
    })?;

    let tx = conn.transaction()?;

    let after: i64 = tx
        .query_row(
            "SELECT offset FROM delivery_cursor WHERE session_id = ?1 AND topic = ?2",
            params![session.as_str(), topic_str],
            |row| row.get(0),
        )
        .optional()?
        // No cursor row => never delivered on this topic => read from the oldest
        // event (offsets start at 0, so "> -1" is "everything"). Safe only because
        // the log is append-only — see the baseline note in
        // `do_subscribe_and_baseline`.
        .unwrap_or(-1);

    let events: Vec<Event> = {
        let mut stmt = tx.prepare(
            "SELECT offset, event_id, timestamp, body
             FROM event
             WHERE topic = ?1 AND offset > ?2
             ORDER BY offset ASC
             LIMIT ?3",
        )?;
        let rows = stmt.query_map(params![topic_str, after, limit], |row| {
            let offset: i64 = row.get(0)?;
            let event_id: String = row.get(1)?;
            let timestamp: i64 = row.get(2)?;
            let body_text: String = row.get(3)?;
            Ok((offset, event_id, timestamp, body_text))
        })?;

        let mut out = Vec::new();
        for row in rows {
            let (offset, event_id, timestamp, body_text) = row?;
            let body: Value = serde_json::from_str(&body_text)?;
            out.push(Event {
                id: EventId(event_id),
                offset: sqlite_to_offset(offset)?,
                topic: topic.clone(),
                timestamp: Timestamp(timestamp),
                body,
            });
        }
        out
    };

    if let Some(last) = events.last() {
        let last_offset = offset_to_sqlite(last.offset)?;
        advance_cursor_tx(&tx, session, &topic, last_offset)?;
        info!(
            session = session.as_str(),
            topic = topic.as_str(),
            count = events.len(),
            new_offset = last.offset.0,
            "delivered unread events and advanced cursor"
        );
    }

    tx.commit()?;
    Ok(events)
}

fn do_upsert_watch(conn: &Connection, spec: &WatchSpec) -> Result<WatchId, StorageError> {
    // Idempotent by (kind, repo, pr): a second session watching the same entity
    // reuses the row and its (possibly running) state. The interval and the
    // (stub) publish count are non-identity fields refreshed on a re-watch;
    // lifecycle state is owned by SetWatchState, never reset here. Saturate rather
    // than wrap on the (practically impossible) overflow of an interval, PR
    // number, or count that exceeds i64 — a wrapped negative would be silently
    // wrong, whereas a clamp is at worst a harmless over-large value.
    let interval_ms = i64::try_from(spec.interval.as_millis()).unwrap_or(i64::MAX);
    let pr = i64::try_from(spec.target.pr_column()).unwrap_or(i64::MAX);
    let count = i64::try_from(spec.target.count_column()).unwrap_or(i64::MAX);
    let id: i64 = conn.query_row(
        "INSERT INTO watch (kind, repo, pr, interval_ms, publish_count, state, child_pid)
         VALUES (?1, ?2, ?3, ?4, ?5, 'desired', NULL)
         ON CONFLICT(kind, repo, pr)
         DO UPDATE SET interval_ms = excluded.interval_ms, publish_count = excluded.publish_count
         RETURNING id",
        params![
            spec.target.kind().as_str(),
            spec.target.repo_column(),
            pr,
            interval_ms,
            count
        ],
        |row| row.get(0),
    )?;
    info!(
        watch = id,
        kind = spec.target.kind().as_str(),
        repo = %spec.target.repo_column(),
        pr = spec.target.pr_column(),
        "upserted watch (created or reused existing entity)"
    );
    Ok(WatchId::new(id))
}

fn do_set_watch_state(
    conn: &Connection,
    id: WatchId,
    state: WatchState,
) -> Result<(), StorageError> {
    // The pid column is meaningful only while running; deriving both from the
    // enum keeps DB and model in lockstep (no "running with NULL pid").
    let (state_str, pid): (&str, Option<i64>) = match state {
        WatchState::Desired => ("desired", None),
        WatchState::Running { pid } => ("running", Some(i64::from(pid.get()))),
        WatchState::Stopped => ("stopped", None),
        WatchState::Failed => ("failed", None),
    };
    conn.execute(
        "UPDATE watch SET state = ?1, child_pid = ?2 WHERE id = ?3",
        params![state_str, pid, id.get()],
    )?;
    info!(watch = id.get(), state = state_str, pid = ?pid, "set watch state");
    Ok(())
}

fn do_get_watch(conn: &Connection, id: WatchId) -> Result<Option<Watch>, StorageError> {
    let row = conn
        .query_row(
            "SELECT kind, repo, pr, interval_ms, state, child_pid, publish_count
             FROM watch WHERE id = ?1",
            params![id.get()],
            |row| {
                let kind: String = row.get(0)?;
                let repo: String = row.get(1)?;
                let pr: i64 = row.get(2)?;
                let interval_ms: i64 = row.get(3)?;
                let state: String = row.get(4)?;
                let child_pid: Option<i64> = row.get(5)?;
                let count: i64 = row.get(6)?;
                Ok((kind, repo, pr, interval_ms, state, child_pid, count))
            },
        )
        .optional()?;

    let Some((kind, repo, pr, interval_ms, state, child_pid, count)) = row else {
        return Ok(None);
    };

    Ok(Some(build_watch(
        id,
        kind,
        repo,
        pr,
        interval_ms,
        &state,
        child_pid,
        count,
    )?))
}

/// Enumerate all watches in stable id order (card-06 `status` / `unwatch`).
///
/// Reuses [`build_watch`] so the same corrupt-row guards that protect
/// [`do_get_watch`] apply to every listed row.
fn do_list_watches(conn: &Connection) -> Result<Vec<Watch>, StorageError> {
    let mut stmt = conn.prepare(
        "SELECT id, kind, repo, pr, interval_ms, state, child_pid, publish_count
         FROM watch ORDER BY id ASC",
    )?;
    let rows = stmt.query_map([], |row| {
        let id: i64 = row.get(0)?;
        let kind: String = row.get(1)?;
        let repo: String = row.get(2)?;
        let pr: i64 = row.get(3)?;
        let interval_ms: i64 = row.get(4)?;
        let state: String = row.get(5)?;
        let child_pid: Option<i64> = row.get(6)?;
        let count: i64 = row.get(7)?;
        Ok((id, kind, repo, pr, interval_ms, state, child_pid, count))
    })?;

    let mut watches = Vec::new();
    for row in rows {
        let (id, kind, repo, pr, interval_ms, state, child_pid, count) = row?;
        watches.push(build_watch(
            WatchId::new(id),
            kind,
            repo,
            pr,
            interval_ms,
            &state,
            child_pid,
            count,
        )?);
    }
    Ok(watches)
}

/// Reconstruct a [`Watch`] read model from its stored columns, rejecting any
/// value this store could not legitimately have written as [`StorageError::Corrupt`].
/// Shared by [`do_get_watch`] and [`do_list_watches`] so the guards stay in one place.
#[allow(clippy::too_many_arguments)]
fn build_watch(
    id: WatchId,
    kind: String,
    repo: String,
    pr: i64,
    interval_ms: i64,
    state: &str,
    child_pid: Option<i64>,
    count: i64,
) -> Result<Watch, StorageError> {
    let kind = WatchKind::parse(&kind).ok_or_else(|| StorageError::Corrupt {
        detail: format!("unknown watch kind {kind:?} for watch {}", id.get()),
    })?;
    // These columns can only hold values this store wrote, so a negative pr,
    // interval, or count is corrupt data, not a value to silently wrap.
    let pr = u64::try_from(pr).map_err(|_| StorageError::Corrupt {
        detail: format!("negative pr {pr} for watch {}", id.get()),
    })?;
    let interval_ms = u64::try_from(interval_ms).map_err(|_| StorageError::Corrupt {
        detail: format!("negative interval {interval_ms} for watch {}", id.get()),
    })?;
    let count = u64::try_from(count).map_err(|_| StorageError::Corrupt {
        detail: format!("negative publish count {count} for watch {}", id.get()),
    })?;
    // Parse the flat columns into the sum type here, at the corruption-checking
    // boundary, so "a github watch with a publish count" or "a stub watch with a
    // PR number" are rejected as corrupt and unrepresentable downstream. Only
    // this store writes these rows, and it always writes the unused column as 0.
    let target = match kind {
        WatchKind::GithubPr => {
            if count != 0 {
                return Err(StorageError::Corrupt {
                    detail: format!(
                        "github-pr watch {} carries a non-zero publish count {count}",
                        id.get()
                    ),
                });
            }
            WatchTarget::GithubPr { repo, pr }
        }
        WatchKind::Stub => {
            if pr != 0 {
                return Err(StorageError::Corrupt {
                    detail: format!("stub watch {} carries a non-zero pr {pr}", id.get()),
                });
            }
            WatchTarget::Stub { label: repo, count }
        }
    };
    let state = reconstruct_state(state, child_pid, id)?;

    Ok(Watch {
        id,
        target,
        interval: std::time::Duration::from_millis(interval_ms),
        state,
    })
}

/// Per-topic count of a session's unread events (card-06 `status`).
///
/// Mirrors the unread predicate in `read_topic_unread` / `ReadOnlyStore` — an
/// event is unread when its offset is strictly beyond the session's delivery
/// cursor on that topic (cursor treated as `-1` when no row exists yet). Only
/// topics with a positive count are returned, in ascending topic order for a
/// deterministic status view. This is a pure read: no cursor is advanced.
fn do_unread_counts(
    conn: &Connection,
    session: &SessionId,
) -> Result<Vec<(Topic, u64)>, StorageError> {
    let mut stmt = conn.prepare(
        "SELECT s.topic, COUNT(e.offset)
         FROM subscription s
         JOIN event e ON e.topic = s.topic
            AND e.offset > COALESCE(
                (SELECT dc.offset FROM delivery_cursor dc
                 WHERE dc.session_id = s.session_id AND dc.topic = s.topic),
                -1)
         WHERE s.session_id = ?1
         GROUP BY s.topic
         ORDER BY s.topic ASC",
    )?;
    let rows = stmt.query_map(params![session.as_str()], |row| {
        let topic: String = row.get(0)?;
        let count: i64 = row.get(1)?;
        Ok((topic, count))
    })?;

    let mut out = Vec::new();
    for row in rows {
        let (topic_str, count) = row?;
        // A subscription row can only hold a topic the bridge accepted, so a value
        // that fails the grammar now is corrupt storage, not user input.
        let topic = Topic::parse(&topic_str).map_err(|_| StorageError::Corrupt {
            detail: format!("invalid topic {topic_str:?} stored in subscription"),
        })?;
        out.push((topic, count.max(0) as u64));
    }
    Ok(out)
}

/// The sessions that have REGISTERED an agent inbox (card-16 `agents`).
///
/// A session is registered exactly when it is subscribed to *its own* inbox topic
/// — the thing `harness arm` guarantees on every SessionStart/Stop (ADR-0007).
/// That "its own" test is what makes this an agent list rather than a list of
/// everyone who happens to be listening to an `agent.*` topic: a session may
/// legitimately subscribe to a PEER's inbox (nothing forbids it), and such a
/// subscriber is not itself addressable.
///
/// The ownership test is done in Rust through the one canonical
/// [`inbox_topic`] constructor rather than as SQL string concatenation, so the
/// topic grammar lives in exactly one place and cannot drift into a query. The
/// scan is over all subscription rows, which is bounded by (live sessions ×
/// topics they watch) — tens of rows on a local dev bus, so the simplicity is
/// worth more than an index-friendly `LIKE`.
fn do_list_agent_inboxes(conn: &Connection) -> Result<Vec<SessionId>, StorageError> {
    let mut stmt =
        conn.prepare("SELECT session_id, topic FROM subscription ORDER BY session_id ASC")?;
    let rows = stmt.query_map([], |row| {
        let session: String = row.get(0)?;
        let topic: String = row.get(1)?;
        Ok((session, topic))
    })?;

    let mut agents = Vec::new();
    for row in rows {
        let (session, topic) = row?;
        let session = SessionId::new(session);
        // A session id that cannot form an inbox topic simply has no inbox; it is
        // not corrupt storage (the id is an opaque harness label), so skip it.
        if let Ok(inbox) = inbox_topic(&session)
            && inbox.as_str() == topic
        {
            agents.push(session);
        }
    }
    Ok(agents)
}

/// Every topic the bridge knows about, with its subscriber count, event count,
/// and newest-event timestamp (card-16 `topics`).
///
/// A topic is not a table: it exists because something subscribed to it or
/// published to it. So the row set is the UNION of both tables' topics — a topic
/// with subscribers but no traffic yet (the common case for a fresh inbox) is
/// listed just as honestly as one with events and no listeners.
///
/// `prefix` filters in Rust rather than with SQL `LIKE`, which would need escaping
/// for `%`/`_` in a user-supplied prefix; the distinct-topic count is small, so the
/// unescapable, obviously-correct filter wins.
fn do_list_topics(
    conn: &Connection,
    prefix: Option<&str>,
) -> Result<Vec<TopicSummary>, StorageError> {
    let mut stmt = conn.prepare(
        "SELECT t.topic,
                (SELECT COUNT(*) FROM subscription s WHERE s.topic = t.topic),
                (SELECT COUNT(*) FROM event e WHERE e.topic = t.topic),
                (SELECT MAX(e.timestamp) FROM event e WHERE e.topic = t.topic)
         FROM (SELECT topic FROM event UNION SELECT topic FROM subscription) t
         ORDER BY t.topic ASC",
    )?;
    let rows = stmt.query_map([], |row| {
        let topic: String = row.get(0)?;
        let subscribers: i64 = row.get(1)?;
        let events: i64 = row.get(2)?;
        let last_event: Option<i64> = row.get(3)?;
        Ok((topic, subscribers, events, last_event))
    })?;

    let mut out = Vec::new();
    for row in rows {
        let (topic_str, subscribers, events, last_event) = row?;
        if let Some(prefix) = prefix
            && !topic_str.starts_with(prefix)
        {
            continue;
        }
        // Both tables can only hold a topic this bridge accepted, so a value that
        // fails the grammar now is corrupt storage, not user input.
        let topic = Topic::parse(&topic_str).map_err(|_| StorageError::Corrupt {
            detail: format!("invalid topic {topic_str:?} stored"),
        })?;
        out.push(TopicSummary {
            topic,
            subscribers: subscribers.max(0) as u64,
            events: events.max(0) as u64,
            last_event: last_event.map(Timestamp),
        });
    }
    Ok(out)
}

/// Rebuild the [`WatchState`] enum from its stored `(state, child_pid)` pair.
///
/// Only [`do_set_watch_state`] writes these, and it pairs a pid with `running`
/// and only `running`. So each of these is genuinely bad data and surfaces as
/// `Corrupt` rather than being silently coerced: an unknown state string, a
/// `running` row without a pid, a pid that does not fit an `i32`, or a
/// non-`running` row that nonetheless carries a pid.
fn reconstruct_state(
    state: &str,
    child_pid: Option<i64>,
    id: WatchId,
) -> Result<WatchState, StorageError> {
    match state {
        "desired" => no_pid(WatchState::Desired, child_pid, state, id),
        "stopped" => no_pid(WatchState::Stopped, child_pid, state, id),
        "failed" => no_pid(WatchState::Failed, child_pid, state, id),
        "running" => match child_pid {
            Some(pid) => {
                let pid = u32::try_from(pid).map_err(|_| StorageError::Corrupt {
                    detail: format!("watch {} child pid {pid} is not a valid u32", id.get()),
                })?;
                Ok(WatchState::Running { pid: Pid::new(pid) })
            }
            None => Err(StorageError::Corrupt {
                detail: format!("watch {} is running with no child pid", id.get()),
            }),
        },
        other => Err(StorageError::Corrupt {
            detail: format!("unknown watch state {other:?} for watch {}", id.get()),
        }),
    }
}

/// A non-`running` state must not carry a pid. Enforce that symmetry (only the
/// running path legitimately stores one).
fn no_pid(
    reconstructed: WatchState,
    child_pid: Option<i64>,
    state: &str,
    id: WatchId,
) -> Result<WatchState, StorageError> {
    match child_pid {
        None => Ok(reconstructed),
        Some(pid) => Err(StorageError::Corrupt {
            detail: format!(
                "watch {} is {state} but carries a child pid {pid}",
                id.get()
            ),
        }),
    }
}

fn do_add_interest(
    conn: &Connection,
    watch: WatchId,
    session: &SessionId,
    last_seen: i64,
) -> Result<u64, StorageError> {
    // Idempotent per session: re-watching the same PR must not double-count. It
    // DOES refresh `last_seen` (a re-watch is a fresh liveness signal), so a
    // session that re-declares interest resets its sweep clock.
    conn.execute(
        "INSERT INTO watch_interest (watch_id, session_id, last_seen) VALUES (?1, ?2, ?3)
         ON CONFLICT(watch_id, session_id) DO UPDATE SET last_seen = excluded.last_seen",
        params![watch.get(), session.as_str(), last_seen],
    )?;
    let count = interest_count(conn, watch)?;
    info!(
        watch = watch.get(),
        interest = count,
        "attached session interest"
    );
    Ok(count)
}

/// Refresh an existing interest's `last_seen`. A no-op (zero rows) if the session
/// is not interested — the heartbeat must not resurrect a dropped interest.
fn do_touch_interest(
    conn: &Connection,
    watch: WatchId,
    session: &SessionId,
    last_seen: i64,
) -> Result<(), StorageError> {
    conn.execute(
        "UPDATE watch_interest SET last_seen = ?3 WHERE watch_id = ?1 AND session_id = ?2",
        params![watch.get(), session.as_str(), last_seen],
    )?;
    Ok(())
}

/// Refresh every interest held by `session`, returning how many were refreshed.
/// The daemon's liveness heartbeat (ADR-0009): the sweeper probes each session's
/// waiter pidfile and calls this for the ones it finds alive, so `last_seen`
/// means "when the daemon last had evidence this session existed" rather than
/// "when the session last spoke to us". Like [`do_touch_interest`] it only ever
/// UPDATEs — a heartbeat must not resurrect a dropped interest.
fn do_touch_session_interests(
    conn: &Connection,
    session: &SessionId,
    last_seen: i64,
) -> Result<u64, StorageError> {
    let refreshed = conn.execute(
        "UPDATE watch_interest SET last_seen = ?2 WHERE session_id = ?1",
        params![session.as_str(), last_seen],
    )?;
    Ok(refreshed as u64)
}

/// Every distinct session holding at least one interest. The sweeper probes these
/// for liveness; a session with no interests needs no probe.
fn do_list_interest_sessions(conn: &Connection) -> Result<Vec<SessionId>, StorageError> {
    let mut stmt = conn.prepare("SELECT DISTINCT session_id FROM watch_interest")?;
    let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
    Ok(rows
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .map(SessionId::new)
        .collect())
}

/// The sessions holding an interest in one watch. The startup reconcile probes
/// these for liveness to decide whether to resume that watch's adapter.
fn do_list_watch_interest_sessions(
    conn: &Connection,
    watch: WatchId,
) -> Result<Vec<SessionId>, StorageError> {
    let mut stmt = conn.prepare("SELECT session_id FROM watch_interest WHERE watch_id = ?1")?;
    let rows = stmt.query_map([watch.get()], |row| row.get::<_, String>(0))?;
    Ok(rows
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .map(SessionId::new)
        .collect())
}

/// Drop every interest older than `cutoff`, returning the watches whose interest
/// thereby fell to zero. One transaction so the "who reached zero" answer is
/// consistent with the deletion that caused it.
fn do_sweep_stale_interests(
    conn: &mut Connection,
    cutoff: i64,
) -> Result<Vec<WatchId>, StorageError> {
    let tx = conn.transaction()?;

    // The watches that had at least one stale interest — only these can have
    // reached zero, so we recount just them rather than every watch.
    let affected: Vec<i64> = {
        let mut stmt =
            tx.prepare("SELECT DISTINCT watch_id FROM watch_interest WHERE last_seen < ?1")?;
        let rows = stmt.query_map(params![cutoff], |row| row.get::<_, i64>(0))?;
        rows.collect::<Result<Vec<_>, _>>()?
    };

    let removed = tx.execute(
        "DELETE FROM watch_interest WHERE last_seen < ?1",
        params![cutoff],
    )?;

    let mut emptied = Vec::new();
    for watch_id in affected {
        let count: i64 = tx.query_row(
            "SELECT COUNT(*) FROM watch_interest WHERE watch_id = ?1",
            params![watch_id],
            |row| row.get(0),
        )?;
        if count == 0 {
            emptied.push(WatchId::new(watch_id));
        }
    }
    tx.commit()?;

    if removed > 0 {
        info!(
            removed,
            emptied = emptied.len(),
            "swept stale watch interests"
        );
    }
    Ok(emptied)
}

fn do_remove_interest(
    conn: &Connection,
    watch: WatchId,
    session: &SessionId,
) -> Result<u64, StorageError> {
    conn.execute(
        "DELETE FROM watch_interest WHERE watch_id = ?1 AND session_id = ?2",
        params![watch.get(), session.as_str()],
    )?;
    let count = interest_count(conn, watch)?;
    if count == 0 {
        // The refcount signal that authorizes teardown; the caller stops the
        // adapter when interest hits zero (design/01 rule 5).
        warn!(
            watch = watch.get(),
            "watch has no remaining interested sessions"
        );
    } else {
        info!(
            watch = watch.get(),
            interest = count,
            "removed session interest"
        );
    }
    Ok(count)
}

fn do_interest_count(conn: &Connection, watch: WatchId) -> Result<u64, StorageError> {
    interest_count(conn, watch)
}

fn interest_count(conn: &Connection, watch: WatchId) -> Result<u64, StorageError> {
    let count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM watch_interest WHERE watch_id = ?1",
        params![watch.get()],
        |row| row.get(0),
    )?;
    Ok(count as u64)
}

fn do_get_baseline(conn: &Connection, watch: WatchId) -> Result<Option<Value>, StorageError> {
    let text: Option<String> = conn
        .query_row(
            "SELECT baseline FROM adapter_baseline WHERE watch_id = ?1",
            params![watch.get()],
            |row| row.get(0),
        )
        .optional()?;
    match text {
        Some(text) => Ok(Some(serde_json::from_str(&text)?)),
        None => Ok(None),
    }
}

fn do_set_baseline(conn: &Connection, watch: WatchId, baseline: Value) -> Result<(), StorageError> {
    let text = serde_json::to_string(&baseline)?;
    conn.execute(
        "INSERT INTO adapter_baseline (watch_id, baseline) VALUES (?1, ?2)
         ON CONFLICT(watch_id) DO UPDATE SET baseline = excluded.baseline",
        params![watch.get(), text],
    )?;
    Ok(())
}

fn do_integrity_check(conn: &Connection) -> Result<(), StorageError> {
    // `PRAGMA integrity_check` returns a single row "ok" on a healthy DB, or one
    // row per problem otherwise.
    let result: String = conn.query_row("PRAGMA integrity_check", [], |row| row.get(0))?;
    if result == "ok" {
        Ok(())
    } else {
        Err(StorageError::Corrupt { detail: result })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn migrated() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        crate::storage::schema::migrate(&conn).unwrap();
        conn
    }

    /// An EXPLICIT subscribe (the common case in these unit tests): unguarded, and
    /// clears any tombstone. Thin wrapper so the many call sites stay readable and
    /// only the guard-specific tests spell out [`SubscribeKind::AutoInbox`].
    fn subscribe_explicit(
        conn: &mut Connection,
        session: &SessionId,
        topic: &Topic,
        now_ms: i64,
    ) -> Result<SubscribeOutcome, StorageError> {
        do_subscribe_and_baseline(conn, session, topic, now_ms, SubscribeKind::Explicit)
    }

    /// The guarded AUTO-INBOX subscribe (the `harness arm` re-registration path).
    fn subscribe_auto_inbox(
        conn: &mut Connection,
        session: &SessionId,
        topic: &Topic,
        now_ms: i64,
    ) -> Result<SubscribeOutcome, StorageError> {
        do_subscribe_and_baseline(conn, session, topic, now_ms, SubscribeKind::AutoInbox)
    }

    #[test]
    fn reconstruct_state_rejects_unknown_state() {
        let err = reconstruct_state("weird", None, WatchId::new(1)).unwrap_err();
        assert!(matches!(err, StorageError::Corrupt { .. }));
    }

    #[test]
    fn reconstruct_state_rejects_running_without_pid() {
        let err = reconstruct_state("running", None, WatchId::new(1)).unwrap_err();
        assert!(matches!(err, StorageError::Corrupt { .. }));
    }

    #[test]
    fn reconstruct_state_rejects_pid_on_non_running_state() {
        // A pid on a desired/stopped row is an inconsistency, not a value to
        // silently drop.
        for state in ["desired", "stopped"] {
            let err = reconstruct_state(state, Some(1234), WatchId::new(1)).unwrap_err();
            assert!(matches!(err, StorageError::Corrupt { .. }), "state {state}");
        }
    }

    #[test]
    fn get_watch_rejects_unknown_kind() {
        let conn = migrated();
        // Insert a row with a kind string the enum does not know.
        conn.execute(
            "INSERT INTO watch (id, kind, repo, pr, interval_ms, publish_count, state, child_pid)
             VALUES (1, 'bogus-kind', 'o/r', 1, 60000, 0, 'desired', NULL)",
            [],
        )
        .unwrap();
        let err = do_get_watch(&conn, WatchId::new(1)).unwrap_err();
        assert!(matches!(err, StorageError::Corrupt { .. }));
    }

    #[test]
    fn stub_watch_round_trips_subsecond_interval_and_count() {
        let conn = migrated();
        // A sub-second interval must survive (the whole reason interval is stored
        // in ms), and the publish count round-trips on the shared row.
        let id = do_upsert_watch(
            &conn,
            &WatchSpec {
                target: WatchTarget::Stub {
                    label: "demo".to_string(),
                    count: 5,
                },
                interval: std::time::Duration::from_millis(200),
            },
        )
        .unwrap();
        let watch = do_get_watch(&conn, id).unwrap().unwrap();
        assert_eq!(
            watch.target,
            WatchTarget::Stub {
                label: "demo".to_string(),
                count: 5
            }
        );
        assert_eq!(watch.interval, std::time::Duration::from_millis(200));

        // Re-watching the same label updates interval/count in place (one entity).
        let again = do_upsert_watch(
            &conn,
            &WatchSpec {
                target: WatchTarget::Stub {
                    label: "demo".to_string(),
                    count: 0,
                },
                interval: std::time::Duration::from_millis(750),
            },
        )
        .unwrap();
        assert_eq!(again, id, "same (kind, repo, pr) reuses the row");
        let watch = do_get_watch(&conn, id).unwrap().unwrap();
        assert_eq!(watch.interval, std::time::Duration::from_millis(750));
        assert_eq!(watch.target.count_column(), 0);
    }

    #[test]
    fn get_watch_rejects_negative_pr() {
        let conn = migrated();
        conn.execute(
            "INSERT INTO watch (id, kind, repo, pr, interval_ms, publish_count, state, child_pid)
             VALUES (1, 'github-pr', 'o/r', -5, 60000, 0, 'desired', NULL)",
            [],
        )
        .unwrap();
        let err = do_get_watch(&conn, WatchId::new(1)).unwrap_err();
        assert!(matches!(err, StorageError::Corrupt { .. }));
    }

    /// Insert one event at `(topic, offset)` with a trivial body. Test-only.
    fn insert_event(conn: &Connection, topic: &str, offset: i64) {
        conn.execute(
            "INSERT INTO event (topic, offset, event_id, adapter, timestamp, body)
             VALUES (?1, ?2, ?3, 'a', 0, '{}')",
            params![topic, offset, format!("evt-{topic}-{offset}")],
        )
        .unwrap();
    }

    // ---- the caller-aware publish (`do_publish_as_session`) ----------------------
    //
    // These sit beside the other writer tests on purpose: the rules are decided in ONE
    // SQL transaction, and they were previously only covered by heavyweight process-level
    // e2e (an inverted pyramid — slow, and it could not reach the interesting states).

    /// A session publish, at the writer level.
    fn publish_as(
        conn: &mut Connection,
        session: &str,
        topic: &Topic,
        body: &str,
    ) -> PublishAttempt {
        do_publish_as_session(
            conn,
            topic,
            &AdapterId("cli".to_string()),
            Timestamp(1),
            serde_json::from_str(body).unwrap(),
            &SessionId::new(session),
        )
        .unwrap()
    }

    /// The unread count as `status` (and the refusal message) reports it: EVERY event
    /// beyond the cursor, whoever wrote it.
    fn unread_of(conn: &Connection, session: &str, topic: &Topic) -> u64 {
        do_unread_counts(conn, &SessionId::new(session))
            .unwrap()
            .into_iter()
            .find(|(t, _)| t == topic)
            .map(|(_, n)| n)
            .unwrap_or(0)
    }

    /// Be caught up to speak: unread mail from SOMEONE ELSE refuses the publish, and the
    /// refusal writes nothing at all (it is one transaction, and it rolls back).
    #[test]
    fn publish_as_session_is_refused_when_others_events_are_unread_and_writes_nothing() {
        let mut conn = migrated();
        let topic = Topic::parse("t.rules").unwrap();
        subscribe_explicit(&mut conn, &SessionId::new("s"), &topic, 1).unwrap();

        // A peer speaks (an adapter would do just as well — neither is `s`).
        publish_as(&mut conn, "peer", &topic, r#"{"n":1}"#);
        assert_eq!(unread_of(&conn, "s", &topic), 1);

        let attempt = publish_as(&mut conn, "s", &topic, r#"{"n":2}"#);
        assert_eq!(attempt, PublishAttempt::RefusedUnread { unread: 1 });
        let events: i64 = conn
            .query_row("SELECT COUNT(*) FROM event", [], |r| r.get(0))
            .unwrap();
        assert_eq!(events, 1, "a refused publish must append nothing");

        // Reading clears it, and the same publish then goes through.
        do_read_unread(&mut conn, &SessionId::new("s"), None).unwrap();
        assert!(matches!(
            publish_as(&mut conn, "s", &topic, r#"{"n":2}"#),
            PublishAttempt::Published(_)
        ));
    }

    /// Your OWN events do not gag you — but nothing marks them read for you.
    ///
    /// This is the adv-2 fix in one test: the rule counts only events you did not
    /// author (so your own message never blocks your next publish), and NO cursor is
    /// advanced (so your own message stays visible and countable). The old design
    /// advanced the publisher's cursor, which silently consumed any event mis-attributed
    /// to it via the ambient `$CLAUDE_CODE_SESSION_ID` — real message loss.
    #[test]
    fn publish_as_session_ignores_your_own_events_but_never_marks_them_read() {
        let mut conn = migrated();
        let topic = Topic::parse("t.own").unwrap();
        let session = SessionId::new("s");
        subscribe_explicit(&mut conn, &session, &topic, 1).unwrap();

        for n in 0..3 {
            assert!(
                matches!(
                    publish_as(&mut conn, "s", &topic, r#"{"n":1}"#),
                    PublishAttempt::Published(_)
                ),
                "publish {n}: your own last message must never block your next one"
            );
        }

        assert_eq!(
            unread_of(&conn, "s", &topic),
            3,
            "your own events are NOT hidden from you: they remain unread until you read them"
        );
        let cursor: Option<i64> = conn
            .query_row(
                "SELECT offset FROM delivery_cursor WHERE session_id = 's' AND topic = 't.own'",
                [],
                |r| r.get(0),
            )
            .optional()
            .unwrap();
        assert_eq!(
            cursor, None,
            "publishing must not advance the publisher's cursor — only a `read` may mark mail read"
        );

        // And they are genuinely deliverable to it.
        let page = do_read_unread(&mut conn, &session, None).unwrap();
        assert_eq!(page.len(), 3);
    }

    /// An event is stamped with its author, and ONLY on the session path: an adapter
    /// publish is anonymous (NULL), which is what makes it wake everyone.
    #[test]
    fn the_author_is_stamped_only_on_a_session_publish() {
        let mut conn = migrated();
        let topic = Topic::parse("t.stamp").unwrap();

        publish_as(&mut conn, "s", &topic, "{}");
        do_publish(
            &mut conn,
            &topic,
            &AdapterId("github-pr".to_string()),
            Timestamp(2),
            serde_json::json!({}),
        )
        .unwrap();

        let authors: Vec<Option<String>> = conn
            .prepare("SELECT author_session FROM event ORDER BY offset")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert_eq!(authors, vec![Some("s".to_string()), None]);
    }

    /// A session that does not SUBSCRIBE to the topic it publishes to has no cursor and
    /// no unread there, so the rule cannot bite it — however far behind it is elsewhere.
    /// (This is the `send`-to-a-peer's-inbox shape.)
    #[test]
    fn publish_as_session_never_refuses_a_topic_you_do_not_subscribe_to() {
        let mut conn = migrated();
        let mine = Topic::parse("t.mine").unwrap();
        let theirs = Topic::parse("t.theirs").unwrap();
        subscribe_explicit(&mut conn, &SessionId::new("s"), &mine, 1).unwrap();

        // Hopelessly behind on my own topic...
        publish_as(&mut conn, "peer", &mine, "{}");
        assert_eq!(unread_of(&conn, "s", &mine), 1);

        // ...but a topic I do not subscribe to is not mine to be caught up on.
        assert!(matches!(
            publish_as(&mut conn, "s", &theirs, "{}"),
            PublishAttempt::Published(_)
        ));
    }

    #[test]
    fn offset_conversions_reject_out_of_range() {
        // The load-bearing integer-world crossing: a u64 above i64::MAX cannot be
        // stored, and a negative i64 read back is corrupt (would make `> ?` match
        // the whole log). These guards are what the cursor code relies on.
        let over = offset_to_sqlite(Offset(u64::MAX)).unwrap_err();
        assert!(matches!(over, StorageError::OffsetOutOfRange { offset } if offset == u64::MAX));
        let neg = sqlite_to_offset(-1).unwrap_err();
        assert!(matches!(neg, StorageError::Corrupt { .. }));
    }

    #[test]
    fn sessions_subscribed_is_empty_for_unsubscribed_topic() {
        let mut conn = migrated();
        let topic = Topic::parse("t.sub.x").unwrap();
        // A topic nobody subscribed to yields no sessions (a zero-subscriber
        // publish then kicks no one).
        assert!(do_sessions_subscribed(&conn, &topic).unwrap().is_empty());

        // Subscribing a different topic must not leak into this one.
        subscribe_explicit(
            &mut conn,
            &SessionId::new("s"),
            &Topic::parse("t.other").unwrap(),
            1_000_000,
        )
        .unwrap();
        assert!(do_sessions_subscribed(&conn, &topic).unwrap().is_empty());
    }

    #[test]
    fn sessions_subscribed_returns_all_subscribers() {
        let mut conn = migrated();
        let topic = Topic::parse("t.sub.y").unwrap();
        subscribe_explicit(&mut conn, &SessionId::new("alice"), &topic, 1_000_000).unwrap();
        subscribe_explicit(&mut conn, &SessionId::new("bob"), &topic, 1_000_000).unwrap();

        let mut got: Vec<String> = do_sessions_subscribed(&conn, &topic)
            .unwrap()
            .iter()
            .map(|s| s.as_str().to_string())
            .collect();
        got.sort();
        assert_eq!(got, ["alice", "bob"]);
    }

    #[test]
    fn session_subscriptions_lists_topics_in_order() {
        let mut conn = migrated();
        let session = SessionId::new("s");
        assert!(
            do_session_subscriptions(&conn, &session)
                .unwrap()
                .is_empty()
        );
        subscribe_explicit(
            &mut conn,
            &session,
            &Topic::parse("t.b").unwrap(),
            1_000_000,
        )
        .unwrap();
        subscribe_explicit(
            &mut conn,
            &session,
            &Topic::parse("t.a").unwrap(),
            1_000_000,
        )
        .unwrap();
        // Another session's subscription must not leak into this one's list.
        subscribe_explicit(
            &mut conn,
            &SessionId::new("other"),
            &Topic::parse("t.z").unwrap(),
            1_000_000,
        )
        .unwrap();
        let topics: Vec<String> = do_session_subscriptions(&conn, &session)
            .unwrap()
            .iter()
            .map(|t| t.as_str().to_string())
            .collect();
        assert_eq!(topics, ["t.a", "t.b"]);
    }

    #[test]
    fn end_session_drops_subscriptions_and_interests_and_reports_emptied() {
        let mut conn = migrated();
        let session = SessionId::new("leaver");
        let other = SessionId::new("stayer");

        // Two watches: one only `leaver` cares about, one both care about.
        let solo = do_upsert_watch(
            &conn,
            &WatchSpec {
                target: WatchTarget::Stub {
                    label: "solo".to_string(),
                    count: 0,
                },
                interval: std::time::Duration::from_secs(1),
            },
        )
        .unwrap();
        let shared = do_upsert_watch(
            &conn,
            &WatchSpec {
                target: WatchTarget::Stub {
                    label: "shared".to_string(),
                    count: 0,
                },
                interval: std::time::Duration::from_secs(1),
            },
        )
        .unwrap();
        do_add_interest(&conn, solo, &session, 0).unwrap();
        do_add_interest(&conn, shared, &session, 0).unwrap();
        do_add_interest(&conn, shared, &other, 0).unwrap();
        subscribe_explicit(
            &mut conn,
            &session,
            &Topic::parse("t.x").unwrap(),
            1_000_000,
        )
        .unwrap();
        subscribe_explicit(
            &mut conn,
            &session,
            &Topic::parse("t.y").unwrap(),
            1_000_000,
        )
        .unwrap();

        let outcome = do_end_session(&mut conn, &session, 1_000_000).unwrap();
        assert_eq!(outcome.subscriptions_removed, 2);
        assert_eq!(outcome.interests_removed, 2);
        // Only the solo watch reached zero interest; the shared one still has `other`.
        assert_eq!(outcome.emptied_watches, vec![solo]);

        // The session is fully gone; the other session's interest is untouched.
        assert!(
            do_session_subscriptions(&conn, &session)
                .unwrap()
                .is_empty()
        );
        assert_eq!(do_interest_count(&conn, shared).unwrap(), 1);
        assert_eq!(do_interest_count(&conn, solo).unwrap(), 0);
    }

    #[test]
    fn end_session_on_unknown_session_is_a_clean_noop() {
        let mut conn = migrated();
        let outcome = do_end_session(&mut conn, &SessionId::new("ghost"), 1_000_000).unwrap();
        assert_eq!(outcome, EndSessionOutcome::default());
    }

    /// Raw tombstone read for tests (the helper takes a `Transaction`).
    fn tombstone_row(conn: &Connection, session: &str) -> Option<i64> {
        conn.query_row(
            "SELECT ended_at_ms FROM session_tombstone WHERE session_id = ?1",
            params![session],
            |row| row.get(0),
        )
        .optional()
        .unwrap()
    }

    #[test]
    fn end_session_writes_a_tombstone_at_the_given_instant() {
        let mut conn = migrated();
        let session = SessionId::new("s-tomb");
        do_end_session(&mut conn, &session, 42_000).unwrap();
        assert_eq!(tombstone_row(&conn, "s-tomb"), Some(42_000));
    }

    #[test]
    fn subscribe_within_guard_after_end_is_refused() {
        // The arm-vs-cleanup race: EndSession commits, then a racing arm auto-inbox
        // re-registration lands a beat later — inside the guard window. It must be
        // REFUSED, not resurrect the dead session's inbox. Only the AutoInbox path is
        // guarded, so this is the path the doomed arm actually uses.
        let mut conn = migrated();
        let session = SessionId::new("s-race");
        let topic = inbox_topic(&session).unwrap();
        subscribe_auto_inbox(&mut conn, &session, &topic, 1_000).unwrap();
        do_end_session(&mut conn, &session, 10_000).unwrap();
        let outcome = subscribe_auto_inbox(&mut conn, &session, &topic, 10_500).unwrap();
        assert_eq!(outcome, SubscribeOutcome::RefusedSessionRecentlyEnded);
        // Nothing was resurrected: no subscription row, so `agents` lists nobody.
        assert!(
            do_session_subscriptions(&conn, &session)
                .unwrap()
                .is_empty()
        );
        assert!(do_list_agent_inboxes(&conn).unwrap().is_empty());
        // The tombstone is left in place (a later within-window subscribe is still refused).
        assert_eq!(tombstone_row(&conn, "s-race"), Some(10_000));
    }

    #[test]
    fn subscribe_after_aged_tombstone_succeeds_and_clears_it() {
        // A genuine resume of the same id, well after the end, arriving on the
        // guarded AUTO-INBOX path: the stale tombstone is aged out, the subscribe
        // proceeds, and the tombstone is cleared so it cannot linger and refuse a
        // future arm.
        let mut conn = migrated();
        let session = SessionId::new("s-resume");
        let topic = inbox_topic(&session).unwrap();
        do_end_session(&mut conn, &session, 0).unwrap();
        let now = SUBSCRIBE_TOMBSTONE_GUARD_MS + 1;
        let outcome = subscribe_auto_inbox(&mut conn, &session, &topic, now).unwrap();
        assert!(matches!(outcome, SubscribeOutcome::Subscribed { .. }));
        assert_eq!(do_list_agent_inboxes(&conn).unwrap(), vec![session.clone()]);
        assert_eq!(
            tombstone_row(&conn, "s-resume"),
            None,
            "aged tombstone cleared"
        );
    }

    #[test]
    fn explicit_subscribe_within_guard_proceeds_and_clears_the_tombstone() {
        // The fix: an EXPLICIT subscribe by a genuinely-resumed session, arriving
        // WITHIN the guard window, must NOT be refused — it comes from a live turn,
        // so it cannot be the doomed post-teardown arm. It proceeds and clears the
        // tombstone, restoring a healthy inbox. (The AutoInbox path in the sibling
        // test is still refused in the same window.)
        let mut conn = migrated();
        let session = SessionId::new("s-explicit-resume");
        let topic = inbox_topic(&session).unwrap();
        subscribe_auto_inbox(&mut conn, &session, &topic, 1_000).unwrap();
        do_end_session(&mut conn, &session, 10_000).unwrap();
        assert_eq!(tombstone_row(&conn, "s-explicit-resume"), Some(10_000));

        // Well inside the 10s window (500ms after the end):
        let outcome = subscribe_explicit(&mut conn, &session, &topic, 10_500).unwrap();
        assert!(
            matches!(outcome, SubscribeOutcome::Subscribed { .. }),
            "an explicit subscribe within the window proceeds, not refused: {outcome:?}"
        );
        assert_eq!(
            do_list_agent_inboxes(&conn).unwrap(),
            vec![session.clone()],
            "the resumed session is addressable again"
        );
        assert_eq!(
            tombstone_row(&conn, "s-explicit-resume"),
            None,
            "an explicit subscribe clears the tombstone (proof-of-life)"
        );
    }

    #[test]
    fn both_arm_cleanup_orderings_leave_no_resurrected_inbox() {
        // Order A: cleanup EndSession commits first, then the racing arm auto-inbox
        // re-registration (the guarded path).
        {
            let mut conn = migrated();
            let s = SessionId::new("s-order-a");
            let t = inbox_topic(&s).unwrap();
            subscribe_auto_inbox(&mut conn, &s, &t, 1_000).unwrap();
            do_end_session(&mut conn, &s, 2_000).unwrap();
            let out = subscribe_auto_inbox(&mut conn, &s, &t, 2_050).unwrap();
            assert_eq!(out, SubscribeOutcome::RefusedSessionRecentlyEnded);
            assert!(do_list_agent_inboxes(&conn).unwrap().is_empty());
        }
        // Order B: the arm Subscribe commits first, then cleanup EndSession.
        {
            let mut conn = migrated();
            let s = SessionId::new("s-order-b");
            let t = inbox_topic(&s).unwrap();
            subscribe_auto_inbox(&mut conn, &s, &t, 1_000).unwrap();
            do_end_session(&mut conn, &s, 2_000).unwrap();
            assert!(
                do_session_subscriptions(&conn, &s).unwrap().is_empty(),
                "the end deleted the subscription"
            );
            assert!(do_list_agent_inboxes(&conn).unwrap().is_empty());
        }
    }

    #[test]
    fn list_topics_includes_a_topic_with_events_but_no_subscribers() {
        // The currently-untested half of the UNION: a topic that has EVENTS but no
        // current subscribers (all ended) must still appear, with subscribers=0 and
        // the right event count/last-event stamp.
        let conn = migrated();
        insert_event(&conn, "t.orphan", 0);
        insert_event(&conn, "t.orphan", 1);
        let topics = do_list_topics(&conn, None).unwrap();
        let orphan = topics
            .iter()
            .find(|t| t.topic.as_str() == "t.orphan")
            .expect("a topic with events but no subscribers must be listed");
        assert_eq!(orphan.subscribers, 0);
        assert_eq!(orphan.events, 2);
        assert!(
            orphan.last_event.is_some(),
            "an event-bearing topic has a last-event stamp"
        );
    }

    #[test]
    fn list_agent_inboxes_filters_by_ownership() {
        // `agents` lists sessions subscribed to their OWN inbox. A session
        // subscribed to a PEER's inbox is a legitimate listener but not itself an
        // agent, so it must be excluded (the ownership test done in Rust via the
        // canonical `inbox_topic`).
        let mut conn = migrated();
        let a = SessionId::new("s-a");
        let b = SessionId::new("s-b");
        subscribe_explicit(&mut conn, &a, &inbox_topic(&a).unwrap(), 1_000_000).unwrap();
        subscribe_explicit(&mut conn, &b, &inbox_topic(&b).unwrap(), 1_000_000).unwrap();
        // The lurker subscribes to a's inbox, not its own.
        let lurker = SessionId::new("s-lurker");
        subscribe_explicit(&mut conn, &lurker, &inbox_topic(&a).unwrap(), 1_000_000).unwrap();

        let ids: Vec<String> = do_list_agent_inboxes(&conn)
            .unwrap()
            .iter()
            .map(|s| s.as_str().to_string())
            .collect();
        assert_eq!(
            ids,
            ["s-a", "s-b"],
            "only self-subscribers are agents; the peer-inbox lurker is excluded"
        );
    }

    #[test]
    fn advance_cursor_tx_is_monotonic() {
        // A lower advance can never rewind delivery, even within one transaction.
        let mut conn = migrated();
        let session = SessionId::new("s".to_string());
        let topic = Topic::parse("t.test.x").unwrap();
        {
            let tx = conn.transaction().unwrap();
            advance_cursor_tx(&tx, &session, &topic, 5).unwrap();
            advance_cursor_tx(&tx, &session, &topic, 2).unwrap();
            tx.commit().unwrap();
        }
        assert_eq!(
            do_get_cursor(&conn, &session, &topic).unwrap(),
            Some(Offset(5))
        );
    }

    #[test]
    fn subscribe_and_baseline_idempotent_leaves_cursor_untouched() {
        let mut conn = migrated();
        let session = SessionId::new("s".to_string());
        let topic = Topic::parse("t.test.x").unwrap();
        insert_event(&conn, "t.test.x", 0);
        insert_event(&conn, "t.test.x", 1);

        // First subscribe baselines to the head (offset 1): no replay of 0/1.
        let first = subscribe_explicit(&mut conn, &session, &topic, 1_000_000).unwrap();
        assert_eq!(
            first,
            SubscribeOutcome::Subscribed {
                baseline: Some(Offset(1))
            }
        );
        assert_eq!(
            do_get_cursor(&conn, &session, &topic).unwrap(),
            Some(Offset(1))
        );

        // A publish after subscribe, still UNREAD by this session.
        insert_event(&conn, "t.test.x", 2);

        // Re-subscribing while still subscribed is a no-op that must NOT advance
        // the cursor past the unread event 2.
        let again = subscribe_explicit(&mut conn, &session, &topic, 1_000_000).unwrap();
        assert_eq!(again, SubscribeOutcome::AlreadySubscribed);
        assert_eq!(
            do_get_cursor(&conn, &session, &topic).unwrap(),
            Some(Offset(1)),
            "idempotent subscribe must leave the cursor untouched"
        );
    }

    #[test]
    fn subscribe_and_baseline_on_empty_topic_sets_no_cursor() {
        let mut conn = migrated();
        let session = SessionId::new("s".to_string());
        let topic = Topic::parse("t.empty.x").unwrap();

        let outcome = subscribe_explicit(&mut conn, &session, &topic, 1_000_000).unwrap();
        // No head to baseline to; the cursor stays absent so the first future
        // publish (offset 0) is still delivered.
        assert_eq!(outcome, SubscribeOutcome::Subscribed { baseline: None });
        assert_eq!(do_get_cursor(&conn, &session, &topic).unwrap(), None);
    }

    #[test]
    fn read_unread_advances_cursor_only_when_a_topic_returned_events() {
        let mut conn = migrated();
        let session = SessionId::new("s".to_string());
        let topic = Topic::parse("t.read.x").unwrap();

        // Subscribe to an empty topic: no cursor row yet.
        subscribe_explicit(&mut conn, &session, &topic, 1_000_000).unwrap();
        assert_eq!(do_get_cursor(&conn, &session, &topic).unwrap(), None);

        // Reading with nothing unread must NOT create/advance a cursor.
        let empty = do_read_unread(&mut conn, &session, None).unwrap();
        assert!(empty.is_empty());
        assert_eq!(
            do_get_cursor(&conn, &session, &topic).unwrap(),
            None,
            "no events => cursor must stay absent"
        );

        // Once an event exists, the read returns it and advances the cursor.
        insert_event(&conn, "t.read.x", 0);
        let page = do_read_unread(&mut conn, &session, None).unwrap();
        assert_eq!(page.len(), 1);
        assert_eq!(page[0].offset, Offset(0));
        assert_eq!(
            do_get_cursor(&conn, &session, &topic).unwrap(),
            Some(Offset(0))
        );
    }

    #[test]
    fn read_unread_honors_per_topic_limit() {
        let mut conn = migrated();
        let session = SessionId::new("s".to_string());
        let topic = Topic::parse("t.limit.x").unwrap();
        subscribe_explicit(&mut conn, &session, &topic, 1_000_000).unwrap();
        for i in 0..5 {
            insert_event(&conn, "t.limit.x", i);
        }

        // The per-topic limit caps one read; the cursor advances only to what was
        // returned, so the remainder surfaces on the next read.
        let first = do_read_unread(&mut conn, &session, Some(2)).unwrap();
        assert_eq!(
            first.iter().map(|e| e.offset.0).collect::<Vec<_>>(),
            vec![0, 1]
        );
        let second = do_read_unread(&mut conn, &session, Some(2)).unwrap();
        assert_eq!(
            second.iter().map(|e| e.offset.0).collect::<Vec<_>>(),
            vec![2, 3]
        );
    }

    #[test]
    fn read_after_out_of_range_cursor_is_empty() {
        let conn = migrated();
        let topic = Topic::parse("t.test.x").unwrap();
        conn.execute(
            "INSERT INTO event (topic, offset, event_id, adapter, timestamp, body)
             VALUES ('t.test.x', 0, 'evt-1', 'a', 0, '{}')",
            [],
        )
        .unwrap();
        let page = do_read_events(
            &conn,
            &topic,
            Cursor::After {
                offset: Offset(u64::MAX),
            },
            None,
        )
        .unwrap();
        // A cursor past i64::MAX yields nothing, NOT a wrapped replay of the log.
        assert!(page.events.is_empty());
    }
}
