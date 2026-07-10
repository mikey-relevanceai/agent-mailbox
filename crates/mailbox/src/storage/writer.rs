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

use mailbox_protocol::{AdapterId, Cursor, Event, EventId, Offset, Timestamp, Topic};

use super::error::StorageError;
use super::model::{
    Pid, ReadPage, SessionId, SubscribeOutcome, Watch, WatchId, WatchKind, WatchSpec, WatchState,
};

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
    /// Refresh an existing interest's `last_seen` (the heartbeat/touch path, card
    /// 08/11). A no-op if the interest row does not exist.
    TouchInterest {
        watch: WatchId,
        session: SessionId,
        last_seen: i64,
        reply: oneshot::Sender<Result<(), StorageError>>,
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
        Command::SubscribeAndBaseline {
            session,
            topic,
            reply,
        } => {
            let result = do_subscribe_and_baseline(conn, &session, &topic);
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
                    spec.kind.as_str(),
                    spec.repo,
                    spec.pr
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
    let body_text = serde_json::to_string(&body)?;
    let tx = conn.transaction()?;

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
        "INSERT INTO event (topic, offset, event_id, adapter, timestamp, body)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            topic.as_str(),
            offset,
            temp_id,
            adapter.0,
            timestamp.0,
            body_text
        ],
    )?;
    let row_id = tx.last_insert_rowid();
    let event_id = format!("evt-{row_id}");
    tx.execute(
        "UPDATE event SET event_id = ?1 WHERE event_row_id = ?2",
        params![event_id, row_id],
    )?;
    tx.commit()?;

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
/// the advance always commits atomically with the read that produced `offset`.
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
fn do_subscribe_and_baseline(
    conn: &mut Connection,
    session: &SessionId,
    topic: &Topic,
) -> Result<SubscribeOutcome, StorageError> {
    // Silent on success (the bus layer owns the subscribe log, like unsubscribe);
    // storage only logs failures via `log_on_err`.
    let tx = conn.transaction()?;
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
    // Idempotent by (kind, repo, pr): a second session watching the same PR
    // reuses the row and its (possibly running) state. We only refresh the
    // interval; lifecycle state is owned by SetWatchState, never reset here.
    // Saturate rather than wrap on the (practically impossible) overflow of a
    // poll interval or PR number that exceeds i64 — a wrapped negative would be
    // silently wrong, whereas a clamp is at worst a harmless over-large value.
    let interval_secs = i64::try_from(spec.interval.as_secs()).unwrap_or(i64::MAX);
    let pr = i64::try_from(spec.pr).unwrap_or(i64::MAX);
    let id: i64 = conn.query_row(
        "INSERT INTO watch (kind, repo, pr, interval_secs, state, child_pid)
         VALUES (?1, ?2, ?3, ?4, 'desired', NULL)
         ON CONFLICT(kind, repo, pr)
         DO UPDATE SET interval_secs = excluded.interval_secs
         RETURNING id",
        params![spec.kind.as_str(), spec.repo, pr, interval_secs],
        |row| row.get(0),
    )?;
    info!(
        watch = id,
        kind = spec.kind.as_str(),
        repo = %spec.repo,
        pr = spec.pr,
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
            "SELECT kind, repo, pr, interval_secs, state, child_pid FROM watch WHERE id = ?1",
            params![id.get()],
            |row| {
                let kind: String = row.get(0)?;
                let repo: String = row.get(1)?;
                let pr: i64 = row.get(2)?;
                let interval_secs: i64 = row.get(3)?;
                let state: String = row.get(4)?;
                let child_pid: Option<i64> = row.get(5)?;
                Ok((kind, repo, pr, interval_secs, state, child_pid))
            },
        )
        .optional()?;

    let Some((kind, repo, pr, interval_secs, state, child_pid)) = row else {
        return Ok(None);
    };

    Ok(Some(build_watch(
        id,
        kind,
        repo,
        pr,
        interval_secs,
        &state,
        child_pid,
    )?))
}

/// Enumerate all watches in stable id order (card-06 `status` / `unwatch`).
///
/// Reuses [`build_watch`] so the same corrupt-row guards that protect
/// [`do_get_watch`] apply to every listed row.
fn do_list_watches(conn: &Connection) -> Result<Vec<Watch>, StorageError> {
    let mut stmt = conn.prepare(
        "SELECT id, kind, repo, pr, interval_secs, state, child_pid FROM watch ORDER BY id ASC",
    )?;
    let rows = stmt.query_map([], |row| {
        let id: i64 = row.get(0)?;
        let kind: String = row.get(1)?;
        let repo: String = row.get(2)?;
        let pr: i64 = row.get(3)?;
        let interval_secs: i64 = row.get(4)?;
        let state: String = row.get(5)?;
        let child_pid: Option<i64> = row.get(6)?;
        Ok((id, kind, repo, pr, interval_secs, state, child_pid))
    })?;

    let mut watches = Vec::new();
    for row in rows {
        let (id, kind, repo, pr, interval_secs, state, child_pid) = row?;
        watches.push(build_watch(
            WatchId::new(id),
            kind,
            repo,
            pr,
            interval_secs,
            &state,
            child_pid,
        )?);
    }
    Ok(watches)
}

/// Reconstruct a [`Watch`] read model from its stored columns, rejecting any
/// value this store could not legitimately have written as [`StorageError::Corrupt`].
/// Shared by [`do_get_watch`] and [`do_list_watches`] so the guards stay in one place.
fn build_watch(
    id: WatchId,
    kind: String,
    repo: String,
    pr: i64,
    interval_secs: i64,
    state: &str,
    child_pid: Option<i64>,
) -> Result<Watch, StorageError> {
    let kind = WatchKind::parse(&kind).ok_or_else(|| StorageError::Corrupt {
        detail: format!("unknown watch kind {kind:?} for watch {}", id.get()),
    })?;
    // These columns can only hold values this store wrote, so a negative pr or
    // interval is corrupt data, not a value to silently wrap.
    let pr = u64::try_from(pr).map_err(|_| StorageError::Corrupt {
        detail: format!("negative pr {pr} for watch {}", id.get()),
    })?;
    let interval_secs = u64::try_from(interval_secs).map_err(|_| StorageError::Corrupt {
        detail: format!("negative interval {interval_secs} for watch {}", id.get()),
    })?;
    let state = reconstruct_state(state, child_pid, id)?;

    Ok(Watch {
        id,
        kind,
        repo,
        pr,
        interval: std::time::Duration::from_secs(interval_secs),
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
            "INSERT INTO watch (id, kind, repo, pr, interval_secs, state, child_pid)
             VALUES (1, 'bogus-kind', 'o/r', 1, 60, 'desired', NULL)",
            [],
        )
        .unwrap();
        let err = do_get_watch(&conn, WatchId::new(1)).unwrap_err();
        assert!(matches!(err, StorageError::Corrupt { .. }));
    }

    #[test]
    fn get_watch_rejects_negative_pr() {
        let conn = migrated();
        conn.execute(
            "INSERT INTO watch (id, kind, repo, pr, interval_secs, state, child_pid)
             VALUES (1, 'github-pr', 'o/r', -5, 60, 'desired', NULL)",
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
        do_subscribe_and_baseline(
            &mut conn,
            &SessionId::new("s"),
            &Topic::parse("t.other").unwrap(),
        )
        .unwrap();
        assert!(do_sessions_subscribed(&conn, &topic).unwrap().is_empty());
    }

    #[test]
    fn sessions_subscribed_returns_all_subscribers() {
        let mut conn = migrated();
        let topic = Topic::parse("t.sub.y").unwrap();
        do_subscribe_and_baseline(&mut conn, &SessionId::new("alice"), &topic).unwrap();
        do_subscribe_and_baseline(&mut conn, &SessionId::new("bob"), &topic).unwrap();

        let mut got: Vec<String> = do_sessions_subscribed(&conn, &topic)
            .unwrap()
            .iter()
            .map(|s| s.as_str().to_string())
            .collect();
        got.sort();
        assert_eq!(got, ["alice", "bob"]);
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
        let first = do_subscribe_and_baseline(&mut conn, &session, &topic).unwrap();
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
        let again = do_subscribe_and_baseline(&mut conn, &session, &topic).unwrap();
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

        let outcome = do_subscribe_and_baseline(&mut conn, &session, &topic).unwrap();
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
        do_subscribe_and_baseline(&mut conn, &session, &topic).unwrap();
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
        do_subscribe_and_baseline(&mut conn, &session, &topic).unwrap();
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
