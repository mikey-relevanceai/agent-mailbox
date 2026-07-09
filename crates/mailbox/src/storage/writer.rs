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
use super::model::{Pid, ReadPage, SessionId, Watch, WatchId, WatchKind, WatchSpec, WatchState};

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
    Subscribe {
        session: SessionId,
        topic: Topic,
        reply: oneshot::Sender<Result<(), StorageError>>,
    },
    Unsubscribe {
        session: SessionId,
        topic: Topic,
        reply: oneshot::Sender<Result<(), StorageError>>,
    },
    AdvanceCursor {
        session: SessionId,
        topic: Topic,
        offset: Offset,
        reply: oneshot::Sender<Result<(), StorageError>>,
    },
    GetCursor {
        session: SessionId,
        topic: Topic,
        reply: oneshot::Sender<Result<Option<Offset>, StorageError>>,
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
        reply: oneshot::Sender<Result<u64, StorageError>>,
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
        Command::Subscribe {
            session,
            topic,
            reply,
        } => {
            let result = do_subscribe(conn, &session, &topic);
            log_on_err(&result, "subscribe", || {
                format!("session={} topic={}", session.as_str(), topic.as_str())
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
        Command::AdvanceCursor {
            session,
            topic,
            offset,
            reply,
        } => {
            let result = do_advance_cursor(conn, &session, &topic, offset);
            log_on_err(&result, "advance_cursor", || {
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
            reply,
        } => {
            let result = do_add_interest(conn, watch, &session);
            log_on_err(&result, "add_interest", || {
                format!("watch={} session={}", watch.get(), session.as_str())
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

fn do_subscribe(conn: &Connection, session: &SessionId, topic: &Topic) -> Result<(), StorageError> {
    conn.execute(
        "INSERT OR IGNORE INTO subscription (session_id, topic) VALUES (?1, ?2)",
        params![session.as_str(), topic.as_str()],
    )?;
    Ok(())
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

fn do_advance_cursor(
    conn: &Connection,
    session: &SessionId,
    topic: &Topic,
    offset: Offset,
) -> Result<(), StorageError> {
    // Reject an out-of-range offset rather than storing a negative (which would
    // later match `offset > ?` for the whole log).
    let stored = offset_to_sqlite(offset)?;
    // MAX(...) keeps the cursor monotonic: a late/duplicate advance can never
    // rewind delivery. Encodes the assumption that cursors only move forward.
    conn.execute(
        "INSERT INTO delivery_cursor (session_id, topic, offset) VALUES (?1, ?2, ?3)
         ON CONFLICT(session_id, topic)
         DO UPDATE SET offset = MAX(offset, excluded.offset)",
        params![session.as_str(), topic.as_str(), stored],
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
        WatchState::Running { pid } => ("running", Some(i64::from(pid.0))),
        WatchState::Stopped => ("stopped", None),
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
    let state = reconstruct_state(&state, child_pid, id)?;

    Ok(Some(Watch {
        id,
        kind,
        repo,
        pr,
        interval: std::time::Duration::from_secs(interval_secs),
        state,
    }))
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
        "running" => match child_pid {
            Some(pid) => {
                let pid = i32::try_from(pid).map_err(|_| StorageError::Corrupt {
                    detail: format!("watch {} child pid {pid} does not fit i32", id.get()),
                })?;
                Ok(WatchState::Running { pid: Pid(pid) })
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
) -> Result<u64, StorageError> {
    conn.execute(
        "INSERT OR IGNORE INTO watch_interest (watch_id, session_id) VALUES (?1, ?2)",
        params![watch.get(), session.as_str()],
    )?;
    let count = interest_count(conn, watch)?;
    info!(
        watch = watch.get(),
        interest = count,
        "attached session interest"
    );
    Ok(count)
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

    #[test]
    fn advance_cursor_rejects_out_of_range_offset() {
        let conn = migrated();
        let session = SessionId("s".to_string());
        let topic = Topic::parse("t.test.x").unwrap();
        let err = do_advance_cursor(&conn, &session, &topic, Offset(u64::MAX)).unwrap_err();
        assert!(matches!(err, StorageError::OffsetOutOfRange { offset } if offset == u64::MAX));
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
