//! Schema definition and `PRAGMA user_version`-based migrations.
//!
//! MVP migration scheme: a single monotonic integer in `PRAGMA user_version`.
//! Opening a fresh DB (version 0) applies [`SCHEMA_V1`] and stamps the version
//! to [`SCHEMA_VERSION`]. Opening an up-to-date DB is a clean no-op. Opening a
//! DB whose version is *higher* than we know is refused (see
//! [`StorageError::UnsupportedSchemaVersion`]) rather than guessed at.

use rusqlite::Connection;
use tracing::{debug, info};

use super::error::StorageError;

/// The schema version this build creates and understands.
pub(crate) const SCHEMA_VERSION: u32 = 1;

/// Version 1 of the schema.
///
/// Per-topic offset ordering is enforced two ways: the writer assigns
/// `offset = MAX(offset)+1` per topic (safe because there is exactly one
/// writer, so no two publishes race for the same next value), and
/// `UNIQUE(topic, offset)` makes a gap-or-dup a hard error rather than silent
/// corruption. `event_row_id` (the implicit rowid via INTEGER PRIMARY KEY) is a
/// global monotonic sequence used to mint opaque `EventId`s.
///
/// `body` is opaque JSON text stored verbatim and never interpreted (ADR-0001);
/// likewise `adapter_baseline.baseline`.
const SCHEMA_V1: &str = r#"
CREATE TABLE event (
    event_row_id INTEGER PRIMARY KEY AUTOINCREMENT,
    topic        TEXT    NOT NULL,
    offset       INTEGER NOT NULL,
    event_id     TEXT    NOT NULL UNIQUE,
    -- Publisher provenance only (a label, not authority); the body stays
    -- untrusted regardless of who published it (ADR-0001).
    adapter      TEXT    NOT NULL,
    timestamp    INTEGER NOT NULL,
    body         TEXT    NOT NULL,
    UNIQUE(topic, offset)
);
-- Cursor reads are always "this topic, offset > N, in order"; this index makes
-- that a range scan rather than a per-topic sort.
CREATE INDEX idx_event_topic_offset ON event(topic, offset);

CREATE TABLE subscription (
    session_id TEXT NOT NULL,
    topic      TEXT NOT NULL,
    PRIMARY KEY (session_id, topic)
);

-- Per (subscriber, topic): the highest offset delivered so far. Two
-- subscribers on the same topic advance independently.
CREATE TABLE delivery_cursor (
    session_id TEXT    NOT NULL,
    topic      TEXT    NOT NULL,
    offset     INTEGER NOT NULL,
    PRIMARY KEY (session_id, topic)
);

CREATE TABLE watch (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    kind          TEXT    NOT NULL,
    repo          TEXT    NOT NULL,
    pr            INTEGER NOT NULL,
    interval_secs INTEGER NOT NULL,
    state         TEXT    NOT NULL,
    child_pid     INTEGER,
    -- One adapter per external entity (design/01): identity is (kind, repo, pr),
    -- not the session. A second session watching the same PR reuses this row.
    UNIQUE(kind, repo, pr)
);

-- (watch_id, session_id): who still cares. COUNT(*) per watch is the refcount
-- that keeps a poller alive; the row vanishing on the last session is what
-- authorizes teardown. ON DELETE CASCADE so removing a watch cleans interest.
CREATE TABLE watch_interest (
    watch_id   INTEGER NOT NULL REFERENCES watch(id) ON DELETE CASCADE,
    session_id TEXT    NOT NULL,
    PRIMARY KEY (watch_id, session_id)
);

-- Centralised adapter baseline (design/01), replacing per-file *.state. Opaque
-- JSON: last mergeable + review/thread/comment + CI aggregates, shaped by the
-- adapter, not interpreted here.
CREATE TABLE adapter_baseline (
    watch_id INTEGER PRIMARY KEY REFERENCES watch(id) ON DELETE CASCADE,
    baseline TEXT NOT NULL
);
"#;

/// Bring an open connection up to [`SCHEMA_VERSION`], creating the schema on a
/// fresh DB and no-op'ing on an up-to-date one. Idempotent: safe to call on
/// every open.
pub(crate) fn migrate(conn: &Connection) -> Result<(), StorageError> {
    let current: u32 = conn.query_row("PRAGMA user_version", [], |row| row.get(0))?;

    if current > SCHEMA_VERSION {
        return Err(StorageError::UnsupportedSchemaVersion {
            found: current,
            supported: SCHEMA_VERSION,
        });
    }

    if current == SCHEMA_VERSION {
        // Already current — nothing to do.
        debug!(version = current, "schema already current");
        return Ok(());
    }

    // current < SCHEMA_VERSION: apply the gap. For the MVP there is only the
    // v0 -> v1 step. Wrap in a transaction so a partial schema never persists.
    if current < 1 {
        conn.execute_batch(&format!(
            "BEGIN;\n{SCHEMA_V1}\nPRAGMA user_version = {SCHEMA_VERSION};\nCOMMIT;"
        ))?;
        info!(
            from = current,
            to = SCHEMA_VERSION,
            "applied schema migration"
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fresh_migrate_then_reopen_is_idempotent() {
        let conn = Connection::open_in_memory().unwrap();
        migrate(&conn).unwrap();
        let v: u32 = conn
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(v, SCHEMA_VERSION);

        // Second call is a clean no-op (does not error re-creating tables).
        migrate(&conn).unwrap();
        let v2: u32 = conn
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(v2, SCHEMA_VERSION);
    }

    #[test]
    fn refuses_a_schema_from_the_future() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("PRAGMA user_version = 999").unwrap();
        let err = migrate(&conn).unwrap_err();
        assert!(matches!(
            err,
            StorageError::UnsupportedSchemaVersion {
                found: 999,
                supported: SCHEMA_VERSION
            }
        ));
    }
}
