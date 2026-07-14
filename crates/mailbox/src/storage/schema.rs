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
///
/// v2 (card 08) adds `watch_interest.last_seen` for the TTL sweeper — see
/// [`SCHEMA_V2`]. v3 (card 09) widens the interval to milliseconds and adds
/// `watch.publish_count` for the stub adapter — see [`SCHEMA_V3`]. v4 (card 16)
/// adds `session_tombstone` for the inbox-resurrection guard — see [`SCHEMA_V4`].
pub(crate) const SCHEMA_VERSION: u32 = 4;

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

/// Version 2 of the schema (card 08): interests carry a `last_seen` timestamp
/// (Unix millis) so a periodic TTL sweeper can drop the interest of a session
/// that hard-died without an explicit `SessionEnd`/`unwatch` (design/01
/// reconcile row). `watch` and the heartbeat (card 11) refresh it; when a
/// watch's interest hits zero — explicitly or by sweep — the adapter is stopped.
///
/// `DEFAULT 0` (the epoch) for any interest row migrated from v1 is deliberate:
/// a pre-upgrade interest is treated as immediately stale until something
/// refreshes it, which is the fail-safe direction — a stale interest whose
/// liveness we cannot vouch for should not pin a poller forever.
const SCHEMA_V2: &str = r#"
ALTER TABLE watch_interest ADD COLUMN last_seen INTEGER NOT NULL DEFAULT 0;
"#;

/// Version 3 of the schema (card 09): support the stub adapter's parameters on
/// the shared watch row.
///
/// - **`interval_secs` → `interval_ms`.** The interval is now stored in
///   milliseconds, so a sub-second stub interval (`--interval-ms 200`) survives
///   the round trip instead of truncating to whole seconds. Existing rows are
///   backfilled `* 1000`, so a pre-upgrade `github-pr` interval is unchanged.
/// - **`publish_count`.** A finite publish count for the stub (`0` = unbounded).
///   `github-pr` rows leave it at the `0` default. It is a *non-identity* column
///   (the watch is keyed by `(kind, repo, pr)`), so re-watching a stub label with
///   a different count updates the one row rather than forking a second entity
///   that would double-publish to the same `stub.<label>` topic.
const SCHEMA_V3: &str = r#"
ALTER TABLE watch RENAME COLUMN interval_secs TO interval_ms;
UPDATE watch SET interval_ms = interval_ms * 1000;
ALTER TABLE watch ADD COLUMN publish_count INTEGER NOT NULL DEFAULT 0;
"#;

/// Version 4 of the schema (card 16): the session tombstone.
///
/// `EndSession` records the wall-clock instant a session ended here, in the same
/// transaction that deletes its subscriptions/interests. The `Subscribe` writer
/// path consults it to refuse a subscription that would *resurrect* a session
/// that ended moments ago — the arm-Subscribe-vs-cleanup-EndSession race
/// (ADR-0007): a `Stop` arm's inbox re-registration can land on the writer just
/// after `SessionEnd`'s delete, permanently re-creating the inbox of a dead
/// session. A short guard window (`SUBSCRIBE_TOMBSTONE_GUARD_MS`) closes that
/// sub-second race; a genuine resume of the same id happens far later and is let
/// through (the tombstone is aged out — see `do_subscribe_and_baseline`).
///
/// Additive, like every prior step: a fresh table, no existing row touched.
///
/// Growth is bounded in practice: one row per session id ever ended (PRIMARY KEY,
/// INSERT OR REPLACE), and a session's aged tombstone is cleared the next time it
/// subscribes. On a local single-user bus that is at most a handful of kilobytes
/// over the daemon's life; a dedicated sweep of aged rows is unneeded for the MVP.
const SCHEMA_V4: &str = r#"
CREATE TABLE session_tombstone (
    session_id  TEXT    PRIMARY KEY,
    ended_at_ms INTEGER NOT NULL
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

    // current < SCHEMA_VERSION: apply the missing steps in order, all inside one
    // transaction so a partial schema never persists. A fresh DB (v0) runs every
    // step; a v1 DB runs only the v1->v2 step.
    let mut sql = String::from("BEGIN;\n");
    if current < 1 {
        sql.push_str(SCHEMA_V1);
    }
    if current < 2 {
        sql.push_str(SCHEMA_V2);
    }
    if current < 3 {
        sql.push_str(SCHEMA_V3);
    }
    if current < 4 {
        sql.push_str(SCHEMA_V4);
    }
    sql.push_str(&format!(
        "\nPRAGMA user_version = {SCHEMA_VERSION};\nCOMMIT;"
    ));
    conn.execute_batch(&sql)?;
    info!(
        from = current,
        to = SCHEMA_VERSION,
        "applied schema migration"
    );

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
    fn migrates_a_v3_db_to_v4_adding_the_tombstone_table() {
        // A v3 DB (pre card-16) must gain the `session_tombstone` table additively
        // — the resurrection guard depends on it — without disturbing prior rows.
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(SCHEMA_V1).unwrap();
        conn.execute_batch(SCHEMA_V2).unwrap();
        conn.execute_batch(SCHEMA_V3).unwrap();
        conn.execute_batch("PRAGMA user_version = 3").unwrap();

        migrate(&conn).unwrap();
        let v: u32 = conn
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(v, SCHEMA_VERSION);
        // The new table is present and writable.
        conn.execute(
            "INSERT INTO session_tombstone (session_id, ended_at_ms) VALUES ('s', 1)",
            [],
        )
        .unwrap();
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
