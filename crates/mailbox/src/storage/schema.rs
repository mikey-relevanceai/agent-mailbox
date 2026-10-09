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
/// v5 (card 19) stamps the AUTHORING session on an event — see [`SCHEMA_V5`].
/// v6 drops that column again, along with the one rule that read it — see
/// [`SCHEMA_V6`]. v7 (ADR-0022) adds the event's subject line — see [`SCHEMA_V7`].
/// v8 (ADR-0026) keeps an ended session's watches so a resume can restore them —
/// see [`SCHEMA_V8`]. v9 (ADR-0029) gives a watch its filters — see [`SCHEMA_V9`].
pub(crate) const SCHEMA_VERSION: u32 = 9;

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
/// Growth, honestly: the table holds one row per DISTINCT session id ever ended
/// over the daemon's lifetime (PRIMARY KEY, INSERT OR REPLACE dedupes re-ends of
/// the same id). A row is cleared only when that id subscribes again — either an
/// aged AutoInbox re-registration past the guard window, or any explicit
/// subscribe/watch (which clears the tombstone as proof-of-life). Most ended
/// sessions never resume, so their rows simply persist. There is NO dedicated
/// sweeper in the MVP. This is acceptable because the rows are tiny (a short id
/// string + an `i64`) and the daemon is a local, single-user process that restarts
/// often (a restart starts a fresh DB only if the file is new; an existing file
/// keeps its rows, but the count still grows only with genuinely-distinct ended
/// sessions, which is small in practice). If a long-lived shared deployment ever
/// makes this unbounded growth matter, add a periodic sweep of tombstones older
/// than the guard window — they can never refuse anything once aged.
const SCHEMA_V4: &str = r#"
CREATE TABLE session_tombstone (
    session_id  TEXT    PRIMARY KEY,
    ended_at_ms INTEGER NOT NULL
);
"#;

/// Version 5 of the schema (card 19): the authoring session of an event.
///
/// `event.author_session` was the session id of the agent that published the event,
/// or `NULL` when nobody did. It was provenance for the caller-aware publish rules,
/// nothing more. Both of those rules are gone, so [`SCHEMA_V6`] drops the column;
/// this step is kept only because the migration chain is a historical record — a v4
/// database on disk must still be walked forward through it.
const SCHEMA_V5: &str = r#"
ALTER TABLE event ADD COLUMN author_session TEXT;
"#;

/// Version 6 of the schema: drop `event.author_session` (ADR-0018).
///
/// The column existed for exactly one reader: the "be caught up to speak" rule, which
/// refused a publish from a session that had unread mail on the topic **that it had
/// not itself written**. That rule is deleted, and the only other behaviour that ever
/// consulted authorship — the no-self-wake filter — was deleted by ADR-0014. Nothing
/// reads the column now, and nothing displays it: `Event` never carried it on the
/// wire, and `mailbox send` (the peer-to-peer path) never stamped it at all, putting
/// its provenance in the body's `from` field instead.
///
/// Dropped rather than left nullable-and-unwritten, because the author was *inferred*
/// from the ambient `$CLAUDE_CODE_SESSION_ID` that Claude Code exports into every
/// process an agent spawns. A column recording a routinely-wrong answer to a question
/// nobody asks is worse than no column: it is an invitation to grow a new rule on top
/// of bad data.
///
/// `ALTER TABLE ... DROP COLUMN` is supported from SQLite 3.35 (we bundle far newer)
/// and is legal here because the column is plain: not indexed, not part of a key, no
/// CHECK or generated column refers to it.
const SCHEMA_V6: &str = r#"
ALTER TABLE event DROP COLUMN author_session;
"#;

/// Version 7 of the schema (ADR-0022): the event's subject line.
///
/// `subject` is the publisher's one-line description of what changed, and
/// `subject_link` an optional URL pointing at it. Both nullable: a subject is
/// optional at every layer, and an event published before this migration (or by an
/// adapter that has nothing to say) simply has none.
///
/// Two flat columns rather than one JSON blob, because unlike `body` this is data
/// the bridge genuinely reads — the wake digest selects it per topic — and a column
/// it can select is the difference between a query and a parse. It is still bounded
/// and inert by the time it lands here: only a parsed `Subject` is ever written, so
/// the single-line and length rules hold in the database too.
const SCHEMA_V7: &str = r#"
ALTER TABLE event ADD COLUMN subject TEXT;
ALTER TABLE event ADD COLUMN subject_link TEXT;
"#;

/// Version 8 of the schema (ADR-0026): a session's watches outlive the session.
///
/// Ending a session used to DELETE its `watch_interest` and `subscription` rows, but
/// Claude Code resumes a session under the same id, and nothing could put them back.
/// These tables hold what the session had when it went away, so `session-start` on
/// the resume can restore it.
///
/// Separate tables rather than a `suspended_at` column on the live ones, because the
/// live tables have many readers — the interest refcount that keeps an adapter alive,
/// the wake's subscriber lookup, `agents`, `topics`, `status` — and each would need
/// to learn to skip a suspended row. A column every reader must remember to filter is
/// the bug waiting to happen; a row that is not in the live table cannot be counted.
///
/// `suspended_at_ms` drives expiry: a session that never comes back is forgotten
/// after the retention window rather than kept forever.
const SCHEMA_V8: &str = r#"
CREATE TABLE suspended_interest (
    watch_id        INTEGER NOT NULL REFERENCES watch(id) ON DELETE CASCADE,
    session_id      TEXT    NOT NULL,
    suspended_at_ms INTEGER NOT NULL,
    PRIMARY KEY (watch_id, session_id)
);

CREATE TABLE suspended_subscription (
    session_id      TEXT    NOT NULL,
    topic           TEXT    NOT NULL,
    suspended_at_ms INTEGER NOT NULL,
    PRIMARY KEY (session_id, topic)
);
"#;

/// Version 9 of the schema (ADR-0029): a watch's filters.
///
/// A JSON array of the filters the watch's adapter applies before publishing,
/// `[]` for none. Only Slack watches have any today; every other kind stores
/// `[]`, which `build_watch` enforces. A non-identity column, like
/// `publish_count`: a watch is still one row per `(kind, repo, pr)`, and two
/// sessions with different filters are a conflict the writer refuses, not two
/// watches.
///
/// `DEFAULT '[]'` gives every existing watch no filters, which is what it had.
const SCHEMA_V9: &str = r#"
ALTER TABLE watch ADD COLUMN filters TEXT NOT NULL DEFAULT '[]';
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
    if current < 5 {
        sql.push_str(SCHEMA_V5);
    }
    if current < 6 {
        sql.push_str(SCHEMA_V6);
    }
    if current < 7 {
        sql.push_str(SCHEMA_V7);
    }
    if current < 8 {
        sql.push_str(SCHEMA_V8);
    }
    if current < 9 {
        sql.push_str(SCHEMA_V9);
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
    fn on_disk_v3_to_v4_migration_preserves_existing_rows() {
        // The in-memory test above proves the new table is writable; this proves the
        // UPGRADE PATH is non-destructive on a real file with real data. Build a v3
        // DB with a subscription, a watch + interest, and a delivery cursor, close
        // it, reopen, migrate to v4, and assert every prior row survived untouched
        // and the new tombstone table is usable.
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("mailbox.db");

        {
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch(SCHEMA_V1).unwrap();
            conn.execute_batch(SCHEMA_V2).unwrap();
            conn.execute_batch(SCHEMA_V3).unwrap();
            // Real rows across the tables the guard/read paths depend on.
            conn.execute(
                "INSERT INTO subscription (session_id, topic) VALUES ('s-keep', 'agent.s-keep')",
                [],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO delivery_cursor (session_id, topic, offset)
                 VALUES ('s-keep', 'agent.s-keep', 7)",
                [],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO watch (id, kind, repo, pr, interval_ms, publish_count, state, child_pid)
                 VALUES (1, 'stub', 'lbl', 0, 1000, 0, 'desired', NULL)",
                [],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO watch_interest (watch_id, session_id, last_seen) VALUES (1, 's-keep', 42)",
                [],
            )
            .unwrap();
            conn.execute_batch("PRAGMA user_version = 3").unwrap();
        }

        // Reopen the FILE and migrate to v4.
        let conn = Connection::open(&path).unwrap();
        migrate(&conn).unwrap();
        let v: u32 = conn
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(v, SCHEMA_VERSION);

        // Every prior row survived, byte-for-byte.
        let sub: String = conn
            .query_row(
                "SELECT topic FROM subscription WHERE session_id = 's-keep'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(sub, "agent.s-keep", "subscription preserved");
        let cursor: i64 = conn
            .query_row(
                "SELECT offset FROM delivery_cursor WHERE session_id = 's-keep' AND topic = 'agent.s-keep'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(cursor, 7, "delivery cursor preserved");
        let (repo, interval, last_seen): (String, i64, i64) = conn
            .query_row(
                "SELECT w.repo, w.interval_ms, wi.last_seen
                 FROM watch w JOIN watch_interest wi ON wi.watch_id = w.id
                 WHERE w.id = 1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!(
            (repo.as_str(), interval, last_seen),
            ("lbl", 1000, 42),
            "watch + interest preserved (interval NOT re-multiplied by the v3 step)"
        );

        // And the new v4 table is present and usable.
        conn.execute(
            "INSERT INTO session_tombstone (session_id, ended_at_ms) VALUES ('s-keep', 99)",
            [],
        )
        .unwrap();
        let ended: i64 = conn
            .query_row(
                "SELECT ended_at_ms FROM session_tombstone WHERE session_id = 's-keep'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(ended, 99);
    }

    /// A v5 database on disk — one that HAS the author column, possibly with values
    /// in it — migrates forward to v6 without losing an event. Dropping a column
    /// rewrites the table, so "the bodies and offsets survive" is the property that
    /// matters; that the column itself is gone is the second half.
    #[test]
    fn on_disk_v5_to_v6_migration_drops_the_author_and_keeps_every_event() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("mailbox.db");

        {
            let conn = Connection::open(&path).unwrap();
            for step in [SCHEMA_V1, SCHEMA_V2, SCHEMA_V3, SCHEMA_V4, SCHEMA_V5] {
                conn.execute_batch(step).unwrap();
            }
            conn.execute(
                "INSERT INTO event (topic, offset, event_id, adapter, timestamp, body, author_session)
                 VALUES ('t.a', 0, 'evt-1', 'github-pr', 123, '{\"edge\":\"ci\"}', NULL),
                        ('t.a', 1, 'evt-2', 'cli', 124, '{}', 's-agent')",
                [],
            )
            .unwrap();
            conn.execute_batch("PRAGMA user_version = 5").unwrap();
        }

        let conn = Connection::open(&path).unwrap();
        migrate(&conn).unwrap();
        let v: u32 = conn
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(v, SCHEMA_VERSION);

        let (body, adapter): (String, String) = conn
            .query_row(
                "SELECT body, adapter FROM event WHERE event_id = 'evt-1'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(body, "{\"edge\":\"ci\"}", "the body survives verbatim");
        assert_eq!(adapter, "github-pr", "provenance survives");

        let (count, max_offset): (i64, i64) = conn
            .query_row("SELECT COUNT(*), MAX(offset) FROM event", [], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })
            .unwrap();
        assert_eq!(count, 2, "no event is lost when the column is dropped");
        assert_eq!(
            max_offset, 1,
            "offsets survive, so the next publish appends"
        );

        // The column is genuinely gone: nothing can read (or start writing) it again.
        let err = conn.query_row("SELECT author_session FROM event", [], |r| {
            r.get::<_, i64>(0)
        });
        assert!(
            err.is_err(),
            "event.author_session must not exist after the v6 migration"
        );
    }

    /// A v6 database on disk — every event in it published before subjects existed —
    /// migrates forward with its mail intact and simply has nothing to say about
    /// those events. The upgrade must be invisible to an agent mid-watch: its unread
    /// mail stays unread, and its next wake describes what it can.
    #[test]
    fn on_disk_v6_to_v7_migration_keeps_every_event_and_leaves_it_subject_less() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("mailbox.db");

        {
            let conn = Connection::open(&path).unwrap();
            for step in [
                SCHEMA_V1, SCHEMA_V2, SCHEMA_V3, SCHEMA_V4, SCHEMA_V5, SCHEMA_V6,
            ] {
                conn.execute_batch(step).unwrap();
            }
            conn.execute(
                "INSERT INTO event (topic, offset, event_id, adapter, timestamp, body)
                 VALUES ('t.a', 0, 'evt-1', 'github-pr', 123, '{\"edge\":\"ci\"}'),
                        ('t.a', 1, 'evt-2', 'cli', 124, '{}')",
                [],
            )
            .unwrap();
            conn.execute_batch("PRAGMA user_version = 6").unwrap();
        }

        let conn = Connection::open(&path).unwrap();
        migrate(&conn).unwrap();
        let v: u32 = conn
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(v, SCHEMA_VERSION);

        let (count, subjects): (i64, i64) = conn
            .query_row("SELECT COUNT(*), COUNT(subject) FROM event", [], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })
            .unwrap();
        assert_eq!(count, 2, "no pre-subject event is lost");
        assert_eq!(subjects, 0, "and none of them claims a subject");

        // The columns are writable, so the next publish can describe itself.
        conn.execute(
            "INSERT INTO event (topic, offset, event_id, adapter, timestamp, body, subject, subject_link)
             VALUES ('t.a', 2, 'evt-3', 'github-pr', 125, '{}', 'new comment', 'https://example.com/c/1')",
            [],
        )
        .unwrap();
        let (subject, link): (String, String) = conn
            .query_row(
                "SELECT subject, subject_link FROM event WHERE event_id = 'evt-3'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(subject, "new comment");
        assert_eq!(link, "https://example.com/c/1");
    }

    /// A v7 database on disk — with sessions mid-watch — migrates forward with
    /// every live row untouched and nothing suspended: the upgrade must not look
    /// like every session ended.
    #[test]
    fn on_disk_v7_to_v8_migration_keeps_live_rows_and_suspends_nothing() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("mailbox.db");

        {
            let conn = Connection::open(&path).unwrap();
            for step in [
                SCHEMA_V1, SCHEMA_V2, SCHEMA_V3, SCHEMA_V4, SCHEMA_V5, SCHEMA_V6, SCHEMA_V7,
            ] {
                conn.execute_batch(step).unwrap();
            }
            conn.execute_batch(
                "INSERT INTO watch (id, kind, repo, pr, interval_ms, publish_count, state, child_pid)
                 VALUES (1, 'stub', 'lbl', 0, 1000, 0, 'desired', NULL);
                 INSERT INTO watch_interest (watch_id, session_id, last_seen) VALUES (1, 's', 42);
                 INSERT INTO subscription (session_id, topic) VALUES ('s', 'stub.lbl');
                 PRAGMA user_version = 7;",
            )
            .unwrap();
        }

        let conn = Connection::open(&path).unwrap();
        migrate(&conn).unwrap();
        let v: u32 = conn
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(v, SCHEMA_VERSION);

        let count = |sql: &str| -> i64 { conn.query_row(sql, [], |r| r.get(0)).unwrap() };
        assert_eq!(count("SELECT COUNT(*) FROM watch_interest"), 1);
        assert_eq!(count("SELECT COUNT(*) FROM subscription"), 1);
        assert_eq!(count("SELECT COUNT(*) FROM suspended_interest"), 0);
        assert_eq!(count("SELECT COUNT(*) FROM suspended_subscription"), 0);
    }

    /// A v8 database on disk with a running Slack watch migrates forward with the
    /// watch intact and filterless: the upgrade must not change what wakes anyone.
    #[test]
    fn on_disk_v8_to_v9_migration_gives_every_watch_no_filters() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("mailbox.db");

        {
            let conn = Connection::open(&path).unwrap();
            for step in [
                SCHEMA_V1, SCHEMA_V2, SCHEMA_V3, SCHEMA_V4, SCHEMA_V5, SCHEMA_V6, SCHEMA_V7,
                SCHEMA_V8,
            ] {
                conn.execute_batch(step).unwrap();
            }
            conn.execute_batch(
                "INSERT INTO watch (id, kind, repo, pr, interval_ms, publish_count, state, child_pid)
                 VALUES (1, 'slack-channel', 'C0C83CXLUL8', 0, 60000, 0, 'running', 4242);
                 INSERT INTO watch_interest (watch_id, session_id, last_seen) VALUES (1, 's', 42);
                 PRAGMA user_version = 8;",
            )
            .unwrap();
        }

        let conn = Connection::open(&path).unwrap();
        migrate(&conn).unwrap();
        let v: u32 = conn
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(v, SCHEMA_VERSION);

        let (repo, state, filters): (String, String, String) = conn
            .query_row(
                "SELECT repo, state, filters FROM watch WHERE id = 1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!(
            (repo.as_str(), state.as_str(), filters.as_str()),
            ("C0C83CXLUL8", "running", "[]")
        );
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
