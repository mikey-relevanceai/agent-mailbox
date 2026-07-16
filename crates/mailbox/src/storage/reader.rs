//! Read-only, side-connection access to the durable store.
//!
//! # Why this exists (and why it is allowed to open the DB directly)
//!
//! The single-writer rule (ADR-0003) says only the bridge mutates the database,
//! and everything else speaks the protocol. But wake has a genuinely separate
//! actor: the **waiter** is its own process (a background hook launched with
//! `asyncRewake`, see docs/01-wake-and-rearm.md), and before it blocks it must
//! answer one question — "does this session have unread mail right now?" — so a
//! publish that landed *before* the waiter started still fires (the missed-kick
//! safety). ADR-0003 explicitly permits this: "reads used for wake/delivery ...
//! stay read-only and never mutate."
//!
//! So this type opens the SQLite file with [`OpenFlags::SQLITE_OPEN_READ_ONLY`]
//! and **no** create flag: it cannot write, cannot create the file, and never
//! advances a cursor. Advancing a session's cursor is the exclusive job of the
//! agent's later `read` through the single writer — checking unread here is
//! deliberately non-destructive so the waiter can peek without consuming.

use std::path::Path;

use rusqlite::{Connection, OpenFlags};

use mailbox_protocol::Topic;

use super::error::StorageError;
use super::model::SessionId;

/// A read-only view of the durable store, opened as a side connection.
///
/// Held by the waiter process. Every method is a plain `SELECT`; there is no
/// path from here to a mutation.
#[derive(Debug)]
pub struct ReadOnlyStore {
    conn: Connection,
}

impl ReadOnlyStore {
    /// Open the database at `path` read-only.
    ///
    /// Uses `SQLITE_OPEN_READ_ONLY` with **no** create flag, so a missing file
    /// is an error ([`StorageError::Open`]) rather than a silently-created empty
    /// DB — a waiter with no bridge/store behind it has nothing to wait on, and
    /// we would rather say so than invent an empty database. A `busy_timeout`
    /// is set as a defensive backstop; with WAL the reader does not contend with
    /// the writer, so it should never fire.
    ///
    /// Residual risk (out of scope for card 05): cross-process read visibility
    /// assumes the bridge/writer process is live so the WAL/-shm files exist. If
    /// the bridge is down this open may fail; that "bridge down" window is owned
    /// by later process-lifecycle cards.
    pub fn open(path: &Path) -> Result<Self, StorageError> {
        let conn = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .map_err(|source| StorageError::Open {
            path: path.to_path_buf(),
            source,
        })?;
        conn.busy_timeout(std::time::Duration::from_secs(5))?;
        Ok(Self { conn })
    }

    /// The subscribed topics on which `session` currently has at least one unread
    /// event, in ascending topic order (deterministic for the stderr reminder).
    ///
    /// "Unread" is exactly the bus definition: an event whose offset is strictly
    /// greater than the session's delivery cursor on that topic, or — when no
    /// cursor row exists yet — any event (cursor treated as `-1`, since offsets
    /// start at 0). This is the read-only twin of `do_read_unread`'s predicate;
    /// it reports which topics *would* deliver without advancing anything.
    /// The waiter both decides whether to wake (non-empty ⇒ wake) and names the
    /// topics for the reminder from this one call, so a separate `has_unread`
    /// boolean would be redundant.
    pub fn topics_with_unread(&self, session: &SessionId) -> Result<Vec<Topic>, StorageError> {
        query_topics_with_unread(&self.conn, session.as_str())
    }

    /// Whether `session` currently has at least one subscription.
    ///
    /// The waiter-side "arm-iff-subscribed" re-check (card 11): distinct from
    /// [`topics_with_unread`](Self::topics_with_unread), which is empty both when
    /// the session has NO subscription AND when it is subscribed but caught up.
    /// This asks the narrower question — "is there anything to be woken about at
    /// all?" — so a waiter that raced a `SessionEnd`/unsubscribe (interest already
    /// dropped) can self-exit instead of blocking forever as an orphan.
    pub fn has_subscription(&self, session: &SessionId) -> Result<bool, StorageError> {
        let exists: bool = self.conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM subscription WHERE session_id = ?1)",
            [session.as_str()],
            |row| row.get(0),
        )?;
        Ok(exists)
    }
}

/// The unread-topics query, factored out so it can run against any
/// [`Connection`] — the read-only side connection in production, and an
/// in-memory connection in unit tests — without opening a file.
///
/// Events the session AUTHORED itself are excluded: you are never woken by your own
/// message (the durable half of the no-self-kick — the kick filter alone would only
/// hold for a waiter that was already blocked, while a waiter armed *after* the
/// publish would find its own event unread and wake on it). Nothing is hidden by
/// this: the event is still returned by `read` and still counted by `status` — it
/// simply is not a reason to wake the session that wrote it.
///
/// A subscription row can only hold a topic the bridge accepted, so a stored
/// value that fails the grammar is corrupt storage, not user input (mirrors
/// `read_topic_unread`).
fn query_topics_with_unread(
    conn: &Connection,
    session_id: &str,
) -> Result<Vec<Topic>, StorageError> {
    let mut stmt = conn.prepare(
        "SELECT s.topic
         FROM subscription s
         WHERE s.session_id = ?1
           AND EXISTS (
               SELECT 1 FROM event e
               WHERE e.topic = s.topic
                 AND (e.author_session IS NULL OR e.author_session <> s.session_id)
                 AND e.offset > COALESCE(
                     (SELECT dc.offset FROM delivery_cursor dc
                      WHERE dc.session_id = s.session_id AND dc.topic = s.topic),
                     -1)
           )
         ORDER BY s.topic ASC",
    )?;
    let rows = stmt.query_map([session_id], |row| row.get::<_, String>(0))?;

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

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::params;

    /// A fresh in-memory DB with the schema applied, for exercising the query
    /// predicate directly (fast; no file, no read-only open required).
    fn migrated() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        crate::storage::schema::migrate(&conn).unwrap();
        conn
    }

    fn subscribe(conn: &Connection, session: &str, topic: &str) {
        conn.execute(
            "INSERT INTO subscription (session_id, topic) VALUES (?1, ?2)",
            params![session, topic],
        )
        .unwrap();
    }

    fn insert_event(conn: &Connection, topic: &str, offset: i64) {
        conn.execute(
            "INSERT INTO event (topic, offset, event_id, adapter, timestamp, body)
             VALUES (?1, ?2, ?3, 'a', 0, '{}')",
            params![topic, offset, format!("evt-{topic}-{offset}")],
        )
        .unwrap();
    }

    fn insert_event_by(conn: &Connection, topic: &str, offset: i64, author: &str) {
        conn.execute(
            "INSERT INTO event (topic, offset, event_id, adapter, timestamp, body, author_session)
             VALUES (?1, ?2, ?3, 'a', 0, '{}', ?4)",
            params![topic, offset, format!("evt-{topic}-{offset}"), author],
        )
        .unwrap();
    }

    fn set_cursor(conn: &Connection, session: &str, topic: &str, offset: i64) {
        conn.execute(
            "INSERT INTO delivery_cursor (session_id, topic, offset) VALUES (?1, ?2, ?3)
             ON CONFLICT(session_id, topic) DO UPDATE SET offset = excluded.offset",
            params![session, topic, offset],
        )
        .unwrap();
    }

    /// You are never woken by your own message — the durable half of the no-self-kick
    /// (a waiter armed *after* your publish must not wake on it either). A PEER's
    /// event on the same topic still wakes you, and an anonymous (adapter /
    /// `--no-session`) event wakes everyone.
    #[test]
    fn an_event_you_authored_does_not_wake_you_but_a_peers_does() {
        let conn = migrated();
        subscribe(&conn, "s", "t.a");
        subscribe(&conn, "peer", "t.a");

        insert_event_by(&conn, "t.a", 0, "s");
        assert!(
            query_topics_with_unread(&conn, "s").unwrap().is_empty(),
            "your own event must not wake you"
        );
        assert_eq!(
            query_topics_with_unread(&conn, "peer").unwrap().len(),
            1,
            "but it IS mail for the peer"
        );

        // A peer's event wakes you.
        insert_event_by(&conn, "t.a", 1, "peer");
        assert_eq!(query_topics_with_unread(&conn, "s").unwrap().len(), 1);

        // Caught up again (the cursor covers both) => quiet.
        set_cursor(&conn, "s", "t.a", 1);
        assert!(query_topics_with_unread(&conn, "s").unwrap().is_empty());

        // An anonymous publish (adapter / `--no-session`) wakes EVERY subscriber,
        // including a session that happens to have spawned the publisher.
        insert_event(&conn, "t.a", 2);
        assert_eq!(query_topics_with_unread(&conn, "s").unwrap().len(), 1);
    }

    #[test]
    fn no_subscription_means_no_unread() {
        let conn = migrated();
        insert_event(&conn, "t.a.x", 0);
        // Not subscribed => nothing is unread for this session.
        assert!(query_topics_with_unread(&conn, "s").unwrap().is_empty());
    }

    #[test]
    fn subscribed_with_event_and_no_cursor_is_unread() {
        let conn = migrated();
        subscribe(&conn, "s", "t.a.x");
        insert_event(&conn, "t.a.x", 0);
        // No cursor row yet => cursor treated as -1 => offset 0 is unread.
        let topics = query_topics_with_unread(&conn, "s").unwrap();
        assert_eq!(
            topics.iter().map(|t| t.as_str()).collect::<Vec<_>>(),
            ["t.a.x"]
        );
    }

    #[test]
    fn cursor_at_head_means_no_unread() {
        let conn = migrated();
        subscribe(&conn, "s", "t.a.x");
        insert_event(&conn, "t.a.x", 0);
        set_cursor(&conn, "s", "t.a.x", 0);
        // Cursor caught up to the only event => nothing unread.
        assert!(query_topics_with_unread(&conn, "s").unwrap().is_empty());

        // A newer event beyond the cursor becomes unread again.
        insert_event(&conn, "t.a.x", 1);
        assert_eq!(query_topics_with_unread(&conn, "s").unwrap().len(), 1);
    }

    /// End-to-end against a real file DB: a read-only side connection sees mail
    /// committed by the writer across connections, `has_unread` tracks it, and
    /// the peek does NOT advance the cursor (the agent's later read still gets
    /// the event). This is the property the waiter relies on.
    #[tokio::test]
    async fn read_only_store_sees_committed_mail_without_consuming() {
        use crate::bus::Bus;
        use crate::storage::{Storage, StorageConfig};
        use mailbox_protocol::{AdapterId, GithubPr, Timestamp};

        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("mailbox.db");
        let storage = Storage::open(StorageConfig::at(&path)).await.unwrap();
        let bus = Bus::new(storage.clone());
        let topic = GithubPr::new("o", "r", 1).unwrap().topic();
        let session = SessionId::new("s-ro");
        bus.subscribe(session.clone(), std::slice::from_ref(&topic))
            .await
            .unwrap();

        let store = ReadOnlyStore::open(&path).unwrap();
        assert!(store.topics_with_unread(&session).unwrap().is_empty());

        bus.publish(
            topic.clone(),
            AdapterId("a".to_string()),
            Timestamp(0),
            serde_json::json!({ "i": 0 }),
        )
        .await
        .unwrap();

        // The read-only connection sees the committed event.
        assert!(!store.topics_with_unread(&session).unwrap().is_empty());
        assert_eq!(
            store
                .topics_with_unread(&session)
                .unwrap()
                .iter()
                .map(|t| t.as_str().to_string())
                .collect::<Vec<_>>(),
            [topic.as_str()]
        );

        // Peeking did not advance the cursor: the agent's real read still gets it.
        assert_eq!(
            storage
                .cursor(session.clone(), topic.clone())
                .await
                .unwrap(),
            None
        );
        assert_eq!(bus.read(session, None).await.unwrap().len(), 1);
    }

    #[test]
    fn reports_only_topics_with_unread_in_topic_order() {
        let conn = migrated();
        // Subscribed to three topics; only two have unread events.
        for t in ["t.a", "t.b", "t.c"] {
            subscribe(&conn, "s", t);
        }
        insert_event(&conn, "t.c", 0); // unread
        insert_event(&conn, "t.a", 0); // read (cursor caught up below)
        set_cursor(&conn, "s", "t.a", 0);
        // t.b has no events at all => not unread.
        let topics: Vec<String> = query_topics_with_unread(&conn, "s")
            .unwrap()
            .iter()
            .map(|t| t.as_str().to_string())
            .collect();
        // Only t.c is unread.
        assert_eq!(topics, ["t.c"]);
    }
}
