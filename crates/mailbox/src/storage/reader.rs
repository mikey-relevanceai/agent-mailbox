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
use super::model::{SessionId, Unread, WakeWatermark};

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
        Ok(query_unread(&self.conn, session.as_str())?
            .topics()
            .to_vec())
    }

    /// The same unread check, plus how far the unread mail extends
    /// ([`WakeWatermark`]) — read in ONE statement, so the topics and the watermark
    /// can never come from two different snapshots (which would let a publish land
    /// between them and be recorded as already-seen).
    ///
    /// Used by the ADR-0012 turn-boundary re-trigger, which must distinguish "mail I
    /// have already re-triggered a wake for" from "mail newer than that".
    pub fn unread(&self, session: &SessionId) -> Result<Unread, StorageError> {
        query_unread(&self.conn, session.as_str())
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

/// The ONE unread predicate, factored out so it can run against any [`Connection`]
/// — the read-only side connection in production, and an in-memory connection in
/// unit tests — without opening a file.
///
/// Both public reads ([`ReadOnlyStore::topics_with_unread`] and
/// [`ReadOnlyStore::unread`]) go through here on purpose: two hand-written copies of
/// this predicate would be free to drift, and a wake that disagrees with itself
/// about what "unread" means is the exact bug class this module exists to prevent.
///
/// `MAX(e.event_row_id)` per topic is the newest unread event on it; the overall
/// [`WakeWatermark`] is the max across topics. `event_row_id` is the store's global
/// monotonic sequence, so that comparison is meaningful across topics.
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
fn query_unread(conn: &Connection, session_id: &str) -> Result<Unread, StorageError> {
    let mut stmt = conn.prepare(
        "SELECT s.topic, MAX(e.event_row_id)
         FROM subscription s
         JOIN event e ON e.topic = s.topic
         WHERE s.session_id = ?1
           AND (e.author_session IS NULL OR e.author_session <> s.session_id)
           AND e.offset > COALESCE(
               (SELECT dc.offset FROM delivery_cursor dc
                WHERE dc.session_id = s.session_id AND dc.topic = s.topic),
               -1)
         GROUP BY s.topic
         ORDER BY s.topic ASC",
    )?;
    let rows = stmt.query_map([session_id], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
    })?;

    let mut topics = Vec::new();
    let mut high_water: Option<i64> = None;
    for row in rows {
        let (topic_str, newest) = row?;
        let topic = Topic::parse(&topic_str).map_err(|_| StorageError::Corrupt {
            detail: format!("invalid topic {topic_str:?} stored in subscription"),
        })?;
        topics.push(topic);
        high_water = Some(high_water.map_or(newest, |seen: i64| seen.max(newest)));
    }

    Ok(Unread::from_parts(
        topics,
        high_water.map(WakeWatermark::new),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::params;

    /// The topic-only view most of these tests assert on.
    fn query_topics_with_unread(
        conn: &Connection,
        session_id: &str,
    ) -> Result<Vec<Topic>, StorageError> {
        Ok(query_unread(conn, session_id)?.topics().to_vec())
    }

    /// The pending mail for a session, panicking if it is caught up — the shape the
    /// watermark tests all want.
    fn pending(conn: &Connection, session_id: &str) -> super::super::model::PendingMail {
        match query_unread(conn, session_id).unwrap() {
            Unread::Pending(mail) => mail,
            Unread::CaughtUp => panic!("expected pending mail for {session_id}"),
        }
    }

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

    /// The watermark is what lets the turn-boundary re-trigger (ADR-0012) tell
    /// "mail I already re-triggered for" from "mail newer than that". It must
    /// therefore advance on a NEW event even when the unread topic SET is unchanged
    /// — the exact case the busy-window bug lived in (a second event on an
    /// already-unread topic).
    #[test]
    fn the_watermark_advances_on_new_mail_even_when_the_topic_set_is_unchanged() {
        let conn = migrated();
        subscribe(&conn, "s", "t.a");
        assert_eq!(query_unread(&conn, "s").unwrap(), Unread::CaughtUp);

        insert_event(&conn, "t.a", 0);
        let first = pending(&conn, "s");
        assert_eq!(first.topics().len(), 1);

        // A second event on the SAME topic: identical topic set, HIGHER watermark.
        insert_event(&conn, "t.a", 1);
        let second = pending(&conn, "s");
        assert_eq!(
            second.topics(),
            first.topics(),
            "the topic set is unchanged..."
        );
        assert!(
            second.high_water() > first.high_water(),
            "...but the watermark must advance"
        );

        // Reading it all returns to caught-up, so nothing is re-triggered.
        set_cursor(&conn, "s", "t.a", 1);
        assert_eq!(query_unread(&conn, "s").unwrap(), Unread::CaughtUp);
    }

    /// The watermark spans topics: it is the store's GLOBAL row-id sequence, not a
    /// per-topic offset, so mail arriving on a second topic advances it too.
    #[test]
    fn the_watermark_is_global_across_topics() {
        let conn = migrated();
        subscribe(&conn, "s", "t.a");
        subscribe(&conn, "s", "t.b");
        insert_event(&conn, "t.a", 0);
        let first = pending(&conn, "s").high_water();

        // A fresh topic whose per-topic OFFSET is 0 — lower than nothing, but its row
        // id is newer, which is what the watermark must reflect.
        insert_event(&conn, "t.b", 0);
        let second = pending(&conn, "s");
        assert_eq!(second.topics().len(), 2);
        assert!(
            second.high_water() > first,
            "a newer event on another topic must advance the watermark, despite its offset 0"
        );
    }

    /// An event the session authored is not mail for it — and must not drag the
    /// watermark either, or a self-publish would suppress the re-trigger for genuine
    /// mail that arrived before it.
    #[test]
    fn a_self_authored_event_moves_neither_the_topics_nor_the_watermark() {
        let conn = migrated();
        subscribe(&conn, "s", "t.a");
        insert_event_by(&conn, "t.a", 0, "peer");
        let before = pending(&conn, "s").high_water();

        insert_event_by(&conn, "t.a", 1, "s");
        assert_eq!(
            pending(&conn, "s").high_water(),
            before,
            "your own event is not mail, so it cannot advance your watermark"
        );
    }

    #[test]
    fn a_watermark_round_trips_through_its_persisted_form() {
        let w = WakeWatermark::new(42);
        assert_eq!(WakeWatermark::parse(&w.get().to_string()), Some(w));
        // Whitespace (a file read back with its trailing newline) is tolerated.
        assert_eq!(WakeWatermark::parse(" 42\n"), Some(w));

        // Everything outside the domain a store can mint is rejected, so no value
        // inhabiting this type is a row id SQLite never assigned. `AUTOINCREMENT`
        // starts at 1, so 0 and negatives are corruption, not watermarks.
        for corrupt in ["not a number", "", "0", "-1", "99999999999999999999999"] {
            assert_eq!(
                WakeWatermark::parse(corrupt),
                None,
                "expected {corrupt:?} to be rejected"
            );
        }
    }

    /// The pairing invariant, enforced by the constructor rather than a comment: an
    /// empty topic set is `CaughtUp`, never `Pending` with nothing to name. Without
    /// this the re-trigger could bump a sentinel with no topics and then record a
    /// watermark for mail it never named — silencing the real nudge.
    #[test]
    fn pending_mail_cannot_be_built_with_no_topics() {
        assert_eq!(
            Unread::from_parts(vec![], Some(WakeWatermark::new(9))),
            Unread::CaughtUp
        );
        assert_eq!(
            Unread::from_parts(vec![Topic::parse("t.a").unwrap()], None),
            Unread::CaughtUp
        );

        let pending = Unread::from_parts(
            vec![Topic::parse("t.a").unwrap()],
            Some(WakeWatermark::new(9)),
        );
        let Unread::Pending(mail) = &pending else {
            panic!("expected pending mail");
        };
        assert!(!mail.topics().is_empty());
        assert_eq!(mail.high_water(), WakeWatermark::new(9));
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
