//! Storage integration tests.
//!
//! These exercise the durable core against real SQLite in a tempdir (never the
//! real home directory) — the store's assumptions live at this level, so we
//! prefer a real DB over mocks (mikey-in-a-box testing-strategy). Each of the
//! four acceptance criteria for card 03 has a named test below.

use std::path::Path;
use std::time::Duration;

use mailbox::storage::{
    SessionId, Storage, StorageConfig, StorageError, WatchKind, WatchSpec, WatchState,
};
use mailbox_protocol::{AdapterId, Cursor, GithubPr, Offset, Timestamp, Topic};
use serde_json::json;
use tempfile::TempDir;

/// Count rows matching a query by opening a throwaway connection to the DB
/// file. Test-only inspection: it only ever SELECTs, so it does not violate the
/// single-writer rule for production code. Used where the public API exposes no
/// read for the state under test (e.g. subscriptions).
fn count_rows(path: &Path, sql: &str) -> i64 {
    let conn = rusqlite::Connection::open(path).unwrap();
    conn.query_row(sql, [], |row| row.get(0)).unwrap()
}

/// Open a fresh store under a tempdir. Returns the dir too so it outlives the
/// store (dropping it deletes the DB files).
async fn fresh_store() -> (Storage, TempDir) {
    let dir = TempDir::new().expect("tempdir");
    let path = dir.path().join("mailbox.db");
    let storage = Storage::open(StorageConfig::at(path))
        .await
        .expect("open storage");
    (storage, dir)
}

fn pr_topic(n: u64) -> Topic {
    GithubPr::new("octocat", "hello-world", n).unwrap().topic()
}

fn adapter() -> AdapterId {
    AdapterId("github-watch".to_string())
}

// ---- Acceptance criterion 1: all mutations flow through the single writer ----

/// The public handle exposes no way to obtain a `Connection`; the only shared
/// thing is the command channel, and clones share one writer. The observable
/// proof is that heavily concurrent writes serialize into one contiguous
/// offset sequence (tested in `ac2_*`); here we assert the structural property
/// that cloning the handle does not create a second writer — both clones write
/// to the same log.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac1_all_writes_share_one_writer_via_cloned_handle() {
    let (storage, _dir) = fresh_store().await;
    let topic = pr_topic(1);

    let clone_a = storage.clone();
    let clone_b = storage.clone();

    let a = clone_a
        .publish(topic.clone(), adapter(), Timestamp(1), json!({"n": "a"}))
        .await
        .unwrap();
    let b = clone_b
        .publish(topic.clone(), adapter(), Timestamp(2), json!({"n": "b"}))
        .await
        .unwrap();

    // Two independently-cloned handles wrote to the same durable log with a
    // single shared offset sequence — there is no second writer.
    assert_eq!(a.offset, Offset(0));
    assert_eq!(b.offset, Offset(1));

    let page = storage
        .read_events(topic, Cursor::Oldest, None)
        .await
        .unwrap();
    assert_eq!(page.events.len(), 2);
}

// ---- Acceptance criterion 2: concurrent publish burst, no SQLITE_BUSY --------

/// Fire many concurrent publishes at one topic. All must be durable, offsets
/// strictly ordered AND contiguous (0..N), and no `SQLITE_BUSY` may reach a
/// caller (every result is `Ok`).
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn ac2_concurrent_publish_burst_is_ordered_and_contiguous() {
    let (storage, _dir) = fresh_store().await;
    let topic = pr_topic(2);

    const BURST: u64 = 500;
    let mut handles = Vec::new();
    for i in 0..BURST {
        let storage = storage.clone();
        let topic = topic.clone();
        handles.push(tokio::spawn(async move {
            storage
                .publish(topic, adapter(), Timestamp(i as i64), json!({ "i": i }))
                .await
        }));
    }

    // Every publish succeeded — no busy error was ever surfaced.
    let mut offsets = Vec::new();
    for h in handles {
        let event = h.await.unwrap().expect("publish must not surface an error");
        offsets.push(event.offset.0);
    }

    // Assigned offsets are exactly {0, .., BURST-1}: strictly ordered and
    // contiguous, which is only possible if a single writer serialized them.
    offsets.sort_unstable();
    let expected: Vec<u64> = (0..BURST).collect();
    assert_eq!(offsets, expected);

    // And the durable log agrees.
    let page = storage
        .read_events(topic, Cursor::Oldest, Some(BURST as u32))
        .await
        .unwrap();
    assert_eq!(page.events.len(), BURST as usize);
    for (i, event) in page.events.iter().enumerate() {
        assert_eq!(event.offset, Offset(i as u64));
    }
}

/// Two topics keep independent, each-contiguous offset sequences.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac2_per_topic_offsets_are_independent() {
    let (storage, _dir) = fresh_store().await;
    let t1 = pr_topic(10);
    let t2 = pr_topic(11);

    for _ in 0..3 {
        storage
            .publish(t1.clone(), adapter(), Timestamp(0), json!({}))
            .await
            .unwrap();
    }
    let e = storage
        .publish(t2.clone(), adapter(), Timestamp(0), json!({}))
        .await
        .unwrap();
    // t2's first event is offset 0 regardless of t1's three events.
    assert_eq!(e.offset, Offset(0));

    let p1 = storage.read_events(t1, Cursor::Oldest, None).await.unwrap();
    let p2 = storage.read_events(t2, Cursor::Oldest, None).await.unwrap();
    assert_eq!(p1.events.len(), 3);
    assert_eq!(p2.events.len(), 1);
}

// ---- Acceptance criterion 3: WAL crash recovery ------------------------------

/// Simulate a process killed mid-life: publish committed events, then leak the
/// store so its writer thread and connection are NEVER cleanly closed (models
/// `kill -9`, leaving the WAL un-checkpointed). Reopen a fresh store on the
/// same file and verify integrity plus that all committed rows survived.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac3_survives_unclean_shutdown_via_wal() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("mailbox.db");
    let topic = pr_topic(3);

    {
        let storage = Storage::open(StorageConfig::at(&path)).await.unwrap();
        for i in 0..20u64 {
            storage
                .publish(
                    topic.clone(),
                    adapter(),
                    Timestamp(i as i64),
                    json!({ "i": i }),
                )
                .await
                .unwrap();
        }
        // Leak the handle: the writer thread keeps its connection open forever
        // and is never dropped/closed. This models an abrupt kill with the WAL
        // left in place — no clean checkpoint runs.
        std::mem::forget(storage);
    }

    // Reopen from scratch: WAL recovery must yield a consistent DB.
    let reopened = Storage::open(StorageConfig::at(&path)).await.unwrap();
    reopened
        .integrity_check()
        .await
        .expect("reopened DB must pass integrity_check");

    let page = reopened
        .read_events(topic, Cursor::Oldest, None)
        .await
        .unwrap();
    assert_eq!(page.events.len(), 20, "all committed rows must survive");
    for (i, event) in page.events.iter().enumerate() {
        assert_eq!(event.offset, Offset(i as u64));
    }
}

// ---- Acceptance criterion 4: fresh create + idempotent reopen ----------------

/// A fresh file gets the schema; reopening the same file is a clean no-op and
/// preserves data written before the reopen.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac4_fresh_create_then_idempotent_reopen() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("mailbox.db");
    let topic = pr_topic(4);

    {
        let storage = Storage::open(StorageConfig::at(&path)).await.unwrap();
        storage
            .publish(
                topic.clone(),
                adapter(),
                Timestamp(1),
                json!({"first": true}),
            )
            .await
            .unwrap();
        // Clean close this time (drops the channel; the writer thread finishes
        // and closes the connection). WAL lets the reopen below proceed even if
        // the old connection is still closing, so no sleep is needed.
        drop(storage);
    }

    // Second open on the up-to-date file migrates/no-ops cleanly and sees the
    // earlier row.
    let reopened = Storage::open(StorageConfig::at(&path)).await.unwrap();
    let page = reopened
        .read_events(topic, Cursor::Oldest, None)
        .await
        .unwrap();
    assert_eq!(page.events.len(), 1);

    // A third open is also fine (idempotent migration).
    drop(reopened);
    let third = Storage::open(StorageConfig::at(&path)).await.unwrap();
    third.integrity_check().await.unwrap();
}

// ---- Cursor independence -----------------------------------------------------

/// Two subscribers on the same topic advance their delivery cursors
/// independently — driven through the supported read-and-advance path (the only
/// way a delivery cursor moves now that `advance_cursor` is gone).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_subscribers_have_independent_cursors() {
    let (storage, _dir) = fresh_store().await;
    let topic = pr_topic(5);
    let alice = SessionId::new("alice".to_string());
    let bob = SessionId::new("bob".to_string());

    // Subscribe on the empty topic so both baseline to "from the start".
    storage
        .subscribe_and_baseline(alice.clone(), topic.clone())
        .await
        .unwrap();
    storage
        .subscribe_and_baseline(bob.clone(), topic.clone())
        .await
        .unwrap();

    for i in 0..5u64 {
        storage
            .publish(
                topic.clone(),
                adapter(),
                Timestamp(i as i64),
                json!({ "i": i }),
            )
            .await
            .unwrap();
    }

    // Each reads a different amount, advancing its own cursor to its last event.
    let alice_page = storage.read_unread(alice.clone(), Some(4)).await.unwrap();
    assert_eq!(alice_page.len(), 4); // offsets 0..3
    let bob_page = storage.read_unread(bob.clone(), Some(2)).await.unwrap();
    assert_eq!(bob_page.len(), 2); // offsets 0,1

    assert_eq!(
        storage.cursor(alice, topic.clone()).await.unwrap(),
        Some(Offset(3))
    );
    assert_eq!(
        storage.cursor(bob, topic.clone()).await.unwrap(),
        Some(Offset(1))
    );

    // Unknown subscriber has no cursor.
    let carol = SessionId::new("carol".to_string());
    assert_eq!(storage.cursor(carol, topic).await.unwrap(), None);
}

/// Reading strictly after a cursor returns only newer events, and the returned
/// `next` cursor continues correctly.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn read_after_cursor_returns_only_newer_events() {
    let (storage, _dir) = fresh_store().await;
    let topic = pr_topic(6);
    for i in 0..5u64 {
        storage
            .publish(
                topic.clone(),
                adapter(),
                Timestamp(i as i64),
                json!({ "i": i }),
            )
            .await
            .unwrap();
    }

    let page = storage
        .read_events(topic.clone(), Cursor::After { offset: Offset(2) }, None)
        .await
        .unwrap();
    let offsets: Vec<u64> = page.events.iter().map(|e| e.offset.0).collect();
    assert_eq!(offsets, vec![3, 4]);
    assert_eq!(page.next, Cursor::After { offset: Offset(4) });

    // Paging with a limit; next cursor continues where the page ended.
    let page1 = storage
        .read_events(topic.clone(), Cursor::Oldest, Some(2))
        .await
        .unwrap();
    assert_eq!(page1.events.len(), 2);
    let page2 = storage
        .read_events(topic, page1.next, Some(2))
        .await
        .unwrap();
    let offsets2: Vec<u64> = page2.events.iter().map(|e| e.offset.0).collect();
    assert_eq!(offsets2, vec![2, 3]);
}

// ---- Watch upsert + refcounted interest --------------------------------------

fn watch_spec(pr: u64) -> WatchSpec {
    WatchSpec {
        kind: WatchKind::GithubPr,
        repo: "octocat/hello-world".to_string(),
        pr,
        interval: Duration::from_secs(60),
    }
}

/// Upsert is idempotent by (kind, repo, pr): the same entity yields the same id
/// and does not reset a running state.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn watch_upsert_is_idempotent_by_entity() {
    let (storage, _dir) = fresh_store().await;

    let id1 = storage.upsert_watch(watch_spec(42)).await.unwrap();
    // Mark it running.
    storage
        .set_watch_state(
            id1,
            WatchState::Running {
                pid: mailbox::storage::Pid(1234),
            },
        )
        .await
        .unwrap();

    // A second session watching the same PR reuses the row and its state.
    let id2 = storage.upsert_watch(watch_spec(42)).await.unwrap();
    assert_eq!(id1, id2);

    let watch = storage.get_watch(id1).await.unwrap().unwrap();
    assert_eq!(watch.pr, 42);
    assert_eq!(watch.kind, WatchKind::GithubPr);
    assert_eq!(
        watch.state,
        WatchState::Running {
            pid: mailbox::storage::Pid(1234)
        }
    );

    // A different PR is a different watch.
    let other = storage.upsert_watch(watch_spec(43)).await.unwrap();
    assert_ne!(id1, other);
}

/// Interest is refcounted: the count tracks distinct sessions, add is
/// idempotent per session, and it only reaches 0 when the last session leaves.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn interest_refcount_add_remove() {
    let (storage, _dir) = fresh_store().await;
    let watch = storage.upsert_watch(watch_spec(7)).await.unwrap();
    let s1 = SessionId::new("s1".to_string());
    let s2 = SessionId::new("s2".to_string());

    assert_eq!(storage.add_interest(watch, s1.clone()).await.unwrap(), 1);
    // Idempotent: re-adding the same session does not double-count.
    assert_eq!(storage.add_interest(watch, s1.clone()).await.unwrap(), 1);
    assert_eq!(storage.add_interest(watch, s2.clone()).await.unwrap(), 2);
    assert_eq!(storage.interest_count(watch).await.unwrap(), 2);

    // First session leaving does NOT drop to zero — the watcher must survive.
    assert_eq!(storage.remove_interest(watch, s1.clone()).await.unwrap(), 1);
    // Removing again is idempotent.
    assert_eq!(storage.remove_interest(watch, s1).await.unwrap(), 1);
    // Last session leaving drops to zero (teardown signal).
    assert_eq!(storage.remove_interest(watch, s2).await.unwrap(), 0);
}

/// Baseline round-trips as opaque JSON and upserts in place.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn baseline_set_get_roundtrip() {
    let (storage, _dir) = fresh_store().await;
    let watch = storage.upsert_watch(watch_spec(8)).await.unwrap();

    assert_eq!(storage.get_baseline(watch).await.unwrap(), None);

    let baseline = json!({ "mergeable": "CONFLICTING", "ci": { "failing": 2 } });
    storage.set_baseline(watch, baseline.clone()).await.unwrap();
    assert_eq!(storage.get_baseline(watch).await.unwrap(), Some(baseline));

    // Upsert overwrites.
    let updated = json!({ "mergeable": "MERGEABLE" });
    storage.set_baseline(watch, updated.clone()).await.unwrap();
    assert_eq!(storage.get_baseline(watch).await.unwrap(), Some(updated));
}

// ---- subscribe / unsubscribe -------------------------------------------------

/// Subscribe (via the baselining path — the only supported one) is idempotent
/// (no duplicate row, no error, cursor untouched on repeat); unsubscribe removes
/// the row; unsubscribing something not subscribed is a harmless no-op.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn subscribe_unsubscribe_semantics() {
    use mailbox::storage::SubscribeOutcome;

    let dir = TempDir::new().unwrap();
    let path = dir.path().join("mailbox.db");
    let storage = Storage::open(StorageConfig::at(&path)).await.unwrap();
    let topic = pr_topic(30);
    let session = SessionId::new("s".to_string());

    let count = || count_rows(&path, "SELECT COUNT(*) FROM subscription");

    let first = storage
        .subscribe_and_baseline(session.clone(), topic.clone())
        .await
        .unwrap();
    // Empty topic, so a fresh subscription with no baseline.
    assert_eq!(first, SubscribeOutcome::Subscribed { baseline: None });
    assert_eq!(count(), 1);

    // Double-subscribe: no error, no duplicate, reported as an idempotent no-op.
    let again = storage
        .subscribe_and_baseline(session.clone(), topic.clone())
        .await
        .unwrap();
    assert_eq!(again, SubscribeOutcome::AlreadySubscribed);
    assert_eq!(count(), 1);

    // Unsubscribe removes the row.
    storage
        .unsubscribe(session.clone(), topic.clone())
        .await
        .unwrap();
    assert_eq!(count(), 0);

    // Unsubscribing again (nonexistent) is a no-op, not an error.
    storage.unsubscribe(session, topic).await.unwrap();
    assert_eq!(count(), 0);
}

// ---- Offset range guard (confirmed bug fix) ----------------------------------

/// A read after an out-of-range cursor returns an empty page (not a wrapped
/// replay of the whole log). (The write-side guard — rejecting an out-of-range
/// offset rather than storing a negative — is covered by the writer unit test
/// `offset_conversions_reject_out_of_range`.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn out_of_range_offsets_do_not_wrap() {
    let (storage, _dir) = fresh_store().await;
    let topic = pr_topic(31);
    for i in 0..3u64 {
        storage
            .publish(
                topic.clone(),
                adapter(),
                Timestamp(i as i64),
                json!({ "i": i }),
            )
            .await
            .unwrap();
    }

    // Reading "after" a garbage/huge cursor must NOT replay the log.
    let page = storage
        .read_events(
            topic.clone(),
            Cursor::After {
                offset: Offset(u64::MAX),
            },
            None,
        )
        .await
        .unwrap();
    assert!(page.events.is_empty());
}

// ---- Future schema version rejection (end to end) ----------------------------

/// Opening a DB whose schema version is newer than we understand fails cleanly
/// with `UnsupportedSchemaVersion` (the writer thread reports the error and
/// exits — `open` returns `Err`, nothing is left running).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn open_rejects_future_schema_version() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("mailbox.db");

    // Stamp a from-the-future version with a throwaway connection, then close it.
    {
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute_batch("PRAGMA user_version = 999;").unwrap();
    }

    let err = Storage::open(StorageConfig::at(&path)).await.unwrap_err();
    assert!(matches!(
        err,
        StorageError::UnsupportedSchemaVersion { found: 999, .. }
    ));
}

// ---- Acceptance criterion 3 (real crash): SIGKILL a child mid-write ----------

/// Env var carrying the DB path into the child "publish forever" mode.
const CRASH_CHILD_DB_ENV: &str = "MAILBOX_CRASH_CHILD_DB";

/// Hidden child mode. In a normal test run (env var unset) this returns
/// immediately and does nothing. When the parent re-invokes THIS test binary
/// with the env var set, it becomes a process that opens the store and publishes
/// in a tight loop forever — until the parent SIGKILLs it mid-write.
#[tokio::test]
async fn crash_writer_child_mode() {
    let Ok(path) = std::env::var(CRASH_CHILD_DB_ENV) else {
        return;
    };
    let storage = Storage::open(StorageConfig::at(path)).await.unwrap();
    let topic = pr_topic(99);
    let mut i: u64 = 0;
    loop {
        storage
            .publish(
                topic.clone(),
                adapter(),
                Timestamp(i as i64),
                json!({ "i": i }),
            )
            .await
            .unwrap();
        i += 1;
    }
}

/// Genuine crash recovery: spawn a child that publishes in a loop, SIGKILL it
/// mid-write (no clean shutdown, WAL left in place), then reopen and prove the
/// DB is consistent AND the surviving events are a CONTIGUOUS prefix `0..k` —
/// the interrupted, uncommitted publish is rolled back by WAL recovery, so
/// there are no gaps or duplicates across the crash boundary.
///
/// Unlike a `mem::forget` of an idle handle, this actually interrupts an
/// in-flight write, so it would fail if commits were not atomic/durable.
#[test]
fn ac3_real_crash_recovery_survives_sigkill() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("mailbox.db");

    let exe = std::env::current_exe().expect("current exe");
    let mut child = std::process::Command::new(exe)
        .args([
            "--exact",
            "crash_writer_child_mode",
            "--nocapture",
            "--test-threads",
            "1",
        ])
        .env(CRASH_CHILD_DB_ENV, &path)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn child publisher");

    // Let the child open the store and commit a burst of rows.
    std::thread::sleep(Duration::from_millis(1000));

    // `Child::kill` sends SIGKILL on Unix — a genuine crash mid-write.
    child.kill().expect("kill child");
    let _ = child.wait();

    // Reopen (async) and verify recovery.
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let storage = Storage::open(StorageConfig::at(&path)).await.unwrap();
        storage
            .integrity_check()
            .await
            .expect("reopened DB must pass integrity_check");

        let topic = pr_topic(99);
        let mut expected: u64 = 0;
        let mut cursor = Cursor::Oldest;
        loop {
            let page = storage
                .read_events(topic.clone(), cursor, Some(1000))
                .await
                .unwrap();
            if page.events.is_empty() {
                break;
            }
            for event in &page.events {
                assert_eq!(event.offset, Offset(expected), "gap/dup across crash");
                expected += 1;
            }
            cursor = page.next;
        }
        assert!(
            expected >= 1,
            "child should have committed at least one event before the kill"
        );
    });
}
