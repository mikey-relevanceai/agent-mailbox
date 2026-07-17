//! Storage integration tests.
//!
//! These exercise the durable core against real SQLite in a tempdir (never the
//! real home directory) — the store's assumptions live at this level, so we
//! prefer a real DB over mocks (mikey-in-a-box testing-strategy). Each of the
//! four acceptance criteria for card 03 has a named test below.

use std::path::Path;
use std::time::Duration;

use mailbox::storage::{
    SessionId, Storage, StorageConfig, StorageError, SubscribeKind, WatchSpec, WatchState,
    WatchTarget,
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
        .subscribe_and_baseline(
            alice.clone(),
            topic.clone(),
            mailbox::clock::now_millis(),
            SubscribeKind::Explicit,
        )
        .await
        .unwrap();
    storage
        .subscribe_and_baseline(
            bob.clone(),
            topic.clone(),
            mailbox::clock::now_millis(),
            SubscribeKind::Explicit,
        )
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
        target: WatchTarget::GithubPr {
            repo: "octocat/hello-world".to_string(),
            pr,
        },
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
                pid: mailbox::storage::Pid::new(1234),
            },
        )
        .await
        .unwrap();

    // A second session watching the same PR reuses the row and its state.
    let id2 = storage.upsert_watch(watch_spec(42)).await.unwrap();
    assert_eq!(id1, id2);

    let watch = storage.get_watch(id1).await.unwrap().unwrap();
    assert_eq!(
        watch.target,
        WatchTarget::GithubPr {
            repo: "octocat/hello-world".to_string(),
            pr: 42
        }
    );
    assert_eq!(
        watch.state,
        WatchState::Running {
            pid: mailbox::storage::Pid::new(1234)
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

    assert_eq!(
        storage.add_interest(watch, s1.clone(), 1000).await.unwrap(),
        1
    );
    // Idempotent: re-adding the same session does not double-count.
    assert_eq!(
        storage.add_interest(watch, s1.clone(), 2000).await.unwrap(),
        1
    );
    assert_eq!(
        storage.add_interest(watch, s2.clone(), 3000).await.unwrap(),
        2
    );
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
        .subscribe_and_baseline(
            session.clone(),
            topic.clone(),
            mailbox::clock::now_millis(),
            SubscribeKind::Explicit,
        )
        .await
        .unwrap();
    // Empty topic, so a fresh subscription with no baseline.
    assert_eq!(first, SubscribeOutcome::Subscribed { baseline: None });
    assert_eq!(count(), 1);

    // Double-subscribe: no error, no duplicate, reported as an idempotent no-op.
    let again = storage
        .subscribe_and_baseline(
            session.clone(),
            topic.clone(),
            mailbox::clock::now_millis(),
            SubscribeKind::Explicit,
        )
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

// ---- card-06 read additions: list_watches + unread_counts --------------------

/// `list_watches` enumerates every watch (there is no other way to discover a
/// watch id from its `(kind, repo, pr)` identity — the CLI `status`/`unwatch`
/// path depends on this).
#[tokio::test]
async fn list_watches_enumerates_all_watches() {
    let (storage, _dir) = fresh_store().await;
    assert!(storage.list_watches().await.unwrap().is_empty());

    let spec1 = WatchSpec {
        target: WatchTarget::GithubPr {
            repo: "octocat/hello-world".to_string(),
            pr: 1,
        },
        interval: Duration::from_secs(30),
    };
    let spec2 = WatchSpec {
        target: WatchTarget::GithubPr {
            repo: "octocat/hello-world".to_string(),
            pr: 2,
        },
        interval: Duration::from_secs(60),
    };
    storage.upsert_watch(spec1).await.unwrap();
    storage.upsert_watch(spec2).await.unwrap();

    let watches = storage.list_watches().await.unwrap();
    assert_eq!(watches.len(), 2);
    // A fresh watch is Desired with no child pid (card 06 never runs one).
    assert!(watches.iter().all(|w| w.state == WatchState::Desired));
    let prs: Vec<u64> = watches.iter().map(|w| w.target.pr_column()).collect();
    assert_eq!(prs, vec![1, 2], "stable id order");
}

/// `unread_counts` reports per-topic unread counts for a session without
/// advancing any cursor (status observes, never consumes).
#[tokio::test]
async fn unread_counts_reports_per_topic_and_does_not_consume() {
    let (storage, _dir) = fresh_store().await;
    let bus = mailbox::bus::Bus::new(storage.clone());
    let session = SessionId::new("s-status");
    let t1 = pr_topic(1);
    let t2 = pr_topic(2);
    bus.subscribe(session.clone(), &[t1.clone(), t2.clone()])
        .await
        .unwrap();

    // Two events on t1, one on t2 — all published after subscribe, so all unread.
    bus.publish(t1.clone(), adapter(), Timestamp(0), json!({"i": 0}))
        .await
        .unwrap();
    bus.publish(t1.clone(), adapter(), Timestamp(1), json!({"i": 1}))
        .await
        .unwrap();
    bus.publish(t2.clone(), adapter(), Timestamp(2), json!({"i": 2}))
        .await
        .unwrap();

    let counts = storage.unread_counts(session.clone()).await.unwrap();
    assert_eq!(
        counts,
        vec![(t1.clone(), 2), (t2.clone(), 1)],
        "per-topic unread counts, in topic order"
    );

    // Counting did NOT advance the cursor: a real read still returns all three.
    assert_eq!(bus.read(session.clone(), None).await.unwrap().len(), 3);
    // After reading, nothing is unread.
    assert!(storage.unread_counts(session).await.unwrap().is_empty());
}

// ---- card-08 additions: interest last-seen, touch, TTL sweep, v1->v2 ----------

/// `touch_interest` refreshes an existing interest but must NOT create or
/// resurrect a row for a session that is not interested.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn touch_interest_is_a_no_op_without_a_row() {
    let (storage, _dir) = fresh_store().await;
    let watch = storage.upsert_watch(watch_spec(11)).await.unwrap();
    let s1 = SessionId::new("s1");

    // No interest yet: touching creates nothing.
    storage
        .touch_interest(watch, s1.clone(), 5_000)
        .await
        .unwrap();
    assert_eq!(storage.interest_count(watch).await.unwrap(), 0);

    // Once interested, touching updates last_seen (observable via the sweeper).
    storage
        .add_interest(watch, s1.clone(), 1_000)
        .await
        .unwrap();
    storage.touch_interest(watch, s1, 9_000).await.unwrap();
    // A cutoff between the two stamps: with last_seen refreshed to 9000, a 5000
    // cutoff does not sweep it.
    assert!(
        storage
            .sweep_stale_interests(5_000)
            .await
            .unwrap()
            .is_empty()
    );
}

/// `touch_session_interests` refreshes ALL of a session's interests in one call
/// (the sweeper's per-session heartbeat, ADR-0009) and, like `touch_interest`,
/// never resurrects a dropped one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn touch_session_interests_refreshes_every_watch_the_session_holds() {
    let (storage, _dir) = fresh_store().await;
    let (a, b) = (
        storage.upsert_watch(watch_spec(21)).await.unwrap(),
        storage.upsert_watch(watch_spec(22)).await.unwrap(),
    );
    let (s1, s2) = (SessionId::new("s1"), SessionId::new("s2"));

    // s1 holds two interests; s2 holds one on the same watch as s1.
    storage.add_interest(a, s1.clone(), 1_000).await.unwrap();
    storage.add_interest(b, s1.clone(), 1_000).await.unwrap();
    storage.add_interest(b, s2.clone(), 1_000).await.unwrap();

    let refreshed = storage
        .touch_session_interests(s1.clone(), 9_000)
        .await
        .unwrap();
    assert_eq!(refreshed, 2, "both of s1's interests are refreshed at once");

    // A cutoff above the original stamp sweeps only s2's un-refreshed interest —
    // and watch `a` survives because s1's refresh spared it.
    let emptied = storage.sweep_stale_interests(5_000).await.unwrap();
    assert_eq!(emptied, vec![], "no watch is emptied: s1 still holds both");
    assert_eq!(storage.interest_count(a).await.unwrap(), 1);
    assert_eq!(
        storage.interest_count(b).await.unwrap(),
        1,
        "s2's stale interest is gone; s1's refreshed one remains"
    );

    // A session holding nothing is a no-op, not an error or a resurrection.
    let refreshed = storage
        .touch_session_interests(SessionId::new("ghost"), 9_000)
        .await
        .unwrap();
    assert_eq!(refreshed, 0);
}

/// `list_interest_sessions` reports each interested session once, however many
/// watches it holds — the sweeper probes a session's waiter, not its watches.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn list_interest_sessions_is_distinct() {
    let (storage, _dir) = fresh_store().await;
    let (a, b) = (
        storage.upsert_watch(watch_spec(31)).await.unwrap(),
        storage.upsert_watch(watch_spec(32)).await.unwrap(),
    );
    assert!(storage.list_interest_sessions().await.unwrap().is_empty());

    let (s1, s2) = (SessionId::new("s1"), SessionId::new("s2"));
    storage.add_interest(a, s1.clone(), 1_000).await.unwrap();
    storage.add_interest(b, s1.clone(), 1_000).await.unwrap();
    storage.add_interest(b, s2.clone(), 1_000).await.unwrap();

    let mut sessions = storage.list_interest_sessions().await.unwrap();
    sessions.sort_by(|x, y| x.as_str().cmp(y.as_str()));
    assert_eq!(
        sessions,
        vec![s1, s2],
        "s1 appears once despite two watches"
    );
}

/// The TTL sweep drops interests older than the cutoff and reports the watches
/// whose interest thereby reached zero.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sweep_stale_interests_reports_emptied_watches() {
    let (storage, _dir) = fresh_store().await;
    let watch = storage.upsert_watch(watch_spec(12)).await.unwrap();
    let fresh_session = SessionId::new("fresh");
    let stale_session = SessionId::new("stale");

    storage
        .add_interest(watch, fresh_session, 10_000)
        .await
        .unwrap();
    storage
        .add_interest(watch, stale_session, 1_000)
        .await
        .unwrap();

    // Cutoff 5000: only the stale (1000) interest is dropped; the watch still has
    // the fresh one, so it is NOT reported as emptied.
    assert!(
        storage
            .sweep_stale_interests(5_000)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(storage.interest_count(watch).await.unwrap(), 1);

    // A later cutoff sweeps the last interest → watch reported emptied.
    let emptied = storage.sweep_stale_interests(50_000).await.unwrap();
    assert_eq!(emptied, vec![watch]);
    assert_eq!(storage.interest_count(watch).await.unwrap(), 0);
}

/// A populated v1 database migrates to v2: existing rows survive, `last_seen`
/// defaults to 0 (the epoch), and such a pre-upgrade interest is immediately
/// TTL-sweep-eligible until refreshed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn populated_v1_db_migrates_to_v2() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("mailbox.db");

    // Build a real v1 DB by hand: the v1 DDL (no last_seen column), user_version=1,
    // and populated watch + watch_interest rows.
    {
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute_batch(
            r#"
            -- A real v1 DB has the event log too; these fixtures used to omit it because
            -- no migration touched it. v5 (the author stamp) ALTERs `event`, so the
            -- fixture must be a faithful v1 schema or the migration has nothing to alter.
            CREATE TABLE event (
                event_row_id INTEGER PRIMARY KEY AUTOINCREMENT,
                topic        TEXT    NOT NULL,
                offset       INTEGER NOT NULL,
                event_id     TEXT    NOT NULL UNIQUE,
                adapter      TEXT    NOT NULL,
                timestamp    INTEGER NOT NULL,
                body         TEXT    NOT NULL,
                UNIQUE(topic, offset)
            );
            CREATE INDEX idx_event_topic_offset ON event(topic, offset);
            CREATE TABLE watch (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                kind          TEXT    NOT NULL,
                repo          TEXT    NOT NULL,
                pr            INTEGER NOT NULL,
                interval_secs INTEGER NOT NULL,
                state         TEXT    NOT NULL,
                child_pid     INTEGER,
                UNIQUE(kind, repo, pr)
            );
            CREATE TABLE watch_interest (
                watch_id   INTEGER NOT NULL REFERENCES watch(id) ON DELETE CASCADE,
                session_id TEXT    NOT NULL,
                PRIMARY KEY (watch_id, session_id)
            );
            INSERT INTO watch (id, kind, repo, pr, interval_secs, state, child_pid)
                VALUES (1, 'github-pr', 'octocat/hello-world', 42, 60, 'desired', NULL);
            INSERT INTO watch_interest (watch_id, session_id) VALUES (1, 's1');
            PRAGMA user_version = 1;
            "#,
        )
        .unwrap();
    }

    // Open through the real Storage: this runs the v1->v2 migration.
    let storage = Storage::open(StorageConfig::at(&path)).await.unwrap();

    // The watch and its interest survived the migration.
    let watches = storage.list_watches().await.unwrap();
    assert_eq!(watches.len(), 1);
    // v3 renamed interval_secs -> interval_ms and backfilled *1000, so the 60s
    // pre-upgrade github interval is unchanged; the new publish_count defaults to
    // 0 (a github target carries no count).
    assert_eq!(
        watches[0].target,
        WatchTarget::GithubPr {
            repo: "octocat/hello-world".to_string(),
            pr: 42
        }
    );
    assert_eq!(watches[0].interval, Duration::from_secs(60));
    let watch = watches[0].id;
    assert_eq!(storage.interest_count(watch).await.unwrap(), 1);

    // The migrated interest has last_seen = 0 (the epoch), so any positive cutoff
    // sweeps it — a pre-upgrade interest is sweep-eligible until refreshed.
    let emptied = storage.sweep_stale_interests(1).await.unwrap();
    assert_eq!(emptied, vec![watch]);
    assert_eq!(storage.interest_count(watch).await.unwrap(), 0);
}

/// A populated **v2** database migrates to v3 (the realistic upgrade — v2 was the
/// pre-card-09 schema): the `interval_secs` column is renamed to `interval_ms`
/// and backfilled `*1000`, `publish_count` is added defaulting to 0, existing
/// rows survive, and re-opening (already v3) is a clean no-op.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn populated_v2_db_migrates_to_v3() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("mailbox.db");

    // Build a real v2 DB by hand: the v2 watch DDL (interval_secs, no
    // publish_count), watch_interest WITH last_seen, user_version=2, and a
    // populated github watch row.
    {
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute_batch(
            r#"
            -- A real v1 DB has the event log too; these fixtures used to omit it because
            -- no migration touched it. v5 (the author stamp) ALTERs `event`, so the
            -- fixture must be a faithful v1 schema or the migration has nothing to alter.
            CREATE TABLE event (
                event_row_id INTEGER PRIMARY KEY AUTOINCREMENT,
                topic        TEXT    NOT NULL,
                offset       INTEGER NOT NULL,
                event_id     TEXT    NOT NULL UNIQUE,
                adapter      TEXT    NOT NULL,
                timestamp    INTEGER NOT NULL,
                body         TEXT    NOT NULL,
                UNIQUE(topic, offset)
            );
            CREATE INDEX idx_event_topic_offset ON event(topic, offset);
            CREATE TABLE watch (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                kind          TEXT    NOT NULL,
                repo          TEXT    NOT NULL,
                pr            INTEGER NOT NULL,
                interval_secs INTEGER NOT NULL,
                state         TEXT    NOT NULL,
                child_pid     INTEGER,
                UNIQUE(kind, repo, pr)
            );
            CREATE TABLE watch_interest (
                watch_id   INTEGER NOT NULL REFERENCES watch(id) ON DELETE CASCADE,
                session_id TEXT    NOT NULL,
                last_seen  INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (watch_id, session_id)
            );
            INSERT INTO watch (id, kind, repo, pr, interval_secs, state, child_pid)
                VALUES (1, 'github-pr', 'octocat/hello-world', 42, 90, 'desired', NULL);
            INSERT INTO watch_interest (watch_id, session_id, last_seen) VALUES (1, 's1', 5000);
            PRAGMA user_version = 2;
            "#,
        )
        .unwrap();
    }

    // Open through the real Storage: this runs the v2->v3 migration.
    let storage = Storage::open(StorageConfig::at(&path)).await.unwrap();

    let watches = storage.list_watches().await.unwrap();
    assert_eq!(watches.len(), 1);
    assert_eq!(
        watches[0].target,
        WatchTarget::GithubPr {
            repo: "octocat/hello-world".to_string(),
            pr: 42
        }
    );
    // interval_secs 90 -> interval_ms 90000 (unchanged 90s), publish_count -> 0.
    assert_eq!(watches[0].interval, Duration::from_secs(90));
    assert_eq!(storage.interest_count(watches[0].id).await.unwrap(), 1);
    // The interest's last_seen survived (a positive cutoff below it does not sweep).
    let emptied = storage.sweep_stale_interests(1000).await.unwrap();
    assert!(
        emptied.is_empty(),
        "last_seen=5000 is newer than cutoff 1000"
    );

    // Re-opening an already-v3 DB is a clean no-op (idempotent migration).
    drop(storage);
    let storage = Storage::open(StorageConfig::at(&path)).await.unwrap();
    let watches = storage.list_watches().await.unwrap();
    assert_eq!(watches.len(), 1);
    assert_eq!(watches[0].interval, Duration::from_secs(90));
}
