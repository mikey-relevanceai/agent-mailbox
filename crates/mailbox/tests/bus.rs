//! Bus integration tests: publish / subscribe / cursor delivery semantics.
//!
//! These exercise the session-facing bus over real SQLite in a tempdir (never
//! the real home directory), because the exactly-once and cursor assumptions
//! live at this level and are best proven against the real store (mikey-in-a-box
//! testing-strategy: real DB over mocks for in-project data). Card 04's four
//! acceptance criteria each have a named `ac*` test; the rest cover multi-topic
//! reads, limits, concurrency, and failure isolation surfaced in review.

use std::path::Path;

use mailbox::bus::{Bus, SessionId, SubscribeOutcome};
use mailbox::storage::{Storage, StorageConfig};
use mailbox_protocol::{AdapterId, Cursor, Event, GithubPr, Offset, Timestamp, Topic};
use serde_json::json;
use tempfile::TempDir;

/// Open a fresh bus over a store in a tempdir. Returns the raw [`Storage`] handle
/// (to assert on the durable log / cursors directly), the DB path (for raw
/// fault-injection), and the dir (kept alive so the DB files outlive the store).
async fn fresh_bus() -> (Bus, Storage, std::path::PathBuf, TempDir) {
    let dir = TempDir::new().expect("tempdir");
    let path = dir.path().join("mailbox.db");
    let storage = Storage::open(StorageConfig::at(&path))
        .await
        .expect("open storage");
    (Bus::new(storage.clone()), storage, path, dir)
}

fn pr_topic(n: u64) -> Topic {
    GithubPr::new("octocat", "hello-world", n).unwrap().topic()
}

fn adapter() -> AdapterId {
    AdapterId("github-watch".to_string())
}

fn session(name: &str) -> SessionId {
    SessionId::new(name.to_string())
}

/// Publish `count` events to `topic`, bodies `{ "i": 0.. }`.
async fn publish_n(bus: &Bus, topic: &Topic, count: u64) {
    for i in 0..count {
        bus.publish(
            topic.clone(),
            adapter(),
            Timestamp(i as i64),
            json!({ "i": i }),
            None,
        )
        .await
        .unwrap();
    }
}

/// The offsets carried by a slice of events, in order.
fn offsets(events: &[Event]) -> Vec<u64> {
    events.iter().map(|e| e.offset.0).collect()
}

/// Read a session's unread events and return their offsets, in order.
async fn read_offsets(bus: &Bus, session: &SessionId, limit: Option<u32>) -> Vec<u64> {
    offsets(bus.read(session.clone(), limit).await.unwrap().events())
}

/// Insert a raw event row directly into the DB file (bypassing the bus) to model
/// a corrupt/poison row. A negative offset trips the write-side offset guard; an
/// invalid JSON body trips the read-side deserialize. Uses its own connection —
/// a quick write while the (idle) bridge writer is not mid-transaction.
fn raw_insert_event(path: &Path, topic: &str, offset: i64, body: &str) {
    let conn = rusqlite::Connection::open(path).unwrap();
    conn.execute(
        "INSERT INTO event (topic, offset, event_id, adapter, timestamp, body)
         VALUES (?1, ?2, ?3, 'raw', 0, ?4)",
        rusqlite::params![topic, offset, format!("evt-raw-{topic}-{offset}"), body],
    )
    .unwrap();
}

// ---- AC1: two subscribers, one topic, each event exactly once ----------------

/// Two subscribers on one topic each receive every event EXACTLY ONCE, with
/// independent cursors: one reading does not consume the other's events, and a
/// second read by the same session returns nothing (no duplicate delivery).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac1_two_subscribers_each_receive_every_event_exactly_once() {
    let (bus, _storage, _path, _dir) = fresh_bus().await;
    let topic = pr_topic(1);
    let alice = session("alice");
    let bob = session("bob");

    // Both subscribe to the (empty) topic BEFORE any publish, so the baseline is
    // "from the start" and every event below is a post-subscribe event.
    bus.subscribe(alice.clone(), std::slice::from_ref(&topic))
        .await
        .unwrap();
    bus.subscribe(bob.clone(), std::slice::from_ref(&topic))
        .await
        .unwrap();

    publish_n(&bus, &topic, 3).await;

    // Alice reads all three, once.
    assert_eq!(read_offsets(&bus, &alice, None).await, vec![0, 1, 2]);
    // Independent cursor: Bob still sees all three despite Alice having read.
    assert_eq!(read_offsets(&bus, &bob, None).await, vec![0, 1, 2]);
    // Exactly once: a second read by Alice returns nothing new.
    assert!(bus.read(alice.clone(), None).await.unwrap().is_empty());

    // A further publish reaches both, once each, independently.
    publish_n(&bus, &topic, 1).await; // offset 3
    assert_eq!(read_offsets(&bus, &alice, None).await, vec![3]);
    assert_eq!(read_offsets(&bus, &bob, None).await, vec![3]);
}

// ---- AC2: mid-turn publish surfaces on the NEXT read -------------------------

/// A publish that arrives after a read (i.e. while the subscriber is mid-turn
/// reacting to the first read) is surfaced on that subscriber's NEXT read — not
/// lost, and not delivered a second time.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac2_mid_turn_publish_surfaces_on_next_read() {
    let (bus, _storage, _path, _dir) = fresh_bus().await;
    let topic = pr_topic(2);
    let alice = session("alice");

    bus.subscribe(alice.clone(), std::slice::from_ref(&topic))
        .await
        .unwrap();

    // First event; the read that starts the turn consumes it and advances cursor.
    publish_n(&bus, &topic, 1).await; // offset 0
    assert_eq!(read_offsets(&bus, &alice, None).await, vec![0]);

    // A publish lands mid-turn (after the read that started the turn).
    bus.publish(
        topic.clone(),
        adapter(),
        Timestamp(1),
        json!({ "mid": "turn" }),
        None,
    )
    .await
    .unwrap(); // offset 1

    // It is surfaced on the NEXT read, exactly once — not re-delivering offset 0.
    assert_eq!(read_offsets(&bus, &alice, None).await, vec![1]);
    // And nothing lingers.
    assert!(bus.read(alice, None).await.unwrap().is_empty());
}

// ---- AC3: unsubscribe stops delivery but keeps the durable log ---------------

/// Unsubscribe stops FUTURE delivery to that session but does NOT delete the
/// durable log: other subscribers keep receiving, and a later re-subscribe works.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac3_unsubscribe_stops_delivery_but_log_persists() {
    let (bus, storage, _path, _dir) = fresh_bus().await;
    let topic = pr_topic(3);
    let alice = session("alice");
    let bob = session("bob");

    bus.subscribe(alice.clone(), std::slice::from_ref(&topic))
        .await
        .unwrap();
    bus.subscribe(bob.clone(), std::slice::from_ref(&topic))
        .await
        .unwrap();

    publish_n(&bus, &topic, 1).await; // offset 0

    // Alice unsubscribes (before reading offset 0).
    bus.unsubscribe(alice.clone(), std::slice::from_ref(&topic))
        .await
        .unwrap();

    publish_n(&bus, &topic, 1).await; // offset 1

    // Delivery stopped for Alice: she is no longer subscribed, so read is empty.
    assert!(bus.read(alice.clone(), None).await.unwrap().is_empty());
    // Bob is unaffected: the durable log is intact, he sees both events.
    assert_eq!(read_offsets(&bus, &bob, None).await, vec![0, 1]);

    // The durable log itself still has both events (unsubscribe deleted no data).
    let page = storage
        .read_events(topic.clone(), Cursor::Oldest, None)
        .await
        .unwrap();
    assert_eq!(offsets(&page.events), vec![0, 1]);

    // Re-subscribe works and baselines to head (offset 1): no replay of 0/1.
    bus.subscribe(alice.clone(), std::slice::from_ref(&topic))
        .await
        .unwrap();
    assert!(bus.read(alice.clone(), None).await.unwrap().is_empty());

    // A fresh publish after re-subscribe is delivered normally.
    bus.publish(
        topic,
        adapter(),
        Timestamp(2),
        json!({ "after": "resub" }),
        None,
    )
    .await
    .unwrap(); // offset 2
    assert_eq!(read_offsets(&bus, &alice, None).await, vec![2]);
}

// ---- AC4: baseline-on-subscribe (no history replay) --------------------------

/// A brand-new subscriber does NOT replay history: events published before it
/// subscribed are never delivered; only post-subscribe events are.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac4_baseline_on_subscribe_does_not_replay_history() {
    let (bus, _storage, _path, _dir) = fresh_bus().await;
    let topic = pr_topic(4);
    let alice = session("alice");

    // History exists BEFORE Alice subscribes.
    publish_n(&bus, &topic, 2).await; // offsets 0, 1

    // The reported baseline is the current head (offset 1).
    let summary = bus
        .subscribe(alice.clone(), std::slice::from_ref(&topic))
        .await
        .unwrap();
    assert_eq!(
        summary,
        vec![(
            topic.clone(),
            SubscribeOutcome::Subscribed {
                baseline: Some(Offset(1))
            }
        )]
    );

    // First read replays nothing — the baseline sat at the head.
    assert!(bus.read(alice.clone(), None).await.unwrap().is_empty());

    // Only events published AFTER the subscribe are delivered.
    bus.publish(topic, adapter(), Timestamp(2), json!({ "i": 2 }), None)
        .await
        .unwrap(); // offset 2
    assert_eq!(read_offsets(&bus, &alice, None).await, vec![2]);
}

/// Re-subscribe baselines to the CURRENT head: events published during the
/// unsubscribed gap are not replayed on the next read.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac4_resubscribe_baselines_to_head() {
    let (bus, _storage, _path, _dir) = fresh_bus().await;
    let topic = pr_topic(5);
    let alice = session("alice");

    bus.subscribe(alice.clone(), std::slice::from_ref(&topic))
        .await
        .unwrap();
    publish_n(&bus, &topic, 1).await; // offset 0
    assert_eq!(read_offsets(&bus, &alice, None).await, vec![0]);

    // Unsubscribe, then events flow while Alice is not subscribed.
    bus.unsubscribe(alice.clone(), std::slice::from_ref(&topic))
        .await
        .unwrap();
    publish_n(&bus, &topic, 2).await; // offsets 1, 2 (published during the gap)

    // Re-subscribe: baseline jumps to the current head (offset 2). The gap events
    // are NOT replayed even though Alice's old cursor was at offset 0.
    bus.subscribe(alice.clone(), std::slice::from_ref(&topic))
        .await
        .unwrap();
    assert!(bus.read(alice.clone(), None).await.unwrap().is_empty());

    // Post-re-subscribe events flow again.
    bus.publish(topic, adapter(), Timestamp(9), json!({ "i": 3 }), None)
        .await
        .unwrap(); // offset 3
    assert_eq!(read_offsets(&bus, &alice, None).await, vec![3]);
}

/// Idempotent re-subscribe while STILL subscribed must not skip unread events:
/// a repeat subscribe is a no-op that leaves the cursor untouched. (Guards the
/// boundary between "baseline on a fresh subscribe" and "idempotent subscribe".)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resubscribe_while_subscribed_does_not_skip_unread() {
    let (bus, _storage, _path, _dir) = fresh_bus().await;
    let topic = pr_topic(6);
    let alice = session("alice");

    bus.subscribe(alice.clone(), std::slice::from_ref(&topic))
        .await
        .unwrap();
    publish_n(&bus, &topic, 2).await; // offsets 0, 1 (unread by Alice)

    // Subscribing again while already subscribed must NOT baseline past 0/1.
    let summary = bus
        .subscribe(alice.clone(), std::slice::from_ref(&topic))
        .await
        .unwrap();
    assert_eq!(
        summary,
        vec![(topic.clone(), SubscribeOutcome::AlreadySubscribed)]
    );

    assert_eq!(read_offsets(&bus, &alice, None).await, vec![0, 1]);
}

// ---- Read across multiple topics for one session ----------------------------

/// One read returns unread events across ALL a session's subscribed topics, in
/// deterministic (topic, offset) order, advancing each topic's cursor.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn read_spans_all_subscribed_topics() {
    let (bus, _storage, _path, _dir) = fresh_bus().await;
    let t_low = pr_topic(10);
    let t_high = pr_topic(11);
    // Confirm the (topic, offset) ordering assumption the read relies on.
    assert!(t_low.as_str() < t_high.as_str());
    let alice = session("alice");

    bus.subscribe(alice.clone(), &[t_low.clone(), t_high.clone()])
        .await
        .unwrap();

    publish_n(&bus, &t_high, 1).await; // t_high offset 0
    publish_n(&bus, &t_low, 2).await; // t_low offsets 0, 1

    let delivery = bus.read(alice.clone(), None).await.unwrap();
    // Grouped by topic (ascending), offsets ascending within a topic.
    let seen: Vec<(&str, u64)> = delivery
        .events()
        .iter()
        .map(|e| (e.topic.as_str(), e.offset.0))
        .collect();
    assert_eq!(
        seen,
        vec![
            (t_low.as_str(), 0),
            (t_low.as_str(), 1),
            (t_high.as_str(), 0),
        ]
    );

    // Cursors on both topics advanced: a second read is empty.
    assert!(bus.read(alice, None).await.unwrap().is_empty());
}

/// Multi-topic + per-topic `limit`: a busy topic's page cap must not starve a
/// quiet topic in the same read, and the busy topic's remainder pages onto the
/// following reads.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn read_limit_is_per_topic_and_does_not_starve_other_topics() {
    let (bus, _storage, _path, _dir) = fresh_bus().await;
    let busy = pr_topic(40);
    let quiet = pr_topic(41);
    assert!(busy.as_str() < quiet.as_str());
    let alice = session("alice");

    bus.subscribe(alice.clone(), &[busy.clone(), quiet.clone()])
        .await
        .unwrap();
    publish_n(&bus, &busy, 5).await; // busy offsets 0..4
    publish_n(&bus, &quiet, 1).await; // quiet offset 0

    // limit=2 is PER TOPIC: busy yields 2, quiet still yields its 1 (not starved).
    let first = bus.read(alice.clone(), Some(2)).await.unwrap();
    let first_seen: Vec<(&str, u64)> = first
        .events()
        .iter()
        .map(|e| (e.topic.as_str(), e.offset.0))
        .collect();
    assert_eq!(
        first_seen,
        vec![(busy.as_str(), 0), (busy.as_str(), 1), (quiet.as_str(), 0)]
    );

    // The busy topic's remainder pages onto subsequent reads; quiet is now drained.
    let second = bus.read(alice.clone(), Some(2)).await.unwrap();
    let second_seen: Vec<(&str, u64)> = second
        .events()
        .iter()
        .map(|e| (e.topic.as_str(), e.offset.0))
        .collect();
    assert_eq!(second_seen, vec![(busy.as_str(), 2), (busy.as_str(), 3)]);

    assert_eq!(read_offsets(&bus, &alice, Some(2)).await, vec![4]);
}

/// A session subscribed to an empty topic and a non-empty one: only the non-empty
/// delivers, with no error and no spurious cursor row on the empty topic.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn read_with_one_empty_topic_delivers_only_the_nonempty() {
    let (bus, storage, _path, _dir) = fresh_bus().await;
    let full = pr_topic(50);
    let empty = pr_topic(51);
    let alice = session("alice");

    bus.subscribe(alice.clone(), &[full.clone(), empty.clone()])
        .await
        .unwrap();
    publish_n(&bus, &full, 1).await; // full offset 0

    assert_eq!(read_offsets(&bus, &alice, None).await, vec![0]);
    // The empty topic never got a cursor row (advance only happens on delivery).
    assert_eq!(storage.cursor(alice, empty).await.unwrap(), None);
}

// ---- Failure isolation (poison topic) ----------------------------------------

/// A corrupt row on ONE topic must not starve a session's other topics: the read
/// skips the bad topic (logged, its transaction rolled back) and still delivers
/// the healthy topics, returning Ok rather than erroring out the whole session.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn poison_topic_does_not_starve_healthy_topics() {
    let (bus, _storage, path, _dir) = fresh_bus().await;
    let a = pr_topic(60);
    let b = pr_topic(61);
    let c = pr_topic(62);
    let alice = session("alice");

    bus.subscribe(alice.clone(), &[a.clone(), b.clone(), c.clone()])
        .await
        .unwrap();
    publish_n(&bus, &a, 1).await; // a offset 0
    publish_n(&bus, &b, 1).await; // b offset 0

    // Poison C: a row whose body is not valid JSON (fails the read's deserialize).
    raw_insert_event(&path, c.as_str(), 0, "this is not json");

    // The read succeeds and delivers A and B; C is skipped, not fatal.
    let delivery = bus.read(alice.clone(), None).await.unwrap();
    let topics: Vec<&str> = delivery.events().iter().map(|e| e.topic.as_str()).collect();
    assert!(topics.contains(&a.as_str()), "A must deliver");
    assert!(topics.contains(&b.as_str()), "B must deliver");
    assert!(!topics.contains(&c.as_str()), "poisoned C must be skipped");
    assert_eq!(delivery.len(), 2);
}

// ---- Bus::subscribe partial failure ------------------------------------------

/// When a later topic in a `subscribe` batch errors, the call returns `Err`, but
/// the topics processed before it are already durably subscribed (each committed
/// in its own writer command).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn subscribe_partial_failure_leaves_earlier_topics_subscribed() {
    let (bus, storage, path, _dir) = fresh_bus().await;
    let good = pr_topic(70);
    let bad = pr_topic(71);
    let alice = session("alice");

    // Poison the `bad` topic's log with a negative offset so its baseline
    // (SELECT MAX(offset)) trips the offset guard (Corrupt) inside
    // subscribe_and_baseline, rolling back only that topic's subscription.
    raw_insert_event(&path, bad.as_str(), -1, "{}");

    let result = bus
        .subscribe(alice.clone(), &[good.clone(), bad.clone()])
        .await;
    assert!(result.is_err(), "the bad topic must fail the batch");

    // `good` was processed first and is durably subscribed despite the later error:
    // a fresh publish to it is delivered (which is only possible if subscribed).
    publish_n(&bus, &good, 1).await;
    assert_eq!(read_offsets(&bus, &alice, None).await, vec![0]);

    // And `bad` did NOT leave a stray subscription (its transaction rolled back).
    assert_eq!(storage.cursor(alice, bad).await.unwrap(), None);
}

// ---- Empty read --------------------------------------------------------------

/// Reading with nothing unread returns an empty delivery (the idle-wake case),
/// and reading with no subscriptions at all is likewise empty — never an error.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn empty_read_when_nothing_unread() {
    let (bus, _storage, _path, _dir) = fresh_bus().await;
    let topic = pr_topic(20);
    let alice = session("alice");

    // No subscriptions yet: empty, not an error.
    assert!(bus.read(alice.clone(), None).await.unwrap().is_empty());

    // Subscribed but nothing published: still empty.
    bus.subscribe(alice.clone(), std::slice::from_ref(&topic))
        .await
        .unwrap();
    assert!(bus.read(alice.clone(), None).await.unwrap().is_empty());

    // After reading everything, the next read is empty again.
    publish_n(&bus, &topic, 1).await;
    assert_eq!(read_offsets(&bus, &alice, None).await, vec![0]);
    assert!(bus.read(alice, None).await.unwrap().is_empty());
}

/// A per-topic `limit` caps a single read's page; the remainder is not lost — it
/// surfaces on the next read via the advanced cursor.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn read_limit_pages_the_remainder_onto_next_read() {
    let (bus, _storage, _path, _dir) = fresh_bus().await;
    let topic = pr_topic(21);
    let alice = session("alice");

    bus.subscribe(alice.clone(), std::slice::from_ref(&topic))
        .await
        .unwrap();
    publish_n(&bus, &topic, 5).await; // offsets 0..4

    assert_eq!(read_offsets(&bus, &alice, Some(2)).await, vec![0, 1]);
    assert_eq!(read_offsets(&bus, &alice, Some(2)).await, vec![2, 3]);
    assert_eq!(read_offsets(&bus, &alice, Some(2)).await, vec![4]);
}

// ---- Concurrency regressions (single-writer serialization) -------------------

/// Two reads for the SAME session racing must together deliver every event
/// exactly once — union equals the published set, with zero overlap. The single
/// writer serializes the two `read_unread` commands, so one wins the batch and
/// the other sees the remainder; neither double-delivers.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_reads_for_one_session_never_double_deliver() {
    let (bus, _storage, _path, _dir) = fresh_bus().await;
    let topic = pr_topic(80);
    let alice = session("alice");

    bus.subscribe(alice.clone(), std::slice::from_ref(&topic))
        .await
        .unwrap();
    publish_n(&bus, &topic, 5).await; // offsets 0..4

    let (r1, r2) = tokio::join!(bus.read(alice.clone(), None), bus.read(alice.clone(), None));

    let mut seen: Vec<u64> = offsets(r1.unwrap().events());
    seen.extend(offsets(r2.unwrap().events()));
    // Drain anything the split left behind.
    seen.extend(read_offsets(&bus, &alice, None).await);
    seen.sort_unstable();

    // Every event exactly once: union == published, no duplicates.
    assert_eq!(seen, vec![0, 1, 2, 3, 4]);
}

/// A publish racing a read is never lost or double-delivered: whichever order the
/// single writer serializes them, the event is delivered exactly once across the
/// racing read plus a follow-up drain.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn publish_racing_a_read_delivers_exactly_once() {
    let (bus, _storage, _path, _dir) = fresh_bus().await;
    let topic = pr_topic(81);
    let alice = session("alice");

    bus.subscribe(alice.clone(), std::slice::from_ref(&topic))
        .await
        .unwrap();

    let (pub_res, read_res) = tokio::join!(
        bus.publish(
            topic.clone(),
            adapter(),
            Timestamp(0),
            json!({ "i": 0 }),
            None
        ),
        bus.read(alice.clone(), None)
    );
    pub_res.unwrap();

    let mut seen: Vec<u64> = offsets(read_res.unwrap().events());
    // Follow-up drain picks it up if the read was serialized first.
    seen.extend(read_offsets(&bus, &alice, None).await);
    seen.sort_unstable();

    assert_eq!(seen, vec![0], "the single event is delivered exactly once");
}
