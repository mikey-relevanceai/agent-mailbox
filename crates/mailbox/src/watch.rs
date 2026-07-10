//! Watch orchestration: the business layer behind `mailbox watch/unwatch/status`.
//!
//! # Why this is a library module, not inline in the CLI
//!
//! Recording a watch is three coordinated steps — record the watch row
//! ([`Storage::upsert_watch`]), attach this session's refcounted interest
//! ([`Storage::add_interest`]), and subscribe the session to the PR topic so it
//! actually receives events and can be woken. Keeping that orchestration here
//! (rather than in the `mailbox` binary's socket handler) makes it unit-testable
//! in isolation and gives **card 08** (adapter process supervision) a direct
//! entry point: supervision hangs off the same interest refcount this module
//! maintains, so it will call `record`/`drop_interest` here rather than
//! re-deriving the logic.
//!
//! # card-06 ↔ card-08 boundary (now closed)
//!
//! Card 06 recorded intent only. **Card 08** makes [`record`] drive the
//! [`Supervisor`]: after attaching interest it calls
//! [`Supervisor::ensure_running`], which (given a resolvable adapter) spawns the
//! poller and marks the watch [`WatchState::Running`] with its child pid; and
//! [`drop_interest`] calls [`Supervisor::stop_watch`] on the last removal so the
//! adapter is torn down. In production the `serve` daemon injects an
//! [`crate::supervisor::UnavailableResolver`] until the real poller ships (card
//! 10), so a `github-pr` watch still sits `Desired` there — but the supervision
//! wiring is real and the pid is populated as soon as an adapter resolves.
//!
//! Layering stays one-way: `watch` → `supervisor` → {`host`, `bus`, `storage`},
//! plus `watch` → `bus`/`storage` directly for the watch/interest tables. It
//! speaks in domain types ([`WatchEntry`], [`UnwatchOutcome`], …); the wire
//! mapping lives in the binary.

use std::time::Duration;

use mailbox_protocol::{GithubPr, Topic};
use tracing::info;

use crate::bus::{Bus, BusError};
use crate::clock::now_millis;
use crate::storage::{
    SessionId, Storage, StorageError, SubscribeOutcome, WatchKind, WatchSpec, WatchState,
};
use crate::supervisor::{Supervisor, SupervisorError};

/// A failure recording or dropping a watch. A dedicated sum type so the CLI edge
/// sees one error surface and the three causes — a durable storage step, a bus
/// step, or driving the supervisor — stay distinguishable.
#[derive(Debug, thiserror::Error)]
pub enum WatchError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Bus(#[from] BusError),
    #[error(transparent)]
    Supervisor(#[from] SupervisorError),
}

/// The outcome of recording a watch: the PR topic, this session's interest
/// refcount after attaching, and how the (aligned) subscription resolved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatchRecorded {
    pub topic: Topic,
    pub interest: u64,
    pub subscribe: SubscribeOutcome,
}

/// The outcome of dropping a watch: always unsubscribes the session from the PR
/// topic; the interest side depends on whether a watch actually existed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatchDropped {
    pub topic: Topic,
    pub outcome: UnwatchOutcome,
}

/// What happened to the watch's interest on `unwatch`. A sum type so the
/// "there was no such watch" case cannot be confused with "dropped, zero left":
/// only the `Dropped` arm carries a remaining count.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnwatchOutcome {
    /// This session's interest was removed; `remaining_interest` sessions still
    /// care (0 authorizes card-08 teardown).
    Dropped { remaining_interest: u64 },
    /// No watch existed for this entity; nothing to drop (the session was still
    /// unsubscribed from the topic, idempotently).
    NoSuchWatch,
}

/// A status snapshot: every known watch plus one session's per-topic unread.
#[derive(Debug, Clone, PartialEq)]
pub struct StatusView {
    pub watches: Vec<WatchEntry>,
    pub unread: Vec<(Topic, u64)>,
}

/// One watch as `status` sees it, including its interest refcount and lifecycle
/// state (the `child_pid`, when card 08 sets one, lives inside
/// [`WatchState::Running`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatchEntry {
    pub kind: WatchKind,
    pub repo: String,
    pub pr: u64,
    pub interval: Duration,
    pub state: WatchState,
    pub interest: u64,
}

/// Record (or reuse) the watch for `pr`, attach `session`'s interest, and
/// subscribe it to the PR topic. Idempotent by `(kind, repo, pr)`: a second
/// session watching the same PR reuses the row and bumps the refcount.
///
/// Interest and subscription are recorded together because they are one user
/// intent ("wake me about this PR"): design/01 notes the subscription "may align
/// with watch_interest", and without the subscription a watch would (in card 08)
/// start a poller yet deliver nothing to the session.
pub async fn record(
    bus: &Bus,
    storage: &Storage,
    supervisor: &Supervisor,
    pr: &GithubPr,
    interval: Duration,
    session: SessionId,
) -> Result<WatchRecorded, WatchError> {
    let topic = pr.topic();
    let spec = WatchSpec {
        kind: WatchKind::GithubPr,
        repo: repo_of(pr),
        pr: pr.number(),
        interval,
    };

    let watch_id = storage.upsert_watch(spec).await?;
    // Stamp the interest's last-seen now so the TTL sweeper (card 08) has a fresh
    // liveness baseline; a re-watch refreshes it.
    let interest = storage
        .add_interest(watch_id, session.clone(), now_millis())
        .await?;
    info!(
        repo = %repo_of(pr),
        pr = pr.number(),
        interest,
        "attached session interest to watch"
    );
    let subscribe = subscribe_one(bus, session, &topic).await?;

    // Now that interest is attached, ask the supervisor to run the adapter. It is
    // idempotent (one adapter per entity), so a second session watching the same
    // PR reuses the running poller rather than spawning another.
    supervisor.ensure_running(watch_id).await?;

    Ok(WatchRecorded {
        topic,
        interest,
        subscribe,
    })
}

/// Drop `session`'s interest in the watch for `pr` and unsubscribe it from the
/// PR topic.
///
/// The watch id is resolved by scanning [`Storage::list_watches`] rather than
/// via `upsert_watch`, precisely so unwatching a PR nobody watches does not
/// *create* a phantom `Desired` row. The unsubscribe happens regardless (the
/// user asked to stop hearing about this PR); interest is only touched when a
/// watch exists.
pub async fn drop_interest(
    bus: &Bus,
    storage: &Storage,
    supervisor: &Supervisor,
    pr: &GithubPr,
    session: SessionId,
) -> Result<WatchDropped, WatchError> {
    let topic = pr.topic();
    let repo = repo_of(pr);

    let existing = storage
        .list_watches()
        .await?
        .into_iter()
        .find(|w| w.kind == WatchKind::GithubPr && w.repo == repo && w.pr == pr.number());

    let outcome = match existing {
        Some(watch) => {
            let remaining_interest = storage.remove_interest(watch.id, session.clone()).await?;
            info!(
                repo = %repo,
                pr = pr.number(),
                interest = remaining_interest,
                "dropped session interest from watch"
            );
            // The last interested session leaving is what authorizes teardown
            // (design/01 rule 5): stop the adapter and mark the watch stopped.
            if remaining_interest == 0 {
                supervisor.stop_watch(watch.id).await?;
            }
            UnwatchOutcome::Dropped { remaining_interest }
        }
        None => UnwatchOutcome::NoSuchWatch,
    };

    bus.unsubscribe(session, std::slice::from_ref(&topic))
        .await?;

    Ok(WatchDropped { topic, outcome })
}

/// Snapshot every watch (with interest counts + lifecycle state) and `session`'s
/// per-topic unread counts. A pure read: it advances no cursor.
pub async fn status(storage: &Storage, session: SessionId) -> Result<StatusView, BusError> {
    let mut watches = Vec::new();
    for watch in storage.list_watches().await? {
        let interest = storage.interest_count(watch.id).await?;
        watches.push(WatchEntry {
            kind: watch.kind,
            repo: watch.repo,
            pr: watch.pr,
            interval: watch.interval,
            state: watch.state,
            interest,
        });
    }
    let unread = storage.unread_counts(session).await?;
    Ok(StatusView { watches, unread })
}

/// `owner/repo`, the watch identity's repo segment.
fn repo_of(pr: &GithubPr) -> String {
    format!("{}/{}", pr.owner(), pr.repo())
}

/// Subscribe `session` to exactly one `topic` and pull out its single per-topic
/// outcome. `Bus::subscribe` returns one outcome per requested topic, so for a
/// single topic there is always exactly one.
async fn subscribe_one(
    bus: &Bus,
    session: SessionId,
    topic: &Topic,
) -> Result<SubscribeOutcome, BusError> {
    let mut summary = bus.subscribe(session, std::slice::from_ref(topic)).await?;
    // Exactly one topic requested ⇒ exactly one outcome; `expect` documents that
    // invariant rather than inventing a fallback for an impossible empty vec.
    let (_, outcome) = summary
        .pop()
        .expect("subscribe to one topic returns one outcome");
    Ok(outcome)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::StorageConfig;
    use crate::supervisor::{RestartPolicy, UnavailableResolver};
    use mailbox_protocol::{AdapterId, Timestamp};
    use std::sync::Arc;

    // These unit tests use the `UnavailableResolver`, so `record` records intent
    // and drives the supervisor but no adapter is spawned (the watch stays
    // `Desired`). The full spawn/stop lifecycle with a real fixture adapter is
    // exercised in `tests/supervision.rs`.
    async fn fresh() -> (Bus, Storage, Supervisor, tempfile::TempDir) {
        let dir = tempfile::TempDir::new().unwrap();
        let storage = Storage::open(StorageConfig::at(dir.path().join("mailbox.db")))
            .await
            .unwrap();
        let bus = Bus::new(storage.clone());
        let supervisor = Supervisor::spawn(
            storage.clone(),
            bus.clone(),
            Arc::new(UnavailableResolver),
            RestartPolicy::default(),
        );
        (bus, storage, supervisor, dir)
    }

    fn pr(n: u64) -> GithubPr {
        GithubPr::new("octocat", "hello-world", n).unwrap()
    }

    #[tokio::test]
    async fn record_is_idempotent_and_refcounts_interest() {
        let (bus, storage, supervisor, _dir) = fresh().await;
        let pr = pr(1);

        let first = record(
            &bus,
            &storage,
            &supervisor,
            &pr,
            Duration::from_secs(30),
            SessionId::new("s1"),
        )
        .await
        .unwrap();
        assert_eq!(first.interest, 1);

        // A second session shares the one watch and bumps interest to 2.
        let second = record(
            &bus,
            &storage,
            &supervisor,
            &pr,
            Duration::from_secs(30),
            SessionId::new("s2"),
        )
        .await
        .unwrap();
        assert_eq!(second.interest, 2);
        assert_eq!(
            storage.list_watches().await.unwrap().len(),
            1,
            "one shared watch"
        );
    }

    #[tokio::test]
    async fn drop_interest_reports_remaining_and_no_such_watch() {
        let (bus, storage, supervisor, _dir) = fresh().await;
        let watched = pr(1);
        record(
            &bus,
            &storage,
            &supervisor,
            &watched,
            Duration::from_secs(30),
            SessionId::new("s1"),
        )
        .await
        .unwrap();

        let dropped = drop_interest(&bus, &storage, &supervisor, &watched, SessionId::new("s1"))
            .await
            .unwrap();
        assert_eq!(
            dropped.outcome,
            UnwatchOutcome::Dropped {
                remaining_interest: 0
            }
        );

        // Unwatching a PR nobody watches is NoSuchWatch — and creates no row.
        let none = drop_interest(&bus, &storage, &supervisor, &pr(2), SessionId::new("s1"))
            .await
            .unwrap();
        assert_eq!(none.outcome, UnwatchOutcome::NoSuchWatch);
        assert_eq!(
            storage.list_watches().await.unwrap().len(),
            1,
            "unwatch must not create a phantom watch"
        );
    }

    #[tokio::test]
    async fn status_reports_watches_interest_and_unread() {
        let (bus, storage, supervisor, _dir) = fresh().await;
        let pr = pr(1);
        let recorded = record(
            &bus,
            &storage,
            &supervisor,
            &pr,
            Duration::from_secs(45),
            SessionId::new("s1"),
        )
        .await
        .unwrap();

        // An event after the watch/subscribe is unread for s1.
        bus.publish(
            recorded.topic.clone(),
            AdapterId("a".to_string()),
            Timestamp(0),
            serde_json::json!({"i": 0}),
        )
        .await
        .unwrap();

        let view = status(&storage, SessionId::new("s1")).await.unwrap();
        assert_eq!(view.watches.len(), 1);
        let entry = &view.watches[0];
        assert_eq!(entry.kind, WatchKind::GithubPr);
        assert_eq!(entry.repo, "octocat/hello-world");
        assert_eq!(entry.pr, 1);
        assert_eq!(entry.interest, 1);
        assert_eq!(
            entry.state,
            WatchState::Desired,
            "the UnavailableResolver spawns no adapter, so the watch stays desired"
        );
        assert_eq!(view.unread, vec![(recorded.topic, 1)]);
    }
}
