//! Watch-supervision acceptance tests (card 08), driven against REAL child
//! processes through the real storage/bus/host.
//!
//! ac-09-3: the happy-path ACs (a normal long-running adapter) now drive the
//! REAL reference stub adapter (`mailbox-stub-adapter`) via
//! [`StubResolverFixture`] — the stub is the canonical happy-path adapter. The
//! `test_adapter` fixture is retained only for the edge cases the stub does not
//! model: repeated crash, ignore-SIGTERM, and never-spawns (nonexistent) paths.
//!
//! Proves the core product guarantee — one bridge-owned adapter per external
//! entity, alive exactly while some session cares — end to end:
//!
//! - AC1 two sessions watch one PR → ONE child; both receive events.
//! - AC2 first session ends → child still running; second still gets edges.
//! - AC3 last session leaves → child gone; zero further activity.
//! - AC4 kill the adapter with interest > 0 → exactly one restart; N crashes →
//!   give up + error event; interest 0 → stays stopped.
//! - AC5 bridge restart with no live interested session → not resumed.
//! - TTL sweeper drops a stale interest and stops the adapter.
//!
//! Flakiness discipline: poll with bounded timeouts (never fixed sleeps waiting
//! for a state), use a fast restart policy + short sweep windows, and assert no
//! orphan process survives (pid reaped). The supervisor is always shut down so
//! its adapters are reaped before the test ends.

use std::sync::Arc;
use std::time::Duration;

use mailbox::bus::Bus;
use mailbox::host::AdapterConfig;
use mailbox::host::subprocess::AdapterSpec;
use mailbox::storage::{Cursor, SessionId, Storage, StorageConfig, Watch, WatchId, WatchState};
use mailbox::supervisor::{
    AdapterResolver, ResolveError, ResolvedAdapter, RestartPolicy, Supervisor, reconcile_startup,
    topic_for_watch,
};
use mailbox::watch::{drop_interest, record};
use mailbox_protocol::{AdapterId, GithubPr, Topic};
use serde_json::json;
use tempfile::TempDir;

// ---- fixtures -----------------------------------------------------------------

/// A resolver that runs the `test_adapter` fixture in a chosen mode, targeting
/// each watch's own topic (so published events land where the sessions listen).
struct FixtureResolver {
    program: String,
    mode: &'static str,
    /// Extra config merged into the fixture config (e.g. interval_ms).
    extra: serde_json::Value,
    /// Extra argv (the fixture ignores unknown args); used to stamp a unique
    /// marker so a spawned child can be found — or not — via `pgrep -f`.
    args: Vec<String>,
}

impl FixtureResolver {
    fn crash() -> Self {
        Self {
            program: fixture_program(),
            mode: "crash",
            extra: json!({ "count": 0 }),
            args: Vec::new(),
        }
    }

    /// A resolver whose program does not exist, so every `SubprocessTransport::
    /// start` fails at spawn — the repeated-start-failure give-up path.
    fn nonexistent() -> Self {
        Self {
            program: "/nonexistent/mailbox-adapter-does-not-exist".to_string(),
            mode: "sleep",
            extra: json!({}),
            args: Vec::new(),
        }
    }

    /// Stamp a unique marker into the child's argv (discoverable via `pgrep -f`).
    fn with_marker(mut self, marker: &str) -> Self {
        self.args.push(marker.to_string());
        self
    }
}

impl AdapterResolver for FixtureResolver {
    fn resolve(&self, watch: &Watch) -> Result<ResolvedAdapter, ResolveError> {
        let topic = topic_for_watch(watch).ok_or_else(|| {
            ResolveError::Invalid(format!("bad repo {:?}", watch.target.repo_column()))
        })?;
        let mut config = json!({ "mode": self.mode, "topic": topic.as_str() });
        if let (Some(obj), Some(extra)) = (config.as_object_mut(), self.extra.as_object()) {
            for (k, v) in extra {
                obj.insert(k.clone(), v.clone());
            }
        }
        Ok(ResolvedAdapter {
            spec: AdapterSpec::new(self.program.clone(), AdapterId("fixture".to_string()))
                .with_args(self.args.clone()),
            config: AdapterConfig::new(config),
        })
    }
}

fn fixture_program() -> String {
    env!("CARGO_BIN_EXE_test_adapter").to_string()
}

/// A unique marker so a spawned child can be found (or asserted gone) with
/// `pgrep -f` without matching anything else on the machine.
fn unique_marker(tag: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("mbx-sup-{tag}-{}-{}", std::process::id(), nanos)
}

/// Whether any process currently has `marker` in its command line.
fn marker_present(marker: &str) -> bool {
    std::process::Command::new("pgrep")
        .arg("-f")
        .arg(marker)
        .output()
        .map(|out| out.status.success())
        .unwrap_or(false)
}

/// A fast policy so restart/give-up assertions run in well under a second.
fn fast_policy() -> RestartPolicy {
    RestartPolicy {
        max_consecutive_failures: 3,
        base_backoff: Duration::from_millis(30),
        max_backoff: Duration::from_millis(100),
        // Large so a crash streak accumulates rather than being reset as "stable".
        reset_after: Duration::from_secs(3600),
        stop_grace: Duration::from_millis(300),
    }
}

async fn fresh_storage() -> (Storage, TempDir) {
    let dir = TempDir::new().expect("tempdir");
    let storage = Storage::open(StorageConfig::at(dir.path().join("mailbox.db")))
        .await
        .expect("open storage");
    (storage, dir)
}

async fn fresh<R: AdapterResolver>(resolver: R) -> (Bus, Storage, Supervisor, TempDir) {
    fresh_with(resolver, fast_policy()).await
}

async fn fresh_with<R: AdapterResolver>(
    resolver: R,
    policy: RestartPolicy,
) -> (Bus, Storage, Supervisor, TempDir) {
    let (storage, dir) = fresh_storage().await;
    let bus = Bus::new(storage.clone());
    let supervisor = Supervisor::spawn(
        storage.clone(),
        bus.clone(),
        Arc::new(resolver) as Arc<dyn AdapterResolver>,
        policy,
    );
    (bus, storage, supervisor, dir)
}

/// The reference stub adapter binary (`mailbox-stub-adapter`), built if missing.
///
/// ac-09-3: the happy-path supervision ACs drive the REAL stub adapter (not the
/// ad-hoc `test_adapter` fixture), locating it beside the `test_adapter` bin in
/// the shared target dir. `cargo test --workspace` builds it during the build
/// phase; the on-demand build is a fallback for `cargo test -p mailbox` alone.
fn stub_program() -> String {
    let dir = std::path::Path::new(env!("CARGO_BIN_EXE_test_adapter"))
        .parent()
        .expect("test_adapter bin has a parent dir")
        .to_path_buf();
    let bin = dir.join("mailbox-stub-adapter");
    if !bin.exists() {
        let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string());
        let status = std::process::Command::new(cargo)
            .args(["build", "-p", "mailbox-stub-adapter"])
            .status()
            .expect("build mailbox-stub-adapter");
        assert!(status.success(), "failed to build mailbox-stub-adapter");
    }
    bin.to_str().expect("stub bin path is utf8").to_string()
}

/// A resolver that runs the REAL reference stub adapter, publishing to each
/// watch's own topic every `interval_ms` forever. This is the canonical
/// happy-path long-running adapter (ac-09-3) — a drop-in for the old
/// `FixtureResolver::interval`, but exercising the shipped binary.
struct StubResolverFixture {
    program: String,
    interval_ms: u64,
    count: u64,
}

impl StubResolverFixture {
    /// Publish forever every `period_ms` (the supervised steady-stream case).
    fn interval(period_ms: u64) -> Self {
        Self {
            program: stub_program(),
            interval_ms: period_ms,
            count: 0,
        }
    }

    /// Publish exactly `count` events then exit cleanly (a FINITE adapter). Used
    /// to prove a clean exit-0 is terminal, not a crash-restart.
    fn finite(period_ms: u64, count: u64) -> Self {
        Self {
            program: stub_program(),
            interval_ms: period_ms,
            count,
        }
    }
}

impl AdapterResolver for StubResolverFixture {
    fn resolve(&self, watch: &Watch) -> Result<ResolvedAdapter, ResolveError> {
        let topic = topic_for_watch(watch).ok_or_else(|| {
            ResolveError::Invalid(format!("bad repo {:?}", watch.target.repo_column()))
        })?;
        let config = json!({
            "topic": topic.as_str(),
            "interval_ms": self.interval_ms,
            "count": self.count,
        });
        Ok(ResolvedAdapter {
            spec: AdapterSpec::new(self.program.clone(), AdapterId("stub-fixture".to_string())),
            config: AdapterConfig::new(config),
        })
    }
}

fn pr(n: u64) -> GithubPr {
    GithubPr::new("octocat", "hello-world", n).unwrap()
}

/// The single watch id in the store (these tests only ever create one).
async fn only_watch_id(storage: &Storage) -> WatchId {
    let watches = storage.list_watches().await.unwrap();
    assert_eq!(watches.len(), 1, "expected exactly one watch");
    watches[0].id
}

async fn watch_state(storage: &Storage, id: WatchId) -> WatchState {
    storage.get_watch(id).await.unwrap().unwrap().state
}

async fn durable_count(storage: &Storage, topic: &Topic) -> usize {
    storage
        .read_events(topic.clone(), Cursor::Oldest, None)
        .await
        .unwrap()
        .events
        .len()
}

/// `kill(pid, 0)`: true while the pid still names a live (non-reaped) process.
fn pid_alive(pid: u32) -> bool {
    matches!(
        nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid as i32), None),
        Ok(())
    )
}

/// Poll `f` until it returns `Some`, or panic after a bounded wait.
async fn poll_until<T, F, Fut>(what: &str, mut f: F) -> T
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Option<T>>,
{
    for _ in 0..300 {
        if let Some(value) = f().await {
            return value;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("condition never held: {what}");
}

async fn assert_pid_reaped(pid: u32) {
    poll_until("adapter pid reaped", || async move {
        (!pid_alive(pid)).then_some(())
    })
    .await;
}

// ---- AC1 ----------------------------------------------------------------------

/// Two sessions watch the same PR → ONE child process; both receive events.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac1_two_sessions_one_child_both_receive_events() {
    let (bus, storage, supervisor, _dir) = fresh(StubResolverFixture::interval(20)).await;
    let watched = pr(1);
    let topic = watched.topic();

    let s1 = SessionId::new("s1");
    let s2 = SessionId::new("s2");
    let first = record(
        &bus,
        &storage,
        &supervisor,
        &watched,
        Duration::from_secs(60),
        s1.clone(),
    )
    .await
    .unwrap();
    assert_eq!(first.interest, 1);
    let second = record(
        &bus,
        &storage,
        &supervisor,
        &watched,
        Duration::from_secs(60),
        s2.clone(),
    )
    .await
    .unwrap();
    assert_eq!(
        second.interest, 2,
        "second session attaches to the shared watch"
    );

    // Exactly one watch row, and exactly one running child (same pid across both
    // records — the second reused it rather than spawning another).
    let watch_id = only_watch_id(&storage).await;
    let pid = poll_until("adapter running", || {
        let s = supervisor.clone();
        async move { s.running_pid(watch_id).await }
    })
    .await;
    assert!(pid_alive(pid), "the one child is running");
    assert!(matches!(
        watch_state(&storage, watch_id).await,
        WatchState::Running { .. }
    ));

    // Both subscribed sessions receive the interval adapter's events.
    poll_until("s1 receives events", || {
        let (bus, s1) = (bus.clone(), s1.clone());
        async move { (!bus.read(s1, None).await.unwrap().is_empty()).then_some(()) }
    })
    .await;
    poll_until("s2 receives events", || {
        let (bus, s2) = (bus.clone(), s2.clone());
        async move { (!bus.read(s2, None).await.unwrap().is_empty()).then_some(()) }
    })
    .await;

    supervisor.shutdown().await.unwrap();
    assert_pid_reaped(pid).await;
    let _ = topic;
}

// ---- AC2 + AC3 ----------------------------------------------------------------

/// First session ends → child still running; the second still gets new events.
/// Then the last session leaves → child gone; zero further activity.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac2_ac3_child_survives_first_leaver_then_dies_with_last() {
    let (bus, storage, supervisor, _dir) = fresh(StubResolverFixture::interval(20)).await;
    let watched = pr(2);
    let topic = watched.topic();
    let s1 = SessionId::new("s1");
    let s2 = SessionId::new("s2");

    record(
        &bus,
        &storage,
        &supervisor,
        &watched,
        Duration::from_secs(60),
        s1.clone(),
    )
    .await
    .unwrap();
    record(
        &bus,
        &storage,
        &supervisor,
        &watched,
        Duration::from_secs(60),
        s2.clone(),
    )
    .await
    .unwrap();
    let watch_id = only_watch_id(&storage).await;
    let pid = poll_until("adapter running", || {
        let s = supervisor.clone();
        async move { s.running_pid(watch_id).await }
    })
    .await;

    // AC2: first session drops interest → child STILL running (one session cares).
    let dropped = drop_interest(&bus, &storage, &supervisor, &watched, s1)
        .await
        .unwrap();
    assert_eq!(
        dropped.outcome,
        mailbox::watch::UnwatchOutcome::Dropped {
            remaining_interest: 1
        }
    );
    assert_eq!(
        supervisor.running_pid(watch_id).await,
        Some(pid),
        "the same child keeps running for the remaining session"
    );
    assert!(pid_alive(pid));

    // And s2 still receives edges published after s1 left.
    poll_until("s2 still receives events after s1 left", || {
        let (bus, s2) = (bus.clone(), s2.clone());
        async move { (!bus.read(s2, None).await.unwrap().is_empty()).then_some(()) }
    })
    .await;

    // AC3: last session leaves → child gone.
    let dropped = drop_interest(&bus, &storage, &supervisor, &watched, s2)
        .await
        .unwrap();
    assert_eq!(
        dropped.outcome,
        mailbox::watch::UnwatchOutcome::Dropped {
            remaining_interest: 0
        }
    );
    assert_pid_reaped(pid).await;
    poll_until("watch marked stopped", || {
        let storage = storage.clone();
        async move { (watch_state(&storage, watch_id).await == WatchState::Stopped).then_some(()) }
    })
    .await;
    assert_eq!(supervisor.running_pid(watch_id).await, None);

    // Zero further activity: the durable log stops growing once the child is gone.
    let count_after_stop = durable_count(&storage, &topic).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        durable_count(&storage, &topic).await,
        count_after_stop,
        "a stopped adapter publishes nothing further"
    );

    supervisor.shutdown().await.unwrap();
}

// ---- AC4: crash → restart / give-up -------------------------------------------

/// Killing the adapter while interest > 0 triggers exactly one restart (a new
/// live pid); after the last interest is dropped it stays stopped.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac4_crash_with_interest_restarts_once() {
    let (bus, storage, supervisor, _dir) = fresh(StubResolverFixture::interval(20)).await;
    let watched = pr(3);
    let s1 = SessionId::new("s1");

    record(
        &bus,
        &storage,
        &supervisor,
        &watched,
        Duration::from_secs(60),
        s1.clone(),
    )
    .await
    .unwrap();
    let watch_id = only_watch_id(&storage).await;
    let pid1 = poll_until("adapter running", || {
        let s = supervisor.clone();
        async move { s.running_pid(watch_id).await }
    })
    .await;

    // Kill the child out from under the supervisor: interest is still 1, so it
    // must restart with a NEW pid.
    nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(pid1 as i32),
        nix::sys::signal::Signal::SIGKILL,
    )
    .unwrap();
    let pid2 = poll_until("adapter restarted with a new pid", || {
        let s = supervisor.clone();
        async move {
            match s.running_pid(watch_id).await {
                Some(p) if p != pid1 => Some(p),
                _ => None,
            }
        }
    })
    .await;
    assert_ne!(pid1, pid2);
    assert!(pid_alive(pid2), "the restarted child is running");

    // The restarted interval adapter is stable: the pid does not keep churning.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        supervisor.running_pid(watch_id).await,
        Some(pid2),
        "no further restarts once the replacement is stable"
    );

    // Interest 0 → stays stopped (no zombie restart of a wanted-by-nobody watch).
    drop_interest(&bus, &storage, &supervisor, &watched, s1)
        .await
        .unwrap();
    assert_pid_reaped(pid2).await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(
        supervisor.running_pid(watch_id).await,
        None,
        "with no interest the adapter stays stopped"
    );

    supervisor.shutdown().await.unwrap();
}

/// An adapter that keeps crashing exhausts the restart budget: the supervisor
/// gives up, marks the watch Failed, and publishes an error event on the topic.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac4_repeated_crash_gives_up_and_publishes_error() {
    let (bus, storage, supervisor, _dir) = fresh(FixtureResolver::crash()).await;
    let watched = pr(4);
    let topic = watched.topic();
    let s1 = SessionId::new("s1");

    record(
        &bus,
        &storage,
        &supervisor,
        &watched,
        Duration::from_secs(60),
        s1,
    )
    .await
    .unwrap();
    let watch_id = only_watch_id(&storage).await;

    // After N consecutive crashes the supervisor gives up → Failed.
    poll_until("watch marked failed", || {
        let storage = storage.clone();
        async move { (watch_state(&storage, watch_id).await == WatchState::Failed).then_some(()) }
    })
    .await;

    assert_eq!(
        supervisor.running_pid(watch_id).await,
        None,
        "a given-up watch has no running adapter"
    );

    // A give-up error event was published on the entity topic by the supervisor.
    let events = storage
        .read_events(topic.clone(), Cursor::Oldest, None)
        .await
        .unwrap()
        .events;
    let giveup = events
        .iter()
        .find(|e| e.body.get("event").and_then(|v| v.as_str()) == Some("adapter_gave_up"));
    assert!(
        giveup.is_some(),
        "the supervisor must publish a give-up error event; got {events:?}"
    );

    supervisor.shutdown().await.unwrap();
}

// ---- clean exit-0 is terminal, NOT a crash-restart (card-09 review, item A) ----

/// A supervised FINITE adapter (`stub --count N`) that publishes its batch and
/// exits 0 while interest is still held is DONE, not crashed: the batch is
/// published EXACTLY ONCE, the watch ends `Stopped` (never `Failed`), and no
/// `adapter_gave_up` event is surfaced. Paired with `ac4_crash_with_interest_
/// restarts_once` (a crash — signal death — DOES restart), this pins the
/// clean-exit-vs-crash distinction so it cannot silently regress.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn clean_finite_exit_is_terminal_not_restarted() {
    const COUNT: u64 = 3;
    let (bus, storage, supervisor, _dir) = fresh(StubResolverFixture::finite(15, COUNT)).await;
    let watched = pr(11);
    let topic = watched.topic();
    let s1 = SessionId::new("s1");

    // Interest is HELD for the whole test (never dropped), so a naive supervisor
    // would treat the clean exit as a crash and restart.
    record(
        &bus,
        &storage,
        &supervisor,
        &watched,
        Duration::from_secs(60),
        s1,
    )
    .await
    .unwrap();
    let watch_id = only_watch_id(&storage).await;

    // The finite adapter publishes its batch then exits 0 → the watch becomes
    // Stopped (terminal), NOT restarted, even though interest remains.
    poll_until("finite adapter completed and marked stopped", || {
        let storage = storage.clone();
        async move { (watch_state(&storage, watch_id).await == WatchState::Stopped).then_some(()) }
    })
    .await;

    // Exactly one batch: the durable log holds precisely COUNT events (no
    // republish loop), and it stays there.
    assert_eq!(durable_count(&storage, &topic).await, COUNT as usize);
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        durable_count(&storage, &topic).await,
        COUNT as usize,
        "a completed finite adapter must not be restarted and republish its batch"
    );

    // It must NOT have been marked Failed, and no give-up event was surfaced.
    assert_eq!(watch_state(&storage, watch_id).await, WatchState::Stopped);
    let events = storage
        .read_events(topic.clone(), Cursor::Oldest, None)
        .await
        .unwrap()
        .events;
    assert!(
        !events
            .iter()
            .any(|e| e.body.get("event").and_then(|v| v.as_str()) == Some("adapter_gave_up")),
        "a healthy finite adapter must not produce a give-up event; got {events:?}"
    );

    supervisor.shutdown().await.unwrap();
}

// ---- AC5: bridge restart, no resume -------------------------------------------

/// On bridge restart a previously-running watch is NOT resumed: reconcile marks
/// it stopped and clears the pid, even though an interest row survives (there is
/// no session-liveness probe yet, so the fail-safe is "do not resume").
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac5_bridge_restart_does_not_resume_watch() {
    let (storage, dir) = fresh_storage().await;

    // Simulate the pre-restart state: a running watch with a surviving interest.
    let watch_id = storage
        .upsert_watch(mailbox::storage::WatchSpec {
            target: mailbox::storage::WatchTarget::GithubPr {
                repo: "octocat/hello-world".to_string(),
                pr: 5,
            },
            interval: Duration::from_secs(60),
        })
        .await
        .unwrap();
    storage
        .set_watch_state(
            watch_id,
            WatchState::Running {
                pid: mailbox::storage::Pid::new(999_999),
            },
        )
        .await
        .unwrap();
    storage
        .add_interest(watch_id, SessionId::new("s1"), 1_000)
        .await
        .unwrap();

    // Bridge restart.
    reconcile_startup(&storage).await.unwrap();

    assert_eq!(
        watch_state(&storage, watch_id).await,
        WatchState::Stopped,
        "a previously-running watch is marked stopped, not resumed"
    );
    // The stored child_pid column is cleared to NULL (a Stopped watch carries no
    // pid — the storage model would reject a Stopped row that still had one).
    assert_eq!(
        raw_child_pid(dir.path(), watch_id),
        None,
        "reconcile must clear the child pid"
    );
    // And the interest row survives (we do not delete interest on restart) but no
    // adapter was started — nothing to assert-alive because none was spawned.
    assert_eq!(storage.interest_count(watch_id).await.unwrap(), 1);
}

/// `reconcile_startup` touches ONLY previously-running watches: a mix of
/// Desired/Running/Stopped leaves Desired and Stopped untouched and marks the
/// Running one Stopped.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reconcile_startup_only_touches_running_watches() {
    let (storage, _dir) = fresh_storage().await;

    let mk = |n: u64| mailbox::storage::WatchSpec {
        target: mailbox::storage::WatchTarget::GithubPr {
            repo: "octocat/hello-world".to_string(),
            pr: n,
        },
        interval: Duration::from_secs(60),
    };
    let desired = storage.upsert_watch(mk(1)).await.unwrap();
    let running = storage.upsert_watch(mk(2)).await.unwrap();
    let stopped = storage.upsert_watch(mk(3)).await.unwrap();
    storage
        .set_watch_state(
            running,
            WatchState::Running {
                pid: mailbox::storage::Pid::new(4242),
            },
        )
        .await
        .unwrap();
    storage
        .set_watch_state(stopped, WatchState::Stopped)
        .await
        .unwrap();

    reconcile_startup(&storage).await.unwrap();

    assert_eq!(
        watch_state(&storage, desired).await,
        WatchState::Desired,
        "Desired untouched"
    );
    assert_eq!(
        watch_state(&storage, running).await,
        WatchState::Stopped,
        "Running -> Stopped"
    );
    assert_eq!(
        watch_state(&storage, stopped).await,
        WatchState::Stopped,
        "Stopped untouched"
    );
}

/// Read the raw `child_pid` column for a watch straight from the DB file.
fn raw_child_pid(db_dir: &std::path::Path, watch_id: WatchId) -> Option<i64> {
    let conn = rusqlite::Connection::open(db_dir.join("mailbox.db")).unwrap();
    conn.query_row(
        "SELECT child_pid FROM watch WHERE id = ?1",
        [watch_id.get()],
        |row| row.get::<_, Option<i64>>(0),
    )
    .unwrap()
}

// ---- TTL sweeper --------------------------------------------------------------

/// A fresh interest is not swept; a stale one is, and its adapter is stopped.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ttl_sweeper_drops_stale_interest_and_stops_adapter() {
    let (bus, storage, supervisor, _dir) = fresh(StubResolverFixture::interval(20)).await;
    let watched = pr(6);
    let s1 = SessionId::new("s1");

    record(
        &bus,
        &storage,
        &supervisor,
        &watched,
        Duration::from_secs(60),
        s1.clone(),
    )
    .await
    .unwrap();
    let watch_id = only_watch_id(&storage).await;
    let pid = poll_until("adapter running", || {
        let s = supervisor.clone();
        async move { s.running_pid(watch_id).await }
    })
    .await;

    // A fresh interest (last-seen stamped at `record`) is NOT stale under a 1s TTL.
    let swept = supervisor.sweep(Duration::from_secs(1)).await.unwrap();
    assert!(swept.is_empty(), "a fresh interest must not be swept");
    assert_eq!(supervisor.running_pid(watch_id).await, Some(pid));
    assert!(pid_alive(pid));

    // Backdate the interest's last-seen so it is now stale, then sweep.
    let old = mailbox::clock::now_millis() - 10_000;
    storage.touch_interest(watch_id, s1, old).await.unwrap();
    let swept = supervisor.sweep(Duration::from_secs(1)).await.unwrap();
    assert_eq!(swept, vec![watch_id], "the stale interest's watch is swept");

    assert_pid_reaped(pid).await;
    poll_until("watch stopped after sweep", || {
        let storage = storage.clone();
        async move { (watch_state(&storage, watch_id).await == WatchState::Stopped).then_some(()) }
    })
    .await;
    assert_eq!(supervisor.running_pid(watch_id).await, None);

    supervisor.shutdown().await.unwrap();
}

// ---- regression: teardown during a crash-restart backoff (proves A) -----------

/// Unwatching (or a sweep firing) DURING a crash-restart backoff window must leave
/// the watch authoritatively `Stopped` — not stuck `Running{dead pid}` — and the
/// pending restart must be cancelled so no adapter is resurrected.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unwatch_during_backoff_ends_stopped_no_restart() {
    // A crash adapter with a comfortable backoff window to unwatch inside.
    let policy = RestartPolicy {
        base_backoff: Duration::from_millis(400),
        ..fast_policy()
    };
    let marker = unique_marker("backoff");
    let (bus, storage, supervisor, _dir) =
        fresh_with(FixtureResolver::crash().with_marker(&marker), policy).await;
    let watched = pr(7);
    let s1 = SessionId::new("s1");

    record(
        &bus,
        &storage,
        &supervisor,
        &watched,
        Duration::from_secs(60),
        s1.clone(),
    )
    .await
    .unwrap();
    let watch_id = only_watch_id(&storage).await;

    // The crash adapter aborts on start; wait for the supervisor to enter the
    // backoff window (no adapter running, watch dropped out of Running).
    poll_until("entered restart backoff window", || {
        let (storage, s) = (storage.clone(), supervisor.clone());
        async move {
            let no_pid = s.running_pid(watch_id).await.is_none();
            let not_running = !matches!(
                watch_state(&storage, watch_id).await,
                WatchState::Running { .. }
            );
            (no_pid && not_running).then_some(())
        }
    })
    .await;

    // Unwatch (last interest) mid-backoff → authoritative Stopped + cancelled restart.
    drop_interest(&bus, &storage, &supervisor, &watched, s1)
        .await
        .unwrap();
    assert_eq!(watch_state(&storage, watch_id).await, WatchState::Stopped);

    // Wait well past the backoff: no restart may fire, so the watch stays Stopped,
    // nothing runs, and no marked process ever spawns.
    tokio::time::sleep(Duration::from_millis(700)).await;
    assert_eq!(
        watch_state(&storage, watch_id).await,
        WatchState::Stopped,
        "the watch must not revert to Running via a stale restart"
    );
    assert_eq!(supervisor.running_pid(watch_id).await, None);
    assert!(
        !marker_present(&marker),
        "a cancelled restart must not spawn a new adapter"
    );

    supervisor.shutdown().await.unwrap();
}

// ---- regression: repeated START failures give up (proves B) -------------------

/// An adapter whose program never spawns (every `start` fails) must accumulate
/// consecutive failures and give up within budget — reaching `Failed` and
/// publishing the give-up event — rather than hammering spawn forever.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repeated_start_failures_give_up() {
    let (bus, storage, supervisor, _dir) = fresh(FixtureResolver::nonexistent()).await;
    let watched = pr(8);
    let topic = watched.topic();
    let s1 = SessionId::new("s1");

    record(
        &bus,
        &storage,
        &supervisor,
        &watched,
        Duration::from_secs(60),
        s1,
    )
    .await
    .unwrap();
    let watch_id = only_watch_id(&storage).await;

    // Within the failure budget the supervisor gives up → Failed (it does NOT
    // spin forever resolving a nonexistent binary).
    poll_until("watch marked failed after repeated start failures", || {
        let storage = storage.clone();
        async move { (watch_state(&storage, watch_id).await == WatchState::Failed).then_some(()) }
    })
    .await;
    assert_eq!(supervisor.running_pid(watch_id).await, None);

    // The give-up error event was surfaced on the entity topic.
    let events = storage
        .read_events(topic.clone(), Cursor::Oldest, None)
        .await
        .unwrap()
        .events;
    assert!(
        events
            .iter()
            .any(|e| e.body.get("event").and_then(|v| v.as_str()) == Some("adapter_gave_up")),
        "a repeated start-failure must still publish the give-up event"
    );

    supervisor.shutdown().await.unwrap();
}

// ---- invariant: running_pid==None ⟹ state is never Running ---------------------

/// Once no adapter is running for a watch, its stored state is never left as
/// `Running{pid}` (which would advertise a dead pid). Checked across the full
/// lifecycle: spawn, stop, and the crash-backoff window.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn running_pid_none_implies_state_not_running() {
    let (bus, storage, supervisor, _dir) = fresh(StubResolverFixture::interval(20)).await;
    let watched = pr(9);
    let s1 = SessionId::new("s1");

    record(
        &bus,
        &storage,
        &supervisor,
        &watched,
        Duration::from_secs(60),
        s1.clone(),
    )
    .await
    .unwrap();
    let watch_id = only_watch_id(&storage).await;
    let pid = poll_until("adapter running", || {
        let s = supervisor.clone();
        async move { s.running_pid(watch_id).await }
    })
    .await;

    // Kill it and, over the churn of exit → backoff → restart, repeatedly assert
    // the invariant: whenever running_pid is None, state is not Running.
    nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(pid as i32),
        nix::sys::signal::Signal::SIGKILL,
    )
    .unwrap();
    for _ in 0..40 {
        if supervisor.running_pid(watch_id).await.is_none() {
            assert!(
                !matches!(
                    watch_state(&storage, watch_id).await,
                    WatchState::Running { .. }
                ),
                "no running adapter but state is Running{{pid}} (a dead pid would be advertised)"
            );
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // After a clean stop the same invariant holds.
    drop_interest(&bus, &storage, &supervisor, &watched, s1)
        .await
        .unwrap();
    poll_until("stopped after last interest", || {
        let (storage, s) = (storage.clone(), supervisor.clone());
        async move {
            (s.running_pid(watch_id).await.is_none()
                && watch_state(&storage, watch_id).await == WatchState::Stopped)
                .then_some(())
        }
    })
    .await;

    supervisor.shutdown().await.unwrap();
}

// ---- shutdown cancels an in-flight restart ------------------------------------

/// Shutting the supervisor down while a crash-restart backoff is pending must not
/// spawn a new adapter afterwards (the pending restart is dropped with the actor).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_during_backoff_spawns_nothing() {
    let policy = RestartPolicy {
        base_backoff: Duration::from_millis(400),
        ..fast_policy()
    };
    let marker = unique_marker("shutdown");
    let (bus, storage, supervisor, _dir) =
        fresh_with(FixtureResolver::crash().with_marker(&marker), policy).await;
    let watched = pr(10);
    let s1 = SessionId::new("s1");

    record(
        &bus,
        &storage,
        &supervisor,
        &watched,
        Duration::from_secs(60),
        s1,
    )
    .await
    .unwrap();
    let watch_id = only_watch_id(&storage).await;

    // Enter the backoff window (crash adapter aborted; restart scheduled).
    poll_until("entered restart backoff window", || {
        let s = supervisor.clone();
        async move { s.running_pid(watch_id).await.is_none().then_some(()) }
    })
    .await;

    // Shut down mid-backoff; the pending restart fires into a closed channel.
    supervisor.shutdown().await.unwrap();

    // Past the backoff, no adapter was spawned after shutdown.
    tokio::time::sleep(Duration::from_millis(700)).await;
    assert!(
        !marker_present(&marker),
        "no adapter may spawn after shutdown"
    );
}
