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
//! - AC5 bridge restart: resumed iff an interested session's watcher is alive;
//!   not resumed (and the stale pid cleared) when none is.
//! - TTL sweeper drops a stale interest and stops the adapter.
//! - ADR-0026 an ended session's watch is suspended, and a resume of the same
//!   session id restarts its adapter.
//! - ADR-0023 a give-up is announced once per outage however many sweeps retry it,
//!   withdrawn by one recovery event, and re-armed for the next outage.
//!
//! Flakiness discipline: poll with bounded timeouts (never fixed sleeps waiting
//! for a state), use a fast restart policy + short sweep windows, and assert no
//! orphan process survives (pid reaped). The supervisor is always shut down so
//! its adapters are reaped before the test ends.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use mailbox::bus::Bus;
use mailbox::host::AdapterConfig;
use mailbox::host::subprocess::AdapterSpec;
use mailbox::storage::{Cursor, SessionId, Storage, StorageConfig, Watch, WatchId, WatchState};
use mailbox::supervisor::{
    AdapterResolver, EVENT_ADAPTER_GAVE_UP, EVENT_ADAPTER_RECOVERED, ResolveError, ResolvedAdapter,
    RestartPolicy, Supervisor, reconcile_startup, topic_for_watch,
};
use mailbox::watch::{SessionResumed, drop_interest, record, resume_session};
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

/// A resolver whose program never exists — every spawn fails — and which counts
/// the spawn attempts made through it.
///
/// The count is what lets a test tell "the sweep retried and gave up again" apart
/// from "the sweep did nothing", which is the difference between proving
/// announce-once and asserting it vacuously.
struct CountingBrokenResolver {
    attempts: Arc<AtomicUsize>,
}

impl AdapterResolver for CountingBrokenResolver {
    fn resolve(&self, watch: &Watch) -> Result<ResolvedAdapter, ResolveError> {
        let topic = topic_for_watch(watch).ok_or_else(|| {
            ResolveError::Invalid(format!("bad repo {:?}", watch.target.repo_column()))
        })?;
        self.attempts.fetch_add(1, Ordering::SeqCst);
        Ok(ResolvedAdapter {
            spec: AdapterSpec::new(
                "/nonexistent/mailbox-adapter-counting-outage".to_string(),
                AdapterId("counting-fixture".to_string()),
            ),
            config: AdapterConfig::new(json!({ "topic": topic.as_str() })),
        })
    }
}

/// A resolver that fails every spawn while "unhealthy" (a nonexistent program, so
/// `start` fails and the streak climbs to give-up) and serves the REAL stub once
/// flipped "healthy". Models a transient upstream outage: broken during the crash
/// streak that drives a watch to `Failed`, recovered by the time the sweeper
/// retries it (ADR-0011).
struct FlakyResolver {
    healthy: Arc<AtomicBool>,
    program: String,
}

impl FlakyResolver {
    fn new(healthy: Arc<AtomicBool>) -> Self {
        Self {
            healthy,
            program: stub_program(),
        }
    }
}

impl AdapterResolver for FlakyResolver {
    fn resolve(&self, watch: &Watch) -> Result<ResolvedAdapter, ResolveError> {
        let topic = topic_for_watch(watch).ok_or_else(|| {
            ResolveError::Invalid(format!("bad repo {:?}", watch.target.repo_column()))
        })?;
        let program = if self.healthy.load(Ordering::SeqCst) {
            self.program.clone()
        } else {
            "/nonexistent/mailbox-adapter-transient-outage".to_string()
        };
        let config = json!({ "topic": topic.as_str(), "interval_ms": 20, "count": 0 });
        Ok(ResolvedAdapter {
            spec: AdapterSpec::new(program, AdapterId("flaky".to_string())),
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

/// How many supervisor-authored events of `kind` (`adapter_gave_up` /
/// `adapter_recovered`) are durably on `topic`. Counting — not merely finding one —
/// is the point for ADR-0023: the bug being guarded against was a *repeat*.
async fn supervisor_events(storage: &Storage, topic: &Topic, kind: &str) -> usize {
    storage
        .read_events(topic.clone(), Cursor::Oldest, None)
        .await
        .unwrap()
        .events
        .iter()
        .filter(|e| e.body.get("event").and_then(|v| v.as_str()) == Some(kind))
        .count()
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

// ---- ADR-0026: an ended session's watches come back when it resumes ------------

/// The reported bug end to end through storage + supervisor: the app quits (every
/// session ends, the last interest leaves, the adapter stops), the app reopens (the
/// same session id resumes), and the adapter must run again for it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_resumed_session_restarts_the_adapter_its_end_stopped() {
    let (bus, storage, supervisor, _dir) = fresh(StubResolverFixture::interval(20)).await;
    let watched = pr(26);
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
    let first = poll_until("adapter running", || {
        let s = supervisor.clone();
        async move { s.running_pid(watch_id).await }
    })
    .await;

    // SessionEnd — backdated past the tombstone guard, so the resume below reads as
    // a real reopen rather than the arm-vs-cleanup race the guard refuses.
    let ended = mailbox::clock::now_millis() - 60_000;
    let outcome = storage.end_session(s1.clone(), ended).await.unwrap();
    assert_eq!(outcome.emptied_watches, vec![watch_id]);
    supervisor.stop_watch(watch_id).await.unwrap();
    assert_pid_reaped(first).await;

    let resumed = resume_session(&storage, &supervisor, s1.clone())
        .await
        .unwrap();
    assert_eq!(
        resumed,
        SessionResumed::Resumed {
            subscriptions_restored: 1,
            interests_restored: 1,
            watches_ensured: 1,
            watches_failed: 0,
        }
    );
    let second = poll_until("adapter running again after resume", || {
        let s = supervisor.clone();
        async move { s.running_pid(watch_id).await }
    })
    .await;
    assert_ne!(second, first, "a fresh adapter, not the reaped one");

    // And the restored subscription delivers what the new adapter publishes.
    poll_until("resumed session receives events", || {
        let (bus, s1) = (bus.clone(), s1.clone());
        async move { (!bus.read(s1, None).await.unwrap().is_empty()).then_some(()) }
    })
    .await;

    supervisor.shutdown().await.unwrap();
}

/// A daemon restart while the app was closed leaves the interest in place and the
/// watch stopped (no live session at reconcile). Nothing was suspended, but the
/// resume must still start the adapter.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_resume_starts_a_watch_the_startup_reconcile_stopped() {
    let (bus, storage, supervisor, _dir) = fresh(StubResolverFixture::interval(20)).await;
    let watched = pr(27);
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
    let first = poll_until("adapter running", || {
        let s = supervisor.clone();
        async move { s.running_pid(watch_id).await }
    })
    .await;
    supervisor.stop_watch(watch_id).await.unwrap();
    assert_pid_reaped(first).await;

    let resumed = resume_session(&storage, &supervisor, s1).await.unwrap();
    assert_eq!(
        resumed,
        SessionResumed::Resumed {
            subscriptions_restored: 0,
            interests_restored: 0,
            watches_ensured: 1,
            watches_failed: 0,
        }
    );
    poll_until("adapter running after resume", || {
        let s = supervisor.clone();
        async move { s.running_pid(watch_id).await }
    })
    .await;

    supervisor.shutdown().await.unwrap();
}

/// A resume inside the tombstone guard starts nothing and restores nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_resume_moments_after_the_end_is_refused_and_starts_nothing() {
    let (bus, storage, supervisor, _dir) = fresh(StubResolverFixture::interval(20)).await;
    let watched = pr(28);
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
    storage
        .end_session(s1.clone(), mailbox::clock::now_millis())
        .await
        .unwrap();
    supervisor.stop_watch(watch_id).await.unwrap();
    assert_pid_reaped(pid).await;

    let resumed = resume_session(&storage, &supervisor, s1).await.unwrap();
    assert_eq!(resumed, SessionResumed::RefusedSessionRecentlyEnded);
    assert_eq!(supervisor.running_pid(watch_id).await, None);
    assert_eq!(storage.interest_count(watch_id).await.unwrap(), 0);

    supervisor.shutdown().await.unwrap();
}

/// When the supervisor cannot be asked to run a watch, the resume still reports
/// what it restored: the rows are committed, so this is a partial resume counted in
/// the success value, not an `Err` that would claim nothing came back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_resume_whose_watch_cannot_start_still_reports_what_it_restored() {
    let (bus, storage, supervisor, _dir) = fresh(StubResolverFixture::interval(20)).await;
    let watched = pr(29);
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
    storage
        .end_session(s1.clone(), mailbox::clock::now_millis() - 60_000)
        .await
        .unwrap();
    // A supervisor that has gone away fails every `ensure_running`: a failure it
    // surfaces, unlike an unresolvable adapter, which is a no-op.
    supervisor.shutdown().await.unwrap();

    let resumed = resume_session(&storage, &supervisor, s1).await.unwrap();
    assert_eq!(
        resumed,
        SessionResumed::Resumed {
            subscriptions_restored: 1,
            interests_restored: 1,
            watches_ensured: 0,
            watches_failed: 1,
        }
    );
    assert_eq!(storage.interest_count(watch_id).await.unwrap(), 1);
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
        .find(|e| e.body.get("event").and_then(|v| v.as_str()) == Some(EVENT_ADAPTER_GAVE_UP));
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
            .any(|e| e.body.get("event").and_then(|v| v.as_str()) == Some(EVENT_ADAPTER_GAVE_UP)),
        "a healthy finite adapter must not produce a give-up event; got {events:?}"
    );

    supervisor.shutdown().await.unwrap();
}

// ---- AC5: bridge restart, resume iff a live session wants it ------------------

/// On bridge restart a previously-running watch whose interested session has NO
/// live process is not resumed: reconcile marks it stopped and clears the pid, even
/// though the interest row survives. The fail-safe half of design/01 rule 6 — an
/// interest we cannot prove belongs to a live session gets no poller.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac5_bridge_restart_does_not_resume_watch_of_a_dead_session() {
    let (_bus, storage, supervisor, dir) = fresh(StubResolverFixture::interval(20)).await;

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

    // Bridge restart with `s1` gone: no Claude Code process carries its id.
    let live = BTreeSet::new();
    reconcile_startup(&storage, &supervisor, &live)
        .await
        .unwrap();

    assert_eq!(
        watch_state(&storage, watch_id).await,
        WatchState::Stopped,
        "a previously-running watch whose session has exited is stopped, not resumed"
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

/// With no live session to resume anything, `reconcile_startup` touches ONLY
/// previously-running watches: a mix of Desired/Running/Stopped leaves Desired and
/// Stopped untouched and marks the Running one Stopped. Desired carries no stale
/// pid, so demoting it would be pure churn.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reconcile_startup_only_touches_running_watches() {
    let (_bus, storage, supervisor, _dir) = fresh(StubResolverFixture::interval(20)).await;

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

    let live = BTreeSet::new();
    reconcile_startup(&storage, &supervisor, &live)
        .await
        .unwrap();

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

/// The regression: a daemon restart must RESUME the watch of a session whose
/// watcher is still alive, rather than stopping it and waiting for a re-`watch`
/// that ADR-0008 guarantees will never come.
///
/// Without the fix this fails at the final assert — reconcile marks the watch
/// `Stopped` and no adapter is spawned, which is exactly the production failure of
/// 2026-07-17: a restart stopped every PR poller, and the idle sessions holding
/// those interests could not notice or recover, because the only event that would
/// have given them a turn was the one the stopped poller would have published.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reconcile_startup_resumes_a_watch_whose_session_is_still_running() {
    let (_bus, storage, supervisor, _dir) = fresh(StubResolverFixture::interval(20)).await;
    let s1 = SessionId::new("s1");

    // The pre-restart state: a watch the previous daemon had running under a pid
    // that died with it, and an interest that outlived it.
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
        .add_interest(watch_id, s1.clone(), 1_000)
        .await
        .unwrap();

    // s1 is still running: the process table carries its id (ADR-0017's probe,
    // handed in by `serve` as one `ps` read per reconcile).
    let live = BTreeSet::from([s1.clone()]);

    reconcile_startup(&storage, &supervisor, &live)
        .await
        .unwrap();

    let pid = poll_until("adapter resumed on restart", || {
        let s = supervisor.clone();
        async move { s.running_pid(watch_id).await }
    })
    .await;
    assert_ne!(
        pid, 999_999,
        "the resumed adapter must be a fresh child, not the previous daemon's dead pid"
    );
    assert_eq!(
        watch_state(&storage, watch_id).await,
        WatchState::Running {
            pid: mailbox::storage::Pid::new(pid)
        },
        "a watch whose interested session is still running is resumed on restart"
    );

    supervisor.shutdown().await.unwrap();
}

/// The old no-resume path could leave a watch `Stopped` while an interest row
/// survived — a state the model says cannot happen ("torn down, last interest
/// gone"). `reconcile_startup`'s doc claims it HEALS that: a `Stopped` watch whose
/// interested session is alive is resumed, not left in the contradictory state.
/// This pins that heal path, which the Running-start tests do not exercise.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reconcile_startup_resumes_a_stopped_watch_of_a_running_session() {
    let (_bus, storage, supervisor, _dir) = fresh(StubResolverFixture::interval(20)).await;
    let s1 = SessionId::new("s1");

    // The inconsistent pre-restart state the old path itself created: Stopped, yet
    // still holding a live interest.
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
        .set_watch_state(watch_id, WatchState::Stopped)
        .await
        .unwrap();
    storage
        .add_interest(watch_id, s1.clone(), 1_000)
        .await
        .unwrap();
    let live = BTreeSet::from([s1.clone()]);

    reconcile_startup(&storage, &supervisor, &live)
        .await
        .unwrap();

    let pid = poll_until("stopped-with-interest watch resumed on restart", || {
        let s = supervisor.clone();
        async move { s.running_pid(watch_id).await }
    })
    .await;
    assert_eq!(
        watch_state(&storage, watch_id).await,
        WatchState::Running {
            pid: mailbox::storage::Pid::new(pid)
        },
        "a Stopped watch whose session is alive is healed back to Running, not left contradictory"
    );

    supervisor.shutdown().await.unwrap();
}

/// design/01 rule 6 is "at least ONE interested session is still alive". A watch
/// with two interested sessions — one dead, one alive — must still resume, so a
/// future refactor to `.all(...)` or a first-match-only check is caught.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reconcile_startup_resumes_when_only_one_of_several_sessions_is_alive() {
    let (_bus, storage, supervisor, _dir) = fresh(StubResolverFixture::interval(20)).await;
    let dead = SessionId::new("dead");
    let alive = SessionId::new("alive");

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
        .add_interest(watch_id, dead.clone(), 1_000)
        .await
        .unwrap();
    storage
        .add_interest(watch_id, alive.clone(), 1_000)
        .await
        .unwrap();
    // Only `alive` still has a Claude Code process; `dead` does not.
    let live = BTreeSet::from([alive.clone()]);

    reconcile_startup(&storage, &supervisor, &live)
        .await
        .unwrap();

    poll_until("resumed because one of two sessions is alive", || {
        let s = supervisor.clone();
        async move { s.running_pid(watch_id).await }
    })
    .await;
    assert!(
        matches!(
            watch_state(&storage, watch_id).await,
            WatchState::Running { .. }
        ),
        "one live session among several is enough to resume (design/01 rule 6)"
    );

    supervisor.shutdown().await.unwrap();
}

/// A `Failed` watch is left alone by `reconcile_startup` even when an interested
/// session is alive: a restart is not evidence the adapter stopped crashing, so
/// the give-up stands until the sweep retries it (ADR-0011) or a re-`watch`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reconcile_startup_leaves_a_failed_watch_alone() {
    let (_bus, storage, supervisor, _dir) = fresh(StubResolverFixture::interval(20)).await;
    let s1 = SessionId::new("s1");

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
        .set_watch_state(watch_id, WatchState::Failed)
        .await
        .unwrap();
    storage
        .add_interest(watch_id, s1.clone(), 1_000)
        .await
        .unwrap();
    // A live session — to prove liveness does NOT override the Failed skip.
    let live = BTreeSet::from([s1.clone()]);

    reconcile_startup(&storage, &supervisor, &live)
        .await
        .unwrap();

    assert_eq!(
        watch_state(&storage, watch_id).await,
        WatchState::Failed,
        "reconcile leaves a Failed watch alone even with a live interested session"
    );
    assert_eq!(supervisor.running_pid(watch_id).await, None);

    supervisor.shutdown().await.unwrap();
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
    // Nobody is running: the live set is empty, so the sweep's liveness probe
    // spares nothing and the TTL alone decides — which is what this test drives.
    let live = BTreeSet::<SessionId>::new();

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
    let swept = supervisor
        .sweep(Duration::from_secs(1), live.clone())
        .await
        .unwrap();
    assert!(swept.is_empty(), "a fresh interest must not be swept");
    assert_eq!(supervisor.running_pid(watch_id).await, Some(pid));
    assert!(pid_alive(pid));

    // Backdate the interest's last-seen so it is now stale, then sweep.
    let old = mailbox::clock::now_millis() - 10_000;
    storage.touch_interest(watch_id, s1, old).await.unwrap();
    let swept = supervisor
        .sweep(Duration::from_secs(1), live)
        .await
        .unwrap();
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

/// REGRESSION (ADR-0009, carried forward to ADR-0017). A session that is still
/// RUNNING must survive a sweep no matter how long ago it last spoke to the bridge.
///
/// An idle session is silent by design — it takes zero turns and makes zero
/// requests until real mail arrives — so its `last_seen` never advances on its own.
/// A TTL keyed on the session's own traffic therefore reaped exactly the healthy
/// idle sessions on-demand wake exists to enable, killing the adapter under a live
/// agent and leaving the session silently deaf (its bus subscription survived, so
/// `subscribe` still answered "already subscribed" while no adapter existed to
/// produce events). The sweep now reads the process table, so liveness comes from
/// the agent's existence rather than from its chatter.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ttl_sweeper_spares_a_live_session_however_stale_its_last_seen() {
    let (bus, storage, supervisor, _dir) = fresh(StubResolverFixture::interval(20)).await;
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
    let pid = poll_until("adapter running", || {
        let s = supervisor.clone();
        async move { s.running_pid(watch_id).await }
    })
    .await;

    // s1's Claude Code process is still running.
    let live = BTreeSet::from([s1.clone()]);

    // Backdate last-seen far past the TTL — the exact state an idle-but-live
    // session reaches on its own, since nothing but `watch` ever stamps it.
    let ancient = mailbox::clock::now_millis() - 10_000;
    storage
        .touch_interest(watch_id, s1.clone(), ancient)
        .await
        .unwrap();

    let swept = supervisor
        .sweep(Duration::from_secs(1), live.clone())
        .await
        .unwrap();
    assert!(
        swept.is_empty(),
        "a session that is still running must never be swept, however stale its last-seen"
    );
    assert_eq!(
        supervisor.running_pid(watch_id).await,
        Some(pid),
        "the adapter must still be running under a live agent"
    );
    assert!(pid_alive(pid));

    // The sweep refreshed it rather than merely skipping it, so the next sweep is
    // decided by fresh evidence and not by the stale stamp we planted.
    let swept = supervisor
        .sweep(Duration::from_secs(1), live)
        .await
        .unwrap();
    assert!(swept.is_empty(), "the refresh must persist across sweeps");

    // Once the agent has exited, the TTL backstop reclaims the watch as before.
    storage.touch_interest(watch_id, s1, ancient).await.unwrap();
    let swept = supervisor
        .sweep(Duration::from_secs(1), BTreeSet::new())
        .await
        .unwrap();
    assert_eq!(
        swept,
        vec![watch_id],
        "with the agent gone the stale interest is swept"
    );
    assert_pid_reaped(pid).await;

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
            .any(|e| e.body.get("event").and_then(|v| v.as_str()) == Some(EVENT_ADAPTER_GAVE_UP)),
        "a repeated start-failure must still publish the give-up event"
    );

    supervisor.shutdown().await.unwrap();
}

// ---- ADR-0011: slow retry of failed watches -----------------------------------

/// The regression for ADR-0011: a watch that gave up (`Failed`) during a transient
/// outage is retried by the sweep and comes back `Running` once upstream recovers,
/// as long as an interested session is still alive — no manual re-`watch`.
///
/// Without the retry pass this hangs at the final poll: the watch stays `Failed`
/// forever, which is the exact production trap — a GitHub API blip during a daemon
/// restart parked three healthy PR watches until they were re-watched by hand.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sweep_retries_a_failed_watch_whose_session_is_alive() {
    let healthy = Arc::new(AtomicBool::new(false));
    let (bus, storage, supervisor, _dir) = fresh(FlakyResolver::new(healthy.clone())).await;
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

    // The transient outage drives the watch to give-up.
    poll_until("watch failed during the outage", || {
        let storage = storage.clone();
        async move { (watch_state(&storage, watch_id).await == WatchState::Failed).then_some(()) }
    })
    .await;

    // Upstream recovers, and s1's Claude Code process is still running — the
    // sweep's liveness signal (ADR-0017's probe).
    healthy.store(true, Ordering::SeqCst);
    let live = BTreeSet::from([s1.clone()]);

    // One sweep retries the failed watch; it comes back Running under a fresh pid.
    supervisor
        .sweep(Duration::from_secs(3600), live)
        .await
        .unwrap();
    let pid = poll_until("failed watch retried to running", || {
        let s = supervisor.clone();
        async move { s.running_pid(watch_id).await }
    })
    .await;
    assert_eq!(
        watch_state(&storage, watch_id).await,
        WatchState::Running {
            pid: mailbox::storage::Pid::new(pid)
        },
        "a sweep retries a Failed watch whose interested session is alive"
    );
    assert_eq!(
        supervisor_events(&storage, &watched.topic(), EVENT_ADAPTER_GAVE_UP).await,
        1,
        "the outage announced itself once; the retry that healed it added nothing (ADR-0023)"
    );

    supervisor.shutdown().await.unwrap();
}

/// The fail-safe half: a `Failed` watch whose session is NO LONGER RUNNING is left
/// `Failed` by the sweep — no zombie retries hammering an upstream nobody is
/// waiting on. Same liveness invariant as the TTL sweep and the startup reconcile.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sweep_does_not_retry_a_failed_watch_of_a_dead_session() {
    // Nonexistent program: every spawn fails, so the watch gives up to Failed and
    // would stay there unless something retries it.
    let (bus, storage, supervisor, _dir) = fresh(FixtureResolver::nonexistent()).await;
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
    poll_until("watch failed", || {
        let storage = storage.clone();
        async move { (watch_state(&storage, watch_id).await == WatchState::Failed).then_some(()) }
    })
    .await;

    // s1 is not in the live set: the sweep cannot prove the session alive. A long
    // TTL keeps the interest from being reaped, isolating the retry decision.
    supervisor
        .sweep(Duration::from_secs(3600), BTreeSet::new())
        .await
        .unwrap();

    assert_eq!(
        watch_state(&storage, watch_id).await,
        WatchState::Failed,
        "a Failed watch with no live interested session is not retried"
    );
    assert_eq!(supervisor.running_pid(watch_id).await, None);

    supervisor.shutdown().await.unwrap();
}

// ---- ADR-0023: one give-up notice per outage ----------------------------------

/// The wake-storm regression. A watch that keeps failing is still retried by every
/// sweep (ADR-0011), but its give-up is announced ONCE — the retries that give up
/// again publish nothing.
///
/// Observed in production (2026-08-12): a laptop lost its network, every `gh` poll
/// failed, and all eight watched PRs gave up. ADR-0011 then retried each of them
/// every 300s, each retry re-published `adapter_gave_up`, and each publish woke
/// every subscribed agent — thirteen identical give-up events per topic, five
/// minutes apart, for as long as the network was down.
///
/// Note the assertion is a COUNT. The pre-existing give-up tests use `.any()`, so
/// they stayed green throughout the storm: only counting catches a repeat.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_repeated_give_up_is_announced_only_once() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let (bus, storage, supervisor, _dir) = fresh(CountingBrokenResolver {
        attempts: attempts.clone(),
    })
    .await;
    let watched = pr(11);
    let topic = watched.topic();
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
    poll_until("watch failed", || {
        let storage = storage.clone();
        async move { (watch_state(&storage, watch_id).await == WatchState::Failed).then_some(()) }
    })
    .await;
    assert_eq!(
        supervisor_events(&storage, &topic, EVENT_ADAPTER_GAVE_UP).await,
        1,
        "the first give-up of an outage is announced"
    );

    // One full restart burst is every attempt within the budget plus the one that
    // exhausts it — how a retry that ran to a fresh give-up is told apart from a
    // sweep that did nothing.
    let burst = fast_policy().max_consecutive_failures as usize + 1;
    for round in 1..=3 {
        let before = attempts.load(Ordering::SeqCst);
        supervisor
            .sweep(Duration::from_secs(3600), BTreeSet::from([s1.clone()]))
            .await
            .unwrap();
        poll_until("sweep retried the failed watch and gave up again", || {
            let attempts = attempts.clone();
            let storage = storage.clone();
            async move {
                (attempts.load(Ordering::SeqCst) >= before + burst
                    && watch_state(&storage, watch_id).await == WatchState::Failed)
                    .then_some(())
            }
        })
        .await;
        assert_eq!(
            supervisor_events(&storage, &topic, EVENT_ADAPTER_GAVE_UP).await,
            1,
            "sweep {round} retried and gave up again; the outage is unchanged, so it must not re-announce"
        );
    }

    supervisor.shutdown().await.unwrap();
}

/// The other half of announce-once: it must not become announce-never. A watch that
/// comes back and STAYS back publishes exactly one recovery event, and the notice
/// re-arms so the NEXT outage is announced too.
///
/// Without the recovery event, announce-once would be a silent black hole — an
/// agent told "mailbox stopped watching this" and nothing after has no way to learn
/// its watch came back, which is the ADR-0008 deafness this bus exists to prevent.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_recovered_watch_announces_once_and_re_arms_for_the_next_outage() {
    let healthy = Arc::new(AtomicBool::new(false));
    // `fast_policy`'s hour-long `reset_after` exists to stop a crash streak being
    // read as stable; here the stability timer is the thing under test, so it has
    // to be short enough to fire. Start-failures never reset the streak whatever
    // this is set to, so the give-up still accumulates as fast as ever.
    let policy = RestartPolicy {
        reset_after: Duration::from_millis(150),
        ..fast_policy()
    };
    let (bus, storage, supervisor, _dir) =
        fresh_with(FlakyResolver::new(healthy.clone()), policy).await;
    let watched = pr(12);
    let topic = watched.topic();
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

    // The outage: the watch gives up and says so once.
    poll_until("watch failed during the outage", || {
        let storage = storage.clone();
        async move { (watch_state(&storage, watch_id).await == WatchState::Failed).then_some(()) }
    })
    .await;
    assert_eq!(
        supervisor_events(&storage, &topic, EVENT_ADAPTER_GAVE_UP).await,
        1
    );
    assert_eq!(
        supervisor_events(&storage, &topic, EVENT_ADAPTER_RECOVERED).await,
        0,
        "nothing has recovered yet"
    );

    // Upstream comes back; the sweep retries the failed watch (ADR-0011) and the
    // adapter this time stays up past `reset_after`.
    healthy.store(true, Ordering::SeqCst);
    supervisor
        .sweep(Duration::from_secs(3600), BTreeSet::from([s1.clone()]))
        .await
        .unwrap();
    let pid = poll_until("failed watch retried to running", || {
        let s = supervisor.clone();
        async move { s.running_pid(watch_id).await }
    })
    .await;
    poll_until("recovery announced once the run proved stable", || {
        let storage = storage.clone();
        let topic = topic.clone();
        async move {
            (supervisor_events(&storage, &topic, EVENT_ADAPTER_RECOVERED).await == 1).then_some(())
        }
    })
    .await;
    assert_eq!(
        supervisor_events(&storage, &topic, EVENT_ADAPTER_GAVE_UP).await,
        1,
        "recovering does not re-announce the give-up it withdraws"
    );

    // The recovery must carry a subject (ADR-0022): the wake wire shows only that
    // line, so an event without one wakes the agent saying nothing — the exact
    // silence the recovery exists to break.
    let recovery = storage
        .read_events(topic.clone(), Cursor::Oldest, None)
        .await
        .unwrap()
        .events
        .into_iter()
        .find(|e| e.body.get("event").and_then(|v| v.as_str()) == Some(EVENT_ADAPTER_RECOVERED))
        .expect("a recovery event");
    let subject = recovery.subject.expect("the recovery carries a subject");
    assert!(
        subject.text().contains("watching this again"),
        "the subject must say the watch is live again, not merely that something happened; got {:?}",
        subject.text()
    );

    // A second, separate outage: kill the healthy adapter with upstream broken
    // again. The withdrawn notice must re-arm, or this outage would be silent.
    healthy.store(false, Ordering::SeqCst);
    nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(pid as i32),
        nix::sys::signal::Signal::SIGKILL,
    )
    .expect("kill the recovered adapter");
    poll_until("the next outage is announced in its own right", || {
        let storage = storage.clone();
        let topic = topic.clone();
        async move {
            (supervisor_events(&storage, &topic, EVENT_ADAPTER_GAVE_UP).await == 2).then_some(())
        }
    })
    .await;
    assert_eq!(
        supervisor_events(&storage, &topic, EVENT_ADAPTER_RECOVERED).await,
        1,
        "the second outage has not recovered"
    );

    supervisor.shutdown().await.unwrap();
}

/// A healthy watch that never gave up publishes no recovery event, however long it
/// runs. The stability timer fires for every spawn, so the latch — not the timer —
/// has to be what decides: only an agent that was told the watch went dark is told
/// it came back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_watch_that_never_gave_up_announces_no_recovery() {
    let policy = RestartPolicy {
        reset_after: Duration::from_millis(100),
        ..fast_policy()
    };
    let (bus, storage, supervisor, _dir) =
        fresh_with(StubResolverFixture::interval(20), policy).await;
    let watched = pr(13);
    let topic = watched.topic();

    record(
        &bus,
        &storage,
        &supervisor,
        &watched,
        Duration::from_secs(60),
        SessionId::new("s1"),
    )
    .await
    .unwrap();
    let watch_id = only_watch_id(&storage).await;
    poll_until("adapter running", || {
        let s = supervisor.clone();
        async move { s.running_pid(watch_id).await }
    })
    .await;

    // Well past `reset_after`, so the stability timer has certainly fired. A fixed
    // sleep is right here: the assertion is that nothing happens.
    tokio::time::sleep(Duration::from_millis(400)).await;

    assert_eq!(
        supervisor_events(&storage, &topic, EVENT_ADAPTER_RECOVERED).await,
        0,
        "a watch that never announced a give-up has nothing to withdraw"
    );
    assert_eq!(
        supervisor_events(&storage, &topic, EVENT_ADAPTER_GAVE_UP).await,
        0
    );

    supervisor.shutdown().await.unwrap();
}

/// A start is not a recovery. While the fault persists, every sweep retry spawns an
/// adapter that dies moments later — so if the stability timer withdrew the notice
/// on the mere fact of a spawn, it would publish one bogus "recovered" per interval
/// and rebuild the storm inverted.
///
/// This is the guard the generation check in `on_stable` exists for. The crash
/// fixture spawns REAL children that die in milliseconds, so every armed timer is
/// stale by the time it fires — deterministically, with no race to lose.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn adapters_that_start_and_die_never_count_as_recovered() {
    // `reset_after` has to sit in a window, and the window is why this is spelled
    // out rather than tuned to taste. It is BOTH the stable-run threshold that
    // resets the crash streak and the delay before a stability timer fires, so:
    //
    //   crash lifetime  <<  reset_after  <<  the poll_until budget (6s)
    //
    // Too LOW and a crash that outlived it counts as a stable run, the streak
    // resets every time, the watch never reaches `Failed`, and this hangs — which
    // is exactly how a 120ms value passed locally (the fixture aborts in ~1ms) and
    // then timed out on a loaded CI runner, where spawning a process and aborting
    // it took longer than that. Too HIGH and the timers never fire inside the test,
    // which would pass for the wrong reason. 1.5s is ~1000x the fixture's lifetime
    // and a quarter of the budget.
    let policy = RestartPolicy {
        reset_after: Duration::from_millis(1500),
        ..fast_policy()
    };
    let (bus, storage, supervisor, _dir) = fresh_with(FixtureResolver::crash(), policy).await;
    let watched = pr(14);
    let topic = watched.topic();
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
    poll_until("watch failed after a crash streak", || {
        let storage = storage.clone();
        async move { (watch_state(&storage, watch_id).await == WatchState::Failed).then_some(()) }
    })
    .await;

    // Retry it twice more, so several generations of short-lived adapter have been
    // spawned and several stale timers are in flight.
    for _ in 0..2 {
        supervisor
            .sweep(Duration::from_secs(3600), BTreeSet::from([s1.clone()]))
            .await
            .unwrap();
        poll_until("the retry crashed back to failed", || {
            let storage = storage.clone();
            async move {
                (watch_state(&storage, watch_id).await == WatchState::Failed).then_some(())
            }
        })
        .await;
    }
    // Well past `reset_after` measured from the LAST spawn above, so every timer
    // armed during this test has certainly fired and declined.
    tokio::time::sleep(Duration::from_millis(1800)).await;

    assert_eq!(
        supervisor_events(&storage, &topic, EVENT_ADAPTER_RECOVERED).await,
        0,
        "adapters that started and immediately died have recovered nothing"
    );
    assert_eq!(
        supervisor_events(&storage, &topic, EVENT_ADAPTER_GAVE_UP).await,
        1,
        "and the outage is still the same outage"
    );

    supervisor.shutdown().await.unwrap();
}

/// Teardown withdraws the notice SILENTLY, and the next watch of the same entity is
/// a new outage that announces in its own right.
///
/// Silently, because the adapter did not recover — it stopped being wanted, and
/// telling subscribers a stopped watch is "being watched again" would be a lie. But
/// the latch must still be dropped: a re-watched entity that failed again to silence
/// would be the announce-never failure, one unwatch later.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unwatching_withdraws_the_notice_silently_and_the_next_watch_re_announces() {
    let (bus, storage, supervisor, _dir) = fresh(FixtureResolver::nonexistent()).await;
    let watched = pr(15);
    let topic = watched.topic();
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
    poll_until("watch failed", || {
        let storage = storage.clone();
        async move { (watch_state(&storage, watch_id).await == WatchState::Failed).then_some(()) }
    })
    .await;
    assert_eq!(
        supervisor_events(&storage, &topic, EVENT_ADAPTER_GAVE_UP).await,
        1
    );

    // The last interested session leaves while the watch is still failed.
    drop_interest(&bus, &storage, &supervisor, &watched, s1.clone())
        .await
        .unwrap();
    assert_eq!(
        supervisor_events(&storage, &topic, EVENT_ADAPTER_RECOVERED).await,
        0,
        "unwatching a failed watch is not a recovery, and must not be announced as one"
    );

    // Watched again, still broken: a new outage, announced in its own right.
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
    poll_until("the re-watched entity failed again", || {
        let storage = storage.clone();
        let topic = topic.clone();
        async move {
            (supervisor_events(&storage, &topic, EVENT_ADAPTER_GAVE_UP).await == 2).then_some(())
        }
    })
    .await;
    assert_eq!(
        supervisor_events(&storage, &topic, EVENT_ADAPTER_RECOVERED).await,
        0,
        "nothing recovered at any point"
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
