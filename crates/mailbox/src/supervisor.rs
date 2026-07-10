//! Watch supervision: one bridge-owned adapter per external entity, alive
//! exactly while some session cares (design/01, the "no zombie pollers" invariant).
//!
//! # What this owns
//!
//! The [`Supervisor`] is the bridge component (owned by the `serve` daemon) that
//! turns the refcounted-interest state in [`crate::storage`] into running adapter
//! processes. It holds the running [`SubprocessTransport`] instances keyed by
//! **entity** (`WatchId` — i.e. `(kind, repo, pr)`), never by session: a second
//! session watching the same PR attaches interest and reuses the one running
//! adapter (design/01 rule 2). It is driven by the `mailbox::watch` control ops
//! ([`crate::watch::record`] / [`crate::watch::drop_interest`]) and by a periodic
//! TTL sweep.
//!
//! # Why an actor (serialized transitions)
//!
//! Spawn / stop / restart must be serialized so two sessions racing to watch one
//! entity spawn **exactly one** adapter, and a stop racing a crash-restart cannot
//! leak a process. So the map lives inside a single owning task (this module's
//! `Actor`) and every transition is a [`Command`] posted to it — the same
//! single-owner discipline the storage writer uses. The public [`Supervisor`] is
//! just a cheap channel handle. Because the actor `await`s `SubprocessTransport::
//! start` inline, no two spawns can interleave; idempotent [`Supervisor::
//! ensure_running`] makes a duplicate watch a no-op.
//!
//! # Crash handling, backoff, give-up
//!
//! Each running adapter has a monitor task that owns it via
//! [`AdapterHost::wait`]; when the child exits on its own the monitor reports it.
//! If interest is still > 0 that is a crash: the supervisor backoff-restarts (the
//! [`RestartPolicy`]) and, after N consecutive failures, gives up — it publishes
//! an error event on the entity's topic and marks the watch [`WatchState::Failed`].
//! While interest is 0 an exit is just a clean stop.
//!
//! # Fail-safe on bridge restart (no orphan resume)
//!
//! [`reconcile_startup`] marks every previously-`Running` watch `Stopped` and
//! clears its pid on daemon start. It deliberately does **not** resume watches:
//! until a session-liveness probe exists, missing some events beats resurrecting
//! a poller nothing is listening to (design/01 rule 6).
//!
//! # Dependencies (one-way)
//!
//! `supervisor` → `host` + `bus` + `storage`. It never depends on `watch` (which
//! calls *into* it), so there is no cycle. Adapter program resolution is behind
//! the injectable [`AdapterResolver`], so this module knows nothing about any
//! concrete adapter — tests inject a fixture, card 10 injects the real
//! `github-pr` poller.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::json;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tracing::{error, info, warn};

use mailbox_protocol::{AdapterId, GithubPr, Timestamp, Topic};

use crate::bus::Bus;
use crate::clock::now_millis;
use crate::host::subprocess::{AdapterSpec, SubprocessTransport};
use crate::host::{AdapterConfig, AdapterExit, AdapterHost};
use crate::storage::{Pid, Storage, StorageError, Watch, WatchId, WatchKind, WatchState};

/// Capacity of the supervisor command channel. Commands are small; a modest
/// buffer absorbs a burst of watch/unwatch ops plus monitor exit reports without
/// callers waiting, while staying bounded.
const COMMAND_CHANNEL_CAPACITY: usize = 64;

/// Resolves a watch to the adapter program that should service it.
///
/// The seam that keeps the supervisor decoupled from any concrete adapter: tests
/// inject a fixture, card 10 injects the real `github-pr` poller. Given the whole
/// [`Watch`] (not just its kind) so a resolver can build the adapter's config
/// from the repo/pr/interval.
pub trait AdapterResolver: Send + Sync + 'static {
    /// The program + config to run for `watch`, or why none is available.
    fn resolve(&self, watch: &Watch) -> Result<ResolvedAdapter, ResolveError>;
}

/// What to spawn for a watch: the transport [`AdapterSpec`] (program + identity)
/// and the opaque [`AdapterConfig`] delivered on the child's stdin.
#[derive(Debug, Clone)]
pub struct ResolvedAdapter {
    pub spec: AdapterSpec,
    pub config: AdapterConfig,
}

/// Why an adapter could not be resolved for a watch.
#[derive(Debug, thiserror::Error)]
pub enum ResolveError {
    /// No adapter program exists yet for this watch kind (the `serve` default
    /// until cards 09/10 ship the real poller). The supervisor treats this as
    /// "record intent, do not spawn" — the watch stays `Desired`.
    #[error("no adapter program available for watch kind {}", .kind.as_str())]
    NoAdapter { kind: WatchKind },
    /// The resolver had a program in mind but could not produce a valid spec.
    #[error("could not resolve adapter: {0}")]
    Invalid(String),
}

/// The `serve` resolver until a real adapter binary ships (cards 09/10).
///
/// Every kind resolves to [`ResolveError::NoAdapter`], so `watch` records intent
/// and the supervisor leaves the watch `Desired` without spawning anything —
/// preserving the card-06 behaviour in production while the supervision machinery
/// itself is fully real and tested with a fixture adapter. Card 10 replaces this
/// with a resolver that points `github-pr` at the real poller.
pub struct UnavailableResolver;

impl AdapterResolver for UnavailableResolver {
    fn resolve(&self, watch: &Watch) -> Result<ResolvedAdapter, ResolveError> {
        Err(ResolveError::NoAdapter { kind: watch.kind })
    }
}

/// Backoff + give-up policy for crash restarts.
///
/// A named type (not scattered constants) so `serve` uses production values and
/// tests inject fast ones. The failure counter is *consecutive*: a run that lasts
/// at least `reset_after` is treated as stable and resets the streak, so a single
/// crash after a long healthy run does not inch toward give-up.
#[derive(Debug, Clone, Copy)]
pub struct RestartPolicy {
    /// Give up after this many consecutive failed runs (design/01 rule 7's "N").
    pub max_consecutive_failures: u32,
    /// Backoff before the first restart; doubles each consecutive failure.
    pub base_backoff: Duration,
    /// Ceiling on the doubling backoff.
    pub max_backoff: Duration,
    /// A run that lasted at least this long counts as stable and resets the
    /// consecutive-failure streak.
    pub reset_after: Duration,
    /// SIGTERM→SIGKILL grace when stopping an adapter.
    pub stop_grace: Duration,
}

impl Default for RestartPolicy {
    fn default() -> Self {
        Self {
            max_consecutive_failures: 5,
            base_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(60),
            reset_after: Duration::from_secs(60),
            stop_grace: Duration::from_secs(3),
        }
    }
}

impl RestartPolicy {
    /// Backoff before restart `attempt` (1-based): `base * 2^(attempt-1)`, capped
    /// at `max_backoff`. Saturating so a large attempt count cannot overflow.
    fn backoff(&self, attempt: u32) -> Duration {
        let shift = attempt.saturating_sub(1).min(20);
        self.base_backoff
            .saturating_mul(1u32 << shift)
            .min(self.max_backoff)
    }
}

/// A supervisor operation failure. Business/transport failures inside the actor
/// are logged and handled (backoff/give-up); this type is for failures a caller
/// must see: a storage error, or the actor being gone.
#[derive(Debug, thiserror::Error)]
pub enum SupervisorError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    /// The supervisor task is no longer running (channel closed).
    #[error("supervisor task is not running")]
    Gone,
}

/// Handle to the watch supervisor. Cheap to clone (a channel sender); every clone
/// talks to the one owning actor task.
#[derive(Clone)]
pub struct Supervisor {
    cmd_tx: mpsc::Sender<Command>,
}

impl Supervisor {
    /// Start the supervisor actor over an open store + bus, using `resolver` to
    /// map watches onto adapter programs and `policy` for crash backoff. Returns
    /// immediately; the actor runs as a spawned task.
    pub fn spawn(
        storage: Storage,
        bus: Bus,
        resolver: Arc<dyn AdapterResolver>,
        policy: RestartPolicy,
    ) -> Self {
        let (cmd_tx, rx) = mpsc::channel(COMMAND_CHANNEL_CAPACITY);
        let actor = Actor {
            storage,
            bus,
            resolver,
            policy,
            running: HashMap::new(),
            failures: HashMap::new(),
            restart_epoch: HashMap::new(),
            next_gen: 0,
            cmd_tx: cmd_tx.clone(),
            rx,
        };
        tokio::spawn(actor.run());
        Self { cmd_tx }
    }

    /// Ensure the adapter for `watch_id` is running (idempotent — one adapter per
    /// entity). Called by `watch::record` after interest is attached; a no-op when
    /// already running, when interest is 0, or when no adapter resolves.
    pub async fn ensure_running(&self, watch_id: WatchId) -> Result<(), SupervisorError> {
        self.request(|reply| Command::EnsureRunning {
            watch_id,
            reply: Some(reply),
        })
        .await
    }

    /// Stop the adapter for `watch_id` (called by `watch::drop_interest` on the
    /// last interest removal). A no-op if nothing is running.
    pub async fn stop_watch(&self, watch_id: WatchId) -> Result<(), SupervisorError> {
        self.request(|reply| Command::StopWatch {
            watch_id,
            reply: Some(reply),
        })
        .await
    }

    /// Run one TTL sweep: drop interests whose last-seen is older than `ttl` and
    /// stop any adapter whose interest thereby reached zero. Returns the watches
    /// that were swept to zero.
    pub async fn sweep(&self, ttl: Duration) -> Result<Vec<WatchId>, SupervisorError> {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::Sweep { ttl, reply })
            .await
            .map_err(|_| SupervisorError::Gone)?;
        rx.await.map_err(|_| SupervisorError::Gone)?
    }

    /// The OS pid of the adapter currently running for `watch_id`, or `None` if
    /// none is. For `status` and tests.
    pub async fn running_pid(&self, watch_id: WatchId) -> Option<u32> {
        let (reply, rx) = oneshot::channel();
        if self
            .cmd_tx
            .send(Command::RunningPid { watch_id, reply })
            .await
            .is_err()
        {
            return None;
        }
        rx.await.unwrap_or(None)
    }

    /// Stop every running adapter and end the actor. Called on daemon shutdown so
    /// no adapter outlives the bridge.
    pub async fn shutdown(&self) -> Result<(), SupervisorError> {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::Shutdown { reply })
            .await
            .map_err(|_| SupervisorError::Gone)?;
        rx.await.map_err(|_| SupervisorError::Gone)
    }

    /// Send a command that replies with `Result<(), SupervisorError>` and await it.
    async fn request(
        &self,
        make: impl FnOnce(oneshot::Sender<Result<(), SupervisorError>>) -> Command,
    ) -> Result<(), SupervisorError> {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx
            .send(make(reply))
            .await
            .map_err(|_| SupervisorError::Gone)?;
        rx.await.map_err(|_| SupervisorError::Gone)?
    }
}

/// On daemon startup, mark every previously-`Running` watch `Stopped` and clear
/// its child pid. **Does not resume** watches (design/01 rule 6): until a
/// session-liveness probe exists, a resumed poller could outlive every session
/// that wanted it, so the fail-safe is to require a live session to re-`watch`.
pub async fn reconcile_startup(storage: &Storage) -> Result<(), StorageError> {
    for watch in storage.list_watches().await? {
        if let WatchState::Running { pid } = watch.state {
            warn!(
                watch = watch.id.get(),
                kind = watch.kind.as_str(),
                repo = %watch.repo,
                pr = watch.pr,
                pid = pid.get(),
                "did not resume previously-running watch on startup (no session-liveness probe); marking stopped"
            );
            storage
                .set_watch_state(watch.id, WatchState::Stopped)
                .await?;
        }
    }
    Ok(())
}

/// Derive the topic a watch publishes on. Used to publish give-up error events
/// and by adapter resolvers building config. `None` if the stored repo is not a
/// well-formed `owner/repo`.
pub fn topic_for_watch(watch: &Watch) -> Option<Topic> {
    match watch.kind {
        WatchKind::GithubPr => {
            let (owner, repo) = watch.repo.split_once('/')?;
            GithubPr::new(owner, repo, watch.pr)
                .ok()
                .map(|pr| pr.topic())
        }
    }
}

/// A command posted to the actor. External requests carry a reply; internal
/// events (a monitor reporting an exit, a scheduled restart) do not.
enum Command {
    EnsureRunning {
        watch_id: WatchId,
        reply: Option<Ack>,
    },
    StopWatch {
        watch_id: WatchId,
        reply: Option<Ack>,
    },
    /// A scheduled backoff restart firing. Carries the `epoch` it was scheduled
    /// under so a restart cancelled meanwhile (by a stop/sweep) is ignored rather
    /// than resurrecting a torn-down watch.
    Restart {
        watch_id: WatchId,
        epoch: u64,
    },
    Sweep {
        ttl: Duration,
        reply: oneshot::Sender<Result<Vec<WatchId>, SupervisorError>>,
    },
    RunningPid {
        watch_id: WatchId,
        reply: oneshot::Sender<Option<u32>>,
    },
    Shutdown {
        reply: oneshot::Sender<()>,
    },
    /// A monitor reporting that its adapter exited on its own (crash or natural
    /// end). `generation` guards against a stale report from a superseded instance.
    AdapterExited {
        watch_id: WatchId,
        generation: u64,
        pid: u32,
        exit: AdapterExit,
    },
}

type Ack = oneshot::Sender<Result<(), SupervisorError>>;

/// A running adapter as the actor tracks it. The transport itself is owned by the
/// monitor task (via `wait()`); here we keep only what the actor needs to
/// identify, log, and stop it.
struct RunningEntity {
    pid: u32,
    /// Distinguishes this instance from any predecessor/successor for the same
    /// entity, so a late exit report from an old instance is ignored.
    generation: u64,
    /// When this instance was spawned. On exit, the run's duration decides
    /// whether it was stable (reset the failure streak) or a fast crash.
    spawned_at: Instant,
    /// Entity identity, so every lifecycle log carries repo/pr/kind, not just the
    /// opaque numeric watch id.
    kind: WatchKind,
    repo: String,
    pr: u64,
    /// Signals the monitor to gracefully stop (SIGTERM→SIGKILL).
    stop_tx: oneshot::Sender<()>,
    /// The monitor task; aborted on shutdown to force process-group teardown.
    monitor: JoinHandle<()>,
}

struct Actor {
    storage: Storage,
    bus: Bus,
    resolver: Arc<dyn AdapterResolver>,
    policy: RestartPolicy,
    running: HashMap<WatchId, RunningEntity>,
    /// Consecutive-failure count per entity, for the give-up budget.
    failures: HashMap<WatchId, u32>,
    /// The epoch of the currently-scheduled restart per entity. A detached
    /// backoff task carries the epoch it was scheduled under; if the epoch has
    /// since moved (a stop/sweep/newer schedule bumped it), that task's restart is
    /// stale and must not resurrect a torn-down watch.
    restart_epoch: HashMap<WatchId, u64>,
    next_gen: u64,
    /// Clone handed to monitors and scheduled restarts so they can post back.
    cmd_tx: mpsc::Sender<Command>,
    rx: mpsc::Receiver<Command>,
}

impl Actor {
    async fn run(mut self) {
        while let Some(cmd) = self.rx.recv().await {
            match cmd {
                Command::EnsureRunning { watch_id, reply } => {
                    let result = self.ensure_running(watch_id).await;
                    ack(reply, result);
                }
                Command::StopWatch { watch_id, reply } => {
                    let result = self.stop_watch(watch_id).await;
                    ack(reply, result);
                }
                Command::Restart { watch_id, epoch } => {
                    // Only fire if this is still the current scheduled restart; a
                    // stop/sweep/newer schedule bumps the epoch to cancel it.
                    if self.restart_epoch.get(&watch_id) == Some(&epoch)
                        && let Err(err) = self.ensure_running(watch_id).await
                    {
                        error!(watch = watch_id.get(), error = %err, "supervisor restart failed");
                    }
                }
                Command::Sweep { ttl, reply } => {
                    let result = self.sweep(ttl).await;
                    let _ = reply.send(result);
                }
                Command::RunningPid { watch_id, reply } => {
                    let _ = reply.send(self.running.get(&watch_id).map(|e| e.pid));
                }
                Command::AdapterExited {
                    watch_id,
                    generation,
                    pid,
                    exit,
                } => {
                    if let Err(err) = self
                        .on_adapter_exited(watch_id, generation, pid, exit)
                        .await
                    {
                        error!(
                            watch = watch_id.get(),
                            error = %err,
                            "supervisor failed while handling adapter exit"
                        );
                    }
                }
                Command::Shutdown { reply } => {
                    self.shutdown_all().await;
                    let _ = reply.send(());
                    return;
                }
            }
        }
        // The channel closed (all handles, incl. our own clone, dropped). Best
        // effort: tear down anything still running so nothing is orphaned.
        self.shutdown_all().await;
    }

    /// Start the adapter for `watch_id` unless one is already running, nobody is
    /// interested, or no adapter resolves. Idempotent — this is what makes two
    /// sessions racing to watch one entity spawn exactly one child.
    async fn ensure_running(&mut self, watch_id: WatchId) -> Result<(), SupervisorError> {
        if self.running.contains_key(&watch_id) {
            // One adapter per external entity: a second interested session reuses
            // the running one rather than spawning another.
            return Ok(());
        }
        let Some(watch) = self.storage.get_watch(watch_id).await? else {
            return Ok(());
        };
        let interest = self.storage.interest_count(watch_id).await?;
        if interest == 0 {
            // A scheduled restart can race a final unwatch; if nobody cares now, do
            // not start (design/01: alive only while interest remains). Crucially,
            // make teardown authoritative: never leave a non-Stopped watch (e.g. a
            // `Running{dead pid}` left by a crash mid-backoff) behind.
            if !matches!(watch.state, WatchState::Stopped) {
                self.storage
                    .set_watch_state(watch_id, WatchState::Stopped)
                    .await?;
                info!(
                    watch = watch_id.get(),
                    kind = watch.kind.as_str(),
                    repo = %watch.repo,
                    pr = watch.pr,
                    "not starting adapter (no remaining interest); marked stopped"
                );
            }
            return Ok(());
        }
        let resolved = match self.resolver.resolve(&watch) {
            Ok(resolved) => resolved,
            Err(err) => {
                info!(
                    watch = watch_id.get(),
                    kind = watch.kind.as_str(),
                    reason = %err,
                    "did not start adapter; no program resolved for this watch kind"
                );
                return Ok(());
            }
        };

        match SubprocessTransport::start(resolved.spec, resolved.config, self.bus.clone()).await {
            Ok(transport) => self.install_running(watch_id, &watch, transport).await,
            Err(err) => {
                warn!(
                    watch = watch_id.get(),
                    kind = watch.kind.as_str(),
                    repo = %watch.repo,
                    pr = watch.pr,
                    error = %err,
                    "adapter failed to start"
                );
                // A start failure counts toward backoff/give-up like a crash, and
                // it did NOT run, so it never resets the failure streak.
                self.handle_failure(watch_id, None).await
            }
        }
    }

    /// Register a freshly started transport: spawn its monitor, record it, and
    /// mark the watch `Running` with its pid.
    async fn install_running(
        &mut self,
        watch_id: WatchId,
        watch: &Watch,
        transport: SubprocessTransport,
    ) -> Result<(), SupervisorError> {
        // A subprocess always has a pid; a pid-less transport is an anomaly we
        // refuse rather than fake with a placeholder, so `Running` always carries
        // a real pid. Tear the anomalous transport down and count it as a failure.
        let Some(pid) = transport.pid() else {
            warn!(
                watch = watch_id.get(),
                "adapter started without a pid; tearing it down and treating as a failed start"
            );
            drop(transport); // Drop backstop: group SIGKILL + reap (no orphan).
            return self.handle_failure(watch_id, None).await;
        };
        self.next_gen += 1;
        let generation = self.next_gen;
        let (stop_tx, stop_rx) = oneshot::channel();
        let monitor = tokio::spawn(monitor_adapter(
            transport,
            watch_id,
            generation,
            pid,
            stop_rx,
            self.cmd_tx.clone(),
            self.policy.stop_grace,
        ));
        // Track before touching storage so a storage error still leaves the
        // adapter reachable for shutdown teardown.
        self.running.insert(
            watch_id,
            RunningEntity {
                pid,
                generation,
                spawned_at: Instant::now(),
                kind: watch.kind,
                repo: watch.repo.clone(),
                pr: watch.pr,
                stop_tx,
                monitor,
            },
        );
        self.storage
            .set_watch_state(watch_id, WatchState::Running { pid: Pid::new(pid) })
            .await?;
        info!(
            watch = watch_id.get(),
            kind = watch.kind.as_str(),
            repo = %watch.repo,
            pr = watch.pr,
            pid,
            generation,
            "spawned adapter for entity (one per external entity)"
        );
        Ok(())
    }

    /// Handle a monitor's report that its adapter exited on its own.
    async fn on_adapter_exited(
        &mut self,
        watch_id: WatchId,
        generation: u64,
        pid: u32,
        exit: AdapterExit,
    ) -> Result<(), SupervisorError> {
        // Capture identity + how long the run lasted BEFORE removing the entry, so
        // the failure clock (stable-run reset) and logs are authoritative.
        let (ran_for, kind, repo, pr) = match self.running.get(&watch_id) {
            Some(entity) if entity.generation == generation => (
                entity.spawned_at.elapsed(),
                entity.kind,
                entity.repo.clone(),
                entity.pr,
            ),
            // Stale (superseded instance) or already removed (a stop won the race).
            _ => return Ok(()),
        };
        self.running.remove(&watch_id);

        let interest = self.storage.interest_count(watch_id).await?;
        if interest == 0 {
            // Exited with nobody interested → a clean stop, not a crash.
            self.failures.remove(&watch_id);
            self.storage
                .set_watch_state(watch_id, WatchState::Stopped)
                .await?;
            info!(
                watch = watch_id.get(),
                kind = kind.as_str(),
                repo = %repo,
                pr,
                pid,
                ?exit,
                "adapter exited with no remaining interest; marked stopped"
            );
            return Ok(());
        }

        warn!(
            watch = watch_id.get(),
            kind = kind.as_str(),
            repo = %repo,
            pr,
            pid,
            ?exit,
            interest,
            ran_ms = ran_for.as_millis() as u64,
            "adapter exited unexpectedly while interest remains; evaluating restart"
        );
        self.handle_failure(watch_id, Some(ran_for)).await
    }

    /// A run failed (crash or failed start) while wanted: bump the consecutive
    /// failure count and either schedule a backoff restart or give up.
    ///
    /// `ran_for` is how long the failed run actually lasted, or `None` if it never
    /// ran (a failed `start`). The streak resets only when a run lasted at least
    /// `reset_after` — so consecutive *start* failures (which never run) always
    /// accumulate toward give-up and back off, rather than being reset to 1 by a
    /// stale "last spawn" timestamp.
    async fn handle_failure(
        &mut self,
        watch_id: WatchId,
        ran_for: Option<Duration>,
    ) -> Result<(), SupervisorError> {
        let policy = self.policy;
        let count = self.failures.entry(watch_id).or_default();
        if ran_for.is_some_and(|ran| ran >= policy.reset_after) {
            // The last run was stable long enough; start a fresh streak.
            *count = 0;
        }
        *count += 1;
        let attempt = *count;

        if attempt > policy.max_consecutive_failures {
            self.failures.remove(&watch_id);
            self.cancel_pending_restart(watch_id);
            warn!(
                watch = watch_id.get(),
                failures = attempt - 1,
                max = policy.max_consecutive_failures,
                "gave up restarting adapter after consecutive failures; publishing error and marking failed"
            );
            self.publish_giveup(watch_id, attempt - 1).await;
            self.storage
                .set_watch_state(watch_id, WatchState::Failed)
                .await?;
            return Ok(());
        }

        // No adapter runs during the backoff window, so drop the dead pid: a
        // session sees `Desired` (wanted, not running), never `Running` with a
        // stale pid. The successful restart re-sets `Running{new pid}`.
        self.storage
            .set_watch_state(watch_id, WatchState::Desired)
            .await?;

        let backoff = policy.backoff(attempt);
        // Bump the restart epoch and schedule under it, so a stop/sweep can cancel
        // this pending restart by moving the epoch on.
        let epoch = {
            let epoch = self.restart_epoch.entry(watch_id).or_default();
            *epoch += 1;
            *epoch
        };
        info!(
            watch = watch_id.get(),
            attempt,
            max = policy.max_consecutive_failures,
            backoff_ms = backoff.as_millis() as u64,
            "scheduling adapter restart after unexpected exit"
        );
        let cmd_tx = self.cmd_tx.clone();
        tokio::spawn(async move {
            tokio::time::sleep(backoff).await;
            let _ = cmd_tx.send(Command::Restart { watch_id, epoch }).await;
        });
        Ok(())
    }

    /// Cancel any pending backoff restart for `watch_id` by advancing its epoch,
    /// so an in-flight scheduled restart is ignored when it fires.
    fn cancel_pending_restart(&mut self, watch_id: WatchId) {
        if let Some(epoch) = self.restart_epoch.get_mut(&watch_id) {
            *epoch += 1;
        }
    }

    /// Publish the give-up error event on the entity's topic (design/01 rule 7:
    /// surface an error rather than restart forever). Best effort — a failure here
    /// is logged, not propagated (the watch is already being marked failed).
    async fn publish_giveup(&self, watch_id: WatchId, failures: u32) {
        let watch = match self.storage.get_watch(watch_id).await {
            Ok(Some(watch)) => watch,
            Ok(None) => return,
            Err(err) => {
                // Observability: the watch is being marked Failed but no signal
                // reaches the session, so make it loud.
                error!(watch = watch_id.get(), error = %err, "could not load watch to publish give-up event; watch marked failed with NO event surfaced");
                return;
            }
        };
        let Some(topic) = topic_for_watch(&watch) else {
            error!(
                watch = watch_id.get(),
                kind = watch.kind.as_str(),
                repo = %watch.repo,
                pr = watch.pr,
                "could not derive topic for give-up event; watch marked failed with NO event surfaced"
            );
            return;
        };
        let body = json!({
            "source": "mailbox-supervisor",
            "event": "adapter_gave_up",
            "repo": watch.repo,
            "pr": watch.pr,
            "consecutive_failures": failures,
        });
        match self
            .bus
            .publish(
                topic.clone(),
                AdapterId("mailbox-supervisor".to_string()),
                Timestamp(now_millis()),
                body,
            )
            .await
        {
            Ok(_) => info!(
                watch = watch_id.get(),
                topic = topic.as_str(),
                "published adapter give-up error event to entity topic"
            ),
            Err(err) => {
                error!(watch = watch_id.get(), error = %err, "failed to publish give-up event; watch marked failed with NO event surfaced")
            }
        }
    }

    /// Stop the adapter for `watch_id` and mark the watch `Stopped`, authoritatively.
    ///
    /// Teardown must not depend on the `running` map holding the entity: a session
    /// unwatching (or a sweep firing) DURING a crash-restart backoff finds no live
    /// entity, yet must still leave the watch `Stopped` and cancel the pending
    /// restart — otherwise the watch would be stuck `Running{dead pid}` (or
    /// resurrected by a stale scheduled restart) forever.
    async fn stop_watch(&mut self, watch_id: WatchId) -> Result<(), SupervisorError> {
        self.failures.remove(&watch_id);
        // Cancel any pending backoff restart so it cannot resurrect this watch.
        self.cancel_pending_restart(watch_id);
        if let Some(entity) = self.running.remove(&watch_id) {
            info!(
                watch = watch_id.get(),
                kind = entity.kind.as_str(),
                repo = %entity.repo,
                pr = entity.pr,
                pid = entity.pid,
                "stopping adapter (last interest gone or swept)"
            );
            // Signal graceful teardown; the monitor SIGTERMs, then SIGKILLs after
            // the grace, and reaps. Dropping the JoinHandle detaches (does not
            // cancel) the monitor, so teardown completes on its own.
            let _ = entity.stop_tx.send(());
        }
        // Authoritative: mark stopped whether or not an adapter was running, so a
        // teardown during the backoff window is never left in a Running/Desired
        // limbo with a dead or pending pid.
        self.storage
            .set_watch_state(watch_id, WatchState::Stopped)
            .await?;
        Ok(())
    }

    /// Sweep stale interests and stop the adapter of any watch swept to zero.
    async fn sweep(&mut self, ttl: Duration) -> Result<Vec<WatchId>, SupervisorError> {
        let ttl_millis = i64::try_from(ttl.as_millis()).unwrap_or(i64::MAX);
        let cutoff = now_millis().saturating_sub(ttl_millis);
        let emptied = self.storage.sweep_stale_interests(cutoff).await?;
        for &watch_id in &emptied {
            info!(
                watch = watch_id.get(),
                "watch interest hit zero via TTL sweep; stopping adapter"
            );
            self.stop_watch(watch_id).await?;
        }
        Ok(emptied)
    }

    /// Tear down every running adapter on shutdown. Aborting each monitor drops
    /// its pending `wait()` future, which triggers the card-07 process-group
    /// teardown (group SIGKILL + child reap) — fast and unconditional, so daemon
    /// shutdown is not prolonged by a per-adapter graceful grace.
    async fn shutdown_all(&mut self) {
        let entities: Vec<(WatchId, RunningEntity)> = self.running.drain().collect();
        if entities.is_empty() {
            return;
        }
        info!(
            count = entities.len(),
            "supervisor shutting down; tearing down all adapters"
        );
        let mut monitors = Vec::with_capacity(entities.len());
        for (watch_id, entity) in entities {
            entity.monitor.abort();
            monitors.push((watch_id, entity.monitor));
        }
        for (watch_id, monitor) in monitors {
            // Await the aborted task so its Drop (the group kill) has run.
            let _ = monitor.await;
            let _ = self
                .storage
                .set_watch_state(watch_id, WatchState::Stopped)
                .await;
        }
    }
}

/// Reply to an optional ack, logging a background failure that nobody is awaiting.
fn ack(reply: Option<Ack>, result: Result<(), SupervisorError>) {
    match reply {
        Some(tx) => {
            let _ = tx.send(result);
        }
        None => {
            if let Err(err) = result {
                error!(error = %err, "supervisor background command failed");
            }
        }
    }
}

/// Own an adapter for its lifetime: report an on-its-own exit (crash/natural), or
/// on a stop signal gracefully terminate its process group and reap.
///
/// This task owns the transport via [`AdapterHost::wait`]. Because `wait`/`stop`
/// both consume the transport, it cannot call `stop()`; instead it captures a
/// [`crate::host::subprocess::Terminator`] up front and, on the stop signal,
/// drives SIGTERM→SIGKILL itself while the same `wait()` future it holds reaps the
/// child and drains the pipes.
async fn monitor_adapter(
    transport: SubprocessTransport,
    watch_id: WatchId,
    generation: u64,
    pid: u32,
    mut stop_rx: oneshot::Receiver<()>,
    events: mpsc::Sender<Command>,
    stop_grace: Duration,
) {
    let terminator = transport.terminator();
    let wait_fut = transport.wait();
    tokio::pin!(wait_fut);

    tokio::select! {
        biased;
        result = &mut wait_fut => {
            // Exited on its own: crash or a finite adapter finishing. Report it so
            // the supervisor can restart (if still wanted) or mark stopped.
            let exit = result.unwrap_or_else(|err| {
                warn!(watch = watch_id.get(), pid, error = %err, "error awaiting adapter exit; treating as unknown");
                AdapterExit::Unknown
            });
            let _ = events
                .send(Command::AdapterExited { watch_id, generation, pid, exit })
                .await;
        }
        _ = &mut stop_rx => {
            // Commanded stop. The supervisor has already updated its own state and
            // storage, so we send no event — just tear the process group down.
            if let Err(err) = terminator.terminate() {
                warn!(watch = watch_id.get(), pid, error = %err, "failed to SIGTERM adapter group on stop");
            }
            if timeout(stop_grace, &mut wait_fut).await.is_err() {
                if let Err(err) = terminator.kill() {
                    warn!(watch = watch_id.get(), pid, error = %err, "failed to SIGKILL adapter group after grace");
                }
                let _ = (&mut wait_fut).await;
            }
            info!(watch = watch_id.get(), pid, "tore down adapter on stop signal");
        }
    }
}
