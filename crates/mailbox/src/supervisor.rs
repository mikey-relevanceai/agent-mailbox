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
//! While interest is 0 an exit is just a clean stop. A `Failed` watch is not dead
//! forever: the periodic sweep retries it once per interval while an interested
//! session is still alive (ADR-0011), so a give-up caused by a transient upstream
//! outage self-heals rather than needing a manual re-`watch`.
//!
//! # One notice per outage (ADR-0023)
//!
//! Those two mechanisms compose into a wake storm if left alone: the sweep retries
//! a `Failed` watch, the retry gives up again, and the give-up publishes again —
//! every interval, to every subscribed agent, for as long as the fault lasts. So
//! the give-up is announced **once per outage**. A watch that gives up again while
//! its notice still stands retries silently; when it finally runs stably again the
//! supervisor publishes a recovery event and re-arms the notice. An agent hears
//! "this stopped being watched" once and "it is being watched again" once, however
//! long the outage ran.
//!
//! # Bridge restart (resume iff a live session wants it)
//!
//! [`reconcile_startup`] restores the invariant on daemon start: it resumes a
//! watch iff some session holding an interest in it still has a live Claude Code
//! process (ADR-0017's probe), and marks the rest `Stopped`, clearing their stale
//! pids.
//! That is design/01 rule 6 as written — the rule always wanted this and only
//! defaulted to "never resume" for want of a liveness probe. Resuming nothing is
//! not a fail-safe under ADR-0008: an idle session takes zero turns, so it can
//! never re-`watch`, and a restart left it deaf forever.
//!
//! # Dependencies (one-way)
//!
//! `supervisor` → `host` + `bus` + `storage`. It never depends on `watch` (which
//! calls *into* it), so there is no cycle. Adapter program resolution is behind
//! the injectable [`AdapterResolver`], so this module knows nothing about any
//! concrete adapter — tests inject a fixture, card 10 injects the real
//! `github-pr` poller.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tracing::{debug, error, info, warn};

use mailbox_protocol::SessionId;
use mailbox_protocol::{AdapterId, GithubPr, Subject, Timestamp, Topic, stub_topic};

use crate::bus::Bus;
use crate::clock::now_millis;
use crate::host::subprocess::{AdapterSpec, SubprocessTransport};
use crate::host::{AdapterConfig, AdapterExit, AdapterHost, BaselineSink};
use crate::storage::{
    Pid, Storage, StorageError, Watch, WatchId, WatchKind, WatchState, WatchTarget,
};

/// Capacity of the supervisor command channel. Commands are small; a modest
/// buffer absorbs a burst of watch/unwatch ops plus monitor exit reports without
/// callers waiting, while staying bounded.
const COMMAND_CHANNEL_CAPACITY: usize = 64;

/// Provenance stamped on the events the supervisor itself publishes (give-up and
/// recovery) — the two events on an entity topic that are about the mailbox rather
/// than the entity. Named once so both carry the identical label.
const SUPERVISOR_ADAPTER_ID: &str = "mailbox-supervisor";

/// The `event` field of the supervisor's give-up notice, in the body an agent reads.
/// Public so a test asserts on the same constant the publisher uses rather than a
/// retyped literal — a mistyped event name in a "published nothing" assertion would
/// otherwise pass by counting a name that never existed.
pub const EVENT_ADAPTER_GAVE_UP: &str = "adapter_gave_up";

/// The `event` field of the recovery notice that withdraws [`EVENT_ADAPTER_GAVE_UP`].
pub const EVENT_ADAPTER_RECOVERED: &str = "adapter_recovered";

/// One of the two events on an entity topic that are about the MAILBOX rather than
/// the entity: the watch stopped being watched, and it started again.
///
/// An enum (mirroring the github-pr adapter's `Edge`) rather than two hand-rolled
/// bodies, so each event's wire name, body, subject and log field come from one
/// place — and so adding a third cannot compile without deciding all four.
#[derive(Debug, Clone, Copy)]
enum SupervisorEvent {
    /// The supervisor exhausted its restart budget and stopped trying (design/01
    /// rule 7). Announced once per outage (ADR-0023).
    GaveUp { failures: u32 },
    /// A watch that announced a give-up is running stably again, withdrawing it.
    Recovered,
}

impl SupervisorEvent {
    /// The `event` field on the wire, and the log field naming which event a failed
    /// publish lost.
    fn kind(self) -> &'static str {
        match self {
            Self::GaveUp { .. } => EVENT_ADAPTER_GAVE_UP,
            Self::Recovered => EVENT_ADAPTER_RECOVERED,
        }
    }

    /// The one line a woken agent sees (ADR-0022). Both say plainly that they are
    /// about the mailbox, not the PR: an agent woken by the first should stop
    /// expecting news, and one woken by the second can start expecting it again.
    fn subject(self) -> Option<Subject> {
        let text = match self {
            Self::GaveUp { failures } => format!(
                "mailbox stopped watching this: the adapter failed {failures} times in a row"
            ),
            Self::Recovered => "mailbox is watching this again: the adapter recovered".to_string(),
        };
        // `.ok()` because a subject is never worth failing a publish over — least of
        // all these publishes, whose whole job is to surface a change of state. Both
        // texts are statically well-formed, so `None` means one of them was EDITED
        // into something unparseable; `subjects_are_well_formed` fails the build
        // first, and the publish path logs if it ever happens anyway.
        Subject::new(&text, None).ok()
    }

    /// The opaque body, naming the entity it concerns.
    ///
    /// Deliberately the flat `repo`/`pr` pair for both events rather than fields
    /// destructured per [`WatchTarget`] variant: these two are the mailbox talking
    /// about a watch, so one shape across both keeps them readable by a consumer
    /// that does not care which kind of entity broke. (A `Stub` watch therefore
    /// reports its label as `repo` and `0` as `pr`, as the give-up event already
    /// did.) The entity itself is named by the topic, which is exact.
    fn body(self, watch: &Watch) -> Value {
        let repo = watch.target.repo_column();
        let pr = watch.target.pr_column();
        match self {
            Self::GaveUp { failures } => json!({
                "source": SUPERVISOR_ADAPTER_ID,
                "event": self.kind(),
                "repo": repo,
                "pr": pr,
                "consecutive_failures": failures,
            }),
            Self::Recovered => json!({
                "source": SUPERVISOR_ADAPTER_ID,
                "event": self.kind(),
                "repo": repo,
                "pr": pr,
            }),
        }
    }
}

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

/// A resolver for which no kind has an adapter — every kind resolves to
/// [`ResolveError::NoAdapter`], so `watch` records intent and the supervisor
/// leaves the watch `Desired` without spawning anything.
///
/// This was the `serve` default before card 09; `serve` now injects
/// [`crate::resolver::StubResolver`] (which spawns the reference adapter for
/// `stub` watches). `UnavailableResolver` is retained for the `watch` unit tests
/// that exercise the record-intent-only path without spawning a child.
pub struct UnavailableResolver;

impl AdapterResolver for UnavailableResolver {
    fn resolve(&self, watch: &Watch) -> Result<ResolvedAdapter, ResolveError> {
        Err(ResolveError::NoAdapter {
            kind: watch.target.kind(),
        })
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
            announced_giveup: HashSet::new(),
            restart_epoch: HashMap::new(),
            next_gen: Generation::default(),
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

    /// Run one TTL sweep: refresh the interests of every session in `live`, suspend
    /// the interests left older than `ttl` (ADR-0026), and stop any adapter whose interest
    /// thereby reached zero. Returns the watches that were swept to zero.
    ///
    /// `live` is the set of session ids that currently have a Claude Code process
    /// ([`crate::doctor::live_claude_sessions`]), read ONCE by the caller and handed
    /// in: the scan shells out to `ps`, so doing it per session would turn one sweep
    /// into one process spawn per interested session.
    pub async fn sweep(
        &self,
        ttl: Duration,
        live: BTreeSet<SessionId>,
    ) -> Result<Vec<WatchId>, SupervisorError> {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::Sweep { ttl, live, reply })
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

/// On daemon startup, restore the invariant *an adapter runs iff a live session
/// wants it* — resuming the watches whose interest belongs to a session that is
/// still alive, and stopping the rest.
///
/// # Why this resumes at all (design/01 rule 6, unblocked)
///
/// Rule 6 is "resume a watch only if at least one interested session is still
/// alive", but it deferred: "exact session-liveness probe is harness-specific;
/// until we have one, default to do not resume orphan watches". So this used to
/// mark EVERY previously-running watch `Stopped` and resume nothing, on the
/// reasoning that a live session would simply re-`watch`.
///
/// ADR-0008 removed that escape hatch. An idle session takes **zero** turns until
/// real mail arrives, so it will never re-`watch` — and the only event that could
/// make it take a turn is the one the stopped poller would have published. A
/// daemon restart therefore left every idle watcher permanently deaf, holding a
/// live `interest` against a dead adapter, with nothing in the system able to
/// notice. Observed in production (2026-07-17): a restart stopped all four PR
/// pollers; three had live sessions still waiting on them.
///
/// [ADR-0017](../../docs/adr/0017-daemon-bumps-the-sentinel.md) supplies the probe
/// rule 6 was waiting for: a session is alive iff a Claude Code process still
/// carries its id in argv ([`crate::doctor::live_claude_sessions`]) — the same
/// signal the TTL sweeper trusts. Rule 6's condition is therefore now decidable,
/// and this implements it as written. (ADR-0009 answered the same question from the
/// detached watcher's pidfile; that watcher is gone, and its pidfile routinely
/// outlived the agent it belonged to.)
///
/// # What it does
///
/// A watch is resumed iff some session holding an interest in it is in `live`. That
/// covers both a watch left `Running` by the previous daemon and one left `Stopped`
/// **with interest still attached** — an inconsistent state that the old no-resume
/// path itself created, and which would otherwise never heal, since `Stopped` means
/// "torn down, last interest gone" and this contradicts it.
///
/// A watch left `Running` that is NOT resumed is marked `Stopped`, clearing the
/// previous daemon's stale pid — the unchanged fail-safe: an interest whose
/// session cannot be proven alive gets no poller, so a dead session's watch cannot
/// resurrect a zombie API-poller. The TTL sweeper reclaims the leftover interest.
/// A `Desired` watch is left alone when it is not resumed: it carries no stale pid
/// to clear, so demoting it would be churn.
///
/// [`WatchState::Failed`] is left alone: it means the supervisor exhausted its
/// restart budget and gave up, and a daemon restart is not evidence the adapter
/// stopped crashing. Re-`watch` is the deliberate way back.
///
/// An over-broad `live` set costs one adapter that the TTL sweep then reclaims on
/// the next pass. That is strictly the cheaper error: the failure this replaces was
/// silent, permanent deafness.
pub async fn reconcile_startup(
    storage: &Storage,
    supervisor: &Supervisor,
    live: &BTreeSet<SessionId>,
) -> Result<(), SupervisorError> {
    for watch in storage.list_watches().await? {
        if matches!(watch.state, WatchState::Failed) {
            // Left alone by design (a restart is not evidence the adapter stopped
            // crashing). Logged so "what did reconcile do to watch X" is answerable.
            debug!(
                watch = watch.id.get(),
                "left a failed watch alone during startup reconcile"
            );
            continue;
        }

        let sessions = storage.list_watch_interest_sessions(watch.id).await?;
        let wanted = sessions.iter().any(|s| live.contains(s));

        if wanted {
            // `ensure_running` owns the state transition (and is idempotent), so
            // the stale pid is replaced by the new child's rather than cleared.
            supervisor.ensure_running(watch.id).await?;
            info!(
                watch = watch.id.get(),
                kind = watch.target.kind().as_str(),
                repo = %watch.target.repo_column(),
                pr = watch.target.pr_column(),
                interested = sessions.len(),
                "resumed watch on startup; an interested session is still running (design/01 rule 6)"
            );
        } else if let WatchState::Running { pid } = watch.state {
            // Write the state first, then log what happened — never log the write
            // as if it had already succeeded.
            storage
                .set_watch_state(watch.id, WatchState::Stopped)
                .await?;
            warn!(
                watch = watch.id.get(),
                kind = watch.target.kind().as_str(),
                repo = %watch.target.repo_column(),
                pr = watch.target.pr_column(),
                pid = pid.get(),
                interested = sessions.len(),
                "stopped a watch on startup; no interested session is still running (cleared its stale pid)"
            );
        } else {
            // Desired / already-Stopped and not live: carries no stale pid, so it is
            // left alone. Logged so the no-op is not a blind spot.
            debug!(
                watch = watch.id.get(),
                state = ?watch.state,
                "left watch alone during startup reconcile (not resumed; no stale pid to clear)"
            );
        }
    }
    Ok(())
}

/// Derive the topic a watch publishes on. Used to publish give-up error events
/// and by adapter resolvers building config. `None` if the stored repo is not a
/// well-formed `owner/repo`.
pub fn topic_for_watch(watch: &Watch) -> Option<Topic> {
    match &watch.target {
        WatchTarget::GithubPr { repo, pr } => {
            let (owner, repo) = repo.split_once('/')?;
            GithubPr::new(owner, repo, *pr).ok().map(|pr| pr.topic())
        }
        WatchTarget::Stub { label, .. } => stub_topic(label).ok(),
        WatchTarget::Slack(slack) => Some(slack.topic()),
    }
}

/// Merge the persisted baseline into an adapter's spawn config under a
/// `"baseline"` key (design/01 / card 10). `None` (never baselined) injects JSON
/// `null`, so an adapter always sees the key and treats null as "first poll —
/// baseline, publish nothing". A non-object config (e.g. `null`) is passed
/// through unchanged, since there is nowhere to insert the key.
fn inject_baseline(config: AdapterConfig, baseline: Option<Value>) -> AdapterConfig {
    let mut value = config.value().clone();
    if let Some(object) = value.as_object_mut() {
        object.insert("baseline".to_string(), baseline.unwrap_or(Value::Null));
    }
    AdapterConfig::new(value)
}

/// The persist side of baseline-via-protocol: a [`BaselineSink`] bound to one
/// watch's `(storage, watch_id)`. The transport calls it for every `Baseline`
/// line the adapter emits; it upserts the opaque snapshot into that watch's
/// `adapter_baseline` row. Best-effort — a persist failure is logged, not
/// propagated (a lost baseline degrades to at most one re-fired edge on the next
/// restart, never a crash).
struct StorageBaselineSink {
    storage: Storage,
    watch_id: WatchId,
}

impl BaselineSink for StorageBaselineSink {
    fn persist(&self, value: Value) -> Pin<Box<dyn Future<Output = ()> + Send + 'static>> {
        let storage = self.storage.clone();
        let watch_id = self.watch_id;
        Box::pin(async move {
            match storage.set_baseline(watch_id, value).await {
                Ok(()) => debug!(
                    watch = watch_id.get(),
                    "persisted adapter baseline via protocol"
                ),
                Err(err) => warn!(
                    watch = watch_id.get(),
                    error = %err,
                    "failed to persist adapter baseline; a restart may re-fire the last edge"
                ),
            }
        })
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
        epoch: RestartEpoch,
    },
    Sweep {
        ttl: Duration,
        /// The sessions with a live Claude Code process — the sweep's liveness
        /// evidence. Passed per-call for the same reason `ttl` is: it is the
        /// caller's measurement, not supervisor state.
        live: BTreeSet<SessionId>,
        reply: oneshot::Sender<Result<Vec<WatchId>, SupervisorError>>,
    },
    RunningPid {
        watch_id: WatchId,
        reply: oneshot::Sender<Option<u32>>,
    },
    Shutdown {
        reply: oneshot::Sender<()>,
    },
    /// A stability timer firing [`RestartPolicy::reset_after`] after a spawn: if
    /// that same instance is still up, the run has lasted long enough to count as
    /// recovered. `generation` guards against a stale timer from a superseded
    /// instance, exactly as [`Command::AdapterExited`] does.
    MarkStable {
        watch_id: WatchId,
        generation: Generation,
    },
    /// A monitor reporting that its adapter exited on its own (crash or natural
    /// end). `generation` guards against a stale report from a superseded instance.
    AdapterExited {
        watch_id: WatchId,
        generation: Generation,
        pid: u32,
        exit: AdapterExit,
    },
}

type Ack = oneshot::Sender<Result<(), SupervisorError>>;

/// Identifies one adapter INSTANCE, so a late report from a superseded instance is
/// ignored. Monotonic across the actor.
///
/// Branded, because the actor carries two `u64` staleness tokens with opposite
/// lifecycles — this one and [`RestartEpoch`] — whose commands sit side by side in
/// [`Command`] and are dispatched in the same `match`. Confusing them type-checks as
/// bare integers, and both failure modes are silent: a give-up notice never
/// withdrawn, or a stale timer publishing a false recovery.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
struct Generation(u64);

impl Generation {
    fn next(self) -> Self {
        Self(self.0 + 1)
    }

    fn get(self) -> u64 {
        self.0
    }
}

/// Identifies one SCHEDULED RESTART for a watch. Per-watch, and bumped to cancel a
/// pending restart so it cannot resurrect a torn-down watch. See [`Generation`] for
/// why this is a distinct type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
struct RestartEpoch(u64);

impl RestartEpoch {
    fn next(self) -> Self {
        Self(self.0 + 1)
    }
}

/// A running adapter as the actor tracks it. The transport itself is owned by the
/// monitor task (via `wait()`); here we keep only what the actor needs to
/// identify, log, and stop it.
struct RunningEntity {
    pid: u32,
    /// Distinguishes this instance from any predecessor/successor for the same
    /// entity, so a late exit report from an old instance is ignored.
    generation: Generation,
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
    /// Entities whose give-up has been announced and not yet withdrawn — the
    /// once-per-outage latch of ADR-0023. A watch in here has already told its
    /// subscribers it stopped being watched, so giving up again publishes nothing.
    /// [`Actor::on_stable`] withdraws the notice (with a recovery event); teardown
    /// withdraws it silently. Deliberately in memory, not storage: it is a property
    /// of *this* daemon's conversation with its subscribers, and a daemon restart
    /// is genuinely new information worth one fresh notice.
    announced_giveup: HashSet<WatchId>,
    /// The epoch of the currently-scheduled restart per entity. A detached
    /// backoff task carries the epoch it was scheduled under; if the epoch has
    /// since moved (a stop/sweep/newer schedule bumped it), that task's restart is
    /// stale and must not resurrect a torn-down watch.
    restart_epoch: HashMap<WatchId, RestartEpoch>,
    next_gen: Generation,
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
                Command::Sweep { ttl, live, reply } => {
                    let result = self.sweep(ttl, &live).await;
                    let _ = reply.send(result);
                }
                Command::RunningPid { watch_id, reply } => {
                    let _ = reply.send(self.running.get(&watch_id).map(|e| e.pid));
                }
                Command::MarkStable {
                    watch_id,
                    generation,
                } => self.on_stable(watch_id, generation).await,
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
                    kind = watch.target.kind().as_str(),
                    repo = %watch.target.repo_column(),
                    pr = watch.target.pr_column(),
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
                    kind = watch.target.kind().as_str(),
                    reason = %err,
                    "did not start adapter; no program resolved for this watch kind"
                );
                return Ok(());
            }
        };

        // Baseline-via-protocol (design/01 / card 10): read the persisted baseline
        // and inject it into the adapter's spawn config, so an edge-triggered
        // adapter resumes from its last snapshot and does not re-fire already-
        // baselined edges on restart. Read here (not in the resolver) so the
        // resolver stays storage-free. Harmless for adapters that ignore it (the
        // stub drops the extra field).
        let baseline = self.storage.get_baseline(watch_id).await?;
        let config = inject_baseline(resolved.config, baseline);
        // The persist side of the round trip: relay each `Baseline` line the
        // adapter emits to `set_baseline` for THIS watch, bound behind a sink so
        // the transport stays decoupled from storage (mirrors Publish→bus).
        let sink: Arc<dyn BaselineSink> = Arc::new(StorageBaselineSink {
            storage: self.storage.clone(),
            watch_id,
        });
        // Provenance (review item C): bind the transport to this entity's topic so
        // the adapter can publish ONLY on it — a Publish to any other topic is
        // rejected by the host. A watch whose stored repo is not a well-formed
        // entity topic cannot be provenance-bound, so we refuse to start it
        // unconstrained (counts as a failed start).
        let Some(entity_topic) = topic_for_watch(&watch) else {
            warn!(
                watch = watch_id.get(),
                kind = watch.target.kind().as_str(),
                repo = %watch.target.repo_column(),
                "cannot derive an entity topic to bind the adapter's publishes; not starting"
            );
            return self.handle_failure(watch_id, None).await;
        };
        match SubprocessTransport::start_with_baseline(
            resolved.spec,
            config,
            self.bus.clone(),
            sink,
            entity_topic,
        )
        .await
        {
            Ok(transport) => self.install_running(watch_id, &watch, transport).await,
            Err(err) => {
                warn!(
                    watch = watch_id.get(),
                    kind = watch.target.kind().as_str(),
                    repo = %watch.target.repo_column(),
                    pr = watch.target.pr_column(),
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
        self.next_gen = self.next_gen.next();
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
                kind: watch.target.kind(),
                repo: watch.target.repo_column().to_string(),
                pr: watch.target.pr_column(),
                stop_tx,
                monitor,
            },
        );
        // Arm the stability timer. A crash-restart streak is only detectable as
        // *over* by a run that outlives it, and an adapter that recovers runs
        // forever — so nothing but a timer can observe the recovery of a watch that
        // never exits again (ADR-0023).
        let cmd_tx = self.cmd_tx.clone();
        let reset_after = self.policy.reset_after;
        tokio::spawn(async move {
            tokio::time::sleep(reset_after).await;
            let _ = cmd_tx
                .send(Command::MarkStable {
                    watch_id,
                    generation,
                })
                .await;
        });
        self.storage
            .set_watch_state(watch_id, WatchState::Running { pid: Pid::new(pid) })
            .await?;
        info!(
            watch = watch_id.get(),
            kind = watch.target.kind().as_str(),
            repo = %watch.target.repo_column(),
            pr = watch.target.pr_column(),
            pid,
            generation = generation.get(),
            "spawned adapter for entity (one per external entity)"
        );
        Ok(())
    }

    /// Handle a monitor's report that its adapter exited on its own.
    async fn on_adapter_exited(
        &mut self,
        watch_id: WatchId,
        generation: Generation,
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
            self.forget_giveup(watch_id);
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

        // A clean exit 0 is a NATURAL COMPLETION, not a crash — a finite adapter
        // (e.g. a `stub --count N`) that published its batch and returned. Even
        // with interest still held, restarting it would republish the batch
        // forever (or, for short runs, exhaust the budget and falsely mark a
        // healthy adapter Failed with a bogus give-up event). So a clean exit is
        // TERMINAL: mark the watch Stopped and do not restart. Only a crash — a
        // non-zero code or a signal — takes the backoff-restart path below.
        if matches!(exit, AdapterExit::Exited { code: 0 }) {
            self.failures.remove(&watch_id);
            self.forget_giveup(watch_id);
            self.storage
                .set_watch_state(watch_id, WatchState::Stopped)
                .await?;
            info!(
                watch = watch_id.get(),
                kind = kind.as_str(),
                repo = %repo,
                pr,
                pid,
                interest,
                "adapter completed cleanly (exit 0) with interest still held; marked stopped (no restart)"
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
            let failures = attempt - 1;
            // Announce once per outage (ADR-0023). A sweep retry that gives up again
            // is the SAME outage continuing, not news: re-publishing would wake every
            // subscriber once per sweep interval for as long as the fault lasts.
            if self.announced_giveup.insert(watch_id) {
                warn!(
                    watch = watch_id.get(),
                    failures,
                    max = policy.max_consecutive_failures,
                    "gave up restarting adapter after consecutive failures; publishing error and marking failed"
                );
                self.publish_supervisor_event(watch_id, SupervisorEvent::GaveUp { failures })
                    .await;
            } else {
                warn!(
                    watch = watch_id.get(),
                    failures,
                    max = policy.max_consecutive_failures,
                    "gave up restarting adapter again; its give-up is already surfaced, so published nothing (marked failed)"
                );
            }
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
            *epoch = epoch.next();
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

    /// A spawned adapter has now been up for [`RestartPolicy::reset_after`] — the
    /// run is stable, so any outage it was recovering from is over.
    ///
    /// Withdraws an announced give-up and publishes the recovery event. A watch with
    /// no announced give-up recovered from nothing, so this is silent: only an agent
    /// that was told the watch went dark is told it came back.
    ///
    /// A stale timer is ignored — recovery means *this* instance lasted, not that
    /// some instance started. That distinction is the whole point: while the network
    /// is down every sweep retry starts an adapter that dies seconds later, and
    /// treating a mere start as recovery would restore the very wake storm this
    /// avoids, one "recovered" per interval.
    async fn on_stable(&mut self, watch_id: WatchId, generation: Generation) {
        let current = self
            .running
            .get(&watch_id)
            .is_some_and(|entity| entity.generation == generation);
        // Two steps, not one `||`: the guard must be checked BEFORE the latch is
        // touched, and short-circuit order is too quiet a place to keep that. A
        // stale timer that withdrew a live notice would leave the outage announced
        // never — silent deafness, which is the failure this whole path prevents.
        if !current {
            return;
        }
        if !self.announced_giveup.remove(&watch_id) {
            return;
        }
        info!(
            watch = watch_id.get(),
            generation = generation.get(),
            stable_after_ms = self.policy.reset_after.as_millis() as u64,
            "adapter ran stably after a give-up; withdrawing the give-up notice and publishing recovery"
        );
        self.publish_supervisor_event(watch_id, SupervisorEvent::Recovered)
            .await;
    }

    /// Withdraw an announced give-up SILENTLY, because the watch is being torn down.
    ///
    /// No recovery event: the adapter did not recover, it stopped being wanted, and
    /// telling subscribers a stopped watch is "being watched again" would be a lie.
    /// The latch is dropped so that if this entity is watched again later, its next
    /// outage is a new outage and gets its own notice.
    fn forget_giveup(&mut self, watch_id: WatchId) {
        if self.announced_giveup.remove(&watch_id) {
            debug!(
                watch = watch_id.get(),
                "dropped an announced give-up on teardown; a later failure is a new outage"
            );
        }
    }

    /// Cancel any pending backoff restart for `watch_id` by advancing its epoch,
    /// so an in-flight scheduled restart is ignored when it fires.
    fn cancel_pending_restart(&mut self, watch_id: WatchId) {
        if let Some(epoch) = self.restart_epoch.get_mut(&watch_id) {
            *epoch = epoch.next();
        }
    }

    /// Publish one supervisor-authored event on the watch's entity topic.
    ///
    /// Best effort throughout — a failure is logged, not propagated: the state change
    /// the event describes has already happened and is not worth unwinding over a lost
    /// notification. Every path that cannot surface the event logs at `error`, because
    /// that is a state change NO signal reaches the session about, which is precisely
    /// the silent failure these events exist to avoid. A watch that has simply been
    /// deleted is not one of those — nobody is left to tell — so it returns quietly.
    async fn publish_supervisor_event(&self, watch_id: WatchId, event: SupervisorEvent) {
        let event_kind = event.kind();
        let watch = match self.storage.get_watch(watch_id).await {
            Ok(Some(watch)) => watch,
            Ok(None) => return,
            Err(err) => {
                error!(watch = watch_id.get(), event = event_kind, error = %err, "could not load watch to publish a supervisor event; NO event surfaced");
                return;
            }
        };
        let Some(topic) = topic_for_watch(&watch) else {
            error!(
                watch = watch_id.get(),
                event = event_kind,
                kind = watch.target.kind().as_str(),
                repo = %watch.target.repo_column(),
                pr = watch.target.pr_column(),
                "could not derive topic for a supervisor event; NO event surfaced"
            );
            return;
        };
        // A subject-less wake says nothing but "something happened", which for the
        // recovery event defeats its whole purpose. Unreachable for both texts as
        // written, so if it ever fires it is a broken edit, not a bad input — and
        // the log is the only place that would show it.
        let subject = event.subject();
        if subject.is_none() {
            warn!(
                watch = watch_id.get(),
                event = event_kind,
                "could not build a subject for a supervisor event; publishing it subject-less"
            );
        }
        match self
            .bus
            .publish(
                topic.clone(),
                AdapterId(SUPERVISOR_ADAPTER_ID.to_string()),
                Timestamp(now_millis()),
                event.body(&watch),
                subject,
            )
            .await
        {
            Ok(_) => info!(
                watch = watch_id.get(),
                event = event_kind,
                topic = topic.as_str(),
                "published supervisor event to entity topic"
            ),
            Err(err) => {
                error!(watch = watch_id.get(), event = event_kind, error = %err, "failed to publish supervisor event; NO event surfaced")
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
        self.forget_giveup(watch_id);
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

    /// Refresh the interests of every session that still has a Claude Code process,
    /// then sweep what is left stale and stop the adapter of any watch swept to zero.
    ///
    /// The refresh pass is what makes the TTL safe (ADR-0009, superseded by
    /// ADR-0017). An idle session is SILENT by design — zero turns, zero requests —
    /// so silence carries no information about whether it is alive, and a TTL keyed
    /// on the session's own traffic reaps exactly the healthy idle sessions the
    /// on-demand wake exists to enable. Liveness therefore comes from the process
    /// table: Claude Code carries the session id in its own argv, so "is this agent
    /// still running?" is answerable from outside with no agent cooperation, no timer
    /// in the harness and no model turn. That makes the invariant *an interest lives
    /// iff its session's agent lives*, and leaves the TTL as what it was always
    /// documented to be: the backstop for a session that hard-died without a
    /// `SessionEnd`.
    ///
    /// The signal it replaces — the detached watcher's pidfile — was strictly worse:
    /// an orphaned watcher outlives the agent it belongs to, so a dead session read
    /// as alive and its poller was kept running indefinitely.
    async fn sweep(
        &mut self,
        ttl: Duration,
        live: &BTreeSet<SessionId>,
    ) -> Result<Vec<WatchId>, SupervisorError> {
        let now = now_millis();
        for session in self.storage.list_interest_sessions().await? {
            if live.contains(&session) {
                let refreshed = self
                    .storage
                    .touch_session_interests(session.clone(), now)
                    .await?;
                debug!(
                    session = session.as_str(),
                    refreshed, "refreshed a live session's interests"
                );
            }
        }

        // Cutoff from the same `now` the refresh stamped, so a just-refreshed
        // interest can never be older than it.
        let ttl_millis = i64::try_from(ttl.as_millis()).unwrap_or(i64::MAX);
        let cutoff = now.saturating_sub(ttl_millis);
        let emptied = self.storage.sweep_stale_interests(cutoff).await?;
        for &watch_id in &emptied {
            info!(
                watch = watch_id.get(),
                "watch interest hit zero via TTL sweep; stopping adapter"
            );
            self.stop_watch(watch_id).await?;
        }

        // After reclaiming dead interests, give the still-wanted `Failed` watches
        // another chance. Runs last so a watch whose only session just aged out
        // above is not retried.
        self.retry_failed_watches(live).await?;
        Ok(emptied)
    }

    /// Retry the watches that gave up (`Failed`) but whose session is still here.
    ///
    /// A give-up is meant to be terminal for a genuinely broken adapter (design/01
    /// rule 7 — surface an error rather than restart forever). But a crash streak
    /// caused by a *transient* upstream outage — the GitHub API failing during a
    /// daemon restart is the case that motivated this (ADR-0011) — would otherwise
    /// park a healthy watch in `Failed` until a human re-`watch`ed it. That is the
    /// silent, manual-recovery failure ADR-0010 set out to kill, re-entering
    /// through the one state ADR-0010 deliberately does not resume.
    ///
    /// The sweeper already proves session liveness from the process table, so it
    /// is the natural owner of a slow retry: once per sweep it re-attempts each
    /// `Failed` watch that still has a live interested session. [`ensure_running`]
    /// starts a fresh attempt — the failure streak was cleared at give-up — so a
    /// transient failure recovers within one sweep, while a genuinely broken
    /// adapter simply crash-loops back to `Failed` and waits for the next sweep.
    /// Retries are therefore bounded to one backoff burst per (slow) sweep
    /// interval, and stop entirely the moment the session ends and its interest is
    /// reclaimed. A `Failed` watch with no live interested session is left alone —
    /// no zombie retries, the same liveness invariant as the TTL sweep itself.
    async fn retry_failed_watches(
        &mut self,
        live: &BTreeSet<SessionId>,
    ) -> Result<(), SupervisorError> {
        for watch in self.storage.list_watches().await? {
            if !matches!(watch.state, WatchState::Failed) {
                continue;
            }
            let sessions = self.storage.list_watch_interest_sessions(watch.id).await?;
            if sessions.iter().any(|s| live.contains(s)) {
                // Log after the retry is issued, not before — never claim a retry
                // that a propagated error would abort (mirrors `reconcile_startup`).
                self.ensure_running(watch.id).await?;
                info!(
                    watch = watch.id.get(),
                    kind = watch.target.kind().as_str(),
                    repo = %watch.target.repo_column(),
                    pr = watch.target.pr_column(),
                    interested = sessions.len(),
                    "retried a failed watch; an interested session is still alive (slow background retry)"
                );
            }
        }
        Ok(())
    }

    /// Tear down every running adapter on shutdown — GRACEFULLY (review item D).
    ///
    /// Each monitor is signalled to stop (SIGTERM → bounded grace → SIGKILL) rather
    /// than aborted-into-an-immediate-SIGKILL. The graceful path gives an edge-
    /// triggered adapter (the github-pr poller) the window to finish its in-flight
    /// poll and flush its final `Baseline` line, and the monitor's `wait()` drains
    /// stdout so that baseline is relayed to storage BEFORE the process is reaped.
    /// This shrinks the at-least-once window on the COMMON (graceful) shutdown path
    /// so a routine restart does not re-fire the last edge.
    ///
    /// Delivery remains at-least-once across an UNGRACEFUL termination (SIGKILL /
    /// OOM / power loss): if the adapter dies between publishing an edge and
    /// emitting its baseline, resuming from the older baseline re-fires that edge.
    /// That is consistent with the card-06 publish-at-least-once stance and is
    /// tolerable for a wake bus — a duplicate wake makes the agent re-check and
    /// find the same state (see ADR-0005).
    async fn shutdown_all(&mut self) {
        let entities: Vec<(WatchId, RunningEntity)> = self.running.drain().collect();
        if entities.is_empty() {
            return;
        }
        info!(
            count = entities.len(),
            "supervisor shutting down; gracefully stopping all adapters (flush final baseline)"
        );
        // Signal every adapter to stop FIRST, so they tear down concurrently rather
        // than one graceful grace after another.
        let mut monitors = Vec::with_capacity(entities.len());
        for (watch_id, entity) in entities {
            let _ = entity.stop_tx.send(());
            monitors.push((watch_id, entity.monitor));
        }
        for (watch_id, monitor) in monitors {
            // Await the graceful teardown (SIGTERM→drain→reap) so the final
            // baseline has been relayed before we mark the watch stopped.
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
    generation: Generation,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inject_baseline_adds_persisted_snapshot() {
        let config = AdapterConfig::new(json!({ "topic": "github.pr.o/r#1" }));
        let injected = inject_baseline(config, Some(json!({ "mergeable": "conflicting" })));
        assert_eq!(
            injected.value()["baseline"],
            json!({ "mergeable": "conflicting" }),
            "the persisted baseline is merged under the baseline key"
        );
        assert_eq!(injected.value()["topic"], "github.pr.o/r#1");
    }

    #[test]
    fn inject_baseline_uses_null_when_unset() {
        // A never-baselined watch injects JSON null, so the adapter still sees the
        // key and treats it as "first poll".
        let config = AdapterConfig::new(json!({ "topic": "t" }));
        let injected = inject_baseline(config, None);
        assert_eq!(injected.value()["baseline"], Value::Null);
    }

    #[test]
    fn inject_baseline_passes_through_non_object_config() {
        // Nowhere to insert the key, so a non-object config is unchanged.
        let injected = inject_baseline(AdapterConfig::new(Value::Null), Some(json!({ "x": 1 })));
        assert_eq!(*injected.value(), Value::Null);
    }

    fn watch_fixture() -> Watch {
        Watch {
            id: WatchId::new(1),
            target: WatchTarget::GithubPr {
                repo: "octocat/hello-world".to_string(),
                pr: 42,
            },
            interval: Duration::from_secs(60),
            state: WatchState::Failed,
        }
    }

    /// The give-up body carries the failure count, which is the only thing in it a
    /// reader could not get from the topic — and the only field the integration
    /// tests, which match on `event` alone, would not notice the loss of.
    #[test]
    fn giveup_body_names_the_entity_and_the_failure_count() {
        let body = SupervisorEvent::GaveUp { failures: 5 }.body(&watch_fixture());
        assert_eq!(body["event"], EVENT_ADAPTER_GAVE_UP);
        assert_eq!(body["consecutive_failures"], 5);
        assert_eq!(body["repo"], "octocat/hello-world");
        assert_eq!(body["pr"], 42);
        assert_eq!(body["source"], SUPERVISOR_ADAPTER_ID);
    }

    /// Recovery says only that the watch is back — no failure count, because the
    /// outage it withdraws is over and its size is no longer actionable.
    #[test]
    fn recovery_body_names_the_entity_and_nothing_about_failures() {
        let body = SupervisorEvent::Recovered.body(&watch_fixture());
        assert_eq!(body["event"], EVENT_ADAPTER_RECOVERED);
        assert_eq!(body["repo"], "octocat/hello-world");
        assert_eq!(body["pr"], 42);
        assert_eq!(
            body.get("consecutive_failures"),
            None,
            "a recovery has no failure count to report"
        );
    }

    /// Both subjects must actually parse. They are the only thing a woken agent
    /// sees (ADR-0022), and `subject()` swallows a parse failure rather than fail a
    /// publish — so without this, editing either text into something unparseable
    /// would degrade a live wake to silence with nothing failing first.
    #[test]
    fn subjects_are_well_formed() {
        let giveup = SupervisorEvent::GaveUp { failures: 5 }
            .subject()
            .expect("the give-up subject parses");
        assert!(
            giveup.text().contains("stopped watching this"),
            "got {:?}",
            giveup.text()
        );
        let recovered = SupervisorEvent::Recovered
            .subject()
            .expect("the recovery subject parses");
        assert!(
            recovered.text().contains("watching this again"),
            "got {:?}",
            recovered.text()
        );
    }
}
