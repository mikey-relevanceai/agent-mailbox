//! `SubprocessTransport`: host an adapter as a child process (the v0 transport).
//!
//! # Lifecycle
//!
//! [`SubprocessTransport::start`] spawns the child (as its own process-group
//! leader) with piped stdio, launches two tasks — one pumping the child's
//! `stdout` (its `Publish` output) onto the bus, one draining its `stderr` into
//! `tracing` — then writes the config line. The returned handle owns the child
//! and both tasks; [`AdapterHost::stop`] / [`AdapterHost::wait`] terminate/await
//! it and reap.
//!
//! # Config delivery: first stdin line, then EOF (bounded)
//!
//! The opaque [`AdapterConfig`] is written as the first line of the child's
//! stdin (one JSON object + `\n`), then stdin is closed. Closing it is a
//! deliberate choice: the child→bridge direction is stdout-only, so the child
//! never needs more stdin, and a clean EOF lets an adapter that reads stdin in a
//! loop terminate that loop. The config line is the adapter's *own* schema — it
//! is **not** wrapped in a `mailbox-protocol` version frame, because the host
//! does not interpret it (opaque-body principle, ADR-0001).
//!
//! The write is **bounded** ([`HostLimits::config_write_timeout`]): config is
//! expected to be modest (comfortably under a pipe buffer, low tens of KiB), and
//! an adapter is expected to consume it promptly. A child that never reads stdin
//! plus a config larger than the pipe buffer would otherwise block the write
//! forever; instead `start` fails with [`HostError::ConfigWriteTimeout`] rather
//! than hang. The stdout/stderr readers are spawned *before* the write so the
//! child can make progress (drain its own stdout) while we deliver config — no
//! stdin-write-vs-stdout-flood deadlock.
//!
//! # Child → bridge: reuse the bus publish path
//!
//! The child writes `mailbox_protocol::Publish` messages as NDJSON to stdout.
//! The transport decodes each with the card-02 framing helper ([`decode_line`],
//! tagged with a [`LineError`] line number for diagnostics) and forwards it via
//! [`Bus::publish`] — the *same* card-04 path a hand-run `mailbox publish` takes,
//! so a transport-delivered publish and a hand-run one are equivalent: both are
//! durable and both fire the card-05 wake kick. We reuse that path rather than
//! reimplement durability or wake.
//!
//! An edge-triggered adapter may also emit a [`mailbox_protocol::Baseline`] line
//! (design/01 / card 10: baseline-via-protocol). The transport relays its opaque
//! snapshot to an optional [`BaselineSink`] the supervisor binds to `(storage,
//! watch_id)` — the same decoupling as `Publish`→bus, keeping the transport
//! storage-free. With no sink bound (a hand-run, a happy-path test) a baseline
//! line is a harmless no-op.
//!
//! # Adapter identity comes from the spawn
//!
//! Every forwarded event is stamped with the [`AdapterId`] the *host* was given
//! at spawn, ignoring whatever `adapter` field the child put on the wire. The
//! child is untrusted content (ADR-0001); provenance must be controlled by who
//! started the adapter, not self-asserted by it.
//!
//! # Robustness (no crash, no OOM, no log DoS)
//!
//! - A malformed/oversized/non-`Publish` stdout line is logged with its line
//!   number and skipped; the transport keeps reading and the bridge stays up
//!   (AC2). Repeated rejects raise [`AdapterHealth::rejected`] so a supervisor
//!   can act — but one bad line is never fatal.
//! - stdout is read with a per-line byte cap ([`DEFAULT_MAX_LINE_BYTES`], env
//!   override [`ENV_MAX_LINE_BYTES`]), reusing the bounded-frame discipline from
//!   the card-06 daemon: a runaway adapter spewing a newline-less stream cannot
//!   grow the host's memory — bytes past the cap are counted and discarded to the
//!   next newline so the stream resyncs.
//! - stderr is logged line-by-line, but only the first [`STDERR_INFO_LIMIT`]
//!   lines per instance are at `info`; beyond that lines are demoted to `debug`
//!   and periodically summarized, so a child flooding stderr cannot DoS the log
//!   volume.
//!
//! # Stop & teardown: whole process group, always reap, never wedge
//!
//! The child is spawned as a **process-group leader** (`process_group(0)`, so
//! its pgid equals its pid), and every termination signal targets the whole
//! group (`kill(-pid, …)`). That is the "no zombie pollers" guarantee: an
//! adapter that spawns a grandchild (a helper, a `gh` poller) has that grandchild
//! torn down by the same signal — nothing is reparented to init and left running.
//!
//! [`AdapterHost::stop`] sends `SIGTERM` to the group (a chance to flush/clean
//! up), waits a bounded grace ([`HostLimits::stop_grace`]), and if the child is
//! still alive sends `SIGKILL` to the group. Either way it then `wait`s to reap
//! the direct child, so no zombie survives; group members reparented to init are
//! reaped by init. As a backstop, dropping the handle without stop/wait kills the
//! group (see [`ChildProcess`]'s `Drop`) and tokio's `kill_on_drop` reaps the
//! direct child.
//!
//! After the child is reaped, the stdio tasks are drained under a bound
//! ([`IO_DRAIN_GRACE`]) and then **aborted**: a grandchild that inherited the
//! stdout pipe could hold it open past the child's own exit, so an unbounded
//! join would wedge stop/wait forever — the bounded drain forwards any buffered
//! publishes, then the abort guarantees return.

use std::os::unix::process::ExitStatusExt;
use std::process::{ExitStatus, Stdio};
use std::sync::Arc;
use std::time::Duration;

use nix::errno::Errno;
use nix::sys::signal::{Signal, kill};
use nix::unistd::Pid;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStderr, ChildStdout, Command};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tracing::{debug, info, warn};

use mailbox_protocol::{AdapterId, LineError, Message, Timestamp, Topic, decode_line};

use crate::bus::Bus;

use super::{AdapterConfig, AdapterExit, AdapterHealth, AdapterHost, BaselineSink, HealthCounters};

/// Default per-line cap on the child's stdout. A single `Publish` line is a
/// topic + an opaque body; 1 MiB is generous head room for a real event while
/// bounding host memory against a newline-less flood. Smaller than the card-06
/// control-frame cap because an adapter event is smaller than a full control
/// request. Not load-bearing exactly; the *existence* of a bound is.
const DEFAULT_MAX_LINE_BYTES: usize = 1024 * 1024;

/// How long [`AdapterHost::stop`] waits after SIGTERM before escalating to
/// SIGKILL. Long enough for a cooperative adapter to flush and exit, short
/// enough that a stuck one is not tolerated indefinitely.
const DEFAULT_STOP_GRACE: Duration = Duration::from_secs(3);

/// How long `start` waits for the child to consume its config line before
/// failing (rather than hanging on a full stdin pipe a deaf child never drains).
const DEFAULT_CONFIG_WRITE_TIMEOUT: Duration = Duration::from_secs(10);

/// Bound on draining a stdio task after the child is reaped. A descendant that
/// inherited the pipe could hold it open forever; we drain up to this long to
/// forward buffered publishes, then abort so stop()/wait() can never wedge on a
/// stray fd.
const IO_DRAIN_GRACE: Duration = Duration::from_secs(5);

/// stderr lines logged at `info` per instance before further lines are demoted
/// to `debug` and counted, so a flooding child cannot blow up log volume.
const STDERR_INFO_LIMIT: u64 = 1000;

/// While flooding (past [`STDERR_INFO_LIMIT`]), emit one aggregate `info` line
/// per this many demoted lines so the flood stays visible without being verbose.
const STDERR_SUPPRESS_REPORT_EVERY: u64 = 10_000;

/// Env override for the stdout per-line byte cap.
const ENV_MAX_LINE_BYTES: &str = "MAILBOX_ADAPTER_MAX_LINE_BYTES";
/// Env override for the SIGTERM→SIGKILL grace, in milliseconds.
const ENV_STOP_GRACE_MS: &str = "MAILBOX_ADAPTER_STOP_GRACE_MS";
/// Env override for the config-write timeout, in milliseconds.
const ENV_CONFIG_WRITE_MS: &str = "MAILBOX_ADAPTER_CONFIG_WRITE_MS";

/// A failure operating the subprocess transport — this is
/// `<SubprocessTransport as AdapterHost>::Error`. Business errors are values,
/// never panics (AGENTS.md / type-driven design). Transport-level I/O the
/// transport recovers from (a single bad stdout line) is *not* here — it is
/// counted in [`AdapterHealth`] instead; this type is for failures that abort a
/// start/stop/wait operation.
///
/// Note it never exposes a `nix` type: a signalling failure is normalized to
/// [`std::io::Error`] so no transport-crate type crosses even this concrete
/// error (the trait boundary is transport-agnostic via `AdapterHost::Error`).
#[derive(Debug, thiserror::Error)]
pub enum HostError {
    /// The child process could not be spawned.
    #[error("could not spawn adapter {program:?}: {source}")]
    Spawn {
        program: String,
        #[source]
        source: std::io::Error,
    },

    /// The child was spawned without a stdio handle we require (stdin for
    /// config, stdout for publishes, stderr for logs).
    #[error("adapter child is missing its {stream} pipe")]
    MissingStdio { stream: &'static str },

    /// The child had no pid immediately after spawn. Not expected (a fresh child
    /// always has one), but resolved once here rather than threaded through the
    /// lifecycle as a dead `Option` state.
    #[error("adapter child had no pid immediately after spawn")]
    MissingPid,

    /// Writing the config line to the child's stdin failed.
    #[error("could not deliver config to adapter stdin: {0}")]
    ConfigDelivery(#[source] std::io::Error),

    /// The child did not consume its config line within the write timeout — most
    /// likely it never reads stdin, or the config exceeds the pipe buffer.
    #[error(
        "adapter did not consume its config within {0:?}; a child must read its config line promptly"
    )]
    ConfigWriteTimeout(Duration),

    /// The config value could not be serialized to a single JSON line.
    #[error("could not serialize adapter config: {0}")]
    ConfigEncode(#[source] serde_json::Error),

    /// Waiting on / reaping the child failed.
    #[error("could not wait on adapter child: {0}")]
    Wait(#[source] std::io::Error),

    /// Sending a termination signal to the child's process group failed for a
    /// reason other than "already gone" (`ESRCH`, treated as success — the goal
    /// is reached). The `nix` errno is normalized to an `io::Error` so no
    /// transport-crate type leaks out.
    #[error("could not signal adapter process group (pid {pid}): {source}")]
    Signal {
        pid: u32,
        #[source]
        source: std::io::Error,
    },
}

/// Runtime host limits: the stdout per-line byte cap, the stop grace, and the
/// config-write timeout.
///
/// Kept as an explicit, passable value (not only env-read) so a supervisor
/// (card 08) or a test can set them programmatically. [`HostLimits::from_env`]
/// is the default source — env overrides, else the constants — which is what
/// the plain [`SubprocessTransport::start`] uses. Tests pass explicit limits via
/// [`SubprocessTransport::start_with_limits`] so they can exercise the oversized,
/// SIGKILL-after-grace, and config-write-timeout paths fast, without the
/// process-global env mutation that would race parallel tests.
#[derive(Debug, Clone, Copy)]
pub struct HostLimits {
    /// Max retained bytes of a single stdout line before it is rejected as
    /// oversized.
    pub max_line_bytes: usize,
    /// How long [`AdapterHost::stop`] waits after SIGTERM before SIGKILL.
    pub stop_grace: Duration,
    /// How long `start` waits for the child to consume its config line.
    pub config_write_timeout: Duration,
}

impl Default for HostLimits {
    fn default() -> Self {
        Self {
            max_line_bytes: DEFAULT_MAX_LINE_BYTES,
            stop_grace: DEFAULT_STOP_GRACE,
            config_write_timeout: DEFAULT_CONFIG_WRITE_TIMEOUT,
        }
    }
}

impl HostLimits {
    /// Limits from the environment, falling back to the defaults. A malformed or
    /// unset override is ignored (the default stands); a zero byte cap is clamped
    /// to 1 so a line can always be at least attempted.
    pub fn from_env() -> Self {
        let default = Self::default();
        Self {
            max_line_bytes: env_usize(ENV_MAX_LINE_BYTES)
                .map(|n| n.max(1))
                .unwrap_or(default.max_line_bytes),
            stop_grace: env_u64(ENV_STOP_GRACE_MS)
                .map(Duration::from_millis)
                .unwrap_or(default.stop_grace),
            config_write_timeout: env_u64(ENV_CONFIG_WRITE_MS)
                .map(Duration::from_millis)
                .unwrap_or(default.config_write_timeout),
        }
    }
}

fn env_usize(key: &str) -> Option<usize> {
    std::env::var(key).ok()?.parse().ok()
}

fn env_u64(key: &str) -> Option<u64> {
    std::env::var(key).ok()?.parse().ok()
}

/// What to launch and under what identity.
///
/// Identity is part of the *spec*, not the config, precisely because it is
/// provenance the host controls (see the module docs). Subprocess-specific
/// (program + argv), so it lives on the concrete transport rather than the
/// transport-agnostic [`AdapterHost`] trait.
#[derive(Debug, Clone)]
pub struct AdapterSpec {
    program: String,
    args: Vec<String>,
    adapter_id: AdapterId,
}

impl AdapterSpec {
    /// A spec that runs `program` with no arguments, publishing under
    /// `adapter_id`.
    pub fn new(program: impl Into<String>, adapter_id: AdapterId) -> Self {
        Self {
            program: program.into(),
            args: Vec::new(),
            adapter_id,
        }
    }

    /// Append process arguments (builder style).
    pub fn with_args<I, S>(mut self, args: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.args.extend(args.into_iter().map(Into::into));
        self
    }
}

/// The child process plus its group, with a `Drop` backstop that tears the whole
/// group down if the handle is dropped without an explicit stop/wait.
///
/// The child is spawned as a process-group LEADER (its pgid == its pid), so one
/// group signal (`kill(-pid, …)`) reaches every descendant it spawned — the
/// no-orphan guarantee. `reaped` disarms the `Drop` backstop once we have
/// already waited, so a normal stop/wait does not double-signal.
#[derive(Debug)]
struct ChildProcess {
    child: Child,
    /// The child's pid, which (because of `process_group(0)`) is also its pgid.
    pid: u32,
    reaped: bool,
}

impl ChildProcess {
    fn pid(&self) -> u32 {
        self.pid
    }

    /// Signal the whole process group (`-pid`). `ESRCH` ("group already gone") is
    /// success — the goal (nothing left running) is reached.
    fn signal_group(&self, signal: Signal) -> Result<(), HostError> {
        checked_group_signal(self.pid, signal)
    }

    /// Reap the direct child, marking us reaped so the `Drop` backstop stays
    /// quiet.
    async fn wait(&mut self) -> Result<ExitStatus, HostError> {
        let status = self.child.wait().await.map_err(HostError::Wait)?;
        self.reaped = true;
        Ok(status)
    }

    /// Reap within `grace`; `Ok(None)` means the child was still alive when the
    /// grace elapsed (escalate to SIGKILL).
    async fn wait_within(&mut self, grace: Duration) -> Result<Option<ExitStatus>, HostError> {
        match timeout(grace, self.child.wait()).await {
            Ok(Ok(status)) => {
                self.reaped = true;
                Ok(Some(status))
            }
            Ok(Err(err)) => Err(HostError::Wait(err)),
            Err(_elapsed) => Ok(None),
        }
    }
}

impl Drop for ChildProcess {
    fn drop(&mut self) {
        if !self.reaped {
            // Best-effort synchronous group kill (we cannot await in Drop). The
            // direct child is reaped by tokio's kill_on_drop reaper when `child`
            // drops; grandchildren, reparented to init, are reaped by init.
            let _ = kill_group(self.pid, Signal::SIGKILL);
        }
    }
}

/// Raw process-group signal: `kill(-pid, signal)`. In its own fn so both the
/// checked path and `Drop` can call it. Targets `-pid` (the whole group led by
/// `pid`), never the bare pid.
fn kill_group(pid: u32, signal: Signal) -> nix::Result<()> {
    kill(Pid::from_raw(-(pid as i32)), signal)
}

/// Signal a process group, treating "nothing live to signal" as success — the
/// goal (nothing left running) is already reached — and normalizing any other
/// errno to an `io::Error` so no `nix` type leaks into [`HostError`].
///
/// Both `ESRCH` and `EPERM` count as success here. `ESRCH` is "no such group".
/// `EPERM` is the macOS/BSD quirk: signalling a group whose leader has become a
/// zombie (exited but not yet reaped) returns `EPERM` rather than `ESRCH`. Since
/// adapters run as the *same user* (docs/02-tech-stack.md), we can always signal
/// a genuinely live descendant, so an `EPERM` on our own child's group can only
/// mean the group is already dead — benign, and the authoritative "it is gone"
/// check is the child reap that follows.
fn checked_group_signal(pid: u32, signal: Signal) -> Result<(), HostError> {
    match kill_group(pid, signal) {
        Ok(()) | Err(Errno::ESRCH) | Err(Errno::EPERM) => Ok(()),
        Err(errno) => Err(HostError::Signal {
            pid,
            source: std::io::Error::from_raw_os_error(errno as i32),
        }),
    }
}

/// A running adapter hosted as a child process.
///
/// Holds the child (and its group) plus the two background stdio tasks. The
/// publish-forwarding task runs autonomously from the moment
/// [`start`](Self::start) returns, so events flow to the bus without the caller
/// pumping anything; the caller only decides *when* the adapter stops
/// ([`AdapterHost::stop`]) or waits for it to finish on its own
/// ([`AdapterHost::wait`]).
#[derive(Debug)]
pub struct SubprocessTransport {
    adapter_id: AdapterId,
    process: ChildProcess,
    stdout_task: JoinHandle<()>,
    stderr_task: JoinHandle<()>,
    health: Arc<HealthCounters>,
    grace: Duration,
}

impl SubprocessTransport {
    /// Spawn the adapter described by `spec`, deliver `config`, and begin
    /// forwarding its publishes onto `bus`.
    ///
    /// Returns once the child is spawned and its config line delivered; the
    /// stdout forwarding and stderr logging run in the background from here.
    /// Fails with [`HostError`] only for start-time faults (spawn, missing
    /// pipe/pid, config delivery/timeout); a bad *line* later is a health event,
    /// not a start failure.
    pub async fn start(
        spec: AdapterSpec,
        config: AdapterConfig,
        bus: Bus,
    ) -> Result<Self, HostError> {
        Self::start_inner(spec, config, bus, HostLimits::from_env(), None, None).await
    }

    /// Like [`start`](Self::start), but with explicit [`HostLimits`] rather than
    /// the env-derived defaults. This is the seam a supervisor or a test uses to
    /// set the stdout cap / stop grace / config-write timeout directly.
    pub async fn start_with_limits(
        spec: AdapterSpec,
        config: AdapterConfig,
        bus: Bus,
        limits: HostLimits,
    ) -> Result<Self, HostError> {
        Self::start_inner(spec, config, bus, limits, None, None).await
    }

    /// Like [`start`](Self::start), but also relays the adapter's
    /// [`mailbox_protocol::Baseline`] lines to `baseline_sink` (design/01 / card
    /// 10: baseline-via-protocol) and CONSTRAINS the adapter to publish only on
    /// `expected_topic` — a `Publish` to any other topic is rejected, not
    /// forwarded (provenance: an adapter must not inject events onto another
    /// entity's topic — review item C). The supervisor passes the watch's own
    /// topic here; `start`/`start_with_limits` pass no sink and no topic, so a
    /// hand-run or a happy-path test is unconstrained.
    pub async fn start_with_baseline(
        spec: AdapterSpec,
        config: AdapterConfig,
        bus: Bus,
        baseline_sink: Arc<dyn BaselineSink>,
        expected_topic: Topic,
    ) -> Result<Self, HostError> {
        Self::start_inner(
            spec,
            config,
            bus,
            HostLimits::from_env(),
            Some(baseline_sink),
            Some(expected_topic),
        )
        .await
    }

    /// The shared start path behind [`start`](Self::start),
    /// [`start_with_limits`](Self::start_with_limits), and
    /// [`start_with_baseline`](Self::start_with_baseline). The optional
    /// `baseline_sink` is threaded into the stdout-forwarding task so a `Baseline`
    /// line is relayed to storage the same way a `Publish` line is relayed to the
    /// bus — the transport itself stays storage-free. `expected_topic`, when set,
    /// binds the adapter to a single entity topic (see
    /// [`start_with_baseline`](Self::start_with_baseline)).
    async fn start_inner(
        spec: AdapterSpec,
        config: AdapterConfig,
        bus: Bus,
        limits: HostLimits,
        baseline_sink: Option<Arc<dyn BaselineSink>>,
        expected_topic: Option<Topic>,
    ) -> Result<Self, HostError> {
        // Serialize the opaque config to ONE line before we spawn, so a bad
        // config value fails fast without leaving a child running.
        let config_line = serde_json::to_string(config.value()).map_err(HostError::ConfigEncode)?;

        let mut child = Command::new(&spec.program)
            .args(&spec.args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            // New process group (leader pid == pgid) so a single group signal
            // tears down the adapter AND any grandchildren it spawns (no orphans),
            // and so `kill(-pid)` can never reach the host's own group.
            .process_group(0)
            // Backstop if the handle is dropped without stop/wait (see
            // ChildProcess::drop): tokio reaps the direct child.
            .kill_on_drop(true)
            .spawn()
            .map_err(|source| HostError::Spawn {
                program: spec.program.clone(),
                source,
            })?;

        // Resolve the pid ONCE (a just-spawned child always has one); a missing
        // pid is a start failure, not a dead state threaded through the lifecycle.
        let pid = child.id().ok_or(HostError::MissingPid)?;

        let mut stdin = child
            .stdin
            .take()
            .ok_or(HostError::MissingStdio { stream: "stdin" })?;
        let stdout = child
            .stdout
            .take()
            .ok_or(HostError::MissingStdio { stream: "stdout" })?;
        let stderr = child
            .stderr
            .take()
            .ok_or(HostError::MissingStdio { stream: "stderr" })?;

        let mut process = ChildProcess {
            child,
            pid,
            reaped: false,
        };

        let health = Arc::new(HealthCounters::default());
        let max_line = limits.max_line_bytes;

        info!(
            adapter = spec.adapter_id.0.as_str(),
            pid,
            program = spec.program.as_str(),
            "spawned adapter subprocess"
        );

        // Spawn the readers BEFORE writing config so the child can drain its own
        // stdout while we deliver config — avoids a stdin-write-vs-stdout-flood
        // deadlock (item B).
        let stdout_task = tokio::spawn(forward_stdout(
            stdout,
            bus,
            spec.adapter_id.clone(),
            max_line,
            Arc::clone(&health),
            baseline_sink,
            expected_topic,
        ));
        let stderr_task = tokio::spawn(log_stderr(stderr, spec.adapter_id.clone(), pid, max_line));

        // Deliver config on stdin under a bounded timeout, then close it (EOF).
        // `async move` owns stdin so it is dropped (pipe closed) at the end of the
        // block regardless of which write step completed.
        let write_result = timeout(limits.config_write_timeout, async move {
            stdin.write_all(config_line.as_bytes()).await?;
            stdin.write_all(b"\n").await?;
            stdin.shutdown().await
        })
        .await;

        match write_result {
            Ok(Ok(())) => {}
            Ok(Err(source)) => {
                teardown_failed_start(&mut process, stdout_task, stderr_task).await;
                return Err(HostError::ConfigDelivery(source));
            }
            Err(_elapsed) => {
                teardown_failed_start(&mut process, stdout_task, stderr_task).await;
                return Err(HostError::ConfigWriteTimeout(limits.config_write_timeout));
            }
        }

        Ok(Self {
            adapter_id: spec.adapter_id,
            process,
            stdout_task,
            stderr_task,
            health,
            grace: limits.stop_grace,
        })
    }
}

impl SubprocessTransport {
    /// A lightweight, owned handle that can signal this adapter's whole process
    /// group without owning the transport.
    ///
    /// # Why this exists (the supervisor's crash-watch, card 08)
    ///
    /// [`AdapterHost::stop`] and [`AdapterHost::wait`] both consume `self`, so a
    /// task that owns the transport via `wait()` to detect a crash cannot also
    /// call `stop()` to tear it down on command. Capturing a [`Terminator`]
    /// *before* moving the transport into a `wait()` future gives that task a way
    /// to drive a graceful group SIGTERM→SIGKILL while the same `wait()` future it
    /// is holding reaps the child and drains the pipes. The terminator only knows
    /// the pgid, so it cannot touch anything but this adapter's own group.
    pub fn terminator(&self) -> Terminator {
        Terminator {
            pid: self.process.pid(),
        }
    }
}

/// Signals an adapter's process group on behalf of a supervisor that owns the
/// transport elsewhere (via a pending `wait()`), keeping the "signal the whole
/// group, never the bare pid" no-orphan discipline inside this module. See
/// [`SubprocessTransport::terminator`].
#[derive(Debug, Clone, Copy)]
pub struct Terminator {
    /// The adapter's pid, which (because it was spawned `process_group(0)`) is
    /// also its pgid — so `kill(-pid, …)` reaches the whole group.
    pid: u32,
}

impl Terminator {
    /// SIGTERM the whole group (graceful request to shut down).
    pub fn terminate(&self) -> Result<(), HostError> {
        checked_group_signal(self.pid, Signal::SIGTERM)
    }

    /// SIGKILL the whole group (forceful, after a grace period).
    pub fn kill(&self) -> Result<(), HostError> {
        checked_group_signal(self.pid, Signal::SIGKILL)
    }
}

impl AdapterHost for SubprocessTransport {
    type Error = HostError;

    fn adapter_id(&self) -> &AdapterId {
        &self.adapter_id
    }

    fn pid(&self) -> Option<u32> {
        Some(self.process.pid())
    }

    fn health(&self) -> AdapterHealth {
        self.health.snapshot()
    }

    async fn stop(self) -> Result<AdapterExit, HostError> {
        let SubprocessTransport {
            adapter_id,
            mut process,
            stdout_task,
            stderr_task,
            grace,
            health: _,
        } = self;
        let pid = process.pid();

        // SIGTERM the whole group first: ask the adapter (and any descendant) to
        // shut down gracefully.
        process.signal_group(Signal::SIGTERM)?;
        info!(
            adapter = adapter_id.0.as_str(),
            pid,
            grace_ms = grace.as_millis() as u64,
            "sent SIGTERM to adapter process group; awaiting graceful exit"
        );

        let status = match process.wait_within(grace).await? {
            // Exited within grace: SIGTERM (or the adapter's own handler) sufficed.
            Some(status) => status,
            None => {
                // Still alive after the grace: escalate to a group SIGKILL, then reap.
                warn!(
                    adapter = adapter_id.0.as_str(),
                    pid,
                    grace_ms = grace.as_millis() as u64,
                    "grace elapsed; sent SIGKILL to adapter process group"
                );
                process.signal_group(Signal::SIGKILL)?;
                process.wait().await?
            }
        };

        // Direct child reaped; drain the readers under a bound then abort, so a
        // descendant that inherited a pipe can never wedge stop().
        finish_io_tasks(stdout_task, stderr_task, &adapter_id).await;

        let exit = exit_from_status(status);
        info!(
            adapter = adapter_id.0.as_str(),
            pid,
            ?exit,
            "adapter stopped"
        );
        Ok(exit)
    }

    async fn wait(self) -> Result<AdapterExit, HostError> {
        let SubprocessTransport {
            adapter_id,
            mut process,
            stdout_task,
            stderr_task,
            health: _,
            grace: _,
        } = self;
        let pid = process.pid();

        // Wait for the child to exit on its own, THEN drain stdout under a bound.
        // Draining AFTER the exit (rather than before) keeps wait() hang-safe if a
        // grandchild still holds the stdout pipe open; buffered publishes remain
        // readable post-exit, so nothing is lost for the common finite adapter.
        let status = process.wait().await?;
        finish_io_tasks(stdout_task, stderr_task, &adapter_id).await;

        let exit = exit_from_status(status);
        info!(
            adapter = adapter_id.0.as_str(),
            pid,
            ?exit,
            "adapter exited on its own"
        );
        Ok(exit)
    }
}

/// Tear down a child whose `start` failed after spawn: kill the whole group,
/// reap the direct child, and abort the readers so nothing is left running.
async fn teardown_failed_start(
    process: &mut ChildProcess,
    stdout_task: JoinHandle<()>,
    stderr_task: JoinHandle<()>,
) {
    let _ = process.signal_group(Signal::SIGKILL);
    let _ = process.wait().await;
    stdout_task.abort();
    stderr_task.abort();
    let _ = stdout_task.await;
    let _ = stderr_task.await;
}

/// Drain both stdio tasks concurrently under [`IO_DRAIN_GRACE`], aborting any
/// that does not finish (a descendant may still hold the pipe open).
async fn finish_io_tasks(
    stdout_task: JoinHandle<()>,
    stderr_task: JoinHandle<()>,
    adapter_id: &AdapterId,
) {
    tokio::join!(
        drain_or_abort(stdout_task, "stdout", adapter_id),
        drain_or_abort(stderr_task, "stderr", adapter_id),
    );
}

/// Await one stdio task up to [`IO_DRAIN_GRACE`]; if it has not finished (its
/// pipe is still held open by a surviving descendant), abort it so the caller
/// cannot wedge. A cancelled join is expected on the abort path and not logged
/// as an error.
async fn drain_or_abort(task: JoinHandle<()>, stream: &'static str, adapter_id: &AdapterId) {
    let abort = task.abort_handle();
    match timeout(IO_DRAIN_GRACE, task).await {
        Ok(Ok(())) => {}
        Ok(Err(join_err)) if join_err.is_cancelled() => {}
        Ok(Err(join_err)) => warn!(
            adapter = adapter_id.0.as_str(),
            stream,
            error = %join_err,
            "adapter io task did not join cleanly"
        ),
        Err(_elapsed) => {
            warn!(
                adapter = adapter_id.0.as_str(),
                stream,
                grace_ms = IO_DRAIN_GRACE.as_millis() as u64,
                "adapter io task did not finish within the drain grace; aborting \
                 (a descendant may still hold the pipe open)"
            );
            abort.abort();
        }
    }
}

/// Classify a finished child's [`ExitStatus`] into the transport-agnostic
/// [`AdapterExit`]. Lives here (not on the boundary type) so all OS-process
/// detail stays inside the subprocess transport. On unix a signal death yields
/// no code and vice versa, so the order is unambiguous.
fn exit_from_status(status: ExitStatus) -> AdapterExit {
    if let Some(code) = status.code() {
        AdapterExit::Exited { code }
    } else if let Some(signal) = status.signal() {
        AdapterExit::Signalled { signal }
    } else {
        AdapterExit::Unknown
    }
}

/// The stdout forwarding loop: decode each `Publish` line and push it onto the
/// bus under the host-stamped identity. Malformed/oversized/non-`Publish` lines
/// are counted and skipped, never fatal.
async fn forward_stdout(
    stdout: ChildStdout,
    bus: Bus,
    adapter_id: AdapterId,
    max_line: usize,
    health: Arc<HealthCounters>,
    baseline_sink: Option<Arc<dyn BaselineSink>>,
    expected_topic: Option<Topic>,
) {
    let mut reader = BufReader::new(stdout);
    let mut line_number = 0usize;

    loop {
        match read_frame(&mut reader, max_line).await {
            Ok(Frame::Eof) => break,
            Ok(Frame::Oversized) => {
                line_number += 1;
                health.record_rejected();
                warn!(
                    adapter = adapter_id.0.as_str(),
                    line = line_number,
                    cap_bytes = max_line,
                    "rejected oversized adapter line (exceeded byte cap); skipping"
                );
            }
            Ok(Frame::Line(bytes)) => {
                line_number += 1;
                handle_line(
                    &bytes,
                    line_number,
                    &bus,
                    &adapter_id,
                    &health,
                    baseline_sink.as_ref(),
                    expected_topic.as_ref(),
                )
                .await;
            }
            Err(err) => {
                // A read error on the pipe ends forwarding; the child is likely
                // gone. Not fatal to the bridge.
                warn!(
                    adapter = adapter_id.0.as_str(),
                    error = %err,
                    "error reading adapter stdout; ending forward"
                );
                break;
            }
        }
    }

    let snapshot = health.snapshot();
    info!(
        adapter = adapter_id.0.as_str(),
        forwarded = snapshot.forwarded,
        rejected = snapshot.rejected,
        publish_failures = snapshot.publish_failures,
        "adapter stdout closed; forwarding finished"
    );
}

/// Decode and forward one non-empty physical line.
async fn handle_line(
    bytes: &[u8],
    line_number: usize,
    bus: &Bus,
    adapter_id: &AdapterId,
    health: &HealthCounters,
    baseline_sink: Option<&Arc<dyn BaselineSink>>,
    expected_topic: Option<&Topic>,
) {
    // A blank line is legal NDJSON padding — skip it silently, uncounted.
    let text = match std::str::from_utf8(bytes) {
        Ok(text) if text.trim().is_empty() => return,
        Ok(text) => text,
        Err(_) => {
            health.record_rejected();
            warn!(
                adapter = adapter_id.0.as_str(),
                line = line_number,
                "rejected adapter line that was not valid UTF-8; skipping"
            );
            return;
        }
    };

    match decode_line(text) {
        Ok(Message::Publish(publish)) => {
            // Provenance (review item C): a transport bound to one entity topic
            // rejects a Publish to any OTHER topic — an adapter must not inject
            // events onto another entity's topic. Host-enforced, exactly like the
            // adapter_id is host-stamped rather than adapter-asserted.
            if let Some(expected) = expected_topic
                && &publish.topic != expected
            {
                health.record_rejected();
                warn!(
                    adapter = adapter_id.0.as_str(),
                    line = line_number,
                    expected = expected.as_str(),
                    got = publish.topic.as_str(),
                    "rejected adapter publish to a foreign topic; skipping"
                );
                return;
            }
            let topic = publish.topic.clone();
            // Stamp OUR identity (provenance), ignoring publish.adapter. Stamp the
            // timestamp here, like the durable bridge does (one clock).
            match bus
                .publish(
                    publish.topic,
                    adapter_id.clone(),
                    Timestamp(crate::clock::now_millis()),
                    publish.body,
                )
                .await
            {
                Ok(event) => {
                    health.record_forwarded();
                    debug!(
                        adapter = adapter_id.0.as_str(),
                        topic = topic.as_str(),
                        offset = event.offset.0,
                        "forwarded adapter publish onto the bus"
                    );
                }
                Err(err) => {
                    // The event decoded fine; the bus append failed (store down).
                    // The adapter is not at fault — separate counter.
                    health.record_publish_failure();
                    warn!(
                        adapter = adapter_id.0.as_str(),
                        topic = topic.as_str(),
                        error = %err,
                        "bus publish failed for adapter event; skipping"
                    );
                }
            }
        }
        Ok(Message::Baseline(baseline)) => {
            // Baseline-via-protocol (design/01 / card 10): relay the opaque
            // snapshot to the bound sink (storage) rather than the bus, mirroring
            // how a Publish is forwarded. Without a sink (a hand-run or a
            // happy-path test) the line is a harmless no-op. Awaited inline so the
            // persist ordering matches the stdout line order.
            match baseline_sink {
                Some(sink) => {
                    sink.persist(baseline.value).await;
                    debug!(
                        adapter = adapter_id.0.as_str(),
                        line = line_number,
                        "relayed adapter baseline snapshot to the persist sink"
                    );
                }
                None => debug!(
                    adapter = adapter_id.0.as_str(),
                    line = line_number,
                    "adapter emitted a baseline but no persist sink is bound; ignoring"
                ),
            }
        }
        Ok(_other) => {
            // A valid protocol message, but not a Publish or Baseline. Adapters
            // publish (and, if edge-triggered, emit baselines); a subscribe/read/
            // etc. from a child is misuse. Skip and count.
            health.record_rejected();
            warn!(
                adapter = adapter_id.0.as_str(),
                line = line_number,
                "adapter sent an unexpected protocol message; skipping"
            );
        }
        Err(source) => {
            // Tag with the line number via LineError so the log matches what a
            // human sees in the raw output (card-02 diagnostics).
            let error = LineError {
                line: line_number,
                source,
            };
            health.record_rejected();
            warn!(
                adapter = adapter_id.0.as_str(),
                line = line_number,
                error = %error,
                "rejected malformed adapter line; skipping"
            );
        }
    }
}

/// Drain the child's stderr into `tracing`, one line at a time. These are
/// adapter diagnostics — fine to log, and never assumed to be protocol. Read
/// with the same byte cap so a runaway stderr cannot OOM the host, and rate
/// limited so a flood cannot DoS log volume: the first [`STDERR_INFO_LIMIT`]
/// lines are at `info`, the rest demoted to `debug` and periodically summarized.
async fn log_stderr(stderr: ChildStderr, adapter_id: AdapterId, pid: u32, max_line: usize) {
    let mut reader = BufReader::new(stderr);
    let mut logged = 0u64;
    let mut suppressed = 0u64;

    loop {
        match read_frame(&mut reader, max_line).await {
            Ok(Frame::Eof) => break,
            Ok(Frame::Oversized) => {
                warn!(
                    adapter = adapter_id.0.as_str(),
                    pid, "adapter emitted an oversized stderr line; truncated"
                );
            }
            Ok(Frame::Line(bytes)) => {
                let line = String::from_utf8_lossy(&bytes);
                if line.trim().is_empty() {
                    continue;
                }
                if logged < STDERR_INFO_LIMIT {
                    logged += 1;
                    info!(adapter = adapter_id.0.as_str(), pid, line = %line, "adapter stderr");
                    if logged == STDERR_INFO_LIMIT {
                        warn!(
                            adapter = adapter_id.0.as_str(),
                            pid,
                            limit = STDERR_INFO_LIMIT,
                            "adapter stderr hit the per-instance info-log limit; \
                             further lines demoted to debug and counted"
                        );
                    }
                } else {
                    suppressed += 1;
                    debug!(adapter = adapter_id.0.as_str(), pid, line = %line, "adapter stderr (demoted)");
                    if suppressed.is_multiple_of(STDERR_SUPPRESS_REPORT_EVERY) {
                        info!(
                            adapter = adapter_id.0.as_str(),
                            pid,
                            suppressed,
                            "adapter stderr still flooding; lines demoted to debug"
                        );
                    }
                }
            }
            Err(err) => {
                warn!(adapter = adapter_id.0.as_str(), pid, error = %err, "error reading adapter stderr");
                break;
            }
        }
    }

    if suppressed > 0 {
        info!(
            adapter = adapter_id.0.as_str(),
            pid, suppressed, "adapter stderr closed (suppressed lines beyond the info limit)"
        );
    } else {
        debug!(
            adapter = adapter_id.0.as_str(),
            pid, "adapter stderr closed"
        );
    }
}

/// One physical line read from a child stream, bounded by a byte cap.
#[derive(Debug)]
enum Frame {
    /// A complete line (trailing `\n`/`\r\n` stripped). May be empty.
    Line(Vec<u8>),
    /// The line exceeded the byte cap. Its bytes past the cap were discarded and
    /// the stream consumed to the next newline so it resyncs — never buffered in
    /// full, so a newline-less flood cannot grow host memory.
    Oversized,
    /// End of stream.
    Eof,
}

/// Read one newline-terminated frame from `reader`, buffering at most `max`
/// bytes of content. Mirrors the card-06 daemon's bounded-frame discipline, but
/// per line for a continuous stream: we scan the buffered chunks for a newline
/// and stop retaining bytes once the line's length passes `max`, while still
/// consuming through the newline so the next line reads cleanly.
async fn read_frame<R: AsyncBufRead + Unpin>(reader: &mut R, max: usize) -> std::io::Result<Frame> {
    let mut buf = Vec::new();
    // Total content bytes seen for this line, INCLUDING any past the cap that we
    // deliberately did not retain. This — not `buf.len()` — decides oversize.
    let mut seen = 0usize;
    let mut oversized = false;

    loop {
        // Consume exactly the bytes we inspected this iteration; compute the
        // decision while the borrow of `reader` is live, then release it before
        // calling `consume`.
        let (consumed, found_newline) = {
            let chunk = reader.fill_buf().await?;
            if chunk.is_empty() {
                // EOF. An empty buffer with nothing seen is a clean end; a
                // partial final line (no trailing newline) is still a line.
                if seen == 0 {
                    return Ok(Frame::Eof);
                }
                return Ok(if oversized {
                    Frame::Oversized
                } else {
                    Frame::Line(strip_cr(buf))
                });
            }

            match chunk.iter().position(|&b| b == b'\n') {
                Some(pos) => {
                    retain_capped(&mut buf, &chunk[..pos], seen, max, oversized);
                    seen += pos;
                    if seen > max {
                        oversized = true;
                    }
                    (pos + 1, true)
                }
                None => {
                    retain_capped(&mut buf, chunk, seen, max, oversized);
                    seen += chunk.len();
                    if seen > max {
                        oversized = true;
                    }
                    (chunk.len(), false)
                }
            }
        };
        reader.consume(consumed);

        if found_newline {
            return Ok(if oversized {
                Frame::Oversized
            } else {
                Frame::Line(strip_cr(buf))
            });
        }
    }
}

/// Append `chunk` to `buf` only while we are under the cap, so `buf` never holds
/// more than ~`max` bytes even for an over-cap line.
fn retain_capped(buf: &mut Vec<u8>, chunk: &[u8], seen: usize, max: usize, oversized: bool) {
    if oversized {
        return;
    }
    let room = max.saturating_sub(seen);
    if room == 0 {
        return;
    }
    let take = chunk.len().min(room);
    buf.extend_from_slice(&chunk[..take]);
}

/// Strip a trailing carriage return so a `\r\n`-authored line decodes like a
/// `\n` one (adapters may be written on Windows). `decode_line` also tolerates a
/// trailing `\r`, but stripping here keeps the stderr path clean too.
fn strip_cr(mut buf: Vec<u8>) -> Vec<u8> {
    if buf.last() == Some(&b'\r') {
        buf.pop();
    }
    buf
}

#[cfg(test)]
mod tests {
    use super::*;

    use serde_json::json;
    use tempfile::TempDir;

    use mailbox_protocol::{GithubPr, Publish, Subscribe, Topic, encode_line};

    use crate::storage::{Storage, StorageConfig};

    /// A short line under the cap round-trips intact, newline stripped.
    #[tokio::test]
    async fn read_frame_returns_a_short_line() {
        let data = b"hello world\nsecond\n";
        let mut reader = BufReader::new(&data[..]);
        let frame = read_frame(&mut reader, 1024).await.unwrap();
        assert!(matches!(frame, Frame::Line(b) if b == b"hello world"));
    }

    /// CRLF is normalized to the bare line.
    #[tokio::test]
    async fn read_frame_strips_crlf() {
        let data = b"line\r\n";
        let mut reader = BufReader::new(&data[..]);
        let frame = read_frame(&mut reader, 1024).await.unwrap();
        assert!(matches!(frame, Frame::Line(b) if b == b"line"));
    }

    /// A line longer than the cap is reported Oversized, and the NEXT line still
    /// reads cleanly — the stream resynced past the offending newline.
    #[tokio::test]
    async fn read_frame_flags_oversized_then_resyncs() {
        let mut data = vec![b'a'; 50];
        data.push(b'\n');
        data.extend_from_slice(b"ok\n");
        let mut reader = BufReader::new(&data[..]);

        let first = read_frame(&mut reader, 8).await.unwrap();
        assert!(matches!(first, Frame::Oversized), "50-byte line over cap 8");

        let second = read_frame(&mut reader, 8).await.unwrap();
        assert!(
            matches!(second, Frame::Line(b) if b == b"ok"),
            "the stream must resync to the next line after an oversized one"
        );
    }

    /// The retained buffer for a hugely oversized line never exceeds the cap —
    /// the invariant a "full-line buffering" regression would violate.
    #[tokio::test]
    async fn read_frame_never_retains_more_than_cap() {
        // 1 MiB of a single newline-less line, cap 64 bytes.
        let mut data = vec![b'x'; 1024 * 1024];
        data.push(b'\n');
        let mut reader = BufReader::new(&data[..]);
        // We can't observe buf directly, but Oversized proves the seen>max path;
        // combined with `retain_capped` capping at `max`, memory stays bounded.
        assert!(matches!(
            read_frame(&mut reader, 64).await.unwrap(),
            Frame::Oversized
        ));
        assert!(matches!(
            read_frame(&mut reader, 64).await.unwrap(),
            Frame::Eof
        ));
    }

    /// A line exactly at the cap is accepted (boundary is inclusive of `max`).
    #[tokio::test]
    async fn read_frame_accepts_line_exactly_at_cap() {
        let data = b"12345678\n";
        let mut reader = BufReader::new(&data[..]);
        let frame = read_frame(&mut reader, 8).await.unwrap();
        assert!(matches!(frame, Frame::Line(b) if b == b"12345678"));
    }

    /// An unterminated final line (no trailing newline) is still surfaced.
    #[tokio::test]
    async fn read_frame_surfaces_unterminated_final_line() {
        let data = b"tail";
        let mut reader = BufReader::new(&data[..]);
        let frame = read_frame(&mut reader, 1024).await.unwrap();
        assert!(matches!(frame, Frame::Line(b) if b == b"tail"));
        // And then EOF.
        assert!(matches!(
            read_frame(&mut reader, 1024).await.unwrap(),
            Frame::Eof
        ));
    }

    #[test]
    fn exit_from_status_classifies_code_and_signal() {
        assert_eq!(
            exit_from_status(std::process::ExitStatus::from_raw(0)),
            AdapterExit::Exited { code: 0 }
        );
        // Raw wait status 9 == killed by signal 9 (SIGKILL), no exit code.
        assert_eq!(
            exit_from_status(std::process::ExitStatus::from_raw(9)),
            AdapterExit::Signalled { signal: 9 }
        );
    }

    /// A group signal to a pid with no process is `ESRCH`, which the transport
    /// treats as success — the goal (nothing running) is already met.
    #[test]
    fn group_signal_treats_no_such_process_as_success() {
        // A very high pid that cannot exist on our targets (Linux pid_max is
        // 2^22, macOS far lower), so the group signal yields ESRCH.
        assert!(checked_group_signal((i32::MAX - 1) as u32, Signal::SIGTERM).is_ok());
    }

    fn gh_topic() -> Topic {
        GithubPr::new("octocat", "hello-world", 1).unwrap().topic()
    }

    /// A healthy bus over a fresh store in a tempdir, plus the DB path (for the
    /// publish-failure poison) and the tempdir (kept alive).
    async fn healthy_bus() -> (Bus, std::path::PathBuf, TempDir) {
        let dir = TempDir::new().expect("tempdir");
        let path = dir.path().join("mailbox.db");
        let storage = Storage::open(StorageConfig::at(&path))
            .await
            .expect("open storage");
        (Bus::new(storage), path, dir)
    }

    /// Poison a topic so the next real publish overflows its offset computation
    /// (`MAX(offset)+1`), forcing `Bus::publish` to return `Err` through the real
    /// path — no mocks.
    fn poison_topic_offset_overflow(path: &std::path::Path, topic: &str) {
        let conn = rusqlite::Connection::open(path).unwrap();
        conn.execute(
            "INSERT INTO event (topic, offset, event_id, adapter, timestamp, body)
             VALUES (?1, ?2, ?3, 'poison', 0, '{}')",
            rusqlite::params![topic, i64::MAX, format!("poison-{topic}")],
        )
        .unwrap();
    }

    /// A non-UTF-8 line is rejected (counted, skipped) and never forwarded.
    #[tokio::test]
    async fn handle_line_rejects_non_utf8() {
        let (bus, _path, _dir) = healthy_bus().await;
        let health = HealthCounters::default();
        let adapter = AdapterId("t".to_string());

        handle_line(&[0xff, 0xfe, 0x00], 1, &bus, &adapter, &health, None, None).await;

        let snap = health.snapshot();
        assert_eq!(snap.rejected, 1);
        assert_eq!(snap.forwarded, 0);
        assert_eq!(snap.publish_failures, 0);
    }

    /// A well-formed but non-`Publish` message (a `Subscribe`) is rejected, not
    /// forwarded — adapters publish; anything else is misuse.
    #[tokio::test]
    async fn handle_line_rejects_non_publish_message() {
        let (bus, _path, _dir) = healthy_bus().await;
        let health = HealthCounters::default();
        let adapter = AdapterId("t".to_string());

        let line = encode_line(&Message::Subscribe(Subscribe { topic: gh_topic() })).unwrap();
        handle_line(line.as_bytes(), 1, &bus, &adapter, &health, None, None).await;

        let snap = health.snapshot();
        assert_eq!(snap.rejected, 1);
        assert_eq!(snap.forwarded, 0);
    }

    /// A `Baseline` line is relayed to the bound sink (not the bus) and is not
    /// counted as a reject — an edge-triggered adapter's baseline is expected
    /// output, not misuse.
    #[tokio::test]
    async fn handle_line_relays_baseline_to_sink() {
        use std::sync::Mutex;

        #[derive(Default)]
        struct RecordingSink {
            seen: Arc<Mutex<Vec<serde_json::Value>>>,
        }
        impl BaselineSink for RecordingSink {
            fn persist(
                &self,
                value: serde_json::Value,
            ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'static>>
            {
                let seen = Arc::clone(&self.seen);
                Box::pin(async move { seen.lock().unwrap().push(value) })
            }
        }

        let (bus, _path, _dir) = healthy_bus().await;
        let health = HealthCounters::default();
        let adapter = AdapterId("t".to_string());
        let seen = Arc::new(Mutex::new(Vec::new()));
        let sink: Arc<dyn BaselineSink> = Arc::new(RecordingSink {
            seen: Arc::clone(&seen),
        });

        let line = encode_line(&Message::Baseline(mailbox_protocol::Baseline {
            value: json!({ "mergeable": "conflicting" }),
        }))
        .unwrap();
        handle_line(
            line.as_bytes(),
            1,
            &bus,
            &adapter,
            &health,
            Some(&sink),
            None,
        )
        .await;

        let snap = health.snapshot();
        assert_eq!(snap.rejected, 0, "a baseline is not a reject");
        assert_eq!(snap.forwarded, 0, "a baseline is not a bus publish");
        assert_eq!(
            *seen.lock().unwrap(),
            vec![json!({ "mergeable": "conflicting" })],
            "the baseline snapshot was relayed to the sink verbatim"
        );
    }

    /// A `Baseline` with no sink bound is a harmless no-op — not a reject.
    #[tokio::test]
    async fn handle_line_ignores_baseline_without_sink() {
        let (bus, _path, _dir) = healthy_bus().await;
        let health = HealthCounters::default();
        let adapter = AdapterId("t".to_string());

        let line = encode_line(&Message::Baseline(mailbox_protocol::Baseline {
            value: json!({ "x": 1 }),
        }))
        .unwrap();
        handle_line(line.as_bytes(), 1, &bus, &adapter, &health, None, None).await;

        let snap = health.snapshot();
        assert_eq!(snap.rejected, 0);
        assert_eq!(snap.forwarded, 0);
    }

    /// A valid `Publish` is forwarded and counted.
    #[tokio::test]
    async fn handle_line_forwards_a_publish() {
        let (bus, _path, _dir) = healthy_bus().await;
        let health = HealthCounters::default();
        let adapter = AdapterId("host-id".to_string());

        let line = encode_line(&Message::Publish(Publish {
            topic: gh_topic(),
            adapter: AdapterId("self-reported".to_string()),
            body: json!({ "hello": "world" }),
        }))
        .unwrap();
        handle_line(line.as_bytes(), 1, &bus, &adapter, &health, None, None).await;

        let snap = health.snapshot();
        assert_eq!(snap.forwarded, 1);
        assert_eq!(snap.rejected, 0);
        assert_eq!(snap.publish_failures, 0);
    }

    /// When the bus publish itself fails, it is counted as a publish failure
    /// (not a reject) and does not panic — the fault is the bridge's.
    #[tokio::test]
    async fn handle_line_counts_publish_failure_without_crashing() {
        let (bus, path, _dir) = healthy_bus().await;
        let health = HealthCounters::default();
        let adapter = AdapterId("host-id".to_string());
        let topic = gh_topic();
        poison_topic_offset_overflow(&path, topic.as_str());

        let line = encode_line(&Message::Publish(Publish {
            topic,
            adapter: AdapterId("self-reported".to_string()),
            body: json!({}),
        }))
        .unwrap();
        handle_line(line.as_bytes(), 1, &bus, &adapter, &health, None, None).await;

        let snap = health.snapshot();
        assert_eq!(snap.publish_failures, 1, "the bus append failed");
        assert_eq!(snap.forwarded, 0);
        assert_eq!(snap.rejected, 0, "a bus failure is not an adapter reject");
    }

    /// Review item C: when the transport is bound to an entity topic, a `Publish`
    /// to a DIFFERENT topic is rejected (counted, not forwarded) — an adapter must
    /// not inject events onto another entity's topic.
    #[tokio::test]
    async fn handle_line_rejects_a_foreign_topic_publish() {
        let (bus, _path, _dir) = healthy_bus().await;
        let health = HealthCounters::default();
        let adapter = AdapterId("host-id".to_string());
        let expected = gh_topic();
        let foreign = GithubPr::new("victim", "repo", 99).unwrap().topic();

        let line = encode_line(&Message::Publish(Publish {
            topic: foreign,
            adapter: AdapterId("self-reported".to_string()),
            body: json!({ "edge": "mergeable_conflicting" }),
        }))
        .unwrap();
        handle_line(
            line.as_bytes(),
            1,
            &bus,
            &adapter,
            &health,
            None,
            Some(&expected),
        )
        .await;

        let snap = health.snapshot();
        assert_eq!(snap.rejected, 1, "a foreign-topic publish is rejected");
        assert_eq!(snap.forwarded, 0, "it is never appended to the other topic");
    }

    /// A `Publish` to the BOUND entity topic is forwarded as normal.
    #[tokio::test]
    async fn handle_line_forwards_a_publish_on_the_bound_topic() {
        let (bus, _path, _dir) = healthy_bus().await;
        let health = HealthCounters::default();
        let adapter = AdapterId("host-id".to_string());
        let expected = gh_topic();

        let line = encode_line(&Message::Publish(Publish {
            topic: expected.clone(),
            adapter: AdapterId("self-reported".to_string()),
            body: json!({ "edge": "new_reviews" }),
        }))
        .unwrap();
        handle_line(
            line.as_bytes(),
            1,
            &bus,
            &adapter,
            &health,
            None,
            Some(&expected),
        )
        .await;

        let snap = health.snapshot();
        assert_eq!(
            snap.forwarded, 1,
            "a publish on the bound topic is forwarded"
        );
        assert_eq!(snap.rejected, 0);
    }
}
