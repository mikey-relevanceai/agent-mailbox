//! The `serve` daemon: the one long-lived process that owns the writer + waker.
//!
//! # Daemon model (ADR-0004)
//!
//! A single `mailbox serve` process opens the [`Storage`] single writer and the
//! [`Waker`], and binds a user-scoped Unix socket derived from the resolved
//! storage path. Every other command is a one-shot socket client
//! ([`crate::client`]); when the daemon is down they fail loudly rather than
//! opening the DB themselves. That keeps single-writer *structural*.
//!
//! # Cross-process single-writer (ADR-0003/0004)
//!
//! Before opening the writer, the daemon takes an exclusive advisory `flock` on
//! `<db-dir>/mailbox.lock` and holds it for its whole life. That is the real
//! mutual exclusion: two `serve` processes on one DB cannot both become writers
//! (the second fails loudly on the lock). Because we hold the lock, a leftover
//! socket node is provably stale and safe to remove — we never unlink a socket
//! we have not proven dead.
//!
//! # Daemon hardening (adversarial review)
//!
//! - **Bounded frames.** A request line is read with a hard byte cap
//!   ([`Limits::max_frame_bytes`]); an over-cap or newline-less frame is rejected
//!   with a `Response::Error`, never buffered unbounded (no OOM on a huge line).
//! - **Read timeout.** A slow/half-open client is dropped after the configured
//!   read timeout, so idle connections cannot pin tasks/fds forever.
//! - **Connection cap.** A [`Semaphore`] bounds concurrent handlers; over the
//!   cap, the connection is dropped rather than spawning unboundedly.
//! - **Accept backoff.** Repeated `accept()` errors (e.g. EMFILE) back off
//!   ([`ACCEPT_BACKOFF`]) instead of tight-looping at 100% CPU.
//! - **Owner-only socket.** The DB directory is created `0700` and the socket
//!   bound `0600` *before* it can accept; a hardening failure is FATAL (we refuse
//!   to serve world-reachable rather than warn and continue).
//!
//! Frame cap, read timeout, and connection cap have env overrides (see
//! [`Limits`]) so tests can exercise the bounds fast without waiting the full
//! production timeout or opening hundreds of sockets.
//!
//! # Publish durability across shutdown
//!
//! On SIGINT/SIGTERM the daemon stops accepting and **drains** in-flight
//! connection tasks for a bounded grace period ([`SHUTDOWN_GRACE`]) so a publish
//! that already committed gets its ack out before the runtime drops. Publish is
//! nonetheless **at-least-once across a hard crash / SIGKILL**: there is no
//! idempotency key in the MVP, so a client that never receives an ack and retries
//! can duplicate an event. Edge-triggered adapters dedup downstream via their
//! stored baselines (design/01); a wire idempotency key is deliberately deferred.
//!
//! # No TCP
//!
//! Unix socket only (AGENTS.md hard boundary).

use std::io;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use nix::errno::Errno;
use nix::fcntl::{Flock, FlockArg};
use nix::sys::stat::{Mode, umask};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::Semaphore;
use tracing::{info, warn};

use mailbox_protocol::{AdapterId, GithubPr, Timestamp, Topic};

use mailbox::bus::Bus;
use mailbox::resolver::DefaultResolver;
use mailbox::storage::{SessionId, Storage, StorageConfig};
use mailbox::supervisor::{RestartPolicy, Supervisor, reconcile_startup};
use mailbox::wake::Waker;

use crate::control::{
    AgentSummary, GithubPrTarget, Request, Response, StatusReport, TopicStatus, decode_frame,
    encode_frame,
};

/// Hard cap on a single control frame (request line). Sized for the largest
/// legitimate frame — a `publish` carrying an event body — with generous head
/// room; a frame beyond it is rejected, never buffered. 4 MiB comfortably fits a
/// large PR-event body while bounding daemon memory against an adversarial client
/// that streams bytes with no newline (the confirmed ~300 MB OOM).
const DEFAULT_MAX_FRAME_BYTES: u64 = 4 * 1024 * 1024;

/// Drop a connection whose request line does not arrive within this window, so a
/// half-open or slow-loris client cannot pin a task + fd indefinitely.
const DEFAULT_READ_TIMEOUT: Duration = Duration::from_secs(10);

/// Maximum concurrently-served connections. Bounds tasks/fds so a burst of
/// connect-and-hang clients cannot exhaust them; over the cap, new connections
/// are dropped (a client retries — the daemon stays up).
const DEFAULT_MAX_CONNECTIONS: usize = 128;

/// Backoff after an `accept()` error, so an fd-exhaustion (EMFILE) storm cannot
/// tight-loop the accept path at 100% CPU.
const ACCEPT_BACKOFF: Duration = Duration::from_millis(50);

/// How long shutdown waits for in-flight connection tasks to finish (so a
/// committed publish gets its ack out) before dropping the runtime.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(5);

/// How often the TTL sweeper runs, dropping interests whose session hard-died
/// without a `SessionEnd`/`unwatch` (design/01 reconcile row).
const DEFAULT_SWEEP_INTERVAL: Duration = Duration::from_secs(300);

/// Interests older than this are swept. Generous by default: a live session
/// refreshes its last-seen on `watch` and (card 11) via the harness heartbeat, so
/// an interest only ages out once a session has genuinely gone away without
/// saying so. Missing a slow cleanup beats dropping a live session's watch.
const DEFAULT_INTEREST_TTL: Duration = Duration::from_secs(3600);

/// Runtime-tunable daemon limits. Defaults are the constants above; each may be
/// overridden by an env var, which keeps the safety property (a limit exists)
/// while letting tests exercise the bounds fast and deterministically instead of
/// waiting the full production timeout or opening hundreds of sockets:
/// `MAILBOX_READ_TIMEOUT_MS`, `MAILBOX_MAX_CONNECTIONS`, `MAILBOX_MAX_FRAME_BYTES`.
#[derive(Debug, Clone, Copy)]
struct Limits {
    read_timeout: Duration,
    max_connections: usize,
    max_frame_bytes: u64,
}

impl Limits {
    fn from_env() -> Self {
        Limits {
            read_timeout: env_var("MAILBOX_READ_TIMEOUT_MS")
                .map(Duration::from_millis)
                .unwrap_or(DEFAULT_READ_TIMEOUT),
            // A cap of 0 would be nonsensical (nothing could ever connect), so
            // clamp any override to at least 1.
            max_connections: env_var("MAILBOX_MAX_CONNECTIONS")
                .map(|n| (n as usize).max(1))
                .unwrap_or(DEFAULT_MAX_CONNECTIONS),
            max_frame_bytes: env_var("MAILBOX_MAX_FRAME_BYTES").unwrap_or(DEFAULT_MAX_FRAME_BYTES),
        }
    }
}

/// Read a `u64` env override, ignoring an unset or unparseable value.
fn env_var(key: &str) -> Option<u64> {
    std::env::var(key).ok()?.parse().ok()
}

/// Everything a request handler needs, bundled so it travels as ONE value from
/// `accept_loop` down to `dispatch` (rather than five positional arguments that
/// grow with every card). Cheap to clone: the bus/storage/supervisor handles are
/// channels, and the waiters path is shared behind an `Arc`.
#[derive(Clone)]
struct Ctx {
    bus: Bus,
    storage: Storage,
    supervisor: Supervisor,
    /// The per-session waiter FIFOs/pidfiles directory — the source of the
    /// live-waiter liveness `agents` reports (card 16).
    waiters_dir: Arc<PathBuf>,
}

/// Run the daemon until a termination signal (SIGINT/SIGTERM) arrives.
pub async fn run(config: StorageConfig) -> anyhow::Result<()> {
    // 1. Create the owner-only directory FIRST (0700), fatal on failure — every
    //    resource beneath it (DB, socket, lock) is then owner-only from creation.
    create_dir_owner_only(&config.dir())?;

    // 2. Take the exclusive daemon lock BEFORE opening the writer. This is the
    //    cross-process single-writer guard: a second `serve` fails loudly here.
    //    Held for the daemon's whole life (released on drop at end of `run`).
    let _lock = acquire_daemon_lock(&config.lock_path())?;

    // 3. Now it is safe to open the single writer + wake channel.
    let storage = Storage::open(config.clone()).await?;
    let waker = Waker::new(config.waiters_dir());
    let bus = Bus::with_waker(storage.clone(), waker);

    // 4. Fail-safe on restart: mark previously-running watches stopped and clear
    //    their pids. We do NOT resume them — until a session-liveness probe
    //    exists, a resumed poller could outlive every session that wanted it
    //    (design/01 rule 6). A live session must re-`watch` to restart a poller.
    reconcile_startup(&storage).await?;

    // 5. Build the watch supervisor with the default resolver: a `stub` watch
    //    spawns the reference adapter (card 09) and a `github-pr` watch spawns the
    //    real PR poller (card 10). The supervisor injects each watch's persisted
    //    baseline into the adapter's spawn config and relays the adapter's
    //    `Baseline` lines back to storage (baseline-via-protocol).
    let supervisor = Supervisor::spawn(
        storage.clone(),
        bus.clone(),
        Arc::new(DefaultResolver::default()),
        RestartPolicy::default(),
    );

    // 6. We hold the lock, so any leftover socket node is provably stale.
    let socket_path = config.socket_path();
    remove_stale_socket(&socket_path)?;
    let listener = bind_socket_owner_only(&socket_path)?;

    let limits = Limits::from_env();
    info!(
        socket = %socket_path.display(),
        db = %config.path().display(),
        max_connections = limits.max_connections,
        "bridge serving (single writer + waker + supervisor); Ctrl-C or SIGTERM to stop"
    );

    // 7. Periodically sweep stale interests so a hard-killed session's watch is
    //    reconciled and its adapter stopped when its interest hits zero.
    let sweeper = spawn_sweeper(supervisor.clone());

    // 8. Serve until a shutdown signal, capping concurrent handlers.
    let ctx = Ctx {
        bus,
        storage,
        supervisor: supervisor.clone(),
        waiters_dir: Arc::new(config.waiters_dir()),
    };
    let connections = Arc::new(Semaphore::new(limits.max_connections));
    let result = accept_loop(&listener, &ctx, &connections, limits).await;

    // 9. Stop the sweeper and tear down every adapter so none outlives the bridge,
    //    then drain in-flight tasks (bounded) and clean up the socket. The lock
    //    releases on drop.
    sweeper.abort();
    if let Err(err) = supervisor.shutdown().await {
        warn!(error = %err, "supervisor shutdown reported an error");
    }
    drain_connections(&connections, limits.max_connections).await;
    if let Err(err) = std::fs::remove_file(&socket_path)
        && err.kind() != io::ErrorKind::NotFound
    {
        warn!(socket = %socket_path.display(), error = %err, "could not remove socket on shutdown");
    }
    info!("bridge stopped");
    result
}

/// Spawn the periodic TTL sweeper. Env overrides (`MAILBOX_SWEEP_INTERVAL_MS`,
/// `MAILBOX_INTEREST_TTL_MS`) let tests drive it fast; production uses the
/// generous defaults so a live session's watch is never swept out from under it.
fn spawn_sweeper(supervisor: Supervisor) -> tokio::task::JoinHandle<()> {
    let interval = env_var("MAILBOX_SWEEP_INTERVAL_MS")
        .map(Duration::from_millis)
        .unwrap_or(DEFAULT_SWEEP_INTERVAL);
    let ttl = env_var("MAILBOX_INTEREST_TTL_MS")
        .map(Duration::from_millis)
        .unwrap_or(DEFAULT_INTEREST_TTL);
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(interval).await;
            match supervisor.sweep(ttl).await {
                Ok(swept) if !swept.is_empty() => {
                    info!(
                        count = swept.len(),
                        "TTL sweep stopped watches with no live interest"
                    )
                }
                Ok(_) => {}
                Err(err) => warn!(error = %err, "TTL sweep failed"),
            }
        }
    })
}

/// Accept connections until a shutdown signal. Each connection is served on its
/// own task guarded by a semaphore permit; accept errors back off.
async fn accept_loop(
    listener: &UnixListener,
    ctx: &Ctx,
    connections: &Arc<Semaphore>,
    limits: Limits,
) -> anyhow::Result<()> {
    let mut sigterm = signal_stream()?;
    loop {
        tokio::select! {
            // Biased so a pending signal wins over a ready connection during a burst.
            biased;
            _ = tokio::signal::ctrl_c() => {
                info!("received interrupt; shutting down");
                return Ok(());
            }
            _ = sigterm.recv() => {
                info!("received terminate; shutting down");
                return Ok(());
            }
            accepted = listener.accept() => {
                match accepted {
                    Ok((stream, _addr)) => spawn_handler(stream, ctx, connections, limits),
                    Err(err) => {
                        // EMFILE and friends: back off so we cannot tight-loop.
                        warn!(error = %err, backoff = ?ACCEPT_BACKOFF, "accept failed; backing off");
                        tokio::time::sleep(ACCEPT_BACKOFF).await;
                    }
                }
            }
        }
    }
}

/// Spawn a handler for `stream` iff a connection permit is available; otherwise
/// drop the connection (the client can retry — the daemon stays healthy).
fn spawn_handler(stream: UnixStream, ctx: &Ctx, connections: &Arc<Semaphore>, limits: Limits) {
    match connections.clone().try_acquire_owned() {
        Ok(permit) => {
            let ctx = ctx.clone();
            tokio::spawn(async move {
                // Hold the permit for the task's life; dropping it frees a slot
                // and lets shutdown drain see this task complete.
                let _permit = permit;
                if let Err(err) = handle_connection(stream, ctx, limits).await {
                    warn!(error = %err, "connection handler failed");
                }
            });
        }
        Err(_) => {
            warn!(
                max = limits.max_connections,
                "connection cap reached; dropping connection"
            );
            // `stream` drops here, closing the connection.
        }
    }
}

/// Wait (bounded) for in-flight connection tasks to finish. Each task holds one
/// permit, so all [`MAX_CONNECTIONS`] permits being free again means every task
/// has completed and flushed its reply (e.g. a committed publish's ack).
async fn drain_connections(connections: &Arc<Semaphore>, max_connections: usize) {
    let all = max_connections as u32;
    match tokio::time::timeout(SHUTDOWN_GRACE, connections.acquire_many(all)).await {
        Ok(Ok(_permits)) => info!("drained in-flight connections"),
        // The semaphore is never closed while we hold `connections`, so this arm
        // is unreachable in practice; treat it as "nothing to drain".
        Ok(Err(_closed)) => {}
        Err(_elapsed) => warn!(
            grace = ?SHUTDOWN_GRACE,
            "shutdown grace elapsed with connections still in flight"
        ),
    }
}

/// A SIGTERM stream (SIGINT is handled via `ctrl_c`).
fn signal_stream() -> anyhow::Result<tokio::signal::unix::Signal> {
    use tokio::signal::unix::{SignalKind, signal};
    Ok(signal(SignalKind::terminate())?)
}

/// Serve exactly one request on `stream`: read one bounded frame, dispatch, reply
/// one line. Logs the command AFTER servicing it (past tense, structured, with
/// session + topic, never the body).
async fn handle_connection(stream: UnixStream, ctx: Ctx, limits: Limits) -> anyhow::Result<()> {
    let (read_half, mut write_half) = stream.into_split();

    let request = match read_incoming(read_half, limits).await {
        // Client connected then closed / timed out — nothing to reply.
        Incoming::Closed => return Ok(()),
        // A malformed/oversized/undecodable frame gets a best-effort error reply.
        Incoming::Reject(response) => {
            warn!(detail = %response_detail(&response), "rejected an invalid request frame");
            return write_response(&mut write_half, &response).await;
        }
        Incoming::Request(request) => request,
    };

    // Capture identifying context before the request is consumed by dispatch.
    let op = request_op(&request);
    let session = request_session(&request).map(|s| s.as_str().to_string());
    let topic = request_topic(&request).map(|t| t.as_str().to_string());

    let response = dispatch(&ctx, request).await;

    let outcome = match &response {
        Response::Error { .. } => "error",
        _ => "ok",
    };
    info!(
        op,
        session = session.as_deref().unwrap_or("-"),
        topic = topic.as_deref().unwrap_or("-"),
        outcome,
        "handled control request"
    );
    write_response(&mut write_half, &response).await
}

/// The result of trying to read one request frame.
enum Incoming {
    /// A well-formed request to dispatch.
    Request(Request),
    /// The frame was invalid (too large, non-UTF8, undecodable); reply this error.
    Reject(Response),
    /// The client closed or timed out before sending a frame; no reply.
    Closed,
}

/// Read exactly one request frame, bounded and timed. Never buffers more than
/// [`MAX_FRAME_BYTES`]: the read half is wrapped in `take`, so an adversarial
/// client streaming bytes with no newline hits EOF at the cap and is rejected.
async fn read_incoming(read_half: tokio::net::unix::OwnedReadHalf, limits: Limits) -> Incoming {
    let mut reader = BufReader::new(read_half.take(limits.max_frame_bytes));
    let mut buf = Vec::new();

    let read = tokio::time::timeout(limits.read_timeout, reader.read_until(b'\n', &mut buf)).await;
    match read {
        Err(_elapsed) => {
            warn!(timeout = ?limits.read_timeout, "dropped a client that sent no frame in time");
            Incoming::Closed
        }
        Ok(Err(err)) => {
            warn!(error = %err, "i/o error reading a request frame; dropping");
            Incoming::Closed
        }
        Ok(Ok(0)) => Incoming::Closed,
        Ok(Ok(_)) => {
            if buf.last() != Some(&b'\n') {
                // No terminating newline: either we hit the byte cap (oversized)
                // or the client closed mid-line (truncated). Reject, never buffer more.
                let message = if buf.len() as u64 >= limits.max_frame_bytes {
                    format!(
                        "request frame exceeds the {}-byte limit",
                        limits.max_frame_bytes
                    )
                } else {
                    "incomplete request frame (connection closed before newline)".to_string()
                };
                return Incoming::Reject(Response::error(message));
            }
            match std::str::from_utf8(&buf) {
                Err(_) => Incoming::Reject(Response::error("request frame is not valid UTF-8")),
                Ok(line) => match decode_frame::<Request>(line) {
                    Ok(request) => Incoming::Request(request),
                    Err(err) => Incoming::Reject(Response::error(format!(
                        "could not decode request: {err}"
                    ))),
                },
            }
        }
    }
}

/// Write one response frame + newline, then close the write half.
async fn write_response(
    write_half: &mut tokio::net::unix::OwnedWriteHalf,
    response: &Response,
) -> anyhow::Result<()> {
    let line = encode_frame(response)?;
    write_half.write_all(line.as_bytes()).await?;
    write_half.write_all(b"\n").await?;
    write_half.shutdown().await?;
    Ok(())
}

/// A stable op label for logging, without moving the request.
fn request_op(request: &Request) -> &'static str {
    match request {
        Request::Publish { .. } => "publish",
        Request::Subscribe { .. } => "subscribe",
        Request::Unsubscribe { .. } => "unsubscribe",
        Request::Read { .. } => "read",
        Request::Watch { .. } => "watch",
        Request::Unwatch { .. } => "unwatch",
        Request::WatchStub { .. } => "watch_stub",
        Request::UnwatchStub { .. } => "unwatch_stub",
        Request::Status { .. } => "status",
        Request::Send { .. } => "send",
        Request::Agents { .. } => "agents",
        Request::Topics { .. } => "topics",
        Request::EndSession { .. } => "end_session",
    }
}

/// The session a request is for, if any (publish carries none — its provenance
/// is the adapter id).
fn request_session(request: &Request) -> Option<&SessionId> {
    match request {
        // Publish carries no session (its provenance is the adapter id), and
        // `topics` is a global read with no session at all.
        Request::Publish { .. } | Request::Topics { .. } => None,
        Request::Subscribe { session, .. }
        | Request::Unsubscribe { session, .. }
        | Request::Read { session, .. }
        | Request::Watch { session, .. }
        | Request::Unwatch { session, .. }
        | Request::WatchStub { session, .. }
        | Request::UnwatchStub { session, .. }
        | Request::Status { session }
        | Request::Agents { session }
        | Request::EndSession { session } => Some(session),
        // For a send, the session that acted is the SENDER (the recipient is
        // logged by the agents module with both ends).
        Request::Send { from, .. } => Some(from),
    }
}

/// The topic a request directly names, if any (watch/unwatch name a target, not
/// a `Topic`, so they are logged by op + session only).
fn request_topic(request: &Request) -> Option<&Topic> {
    match request {
        Request::Publish { topic, .. }
        | Request::Subscribe { topic, .. }
        | Request::Unsubscribe { topic, .. } => Some(topic),
        _ => None,
    }
}

/// A short, body-free detail for logging a rejected frame.
fn response_detail(response: &Response) -> &str {
    match response {
        Response::Error { message } => message,
        _ => "invalid frame",
    }
}

/// Map a request onto bus/watch operations, converting any business error into a
/// [`Response::Error`] the client can surface. Never returns `Err`: transport
/// failures are the caller's concern, business failures travel as a value.
async fn dispatch(ctx: &Ctx, request: Request) -> Response {
    let Ctx {
        bus,
        storage,
        supervisor,
        waiters_dir,
    } = ctx;
    match request {
        Request::Publish {
            topic,
            adapter,
            body,
        } => publish(bus, topic, adapter, body).await,
        Request::Subscribe { session, topic } => subscribe(bus, session, topic).await,
        Request::Unsubscribe { session, topic } => unsubscribe(bus, session, topic).await,
        Request::Read { session, limit } => read(bus, session, limit).await,
        Request::Watch {
            session,
            target,
            interval_secs,
        } => watch(bus, storage, supervisor, session, target, interval_secs).await,
        Request::Unwatch { session, target } => {
            unwatch(bus, storage, supervisor, session, target).await
        }
        Request::WatchStub {
            session,
            label,
            interval_ms,
            count,
        } => watch_stub(bus, storage, supervisor, session, label, interval_ms, count).await,
        Request::UnwatchStub { session, label } => {
            unwatch_stub(bus, storage, supervisor, session, label).await
        }
        Request::Status { session } => status(storage, session).await,
        Request::Send { from, to, body } => send(bus, storage, from, to, body).await,
        Request::Agents { session } => agents(storage, waiters_dir, session).await,
        Request::Topics { prefix } => topics(storage, prefix).await,
        Request::EndSession { session } => end_session(storage, supervisor, session).await,
    }
}

/// Thin translation over [`mailbox::agents::send`] (card 16): publish to the
/// target's inbox, refusing loudly if that agent has no registered inbox.
async fn send(
    bus: &Bus,
    storage: &Storage,
    from: SessionId,
    to: SessionId,
    body: serde_json::Map<String, serde_json::Value>,
) -> Response {
    match mailbox::agents::send(bus, storage, from, to, body).await {
        Ok(sent) => Response::Sent {
            to: sent.to,
            topic: sent.topic,
            id: sent.event.id,
            offset: sent.event.offset,
        },
        Err(err) => Response::error(err.to_string()),
    }
}

/// Thin translation over [`mailbox::agents::list`] (card-16 discovery).
async fn agents(storage: &Storage, waiters_dir: &Path, caller: SessionId) -> Response {
    match mailbox::agents::list(storage, waiters_dir, &caller).await {
        Ok(agents) => Response::Agents {
            agents: agents
                .into_iter()
                .map(|agent| AgentSummary {
                    session: agent.session,
                    inbox: agent.inbox,
                    live_waiter: agent.live_waiter,
                    is_self: agent.is_self,
                })
                .collect(),
        },
        Err(err) => Response::error(err.to_string()),
    }
}

/// Topic discovery: every known topic with its subscriber/event counts.
async fn topics(storage: &Storage, prefix: Option<String>) -> Response {
    match storage.list_topics(prefix).await {
        Ok(topics) => Response::Topics {
            topics: topics.into_iter().map(TopicStatus::from).collect(),
        },
        Err(err) => Response::error(err.to_string()),
    }
}

async fn publish(bus: &Bus, topic: Topic, adapter: AdapterId, body: serde_json::Value) -> Response {
    // An agent inbox is writable ONLY through `mailbox send`, which stamps
    // provenance (`from`) and refuses an unregistered target (ADR-0007). The
    // generic publish path does neither, so allowing it here would let any caller
    // forge a `from` into a victim's inbox — or write into an inbox nobody has
    // registered, where baseline-on-subscribe guarantees it can never be read.
    // Reuse the protocol's own grammar (`as_agent_inbox`) rather than string-
    // matching `"agent."`, so the namespace test can never drift from the minter.
    if topic.as_agent_inbox().is_ok() {
        return Response::error(format!(
            "refusing to publish to inbox topic {}: agent inboxes are writable only via \
             `mailbox send`, which stamps the sender and checks the target is registered",
            topic.as_str()
        ));
    }
    // The daemon stamps the timestamp (one clock, like the durable bridge does).
    match bus
        .publish(
            topic,
            adapter,
            Timestamp(mailbox::clock::now_millis()),
            body,
        )
        .await
    {
        Ok(event) => Response::Published {
            id: event.id,
            offset: event.offset,
        },
        Err(err) => Response::error(err.to_string()),
    }
}

async fn subscribe(bus: &Bus, session: SessionId, topic: Topic) -> Response {
    match bus.subscribe(session, std::slice::from_ref(&topic)).await {
        Ok(mut summary) => match summary.pop() {
            Some((_, outcome)) => Response::Subscribed {
                topic,
                outcome: outcome.into(),
            },
            None => Response::error("subscribe returned no per-topic outcome"),
        },
        Err(err) => Response::error(err.to_string()),
    }
}

async fn unsubscribe(bus: &Bus, session: SessionId, topic: Topic) -> Response {
    match bus.unsubscribe(session, std::slice::from_ref(&topic)).await {
        Ok(()) => Response::Unsubscribed { topic },
        Err(err) => Response::error(err.to_string()),
    }
}

async fn read(bus: &Bus, session: SessionId, limit: Option<u32>) -> Response {
    match bus.read(session, limit).await {
        Ok(delivery) => Response::Read {
            events: delivery.into_events(),
        },
        Err(err) => Response::error(err.to_string()),
    }
}

/// Thin translation over [`mailbox::watch::record`] (see that module and the
/// card-06↔08 boundary there).
async fn watch(
    bus: &Bus,
    storage: &Storage,
    supervisor: &Supervisor,
    session: SessionId,
    target: GithubPrTarget,
    interval_secs: u64,
) -> Response {
    let pr = match github_pr(&target) {
        Ok(pr) => pr,
        Err(message) => return Response::error(message),
    };
    match mailbox::watch::record(
        bus,
        storage,
        supervisor,
        &pr,
        Duration::from_secs(interval_secs),
        session,
    )
    .await
    {
        Ok(recorded) => Response::Watched {
            topic: recorded.topic,
            interest: recorded.interest,
            subscribe: recorded.subscribe.into(),
        },
        Err(err) => Response::error(err.to_string()),
    }
}

/// Thin translation over [`mailbox::watch::drop_interest`].
async fn unwatch(
    bus: &Bus,
    storage: &Storage,
    supervisor: &Supervisor,
    session: SessionId,
    target: GithubPrTarget,
) -> Response {
    let pr = match github_pr(&target) {
        Ok(pr) => pr,
        Err(message) => return Response::error(message),
    };
    match mailbox::watch::drop_interest(bus, storage, supervisor, &pr, session).await {
        Ok(dropped) => Response::Unwatched {
            topic: dropped.topic,
            outcome: dropped.outcome.into(),
        },
        Err(err) => Response::error(err.to_string()),
    }
}

/// Thin translation over [`mailbox::watch::record_stub`] (the stub twin of
/// [`watch`]). The daemon stamps the interval in milliseconds into a `Duration`.
#[allow(clippy::too_many_arguments)]
async fn watch_stub(
    bus: &Bus,
    storage: &Storage,
    supervisor: &Supervisor,
    session: SessionId,
    label: String,
    interval_ms: u64,
    count: u64,
) -> Response {
    match mailbox::watch::record_stub(
        bus,
        storage,
        supervisor,
        &label,
        Duration::from_millis(interval_ms),
        count,
        session,
    )
    .await
    {
        Ok(recorded) => Response::Watched {
            topic: recorded.topic,
            interest: recorded.interest,
            subscribe: recorded.subscribe.into(),
        },
        Err(err) => Response::error(err.to_string()),
    }
}

/// Thin translation over [`mailbox::watch::drop_interest_stub`].
async fn unwatch_stub(
    bus: &Bus,
    storage: &Storage,
    supervisor: &Supervisor,
    session: SessionId,
    label: String,
) -> Response {
    match mailbox::watch::drop_interest_stub(bus, storage, supervisor, &label, session).await {
        Ok(dropped) => Response::Unwatched {
            topic: dropped.topic,
            outcome: dropped.outcome.into(),
        },
        Err(err) => Response::error(err.to_string()),
    }
}

/// Thin translation over [`mailbox::watch::status`].
async fn status(storage: &Storage, session: SessionId) -> Response {
    match mailbox::watch::status(storage, session.clone()).await {
        Ok(view) => Response::Status(StatusReport::from_view(session, view)),
        Err(err) => Response::error(err.to_string()),
    }
}

/// Thin translation over [`mailbox::watch::end_session`] (the harness `SessionEnd`
/// teardown, card 11): drop the session's subscriptions + interests and stop any
/// now-orphaned adapters.
async fn end_session(storage: &Storage, supervisor: &Supervisor, session: SessionId) -> Response {
    match mailbox::watch::end_session(storage, supervisor, session).await {
        Ok(ended) => Response::SessionEnded {
            subscriptions_dropped: ended.subscriptions_dropped,
            interests_dropped: ended.interests_dropped,
            adapters_stopped: ended.adapters_stopped,
        },
        Err(err) => Response::error(err.to_string()),
    }
}

/// Validate a wire [`GithubPrTarget`] into a domain [`GithubPr`] at the daemon
/// edge, turning a bad owner/repo/number into a clean error message.
fn github_pr(target: &GithubPrTarget) -> Result<GithubPr, String> {
    GithubPr::new(&target.owner, &target.repo, target.number)
        .map_err(|err| format!("invalid github-pr target: {err}"))
}

/// Create the daemon directory `0700`, fatal on failure (B4: refuse to serve
/// world-reachable rather than fail open). `set_permissions` runs even if the
/// directory already existed, so a dir left looser by an earlier process is
/// tightened.
fn create_dir_owner_only(dir: &Path) -> anyhow::Result<()> {
    std::fs::create_dir_all(dir)
        .map_err(|e| anyhow::anyhow!("could not create daemon dir {}: {e}", dir.display()))?;
    std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))
        .map_err(|e| anyhow::anyhow!("could not set 0700 on daemon dir {}: {e}", dir.display()))?;
    Ok(())
}

/// Acquire the exclusive daemon lock (non-blocking). A second `serve` on the same
/// DB gets `EWOULDBLOCK` and fails loudly. The returned [`Flock`] releases on drop
/// (end of `run`).
fn acquire_daemon_lock(path: &Path) -> anyhow::Result<Flock<std::fs::File>> {
    let file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(path)
        .map_err(|e| anyhow::anyhow!("could not open daemon lockfile {}: {e}", path.display()))?;

    match Flock::lock(file, FlockArg::LockExclusiveNonblock) {
        Ok(lock) => Ok(lock),
        // EAGAIN and EWOULDBLOCK are the same errno on our targets, so match by
        // guard (an or-pattern would be an unreachable second arm).
        Err((_file, errno)) if errno == Errno::EAGAIN || errno == Errno::EWOULDBLOCK => {
            anyhow::bail!(
                "another `mailbox serve` is already running (lock held at {})",
                path.display()
            )
        }
        Err((_file, errno)) => {
            anyhow::bail!("could not acquire daemon lock {}: {errno}", path.display())
        }
    }
}

/// Remove a stale socket node so `bind` can succeed. Safe because we hold the
/// daemon lock, so no live daemon owns this socket. Logs only when it actually
/// removed something.
fn remove_stale_socket(socket: &Path) -> anyhow::Result<()> {
    match std::fs::remove_file(socket) {
        Ok(()) => {
            info!(socket = %socket.display(), "removed stale socket left by a previous daemon");
            Ok(())
        }
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(err) => {
            anyhow::bail!("could not remove stale socket {}: {err}", socket.display())
        }
    }
}

/// Bind the socket owner-only. A restrictive umask around `bind` makes the node
/// `0600` from creation (no world-open window), and an explicit `set_permissions`
/// confirms it. Any failure is FATAL — we refuse to serve rather than serve
/// fail-open.
fn bind_socket_owner_only(socket: &Path) -> anyhow::Result<UnixListener> {
    // 0o177 masks group/other entirely and owner-execute, so a fresh socket is
    // created rw-------  (0600). Restore the previous umask immediately after.
    let previous = umask(Mode::from_bits_truncate(0o177));
    let bound = UnixListener::bind(socket);
    umask(previous);

    let listener = bound
        .map_err(|e| anyhow::anyhow!("could not bind bridge socket {}: {e}", socket.display()))?;
    std::fs::set_permissions(socket, std::fs::Permissions::from_mode(0o600))
        .map_err(|e| anyhow::anyhow!("could not set 0600 on socket {}: {e}", socket.display()))?;
    Ok(listener)
}
