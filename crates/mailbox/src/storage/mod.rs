//! Durable, single-writer SQLite storage for the bridge.
//!
//! # What this is
//!
//! The bridge's durable core: the append-only topic log, subscriptions,
//! per-subscriber delivery cursors, supervised-watch bookkeeping, refcounted
//! interest, and adapter baselines. It is bridge-internal (ADR-0003): adapters
//! and the harness never touch it, they speak `mailbox-protocol` and let the
//! bridge mutate.
//!
//! # Single writer (ADR-0003)
//!
//! One dedicated OS thread owns the `rusqlite::Connection`. The public
//! [`Storage`] handle holds only a channel to that thread — cloning the handle
//! clones the channel, never the connection — so there is exactly one writer
//! and no second path to the *write* connection. Reads for the bus travel the
//! same channel and are answered by that connection in the same process.
//!
//! The single exception is [`ReadOnlyStore`]: the one permitted read-only side
//! connection (ADR-0003 explicitly allows read-only side opens for wake). It is
//! opened by the separate waiter process with `SQLITE_OPEN_READ_ONLY` (no
//! create) and never mutates — the missed-kick unread check. See [`reader`].
//!
//! # Errors
//!
//! Every method returns [`StorageError`]; a database failure is a value, not a
//! panic.

mod error;
mod model;
mod schema;
mod writer;

use std::path::{Path, PathBuf};

use serde_json::Value;
use tokio::sync::{mpsc, oneshot};

use mailbox_protocol::{AdapterId, Event, Offset, Subject, Timestamp, Topic};

pub use error::StorageError;
// Re-exported so callers of the public read API (e.g. `read_events`) can name
// the cursor type without reaching into `mailbox-protocol` directly.
pub use mailbox_protocol::Cursor;
pub use model::{
    EndSessionOutcome, Pid, ReadPage, SessionId, SubscribeKind, SubscribeOutcome, TopicDigest,
    TopicSummary, Watch, WatchId, WatchKind, WatchSpec, WatchState, WatchTarget,
};
// The one permitted read-only side connection (ADR-0003), used by the wake
// waiter. Crate-private like its `Command` sibling — its only consumer is the
// `wake` module.

use writer::Command;

/// Environment variable that overrides the full database file path.
const ENV_DB_PATH: &str = "AGENT_MAILBOX_DB";
/// Environment variable that overrides the home directory used for the default
/// path. Falls back to `HOME`. Lets tests and sandboxes avoid the real home.
const ENV_HOME: &str = "AGENT_MAILBOX_HOME";
/// Directory (under home) and file name of the default database.
const DEFAULT_DIR: &str = ".agent-mailbox";
const DEFAULT_FILE: &str = "mailbox.db";
/// File name (beside the database file) of the user-scoped Unix socket the
/// `serve` daemon binds. Derived from the resolved DB path — rather than from a
/// separate env var — so the CLI clients and the daemon agree on one path under
/// every storage override, with nothing to keep in sync (card 06, ADR-0004).
const SOCKET_FILE: &str = "mailbox.sock";
/// File name (beside the database file) of the daemon's exclusive lockfile. The
/// `serve` daemon holds an advisory `flock` on this for its whole life BEFORE it
/// opens the writer, so two `serve` processes on one DB cannot both become
/// writers (the real cross-process single-writer guard — ADR-0003/0004).
const LOCK_FILE: &str = "mailbox.lock";

/// The append-only log every hook-run command writes to, beside the database. It is
/// the only durable record of the wake path's decisions, and so the only place to
/// reconstruct why a given session was — or was not — woken.
const HARNESS_LOG_FILE: &str = "harness.log";

/// Capacity of the writer command channel.
///
/// The channel is BOUNDED so a sustained publish burst applies backpressure
/// (callers await a send permit) instead of growing an unbounded queue — each
/// queued command can hold a full event body, so an unbounded queue is an OOM
/// waiting to happen. 1024 is a generous buffer for local agent volumes: deep
/// enough to absorb normal bursts without callers ever waiting, shallow enough
/// that a runaway producer is throttled to the writer's pace rather than
/// buffering gigabytes. Bounded-vs-unbounded is the safety property here; the
/// exact number is not load-bearing.
const COMMAND_CHANNEL_CAPACITY: usize = 1024;

/// Where the database lives and how to open it.
#[derive(Debug, Clone)]
pub struct StorageConfig {
    path: PathBuf,
}

impl StorageConfig {
    /// Use an explicit database file path. The parent directory is created on
    /// open if missing. Tests use this with a tempdir so they never touch the
    /// real home directory.
    pub fn at(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    /// Resolve the path from the environment, else the default
    /// `~/.agent-mailbox/mailbox.db`.
    ///
    /// Precedence: [`ENV_DB_PATH`] (full path) → [`ENV_HOME`]/`HOME` +
    /// `.agent-mailbox/mailbox.db`. Errors with [`StorageError::NoStoragePath`]
    /// if none is available rather than guessing.
    pub fn from_env() -> Result<Self, StorageError> {
        if let Some(path) = env_path(ENV_DB_PATH) {
            return Ok(Self { path });
        }
        let home = env_path(ENV_HOME)
            .or_else(|| env_path("HOME"))
            .ok_or(StorageError::NoStoragePath)?;
        Ok(Self {
            path: home.join(DEFAULT_DIR).join(DEFAULT_FILE),
        })
    }

    /// The resolved database file path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The user-scoped Unix socket the `serve` daemon binds and CLI clients
    /// connect to (`<db-parent>/mailbox.sock`). Derived from the resolved DB path
    /// so daemon and clients agree on one location under every storage override
    /// without a separate env var (card 06 / ADR-0004). Falls back to a bare
    /// relative name only if the DB path has no parent, matching how
    /// [`Storage::open`] treats an empty parent.
    pub fn socket_path(&self) -> PathBuf {
        self.sibling(SOCKET_FILE)
    }

    /// The harness log (`<db-parent>/harness.log`), where every hook-run and
    /// detached command records what the wake path decided. Derived from the DB path
    /// like the socket and the lock, so a test pointing storage at a tempdir gets an
    /// isolated log rather than appending to the developer's real one.
    pub fn harness_log_path(&self) -> PathBuf {
        self.sibling(HARNESS_LOG_FILE)
    }

    /// The daemon's exclusive lockfile (`<db-parent>/mailbox.lock`). Derived from
    /// the resolved DB path like the socket so the lock, the socket, and the
    /// database always live together under any override (see [`LOCK_FILE`]).
    pub fn lock_path(&self) -> PathBuf {
        self.sibling(LOCK_FILE)
    }

    /// The directory that holds the database, socket, and lockfile. The daemon
    /// creates it `0700` so every user-scoped resource beneath it is owner-only
    /// from creation.
    pub fn dir(&self) -> PathBuf {
        match self.path.parent() {
            Some(parent) if !parent.as_os_str().is_empty() => parent.to_path_buf(),
            _ => PathBuf::from("."),
        }
    }

    /// A file beside the database. Falls back to a bare relative name only if the
    /// DB path has no parent (a bare filename), matching how [`Storage::open`]
    /// treats an empty parent.
    fn sibling(&self, name: &str) -> PathBuf {
        match self.path.parent() {
            Some(parent) if !parent.as_os_str().is_empty() => parent.join(name),
            _ => PathBuf::from(name),
        }
    }
}

/// Read a non-empty environment variable as a path.
fn env_path(key: &str) -> Option<PathBuf> {
    match std::env::var(key) {
        Ok(value) if !value.is_empty() => Some(PathBuf::from(value)),
        _ => None,
    }
}

/// Async handle to the durable store.
///
/// Cheap to clone (it is just a channel sender); every clone talks to the same
/// single writer. Dropping the last clone closes the channel and stops the
/// writer thread.
#[derive(Clone, Debug)]
pub struct Storage {
    cmd_tx: mpsc::Sender<Command>,
}

impl Storage {
    /// Open (creating if needed) the database at `config`'s path and start the
    /// writer thread. Returns once the schema is migrated and the writer is
    /// ready, or with the open/migration error.
    pub async fn open(config: StorageConfig) -> Result<Self, StorageError> {
        let path = config.path;

        // Create the containing directory up front so a fresh install works
        // without the user pre-creating ~/.agent-mailbox.
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent).map_err(|source| StorageError::CreateDir {
                path: parent.to_path_buf(),
                source,
            })?;
        }

        let (cmd_tx, cmd_rx) = mpsc::channel(COMMAND_CHANNEL_CAPACITY);
        let (init_tx, init_rx) = oneshot::channel();

        // The connection is created and owned entirely inside this thread; only
        // `cmd_tx` escapes. That ownership is what makes "single writer"
        // structural rather than a convention.
        std::thread::Builder::new()
            .name("mailbox-storage-writer".to_string())
            .spawn(move || writer::run(path, cmd_rx, init_tx))
            .map_err(StorageError::WriterSpawn)?;

        match init_rx.await {
            Ok(Ok(())) => Ok(Self { cmd_tx }),
            Ok(Err(err)) => Err(err),
            Err(_) => Err(StorageError::WriterGone),
        }
    }

    /// Post a command to the writer and await its reply. The single choke point
    /// through which every public method reaches the database.
    async fn call<T>(
        &self,
        make: impl FnOnce(oneshot::Sender<Result<T, StorageError>>) -> Command,
    ) -> Result<T, StorageError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        // Awaiting the send is what turns a full queue into backpressure rather
        // than unbounded growth; a closed channel (writer gone) surfaces here.
        self.cmd_tx
            .send(make(reply_tx))
            .await
            .map_err(|_| StorageError::WriterGone)?;
        reply_rx.await.map_err(|_| StorageError::WriterGone)?
    }

    /// Append an event to a topic's durable log, assigning the next per-topic
    /// [`Offset`] and a fresh opaque [`mailbox_protocol::EventId`]. The `body`
    /// is stored verbatim and never interpreted.
    pub async fn publish(
        &self,
        topic: Topic,
        adapter: AdapterId,
        timestamp: Timestamp,
        body: Value,
        subject: Option<Subject>,
    ) -> Result<Event, StorageError> {
        self.call(|reply| Command::Publish {
            topic,
            adapter,
            timestamp,
            body,
            subject,
            reply,
        })
        .await
    }

    /// Read a page of events on `topic` starting from `cursor`, up to `limit`
    /// (bridge default if `None`). The returned [`ReadPage::next`] cursor
    /// continues where this page ended.
    pub async fn read_events(
        &self,
        topic: Topic,
        cursor: Cursor,
        limit: Option<u32>,
    ) -> Result<ReadPage, StorageError> {
        self.call(|reply| Command::ReadEvents {
            topic,
            cursor,
            limit,
            reply,
        })
        .await
    }

    /// Drop `session`'s subscription to `topic` (idempotent).
    ///
    /// Deliberately there is NO general `subscribe` or `advance_cursor` on this
    /// handle: a subscription must always be created together with its baseline
    /// (see [`Storage::subscribe_and_baseline`]) — an un-baselined subscription
    /// row would make [`Storage::read_unread`] replay the whole log — and the
    /// delivery cursor is owned exclusively by the read-and-advance path, so no
    /// caller may nudge it independently (that could silently skip events).
    pub async fn unsubscribe(&self, session: SessionId, topic: Topic) -> Result<(), StorageError> {
        self.call(|reply| Command::Unsubscribe {
            session,
            topic,
            reply,
        })
        .await
    }

    /// Subscribe `session` to `topic` and baseline its delivery cursor to the
    /// topic head, atomically (one writer transaction). Idempotent: a repeat
    /// subscribe is a no-op that leaves the cursor untouched. See
    /// [`SubscribeOutcome`] and the writer's `do_subscribe_and_baseline` for the
    /// baseline-on-subscribe rationale.
    ///
    /// `now_ms` (Unix millis, caller-stamped) drives the tombstone guard, and
    /// `kind` scopes it: an [`SubscribeKind::AutoInbox`] re-registration racing this
    /// session's own recent `end_session` is REFUSED
    /// ([`SubscribeOutcome::RefusedSessionRecentlyEnded`]) rather than resurrect a
    /// dead inbox (ADR-0007), whereas an [`SubscribeKind::Explicit`] subscribe from a
    /// live turn proceeds and clears any tombstone (proof-of-life).
    pub async fn subscribe_and_baseline(
        &self,
        session: SessionId,
        topic: Topic,
        now_ms: i64,
        kind: SubscribeKind,
    ) -> Result<SubscribeOutcome, StorageError> {
        self.call(|reply| Command::SubscribeAndBaseline {
            session,
            topic,
            now_ms,
            kind,
            reply,
        })
        .await
    }

    /// Read `session`'s unread events across ALL its subscribed topics — each
    /// strictly after that session's per-topic cursor — and advance those cursors
    /// to the last event returned, atomically (one writer transaction). This is
    /// the exactly-once delivery primitive (advance-on-read, no ack). `limit`
    /// bounds the page PER topic (bridge default if `None`); anything beyond it
    /// surfaces on the next read.
    pub async fn read_unread(
        &self,
        session: SessionId,
        limit: Option<u32>,
    ) -> Result<Vec<Event>, StorageError> {
        self.call(|reply| Command::ReadUnread {
            session,
            limit,
            reply,
        })
        .await
    }

    /// The highest offset delivered to `session` on `topic`, or `None` if it
    /// has never been advanced.
    pub async fn cursor(
        &self,
        session: SessionId,
        topic: Topic,
    ) -> Result<Option<Offset>, StorageError> {
        self.call(|reply| Command::GetCursor {
            session,
            topic,
            reply,
        })
        .await
    }

    /// List the sessions currently subscribed to `topic`.
    ///
    /// The delivery side of wake: after a publish lands, the bridge asks this so it
    /// knows whose inbox socket to write. A read that travels the single-writer
    /// channel like every other op, so it observes a consistent view relative to the
    /// publish that preceded it.
    pub async fn sessions_subscribed(&self, topic: Topic) -> Result<Vec<SessionId>, StorageError> {
        self.call(|reply| Command::SessionsSubscribed { topic, reply })
            .await
    }

    /// Create the watch for this entity, or return the existing one's id if a
    /// watch for the same `(kind, repo, pr)` already exists (idempotent start).
    pub async fn upsert_watch(&self, spec: WatchSpec) -> Result<WatchId, StorageError> {
        self.call(|reply| Command::UpsertWatch { spec, reply })
            .await
    }

    /// Set a watch's lifecycle [`WatchState`] (and, for `Running`, its pid).
    pub async fn set_watch_state(
        &self,
        id: WatchId,
        state: WatchState,
    ) -> Result<(), StorageError> {
        self.call(|reply| Command::SetWatchState { id, state, reply })
            .await
    }

    /// Fetch a watch by id, or `None` if it does not exist.
    pub async fn get_watch(&self, id: WatchId) -> Result<Option<Watch>, StorageError> {
        self.call(|reply| Command::GetWatch { id, reply }).await
    }

    /// Add `session`'s interest in `watch`, returning the new refcount
    /// (idempotent per session). `last_seen` (Unix millis) stamps the interest
    /// for the TTL sweeper; a re-watch refreshes it.
    pub async fn add_interest(
        &self,
        watch: WatchId,
        session: SessionId,
        last_seen: i64,
    ) -> Result<u64, StorageError> {
        self.call(|reply| Command::AddInterest {
            watch,
            session,
            last_seen,
            reply,
        })
        .await
    }

    /// Refresh one interest's `last_seen`. A no-op if `session` is not interested
    /// in `watch` — a heartbeat must not resurrect a dropped interest.
    ///
    /// The sweeper heartbeats whole sessions via [`Self::touch_session_interests`];
    /// this single-watch variant is for callers that hold one specific interest.
    pub async fn touch_interest(
        &self,
        watch: WatchId,
        session: SessionId,
        last_seen: i64,
    ) -> Result<(), StorageError> {
        self.call(|reply| Command::TouchInterest {
            watch,
            session,
            last_seen,
            reply,
        })
        .await
    }

    /// Refresh every interest held by `session`, returning how many were
    /// refreshed. The daemon-side liveness heartbeat (ADR-0009): the sweeper
    /// calls this for each session whose waiter pidfile it finds alive, which is
    /// what keeps a live-but-silent session's watch out of the TTL sweep. A no-op
    /// for a session holding no interests.
    pub async fn touch_session_interests(
        &self,
        session: SessionId,
        last_seen: i64,
    ) -> Result<u64, StorageError> {
        self.call(|reply| Command::TouchSessionInterests {
            session,
            last_seen,
            reply,
        })
        .await
    }

    /// Every distinct session holding at least one interest — the sweeper's
    /// liveness-probe candidates.
    pub async fn list_interest_sessions(&self) -> Result<Vec<SessionId>, StorageError> {
        self.call(|reply| Command::ListInterestSessions { reply })
            .await
    }

    /// The sessions holding an interest in `watch` — the startup reconcile's
    /// liveness-probe candidates for that one watch (design/01 rule 6).
    pub async fn list_watch_interest_sessions(
        &self,
        watch: WatchId,
    ) -> Result<Vec<SessionId>, StorageError> {
        self.call(|reply| Command::ListWatchInterestSessions { watch, reply })
            .await
    }

    /// Drop every interest whose `last_seen` is strictly older than `cutoff`
    /// (Unix millis), returning the watches whose interest thereby reached zero —
    /// the ones whose adapter the caller should now stop (design/01 reconcile /
    /// TTL sweep).
    pub async fn sweep_stale_interests(&self, cutoff: i64) -> Result<Vec<WatchId>, StorageError> {
        self.call(|reply| Command::SweepStaleInterests { cutoff, reply })
            .await
    }

    /// Remove `session`'s interest in `watch`, returning the remaining refcount.
    /// When this reaches 0 the caller should tear the adapter down.
    pub async fn remove_interest(
        &self,
        watch: WatchId,
        session: SessionId,
    ) -> Result<u64, StorageError> {
        self.call(|reply| Command::RemoveInterest {
            watch,
            session,
            reply,
        })
        .await
    }

    /// The number of sessions currently interested in `watch`.
    pub async fn interest_count(&self, watch: WatchId) -> Result<u64, StorageError> {
        self.call(|reply| Command::InterestCount { watch, reply })
            .await
    }

    /// Every watch the bridge knows about, in stable id order.
    ///
    /// Added for the card-06 `status` command and `unwatch` resolution: the CLI
    /// needs to enumerate watches (there is no other way to discover a watch id
    /// from its `(kind, repo, pr)` identity). A read routed through the single
    /// writer channel like [`sessions_subscribed`](Self::sessions_subscribed), so
    /// it never opens a second connection (ADR-0003).
    pub async fn list_watches(&self) -> Result<Vec<Watch>, StorageError> {
        self.call(|reply| Command::ListWatches { reply }).await
    }

    /// What `session` has waiting: per subscribed topic with at least one unread
    /// event, the count and the subjects of the newest `subjects_per_topic` of
    /// them, in ascending topic order.
    ///
    /// The read behind the wake wire (ADR-0022) and the card-06 `status` command,
    /// which asks for `0` subjects because it prints counts. "Unread" is exactly
    /// the bus definition (offset strictly beyond the session's delivery cursor,
    /// cursor treated as `-1` when absent) — the counting twin of
    /// [`ReadOnlyStore::topics_with_unread`](crate::storage::ReadOnlyStore). It is
    /// a pure `SELECT` and, unlike [`read_unread`](Self::read_unread), does **not**
    /// advance any cursor — observing mail is not reading it.
    pub async fn unread_digest(
        &self,
        session: SessionId,
        subjects_per_topic: u32,
    ) -> Result<Vec<TopicDigest>, StorageError> {
        self.call(|reply| Command::UnreadDigest {
            session,
            subjects_per_topic,
            reply,
        })
        .await
    }

    /// The topics `session` is currently subscribed to, in ascending topic order.
    ///
    /// The read behind "arm-iff-subscribed" (card 11): the harness `arm` hook asks
    /// this to decide whether an idle session has anything to be woken about before
    /// it launches a waiter. A pure read routed through the single writer channel
    /// like [`list_watches`](Self::list_watches), so it never opens a second
    /// connection (ADR-0003).
    pub async fn session_subscriptions(
        &self,
        session: SessionId,
    ) -> Result<Vec<Topic>, StorageError> {
        self.call(|reply| Command::SessionSubscriptions { session, reply })
            .await
    }

    /// The sessions that currently have a REGISTERED agent inbox — i.e. that are
    /// subscribed to their own `agent.<session-id>` topic — in ascending session
    /// order.
    ///
    /// The read behind `mailbox agents` (card 16): the discovery half of
    /// inter-agent messaging, and the check `send` makes before publishing (a
    /// message to a session with no inbox subscription could never be delivered —
    /// baseline-on-subscribe would skip it — so it must fail loudly, ADR-0007).
    /// A pure read routed through the single writer channel like
    /// [`list_watches`](Self::list_watches).
    pub async fn list_agent_inboxes(&self) -> Result<Vec<SessionId>, StorageError> {
        self.call(|reply| Command::ListAgentInboxes { reply }).await
    }

    /// Every known topic (anything subscribed to or published to) with its
    /// subscriber count, event count, and newest-event timestamp, in ascending
    /// topic order. `prefix` filters to topics starting with it.
    ///
    /// The read behind `mailbox topics` (card 16). A pure read: it advances no
    /// cursor and delivers no event.
    pub async fn list_topics(
        &self,
        prefix: Option<String>,
    ) -> Result<Vec<TopicSummary>, StorageError> {
        self.call(|reply| Command::ListTopics { prefix, reply })
            .await
    }

    /// Drop every subscription AND every watch interest held by `session`, in one
    /// transaction, returning what was removed plus the watches whose interest
    /// thereby reached zero.
    ///
    /// The durable half of the SessionEnd teardown (card 11): a departing session
    /// must leave no subscription (so a late publish wakes nobody) and no interest
    /// (so the card-08 supervisor can stop adapters nobody else wants). The caller
    /// stops each [`EndSessionOutcome::emptied_watches`] adapter — the same signal
    /// the TTL sweeper uses, but triggered promptly by an explicit session end
    /// rather than by ageing out.
    ///
    /// `now_ms` (Unix millis, caller-stamped) is recorded as the session's
    /// tombstone in the same transaction, so a `subscribe` racing this end is
    /// refused rather than resurrecting the inbox (ADR-0007).
    pub async fn end_session(
        &self,
        session: SessionId,
        now_ms: i64,
    ) -> Result<EndSessionOutcome, StorageError> {
        self.call(|reply| Command::EndSession {
            session,
            now_ms,
            reply,
        })
        .await
    }

    /// The stored adapter baseline for `watch`, or `None` if unset. Opaque JSON
    /// shaped by the adapter; storage does not interpret it.
    pub async fn get_baseline(&self, watch: WatchId) -> Result<Option<Value>, StorageError> {
        self.call(|reply| Command::GetBaseline { watch, reply })
            .await
    }

    /// Set (upsert) the adapter baseline for `watch`.
    pub async fn set_baseline(&self, watch: WatchId, baseline: Value) -> Result<(), StorageError> {
        self.call(|reply| Command::SetBaseline {
            watch,
            baseline,
            reply,
        })
        .await
    }

    /// Run `PRAGMA integrity_check`; `Ok(())` means the database is consistent.
    pub async fn integrity_check(&self) -> Result<(), StorageError> {
        self.call(|reply| Command::IntegrityCheck { reply }).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mailbox_protocol::GithubPr;

    /// If the writer is gone (receiver dropped, i.e. the thread died or the
    /// store was torn down), every call fails cleanly with `WriterGone` rather
    /// than hanging or panicking.
    #[tokio::test]
    async fn calls_fail_with_writer_gone_when_receiver_dropped() {
        let (cmd_tx, cmd_rx) = mpsc::channel(1);
        // Drop the receiver so the channel is closed — models a dead writer.
        drop(cmd_rx);
        let storage = Storage { cmd_tx };

        let topic = GithubPr::new("o", "r", 1).unwrap().topic();
        let err = storage
            .read_events(topic, Cursor::Oldest, None)
            .await
            .unwrap_err();
        assert!(matches!(err, StorageError::WriterGone));
    }
}
