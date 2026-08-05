//! The central per-session wake sentinel (ADR-0008).
//!
//! # What this is, and why it exists
//!
//! The detached watcher (see [`crate::wake::Waiter::watch_sentinel`]) does not wake
//! an idle Claude Code session directly — a background process has no way to. What
//! DOES wake an idle session is Claude Code's `FileChanged` hook: when an external
//! process changes a watched file, the hook fires even on a truly-idle session, and
//! an `asyncRewake` hook that exits 2 wakes it. So the watcher's job on a real-mail
//! kick is to **change a file** — this sentinel — and let the `FileChanged` hook
//! (`mailbox harness wake`) do the waking. This replaces the ADR-0006 exit-2
//! re-arm, whose every re-arm cost a full model turn on a long idle.
//!
//! # Layout
//!
//! ```text
//! <sentinel-root>/by-agent/<encoded-session>/.mailbox-wake
//! ```
//!
//! - `<sentinel-root>` defaults to `~/.mailbox`, overridable by
//!   [`ENV_SENTINEL_ROOT`] (tests point it at a tempdir). `~` is expanded HERE
//!   (`AGENT_MAILBOX_HOME`, then `HOME`) — never left to the shell.
//! - `<encoded-session>` is [`SessionId::encode_filename`], the one shared encoder
//!   the FIFO/lock/pidfile also use, so an unsafe session id can never escape its
//!   directory.
//! - The basename is [`SENTINEL_BASENAME`] = `.mailbox-wake`. It is deliberately
//!   NOT the bare word `wake`: `FileChanged` matches by basename and also watches
//!   the session's cwd recursively, so a plain file literally named `wake` in a
//!   repo would trip the hook. A dotted, mailbox-specific basename makes an
//!   accidental collision in a working tree vanishingly unlikely.
//!
//! # Per-session isolation
//!
//! The basename is FIXED (it is the static `FileChanged` matcher in settings.json).
//! Isolation between sessions comes from the ABSOLUTE path the `SessionStart` hook
//! registers via `watchPaths` — each session registers only its own sentinel — so
//! touching session A's sentinel fires only A's watch. (This per-session isolation
//! is the key property to smoke-test against a real agent; see ADR-0008.)
//!
//! # Payload-free
//!
//! The sentinel holds topic NAMES only — never an event body — exactly like the
//! wake wire it triggers. It records "there is mail on these topics", nothing more.

use std::path::{Path, PathBuf};

use mailbox_protocol::{SessionId, Topic};

use crate::storage::WakeWatermark;

/// The fixed sentinel basename, and the static `FileChanged` matcher — re-exported
/// from `mailbox-protocol` so the bridge (which writes the sentinel) and the
/// harness-installer (which writes the matcher) share ONE definition and can never
/// drift. Dotted and mailbox-specific on purpose (see the module docs).
pub use mailbox_protocol::WAKE_SENTINEL_BASENAME as SENTINEL_BASENAME;

/// The `by-agent/<session>` grandparent directory name.
const BY_AGENT_DIR: &str = "by-agent";

/// The basename of the sibling file recording the newest mail the TURN-BOUNDARY
/// re-trigger (ADR-0012) has already bumped the sentinel for.
///
/// Deliberately NOT a `.mailbox-wake…` name: the `FileChanged` matcher is the
/// sentinel's basename, and this file must never be mistaken for — or accidentally
/// matched alongside — the sentinel itself. It lives in the same per-session
/// directory so `SessionEnd`'s [`Sentinel::remove_dir`] cleans it up for free.
const RETRIGGER_BASENAME: &str = ".mailbox-retriggered";

/// The basename of the sibling file the `FileChanged` wake hook stamps on EVERY
/// run, whatever it decides to do (ADR-0016).
///
/// This is the *ack* half of the wake path. The bridge can see that it bumped a
/// sentinel, but it has never been able to see whether Claude Code noticed —
/// that last hop is another process and, until now, only `harness.log` recorded
/// it. Log archaeology turned out to be wrong in BOTH directions (a quiet session
/// looks identical to an unwatched one, and a session that woke last week looks
/// healthy today), so the hook now leaves a machine-readable mark instead.
///
/// Like [`RETRIGGER_BASENAME`], deliberately NOT a `.mailbox-wake…` name: the
/// `FileChanged` matcher IS the sentinel's basename, so a colliding name would
/// turn this bookkeeping write into a wake trigger and the hook would feed itself
/// forever. It lives in the same per-session directory, so `SessionEnd`'s
/// [`Sentinel::remove_dir`] cleans it up for free.
const HOOK_RAN_BASENAME: &str = ".mailbox-hook-ran";

/// The basenames of the turn-boundary pair, stamped by the `UserPromptSubmit` and
/// `Stop` hooks respectively (ADR-0016).
///
/// Together they answer "is this session mid-turn right now?", which a health probe
/// MUST know before it accuses anyone of being unreachable: a session executing a
/// turn cannot run its `FileChanged` hook, so it is silent for a completely ordinary
/// reason and looks identical to one whose watch is dead.
///
/// Two files rather than one mutable state, because each is written by a different
/// hook process and they must never race over a shared value: the later stamp simply
/// wins the comparison.
const TURN_STARTED_BASENAME: &str = ".mailbox-turn-started";
const TURN_ENDED_BASENAME: &str = ".mailbox-turn-ended";

/// Env var overriding the sentinel root (defaults to `~/.mailbox`). Tests point it
/// at a tempdir so a test can NEVER touch a real `~/.mailbox`.
pub const ENV_SENTINEL_ROOT: &str = "MAILBOX_SENTINEL_ROOT";

/// Env var overriding the home the default sentinel root resolves under, falling
/// back to `HOME` (mirrors the storage/harness home precedence).
const ENV_HOME: &str = "AGENT_MAILBOX_HOME";

/// The sentinel root could not be resolved (no override, and no home to expand
/// `~/.mailbox` under). A value, not a panic — the watcher/hook that hits it logs
/// and degrades rather than crashing.
#[derive(Debug, thiserror::Error)]
pub enum SentinelError {
    /// Neither [`ENV_SENTINEL_ROOT`] nor a usable home (`AGENT_MAILBOX_HOME` /
    /// `HOME`) is set, so there is nowhere to place the sentinel.
    #[error(
        "no sentinel root: set {ENV_SENTINEL_ROOT}, or a home ({ENV_HOME} or HOME) so ~/.mailbox can be resolved"
    )]
    NoRoot,

    /// The sentinel directory could not be created.
    #[error("could not create sentinel directory {path}: {source}")]
    CreateDir {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    /// The sentinel file could not be written.
    #[error("could not write sentinel {path}: {source}")]
    Write {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

/// What the per-session re-trigger record says (ADR-0012).
///
/// `Missing` and `Unreadable` both mean "re-trigger" to the caller — the fail-safe
/// direction, costing at most one redundant wake — but they are kept apart so the log
/// can distinguish a normal first turn from a broken bookkeeping file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetriggerRecord {
    /// No record yet: nothing has been re-triggered for this session.
    Missing,
    /// The record names the newest mail already re-triggered for.
    At(WakeWatermark),
    /// A record exists but could not be read or parsed (permissions, truncation,
    /// garbage). Treated as `Missing` for the decision, and logged as a fault.
    Unreadable,
}

/// Proof that the `FileChanged` wake hook ran, as an opaque stamp.
///
/// Deliberately not a timestamp in the type system: a caller must never be tempted
/// to do arithmetic on it or to trust it as a clock. The ONLY meaningful operation
/// is inequality — "this differs from the stamp I read before I bumped the
/// sentinel", which is exactly what proves a fresh hook run rather than an old one.
/// Nanosecond resolution is what makes back-to-back probes distinguishable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HookRun(u128);

impl HookRun {
    /// Mint a stamp directly. Test-only on purpose: production code obtains a
    /// `HookRun` only by reading what the hook actually wrote, so there is no way to
    /// fabricate proof that a session was reached.
    #[cfg(test)]
    pub fn for_test(stamp: u128) -> Self {
        Self(stamp)
    }
}

/// What the per-session hook-ran record says.
///
/// Three-way for the same reason as [`RetriggerRecord`]: `Never` and `Unreadable`
/// both mean "no usable proof", but one is a brand-new session and the other is a
/// fault, and a health check that cannot tell them apart is how silent deafness
/// stayed invisible in the first place.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HookRunRecord {
    /// The hook has never run for this session (or the record was cleaned up).
    Never,
    /// The hook last ran at this stamp.
    At(HookRun),
    /// A record exists but could not be read or parsed.
    Unreadable,
}

/// Whether a session is executing a turn.
///
/// This exists because "did not answer" has two completely different causes, and
/// conflating them is the single most misleading thing a wake health check can do:
/// a **busy** session is silent because Claude Code is running its turn and will
/// pick up mail at the turn boundary anyway (ADR-0012), whereas a **deaf** idle
/// session is silent because nothing will ever tell it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TurnState {
    /// Not mid-turn: a `FileChanged` now is the session's real chance to be woken,
    /// so silence here is meaningful.
    Idle,
    /// Mid-turn: the last turn started after the last one ended. Silence proves
    /// nothing about the watch.
    Busy,
}

/// What bumping a sentinel actually did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BumpOutcome {
    /// The file was rewritten with its existing content: mtime advanced, topic set
    /// untouched. This is what a health probe wants — a change the watch must see,
    /// with no effect on what the agent will be told is unread.
    Bumped,
    /// There is no sentinel to bump: nothing has ever written one for this session,
    /// so there is no watched file and nothing to prove. Distinct from a failure —
    /// a brand-new session sits here legitimately.
    NothingToBump,
}

/// A resolved per-session sentinel: its directory and the `.mailbox-wake` file.
#[derive(Debug, Clone)]
pub struct Sentinel {
    /// `<root>/by-agent/<encoded-session>` — removed wholesale at `SessionEnd`.
    dir: PathBuf,
    /// `<dir>/.mailbox-wake` — the watched file whose mtime the watcher bumps.
    path: PathBuf,
}

impl Sentinel {
    /// Resolve the sentinel for `session` from the environment.
    ///
    /// The one env-reading edge; [`resolve_root`] is the pure core so the path
    /// scheme is unit-testable without mutating the (process-global) environment.
    pub fn for_session(session: &SessionId) -> Result<Self, SentinelError> {
        Ok(Self::under_root(&root_from_env()?, session))
    }

    /// Build the sentinel paths under an explicit `root` (used by
    /// [`Self::for_session`] and directly by tests).
    pub fn under_root(root: &Path, session: &SessionId) -> Self {
        let dir = root.join(BY_AGENT_DIR).join(session.encode_filename());
        let path = dir.join(SENTINEL_BASENAME);
        Self { dir, path }
    }

    /// The absolute path of the watched `.mailbox-wake` file. This is what the
    /// `SessionStart` hook registers in `watchPaths` and what the watcher bumps.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The per-session directory (`<root>/by-agent/<session>`), removed at
    /// `SessionEnd`.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Write the unread `topics` into the sentinel, bumping its mtime so the
    /// `FileChanged` watcher fires. Topic NAMES only (payload-free); one per line,
    /// so a reader can split them trivially.
    ///
    /// Creates the directory if needed. Truncates and rewrites every call, so the
    /// mtime advances even when the topic set is unchanged — it is the *change* that
    /// wakes, not the content. The watcher calls this **unconditionally on every kick**
    /// (ADR-0008, revised): there is no coalescing, so every real message advances the
    /// mtime and no wake can be lost. Passing an empty slice clears the sentinel to the
    /// empty set (a benign `FileChanged` the wake hook answers with exit 0).
    pub fn write_topics(&self, topics: &[Topic]) -> Result<(), SentinelError> {
        std::fs::create_dir_all(&self.dir).map_err(|source| SentinelError::CreateDir {
            path: self.dir.clone(),
            source,
        })?;
        let mut body = topics
            .iter()
            .map(Topic::as_str)
            .collect::<Vec<_>>()
            .join("\n");
        body.push('\n');
        std::fs::write(&self.path, body).map_err(|source| SentinelError::Write {
            path: self.path.clone(),
            source,
        })
    }

    /// The topic names last written into the sentinel, or an empty vec if it does
    /// not exist / is empty. Best-effort and payload-free — the authoritative
    /// unread check is the read-only store; this is a convenience for diagnostics
    /// and the E2E assertion.
    pub fn read_topics(&self) -> Vec<String> {
        match std::fs::read_to_string(&self.path) {
            Ok(text) => text
                .lines()
                .map(str::trim)
                .filter(|l| !l.is_empty())
                .map(str::to_string)
                .collect(),
            Err(_) => Vec::new(),
        }
    }

    /// The newest mail a TURN-BOUNDARY re-trigger has already bumped this sentinel
    /// for (ADR-0012).
    ///
    /// Three-way, not `Option`, because [`RetriggerRecord::Missing`] and
    /// [`RetriggerRecord::Unreadable`] mean the same thing to the DECISION (re-trigger
    /// — the fail-safe direction) but very different things to a human reading the
    /// log. "No record yet" is the normal first turn; "I cannot read my own
    /// bookkeeping, every turn" is a fault worth seeing. Collapsing them to a silent
    /// `None` is how the previous generation of lost-wake bugs stayed invisible
    /// (ADR-0008/0009), so the caller is given enough to say which it was.
    pub fn last_retriggered(&self) -> RetriggerRecord {
        match std::fs::read_to_string(self.retrigger_path()) {
            Ok(text) => match WakeWatermark::parse(&text) {
                Some(watermark) => RetriggerRecord::At(watermark),
                // Present but not a watermark: truncated or garbled, i.e. a fault.
                None => RetriggerRecord::Unreadable,
            },
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => RetriggerRecord::Missing,
            Err(_) => RetriggerRecord::Unreadable,
        }
    }

    /// Record that the sentinel has been re-triggered for mail up to `watermark`.
    ///
    /// Call this only AFTER the sentinel bump it describes: a crash between the two
    /// then costs a duplicate re-trigger (harmless — the wake hook re-checks the
    /// store), whereas recording first would lose the wake outright.
    pub fn record_retriggered(&self, watermark: WakeWatermark) -> Result<(), SentinelError> {
        std::fs::create_dir_all(&self.dir).map_err(|source| SentinelError::CreateDir {
            path: self.dir.clone(),
            source,
        })?;
        let path = self.retrigger_path();
        std::fs::write(&path, watermark.get().to_string())
            .map_err(|source| SentinelError::Write { path, source })
    }

    /// `<dir>/.mailbox-retriggered` — the re-trigger record's path.
    fn retrigger_path(&self) -> PathBuf {
        self.dir.join(RETRIGGER_BASENAME)
    }

    /// Stamp that the `FileChanged` wake hook ran (ADR-0016).
    ///
    /// Called on EVERY exit path of the hook, before it decides anything: the value
    /// of this record is that it proves Claude Code delivered the file-change event
    /// at all. Whether the hook then woke the agent, found nothing unread, or could
    /// not reach the store is a separate question the log already answers.
    ///
    /// Writes into the per-session directory, which is created if needed — the hook
    /// can legitimately run before any watcher has written a sentinel.
    pub fn record_hook_ran(&self, at: std::time::SystemTime) -> Result<(), SentinelError> {
        std::fs::create_dir_all(&self.dir).map_err(|source| SentinelError::CreateDir {
            path: self.dir.clone(),
            source,
        })?;
        let stamp = at
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let path = self.hook_ran_path();
        std::fs::write(&path, stamp.to_string())
            .map_err(|source| SentinelError::Write { path, source })
    }

    /// The stamp of the last `FileChanged` hook run, if any.
    pub fn hook_ran(&self) -> HookRunRecord {
        match std::fs::read_to_string(self.hook_ran_path()) {
            Ok(text) => match text.trim().parse::<u128>() {
                Ok(stamp) => HookRunRecord::At(HookRun(stamp)),
                Err(_) => HookRunRecord::Unreadable,
            },
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => HookRunRecord::Never,
            Err(_) => HookRunRecord::Unreadable,
        }
    }

    /// Rewrite the sentinel with the content it already has, advancing its mtime
    /// without changing what it says (ADR-0016).
    ///
    /// This is the health probe's bump. It must be content-preserving: the sentinel
    /// is the agent-facing statement of which topics have mail, and a probe that
    /// rewrote it with a freshly-computed set would be a probe that could CHANGE
    /// what the agent is told — an observation that alters the thing observed. The
    /// watcher's [`Self::write_topics`] is the only writer allowed to decide content.
    ///
    /// Returns [`BumpOutcome::NothingToBump`] rather than creating the file when it
    /// is absent: creating it would be a different event from modifying it (and on
    /// some watch implementations, one the watch would not even see), so a probe
    /// must not silently turn "never armed" into a bump that proves nothing.
    pub fn bump_in_place(&self) -> Result<BumpOutcome, SentinelError> {
        let body = match std::fs::read(&self.path) {
            Ok(body) => body,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                return Ok(BumpOutcome::NothingToBump);
            }
            Err(source) => {
                return Err(SentinelError::Write {
                    path: self.path.clone(),
                    source,
                });
            }
        };
        std::fs::write(&self.path, body)
            .map(|()| BumpOutcome::Bumped)
            .map_err(|source| SentinelError::Write {
                path: self.path.clone(),
                source,
            })
    }

    /// `<dir>/.mailbox-hook-ran` — the wake hook's ack record.
    fn hook_ran_path(&self) -> PathBuf {
        self.dir.join(HOOK_RAN_BASENAME)
    }

    /// Stamp that a turn has STARTED (the `UserPromptSubmit` hook).
    pub fn record_turn_started(&self, at: std::time::SystemTime) -> Result<(), SentinelError> {
        self.stamp(TURN_STARTED_BASENAME, at)
    }

    /// Stamp that a turn has ENDED (the `Stop` hook, which already fires at every
    /// turn boundary).
    pub fn record_turn_ended(&self, at: std::time::SystemTime) -> Result<(), SentinelError> {
        self.stamp(TURN_ENDED_BASENAME, at)
    }

    /// Whether this session is mid-turn.
    ///
    /// A turn is open when its start is newer than the last end. Both stamps missing
    /// means a session that has never taken a turn — it is sitting at the prompt,
    /// which is [`TurnState::Idle`] and genuinely probeable.
    ///
    /// Note what is deliberately NOT counted as a turn start: a wake. A turn that
    /// begins because the `FileChanged` hook exited 2 has, by definition, already
    /// written its ack before the turn opened — so the probe has its answer and does
    /// not need this signal at all.
    pub fn turn_state(&self) -> TurnState {
        let started = self.read_stamp(TURN_STARTED_BASENAME);
        let ended = self.read_stamp(TURN_ENDED_BASENAME);
        match (started, ended) {
            (Some(started), Some(ended)) if started > ended => TurnState::Busy,
            (Some(_), None) => TurnState::Busy,
            _ => TurnState::Idle,
        }
    }

    /// Write a nanosecond stamp into `basename`, creating the session dir if needed.
    fn stamp(&self, basename: &str, at: std::time::SystemTime) -> Result<(), SentinelError> {
        std::fs::create_dir_all(&self.dir).map_err(|source| SentinelError::CreateDir {
            path: self.dir.clone(),
            source,
        })?;
        let nanos = at
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let path = self.dir.join(basename);
        std::fs::write(&path, nanos.to_string())
            .map_err(|source| SentinelError::Write { path, source })
    }

    /// Read a nanosecond stamp, treating absent or corrupt as "no stamp".
    fn read_stamp(&self, basename: &str) -> Option<u128> {
        std::fs::read_to_string(self.dir.join(basename))
            .ok()?
            .trim()
            .parse::<u128>()
            .ok()
    }

    /// Remove the session's sentinel directory (and the file within), idempotently
    /// — a `SessionEnd` that races a never-created sentinel is normal, not an
    /// error.
    pub fn remove_dir(&self) -> std::io::Result<()> {
        match std::fs::remove_dir_all(&self.dir) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(err),
        }
    }
}

/// The sentinel root, resolved from the environment.
///
/// The `serve` daemon resolves this ONCE at startup and hands it to its
/// [`crate::wake::Waker`], rather than re-reading three environment variables on
/// every publish. Hooks, which are one-shot processes, go through
/// [`Sentinel::for_session`] instead.
pub fn root_from_env() -> Result<PathBuf, SentinelError> {
    resolve_root(
        env_nonempty(ENV_SENTINEL_ROOT),
        env_nonempty(ENV_HOME),
        env_nonempty("HOME"),
    )
    .ok_or(SentinelError::NoRoot)
}

/// The pure sentinel-root rule: [`ENV_SENTINEL_ROOT`] wins; else `~/.mailbox` with
/// the home from `AGENT_MAILBOX_HOME` then `HOME`; else `None` (never a guess).
fn resolve_root(
    env_root: Option<String>,
    env_home: Option<String>,
    home: Option<String>,
) -> Option<PathBuf> {
    if let Some(root) = env_root {
        return Some(PathBuf::from(root));
    }
    let home = env_home.or(home)?;
    Some(PathBuf::from(home).join(".mailbox"))
}

/// A non-empty, whitespace-trimmed environment variable, else `None`.
fn env_nonempty(key: &str) -> Option<String> {
    let value = std::env::var(key).ok()?;
    let trimmed = value.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sentinel_root_prefers_the_explicit_override() {
        assert_eq!(
            resolve_root(Some("/tmp/mbx".into()), Some("/home/u".into()), None),
            Some(PathBuf::from("/tmp/mbx"))
        );
    }

    #[test]
    fn sentinel_root_falls_back_to_home_dot_mailbox() {
        assert_eq!(
            resolve_root(None, Some("/home/u".into()), None),
            Some(PathBuf::from("/home/u/.mailbox"))
        );
        assert_eq!(
            resolve_root(None, None, Some("/root".into())),
            Some(PathBuf::from("/root/.mailbox"))
        );
    }

    #[test]
    fn sentinel_root_is_none_without_an_override_or_home() {
        assert_eq!(resolve_root(None, None, None), None);
    }

    #[test]
    fn the_path_scheme_encodes_the_session_and_uses_the_dotted_basename() {
        let s = Sentinel::under_root(Path::new("/r"), &SessionId::new("a/b"));
        // The session id is filename-encoded, so a slash cannot escape the dir.
        assert_eq!(s.dir(), Path::new("/r/by-agent/a%2Fb"));
        assert_eq!(s.path(), Path::new("/r/by-agent/a%2Fb/.mailbox-wake"));
        assert!(
            s.path().file_name().unwrap().to_str().unwrap() == SENTINEL_BASENAME,
            "the matcher basename must be the dotted, collision-resistant name"
        );
    }

    #[test]
    fn write_then_read_round_trips_topic_names_and_bumps_mtime() {
        let dir = tempfile::TempDir::new().unwrap();
        let s = Sentinel::under_root(dir.path(), &SessionId::new("s1"));
        let t1 = Topic::parse("agent.s1").unwrap();
        let t2 = Topic::parse("github.pr.o/r#1").unwrap();

        s.write_topics(std::slice::from_ref(&t1)).unwrap();
        assert_eq!(s.read_topics(), vec!["agent.s1".to_string()]);

        // A second write with a different set advances mtime (it is the change that
        // wakes) and reflects the new topics.
        let before = std::fs::metadata(s.path()).unwrap().modified().unwrap();
        std::thread::sleep(std::time::Duration::from_millis(10));
        s.write_topics(&[t1, t2]).unwrap();
        let after = std::fs::metadata(s.path()).unwrap().modified().unwrap();
        assert!(after >= before);
        assert_eq!(s.read_topics(), vec!["agent.s1", "github.pr.o/r#1"]);

        // The body carries topic NAMES only — no braces, no event body.
        let raw = std::fs::read_to_string(s.path()).unwrap();
        assert!(!raw.contains('{'), "sentinel must be payload-free: {raw:?}");
    }

    #[test]
    fn write_topics_always_rewrites_and_an_empty_slice_clears_the_sentinel() {
        let dir = tempfile::TempDir::new().unwrap();
        let s = Sentinel::under_root(dir.path(), &SessionId::new("s1"));
        let a = Topic::parse("t.a").unwrap();
        let mtime = |s: &Sentinel| std::fs::metadata(s.path()).unwrap().modified().unwrap();

        s.write_topics(std::slice::from_ref(&a)).unwrap();
        let after_first = mtime(&s);
        assert_eq!(s.read_topics(), vec!["t.a".to_string()]);

        // The SAME set written again STILL rewrites (unconditional — no coalescing), so
        // the mtime advances: this is what guarantees every real message re-fires the
        // FileChanged wake and no message can be silently dropped.
        std::thread::sleep(std::time::Duration::from_millis(10));
        s.write_topics(std::slice::from_ref(&a)).unwrap();
        assert!(
            mtime(&s) > after_first,
            "an unconditional write must always bump the mtime, even for an identical set"
        );

        // An empty slice clears the sentinel to the empty set (a benign FileChanged the
        // wake hook answers with exit 0).
        s.write_topics(&[]).unwrap();
        assert!(s.read_topics().is_empty());
    }

    #[test]
    fn the_retrigger_record_round_trips_and_reports_absent_apart_from_corrupt() {
        let dir = tempfile::TempDir::new().unwrap();
        let s = Sentinel::under_root(dir.path(), &SessionId::new("s1"));

        // Nothing recorded yet — the normal first turn.
        assert_eq!(s.last_retriggered(), RetriggerRecord::Missing);

        let w = crate::storage::WakeWatermark::parse("7").unwrap();
        s.record_retriggered(w).unwrap();
        assert_eq!(s.last_retriggered(), RetriggerRecord::At(w));

        // A corrupt record is reported as its OWN state, not silently as "missing":
        // both re-trigger (fail-safe), but only one of them is a fault worth logging.
        std::fs::write(s.dir().join(".mailbox-retriggered"), "garbage").unwrap();
        assert_eq!(s.last_retriggered(), RetriggerRecord::Unreadable);

        // An out-of-range row id is corruption too — no store ever minted it.
        std::fs::write(s.dir().join(".mailbox-retriggered"), "0").unwrap();
        assert_eq!(s.last_retriggered(), RetriggerRecord::Unreadable);
    }

    /// The record must never be mistaken for the sentinel: the `FileChanged` matcher
    /// IS the sentinel's basename, so a name that collided with it would turn a
    /// bookkeeping write into a wake trigger.
    #[test]
    fn the_retrigger_record_is_a_distinct_file_from_the_sentinel() {
        let dir = tempfile::TempDir::new().unwrap();
        let s = Sentinel::under_root(dir.path(), &SessionId::new("s1"));
        s.record_retriggered(crate::storage::WakeWatermark::parse("1").unwrap())
            .unwrap();

        assert_ne!(s.retrigger_path(), s.path());
        assert_ne!(
            s.retrigger_path().file_name().unwrap().to_str().unwrap(),
            SENTINEL_BASENAME
        );
        // It lives inside the per-session dir, so SessionEnd's teardown removes it.
        assert_eq!(s.retrigger_path().parent(), Some(s.dir()));
        s.remove_dir().unwrap();
        assert!(!s.retrigger_path().exists());
    }

    #[test]
    fn the_hook_ran_record_round_trips_and_distinguishes_never_from_corrupt() {
        let dir = tempfile::TempDir::new().unwrap();
        let s = Sentinel::under_root(dir.path(), &SessionId::new("s1"));

        assert_eq!(s.hook_ran(), HookRunRecord::Never);

        let at = std::time::UNIX_EPOCH + std::time::Duration::from_nanos(1_234_567_890);
        s.record_hook_ran(at).unwrap();
        let HookRunRecord::At(first) = s.hook_ran() else {
            panic!("expected a stamp");
        };

        // A later run yields a DIFFERENT stamp — that difference is the only thing a
        // health probe is allowed to conclude anything from.
        s.record_hook_ran(at + std::time::Duration::from_nanos(1))
            .unwrap();
        let HookRunRecord::At(second) = s.hook_ran() else {
            panic!("expected a stamp");
        };
        assert_ne!(first, second);

        std::fs::write(s.hook_ran_path(), "garbage").unwrap();
        assert_eq!(s.hook_ran(), HookRunRecord::Unreadable);
    }

    /// The hook's ack must never be mistaken for the sentinel: the `FileChanged`
    /// matcher IS the sentinel's basename, so a colliding name would make the hook
    /// re-trigger itself on every run — an infinite wake loop.
    #[test]
    fn the_hook_ran_record_is_a_distinct_file_from_the_sentinel() {
        let dir = tempfile::TempDir::new().unwrap();
        let s = Sentinel::under_root(dir.path(), &SessionId::new("s1"));
        s.record_hook_ran(std::time::SystemTime::now()).unwrap();

        assert_ne!(s.hook_ran_path(), s.path());
        assert_ne!(
            s.hook_ran_path().file_name().unwrap().to_str().unwrap(),
            SENTINEL_BASENAME
        );
        assert_ne!(s.hook_ran_path(), s.retrigger_path());
        // Inside the per-session dir, so SessionEnd's teardown removes it.
        assert_eq!(s.hook_ran_path().parent(), Some(s.dir()));
        s.remove_dir().unwrap();
        assert!(!s.hook_ran_path().exists());
    }

    #[test]
    fn bumping_in_place_advances_the_mtime_without_changing_the_topics() {
        let dir = tempfile::TempDir::new().unwrap();
        let s = Sentinel::under_root(dir.path(), &SessionId::new("s1"));
        let topics = [
            Topic::parse("agent.s1").unwrap(),
            Topic::parse("github.pr.o/r#1").unwrap(),
        ];
        s.write_topics(&topics).unwrap();
        let before_mtime = std::fs::metadata(s.path()).unwrap().modified().unwrap();
        let before_body = std::fs::read(s.path()).unwrap();

        std::thread::sleep(std::time::Duration::from_millis(10));
        assert_eq!(s.bump_in_place().unwrap(), BumpOutcome::Bumped);

        assert!(
            std::fs::metadata(s.path()).unwrap().modified().unwrap() > before_mtime,
            "a bump must advance the mtime or the watch has nothing to notice"
        );
        assert_eq!(
            std::fs::read(s.path()).unwrap(),
            before_body,
            "a bump must not change what the sentinel says"
        );
    }

    /// A probe must not conjure a sentinel that no watcher has ever written: file
    /// CREATION is a different event from modification, and reporting it as a bump
    /// would claim to have tested something that was never tested.
    #[test]
    fn bumping_a_sentinel_that_does_not_exist_reports_nothing_to_bump() {
        let dir = tempfile::TempDir::new().unwrap();
        let s = Sentinel::under_root(dir.path(), &SessionId::new("s1"));
        assert_eq!(s.bump_in_place().unwrap(), BumpOutcome::NothingToBump);
        assert!(!s.path().exists(), "the probe must not create the sentinel");
    }

    /// Two sessions get DIFFERENT sentinel directories under the shared root. That
    /// separation is the whole of per-session isolation: the `FileChanged` matcher is
    /// the shared basename, so the only thing keeping one session's bump from firing
    /// another's hook is that each registers its own absolute path and nothing wider.
    #[test]
    fn each_session_gets_its_own_sentinel_directory() {
        let a = Sentinel::under_root(Path::new("/r"), &SessionId::new("session-a"));
        let b = Sentinel::under_root(Path::new("/r"), &SessionId::new("session-b"));

        assert_ne!(a.dir(), b.dir());
        assert_ne!(a.path(), b.path());
        assert_eq!(a.dir().parent(), b.dir().parent());
    }

    #[test]
    fn remove_dir_is_idempotent() {
        let dir = tempfile::TempDir::new().unwrap();
        let s = Sentinel::under_root(dir.path(), &SessionId::new("s1"));
        // Removing a never-created sentinel is a clean no-op.
        s.remove_dir().unwrap();
        s.write_topics(&[Topic::parse("agent.s1").unwrap()])
            .unwrap();
        assert!(s.dir().exists());
        s.remove_dir().unwrap();
        assert!(!s.dir().exists());
        // And again.
        s.remove_dir().unwrap();
    }
}
