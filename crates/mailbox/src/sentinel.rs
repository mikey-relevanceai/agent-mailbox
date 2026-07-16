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

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use mailbox_protocol::{SessionId, Topic};

/// The fixed sentinel basename, and the static `FileChanged` matcher — re-exported
/// from `mailbox-protocol` so the bridge (which writes the sentinel) and the
/// harness-installer (which writes the matcher) share ONE definition and can never
/// drift. Dotted and mailbox-specific on purpose (see the module docs).
pub use mailbox_protocol::WAKE_SENTINEL_BASENAME as SENTINEL_BASENAME;

/// The `by-agent/<session>` grandparent directory name.
const BY_AGENT_DIR: &str = "by-agent";

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

/// Whether a coalesced [`Sentinel::sync_topics`] actually rewrote the sentinel.
///
/// A named two-state result rather than a bare `bool`, so the watcher's log line
/// (and a test) can say plainly whether this kick bumped the mtime — and therefore
/// whether a `FileChanged` wake will follow — or was coalesced away.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SentinelWrite {
    /// The unread set changed, so the sentinel was rewritten and its mtime bumped;
    /// a `FileChanged` wake will follow.
    Bumped,
    /// The unread set was identical to what the sentinel already held, so nothing
    /// was written — no redundant `FileChanged`, no extra wake for mail already
    /// surfaced (the coalescing guard, ADR-0008).
    Unchanged,
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
        let root = resolve_root(
            env_nonempty(ENV_SENTINEL_ROOT),
            env_nonempty(ENV_HOME),
            env_nonempty("HOME"),
        )
        .ok_or(SentinelError::NoRoot)?;
        Ok(Self::under_root(&root, session))
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
    /// wakes, not the content. Callers that must not fire a redundant wake for an
    /// already-surfaced unread set use [`Self::sync_topics`], which compares first;
    /// this is the low-level primitive it (and the tests) build on.
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

    /// Coalesced write: make the sentinel hold exactly `topics`, but ONLY when that
    /// set differs from what it currently holds. Returns [`SentinelWrite::Bumped`] if
    /// it rewrote (mtime advanced, a `FileChanged` wake will follow) or
    /// [`SentinelWrite::Unchanged`] if the set was identical and nothing was written.
    ///
    /// This is the coalescing guard (ADR-0008): `write_topics` truncates and rewrites
    /// on EVERY call, so a burst of messages on an ALREADY-unread topic — or a
    /// create+modify pair from a single write — fired several `FileChanged` events and
    /// therefore several exit-2 wakes for the *same* unread state, before the agent had
    /// read a thing. Comparing the unread SET first collapses those into one bump: a
    /// second message on a topic that is already listed changes nothing, so it is
    /// skipped; a NEW topic becoming unread does change the set, so it bumps.
    ///
    /// The comparison is by SET (order-independent), so it never depends on the order
    /// `topics_with_unread` happens to return. The current content is read best-effort
    /// (an unreadable/absent sentinel reads as the empty set, so a non-empty `topics`
    /// still bumps) — the sentinel is only a trigger, and the wake hook re-checks the
    /// store, so a stale read here can at worst cause one extra (harmless) bump.
    pub fn sync_topics(&self, topics: &[Topic]) -> Result<SentinelWrite, SentinelError> {
        let desired: BTreeSet<&str> = topics.iter().map(Topic::as_str).collect();
        let current = self.read_topics();
        let current: BTreeSet<&str> = current.iter().map(String::as_str).collect();
        if desired == current {
            return Ok(SentinelWrite::Unchanged);
        }
        self.write_topics(topics)?;
        Ok(SentinelWrite::Bumped)
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
    fn sync_topics_coalesces_an_unchanged_set_but_bumps_a_changed_one() {
        let dir = tempfile::TempDir::new().unwrap();
        let s = Sentinel::under_root(dir.path(), &SessionId::new("s1"));
        let a = Topic::parse("t.a").unwrap();
        let b = Topic::parse("t.b").unwrap();

        // First sync of a non-empty set writes (nothing was there).
        assert_eq!(
            s.sync_topics(std::slice::from_ref(&a)).unwrap(),
            SentinelWrite::Bumped
        );
        let mtime = |s: &Sentinel| std::fs::metadata(s.path()).unwrap().modified().unwrap();
        let after_first = mtime(&s);

        // Re-syncing the SAME set is coalesced away — no write, so the mtime is frozen
        // (this is what stops a second message on an already-unread topic re-waking).
        std::thread::sleep(std::time::Duration::from_millis(10));
        assert_eq!(
            s.sync_topics(std::slice::from_ref(&a)).unwrap(),
            SentinelWrite::Unchanged
        );
        assert_eq!(
            mtime(&s),
            after_first,
            "an unchanged set must not bump the mtime"
        );

        // Order does not matter: [a] vs [a] compared as a set, and a NEW topic bumps.
        std::thread::sleep(std::time::Duration::from_millis(10));
        assert_eq!(
            s.sync_topics(&[b.clone(), a.clone()]).unwrap(),
            SentinelWrite::Bumped
        );
        assert!(mtime(&s) > after_first, "a changed set must bump the mtime");
        // `write_topics` preserves the given order (here [b, a]); the SET is what the
        // coalescing compares, so a reordering of the same set is still Unchanged.
        assert_eq!(s.read_topics(), vec!["t.b", "t.a"]);

        // Reordering the same set is still Unchanged.
        assert_eq!(s.sync_topics(&[a, b]).unwrap(), SentinelWrite::Unchanged);

        // Shrinking to the empty set is a change (used by the Stop re-sync to reset).
        assert_eq!(s.sync_topics(&[]).unwrap(), SentinelWrite::Bumped);
        assert!(s.read_topics().is_empty());
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
