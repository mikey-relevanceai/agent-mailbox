//! Claude Code's own session registry: who is running, and where to reach them
//! (ADR-0020).
//!
//! # What this is
//!
//! Since 2.1.224 every Claude Code session writes a small JSON file describing
//! itself, and — when the `agents_cross_session_inbox` gate is on for that session —
//! binds a Unix socket that anything running as the same user can deliver a message
//! on:
//!
//! ```text
//! ~/.claude/sessions/<pid>.json
//! {"pid":40741,"sessionId":"700a3bf5-…","cwd":"/…/arg","status":"idle",
//!  "messagingSocketPath":"/tmp/cc-socks/40741.sock","name":"arg-16", …}
//! ```
//!
//! That is the lookup the wake path needs: **session id → inbox socket**. It also
//! publishes, first-hand, what [`crate::doctor::live_claude_sessions`] reconstructs
//! by scraping `ps` for `--session-id` / `--resume`.
//!
//! # This file is not evidence of liveness
//!
//! A registry entry outlives the process that wrote it, exactly as ADR-0017 found
//! for every other per-session artefact — this machine had nineteen entries for
//! processes spanning five days. So this module answers "where would I reach this
//! session?", never "is this session alive?". Liveness stays with the process table.
//!
//! Two guards follow from that, and both are load-bearing:
//!
//! - A session id can appear more than once (a resume is a fresh pid reusing the id).
//!   We take the entry with the newest `updatedAt`.
//! - We only report a socket that still **exists on disk**. A stale entry naming a
//!   deleted socket must read as "no socket" so the caller falls back to the
//!   sentinel, rather than as a delivery target that will always fail.
//!
//! # Absence is normal, not an error
//!
//! Most sessions have no `messagingSocketPath` at all: the gate is a gradual
//! rollout, cannot be turned on from outside Claude Code, and differs between
//! same-version sessions on one machine. A missing socket is the ordinary case and
//! means "use the fallback channel", never "something is broken".

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::Deserialize;
use tracing::{debug, warn};

use mailbox_protocol::SessionId;

/// Env var pointing at the sessions directory outright. Tests set it at a tempdir
/// so a test can never read — or depend on — the developer's real `~/.claude`.
pub const ENV_SESSIONS_DIR: &str = "MAILBOX_CLAUDE_SESSIONS_DIR";

/// Claude Code's own override for the config directory that holds `sessions/`.
/// Honoured so a relocated Claude Code install is found without extra configuration.
const ENV_CLAUDE_CONFIG_DIR: &str = "CLAUDE_CONFIG_DIR";

/// The directory under the Claude config dir holding one JSON file per session.
const SESSIONS_SUBDIR: &str = "sessions";

/// The default config directory name under the user's home.
const CLAUDE_DIR: &str = ".claude";

/// One session as Claude Code describes itself on disk.
///
/// Deliberately a **subset**: we deserialise only the fields the wake path and
/// `doctor` actually use. Serde ignores the rest, so Claude Code adding fields —
/// which it does between patch releases — cannot break the read.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClaudeSession {
    /// The session id the mailbox keys everything else by.
    pub session_id: SessionId,
    /// The OS process id, for cross-checking against the process table.
    pub pid: u32,
    /// The session's inbox socket, present only when the gate bound one.
    #[serde(default)]
    pub messaging_socket_path: Option<PathBuf>,
    /// Claude Code's display name for the session. Derived and MUTABLE (it tracks
    /// the conversation), so it is fit for logs and never for addressing.
    #[serde(default)]
    pub name: Option<String>,
    /// The session's working directory.
    #[serde(default)]
    pub cwd: Option<PathBuf>,
    /// Claude Code's own status word (`idle`, `busy`, `waiting`, …).
    ///
    /// Reported for operators, never branched on: it is too coarse to be a delivery
    /// signal — a short turn can begin and end without it ever leaving `idle`
    /// (observed while proving out ADR-0020).
    #[serde(default)]
    pub status: Option<String>,
    /// Milliseconds since the epoch, as Claude Code last refreshed this entry. Used
    /// only to pick between duplicate entries for one session id.
    #[serde(default)]
    pub updated_at: Option<i64>,
}

impl ClaudeSession {
    /// This session's inbox socket, if it bound one **and** the socket is still
    /// there.
    ///
    /// The existence check is what stops a stale registry file — one whose process
    /// died without cleaning up — from being reported as a live delivery target.
    pub fn inbox_socket(&self) -> Option<&Path> {
        let path = self.messaging_socket_path.as_deref()?;
        path.exists().then_some(path)
    }

    /// How recently Claude Code refreshed this entry, for choosing between
    /// duplicates. A missing timestamp sorts oldest.
    fn freshness(&self) -> i64 {
        self.updated_at.unwrap_or(i64::MIN)
    }
}

/// Why the registry directory could not be located.
///
/// Only *resolution* fails loudly. A directory that is missing, unreadable, or full
/// of junk yields an EMPTY registry rather than an error: on a machine without
/// Claude Code, or with an older one, "no sessions are reachable by socket" is the
/// correct answer and the caller's fallback handles it.
#[derive(Debug, thiserror::Error)]
pub enum RegistryError {
    /// Neither an explicit directory nor a home to resolve the default under.
    #[error(
        "no Claude Code sessions directory: set {ENV_SESSIONS_DIR}, {ENV_CLAUDE_CONFIG_DIR}, or HOME"
    )]
    NoDirectory,
}

/// A point-in-time read of Claude Code's session registry.
///
/// Cheap to build and deliberately **not** cached: sessions start, stop and resume
/// constantly, and a wake delivered to a socket that closed a minute ago is a lost
/// wake. The daemon re-reads per publish.
#[derive(Debug, Default, Clone)]
pub struct ClaudeRegistry {
    by_session: BTreeMap<String, ClaudeSession>,
    /// Whether the directory was actually read.
    ///
    /// **Absence of evidence is not evidence of absence**, and conflating the two is
    /// how a sweeper reaps live agents' watches (ADR-0009). An unreadable directory
    /// must not be indistinguishable from "no sessions are running", because the two
    /// demand opposite responses: report nothing reachable, versus stop every watch
    /// nobody can be proven to want.
    readable: bool,
}

impl ClaudeRegistry {
    /// Read the registry from the resolved sessions directory.
    pub fn open() -> Result<Self, RegistryError> {
        Ok(Self::read_dir(&sessions_dir_from_env()?))
    }

    /// Read every session file in `dir`, keeping the freshest entry per session id.
    ///
    /// Tolerant on purpose: an unreadable directory, an unreadable file, or a file
    /// that does not parse are all skipped with a `debug` line. This runs on the
    /// publish path, where the event is already durable — it must never be the thing
    /// that fails a publish.
    pub fn read_dir(dir: &Path) -> Self {
        let mut by_session: BTreeMap<String, ClaudeSession> = BTreeMap::new();

        let entries = match std::fs::read_dir(dir) {
            Ok(entries) => entries,
            Err(error) => {
                // Not readable is NOT the same as empty; see [`ClaudeRegistry::readable`].
                warn!(
                    dir = %dir.display(),
                    %error,
                    "could not read Claude Code's sessions directory; no session can be \
                     proven live or reachable from it"
                );
                return Self {
                    by_session,
                    readable: false,
                };
            }
        };

        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().is_none_or(|ext| ext != "json") {
                continue;
            }
            let raw = match std::fs::read_to_string(&path) {
                Ok(raw) => raw,
                Err(error) => {
                    // Logged rather than skipped in silence: a session that vanishes
                    // from the registry with no trace is the invisible failure this
                    // project keeps paying for.
                    debug!(
                        file = %path.display(),
                        %error,
                        "skipped an unreadable Claude Code session file"
                    );
                    continue;
                }
            };
            let session: ClaudeSession = match serde_json::from_str(&raw) {
                Ok(session) => session,
                Err(error) => {
                    // Not a warning: a half-written file is normal for a directory
                    // another process is actively writing.
                    debug!(
                        file = %path.display(),
                        %error,
                        "skipped an unparsable Claude Code session file"
                    );
                    continue;
                }
            };

            // A resume reuses the session id under a new pid, leaving the old entry
            // behind. Newest wins; the loser is simply dropped.
            match by_session.get(session.session_id.as_str()) {
                Some(existing) if existing.freshness() >= session.freshness() => {}
                _ => {
                    by_session.insert(session.session_id.as_str().to_string(), session);
                }
            }
        }

        Self {
            by_session,
            readable: true,
        }
    }

    /// Whether the sessions directory could be read at all.
    ///
    /// `false` means **unknown**, not empty: every answer this registry gives is the
    /// absence of information rather than information. Callers that would act
    /// destructively on "nothing is live" — the startup reconcile and the TTL sweep —
    /// MUST check this and skip instead.
    pub fn is_readable(&self) -> bool {
        self.readable
    }

    /// Where to deliver to `session`, or `None` if it has no usable socket.
    pub fn inbox_socket(&self, session: &SessionId) -> Option<&Path> {
        self.by_session.get(session.as_str())?.inbox_socket()
    }

    /// The entry for `session`, if Claude Code has registered one.
    pub fn get(&self, session: &SessionId) -> Option<&ClaudeSession> {
        self.by_session.get(session.as_str())
    }

    /// Every registered session, freshest entry per id.
    pub fn sessions(&self) -> impl Iterator<Item = &ClaudeSession> {
        self.by_session.values()
    }

    /// How many sessions are registered — for a log line that distinguishes "no
    /// Claude Code here" from "Claude Code here, but nothing has a socket".
    pub fn len(&self) -> usize {
        self.by_session.len()
    }

    /// Whether the registry is empty.
    pub fn is_empty(&self) -> bool {
        self.by_session.is_empty()
    }
}

/// Resolve the sessions directory: explicit override, then Claude Code's own config
/// dir, then `~/.claude/sessions`.
///
/// `~` is expanded here, never left to a shell — the daemon has no shell.
pub fn sessions_dir_from_env() -> Result<PathBuf, RegistryError> {
    if let Some(dir) = std::env::var_os(ENV_SESSIONS_DIR) {
        return Ok(PathBuf::from(dir));
    }
    if let Some(config) = std::env::var_os(ENV_CLAUDE_CONFIG_DIR) {
        return Ok(PathBuf::from(config).join(SESSIONS_SUBDIR));
    }
    let home = std::env::var_os("HOME").ok_or(RegistryError::NoDirectory)?;
    Ok(PathBuf::from(home).join(CLAUDE_DIR).join(SESSIONS_SUBDIR))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Write one registry file, optionally naming a socket path.
    fn write_entry(dir: &Path, pid: u32, session: &str, socket: Option<&Path>, updated_at: i64) {
        let socket_field = match socket {
            Some(path) => format!(r#","messagingSocketPath":"{}""#, path.display()),
            None => String::new(),
        };
        let body = format!(
            r#"{{"pid":{pid},"sessionId":"{session}","cwd":"/tmp","status":"idle",
                "name":"probe","updatedAt":{updated_at},"peerProtocol":1{socket_field}}}"#
        );
        std::fs::write(dir.join(format!("{pid}.json")), body).unwrap();
    }

    #[test]
    fn finds_the_inbox_socket_for_a_registered_session() {
        let dir = tempfile::TempDir::new().unwrap();
        let socket = dir.path().join("live.sock");
        std::fs::write(&socket, b"").unwrap();
        write_entry(dir.path(), 100, "sess-a", Some(&socket), 10);

        let registry = ClaudeRegistry::read_dir(dir.path());

        assert_eq!(
            registry.inbox_socket(&SessionId::new("sess-a")),
            Some(socket.as_path())
        );
    }

    /// The gate leaves most sessions with no socket at all. That is the ordinary
    /// case and must read as "fall back", not as an error or an empty registry.
    #[test]
    fn a_session_without_a_socket_is_registered_but_unreachable() {
        let dir = tempfile::TempDir::new().unwrap();
        write_entry(dir.path(), 101, "sess-b", None, 10);

        let registry = ClaudeRegistry::read_dir(dir.path());

        assert_eq!(registry.len(), 1, "the session is still registered");
        assert!(
            registry.inbox_socket(&SessionId::new("sess-b")).is_none(),
            "no socket means no peer delivery, so the caller must fall back"
        );
    }

    /// A registry file outlives its process (ADR-0017's lesson, restated for this
    /// artefact). An entry naming a socket that no longer exists must NOT be offered
    /// as a delivery target, or every publish to that session burns a failed connect
    /// instead of writing the sentinel.
    #[test]
    fn a_stale_entry_naming_a_deleted_socket_reads_as_unreachable() {
        let dir = tempfile::TempDir::new().unwrap();
        write_entry(
            dir.path(),
            102,
            "sess-c",
            Some(&dir.path().join("gone.sock")),
            10,
        );

        let registry = ClaudeRegistry::read_dir(dir.path());

        assert!(
            registry.inbox_socket(&SessionId::new("sess-c")).is_none(),
            "a socket path that does not exist is not a delivery target"
        );
    }

    /// A resume reuses the session id under a fresh pid, leaving the old file behind.
    /// The newest entry must win, whatever order the directory happens to list them.
    #[test]
    fn the_freshest_entry_wins_for_a_resumed_session() {
        let dir = tempfile::TempDir::new().unwrap();
        let old = dir.path().join("old.sock");
        let new = dir.path().join("new.sock");
        std::fs::write(&old, b"").unwrap();
        std::fs::write(&new, b"").unwrap();
        write_entry(dir.path(), 200, "sess-d", Some(&old), 100);
        write_entry(dir.path(), 201, "sess-d", Some(&new), 999);

        let registry = ClaudeRegistry::read_dir(dir.path());

        assert_eq!(registry.len(), 1, "one session id, one entry");
        assert_eq!(
            registry.inbox_socket(&SessionId::new("sess-d")),
            Some(new.as_path()),
            "the resumed process's socket must win over its predecessor's"
        );
    }

    /// The publish path reads this directory while Claude Code is writing it, so a
    /// truncated or foreign file must be skipped rather than poisoning the read.
    #[test]
    fn junk_files_are_skipped_without_losing_the_good_ones() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(dir.path().join("half-written.json"), b"{\"pid\":1,").unwrap();
        std::fs::write(dir.path().join("notes.txt"), b"not json at all").unwrap();
        write_entry(dir.path(), 300, "sess-e", None, 10);

        let registry = ClaudeRegistry::read_dir(dir.path());

        assert_eq!(registry.len(), 1);
        assert!(registry.get(&SessionId::new("sess-e")).is_some());
    }

    /// No Claude Code on this machine is not an error — it is "nothing is reachable
    /// by socket", and every subscriber falls back.
    #[test]
    fn a_missing_directory_yields_an_empty_registry() {
        let dir = tempfile::TempDir::new().unwrap();
        let registry = ClaudeRegistry::read_dir(&dir.path().join("no-such-dir"));
        assert!(registry.is_empty());
    }
}
