//! Emitting (and merging) the Claude Code `settings.json` hooks snippet.
//!
//! `install-hooks` wires TWO plain hooks, and **neither can wake the session**:
//!
//! - `SessionStart` (matcher `""` — all sources) runs `mailbox harness session-start`,
//!   registering the always-on agent inbox so peers can address this session
//!   (ADR-0007). It fires on `startup` AND on `resume`/`clear`/`compact`, so a resumed
//!   session (a fresh process) re-establishes it — the gap ADR-0013 closes.
//! - `SessionEnd` runs `mailbox harness cleanup`: drop interests/subscriptions, so no
//!   poller outlives the session that wanted it.
//!
//! # What used to be here
//!
//! Three more: a `FileChanged` `asyncRewake` hook that exited 2 to wake an idle
//! session, a `Stop` hook that re-triggered for mail whose wake edge was spent while
//! the agent was busy, and a `UserPromptSubmit` hook stamping turn boundaries so a
//! health probe could tell "busy" from "deaf". All three existed to compensate for a
//! wake wire that could silently lose an edge. The wire is now the session's inbox
//! socket, which the daemon writes directly, so none of them has anything to do
//! ([ADR-0021](../../docs/adr/0021-delete-the-sentinel-fallback.md)).
//!
//! A re-run **sweeps** every hook name we have ever installed, so upgrading over an
//! older install removes the retired three rather than leaving them firing.
//!
//! # Where the hooks go, and why that is a TYPE
//!
//! Setup is two symmetric commands: `install-skills` writes `~/.claude/skills`, and
//! `install-hooks` writes `~/.claude/settings.json` — *when that file exists*. It
//! must not conjure a `settings.json` on a machine with no Claude Code, so the
//! command genuinely has three outcomes, not a merge/print boolean:
//! [`SettingsTarget`] names them, is resolved ONCE by [`resolve_settings_target`],
//! and is matched exhaustively at the CLI edge. That way "we only printed" always
//! arrives with the reason attached, instead of the user being left to guess.

use std::path::{Path, PathBuf};

use serde_json::{Value, json};

use crate::atomic::{write_atomic, write_atomic_guarded};
use crate::home::{CLAUDE_DIR, harness_home};

/// Claude Code's settings file inside its config dir (`~/.claude/settings.json`).
const SETTINGS_FILE: &str = "settings.json";

/// Where `install-hooks` will put the snippet — and, when it will not, why.
///
/// Resolved once (by [`resolve_settings_target`]) and then matched exhaustively, so
/// the merge-or-print decision and its *reason* travel together. A bool could not
/// carry the reason, and the reason is the whole difference between "your hooks are
/// installed" and "you have no Claude Code settings for me to install them into".
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SettingsTarget {
    /// `--settings <path>` was passed: merge there, creating the file if missing.
    /// An explicit path is an instruction, so it is honoured even when nothing is
    /// there yet — that is how you bootstrap a settings file on purpose.
    Explicit(PathBuf),
    /// No flag, and the default settings file EXISTS: merge into it. The common
    /// case, and what makes `install-hooks` the mirror of `install-skills`.
    DefaultFound(PathBuf),
    /// No flag and nothing to merge into: print the snippet only, and say why.
    NoDefault {
        /// The default path we looked at, or `None` when no home could be resolved
        /// at all (so there was not even a path to look at). Either way we write
        /// NOTHING: creating a `settings.json` on a machine with no Claude Code —
        /// or under a guessed home — is not ours to do.
        looked_at: Option<PathBuf>,
    },
}

impl SettingsTarget {
    /// The file to merge into, or `None` when this run only prints.
    pub fn merge_path(&self) -> Option<&Path> {
        match self {
            SettingsTarget::Explicit(path) | SettingsTarget::DefaultFound(path) => Some(path),
            SettingsTarget::NoDefault { .. } => None,
        }
    }
}

/// The default Claude Code settings path under a home: `<home>/.claude/settings.json`.
pub fn default_settings_path(home: &Path) -> PathBuf {
    home.join(CLAUDE_DIR).join(SETTINGS_FILE)
}

/// Resolve where the hooks go. **Pure**: the home and the existence check are
/// injected, never read from the global environment here — that is what lets every
/// variant (including "no home") be unit-tested without mutating a process-global
/// the whole test binary shares. [`settings_target`] is the thin env-reading edge.
pub fn resolve_settings_target(
    explicit: Option<PathBuf>,
    home: Option<&Path>,
    exists: impl Fn(&Path) -> bool,
) -> SettingsTarget {
    if let Some(path) = explicit {
        return SettingsTarget::Explicit(path);
    }
    let Some(home) = home else {
        return SettingsTarget::NoDefault { looked_at: None };
    };
    let candidate = default_settings_path(home);
    if exists(&candidate) {
        SettingsTarget::DefaultFound(candidate)
    } else {
        SettingsTarget::NoDefault {
            looked_at: Some(candidate),
        }
    }
}

/// [`resolve_settings_target`] against the real environment: home from
/// `AGENT_MAILBOX_HOME` then `HOME` (the same resolution `install-skills` uses), and
/// the real filesystem for existence.
pub fn settings_target(explicit: Option<PathBuf>) -> SettingsTarget {
    resolve_settings_target(explicit, harness_home().as_deref(), |path| path.exists())
}

/// How to render the hooks snippet: the binary to invoke, and the hook timeout.
#[derive(Debug, Clone)]
pub struct HookInstallSpec {
    /// Absolute path to the `mailbox` binary the hooks invoke. Absolute so the
    /// hook works regardless of the session's `PATH`.
    pub mailbox_bin: String,
}

impl HookInstallSpec {
    /// The `session-start` hook command (`<bin> harness session-start`). A
    /// short-lived, synchronous hook: it registers the inbox, arms the wake sentinel,
    /// then exits 0. It can never wake the
    /// session itself.
    fn session_start_command(&self) -> String {
        format!("{} harness session-start", self.mailbox_bin)
    }

    /// The `cleanup` hook command string (`<bin> harness cleanup`).
    fn cleanup_command(&self) -> String {
        format!("{} harness cleanup", self.mailbox_bin)
    }
}

/// Build the `{ "hooks": { … } }` snippet.
///
/// TWO hooks, and **neither can wake the session** — waking is the daemon writing the
/// session's inbox socket, not a hook (ADR-0021):
///
/// - `SessionStart` (matcher `""`, all sources) runs `session-start`: register the
///   always-on agent inbox so peers can address this session (ADR-0007). Firing on
///   every source rather than just `startup` is what re-establishes it on a resume,
///   which is a fresh process (ADR-0013).
/// - `SessionEnd` runs `cleanup`: drop subscriptions and interests, so no poller
///   outlives the session that wanted it.
pub fn hooks_snippet(spec: &HookInstallSpec) -> Value {
    json!({
        "hooks": {
            // SessionStart fires on startup AND on resume/clear/compact. The matcher is
            // "" (all sources), NOT "startup": a RESUMED session is a fresh process that
            // must re-register its inbox, and gating this to "startup" left every
            // resumed session unaddressable (ADR-0013). It is idempotent, so firing on
            // every source is safe.
            "SessionStart": [json!({
                "matcher": "",
                "hooks": [{
                    "type": "command",
                    "command": spec.session_start_command(),
                }],
            })],
            // SessionEnd drops interests/subscriptions, so no poller outlives the
            // session that wanted it.
            "SessionEnd": [json!({
                "matcher": "",
                "hooks": [{
                    "type": "command",
                    "command": spec.cleanup_command(),
                }],
            })],
        }
    })
}

/// Merge [`hooks_snippet`] into an existing `settings.json` value: **our** hook
/// groups are replaced, everyone else's are left exactly as they were.
///
/// # Why ours are identified by shape, not by their exact command string
///
/// Dedup by full command string made a re-run with a *different* `--mailbox-bin` (or
/// `--max-block-ms`) append a SECOND arm group instead of updating the first —
/// leaving a stale hook pointing at the old, possibly deleted, binary. That is the
/// documented install flow (run it once from `target/`, again from `~/.local/bin`),
/// so it was not a corner case. A group is ours if any of its commands is a
/// `<bin> harness arm|cleanup` invocation ([`is_our_command`]) — whatever the bin
/// path or flags — so a re-run *updates in place*.
///
/// Any non-object `hooks` (or non-array event) in the existing settings is treated
/// as absent and replaced for that key — we never silently discard a *well-formed*
/// foreign hook, only overwrite a malformed one.
pub fn merge_into_settings(mut existing: Value, snippet: &Value) -> Value {
    // Ensure the top level is an object we can insert `hooks` into.
    if !existing.is_object() {
        existing = json!({});
    }
    let root = existing.as_object_mut().expect("existing is an object");

    let incoming_hooks = snippet
        .get("hooks")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();

    let hooks = root.entry("hooks").or_insert_with(|| json!({}));
    if !hooks.is_object() {
        *hooks = json!({});
    }
    let hooks = hooks.as_object_mut().expect("hooks is an object");

    // Pre-sweep: drop OUR hook groups from EVERY existing event, including ones the
    // incoming snippet no longer writes. This is what makes an UPGRADE clean — a retired
    // ADR-0006 `arm` hook on an event the current snippet writes differently (e.g. the
    // `Stop` event now carries `ensure-watcher`, not the exit-2 `arm` re-arm) is removed
    // here, so the old re-arm can never survive. Only OUR groups are removed (foreign
    // hooks are left exactly as they were).
    for groups in hooks.values_mut() {
        if let Some(array) = groups.as_array_mut() {
            array.retain(|group| !is_our_group(group));
        }
    }

    for (event, incoming_groups) in incoming_hooks {
        let slot = hooks.entry(event).or_insert_with(|| json!([]));
        if !slot.is_array() {
            *slot = json!([]);
        }
        let array = slot.as_array_mut().expect("event slot is an array");
        // Drop any previous generation of OUR hooks (a stale bin path, an old
        // max-block), keeping every foreign group untouched...
        array.retain(|group| !is_our_group(group));
        // ...then install the current ones.
        if let Some(groups) = incoming_groups.as_array() {
            array.extend(groups.iter().cloned());
        }
    }
    existing
}

/// Claude Code's setting for what a session does with messages arriving on its inbox
/// socket (ADR-0020).
pub const INBOUND_SETTING_KEY: &str = "crossSessionInbound";

/// The value that lets a `bypassPermissions` session receive a mailbox wake without
/// a human approving each one.
pub const INBOUND_ACCEPT: &str = "accept";

/// What [`inbound_state`] found in a settings document.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InboundState {
    /// No value set. Claude Code then decides per message from the two sessions'
    /// permission classes — which HOLDS our wakes for a `bypassPermissions` session.
    Unset,
    /// Already `accept`: nothing to do.
    Accept,
    /// Set to something else (`hold` / `refuse`, or a value we do not recognise).
    /// Reported rather than silently overwritten.
    Other(String),
    /// Present but not a string, so we cannot say what policy it expresses — only
    /// that the user had one. Distinct from [`InboundState::Unset`] so the command
    /// reports "replaced" rather than "set".
    Unreadable,
}

/// Read the current inbound policy out of a settings document.
///
/// Separate from the write so the CLI can tell the operator what is there *before*
/// changing it — and so re-running the command on an already-configured machine can
/// say "already accept" instead of rewriting the file.
pub fn inbound_state(existing: &Value) -> InboundState {
    match existing.get(INBOUND_SETTING_KEY) {
        None => InboundState::Unset,
        Some(value) => match value.as_str() {
            Some(INBOUND_ACCEPT) => InboundState::Accept,
            Some(other) => InboundState::Other(other.to_string()),
            None => InboundState::Unreadable,
        },
    }
}

/// Set `crossSessionInbound: "accept"`, leaving every other setting untouched.
///
/// # Why this is its own command and never part of `install-hooks`
///
/// `accept` re-opens unattended delivery in exactly the configuration Claude Code's
/// default guards: a session running `--dangerously-skip-permissions` acts without
/// asking, so accepting messages from any same-user process means any such process
/// can direct that agent. That is a real widening of trust, and it is the operator's
/// call to make — not a side effect of installing a wake path. `install-hooks` must
/// never do this implicitly.
///
/// Idempotent, as [`merge_settings_file`] requires.
pub fn set_inbound_accept(mut existing: Value) -> Value {
    if !existing.is_object() {
        existing = json!({});
    }
    existing
        .as_object_mut()
        .expect("existing is an object")
        .insert(INBOUND_SETTING_KEY.to_string(), json!(INBOUND_ACCEPT));
    existing
}

/// Whether a hook group is one WE installed — i.e. it runs any agent-mailbox
/// harness command. Matched on the command's shape rather than its exact text, so
/// the group we planted with a different binary path or `--max-block-ms` is still
/// recognised as ours and gets replaced rather than duplicated.
fn is_our_group(group: &Value) -> bool {
    group_commands(group).iter().any(|c| is_our_command(c))
}

/// Whether a command string is one of ours: `<any-bin> harness <sub>` where `<sub>`
/// is a hook we install or have ever installed. The binary path is deliberately
/// ignored (it legitimately changes between installs), and the OLD `arm` subcommand
/// is still recognised so an UPGRADE from the ADR-0006 re-arm hooks REPLACES them
/// with the ADR-0008 on-demand hooks rather than leaving a stale arm hook behind.
fn is_our_command(command: &str) -> bool {
    let mut tokens = command.split_whitespace();
    let has_bin = tokens.next().is_some();
    has_bin
        && tokens.next() == Some("harness")
        && matches!(
            tokens.next(),
            // Current hooks, plus every subcommand we have EVER installed as one, so
            // an upgrade REPLACES the old group instead of leaving a hook pointing at
            // a subcommand this binary no longer has: `arm` (ADR-0006),
            // `ensure-watcher` (renamed to `turn-end`), and `watch` (the deleted
            // detached watcher, ADR-0017).
            Some("session-start")
                | Some("wake")
                | Some("turn-end")
                | Some("turn-start")
                | Some("cleanup")
                | Some("ensure-watcher")
                | Some("watch")
                | Some("arm")
        )
}

/// The set of command strings a hook group runs.
fn group_commands(group: &Value) -> Vec<String> {
    group
        .get("hooks")
        .and_then(Value::as_array)
        .map(|hooks| {
            hooks
                .iter()
                .filter_map(|h| h.get("command").and_then(Value::as_str))
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

// ==== merging the snippet into a real settings.json on disk =====================

/// Whether to keep a copy of the settings file we are about to rewrite.
///
/// `install-hooks` now edits the user's real config **by default**, with no preview
/// and no confirmation, so the default path keeps [`BackupPolicy::Keep`] insurance.
/// An explicitly-named `--settings <path>` is a deliberate instruction at a
/// deliberate path, and litters no `.bak` beside it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackupPolicy {
    /// Write `<file>.bak` (the pre-image) before publishing the merge.
    Keep,
    /// Write no backup.
    Skip,
}

/// What one successful [`merge_hooks_file`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeReport {
    /// The file the merged settings were actually written to. With a symlinked
    /// settings.json this is the **resolved** target, not the link.
    pub written: PathBuf,
    /// The symlink we followed to get there, when the requested path was one.
    pub via_symlink: Option<PathBuf>,
    /// The `.bak` we wrote first, when there was a pre-image to save.
    pub backup: Option<PathBuf>,
}

/// Merging the snippet into a settings file failed. **Every variant leaves the
/// user's file exactly as it was** — which is the whole point of this type: the
/// code it replaced treated *every* read failure (permission denied, non-UTF-8,
/// EIO) as "the file isn't there", started from `{}`, and atomically published a
/// hooks-ONLY document over the user's real settings. Their model, their
/// permissions (deny rules included), their apiKeyHelper: gone, exit 0, "merged".
/// A read that fails is now an error, never an empty document.
#[derive(Debug, thiserror::Error)]
pub enum MergeError {
    /// The existing settings could not be read (permissions, EIO, a directory in
    /// the way). We do NOT know what is in the file, so we must not replace it.
    #[error("reading the existing settings at {path} (refusing to overwrite them)")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    /// The existing settings are not UTF-8. Corrupt, perhaps — but corrupt-and-kept
    /// beats silently-replaced: only the user knows whether those bytes mattered.
    #[error("the existing settings at {path} are not valid UTF-8 (refusing to overwrite them)")]
    NotUtf8 { path: PathBuf },

    /// The existing settings are not valid JSON. Same reasoning as [`Self::NotUtf8`].
    #[error("parsing the existing settings at {path} (refusing to overwrite them)")]
    Parse {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },

    /// The settings path is a symlink we could not resolve (a broken link). We
    /// write THROUGH a link, never over it, so an unresolvable one is fatal rather
    /// than an invitation to replace it with a regular file.
    #[error("resolving the symlinked settings at {path}")]
    Resolve {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    /// The pre-image backup could not be written, so the merge was not attempted:
    /// no insurance, no edit.
    #[error("backing up the existing settings to {path}")]
    Backup {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    /// The merged settings could not be published; the target is untouched.
    #[error("writing the merged settings to {path}")]
    Write {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    /// Something else kept rewriting the file while we merged (Claude Code writes
    /// `settings.json` on a `/config` change or an "always allow" click). We retried
    /// and lost every time, and would rather report that than discard their edit.
    #[error(
        "the settings at {path} kept changing underneath us ({attempts} attempts); nothing was written"
    )]
    Contended { path: PathBuf, attempts: u32 },

    /// The merged document could not be serialized (not reachable in practice).
    #[error("serializing the merged settings")]
    Serialize(#[source] serde_json::Error),
}

/// How many times a [`merge_hooks_file`] re-merges when it loses the compare-and-swap.
const MERGE_ATTEMPTS: u32 = 5;

/// Merge the hooks snippet into the settings file at `path`, atomically.
///
/// Guarantees, each of which exists because its absence was a real bug:
///
/// - **Nothing is destroyed.** A settings file we cannot read or parse is an error
///   ([`MergeError`]); only a genuinely *absent* file starts from `{}`.
/// - **A symlink is written THROUGH**, never replaced. Dotfiles setups symlink
///   `~/.claude/settings.json` into a tracked repo; replacing the link with a
///   regular file would leave the tracked file without the hooks and let the next
///   `stow -R` silently revert them.
/// - **No lost updates.** Claude Code rewrites this same file (model changes,
///   permission grants). The publish is a compare-and-swap against the bytes we
///   merged from: if they changed, we re-merge from the new content and retry
///   (safe, because the merge is idempotent).
/// - **The pre-image is backed up** under [`BackupPolicy::Keep`].
pub fn merge_hooks_file(
    path: &Path,
    snippet: &Value,
    backup: BackupPolicy,
) -> Result<MergeReport, MergeError> {
    merge_settings_file(path, backup, |existing| {
        merge_into_settings(existing, snippet)
    })
}

/// The general form of [`merge_hooks_file`]: apply `merge` to the settings document
/// at `path` under every guarantee listed there.
///
/// Split out because the hooks are no longer the only thing we edit — ADR-0020 adds
/// `crossSessionInbound` — and every one of those guarantees (do not destroy an
/// unreadable file, write through a symlink, compare-and-swap against Claude Code's
/// own writes, keep a pre-image) is the product of a real bug. A second command
/// hand-rolling its own settings write would re-open all of them.
///
/// `merge` must be **idempotent**: it is re-applied from scratch on every retry
/// after a lost compare-and-swap.
pub fn merge_settings_file(
    path: &Path,
    backup: BackupPolicy,
    merge: impl Fn(Value) -> Value,
) -> Result<MergeReport, MergeError> {
    let (target, via_symlink) = resolve_symlink(path)?;
    let mut backup_path = None;

    for _ in 0..MERGE_ATTEMPTS {
        // The pre-image: both what we merge from, and what the compare-and-swap
        // below checks is still there when we publish.
        let raw = read_settings(&target)?;
        let existing = parse_settings(&target, raw.as_deref())?;
        let merged = merge(existing);
        let mut body = serde_json::to_string_pretty(&merged).map_err(MergeError::Serialize)?;
        body.push('\n');

        if backup == BackupPolicy::Keep
            && let Some(bytes) = &raw
            && backup_path.is_none()
        {
            let bak = backup_path_for(&target);
            write_atomic(&bak, bytes).map_err(|source| MergeError::Backup {
                path: bak.clone(),
                source,
            })?;
            backup_path = Some(bak);
        }

        // Compare-and-swap: in the last moment before the rename, re-read the file.
        // If it is not the pre-image we merged from, somebody else wrote it — abort
        // the publish (their edit survives) and go round again from their content.
        let guard = || match std::fs::read(&target) {
            Ok(current) if raw.as_deref() == Some(current.as_slice()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound && raw.is_none() => Ok(()),
            Ok(_) | Err(_) => Err(std::io::Error::new(
                std::io::ErrorKind::Interrupted,
                "the settings file changed while we were merging it",
            )),
        };

        match write_atomic_guarded(&target, body.as_bytes(), guard) {
            Ok(()) => {
                return Ok(MergeReport {
                    written: target,
                    via_symlink,
                    backup: backup_path,
                });
            }
            // We lost the race. Re-merge from whatever they wrote and try again.
            Err(err) if err.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(source) => {
                return Err(MergeError::Write {
                    path: target,
                    source,
                });
            }
        }
    }

    Err(MergeError::Contended {
        path: target,
        attempts: MERGE_ATTEMPTS,
    })
}

/// Follow a symlinked settings path to the file it names, so the merge is written
/// THROUGH the link (preserving it) rather than over it. Returns the path to write
/// and, when one was followed, the link itself — which the CLI reports, because a
/// user whose config is symlinked deserves to be told where their hooks landed.
fn resolve_symlink(path: &Path) -> Result<(PathBuf, Option<PathBuf>), MergeError> {
    match std::fs::symlink_metadata(path) {
        Ok(meta) if meta.file_type().is_symlink() => {
            let resolved = std::fs::canonicalize(path).map_err(|source| MergeError::Resolve {
                path: path.to_path_buf(),
                source,
            })?;
            Ok((resolved, Some(path.to_path_buf())))
        }
        // Not a link (or not there at all — an absent file is created in place).
        _ => Ok((path.to_path_buf(), None)),
    }
}

/// The raw bytes of the settings file, or `None` iff it does not exist.
///
/// **Only `NotFound` may become `None`.** Every other read failure is an error: the
/// caller must not be able to mistake "I could not read it" for "there is nothing
/// there" and publish a document that drops everything the file held.
fn read_settings(path: &Path) -> Result<Option<Vec<u8>>, MergeError> {
    match std::fs::read(path) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(source) => Err(MergeError::Read {
            path: path.to_path_buf(),
            source,
        }),
    }
}

/// The settings document to merge into: `{}` for an absent or blank file, an error
/// for one whose content we cannot understand (see [`MergeError`]).
fn parse_settings(path: &Path, raw: Option<&[u8]>) -> Result<Value, MergeError> {
    let Some(bytes) = raw else {
        return Ok(json!({}));
    };
    let text = std::str::from_utf8(bytes).map_err(|_| MergeError::NotUtf8 {
        path: path.to_path_buf(),
    })?;
    if text.trim().is_empty() {
        return Ok(json!({}));
    }
    serde_json::from_str(text).map_err(|source| MergeError::Parse {
        path: path.to_path_buf(),
        source,
    })
}

/// `<file>.bak`, beside the file (so the backup shares its filesystem and its
/// directory's permissions).
fn backup_path_for(target: &Path) -> PathBuf {
    let mut name = target
        .file_name()
        .unwrap_or_else(|| std::ffi::OsStr::new(SETTINGS_FILE))
        .to_os_string();
    name.push(".bak");
    target.with_file_name(name)
}

/// Convenience: the mailbox binary path a fresh install should default to (the
/// running executable, made absolute), or a plain `"mailbox"` fallback.
pub fn default_mailbox_bin(current_exe: std::io::Result<std::path::PathBuf>) -> String {
    match current_exe {
        Ok(path) => path.display().to_string(),
        Err(_) => "mailbox".to_string(),
    }
}

/// The absolute form of a caller-supplied binary path.
///
/// Returns the failure rather than falling back to the input. The one thing that
/// makes this path worth computing is that it is absolute — a relative path in
/// `settings.json` is resolved against whatever cwd the hook happened to fire in —
/// so quietly writing a hook we know to be unanchored would install exactly the
/// silent breakage the rest of this function is about.
///
/// `std::path::absolute` fails only on an empty path, which clap already rejects
/// before `--mailbox-bin` reaches here. That makes this unreachable through the
/// CLI today, and it is still the right signature: the guarantee belongs to this
/// function, not to an argument parser one crate away that could stop enforcing it
/// without anything here noticing.
///
/// Absolute but deliberately NOT canonical: `absolute` anchors a relative path to
/// the cwd without following symlinks, where `canonicalize` would resolve them.
/// Under a package manager that distinction decides whether the hook survives an
/// upgrade. Homebrew installs into a versioned Cellar directory and exposes it via
/// `opt_bin` — `/opt/homebrew/opt/mailbox/bin/mailbox` — a symlink it re-points on
/// every upgrade. Canonicalising that would bake the Cellar path `brew upgrade`
/// then deletes into `settings.json`, leaving `SessionStart` pointing at a binary
/// that is gone: the inbox stops being registered, so peers cannot address the
/// session, while topic wakes keep working and nothing looks broken (ADR-0025).
pub fn abs_bin(path: &Path) -> std::io::Result<String> {
    std::path::absolute(path).map(|p| p.display().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec() -> HookInstallSpec {
        HookInstallSpec {
            mailbox_bin: "/opt/mailbox".to_string(),
        }
    }

    // ==== crossSessionInbound (ADR-0020) ======================================

    /// The whole point of the setting, and the whole risk of it, is one key. It must
    /// land without disturbing anything else the user has configured — this command
    /// edits the same real settings.json that `install-hooks` does.
    #[test]
    fn setting_inbound_accept_preserves_every_other_setting() {
        let existing = json!({
            "model": "opus",
            "permissions": { "deny": ["Bash(rm -rf /)"] },
            "hooks": { "Stop": [{ "matcher": "" }] },
        });

        let merged = set_inbound_accept(existing);

        assert_eq!(merged[INBOUND_SETTING_KEY], "accept");
        assert_eq!(merged["model"], "opus");
        assert_eq!(merged["permissions"]["deny"][0], "Bash(rm -rf /)");
        assert_eq!(merged["hooks"]["Stop"][0]["matcher"], "");
    }

    /// `merge_settings_file` re-applies the transform on every compare-and-swap
    /// retry, so a non-idempotent one would produce a different document depending on
    /// how many times it lost the race.
    #[test]
    fn setting_inbound_accept_is_idempotent() {
        let once = set_inbound_accept(json!({"model": "opus"}));
        let twice = set_inbound_accept(once.clone());
        assert_eq!(once, twice);
    }

    /// The command reports what it found before changing it, so the three states have
    /// to be told apart — including a value we do not recognise, which must read as
    /// "something is set" rather than as "unset" and be reported, not silently kept.
    #[test]
    fn inbound_state_distinguishes_unset_accept_and_anything_else() {
        assert_eq!(inbound_state(&json!({})), InboundState::Unset);
        assert_eq!(
            inbound_state(&json!({ INBOUND_SETTING_KEY: "accept" })),
            InboundState::Accept
        );
        assert_eq!(
            inbound_state(&json!({ INBOUND_SETTING_KEY: "hold" })),
            InboundState::Other("hold".to_string())
        );
        assert_eq!(
            inbound_state(&json!({ INBOUND_SETTING_KEY: "banana" })),
            InboundState::Other("banana".to_string())
        );
        // A non-string value is not a policy we can read, and it is NOT "unset" — the
        // user has something there. Reporting it as unset would tell them we "set"
        // the key when we in fact replaced whatever they had, which is the wrong
        // report for a command whose whole justification is that widening this trust
        // boundary must be explicit.
        assert_eq!(
            inbound_state(&json!({ INBOUND_SETTING_KEY: 7 })),
            InboundState::Unreadable
        );
    }

    /// Setting the inbound policy must never install hooks as a side effect. They are
    /// separate decisions with separate consequences, and `install-hooks` is
    /// deliberately the one that does NOT widen a security default.
    #[test]
    fn setting_inbound_accept_installs_no_hooks() {
        let merged = set_inbound_accept(json!({}));
        assert!(
            merged.get("hooks").is_none(),
            "install-inbound changes one key and nothing else: {merged}"
        );
    }

    #[test]
    fn snippet_wires_exactly_two_plain_hooks_and_nothing_that_can_wake() {
        let snippet = hooks_snippet(&spec());
        let hooks = snippet["hooks"].as_object().expect("hooks object");

        // TWO hooks, and the count is asserted: the retired wake path needed five, of
        // which three (FileChanged/Stop/UserPromptSubmit) existed only to compensate
        // for a wake wire that could lose an edge (ADR-0021). Re-growing this set is
        // the shape of that mistake coming back.
        assert_eq!(
            hooks.keys().collect::<Vec<_>>(),
            vec!["SessionEnd", "SessionStart"],
            "only SessionStart and SessionEnd remain: {hooks:?}"
        );

        let start = &hooks["SessionStart"][0];
        assert_eq!(
            start["matcher"], "",
            "all sources, so it re-fires on resume"
        );
        assert_eq!(
            start["hooks"][0]["command"],
            "/opt/mailbox harness session-start"
        );

        let end = &hooks["SessionEnd"][0];
        assert_eq!(end["hooks"][0]["command"], "/opt/mailbox harness cleanup");

        // NOTHING here may wake the session. A session is woken by the daemon writing
        // its inbox socket; a hook that could exit 2 would be a second, unaccountable
        // wake wire.
        let wire = snippet.to_string();
        assert!(
            !wire.contains("asyncRewake"),
            "no hook may be a wake wire: {wire}"
        );
        assert!(
            !wire.contains("FileChanged"),
            "the FileChanged wake path is gone: {wire}"
        );
    }

    /// The shipped defaults must satisfy the invariant they exist to protect — a
    /// default that failed `validate` would make `install-hooks` refuse its own
    /// snippet, and a default with `max_block >= timeout` would reintroduce the
    /// silent-un-arm bug on every install.
    #[test]
    fn merge_into_empty_settings_yields_the_snippet_hooks() {
        let merged = merge_into_settings(json!({}), &hooks_snippet(&spec()));
        assert!(merged["hooks"]["SessionStart"].is_array());
        assert_eq!(
            merged["hooks"]["SessionEnd"][0]["hooks"][0]["command"],
            "/opt/mailbox harness cleanup"
        );
    }

    #[test]
    fn merge_preserves_unrelated_settings_and_foreign_hooks() {
        let existing = json!({
            "model": "sonnet",
            "hooks": {
                // A foreign hook on an event WE ALSO write (SessionStart): it must be
                // kept alongside ours, not clobbered.
                "SessionStart": [{"matcher":"","hooks":[{"type":"command","command":"echo other"}]}]
            }
        });
        let merged = merge_into_settings(existing, &hooks_snippet(&spec()));
        // Unrelated top-level setting survives.
        assert_eq!(merged["model"], "sonnet");
        // The foreign SessionStart hook is kept AND our session-start hook is appended.
        let start = merged["hooks"]["SessionStart"].as_array().unwrap();
        assert_eq!(start.len(), 2);
        assert_eq!(start[0]["hooks"][0]["command"], "echo other");
        assert_eq!(
            start[1]["hooks"][0]["command"],
            "/opt/mailbox harness session-start"
        );
    }

    #[test]
    fn merge_is_idempotent() {
        let once = merge_into_settings(json!({}), &hooks_snippet(&spec()));
        let twice = merge_into_settings(once.clone(), &hooks_snippet(&spec()));
        // Re-merging must not duplicate our hook groups.
        assert_eq!(once, twice);
        assert_eq!(twice["hooks"]["SessionStart"].as_array().unwrap().len(), 1);
        assert_eq!(twice["hooks"]["SessionStart"].as_array().unwrap().len(), 1);
    }

    // ==== where the hooks go: every variant, no env mutation ======================

    /// The pure resolver's inputs: a home, and an existence check that answers only
    /// for the paths we say exist. No filesystem, no environment.
    fn resolve(explicit: Option<&str>, home: Option<&str>, existing: &[&str]) -> SettingsTarget {
        let existing: Vec<PathBuf> = existing.iter().map(PathBuf::from).collect();
        resolve_settings_target(explicit.map(PathBuf::from), home.map(Path::new), |path| {
            existing.iter().any(|e| e == path)
        })
    }

    #[test]
    fn an_explicit_settings_path_is_the_target_even_when_it_does_not_exist() {
        // `--settings` is an instruction, not a hint: a missing file is created, so
        // a settings file CAN be bootstrapped — but only when explicitly asked for.
        assert_eq!(
            resolve(Some("/tmp/custom.json"), Some("/home/u"), &[]),
            SettingsTarget::Explicit(PathBuf::from("/tmp/custom.json")),
        );
    }

    #[test]
    fn an_explicit_path_wins_over_an_existing_default() {
        assert_eq!(
            resolve(
                Some("/tmp/custom.json"),
                Some("/home/u"),
                &["/home/u/.claude/settings.json"],
            ),
            SettingsTarget::Explicit(PathBuf::from("/tmp/custom.json")),
        );
    }

    #[test]
    fn an_existing_default_settings_file_is_merged_into() {
        // The common case, and what makes install-hooks symmetric with
        // install-skills: with Claude Code present, setup installs by default.
        assert_eq!(
            resolve(None, Some("/home/u"), &["/home/u/.claude/settings.json"]),
            SettingsTarget::DefaultFound(PathBuf::from("/home/u/.claude/settings.json")),
        );
    }

    /// No settings file → print only, and the path we looked at rides along so the
    /// user is told WHY rather than left wondering. We must NOT conjure a
    /// settings.json on a machine that has no Claude Code.
    #[test]
    fn a_missing_default_settings_file_prints_only_and_names_the_path_it_looked_at() {
        assert_eq!(
            resolve(None, Some("/home/u"), &[]),
            SettingsTarget::NoDefault {
                looked_at: Some(PathBuf::from("/home/u/.claude/settings.json")),
            },
        );
    }

    /// No home at all (neither AGENT_MAILBOX_HOME nor HOME): print only, with no
    /// path — never a panic, and never a guessed path inside someone's config.
    #[test]
    fn no_home_prints_only_and_guesses_no_path() {
        assert_eq!(
            resolve(None, None, &["/home/u/.claude/settings.json"]),
            SettingsTarget::NoDefault { looked_at: None },
        );
    }

    #[test]
    fn the_default_settings_path_is_claude_settings_json_under_home() {
        // Same `<home>/.claude/...` layout as the default skills dir, off the same
        // resolved home — one convention, one thing for a test to redirect.
        assert_eq!(
            default_settings_path(Path::new("/home/u")),
            Path::new("/home/u/.claude/settings.json"),
        );
    }

    // ==== merging a real file: the user's settings are NEVER destroyed ===========

    use std::os::unix::fs::PermissionsExt;

    use tempfile::TempDir;

    /// A settings.json with the things a user would actually lose: their model,
    /// their permission policy (deny rules!), their key helper.
    const PRECIOUS: &str = r#"{
  "model": "opus",
  "apiKeyHelper": "/usr/local/bin/key",
  "permissions": {"deny": ["Bash(rm -rf *)"]}
}
"#;

    fn merge(path: &Path, backup: BackupPolicy) -> Result<MergeReport, MergeError> {
        merge_hooks_file(path, &hooks_snippet(&spec()), backup)
    }

    fn set_mode(path: &Path, mode: u32) {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).expect("chmod");
    }

    /// Whether the filesystem enforces permissions for us (it does not for root, and
    /// asserting a denial that cannot happen would be asserting something untrue).
    fn permissions_are_enforced(path: &Path) -> bool {
        std::fs::read(path).is_err()
    }

    /// Every hook command wired to `event`. An ABSENT event yields an empty list
    /// rather than panicking: since ADR-0021 we write no hook to most events, and
    /// "nothing of ours is on Stop" is a thing tests need to assert.
    fn hook_commands(settings: &Value, event: &str) -> Vec<String> {
        settings["hooks"][event]
            .as_array()
            .map(|groups| groups.iter().flat_map(group_commands).collect())
            .unwrap_or_default()
    }

    #[test]
    fn merging_an_absent_file_creates_it() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("settings.json");

        let report = merge(&path, BackupPolicy::Skip).expect("merge");

        assert_eq!(report.written, path);
        assert!(report.backup.is_none(), "there was no pre-image to back up");
        let written: Value =
            serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        assert!(written["hooks"]["SessionStart"].is_array());
    }

    #[test]
    fn merging_preserves_unrelated_settings_and_backs_the_original_up() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("settings.json");
        std::fs::write(&path, PRECIOUS).unwrap();

        let report = merge(&path, BackupPolicy::Keep).expect("merge");

        let written: Value =
            serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(written["model"], "opus");
        assert_eq!(written["permissions"]["deny"][0], "Bash(rm -rf *)");
        assert!(written["hooks"]["SessionStart"].is_array());

        let backup = report.backup.expect("the pre-image is kept");
        assert_eq!(
            std::fs::read_to_string(&backup).unwrap(),
            PRECIOUS,
            "the backup is the file exactly as it was"
        );
    }

    /// **The data-loss regression guard (A).** A settings.json we cannot READ must be
    /// an error — never "the file isn't there", which merged from `{}` and published
    /// a hooks-ONLY document over the user's real config, exit 0, reporting "merged".
    #[test]
    fn an_unreadable_settings_file_is_an_error_and_is_left_intact() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("settings.json");
        std::fs::write(&path, PRECIOUS).unwrap();
        set_mode(&path, 0o000);

        if permissions_are_enforced(&path) {
            let err = merge(&path, BackupPolicy::Keep).expect_err("an unreadable file is fatal");
            assert!(matches!(err, MergeError::Read { .. }), "{err:?}");

            set_mode(&path, 0o600);
            assert_eq!(
                std::fs::read_to_string(&path).unwrap(),
                PRECIOUS,
                "the user's settings must survive byte for byte"
            );
        }
        set_mode(&path, 0o600);
    }

    /// Same guard for a file that reads fine but is not UTF-8: corrupt-and-kept beats
    /// silently-replaced. Only the user knows whether those bytes mattered.
    #[test]
    fn a_non_utf8_settings_file_is_an_error_and_is_left_intact() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("settings.json");
        let bytes = [b'{', 0xff, 0xfe, b'}'];
        std::fs::write(&path, bytes).unwrap();

        let err = merge(&path, BackupPolicy::Keep).expect_err("non-UTF-8 is fatal");

        assert!(matches!(err, MergeError::NotUtf8 { .. }), "{err:?}");
        assert_eq!(std::fs::read(&path).unwrap(), bytes, "left byte for byte");
    }

    #[test]
    fn an_unparseable_settings_file_is_an_error_and_is_left_intact() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("settings.json");
        std::fs::write(&path, "{ not json").unwrap();

        let err = merge(&path, BackupPolicy::Keep).expect_err("bad JSON is fatal");

        assert!(matches!(err, MergeError::Parse { .. }), "{err:?}");
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "{ not json");
    }

    #[test]
    fn a_blank_settings_file_merges_from_an_empty_document() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("settings.json");
        std::fs::write(&path, "  \n").unwrap();

        merge(&path, BackupPolicy::Skip).expect("a blank file is not corrupt");

        let written: Value =
            serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        assert!(written["hooks"]["SessionStart"].is_array());
    }

    /// **(B)** A symlinked settings.json (the dotfiles setup) is written THROUGH: the
    /// tracked file receives the hooks and the link survives. Replacing the link with
    /// a regular file left the tracked file hookless — and the next `stow -R` silently
    /// reverted the hooks, breaking wake.
    #[test]
    fn a_symlinked_settings_file_is_written_through_and_the_link_survives() {
        let dir = TempDir::new().unwrap();
        let tracked = dir.path().join("dotfiles").join("settings.json");
        std::fs::create_dir_all(tracked.parent().unwrap()).unwrap();
        std::fs::write(&tracked, PRECIOUS).unwrap();
        let link = dir.path().join("settings.json");
        std::os::unix::fs::symlink(&tracked, &link).unwrap();

        let report = merge(&link, BackupPolicy::Keep).expect("merge through the link");

        assert!(
            std::fs::symlink_metadata(&link).unwrap().is_symlink(),
            "the link must NOT be replaced by a regular file"
        );
        assert_eq!(report.via_symlink.as_deref(), Some(link.as_path()));
        assert_eq!(
            report.written,
            std::fs::canonicalize(&tracked).unwrap(),
            "the resolved, dotfiles-tracked file is what we wrote"
        );
        let written: Value =
            serde_json::from_str(&std::fs::read_to_string(&tracked).unwrap()).unwrap();
        assert_eq!(written["model"], "opus", "unrelated settings survive");
        assert!(
            written["hooks"]["SessionStart"].is_array(),
            "the TRACKED file is the one that got the hooks"
        );
    }

    /// A broken symlink is refused, not replaced: we write through links, so one we
    /// cannot resolve is an error rather than an invitation to clobber it.
    #[test]
    fn a_broken_symlink_is_refused_rather_than_replaced() {
        let dir = TempDir::new().unwrap();
        let link = dir.path().join("settings.json");
        std::os::unix::fs::symlink(dir.path().join("gone"), &link).unwrap();

        let err = merge(&link, BackupPolicy::Skip).expect_err("a broken link is fatal");

        assert!(matches!(err, MergeError::Resolve { .. }), "{err:?}");
        assert!(std::fs::symlink_metadata(&link).unwrap().is_symlink());
    }

    /// **(E)** The user's 0644 settings.json must not come back 0600.
    #[test]
    fn merging_preserves_the_file_mode() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("settings.json");
        std::fs::write(&path, PRECIOUS).unwrap();
        set_mode(&path, 0o644);

        merge(&path, BackupPolicy::Skip).expect("merge");

        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o644,
        );
    }

    /// **(D)** A concurrent writer (Claude Code saving a permission grant) must not
    /// have its edit discarded. The compare-and-swap loses the race, re-merges from
    /// the winner's content, and retries — so BOTH survive.
    #[test]
    fn a_concurrent_writer_is_not_lost() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("settings.json");
        std::fs::write(&path, PRECIOUS).unwrap();

        // Eight mergers racing a writer that keeps rewriting the file underneath
        // them. Each merge is idempotent, so retrying is always safe.
        std::thread::scope(|scope| {
            scope.spawn(|| {
                for i in 0..20 {
                    let value = json!({"model": format!("model-{i}")});
                    let body = serde_json::to_string_pretty(&value).unwrap();
                    write_atomic(&path, body.as_bytes()).expect("the rival writer");
                    std::thread::yield_now();
                }
            });
            for _ in 0..4 {
                scope.spawn(|| {
                    // A loss to the rival is reported as Contended, never as a
                    // clobber — either outcome is safe, and neither is data loss.
                    let _ = merge(&path, BackupPolicy::Skip);
                });
            }
        });

        // Whatever the interleaving, the file is COMPLETE and parseable — never a
        // half-merged or truncated document.
        let final_text = std::fs::read_to_string(&path).unwrap();
        let value: Value = serde_json::from_str(&final_text).expect("always valid JSON");
        assert!(value.is_object());

        // And a final, uncontended merge still lands on top of the rival's last write
        // without dropping it.
        merge(&path, BackupPolicy::Skip).expect("a quiet merge succeeds");
        let value: Value = serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        assert!(value["hooks"]["SessionStart"].is_array());
        assert!(
            value.get("model").is_some(),
            "the concurrent writer's key survived the merge"
        );
    }

    // ==== (G) a re-run UPDATES our hooks; it never appends a stale second copy ====

    /// Re-running with a different `--mailbox-bin` (the documented flow: once from
    /// `target/`, again from `~/.local/bin`) must REPLACE our hook group, not append
    /// a second one pointing at a binary that may no longer exist.
    #[test]
    fn re_merging_over_the_old_arm_hooks_replaces_them_with_the_new_loop() {
        let old = json!({
            "hooks": {
                "SessionStart": [{"matcher":"startup","hooks":[{"type":"command","command":"/opt/mailbox harness arm --max-block-ms 3300000 --timeout-secs 3600","asyncRewake":true,"timeout":3600}]}],
                "Stop": [{"matcher":"","hooks":[{"type":"command","command":"/opt/mailbox harness arm --max-block-ms 3300000 --timeout-secs 3600","asyncRewake":true,"timeout":3600}]}],
                "SessionEnd": [{"matcher":"","hooks":[{"type":"command","command":"/opt/mailbox harness cleanup"}]}]
            }
        });
        let merged = merge_into_settings(old, &hooks_snippet(&spec()));

        // Every retired hook of ours is SWEPT, including from events we no longer
        // write at all. An upgrade that left a `Stop → arm` (or a `FileChanged → wake`)
        // behind would keep an exit-2 wake wire firing against a binary that no longer
        // has the subcommand.
        let stop = hook_commands(&merged, "Stop");
        assert!(
            stop.is_empty(),
            "we install no Stop hook, and the retired one must not survive: {stop:?}"
        );
        // SessionStart now runs session-start — the whole remaining loop.
        assert_eq!(
            hook_commands(&merged, "SessionStart"),
            vec!["/opt/mailbox harness session-start"]
        );
        // And no exit-2 wake wire survives anywhere.
        assert!(
            hook_commands(&merged, "FileChanged").is_empty(),
            "the FileChanged wake hook is gone and must not be re-installed"
        );
    }

    #[test]
    fn re_merging_still_leaves_foreign_hooks_untouched() {
        let existing = json!({
            "hooks": {
                "SessionStart": [{"matcher":"","hooks":[{"type":"command","command":"echo other"}]}]
            }
        });
        let merged = merge_into_settings(existing, &hooks_snippet(&spec()));
        let merged = merge_into_settings(merged, &hooks_snippet(&spec()));

        let start = hook_commands(&merged, "SessionStart");
        assert_eq!(
            start,
            vec![
                "echo other".to_string(),
                "/opt/mailbox harness session-start".to_string(),
            ],
            "a foreign hook survives every re-merge, and ours is not duplicated"
        );
    }

    #[test]
    fn our_commands_are_recognised_whatever_the_binary_path_or_flags() {
        assert!(is_our_command("/opt/mailbox harness session-start"));
        assert!(is_our_command("mailbox harness wake"));
        assert!(is_our_command("mailbox harness turn-end"));
        // The retired names are still recognised, so an UPGRADE replaces them rather
        // than leaving a hook pointing at a subcommand this binary no longer has.
        assert!(is_our_command("mailbox harness ensure-watcher"));
        assert!(is_our_command("mailbox harness watch --session x"));
        assert!(is_our_command("/a/b/c/mailbox harness cleanup"));
        // The retired `arm` is still recognised, so an upgrade sweeps it.
        assert!(is_our_command("/opt/mailbox harness arm --max-block-ms 1"));
        // Not ours: another tool, and our own non-hook commands.
        assert!(!is_our_command("echo other"));
        assert!(!is_our_command("/opt/mailbox read --session x"));
        assert!(!is_our_command(""));
    }

    #[test]
    fn only_the_merging_variants_carry_a_merge_path() {
        assert_eq!(
            SettingsTarget::DefaultFound(PathBuf::from("/x")).merge_path(),
            Some(Path::new("/x")),
        );
        assert_eq!(
            SettingsTarget::Explicit(PathBuf::from("/x")).merge_path(),
            Some(Path::new("/x")),
        );
        assert!(
            SettingsTarget::NoDefault { looked_at: None }
                .merge_path()
                .is_none(),
            "a print-only run must expose no path to write to"
        );
    }
    #[test]
    fn re_merging_with_a_different_binary_replaces_our_hooks_instead_of_appending() {
        let first = hooks_snippet(&HookInstallSpec {
            mailbox_bin: "/tmp/target/debug/mailbox".to_string(),
        });
        let second = hooks_snippet(&HookInstallSpec {
            mailbox_bin: "/home/u/.local/bin/mailbox".to_string(),
        });

        let once = merge_into_settings(json!({}), &first);
        let twice = merge_into_settings(once, &second);

        // Exactly ONE of each of our hooks — the first bin's are REPLACED, not
        // appended (a stale hook would point at a deleted binary).
        let start = hook_commands(&twice, "SessionStart");
        assert_eq!(start.len(), 1, "exactly ONE session-start hook: {start:?}");
        assert_eq!(start[0], "/home/u/.local/bin/mailbox harness session-start");
        let end = hook_commands(&twice, "SessionEnd");
        assert_eq!(end, vec!["/home/u/.local/bin/mailbox harness cleanup"]);
    }

    // ==== abs_bin: absolute, never canonical (ADR-0025) =========================

    /// The load-bearing assumption behind the Homebrew install, and one the type
    /// system cannot hold: `abs_bin` must ANCHOR a path, not RESOLVE it. Homebrew
    /// hands us `opt_bin` — a stable symlink into a versioned Cellar directory that
    /// `brew upgrade` deletes and re-points. Swapping `absolute` back to
    /// `canonicalize` would bake the perishable side into `settings.json`, and the
    /// resulting breakage is silent (peers can no longer address the session, topic
    /// wakes carry on), so nothing else in the suite would notice.
    #[test]
    fn abs_bin_keeps_the_symlink_it_was_given() {
        let dir = TempDir::new().unwrap();
        // Stand in for Homebrew's layout: the versioned directory an upgrade
        // replaces, and the stable link callers are told to point the hook at.
        let cellar = dir.path().join("Cellar/mailbox/0.1.0/bin");
        std::fs::create_dir_all(&cellar).unwrap();
        let real = cellar.join("mailbox");
        std::fs::write(&real, b"#!/bin/sh\n").unwrap();

        let opt = dir.path().join("opt-mailbox-bin");
        std::os::unix::fs::symlink(&cellar, &opt).unwrap();
        let via_link = opt.join("mailbox");

        assert_eq!(
            abs_bin(&via_link).unwrap(),
            via_link.display().to_string(),
            "the link must survive: resolving it here is what breaks `brew upgrade`"
        );
    }

    /// The other half of the contract — it is still ABSOLUTE. A relative path in
    /// `settings.json` would be resolved against whatever cwd the hook happened to
    /// fire in, which is not ours to predict.
    #[test]
    fn abs_bin_anchors_a_relative_path_to_the_working_directory() {
        let anchored = abs_bin(Path::new("target/release/mailbox")).unwrap();

        assert!(
            Path::new(&anchored).is_absolute(),
            "hooks run with an unknown cwd, so a relative path is unusable: {anchored}"
        );
        assert!(anchored.ends_with("target/release/mailbox"), "{anchored}");
    }

    /// The failure is reported, not papered over. Returning the input unchanged
    /// would install a hook whose command is a bare relative path — resolved
    /// against whatever cwd Claude Code happens to run the hook in, which is the
    /// silent breakage the absolute path exists to prevent.
    #[test]
    fn abs_bin_reports_a_path_it_cannot_anchor() {
        let err = abs_bin(Path::new("")).expect_err("an empty path has no absolute form");

        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    }
}
