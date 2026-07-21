//! Emitting (and merging) the Claude Code `settings.json` hooks snippet.
//!
//! `install-hooks` wires the ADR-0008 on-demand wake loop with four hooks and NO
//! periodic re-arm WAKE (the `Stop` hook is a plain exit-0 liveness poke, never a wake):
//!
//! - `SessionStart` (matcher `""` — all sources) runs `mailbox harness session-start`,
//!   a plain synchronous hook: it registers the inbox, prints the `watchPaths`
//!   registering this session's sentinel, and spawns the detached watcher. It fires on
//!   `startup` AND on `resume`/`clear`/`compact`, so a resumed session (a fresh process)
//!   re-establishes all three — the gap ADR-0013 closes.
//! - `Stop` (matcher `""`) runs `mailbox harness ensure-watcher`, a plain synchronous
//!   hook (NEVER asyncRewake): the pessimistic Stop-liveness net — respawn the detached
//!   watcher iff it is dead, and re-register the inbox (best-effort). It NEVER exits 2
//!   (never wakes) — it exits 1 only on a config/stdin error and 0 otherwise — so it is
//!   the primary recovery for a died watcher without ever costing a model turn.
//! - `FileChanged` (matcher [`WAKE_SENTINEL_BASENAME`]) runs `mailbox harness wake`
//!   as an `asyncRewake` hook with a `timeout` (seconds): when the watcher bumps the
//!   sentinel, it fires even on an idle session and exits 2 iff there is real unread
//!   mail. It is a fast read-only peek, so the timeout is a backstop, not a re-arm
//!   timer.
//! - `SessionEnd` runs `mailbox harness cleanup` (a plain, synchronous hook): reap
//!   the watcher, remove the sentinel, drop interests/subscriptions.
//!
//! This REPLACES the ADR-0006 `SessionStart`/`Stop` → `arm` → exit-2-re-arm loop,
//! whose every re-arm cost a full model turn on a long idle. The `arm` command
//! itself is retained as a primitive (and its timing knobs `--max-block-ms` /
//! `--timeout-secs` still validated by [`HookInstallSpec::validate`]), but it is no
//! longer wired into the snippet. See `docs/01-wake-and-rearm.md`, ADR-0008.
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

use mailbox_protocol::WAKE_SENTINEL_BASENAME;
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

/// How to render the hooks snippet: the binary to invoke and the two timing knobs.
#[derive(Debug, Clone)]
pub struct HookInstallSpec {
    /// Absolute path to the `mailbox` binary the hooks invoke. Absolute so the
    /// hook works regardless of the session's `PATH`.
    pub mailbox_bin: String,
    /// Claude Code's per-hook kill deadline, in **seconds** (the `timeout` field).
    pub timeout_secs: u64,
    /// The waiter's max block before it yields for a re-arm, in **milliseconds**.
    /// MUST be below `timeout_secs` (with a margin) so the re-arm exit always
    /// precedes the harness's kill — see [`HookInstallSpec::validate`].
    pub max_block_ms: u64,
}

/// The async-hook `timeout` the snippet writes by default: **1 hour**, well above
/// Claude Code's own 10-minute default for command hooks.
///
/// A large timeout IS honoured (measured: a hook with `timeout: 3600` sailed past
/// the 600s default and was still alive at 703s — there is no hidden 600s cap), and
/// it is the ONLY thing that buys a long idle now that we know a waiter cannot
/// extend its own life (ADR-0006). The trade: the waiter yields for a re-arm every
/// `max_block`, so a *larger* timeout means *fewer* benign re-arm wakes. 1h is the
/// verified-safe maximum we are willing to ship; both knobs stay tunable
/// (`--timeout-secs`, `--max-block-ms`).
pub const DEFAULT_HOOK_TIMEOUT_SECS: u64 = 3600;

/// The waiter max-block the snippet writes by default: **55 minutes**, i.e. 5
/// minutes inside [`DEFAULT_HOOK_TIMEOUT_SECS`] — so the re-arm exit always
/// precedes the harness's kill, with room to spare.
pub const DEFAULT_MAX_BLOCK_MS: u64 = 3_300_000;

/// Claude Code's OWN default `timeout` for a command hook (**600s**), which applies
/// whenever a hook entry omits the field.
///
/// This is what `arm` must assume when the hook that launched it did not tell it
/// otherwise — and it is precisely why [`DEFAULT_MAX_BLOCK_MS`] (55m) cannot be
/// trusted blindly at the point of use: under a 600s deadline it would have the waiter
/// killed mid-block, and an idle session fires no further `Stop` to re-arm it. A
/// hand-edited `settings.json`, or an arm hook written without our `timeout`, lands
/// exactly there. See [`resolve_max_block`].
pub const CLAUDE_CODE_DEFAULT_HOOK_TIMEOUT_SECS: u64 = 600;

/// The lower bound of the safety margin between `max_block_ms` and the hook
/// `timeout` (10s). Enough for the waiter to notice its deadline, drop its pidfile
/// and exit.
const MIN_TIMING_MARGIN_MS: u64 = 10_000;

/// The upper bound of that margin (5 minutes). Without a cap the 10% rule would
/// scale the margin with the timeout — at `timeout = 3600s` it would demand 6
/// minutes of slack and reject the shipped default (`max_block = 55m`), even though
/// the waiter needs only milliseconds to yield. The margin covers a slow exit, not
/// a proportion of the idle.
const MAX_TIMING_MARGIN_MS: u64 = 300_000;

/// The install spec is invalid — the timing knobs would defeat the re-arm exit.
#[derive(Debug, thiserror::Error)]
pub enum InstallError {
    /// `max_block_ms` is not safely below the async-hook timeout, so Claude Code
    /// would kill the waiter while it was still blocked — and a truly idle session
    /// fires no further `Stop`, so nothing would ever re-arm it. That is the exact
    /// silent-un-arm bug the re-arm exit exists to prevent, so we refuse loudly
    /// rather than install a config that reintroduces it.
    #[error(
        "max_block_ms ({max_block_ms}) must be at least {margin_ms}ms below the async-hook timeout ({timeout_secs}s = {timeout_ms}ms), else Claude Code kills the waiter while it is still blocked — and an idle session fires no further Stop, so nothing would re-arm it (the session would go silently un-armed)"
    )]
    MaxBlockNotBelowTimeout {
        max_block_ms: u64,
        timeout_secs: u64,
        timeout_ms: u64,
        margin_ms: u64,
    },
}

/// The safety margin a `max_block` must leave below a hook `timeout`: 10% of the
/// timeout, clamped to [`MIN_TIMING_MARGIN_MS`]..=[`MAX_TIMING_MARGIN_MS`].
fn timing_margin_ms(timeout_ms: u64) -> u64 {
    (timeout_ms / 10).clamp(MIN_TIMING_MARGIN_MS, MAX_TIMING_MARGIN_MS)
}

/// The largest `max_block_ms` that is safely below `timeout_secs` — i.e. the biggest
/// value [`HookInstallSpec::validate`] would accept.
///
/// Floored at 1s: a hook `timeout` so small that no positive max-block is safe (≤10s)
/// is a broken configuration with no good outcome, and of the two bad ones we pick the
/// LOUD one. A tiny max-block re-arms noisily and visibly; refusing to arm at all
/// would be silent, permanent deafness — the bug this whole design exists to prevent.
pub fn max_safe_max_block_ms(timeout_secs: u64) -> u64 {
    let timeout_ms = timeout_secs.saturating_mul(1000);
    timeout_ms
        .saturating_sub(timing_margin_ms(timeout_ms))
        .max(1_000)
}

/// What `arm` will actually block for, given the hook `timeout` it runs under and the
/// `max_block` it was asked for.
///
/// The `max_block < timeout` invariant is load-bearing (ADR-0006) and used to be
/// checked ONLY at install time — where the value is written, not where it is used. So
/// a hand-edited `settings.json`, or an arm hook whose entry omits `timeout` (Claude
/// Code then applies its own 600s default, under which our 55m default max-block is
/// lethal), silently restored the original catastrophic bug. This is the same rule,
/// enforced at the point of use.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MaxBlockDecision {
    /// The requested max-block is safely below the timeout: use it as asked.
    AsRequested(u64),
    /// The requested max-block would have let Claude Code kill the waiter mid-block.
    /// CLAMPED down to the largest safe value, loudly — clamping keeps the session
    /// armed (a waiter that yields early is harmless), whereas refusing to arm would
    /// be the very deafness the invariant protects against.
    Clamped {
        requested: u64,
        resolved: u64,
        timeout_secs: u64,
    },
}

impl MaxBlockDecision {
    /// The max-block to actually pass to the waiter.
    pub fn resolved(self) -> u64 {
        match self {
            MaxBlockDecision::AsRequested(ms) => ms,
            MaxBlockDecision::Clamped { resolved, .. } => resolved,
        }
    }
}

/// Enforce `max_block < timeout` at the point of use (see [`MaxBlockDecision`]).
pub fn resolve_max_block(timeout_secs: u64, requested_ms: u64) -> MaxBlockDecision {
    let spec_ok = HookInstallSpec {
        mailbox_bin: String::new(),
        timeout_secs,
        max_block_ms: requested_ms,
    }
    .validate()
    .is_ok();
    if spec_ok {
        return MaxBlockDecision::AsRequested(requested_ms);
    }
    MaxBlockDecision::Clamped {
        requested: requested_ms,
        resolved: max_safe_max_block_ms(timeout_secs),
        timeout_secs,
    }
}

impl HookInstallSpec {
    /// Reject a spec whose `max_block_ms` is not safely below the async-hook
    /// `timeout`. The waiter must reach its max-block AND exit 2 before Claude
    /// Code's kill deadline, so we require a margin (10% of the timeout, clamped to
    /// [`MIN_TIMING_MARGIN_MS`]..=[`MAX_TIMING_MARGIN_MS`]).
    ///
    /// This is the load-bearing invariant (ADR-0006): a `max_block >= timeout`
    /// silently reintroduces the un-armed-forever bug, so it is a hard install-time
    /// failure and never a warning. `arm` enforces the SAME rule at the point of use
    /// ([`resolve_max_block`]), because settings.json can also be written by hand.
    pub fn validate(&self) -> Result<(), InstallError> {
        let timeout_ms = self.timeout_secs.saturating_mul(1000);
        let margin_ms = timing_margin_ms(timeout_ms);
        if self.max_block_ms.saturating_add(margin_ms) > timeout_ms {
            return Err(InstallError::MaxBlockNotBelowTimeout {
                max_block_ms: self.max_block_ms,
                timeout_secs: self.timeout_secs,
                timeout_ms,
                margin_ms,
            });
        }
        Ok(())
    }

    /// The `session-start` hook command (`<bin> harness session-start`). A
    /// short-lived, synchronous hook (ADR-0008): it registers the inbox, prints the
    /// `watchPaths`, and spawns the detached watcher, then exits 0. NOT asyncRewake —
    /// it never wakes the session itself.
    fn session_start_command(&self) -> String {
        format!("{} harness session-start", self.mailbox_bin)
    }

    /// The `wake` hook command (`<bin> harness wake`). The `FileChanged` hook
    /// (ADR-0008): it wakes the session (exit 2) only when there is genuine unread
    /// mail, else exits 0.
    fn wake_command(&self) -> String {
        format!("{} harness wake", self.mailbox_bin)
    }

    /// The `ensure-watcher` hook command (`<bin> harness ensure-watcher`). The
    /// `Stop`-liveness hook (ADR-0008): a plain hook that respawns the detached watcher
    /// iff it is dead and re-registers the inbox (best-effort, ADR-0013). NOT
    /// asyncRewake — it never wakes the session.
    fn ensure_watcher_command(&self) -> String {
        format!("{} harness ensure-watcher", self.mailbox_bin)
    }

    /// The `cleanup` hook command string (`<bin> harness cleanup`).
    fn cleanup_command(&self) -> String {
        format!("{} harness cleanup", self.mailbox_bin)
    }
}

/// Build the `{ "hooks": { … } }` snippet for the ADR-0008 on-demand wake loop.
///
/// Four hooks, and NO periodic re-arm WAKE (that is the whole point — it eliminates the
/// spurious re-arm model turns of ADR-0006). The `Stop` hook is NOT a re-arm wake: it
/// is a plain, exit-0 liveness poke.
///
/// - `SessionStart` (matcher `""`, all sources) runs `session-start` (plain,
///   synchronous): register the inbox, print the `watchPaths` registering this session's
///   sentinel, spawn the detached watcher. Firing on every source (not just `startup`)
///   is what re-establishes all three on a resume (ADR-0013).
/// - `Stop` runs `ensure-watcher` (plain, synchronous, NEVER asyncRewake): the
///   pessimistic Stop-liveness net (ADR-0008 FIX 3) — respawn the detached watcher iff
///   it is dead, and re-register the inbox (best-effort, restoring ADR-0007's
///   register-on-every-Stop invariant). It NEVER exits 2 (never wakes) — it exits 1 only
///   on a config/stdin error and 0 otherwise — so it costs a per-turn process spawn but
///   NEVER a model turn. This is the primary recovery for a watcher that died.
/// - `FileChanged` runs `wake` as an `asyncRewake` hook, matched on the sentinel
///   basename ([`WAKE_SENTINEL_BASENAME`]): when the watcher bumps the sentinel, this
///   fires even on a truly-idle session and exits 2 (iff there is real unread mail).
///   It carries `timeout_secs` as its kill deadline — it is a fast read-only peek, so
///   the timeout is only a backstop.
/// - `SessionEnd` runs `cleanup` (plain): reap the watcher, remove the sentinel, drop
///   subscriptions/interests.
pub fn hooks_snippet(spec: &HookInstallSpec) -> Value {
    json!({
        "hooks": {
            // SessionStart fires on startup AND on resume/clear/compact. The matcher is
            // "" (all sources), NOT "startup": a RESUMED session is a fresh process that
            // must re-register its inbox, re-print its watchPaths, and re-spawn its
            // watcher — none of which the Stop hook can do for it (a Stop cannot emit a
            // SessionStart watchPaths registration). Gating this to "startup" left every
            // resumed session unaddressable and unwakeable (ADR-0013). session-start is
            // idempotent, so firing on every source is safe.
            "SessionStart": [json!({
                "matcher": "",
                "hooks": [{
                    "type": "command",
                    "command": spec.session_start_command(),
                }],
            })],
            // Stop fires at every turn boundary: a plain (NOT asyncRewake) liveness poke
            // that respawns a dead watcher and re-prints watchPaths, exiting 0 always so
            // it can never itself wake the session (ADR-0008 Stop-liveness).
            "Stop": [json!({
                "matcher": "",
                "hooks": [{
                    "type": "command",
                    "command": spec.ensure_watcher_command(),
                }],
            })],
            // FileChanged fires when the watcher bumps the sentinel. The matcher is
            // the sentinel BASENAME (Claude Code matches FileChanged by basename);
            // per-session isolation comes from the absolute path SessionStart
            // registered via watchPaths.
            "FileChanged": [json!({
                "matcher": WAKE_SENTINEL_BASENAME,
                "hooks": [{
                    "type": "command",
                    "command": spec.wake_command(),
                    // asyncRewake: wake the idle session when this exits 2 (the
                    // payload is the wake hook's payload-free stderr reminder).
                    "asyncRewake": true,
                    // Claude Code's per-hook kill deadline (seconds) — a backstop for
                    // a fast hook, not a re-arm timer.
                    "timeout": spec.timeout_secs,
                }],
            })],
            // SessionEnd tears the watcher down, removes the sentinel, and drops
            // interests/subscriptions.
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
            // Current (ADR-0008) hooks, plus the retired `arm` so it is swept on
            // upgrade, plus `watch` for symmetry (never installed as a hook, but ours).
            Some("session-start")
                | Some("wake")
                | Some("ensure-watcher")
                | Some("watch")
                | Some("arm")
                | Some("cleanup")
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
    let (target, via_symlink) = resolve_symlink(path)?;
    let mut backup_path = None;

    for _ in 0..MERGE_ATTEMPTS {
        // The pre-image: both what we merge from, and what the compare-and-swap
        // below checks is still there when we publish.
        let raw = read_settings(&target)?;
        let existing = parse_settings(&target, raw.as_deref())?;
        let merged = merge_into_settings(existing, snippet);
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

/// The absolute form of a caller-supplied binary path (best-effort).
pub fn abs_bin(path: &Path) -> String {
    std::fs::canonicalize(path)
        .map(|p| p.display().to_string())
        .unwrap_or_else(|_| path.display().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec() -> HookInstallSpec {
        HookInstallSpec {
            mailbox_bin: "/opt/mailbox".to_string(),
            timeout_secs: DEFAULT_HOOK_TIMEOUT_SECS,
            max_block_ms: DEFAULT_MAX_BLOCK_MS,
        }
    }

    #[test]
    fn snippet_is_valid_and_wires_the_adr0008_hooks() {
        let snippet = hooks_snippet(&spec());
        let hooks = &snippet["hooks"];

        // SessionStart: matcher "" (all sources, so it re-fires on resume — ADR-0013),
        // a PLAIN session-start command (NOT asyncRewake — it never wakes the session).
        let start = &hooks["SessionStart"][0];
        assert_eq!(start["matcher"], "");
        let ss = &start["hooks"][0];
        assert_eq!(ss["type"], "command");
        assert_eq!(ss["command"], "/opt/mailbox harness session-start");
        assert!(
            ss.get("asyncRewake").is_none(),
            "session-start must NOT be asyncRewake"
        );

        // The Stop hook is the ADR-0008 Stop-liveness poke: `ensure-watcher`, plain
        // (NOT asyncRewake, no timeout), so it can never itself wake the session.
        let stop = &hooks["Stop"][0];
        assert_eq!(stop["matcher"], "");
        let ew = &stop["hooks"][0];
        assert_eq!(ew["command"], "/opt/mailbox harness ensure-watcher");
        assert!(
            ew.get("asyncRewake").is_none(),
            "the Stop-liveness hook must NOT be asyncRewake (it never wakes)"
        );

        // FileChanged: matcher = the sentinel basename, asyncRewake wake with timeout.
        let fc = &hooks["FileChanged"][0];
        assert_eq!(fc["matcher"], WAKE_SENTINEL_BASENAME);
        let wake = &fc["hooks"][0];
        assert_eq!(wake["command"], "/opt/mailbox harness wake");
        assert_eq!(wake["asyncRewake"], true);
        assert_eq!(wake["timeout"], 3600);

        // SessionEnd: cleanup, NOT asyncRewake.
        let end = &hooks["SessionEnd"][0]["hooks"][0];
        assert_eq!(end["command"], "/opt/mailbox harness cleanup");
        assert!(end.get("asyncRewake").is_none());
    }

    /// The shipped defaults must satisfy the invariant they exist to protect — a
    /// default that failed `validate` would make `install-hooks` refuse its own
    /// snippet, and a default with `max_block >= timeout` would reintroduce the
    /// silent-un-arm bug on every install.
    #[test]
    fn the_shipped_defaults_validate_with_a_rearm_margin() {
        let s = spec();
        assert!(s.max_block_ms < s.timeout_secs * 1000);
        s.validate().expect("the default spec must be valid");
        // 5 minutes of slack for an exit that takes milliseconds.
        assert_eq!(s.timeout_secs * 1000 - s.max_block_ms, MAX_TIMING_MARGIN_MS);
    }

    /// The previous defaults (10-minute timeout, 9-minute block) are still a legal
    /// hand-configured spec: raising the default must not invalidate a user who
    /// pinned the old knobs.
    #[test]
    fn the_previous_defaults_are_still_a_valid_spec() {
        HookInstallSpec {
            mailbox_bin: "/opt/mailbox".to_string(),
            timeout_secs: 600,
            max_block_ms: 540_000,
        }
        .validate()
        .expect("600s/540s must remain valid");
    }

    #[test]
    fn validate_rejects_max_block_at_or_above_timeout() {
        // A max-block at (or above) the timeout is the bug itself: Claude Code kills
        // the waiter mid-block, and an idle session never fires the Stop that would
        // re-arm it. Refused at install time, loudly.
        let bad = HookInstallSpec {
            mailbox_bin: "/opt/mailbox".to_string(),
            timeout_secs: 600,
            max_block_ms: 600_000,
        };
        assert!(matches!(
            bad.validate(),
            Err(InstallError::MaxBlockNotBelowTimeout { .. })
        ));
        // Above the timeout, likewise.
        assert!(
            HookInstallSpec {
                max_block_ms: 900_000,
                ..bad.clone()
            }
            .validate()
            .is_err()
        );
        // Just inside the margin is also rejected (the block AND the exit must fit).
        assert!(
            HookInstallSpec {
                max_block_ms: 599_000,
                ..bad
            }
            .validate()
            .is_err()
        );
    }

    /// The invariant, enforced where it is USED. `install-hooks` validating at write
    /// time protects nothing if `arm` is launched from a hand-edited settings.json —
    /// or from a hook entry with no `timeout` field at all, where Claude Code applies
    /// its own 600s default and our 55-minute max-block becomes lethal.
    #[test]
    fn resolve_max_block_clamps_a_max_block_that_is_not_safely_below_the_timeout() {
        // The shipped pairing passes through untouched.
        assert_eq!(
            resolve_max_block(DEFAULT_HOOK_TIMEOUT_SECS, DEFAULT_MAX_BLOCK_MS),
            MaxBlockDecision::AsRequested(DEFAULT_MAX_BLOCK_MS)
        );

        // The trap: the shipped default max-block under Claude Code's OWN default
        // timeout. Left alone this is the original bug — the waiter is killed at 600s,
        // 50 minutes before it would have yielded, and an idle session fires no further
        // Stop to re-arm it.
        let decision =
            resolve_max_block(CLAUDE_CODE_DEFAULT_HOOK_TIMEOUT_SECS, DEFAULT_MAX_BLOCK_MS);
        assert_eq!(
            decision,
            MaxBlockDecision::Clamped {
                requested: DEFAULT_MAX_BLOCK_MS,
                resolved: 540_000, // 600s - the 60s margin
                timeout_secs: CLAUDE_CODE_DEFAULT_HOOK_TIMEOUT_SECS,
            }
        );
        // And the clamped value is one `validate` itself accepts — the two rules agree.
        HookInstallSpec {
            mailbox_bin: String::new(),
            timeout_secs: CLAUDE_CODE_DEFAULT_HOOK_TIMEOUT_SECS,
            max_block_ms: decision.resolved(),
        }
        .validate()
        .expect("the clamped max-block must satisfy the install-time rule");
    }

    /// A timeout too small for ANY safe max-block has no good outcome. We pick the loud
    /// one: a floor of 1s, so the waiter re-arms noisily and visibly. Refusing to arm
    /// would be silent, permanent deafness — the failure mode this design exists to
    /// prevent.
    #[test]
    fn an_absurdly_small_timeout_still_arms_with_a_floored_max_block() {
        assert_eq!(max_safe_max_block_ms(5), 1_000);
        assert_eq!(resolve_max_block(5, DEFAULT_MAX_BLOCK_MS).resolved(), 1_000);
        assert!(resolve_max_block(5, DEFAULT_MAX_BLOCK_MS).resolved() > 0);
    }

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
        assert_eq!(twice["hooks"]["FileChanged"].as_array().unwrap().len(), 1);
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

    fn hook_commands(settings: &Value, event: &str) -> Vec<String> {
        settings["hooks"][event]
            .as_array()
            .expect("event array")
            .iter()
            .flat_map(group_commands)
            .collect()
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
        assert!(written["hooks"]["FileChanged"].is_array());

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
        assert!(written["hooks"]["FileChanged"].is_array());
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
            written["hooks"]["FileChanged"].is_array(),
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
        assert!(value["hooks"]["FileChanged"].is_array());
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
    fn re_merging_with_a_different_binary_replaces_our_hooks_instead_of_appending() {
        let first = hooks_snippet(&HookInstallSpec {
            mailbox_bin: "/tmp/target/debug/mailbox".to_string(),
            timeout_secs: 600,
            max_block_ms: 540_000,
        });
        let second = hooks_snippet(&HookInstallSpec {
            mailbox_bin: "/home/u/.local/bin/mailbox".to_string(),
            timeout_secs: 300,
            max_block_ms: 120_000,
        });

        let once = merge_into_settings(json!({}), &first);
        let twice = merge_into_settings(once, &second);

        // Exactly ONE session-start and ONE wake hook — the first bin's hooks are
        // REPLACED, not appended (a stale hook would point at a deleted binary).
        let start = hook_commands(&twice, "SessionStart");
        assert_eq!(start.len(), 1, "exactly ONE session-start hook: {start:?}");
        assert_eq!(start[0], "/home/u/.local/bin/mailbox harness session-start");
        let wake = hook_commands(&twice, "FileChanged");
        assert_eq!(wake.len(), 1, "exactly ONE wake hook: {wake:?}");
        assert_eq!(wake[0], "/home/u/.local/bin/mailbox harness wake");
        let end = hook_commands(&twice, "SessionEnd");
        assert_eq!(end, vec!["/home/u/.local/bin/mailbox harness cleanup"]);
    }

    /// An UPGRADE from the old ADR-0006 re-arm hooks must sweep the stale `arm`
    /// groups (SessionStart + Stop) and install the ADR-0008 hooks in their place —
    /// leaving no hook pointing at the retired mechanism.
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

        // The stale Stop → arm hook is swept and REPLACED by the Stop-liveness hook
        // (ensure-watcher) — the retired exit-2 re-arm must not survive.
        let stop = hook_commands(&merged, "Stop");
        assert!(
            !stop.iter().any(|c| c.contains("harness arm")),
            "the retired arm hook must not survive an upgrade: {stop:?}"
        );
        assert_eq!(
            stop,
            vec!["/opt/mailbox harness ensure-watcher"],
            "the Stop event now carries the ADR-0008 Stop-liveness hook: {stop:?}"
        );
        // SessionStart now runs session-start, FileChanged runs wake — the new loop.
        assert_eq!(
            hook_commands(&merged, "SessionStart"),
            vec!["/opt/mailbox harness session-start"]
        );
        assert_eq!(
            hook_commands(&merged, "FileChanged"),
            vec!["/opt/mailbox harness wake"]
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
        assert!(is_our_command("mailbox harness ensure-watcher"));
        assert!(is_our_command("/a/b/c/mailbox harness cleanup"));
        // The retired `arm` is still recognised, so an upgrade sweeps it.
        assert!(is_our_command("/opt/mailbox harness arm --max-block-ms 1"));
        assert!(is_our_command("mailbox harness watch --session x"));
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
}
