//! Emitting (and merging) the Claude Code `settings.json` hooks snippet.
//!
//! `install-hooks` wires the whole loop with three hooks:
//!
//! - `SessionStart` (matcher `startup`) and `Stop` run `mailbox harness arm` as a
//!   background `asyncRewake` hook with a `timeout` (seconds). asyncRewake means
//!   the process's exit-2 wakes the idle session; the timeout is Claude Code's
//!   per-hook kill deadline (default 10 minutes for command hooks).
//! - `SessionEnd` runs `mailbox harness cleanup` (a plain, synchronous hook).
//!
//! The `arm` command carries `--max-block-ms`, which it passes to the waiter it
//! execs. That max-block is deliberately shorter than the async-hook `timeout`, so
//! the waiter re-execs a fresh image *before* Claude Code's timeout would kill it
//! (see `docs/01-wake-and-rearm.md`).

use std::path::Path;

use serde_json::{Value, json};

/// How to render the hooks snippet: the binary to invoke and the two timing knobs.
#[derive(Debug, Clone)]
pub struct HookInstallSpec {
    /// Absolute path to the `mailbox` binary the hooks invoke. Absolute so the
    /// hook works regardless of the session's `PATH`.
    pub mailbox_bin: String,
    /// Claude Code's per-hook kill deadline, in **seconds** (the `timeout` field).
    pub timeout_secs: u64,
    /// The waiter's max block before it self-respawns, in **milliseconds**. Kept
    /// below `timeout_secs` so a re-exec always precedes the kill deadline.
    pub max_block_ms: u64,
}

/// The install spec is invalid — the timing knobs would defeat the self-respawn.
#[derive(Debug, thiserror::Error)]
pub enum InstallError {
    /// `max_block_ms` is not safely below the async-hook timeout, so Claude Code
    /// would SIGKILL the waiter before it could re-exec — silently un-arming the
    /// session. The margin exists so the block AND the re-exec both fit.
    #[error(
        "max_block_ms ({max_block_ms}) must be at least {margin_ms}ms below the async-hook timeout ({timeout_secs}s = {timeout_ms}ms), else the waiter is killed before it can self-respawn"
    )]
    MaxBlockNotBelowTimeout {
        max_block_ms: u64,
        timeout_secs: u64,
        timeout_ms: u64,
        margin_ms: u64,
    },
}

impl HookInstallSpec {
    /// Reject a spec whose `max_block_ms` is not safely below the async-hook
    /// `timeout`. The waiter must finish a full block AND re-exec before Claude
    /// Code's kill deadline, so we require a margin (the larger of 10s or 10% of
    /// the timeout). This is the load-bearing self-respawn invariant (ADR-0006).
    pub fn validate(&self) -> Result<(), InstallError> {
        let timeout_ms = self.timeout_secs.saturating_mul(1000);
        let margin_ms = (timeout_ms / 10).max(10_000);
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

    /// The `arm` hook command string (`<bin> harness arm --max-block-ms <n>`).
    fn arm_command(&self) -> String {
        format!(
            "{} harness arm --max-block-ms {}",
            self.mailbox_bin, self.max_block_ms
        )
    }

    /// The `cleanup` hook command string (`<bin> harness cleanup`).
    fn cleanup_command(&self) -> String {
        format!("{} harness cleanup", self.mailbox_bin)
    }
}

/// One `asyncRewake` arm hook group for a given matcher.
fn arm_group(spec: &HookInstallSpec, matcher: &str) -> Value {
    json!({
        "matcher": matcher,
        "hooks": [{
            "type": "command",
            "command": spec.arm_command(),
            // asyncRewake: run in the background and wake the idle session when the
            // process exits 2 (payload = the waiter's stderr reminder).
            "asyncRewake": true,
            // Claude Code's per-hook kill deadline, in seconds.
            "timeout": spec.timeout_secs,
        }],
    })
}

/// Build the `{ "hooks": { … } }` snippet for the three hooks.
pub fn hooks_snippet(spec: &HookInstallSpec) -> Value {
    json!({
        "hooks": {
            // SessionStart fires with source "startup" on a fresh session; arm then.
            "SessionStart": [arm_group(spec, "startup")],
            // Stop fires whenever the agent goes idle; re-arm (iff still subscribed).
            "Stop": [arm_group(spec, "")],
            // SessionEnd tears the waiter down and drops interests/subscriptions.
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

/// Merge [`hooks_snippet`] into an existing `settings.json` value, appending our
/// hook groups to each event array without clobbering unrelated settings.
///
/// Idempotent by command string: re-running a merge does not duplicate our hooks
/// (a group whose single command already appears in the event array is skipped).
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

    for (event, incoming_groups) in incoming_hooks {
        let slot = hooks.entry(event).or_insert_with(|| json!([]));
        if !slot.is_array() {
            *slot = json!([]);
        }
        let array = slot.as_array_mut().expect("event slot is an array");
        if let Some(groups) = incoming_groups.as_array() {
            for group in groups {
                if !group_already_present(array, group) {
                    array.push(group.clone());
                }
            }
        }
    }
    existing
}

/// Whether an event array already contains a group with the same command(s), so a
/// re-merge is a no-op rather than a duplicate.
fn group_already_present(array: &[Value], group: &Value) -> bool {
    let commands = group_commands(group);
    array
        .iter()
        .any(|existing| group_commands(existing) == commands)
}

/// The set of command strings a hook group runs (its identity for dedup).
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
            timeout_secs: 600,
            max_block_ms: 540_000,
        }
    }

    #[test]
    fn snippet_is_valid_and_wires_the_three_hooks() {
        let snippet = hooks_snippet(&spec());
        let hooks = &snippet["hooks"];

        // SessionStart: matcher "startup", asyncRewake arm with the timeout.
        let start = &hooks["SessionStart"][0];
        assert_eq!(start["matcher"], "startup");
        let arm = &start["hooks"][0];
        assert_eq!(arm["type"], "command");
        assert_eq!(
            arm["command"],
            "/opt/mailbox harness arm --max-block-ms 540000"
        );
        assert_eq!(arm["asyncRewake"], true);
        assert_eq!(arm["timeout"], 600);

        // Stop: same arm, empty matcher (fires on every idle).
        assert_eq!(hooks["Stop"][0]["matcher"], "");
        assert_eq!(
            hooks["Stop"][0]["hooks"][0]["command"],
            "/opt/mailbox harness arm --max-block-ms 540000"
        );

        // SessionEnd: cleanup, NOT asyncRewake.
        let end = &hooks["SessionEnd"][0]["hooks"][0];
        assert_eq!(end["command"], "/opt/mailbox harness cleanup");
        assert!(end.get("asyncRewake").is_none());
    }

    #[test]
    fn default_spec_validates_and_leaves_a_respawn_margin() {
        // The self-respawn invariant: the waiter must re-exec before Claude Code's
        // per-hook timeout would kill it, with margin.
        let s = spec();
        assert!(s.max_block_ms < s.timeout_secs * 1000);
        s.validate().expect("the default spec must be valid");
    }

    #[test]
    fn validate_rejects_max_block_at_or_above_timeout() {
        // A max-block equal to (or above) the timeout leaves no room to re-exec.
        let bad = HookInstallSpec {
            mailbox_bin: "/opt/mailbox".to_string(),
            timeout_secs: 600,
            max_block_ms: 600_000,
        };
        assert!(matches!(
            bad.validate(),
            Err(InstallError::MaxBlockNotBelowTimeout { .. })
        ));
        // Just inside the margin is also rejected (block + re-exec must both fit).
        let tight = HookInstallSpec {
            max_block_ms: 599_000,
            ..bad
        };
        assert!(tight.validate().is_err());
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
                "Stop": [{"matcher":"","hooks":[{"type":"command","command":"echo other"}]}]
            }
        });
        let merged = merge_into_settings(existing, &hooks_snippet(&spec()));
        // Unrelated top-level setting survives.
        assert_eq!(merged["model"], "sonnet");
        // The foreign Stop hook is kept AND our arm hook is appended.
        let stop = merged["hooks"]["Stop"].as_array().unwrap();
        assert_eq!(stop.len(), 2);
        assert_eq!(stop[0]["hooks"][0]["command"], "echo other");
        assert_eq!(
            stop[1]["hooks"][0]["command"],
            "/opt/mailbox harness arm --max-block-ms 540000"
        );
    }

    #[test]
    fn merge_is_idempotent() {
        let once = merge_into_settings(json!({}), &hooks_snippet(&spec()));
        let twice = merge_into_settings(once.clone(), &hooks_snippet(&spec()));
        // Re-merging must not duplicate our hook groups.
        assert_eq!(once, twice);
        assert_eq!(twice["hooks"]["Stop"].as_array().unwrap().len(), 1);
    }
}
