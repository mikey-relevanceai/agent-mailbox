//! Harness-side setup for waking agent sessions (Claude Code first).
//!
//! # The whole agent-facing loop is subscribe / read / react / unsubscribe
//!
//! The agent never re-arms itself. Infrastructure owns the wake loop via Claude
//! Code hooks (see `docs/01-wake.md`): the `serve` daemon writes a
//! session's wake sentinel when mail lands for it, the `FileChanged` hook fires on
//! that change even against an idle session, and an `asyncRewake` hook that exits 2
//! wakes it. [`install`](install) wires that hook set into `~/.claude/settings.json`
//! (when that file exists — else it prints the snippet and says why), merging rather
//! than clobbering.
//!
//! The other half of setup is [`skills`](skills), which installs the embedded
//! `agent-mailbox` skill into the user's Claude Code skills dir. The hooks make
//! wake infrastructure; the skill teaches the agent the loop it wakes into. Both
//! default under the same home ([`home`](home)) — one convention, one override.
//!
//! # What this crate is (and is not)
//!
//! It is the harness INTEGRATION: settings, skills, and the hook payload parse. The
//! hook *behaviour* lives in the `mailbox` binary, which owns the socket client and
//! the store; the dependency edge is one-way (`mailbox` → `mailbox-harness`, never
//! back).
//!
//! # Session identity comes from the hook (settled, card 11)
//!
//! Claude Code passes each hook its payload as JSON on stdin, including
//! `session_id`. [`hook::HookInput::parse`] reads it into a branded
//! [`mailbox_protocol::SessionId`], and the hook handler uses that directly. A hook
//! therefore never depends on the environment for identity, which is why the CLI's
//! `--session` flag could be deleted without touching this crate: a *client* command
//! resolves itself from `$CLAUDE_CODE_SESSION_ID`, a *hook* is told who it is.

pub mod atomic;
pub mod home;
pub mod hook;
pub mod install;
pub mod skills;

use mailbox_protocol::PROTOCOL_VERSION;

/// Confirms the harness crate is linked against the expected protocol version.
pub fn protocol_version() -> u32 {
    PROTOCOL_VERSION
}
