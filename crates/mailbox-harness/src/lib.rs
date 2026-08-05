//! Harness-side logic for arming and waking agent sessions (Claude Code first).
//!
//! # The whole agent-facing loop is subscribe / read / react / unsubscribe
//!
//! The agent never re-arms itself. Infrastructure owns the wake loop via Claude
//! Code hooks configured with `asyncRewake` (see `docs/01-wake-and-rearm.md`):
//!
//! - `SessionStart` / `Stop` run [`arm`](arm) — a background hook that launches a
//!   waiter *iff the session has subscriptions*. The waiter blocks on the bridge
//!   kick and exits 2 to wake the idle session; the payload is a short reminder
//!   ("mail on topic X"), never a body.
//! - `SessionEnd` runs [`cleanup`](cleanup) — it reaps the waiter and drops the
//!   session's interests/subscriptions (feeding the card-08 refcount so no zombie
//!   poller outlives the session).
//! - [`install`](install) wires all three into `~/.claude/settings.json` (when that
//!   file exists — else it prints the snippet and says why), merging rather than
//!   clobbering.
//!
//! The other half of setup is [`skills`](skills), which installs the embedded
//! `agent-mailbox` skill into the user's Claude Code skills dir. The hooks make
//! wake infrastructure; the skill teaches the agent the loop it wakes into. Both
//! default under the same home ([`home`](home)) — one convention, one override.
//!
//! # Why this crate holds the logic (and shells out for the bridge)
//!
//! Business logic lives here so it is unit-testable in isolation; the `mailbox`
//! binary is a thin dispatcher that owns the socket client and hands this crate
//! the resolved paths. Because the dependency edge is one-way (`mailbox` →
//! `mailbox-harness`, never back), this crate reaches the card-05 waiter by
//! *executing* `mailbox wait` rather than linking it — the same reuse the design
//! calls for, across a process boundary.
//!
//! # Session identity comes from the hook (settled, card 11)
//!
//! Claude Code passes each hook its payload as JSON on stdin, including
//! `session_id`. [`hook::HookInput::parse`] reads it into a branded
//! [`mailbox_protocol::SessionId`]; the binary exports it to the waiter/CLI as
//! `--session` / `MAILBOX_SESSION_ID`. That settles the previously-open "how does
//! a session name itself" question.

pub mod atomic;
pub mod cleanup;
pub mod home;
pub mod hook;
pub mod install;
pub mod pidfile;
pub mod skills;

use mailbox_protocol::PROTOCOL_VERSION;

/// Confirms the harness crate is linked against the expected protocol version.
pub fn protocol_version() -> u32 {
    PROTOCOL_VERSION
}
