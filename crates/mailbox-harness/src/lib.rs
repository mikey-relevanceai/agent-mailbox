//! Harness-side helpers for arming and waking agent sessions.
//!
//! Claude Code uses hook-owned `asyncRewake` waiters; see `docs/01-wake-and-rearm.md`.

use mailbox_protocol::PROTOCOL_VERSION;

/// Confirms the harness crate is linked against the expected protocol version.
pub fn protocol_version() -> u32 {
    PROTOCOL_VERSION
}
