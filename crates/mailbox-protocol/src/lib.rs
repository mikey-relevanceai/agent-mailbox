//! Shared wire and domain types for agent-mailbox.
//!
//! Adapters and the bridge speak this protocol. Transport (subprocess today,
//! WASI later) stays behind a separate host boundary — see `docs/02-tech-stack.md`.

/// Protocol schema version carried on the wire so older clients fail loudly.
pub const PROTOCOL_VERSION: u32 = 1;
