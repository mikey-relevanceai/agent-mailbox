//! The agent-mailbox bridge library.
//!
//! The bridge is the one process that owns durability. Everything else
//! (adapters, the harness, CLI invocations) speaks the `mailbox-protocol`
//! wire types and never touches the database directly (ADR-0003).
//!
//! The [`storage`] module is the durable core: a single-writer SQLite store
//! behind an async handle. The [`bus`] module is the session-facing business
//! layer on top of it — publish, subscribe, and cursor-based read semantics.
//! [`watch`] and [`agents`] are the two policies layered on that bus: watching an
//! external entity (a supervised poller per PR) and messaging a peer agent
//! (a per-session inbox topic).

pub mod agents;
pub mod bus;
pub mod clock;
pub mod dashboard;
pub mod host;
pub mod resolver;
pub mod sentinel;
pub mod storage;
pub mod supervisor;
pub mod wake;
pub mod watch;
