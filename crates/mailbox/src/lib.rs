//! The agent-mailbox bridge library.
//!
//! The bridge is the one process that owns durability. Everything else
//! (adapters, the harness, CLI invocations) speaks the `mailbox-protocol`
//! wire types and never touches the database directly (ADR-0003).
//!
//! The [`storage`] module is the durable core: a single-writer SQLite store
//! behind an async handle.

pub mod storage;
