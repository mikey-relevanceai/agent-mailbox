//! The adapter host boundary: how a running adapter is *hosted*, kept separate
//! from what it *says*.
//!
//! # Why a trait here (protocol vs transport, ADR-0001)
//!
//! `mailbox-protocol` is the adapter-facing wire language (`Publish`, cursors,
//! topics). This module is the other half of that split: the *transport* — the
//! machinery that spawns an adapter, feeds it config, and pumps its `Publish`
//! output onto the durable bus. The two must not leak into each other, so the
//! transport lives behind [`AdapterHost`], a replaceable boundary. Today the one
//! implementation is [`subprocess::SubprocessTransport`] (a child process); a
//! future `WasiTransport` would implement the same trait, and swapping it must
//! not change topics, cursors, or harness code (docs/02-tech-stack.md).
//!
//! # What the boundary exposes (and what it hides)
//!
//! Above the trait a caller (the card-08 supervisor) deals only in the typed
//! lifecycle — start with an opaque [`AdapterConfig`], observe [`AdapterHealth`],
//! [`AdapterHost::stop`] it, learn how it [`AdapterExit`]ed. It never sees a
//! `tokio::process::Child`, a pipe, or a signal: those are transport internals.
//! Publishes the adapter emits are forwarded onto the bus *inside* the transport
//! (see [`subprocess`]), so the adapter never touches SQLite or the wake logic —
//! exactly the ADR-0001 boundary. The event bodies stay opaque and are forwarded
//! verbatim, never interpreted.
//!
//! # Why `start` is not on the trait
//!
//! Starting is inherently transport-specific: a subprocess needs a program path
//! and argv; a WASI guest would need a module and a capability grant. There is no
//! honest shared `start` signature, so each transport exposes its own
//! constructor (e.g. [`subprocess::SubprocessTransport::start`]) and the trait
//! covers only what *is* uniform once an adapter is running: identity, health,
//! stop, and exit. That is the surface a supervisor programs against regardless
//! of transport.
//!
//! # Extensibility (acks / health without a breaking change)
//!
//! The trait is intentionally small and its outputs are dedicated types, not
//! bare primitives. Health is already a snapshot struct with room for more
//! counters; an ack channel (per-`Publish` acknowledgement, once the protocol
//! grows one) can be threaded through the transport's forwarding path and
//! surfaced as a new trait method or a field on a new return type — additively,
//! without changing the existing signatures.

pub mod subprocess;

use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};

use serde_json::Value;

use mailbox_protocol::AdapterId;

pub use subprocess::SubprocessTransport;

/// A running, hosted adapter instance — the transport-agnostic lifecycle a
/// supervisor programs against.
///
/// Implemented by [`SubprocessTransport`] now and a future `WasiTransport`. The
/// swap boundary of ADR-0001: nothing here mentions a process, a pipe, or a
/// signal, so a different transport can satisfy it unchanged.
///
/// `stop`/`wait` take `self` by value: an adapter is stopped or awaited exactly
/// once, and consuming the handle makes "use after stop" unrepresentable. The
/// futures are `Send` so a supervisor can drive them on any runtime worker.
///
/// # Transport-owned error ([`Self::Error`])
///
/// The failure type is an associated type, not a fixed enum, so no transport's
/// internals leak across the boundary: `stop`/`wait` return
/// `Result<AdapterExit, Self::Error>`, and a future `WasiTransport` chooses its
/// own error. The one requirement is the standard `Error + Send + Sync +
/// 'static` bound, so a supervisor can box or log any transport's error without
/// depending on that transport's crates (the subprocess impl deliberately keeps
/// even `nix` errno values out of its public error — see [`subprocess`]).
///
/// # Object safety
///
/// This trait is deliberately **not** `dyn`-compatible: the RPITIT `stop`/`wait`
/// futures and the `self`-by-value receiver (which is what makes use-after-stop
/// unrepresentable) both preclude `dyn AdapterHost`. That is a considered
/// trade-off, not an oversight. Heterogeneous supervision (card 08) should reach
/// for **enum dispatch** (a `Adapter { Subprocess(SubprocessTransport), … }`
/// wrapper) or generics, never `Box<dyn AdapterHost>`.
pub trait AdapterHost {
    /// How this transport reports a failed stop/wait. Kept opaque behind the
    /// standard error bound so no transport-specific type crosses the boundary.
    type Error: std::error::Error + Send + Sync + 'static;

    /// The identity this adapter publishes under. Stamped from the spawn, not
    /// from anything the adapter self-reports (provenance is controlled by who
    /// started it — see [`subprocess`]).
    fn adapter_id(&self) -> &AdapterId;

    /// The OS process id, for supervision and orphan checks. `None` for a
    /// transport with no pid concept (WASI later).
    fn pid(&self) -> Option<u32>;

    /// A cheap snapshot of the adapter's forwarding health, for a supervisor to
    /// decide whether a chatty-but-broken adapter should be torn down.
    fn health(&self) -> AdapterHealth;

    /// Stop the adapter and reap it, returning how it exited. Implementations
    /// must terminate the instance (graceful first, then forceful) and always
    /// reap so no orphan/zombie — nor any descendant it spawned — is left behind.
    fn stop(self) -> impl Future<Output = Result<AdapterExit, Self::Error>> + Send;

    /// Wait for the adapter to exit on its own — e.g. a finite adapter that
    /// publishes its events and returns — forwarding publishes until it does,
    /// then reap and report the exit.
    fn wait(self) -> impl Future<Output = Result<AdapterExit, Self::Error>> + Send;
}

/// Opaque adapter configuration, handed to the adapter at start.
///
/// The host does **not** interpret it: each adapter defines its own config
/// schema, and this is just JSON the transport delivers verbatim — consistent
/// with the opaque-body principle (ADR-0001). Delivering *structured* config
/// (rather than argv/env) is a deliberate choice: it is extensible and rides the
/// same NDJSON line discipline the adapter already speaks (see [`subprocess`]
/// for how it reaches the child).
#[derive(Debug, Clone, PartialEq)]
pub struct AdapterConfig(Value);

impl AdapterConfig {
    /// Wrap a JSON value as adapter config. `Null` is a valid "no config" value.
    pub fn new(value: Value) -> Self {
        Self(value)
    }

    /// The underlying JSON, for a transport that needs to serialize it.
    pub fn value(&self) -> &Value {
        &self.0
    }
}

/// How a hosted adapter terminated.
///
/// A sum type, not a bare `i32`: "exited with code N" and "was killed by signal
/// S" are genuinely different outcomes a supervisor reasons about differently
/// (a signal exit under `stop` is expected; a signal exit under `wait` is a
/// crash). Modelled explicitly so a caller pattern-matches rather than decoding
/// a magic number (type-driven design).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdapterExit {
    /// The process returned this status code (0 = clean).
    Exited { code: i32 },
    /// The process was terminated by this signal — our SIGTERM/SIGKILL during
    /// [`AdapterHost::stop`], or a crash signal otherwise.
    Signalled { signal: i32 },
    /// Neither a code nor a signal was reported. Not expected on unix, but an OS
    /// exit status permits it, so it is represented rather than panicked on. Each
    /// transport builds this sum type from its own exit representation (the
    /// subprocess mapping lives in [`subprocess`]), keeping this boundary type
    /// free of any OS-process detail.
    Unknown,
}

/// Live counters shared between the transport and its background forwarding
/// task. Internal; the public snapshot is [`AdapterHealth`].
#[derive(Debug, Default)]
pub(crate) struct HealthCounters {
    forwarded: AtomicU64,
    rejected: AtomicU64,
    publish_failures: AtomicU64,
}

impl HealthCounters {
    fn incr(counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_forwarded(&self) {
        Self::incr(&self.forwarded);
    }

    pub(crate) fn record_rejected(&self) {
        Self::incr(&self.rejected);
    }

    pub(crate) fn record_publish_failure(&self) {
        Self::incr(&self.publish_failures);
    }

    pub(crate) fn snapshot(&self) -> AdapterHealth {
        AdapterHealth {
            forwarded: self.forwarded.load(Ordering::Relaxed),
            rejected: self.rejected.load(Ordering::Relaxed),
            publish_failures: self.publish_failures.load(Ordering::Relaxed),
        }
    }
}

/// A snapshot of an adapter's forwarding health.
///
/// The point of `rejected` is the AC2 health signal: one bad line is not fatal
/// (the transport skips it and keeps reading), but a supervisor watching this
/// count climb can decide the adapter is broken and tear it down. A dedicated
/// struct rather than a tuple so new signals (last-error, restart count) can be
/// added without breaking callers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct AdapterHealth {
    /// `Publish` lines successfully forwarded onto the bus.
    pub forwarded: u64,
    /// Lines rejected without forwarding: malformed/oversized/non-UTF-8 frames,
    /// and well-formed-but-non-`Publish` messages. Skipped, never fatal.
    pub rejected: u64,
    /// `Publish` lines that decoded fine but whose bus append failed (e.g. the
    /// store is down). Distinguished from `rejected` because the fault is the
    /// bridge's, not the adapter's.
    pub publish_failures: u64,
}
