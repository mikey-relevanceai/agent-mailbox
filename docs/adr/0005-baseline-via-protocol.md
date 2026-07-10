# ADR-0005: Edge-triggered adapter baseline persists via the protocol

- Status: Accepted
- Date: 2026-07-10

## Context

An edge-triggered adapter (the card-10 `github-pr` poller) must be
**exactly-once**: it baselines the watched entity's state on its first poll and
publishes only on *transitions* thereafter. For a restart not to re-fire an edge
it already published, the baseline has to **persist** across the adapter process's
life.

Two constraints fix where and how it can live:

- **Adapters never touch the bridge's SQLite** ([ADR-0001](0001-rust-bridge-subprocess-adapters.md)):
  an adapter is a separate, same-user process treated as untrusted content. It
  speaks `mailbox-protocol` NDJSON on stdio and must not open the database, so the
  old `~/.claude/agent-ipc/watchers/*.state` file approach (adapter-owned state) is
  out.
- **Single-writer SQLite** ([ADR-0003](0003-single-writer-sqlite.md)): only the
  `serve` daemon mutates the store. The baseline row (`adapter_baseline`) is the
  daemon's to write.

So the durable baseline lives in the bridge, but only the adapter knows when it
changed. We need a channel between them that does not hand the adapter a second
writer nor a request/response RPC (the adapter→host link is stdout-only NDJSON).

## Decision

**The baseline round-trips through the protocol, one-way in each direction — no
request/reply channel.**

- **Startup (storage → config → adapter).** At spawn the supervisor reads
  `storage.get_baseline(watch_id)` and merges it into the adapter's config under a
  `baseline` key (`null` when never baselined). The adapter starts from its last
  snapshot, so a restart resumes edge-detection where it left off. The *resolver*
  stays storage-free; only the supervisor reads storage.
- **Updates (adapter → `Baseline` stdout → host → storage).** A new
  `mailbox_protocol::Message::Baseline { value }` variant carries an opaque
  snapshot. After each poll that changes its in-memory baseline, the adapter emits
  one `Baseline` line. The card-07 subprocess host relays it to a `BaselineSink`
  the supervisor bound to `(storage, watch_id)`, which calls
  `storage.set_baseline`. This mirrors exactly how a `Publish` line is forwarded to
  the `Bus` rather than written to SQLite by the transport — the transport stays
  storage-free, and provenance (which watch a baseline belongs to) is host-stamped
  from the spawn, never adapter-asserted.

The baseline `value` is opaque `serde_json::Value`: the protocol, transport, and
storage never interpret it; the adapter owns its schema.

## Consequences

- **Restart does not re-fire** for the common path: a graceful shutdown SIGTERMs
  the adapter (it finishes its in-flight poll and flushes its final `Baseline`),
  and the host drains stdout before reaping, so the persisted baseline is current.
- **Delivery is at-least-once across an *ungraceful* termination** (SIGKILL / OOM /
  power loss). There is an unavoidable window between publishing an edge and
  emitting the baseline that reflects it; a hard kill inside that window means the
  resumed adapter re-fires that one edge. This is consistent with the card-06
  publish-at-least-once stance and is tolerable for a *wake* bus: a duplicate wake
  makes the agent re-check and find the same state, not act twice on a phantom. We
  deliberately do **not** add a two-phase commit or a wire idempotency key at v0.
- **The transport stays decoupled from storage.** `Baseline` handling is a sink
  interface, addable without the transport importing bridge internals — the same
  boundary `Publish`→`Bus` already respects.
- Adapters that don't need a baseline (the stub) simply ignore the injected
  `baseline` field and never emit a `Baseline` line.

## Alternatives considered

- **Adapter writes its own state file / SQLite.** Rejected: violates ADR-0001
  (adapters never touch bridge storage) and ADR-0003 (single writer), and
  re-introduces the scattered `*.state` files the bridge was meant to centralize.
- **A request/response protocol** (adapter asks the bridge for its baseline, acks
  each persist). Rejected: the adapter→host link is one-way stdout NDJSON; adding a
  back-channel and an ack protocol is far more machinery than a wake bus needs, and
  the one-way flow is sufficient for the exactly-once-modulo-hard-crash guarantee
  we're targeting.
- **Two-phase commit / wire idempotency key** to close the ungraceful-termination
  window. Deferred: a duplicate wake is cheap and self-correcting, so the
  complexity isn't justified at v0 (revisit if a downstream consumer ever *acts*
  on an edge non-idempotently).
