# ADR-0003: Single-writer SQLite

- Status: Accepted
- Date: 2026-07-09

## Context

The bridge needs durable topics, events, and per-subscriber cursors. Multiple
processes may want to publish or read (CLI invocations, adapters, harness
waiters, later a status UI). SQLite handles concurrent readers well but wants a
clear writer story; uncoordinated multi-process writers invite lock storms and
subtle corruption/busy loops.

## Decision

**One logical writer owns the database.**

- All mutations (publish, subscribe, cursor advance, adapter registration) go
  through a single writer path inside the bridge.
- Other processes do **not** open the DB for writes. They call the bridge
  (`mailbox …` CLI and/or a local Unix socket) and **queue behind** that writer.
- Reads used for wake/delivery may be served by the same process; if we ever
  allow read-only side opens, they stay read-only and never mutate.
- Prefer a long-lived bridge process for MVP supervision; short-lived CLI
  commands that must mutate still serialize via SQLite locking / the socket API
  so there is still one writer *at a time*, with the durable design assuming a
  single owner process when the daemon is up.

Implementation sketch (not normative API): a dedicated OS thread or tokio task
holding the `rusqlite::Connection`, with a channel of write requests; callers
await a oneshot reply. Busy timeouts and WAL mode are expected.

## Consequences

- Adapter and harness code stay simple: speak the protocol, never touch the DB
  file.
- Throughput is bounded by one writer — fine for local agent volumes.
- We must design process lifecycle so “bridge down” is obvious (publish fails
  loudly) rather than spawning ad-hoc writers.
- Matches SQLite’s strengths for a local-first product.

## Alternatives considered

- **Multi-process rusqlite with `busy_timeout` only** — works until it doesn’t;
  harder to reason about under adapter storms.
- **sqlx pool with many writers** — still serializes under the hood for SQLite;
  doesn’t remove the need for discipline, adds async pool complexity.
- **One SQLite file per topic/agent** — avoids some contention; makes
  multi-subscriber topics and global adapter registry awkward.
