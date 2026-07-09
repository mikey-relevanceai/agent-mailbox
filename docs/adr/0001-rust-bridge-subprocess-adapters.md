# ADR-0001: Rust bridge, subprocess adapters, local-only v0

- Status: Accepted
- Date: 2026-07-09

## Context

Agent mailbox needs a local durable topic bus with pluggable adapters and
harness-owned wake. Distribution, extensibility, and security all matter from
day one. An earlier prototype (`agent-ipc`) proved durable log + kick + arm, but
not multi-subscriber topics or automatic re-arm.

## Decision

1. **Language:** Rust for the bridge CLI / core (not Go).
2. **Adapters:** Separate processes speaking a versioned protocol; transport
   behind an abstraction aimed at WASM/WASI later.
3. **Network (v0):** No TCP listen — CLI and/or user-scoped Unix socket only.
4. **Storage:** SQLite via `rusqlite` (`bundled`), **single writer** in the
   bridge ([ADR-0003](0003-single-writer-sqlite.md)).
5. **Wake:** Claude Code first via hook-owned `asyncRewake`; Codex has no
   equivalent yet (manual arm fallback).
6. **Security shape:** Payload-free wake; treat adapter event bodies as
   untrusted content; prefer a separate ingress process if/when webhooks appear.

MVP crate details: [ADR-0002](0002-mvp-crate-stack.md). MVP product slice:
[design/01-mvp-github-watch](../design/01-mvp-github-watch.md).

## Consequences

- Single static binary is straightforward to distribute.
- Adapters can be any language; they must not link bridge internals.
- Moving to WASI later should not change topics, cursors, or harness code if the
  transport trait holds.
- No remote publish surface until we deliberately add authenticated ingress.
- Codex users get a weaker wake story until upstream ships a primitive.

## Alternatives considered

- **Go bridge** — fine for CLIs; rejected on preference and team taste.
- **In-process adapter plugins** — faster, but couples crash/security domains;
  rejected for v0.
- **NDJSON-only storage** — simple, but awkward for multi-cursor concurrency;
  SQLite preferred as the default assumption.
- **Cloud automations as the core** — spawn new runs; fights local session
  continuity (our differentiator).
