# Tech stack

Decisions for the bridge and how adapters talk to it. Formalized as
[ADR-0001](adr/0001-rust-bridge-subprocess-adapters.md).

## Language

**Rust** for the bridge (`mailbox` CLI / optional local daemon).

Reasons: single static binary for distribution, strong control over FS and
concurrency, and a natural home for a later WASM/WASI host. Go was considered
and rejected.

## Layout

Scaffolded as a Cargo workspace (edition 2024):

```text
agent-mailbox/
  Cargo.toml           # workspace root
  crates/
    mailbox/           # CLI + bridge core
    mailbox-protocol/  # shared types: publish, subscribe, events, cursors
    mailbox-harness/   # Claude Code hook helpers (may stay thin shell + calls)
  adapters/            # reference adapters (any language; speak the protocol)
  docs/
```

Exact module split inside crates can move; the important boundary is **protocol vs transport**.

## Extensibility: adapters

### v0 — subprocess

Adapters are separate processes. They do not link against bridge internals.
They communicate through a small, versioned protocol:

- **Publish:** `mailbox publish …` (CLI) and/or a Unix socket / stdin JSON line
  protocol with the same messages.
- **Lifecycle:** bridge (or a supervisor) may spawn/stop adapters; adapters may
  also be started by hand for experiments.

Early adapters only need to **speak the protocol correctly**. Polling GitHub (or
anything real) is optional; a stub that publishes a fake edge on an interval is
enough to test wake, cursors, and multi-subscriber fan-out.

### Abstraction boundary

Transport is behind the `AdapterHost` trait (settled in card 07,
`crates/mailbox/src/host/`):

```text
AdapterHost
  ├─ SubprocessTransport   ← v0 (implemented)
  └─ WasiTransport         ← later
```

Everything above that boundary sees only:

- start / stop adapter instance
- deliver config (an opaque JSON value the host never interprets)
- receive `Publish` (forwarded onto the bus inside the transport; later: acks /
  health — `AdapterHealth` already exists)

The trait's error is an associated `type Error`, so no transport's internals
(e.g. `nix` errno) leak across the boundary. It is deliberately **not**
`dyn`-compatible (RPITIT futures + `self`-by-value); heterogeneous supervision
(card 08) uses enum dispatch or generics, not `Box<dyn AdapterHost>`.

`SubprocessTransport` spawns each adapter as its own **process-group leader** and
signals the whole group (SIGTERM → grace → SIGKILL), so an adapter that spawns a
grandchild (a helper, a `gh` poller) is torn down whole — no orphans, upholding
the "no zombie pollers" invariant.

Adapters never import bridge storage or wake logic. Swapping subprocess → WASI
should not change topics, cursors, or harness integrators.

### Later — WASM/WASI

Goal: run untrusted or semi-trusted adapter code without `npm i -g`-shaped
supply-chain risk.

WASI adapters would:

- run in a sandbox (no ambient FS/network unless granted)
- call host functions that map to the same publish/subscribe protocol
- be distributed as `.wasm` artifacts the bridge verifies/loads

Capabilities (HTTP, `gh` auth, clocks) become **explicit imports**, not “whatever
the process user can do.” Polling adapters that need network get a narrow
allowlist; pure transformers get almost nothing.

Subprocess remains valid forever for “I already trust this binary / script.”

## Security posture by process

| Component | Process | Trust |
|---|---|---|
| Bridge core | `mailbox` | Trusted; owns DB, cursors, kicks |
| Harness hooks | Claude hook / child of session | Trusted per user session |
| Subprocess adapter | separate OS process | Same user; treat events as untrusted *content* |
| WASI adapter (later) | sandboxed guest | Least privilege; host mediates I/O |
| Ingress (later, if webhooks) | separate process preferred | Hostile network; verify then enqueue only |

v0: no TCP listen. CLI and/or user-scoped Unix socket only.

## Storage

**SQLite** via `rusqlite` (`bundled`) under `~/.agent-mailbox/` (or project
override). **Single writer** — all mutations go through the bridge; other
processes queue via CLI/socket ([ADR-0003](adr/0003-single-writer-sqlite.md)).

Crate choices for MVP: [ADR-0002](adr/0002-mvp-crate-stack.md).

## MVP target

Parity with the existing `agent-ipc` / `agent-ipc-github` skills: supervised
GitHub PR watch, edge-triggered publish, harness-owned wake, **no zombie
pollers**. Design: [design/01-mvp-github-watch.md](design/01-mvp-github-watch.md).

## Early test bar

1. Stub adapter publishes on a schedule via the subprocess protocol.
2. One or more sessions subscribe to the same topic.
3. Claude harness wakes the idle session without an agent re-arm.
4. Delivery cursor advances so mid-turn publishes surface on the next `Stop`.
5. Two subscribers each see the event with independent cursors.
6. GitHub watch: one process per PR; two sessions share it; first SessionEnd
   leaves the poller up; last interest gone stops it; no orphan resume.

## Non-goals (for now)

- In-process Rust plugin ABI for adapters
- Cloud bus / multi-tenant routing
- Codex `asyncRewake` parity (manual arm fallback only)
- Real adapter capability surface beyond “can publish”
