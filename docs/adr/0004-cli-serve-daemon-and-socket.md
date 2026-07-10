# ADR-0004: `serve` daemon + Unix-socket CLI clients

- Status: Accepted
- Date: 2026-07-10

## Context

[ADR-0003](0003-single-writer-sqlite.md) says one logical writer owns the
database and everyone else "speaks the protocol" — but left the *process*
lifecycle open ("prefer a long-lived bridge"; the "bridge down" window is owned
by a later card). Card 06 settles that surface: the `mailbox` CLI is the single
user/agent/adapter entry point, and it needs a concrete answer to "where does
the writer live, and what happens when it isn't running?"

Constraints already fixed elsewhere: no TCP, Unix socket or CLI only (AGENTS.md,
ADR-0001); single writer (ADR-0003); reuse the card-02 `mailbox-protocol` +
NDJSON framing where it fits.

## Decision

**A long-lived `mailbox serve` process owns the single `Storage` writer and the
`Waker`, and binds a user-scoped Unix socket.** The socket path is derived from
the resolved storage path (`<db-parent>/mailbox.sock`), exactly like the waiters
directory, so clients and daemon agree on one location under every storage
override with no extra env var. The socket is created `0600` and its directory
`0700` (owner-only).

**All mutating/reading commands are one-shot socket clients.** `publish`,
`subscribe`, `unsubscribe`, `read`, `watch`, `unwatch`, and `status` connect,
send one request, read one reply, and disconnect. They never open the database.

**When the daemon is down, clients fail loudly.** A failed connect yields a
clear, actionable error — `bridge not running; start it with `mailbox serve`` —
and a non-zero exit. Clients do **not** auto-spawn a daemon and do **not** open
the DB directly. That keeps single-writer *structural* (only `serve` ever holds
a write connection, so there is no cross-process second-writer race) and makes
"bridge down" obvious rather than silently degrading (ADR-0003 consequence).

**`wait` is the sole exception.** It stays a direct read-only client
(`ReadOnlyStore` + blocking FIFO), never touches the socket, and keeps its
card-05 exit-code contract (exit 2 on mail). ADR-0003 already permits read-only
side opens for wake.

**Naming: `watch` / `unwatch`,** not `adapter start`/`stop` — user-facing,
intent-declaring, matching `design/01`. This resolves that design's open
question.

**CLI ↔ daemon protocol.** The socket speaks the same versioned NDJSON dialect
as the adapter protocol (same `version` field, same reject-newer
`check_version`) and reuses the `mailbox-protocol` **domain types** (`Topic`,
`AdapterId`, `Event`, `Offset`, …) for publish/subscribe/read payloads. But the
request/response *envelope* is a small daemon-local enum in the `mailbox` binary
(`control.rs`), not an addition to `mailbox-protocol`, for two reasons:
`watch`/`unwatch`/`status` are CLI/daemon control ops with no place in the
adapter-facing pub/sub set (keeping that protocol clean is an AGENTS.md hard
boundary), and one-shot clients must carry `SessionId` per request — a field the
persistent-connection `Message` set deliberately omits.

## Consequences

- Predictable lifecycle, no spawn races. Auto-spawn (a client starting the
  daemon on demand) is a possible later enhancement, explicitly out of scope.
- Concurrency is free: a `Storage`/`Bus` clone is just a channel to the one
  writer thread, so the daemon serves each connection on its own task and
  mutations serialize at the writer with no locking in the socket layer.
- `mailbox-protocol` stays the adapter-facing pub/sub contract; CLI control ops
  evolve independently in the binary.

### Cross-process single-writer is a real lock (closes an ADR-0003 deferral)

The daemon takes an exclusive advisory `flock` on `<db-dir>/mailbox.lock` BEFORE
opening the writer and holds it for its whole life. Two `serve` processes on one
DB therefore cannot both become writers — the second fails loudly on the lock.
This closes the cross-process second-writer window ADR-0003 deliberately left to
the daemon-lifecycle card. Because the daemon holds the lock, a leftover socket
node is provably stale and is removed before binding; we never unlink a socket we
have not proven dead.

### Daemon hardening (adversarial review)

The socket is an untrusted boundary even though it is owner-scoped, so the daemon
bounds every resource a client can consume: request frames are read under a hard
byte cap (reject, never buffer — no OOM on a huge line), a per-connection read
timeout drops slow/half-open clients, a semaphore caps concurrent connections,
and repeated `accept()` errors back off (no EMFILE tight-loop). The DB directory
(`0700`) and socket (`0600`) are made owner-only *before* the socket can accept,
and a permissions-hardening failure is **fatal** (refuse to serve rather than
serve world-reachable). Frame cap, read timeout, and connection cap have env
overrides so the bounds are testable without production-scale inputs.

### Publish is at-least-once across a hard crash

On SIGINT/SIGTERM the daemon stops accepting and drains in-flight connection
tasks for a bounded grace period so a committed publish's ack is delivered. But
there is no idempotency key in the MVP, so a client that never receives an ack
(hard crash / SIGKILL between commit and ack) and retries can duplicate an event
— publish is **at-least-once** across that boundary. Edge-triggered adapters
dedup downstream via their stored baselines (design/01); a wire idempotency key
is deliberately deferred.

## Alternatives considered

- **Short-lived CLI opens the DB per command (SQLite locking only).** Re-opens
  the second-writer hazard ADR-0003 rejected; "bridge down" stops being a clear
  signal.
- **Auto-spawn the daemon from the first client.** Spawn races and unclear
  ownership; deferred, not adopted for MVP.
- **Put control ops in `mailbox-protocol`.** Leaks bridge-lifecycle concerns
  into the wire contract every adapter depends on.
- **TCP / localhost HTTP.** Forbidden at v0 (ADR-0001); a Unix socket gives
  owner-only scoping for free.
