# ADR-0006: Harness wake loop — self-respawn, waiter-owned pidfile, arm-iff-subscribed

- Status: Accepted
- Date: 2026-07-11

## Context

Claude Code owns the re-arm loop through hooks (card 11, see
[docs/01-wake-and-rearm.md](../01-wake-and-rearm.md)): `SessionStart`/`Stop` run
`mailbox harness arm`, which — for a subscribed session — becomes the card-05
waiter (`mailbox wait`), and `SessionEnd` runs `mailbox harness cleanup`. Three
forces shape how this must be coordinated:

1. **The async-hook timeout.** A background `asyncRewake` hook is killed after its
   `timeout` (default ~10 minutes for command hooks). A truly idle session gets no
   further `Stop`, so a killed waiter would silently un-arm the session.
2. **No zombie waiters / pollers.** Exactly one waiter may be live per session, and
   it must not outlive the session. A departing or unsubscribed session must leave
   no waiter process AND no orphaned adapter poller (the card-08 refcount).
3. **The wake wire is exit-2 stderr.** On exit 2 the harness surfaces the waiter's
   stderr to the agent as the payload-free reminder, so nothing else may write it.

Two HIGH bugs were found and reproduced in review:

- **HIGH#1** — `arm` wrote the pidfile *before* the single-waiter lock existed. On
  an idle first turn, a doomed second arm (which would lose the lock) overwrote the
  pidfile with its own soon-dead pid, leaving the real waiter unrecorded and
  orphaned forever.
- **HIGH#2** — an `arm` that raced `SessionEnd` (interest/subscription already
  dropped) still started a waiter that blocked forever as an orphan.

## Decision

**1. Self-respawn via `execv`.** The waiter runs with a `max_block` shorter than
the async-hook `timeout`. On reaching the bound with no mail it re-execs a fresh
`mailbox wait` (`execv`, same argv). `execv` preserves the PID and starts a fresh
process image, so an arbitrarily long idle stays armed and the per-hook timeout
never lands on a live wait. Each fresh waiter repeats the card-05
open→check-then-block ordering, so a publish during the re-exec gap is caught by
the next waiter's unread check, not missed. `install-hooks` **rejects** a
`max_block_ms` that is not at least a margin below `timeout_secs` (the larger of
10s or 10% of the timeout), because otherwise Claude Code would SIGKILL the waiter
before it could re-exec.

**2. The waiter owns the pidfile, written after the lock.** `arm` no longer writes
the pidfile. The waiter writes it (own pid) only *after* it acquires the
single-waiter advisory lock, and a waiter that fails to acquire the lock
(`AlreadyWaiting`) exits without touching it. So the pidfile always names the one
live lock-holding waiter (fixes HIGH#1). On the clean `Unsubscribed` exit the
waiter removes the pidfile while still holding the lock; on `Woken`/`TimedOut` it
leaves it (a same-pid re-exec re-writes it, and the next `arm`'s waiter overwrites
it under lock). `cleanup` reaps that one stable pid with `SIGTERM` and removes the
pidfile. The pidfile, FIFO, and lock share one filename stem via the single
`SessionId::encode_filename` encoder (in `mailbox-protocol`), so all three key a
session identically.

**3. Arm-iff-subscribed, enforced twice.** `arm` probes the bridge and only arms a
subscribed session (a down/erroring bridge or no subscription → exit 0, no wake).
The waiter *also* re-checks `has_subscription` after taking the lock and self-exits
(`Unsubscribed`, exit 0, pidfile removed) if there is none — catching an `arm`
whose probe passed but whose `SessionEnd` then landed (fixes HIGH#2).

**4. exec-failure and bridge-down degrade safely.** A failed initial arm-exec or
re-exec exits **2** (a wake → the harness re-runs `Stop` and re-arms) rather than
1 (a silent un-arm), after clearing any stale pidfile. A `cleanup` whose
`EndSession` cannot reach the bridge retries with backoff, then defers to the
card-08 TTL sweeper (an un-torn-down interest ages out via `last_seen`) rather
than failing the hook or building a durable pending-end queue.

## Consequences

- Easier: an idle session stays armed across arbitrarily long idles; a departing
  or doomed-arm session leaves no zombie; the wake wire stays clean (`wait`/`arm`
  route their `tracing` to `<db-dir>/harness.log`, not stderr).
- Constrained: `max_block < timeout` is load-bearing and enforced at install time.
- **Load-bearing assumption (CLOEXEC).** Across `execv`, the advisory lock fd must
  be released so the fresh image can re-acquire it. Rust opens files `O_CLOEXEC` by
  default, so the lock fd closes on `execv` and the lock releases; the same-pid
  re-exec then re-locks cleanly. If a future change opened the lock without
  CLOEXEC, the re-exec would deadlock on its own stale lock.
- **Open empirical question.** Whether Claude Code resets the async-hook `timeout`
  when the waiter `execv`s in place (same PID, new image) is undocumented and
  unconfirmed. If it resets, idle survival is unbounded; if not, the waiter still
  survives to the configured `timeout`, after which the next `Stop` re-arms — and
  `--timeout-secs` is tunable. This should be verified against a live Claude Code
  before relying on multi-hour idles.

## Alternatives considered

- **Internal loop instead of re-exec.** An in-process loop never presents the
  harness with a fresh process, so it cannot dodge the per-hook timeout. Rejected.
- **`arm` runs the waiter as a child and re-execs itself.** Needs a process group
  to avoid orphaning the child on re-exec, and `arm`'s subscription re-check would
  race the child. The exec-chain (arm → wait → wait, one PID) is simpler and has no
  child to leak. Rejected.
- **Bridge reaps the waiter.** The bridge does not manage harness processes; the
  pidfile + `SIGTERM` keeps that boundary clean. Rejected.
