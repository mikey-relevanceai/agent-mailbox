# ADR-0006: Harness wake loop — re-arm exit, waiter-owned pidfile, arm-iff-subscribed

- Status: Accepted
- Date: 2026-07-11 (revised 2026-07-14: self-respawn REPLACED by the re-arm exit)

## Context

Claude Code owns the re-arm loop through hooks (see
[docs/01-wake-and-rearm.md](../01-wake-and-rearm.md)): `SessionStart`/`Stop` run
`mailbox harness arm`, which — for a subscribed session — becomes the card-05
waiter (`mailbox wait`), and `SessionEnd` runs `mailbox harness cleanup`. Three
forces shape how this must be coordinated:

1. **The async-hook timeout.** A background `asyncRewake` hook is killed after its
   `timeout`. A truly idle session gets no further `Stop`, so a killed waiter would
   silently un-arm the session — with nothing left to notice or fix it.
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

### What the original decision got wrong (the wake-lifetime bug)

The first version of this ADR had the waiter **self-respawn**: on reaching its
`max_block` with no mail it re-exec'd itself (`execv`, same argv), on the theory
that a fresh process image would present the harness with a fresh per-hook timeout.
Whether that was true was recorded here as "the one open empirical question".

**It was not true, and the question is now closed — both halves measured:**

1. **`execv` does NOT reset the hook timeout.** `execv` preserves the PID, and the
   harness measures the deadline **per process**, so the clock keeps running across
   the re-exec. The self-respawn bought exactly nothing: the waiter was killed at
   `timeout` regardless. (Observed in the field: a waiter armed at 14:03 was killed
   at ~14:13 — one 600s default timeout later — despite having re-exec'd at 540s.)
2. **A large `timeout` IS honoured.** A hook configured with `timeout: 3600` ran
   well past the 600s default (alive at 703s). There is no hidden 600s cap.

The consequence made this a *silent, permanent* failure rather than a degradation.
The old reasoning ("if it does not reset, the waiter still survives to `timeout`,
after which the next `Stop` re-arms") assumed a `Stop` would come. **A truly idle
session fires no further `Stop`** — that is the definition of idle, and the only
case that matters here. So the killed waiter was never re-armed: subscription
intact, events still durably delivered, `publish` reporting `NoReader`, and a
session that had silently stopped being wakeable. The fingerprint was a **stale
pidfile naming a dead pid**, which made `mailbox agents`/`status` keep reporting a
live waiter that did not exist.

## Decision

**1. The re-arm exit (replaces the self-respawn).** A waiter cannot outrun its hook
timeout, so it must not try. On reaching `max_block` with no mail it **exits 2** —
a wake — carrying a benign, honest notice on stderr:

```text
mailbox: re-arming the waiter (no new mail) — nothing to read; just end your turn
and the Stop hook will re-arm it
```

That exit *guarantees* the thing an idle session otherwise never produces: the
harness wakes the session, the agent ends its turn, `Stop` fires, `arm` runs, and a
**fresh hook process with a fresh timeout** takes over. The session is therefore
never left silently un-armed. The notice must never resemble the mail reminder
(`mail on topic X`) — there is no mail — and the skill tells the agent that the
correct response is to do nothing at all. The wake stays payload-free.

The cost is one benign wake per `max_block` of idle. That is why the default
`timeout` is raised: **a larger timeout means fewer re-arm wakes.**

**2. Timing defaults, and install-time validation.** `install-hooks` writes
`timeout = 3600` (1h, verified honoured) and `max_block = 3_300_000` ms (55 min),
and both remain knobs (`--timeout-secs`, `--max-block-ms`). It **refuses** a spec
whose `max_block` is not below `timeout` by a margin (10% of the timeout, clamped to
10s..=5min): a `max_block >= timeout` silently reintroduces the exact bug above, so
it is a hard install-time failure, never a warning.

**3. The waiter owns the pidfile, written after the lock.** `arm` does not write it.
The waiter writes it (own pid) only *after* acquiring the single-waiter advisory
lock, and a waiter that fails to acquire the lock (`AlreadyWaiting`) exits without
touching it. So the pidfile always names the one live lock-holding waiter (fixes
HIGH#1). On the `Unsubscribed` **and re-arm (`TimedOut`)** exits the waiter removes
the pidfile while still holding the lock — it is about to die, and a pidfile naming a
dead pid is what made the failure invisible. On `Woken` it leaves the pidfile (a
racing `cleanup` must still find something to reap). `cleanup` reaps that pid with
`SIGTERM` and removes the pidfile.

**4. `arm` reaps a stale pidfile.** Before arming, `arm` removes a pidfile whose pid
is dead — the residue of a waiter that was killed or crashed. A **live** pid is left
untouched: that is the winner of a SessionStart-vs-Stop arm race, and this arm's
waiter will correctly lose the lock and exit.

**5. Arm-iff-subscribed, enforced twice.** `arm` probes the bridge and only arms a
subscribed session (a down/erroring bridge or no subscription → exit 0, no wake).
The waiter *also* re-checks `has_subscription` after taking the lock and self-exits
(`Unsubscribed`, exit 0, pidfile removed) if there is none — catching an `arm` whose
probe passed but whose `SessionEnd` then landed (fixes HIGH#2).

**6. exec-failure and bridge-down degrade safely.** A failed arm-exec exits **2** (a
wake → the harness re-runs `Stop` and re-arms) rather than 1 (a silent un-arm), after
clearing any stale pidfile. A `cleanup` whose `EndSession` cannot reach the bridge
retries with backoff, then defers to the card-08 TTL sweeper.

**7. The lifecycle is visible at the default log level.** The bug was
unfalsifiable from outside the process: the four lines that answer "why didn't my
agent wake?" (armed / found-no-subscriptions / woke-with-unread / yielded-at-max-block,
plus the publisher's delivered-vs-no-reader kick counts) are all `info!`, and both
sinks discarded them by default. `harness.log` now defaults to **INFO**, and so does
the `serve` daemon's stderr. The benign `AlreadyWaiting` lock-race loser — the
*expected* outcome of the single-waiter invariant — is logged at `info`, not `error`;
logging it as a failure sent a bug reporter down a dead end.

## Consequences

- Easier: an idle session can no longer be silently un-armed; a killed waiter leaves
  no phantom pidfile behind it; the wake loop is diagnosable from `harness.log` alone.
- Cost: a benign re-arm wake every `max_block` (55 min by default) on a long idle.
  It costs the agent one turn boundary and nothing else — the notice tells it to do
  nothing. Raising `--timeout-secs` (with `--max-block-ms`) reduces the frequency.
- Constrained: `max_block < timeout` is load-bearing and enforced at install time.
- Bounded by the harness: the maximum unbroken idle is one hook `timeout`. We ship
  the verified-safe 1h; a longer probe for a cap between 1h and 8h was still running
  when this landed, so **3600s is the maximum we rely on** until it reports.
- `execv` survives only as `arm`'s way of *becoming* the waiter (one hook process,
  one PID, which `cleanup` reaps). It is no longer used to extend a waiter's life.

## Alternatives considered

- **Self-respawn via `execv` (the previous decision).** Measured not to reset the
  hook timeout, so it extended nothing. **Retired.**
- **Internal loop instead of re-exec.** Same fatal flaw: the hook process is what
  gets killed, so no in-process arrangement outlives it. Rejected.
- **Bridge-side waiter (a daemon-owned watcher instead of a hook process).** Would
  dodge the hook timeout entirely, but the wake wire IS the hook's exit-2 — only a
  hook process can wake an idle session — so the bridge would still need a hook to
  deliver the wake. Rejected for the MVP; revisit if Claude Code ever exposes a
  wake API that is not a hook exit code.
- **Bridge reaps the waiter.** The bridge does not manage harness processes; the
  pidfile + `SIGTERM` keeps that boundary clean. Rejected.
