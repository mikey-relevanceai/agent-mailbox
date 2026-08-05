# ADR-0010: A daemon restart resumes the watches of live sessions

- Status: **Accepted — but the liveness probe below is amended by
  [ADR-0017](0017-daemon-bumps-the-sentinel.md).** The decision (resume a watch iff
  an interested session is still alive) is in force. The `waiter_alive` pidfile
  probe it names is not: that file no longer exists.
- Date: 2026-07-17
- Amends: design/01 rule 6 (bridge restart), whose deferred session-liveness probe
  this supplies from [ADR-0009](0009-interest-liveness-from-the-waiter-pidfile.md).
  Second instance of the same class of bug as ADR-0009: a fail-safe whose safety
  depended on a live session doing something, in a system where
  [ADR-0008](0008-on-demand-wake-filechanged.md) guarantees it does nothing.

> **Amended by [ADR-0017](0017-daemon-bumps-the-sentinel.md) (2026-08-05).** The
> DECISION here — resume a watch iff an interested session is still alive — is
> unchanged. Only the liveness probe changed: it was the detached watcher's pidfile
> (ADR-0009), and that watcher no longer exists. `reconcile_startup` now takes the set
> of sessions with a live Claude Code process, read once from the process table. It is
> also strictly more accurate: an orphaned watcher's pidfile used to make a dead
> session look alive, so its poller was resumed for nobody.

## Context

design/01 rule 6 says: *"Resume a watch only if at least one interested session is
still alive; otherwise mark stopped. Exact session-liveness probe is
harness-specific; until we have one, default to **do not resume orphan watches**
(fail safe: missed events > zombie API load)."*

`reconcile_startup` implemented the default, not the rule: on daemon start it
marked **every** previously-`Running` watch `Stopped`, cleared its pid, and
resumed nothing. The stated recovery was that a live session would simply
re-`watch`.

**That recovery does not exist.** ADR-0008's headline guarantee is that an idle
subscribed session costs **zero** turns until real mail arrives. An idle agent
will therefore never re-`watch` — and the only event that could give it a turn is
the one the stopped poller would have published. The recovery path requires the
thing whose absence is the problem.

So a daemon restart put the system in a stable, silent, unrecoverable state:

- `watch_interest` rows survive the restart (nothing deletes them), so `interest`
  stays ≥ 1 and `status` reports the watch as wanted.
- The adapter is gone and nothing will respawn it, so no events are ever published.
- The bus subscription survives, so `subscribe` still answers "already subscribed".
- The session's detached watcher survives, blocked on a FIFO that will never be
  kicked.

Every layer reports healthy. The agent is deaf forever.

`Stopped` + `interest > 0` is itself an inconsistent state — `WatchState::Stopped`
is documented as "torn down (last interest gone)" — so the old path did not merely
decline to resume; it wrote a state the model says cannot happen, and left it.

### Observed in production (2026-07-17)

A `mailbox serve` restart stopped all four GitHub PR pollers. Three of the four
interested sessions were still alive with live watcher pidfiles, waiting on PRs
#3830, #3847 and #3848; the fourth (#3785) belonged to a session that was gone.
None of the three could notice or recover. They were only unstuck by re-issuing
`mailbox watch` **on their behalf** from another session — an operator action no
part of the design provides for.

### Why the old default was not fail-safe

"Missed events > zombie API load" is the right trade, but "do not resume" bought
safety against zombies at the price of a *permanent* failure, not a *missed* one.
The asymmetry it assumed — that a resumed poller outlives its sessions, while a
stopped one gets restarted by them — is backwards under ADR-0008: a zombie poller
is self-limiting (the TTL sweeper reclaims it within the hour), whereas a stopped
poller under a live watcher is terminal for that session's whole life.

## Decision

**Resume a watch iff some session holding an interest in it has a live watcher**,
using ADR-0009's probe (`waiter_alive` — `kill(pid, 0)` on the detached watcher's
pidfile). This is rule 6 as written; only its deferred probe was missing, and
ADR-0009 built it.

- **Resumed** via the idempotent `Supervisor::ensure_running`, which owns the state
  transition, so a stale pid is replaced by the new child's rather than cleared.
  This covers both a watch left `Running` by the previous daemon and one left
  `Stopped` **with interest attached** — healing the inconsistent state the old
  path created, which would otherwise never heal.
- **Not resumed** → a `Running` watch is marked `Stopped` (clearing the previous
  daemon's stale pid). Unchanged fail-safe: an interest whose session cannot be
  proven alive gets no poller. The TTL sweeper reclaims the leftover interest.
- **`Desired`** is left alone when not resumed: it carries no stale pid, so
  demoting it would be churn.
- **`Failed`** is left alone: it means the supervisor exhausted its restart budget
  and gave up, and a restart is not evidence the adapter stopped crashing.
  Re-`watch` is the deliberate way back.

`reconcile_startup` therefore moves **after** the supervisor is constructed in
`serve::run` (it now spawns through it) and takes `(&Storage, &Supervisor,
&Path)`, returning `SupervisorError`.

## Consequences

- A daemon restart is no longer a silent, permanent deafness event for every idle
  session watching an entity. This is the property the restart path always claimed.
- The invariant is now uniform across both reclaim paths: **an interest lives, and
  its adapter runs, iff its session's watcher lives.** The TTL sweeper (ADR-0009)
  and the startup reconcile now read liveness from the same signal, so they cannot
  disagree about whether a session is alive.
- A `waiter_alive` false positive (PID reuse — see its docs) costs one adapter that
  the TTL sweep reclaims once the pidfile ages out. Strictly the cheaper error: the
  failure it replaces was silent and permanent.
- Restart cost now scales with live interest: a restart respawns one adapter per
  wanted entity, rather than zero. That is the intended load — it is exactly what
  was running before the restart.
- A watch whose session died between the restart and the probe is not resumed, and
  its interest is reclaimed by the TTL sweeper as before.

## Alternatives considered

- **Keep "never resume"; have agents re-`watch` on wake.** Circular: the wake it
  depends on requires the poller it is meant to restart. This is precisely the
  ADR-0009 mistake — a refresh path the skill tells agents never to take.
- **Re-`watch` from the `Stop` hook.** Would work for a session taking turns, but
  not for the idle sessions this is about (no `Stop` fires), and it re-introduces a
  per-turn bridge call that ADR-0008 removed. The daemon already knows who wants
  what; asking the agent to restate it is the wrong direction.
- **Resume every watch with `interest > 0`, without probing.** Simpler, but
  resurrects pollers for sessions that hard-died without a `SessionEnd` — the exact
  zombie rule 6 exists to prevent. The probe costs one `kill(pid, 0)` per watch.
- **Have the harness heartbeat liveness into the bridge.** Rejected by ADR-0009 for
  the same reason: it puts a timer back in the harness and costs turns, when the
  watcher pidfile already tracks the session.
