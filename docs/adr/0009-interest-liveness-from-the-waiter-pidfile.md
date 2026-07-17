# ADR-0009: Interest liveness comes from the waiter pidfile, not a TTL clock

- Status: Accepted
- Date: 2026-07-17
- Amends: [ADR-0008](0008-on-demand-wake-filechanged.md) (the zero-idle-turn wake
  design, whose success condition this bug turned into a kill condition) and the
  card-08 TTL sweeper introduced with watch supervision.

## Context

The card-08 TTL sweeper drops a `watch_interest` whose `last_seen` is older than
an hour and stops the adapter of any watch whose interest thereby hits zero. It
exists as the backstop for a session that hard-died without a `SessionEnd` hook —
`cli.rs`'s `run_harness_cleanup` names it as exactly that, the fallback when the
best-effort teardown fails.

Its safety rested on two documented refresh paths (`serve.rs`):

> a live session refreshes its last-seen on `watch` and (card 11) via the harness
> heartbeat, so an interest only ages out once a session has genuinely gone away

**Both were fiction.**

1. The **card-11 harness heartbeat was never wired.** `touch_interest` existed
   end-to-end — a `Storage` method, a `TouchInterest` writer command, tests — with
   zero production callers.
2. The **`watch` refresh is real but unreachable**: `do_add_interest` upserts
   `last_seen`, so re-watching does reset the clock, but the mailbox skill tells
   agents to watch once and never re-arm. The only working refresh path is the one
   agents are told not to use.

So `last_seen` was stamped exactly once, at `watch` time, and never again. Every
interest carried a hard 60-minute fuse that nothing could reset.

### ADR-0008 turned this from a bug into a guarantee of failure

ADR-0008's headline consequence is that "an idle subscribed session now costs
**zero** turns until real mail arrives". An idle session is therefore SILENT by
design — no turns, no requests, nothing that could ever refresh `last_seen`.

That makes silence carry **no information** about whether a session is alive, so a
TTL keyed on the session's own traffic reaps precisely the healthy idle sessions
ADR-0008 exists to enable. The better ADR-0008 worked, the more reliably the daemon
killed the adapter under a live watcher.

It was never only an idle problem. Because *no* operation except `watch` stamped
`last_seen`, a continuously busy session was reaped on the same 60-minute fuse.

### Observed in production (2026-07-17)

A session watching `RelevanceAI/arg#3830` issued a subscribe at `01:33:55` and was
reaped three seconds later:

```
01:33:58  swept stale watch interests removed=1 emptied=1
01:33:58  watch interest hit zero via TTL sweep; stopping adapter watch=14
01:33:58  stopping adapter ... repo=RelevanceAI/arg pr=3830 pid=51989
```

Nothing polled that PR for the next 20 minutes. It recovered only because the agent
happened to re-issue `watch` at `01:54:11`, respawning the adapter at
`generation=6` — the sixth reap/respawn cycle on that watch.

**The failure is silent.** The sweep drops the watch interest but leaves the bus
subscription, so the agent's next `subscribe` answers `already subscribed` while no
adapter exists to produce events. Nothing on the session's side can detect it. This
is the same silent-deafness class the wake-coalescing removal was fixing.

ADR-0008 named `watchPath` persistence as "the first suspect if a long-idle session
is ever observed missing mail". That was wrong, and cost debugging time — the
sweeper says what it did, in the log, at the moment it does it.

## Decision

**An interest lives iff its session's watcher lives.** Liveness comes from the
detached watcher's pidfile, not from a clock counting the session's silence.

Each sweep, before deciding anything, refreshes the interests of every session
whose waiter it can prove is alive:

1. `list_interest_sessions()` — the distinct sessions holding an interest.
2. For each, probe `waiter_alive(waiters_dir, &session)` — the existing helper
   (`agents.rs` already used it to report agent liveness): read
   `<session>.waiter.pid`, `kill(pid, 0)`.
3. Alive → `touch_session_interests(session, now)`, one UPDATE across all of that
   session's interests.
4. Then sweep `last_seen < now - ttl` exactly as before.

The cutoff derives from the same `now` the refresh stamped, so a just-refreshed
interest can never fall below it.

**Why the pidfile.** It is the one artefact that tracks the *session* rather than
its chatter: written by the watcher under the single-instance lock, alive for as
long as the session is wakeable, removed on every watcher exit path, and reaped at
`SessionEnd`. It needs no agent cooperation, adds no timer to the harness, and
costs no model turn — the probe is a `kill(pid, 0)` in the daemon.

**`last_seen` keeps its name and gains its intended meaning**: when the daemon last
had *evidence* the session existed. Nothing about the schema changes.

**The TTL survives, demoted to two honest jobs**: the backstop for a session whose
watcher died with it (no refresh → aged out within the TTL, which is what card 08
always claimed), and the grace period for a transiently-absent pidfile — the spawn
race, and ADR-0008's exit-window respawn transient where a live session briefly has
no pidfile. The sweep interval (300s) stays far below the TTL (3600s) so a session
gets ~12 probes per window and no single missed probe can reap a live watch.

## Consequences

- **Fixed:** a live session is never swept. The regression test plants a live
  pidfile, backdates `last_seen` past the TTL, and asserts the watch survives —
  and that it survives the *next* sweep too, proving the refresh persisted rather
  than the probe merely skipping one pass.
- **The backstop still works.** With the watcher gone the interest ages out and the
  adapter is reclaimed, which the same test asserts by removing the pidfile.
- **`touch_interest` is no longer dead code** — the daemon drives it per-session.
  Card 11's heartbeat is delivered, from the daemon rather than the harness, which
  is the only side that can observe an idle session at all.
- **The sweep now does I/O per interested session per pass** — one small file read
  and a signal probe, every 300s, for sessions that hold watches. Negligible, and
  it buys the correctness of every watch on the box.
- **Residual: PID reuse.** A pidfile naming a dead PID that the OS has recycled
  onto an unrelated process reads as alive, holding an interest (and its adapter)
  open until `SessionEnd` or a daemon restart. This fails toward keeping a watch
  alive rather than silently killing it — the right direction — and it is the same
  exposure `agents.rs` already accepts for liveness reporting.
- **Residual: idle-forever watcher death** (unchanged, ADR-0008). A session that
  never takes another turn and whose watcher then dies is deaf, and its interest
  now correctly ages out rather than holding an adapter open for a session that can
  never hear it. The Stop-liveness hook remains the recovery path for any session
  that takes another turn.

## Alternatives considered

- **Heartbeat on every control request** (`read`/`status`/`subscribe`). The obvious
  fix, and wrong under ADR-0008: an idle agent makes no requests, so this refreshes
  busy sessions and reaps idle ones — precisely backwards. A `Stop`-hook heartbeat
  fails identically, since a truly-idle session fires no `Stop`.
- **The watcher heartbeats itself on a timer.** Small, and needs no cross-crate
  plumbing — but it puts a timer back into the design that exists to have none, and
  it is strictly less truthful than asking whether the process is alive. It also
  makes a wedged-but-alive watcher look healthy.
- **Drop the TTL entirely, sweep purely on the pidfile.** Tempting, but it removes
  the grace that absorbs the spawn race and ADR-0008's exit-window transient, where
  a live session legitimately has no pidfile for a moment.
- **A dedicated `watcher_pid` column on the interest.** Duplicates the pidfile,
  which is already the single source of truth for watcher liveness and is
  maintained by the watcher under its lock. Two sources drift.
