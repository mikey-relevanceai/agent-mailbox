# ADR-0017: The daemon bumps the sentinel directly — no per-session watcher, no FIFO

- Status: Accepted
- Date: 2026-08-05
- Amends: [ADR-0008](0008-on-demand-wake-filechanged.md) (the sentinel, the
  `FileChanged` hook and the anti-loop all survive; the detached watcher that sat
  between the daemon and the sentinel does not).
- Supersedes: [ADR-0009](0009-interest-liveness-from-the-waiter-pidfile.md) (the
  liveness signal it chose — the watcher's pidfile — no longer exists, and was worse
  than the one that replaces it).

## Context

ADR-0008 made an idle session cost zero model turns by putting a file between the
bridge and Claude Code: the bridge changes a per-session sentinel, Claude Code's
`FileChanged` hook fires even against an idle session, and the hook exits 2 only when
the store confirms genuine unread mail. That part works, and none of it changes here.

What did not work was the machinery ADR-0008 built to get a kick from the daemon into
that file. Wakeability was an emergent property of **six** live components:

```text
daemon → FIFO → per-session detached watcher → sentinel → watchPaths → FileChanged hook
```

Every one of them could fail silently, and every failure produced the same
indistinguishable symptom: the agent goes deaf. The three middle links existed only to
carry a signal from a process that had just committed the event to a file that process
could have written itself.

The costs were measurable on one developer machine:

- **125** detached watcher processes, one per session that had ever started.
- **~1,700** leaked files under `waiters/` — a FIFO, a lockfile and a pidfile per
  session, none of which `SessionEnd` removes when a terminal is simply closed.
- An **orphaned watcher outlives the agent it belongs to**, which is what made
  ADR-0009's liveness signal wrong: a session whose Claude Code process had exited
  still read as alive, so its adapter was kept polling for nobody.

The watcher also needed its own resilience apparatus — an EINTR retry around `poll`, a
phantom-EOF guard, a single-waiter `flock` so two `SessionStart`s could not steal each
other's kick bytes, a pidfile written only under that lock, and a `Stop`-hook respawn
for when it died anyway. All of it was in service of a step that did not need to
exist.

## Decision

**The `serve` daemon writes the subscriber's sentinel itself, inside the publish it is
already serving.** The FIFO, the watcher process, the advisory lock, the pidfile and
the `waiters/` directory are deleted.

The wake path becomes:

```text
publish → daemon writes the sentinel → FileChanged → wake hook exits 2 → agent wakes
```

Three consequences follow directly:

1. **The wake is synchronous with the publish.** `mailbox publish` does not return
   until every subscriber's sentinel has been written. There is no third process whose
   scheduling could interleave, so the missed-kick race ADR-0008's open→check→block
   ordering existed to close cannot occur at all.
2. **The daemon writes each subscriber's WHOLE unread topic set**, not just the topic
   it is publishing to. This is what the watcher did, and what the ADR-0012
   turn-boundary re-trigger does; writing only the new topic would make mail the agent
   was already sitting on appear to vanish from the file on the next unrelated publish.
3. **Liveness comes from the process table** ([`doctor::live_claude_sessions`], added
   for ADR-0016): Claude Code carries the session id in its own argv (`--session-id`
   on create, `--resume` on reopen). This replaces the pidfile for all three of its
   consumers — `mailbox agents` reporting, the TTL sweep's interest refresh, and the
   supervisor's resume/retry decisions — and is strictly more accurate, because it
   tracks the agent rather than an artefact that outlives it.

Two things had to be **added**, each replacing something the watcher did:

- **`session-start` arms the sentinel** — writes the session's current unread set,
  creating the file — *before* printing the `watchPaths` that registers a watch on it.
  Claude Code watches a path; the daemon's writes are MODIFY events, so the file must
  exist first or the very first write is a CREATE the watch may not deliver. This also
  restores the level-triggered arm ADR-0008 got from the watcher's prime step: a
  session starting (or resuming) on top of unread mail wakes for it rather than
  waiting for the next publish.
- **The `Stop` hook re-arms a MISSING sentinel.** That is the honest replacement for
  "respawn the dead watcher": the sentinel is now the only per-session artefact the
  wake path has, so its disappearance is the one deafness a per-turn hook can still
  heal. It costs one `stat` per turn.

The `Stop` hook is renamed `ensure-watcher` → **`turn-end`**, because there is no
watcher to ensure. Its three jobs are now what its name says: close the turn
(ADR-0016), re-register the inbox (ADR-0013), re-arm and re-trigger (ADR-0012).

The hook set stays at **five** — `SessionStart`, `Stop`, `UserPromptSubmit`,
`FileChanged`, `SessionEnd` — and exactly one of them (`FileChanged`) can wake
anything.

## Consequences

**Easier.**

- One background process per machine (the daemon) instead of one per live session.
- The `wake` module is 1,097 → ~430 lines and has no Unix IPC in it at all: no
  `mkfifo`, no `poll`, no `flock`, no signals.
- `SessionEnd` reaps nothing — teardown is removing a directory and one socket call.
- Most wake tests assert directly instead of polling, because the write is synchronous
  with the publish.
- A dead session's adapter is now reclaimed, because liveness stopped lying.

**Harder / constrained.**

- The daemon does one extra read per subscriber per publish (that subscriber's unread
  topic set, the same predicate the wake hook uses). Bounded by subscriber count.
- The daemon must be able to resolve the sentinel root. It resolves it once at
  startup and **refuses to serve** if it cannot: a bridge whose whole purpose is
  waking agents should fail loudly rather than run deaf (ADR-0004's stance).
- Every test that starts a daemon must point `MAILBOX_SENTINEL_ROOT` at a tempdir.
  Previously only the hooks wrote sentinels; now the daemon does, so a test that
  forgot would write into the developer's real `~/.mailbox`.

**Unchanged residual (stated, not fixed).** Mail that arrives while the DAEMON is down
bumps nothing. Recovery is what it was before: the session's next turn boundary
re-triggers for it (ADR-0012), and its next `SessionStart` re-arms from unread. A
session that never takes another turn and is never restarted stays deaf — the accepted
limit of a design with zero spurious wakes.

**Leftovers on existing machines.** Nothing cleans up an existing `waiters/` directory
or kills the 125 already-running watchers. `pkill -f 'mailbox harness watch'` and
`rm -rf ~/.agent-mailbox/waiters` do it; a future `mailbox` release will not recreate
either.

## Alternatives considered

- **Keep the watcher, fix its failure modes.** Rejected: the failure modes were not a
  bug list, they were the cost of the extra process. Each fix (EINTR retry, phantom-EOF
  guard, single-waiter lock, Stop-hook respawn) added a component that could itself
  fail silently. Two links had already been deleted from this path in the preceding
  week for the same reason.
- **Have the daemon write only the published topic.** Simpler and one read cheaper, but
  it would make the daemon's sentinel content disagree with the `Stop` hook's, so an
  agent sitting on mail for topic A would see A disappear when B was published. The
  sentinel's content is diagnostic rather than authoritative — the hook re-checks the
  store — but a diagnostic that contradicts itself is worse than one extra read.
- **Keep the pidfile as the liveness signal.** Rejected: it is the signal this ADR is
  deleting the writer of, and it was already known to be wrong in the damaging
  direction (an orphan reads as alive).
- **Let the daemon signal Claude Code directly.** There is no such interface. The
  watched file IS the interface Claude Code offers for waking an idle session.
