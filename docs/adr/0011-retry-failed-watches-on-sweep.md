# ADR-0011: A give-up is not permanent — the sweep retries failed watches

- Status: Accepted
- Date: 2026-07-20
- Amends: design/01 rule 7 (adapter crash → give up after N failures), and
  [ADR-0010](0010-resume-watches-on-restart.md), whose `Failed`-is-terminal carve-out
  this softens for a watch whose session is still alive.

## Context

design/01 rule 7: after N consecutive failed runs the supervisor gives up — it
publishes an `adapter_gave_up` error event on the entity's topic and marks the
watch `Failed`. Rule 7 exists so a genuinely broken adapter (a deleted PR, revoked
`gh` auth, a bad binary) does not restart-loop forever burning API quota.

But give-up does not distinguish a **permanent** fault from a **transient** one.
The crash counter is just "N consecutive failures"; a brief upstream outage that
happens to span N spawn attempts reaches give-up exactly as a permanent breakage
does. And the window most likely to contain such an outage is a daemon restart:
ADR-0010 now resumes every live session's watches at once, so a GitHub API blip
lasting the few seconds the restart takes will crash-loop *all* of them straight
to `Failed`.

ADR-0010 deliberately does **not** resume a `Failed` watch — a restart is not
evidence an adapter stopped crashing, so re-`watch` is the way back. That is
correct for a permanent fault, but for a transient one it re-creates the precise
failure ADR-0010 set out to eliminate: a healthy watch parked in a dead state,
under a live idle session that (ADR-0008) takes zero turns and will never
re-`watch`, recoverable only by a human noticing and re-issuing the command.

### Observed in production (2026-07-20)

GitHub's API was intermittently failing. A `mailbox serve` restart resumed the
three live PR watches (#3830, #3847, #3848) — ADR-0010 working — but the API was
throwing during the bring-up, so each adapter crashed its full budget and gave up.
All three sat `Failed` under live sessions. `gh` was healthy again minutes later,
yet nothing retried them; they were only recovered by re-issuing `mailbox watch`
on the sessions' behalf from another session.

## Decision

**The periodic sweep retries a `Failed` watch once per interval, iff an interested
session is still alive** — the same watcher-pidfile liveness probe (ADR-0009) the
sweep already uses to refresh interests and the reconcile uses to resume watches.

Mechanics:

- A `Failed` watch whose interest set contains a session with a live waiter is
  retried via the idempotent `Supervisor::ensure_running`. The failure streak was
  cleared when the watch gave up, so this begins a fresh attempt: success →
  `Running{pid}`; failure → the normal backoff path, climbing back to `Failed`.
- The retry pass runs **after** the TTL reclaim within a sweep, so a watch whose
  only session just aged out is not retried.
- A `Failed` watch with **no** live interested session is left `Failed` — no zombie
  retries against an upstream nobody is waiting on. Same invariant as the TTL sweep
  and the ADR-0010 reconcile.

Nothing new is scheduled: this reuses the existing sweep task (default 300s), so
"slow" is inherent — a transient failure heals within one interval, and a genuinely
broken adapter is re-tried at most one backoff burst per interval.

## Consequences

- A transient upstream outage no longer permanently parks a watch. The whole
  ADR-0010 → ADR-0011 pair now holds end to end: a restart resumes live sessions'
  watches, and a give-up during that resume self-heals once upstream recovers.
- The three states a watch can be reclaimed *from* now read liveness from one
  signal — the watcher pidfile — across the TTL sweep (ADR-0009), the startup
  reconcile (ADR-0010), and this retry. They cannot disagree about whether a
  session is alive.
- `Failed` changes meaning: it is now "not running, last attempt exhausted its
  budget", not "terminal until a human intervenes". It still suppresses the tight
  restart loop (retries are one-per-sweep, not one-per-backoff); it no longer
  suppresses recovery.
- A permanently broken adapter under a live session is retried indefinitely at the
  slow cadence, re-publishing an `adapter_gave_up` event each time it gives up
  again. That is bounded, low-rate, and visible — and stops the moment the session
  ends. The alternative (stay silent, stay dead) is the worse failure for a wake
  bus whose entire purpose is not to miss events.

## Alternatives considered

- **Classify the crash cause; only retry transient failures.** More precise — a
  spawn failure (bad binary) is permanent, a `gh` non-zero exit is probably
  transient — but reliable classification across adapters is hard, and a
  misclassified permanent fault would either loop (if called transient) or stay
  parked (if called permanent). The slow, liveness-gated blanket retry gets the
  transient case right without needing to be right about the cause, and bounds the
  permanent case to a low rate. Revisit if the give-up event rate from a genuinely
  broken watch proves noisy.
- **Exponential backoff on the retry itself** (retry a Failed watch less often the
  longer it stays Failed). Reduces the permanent-fault rate further, but adds
  per-watch retry state to the sweep for a case that is already low-rate and
  self-terminating at `SessionEnd`. Not worth the complexity yet.
- **Clear `Failed` on the next `watch` only** (status quo). This is the manual
  recovery ADR-0010 exists to remove; leaving it in place for the transient case
  keeps a silent, human-in-the-loop failure mode.
- **Retry from a dedicated timer rather than the sweep.** A second periodic task
  doing liveness-gated work the sweep already does, for no benefit. Folding it into
  the sweep keeps one cadence and one liveness probe.
