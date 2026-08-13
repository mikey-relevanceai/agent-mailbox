# ADR-0023: One give-up notice per outage, withdrawn by a recovery event

- Status: **Accepted**
- Date: 2026-08-12
- Amends: [ADR-0011](0011-retry-failed-watches-on-sweep.md) — its retry decision
  stands unchanged; the consequence it flagged for revisit ("re-publishing an
  `adapter_gave_up` event each time it gives up again") is what changes here.
  Also amends design/01 rule 7 (adapter crash → surface an error event).
- Builds on: [ADR-0022](0022-the-wake-carries-a-subject.md) (the new event needs a
  subject, because the subject is all a woken agent sees).

## Context

design/01 rule 7 and ADR-0011 each behave correctly alone and compose into a wake
storm.

Rule 7: after N consecutive failed runs the supervisor gives up, publishes
`adapter_gave_up` on the entity topic, and marks the watch `Failed`. ADR-0011: the
periodic sweep (default 300s) retries a `Failed` watch whose interested session is
still alive, so a transient outage self-heals.

Put together, a fault that outlasts one sweep interval loops: retry → crash streak →
give up → **publish** → `Failed` → next sweep → retry → … Each publish wakes every
subscriber of that topic. The rate is one wake per watch per sweep interval, for as
long as the fault lasts, and the wakes carry no new information — the second notice
says exactly what the first one said.

ADR-0011 saw this coming and accepted it, on the estimate that a genuinely broken
watch is rare and low-rate:

> A permanently broken adapter under a live session is retried indefinitely at the
> slow cadence, re-publishing an `adapter_gave_up` event each time it gives up
> again. That is bounded, low-rate, and visible … **Revisit if the give-up event
> rate from a genuinely broken watch proves noisy.**

That estimate had a blind spot: the common cause of a give-up is not one broken
watch, it is one broken *network*, and that fails every watch at once.

### Observed in production (2026-08-12)

A laptop moved between networks. Every `gh` invocation failed, so the github-pr
adapter exited non-zero on its next poll (a connection error is classified
`GhError::Failed`, which is fatal). All eight watched PRs burned their restart
budget and gave up within seconds of each other.

The sweep then retried all eight every 300s, each retry gave up again, and each
give-up published. By the time the network returned, each of the eight topics
carried **13 identical `adapter_gave_up` events, five minutes apart** — and every
agent subscribed to one of them had been woken 13 times to be told the same thing.
Idle agents exist to take zero turns; these took a turn every five minutes to read
a message they had already read.

The give-up event was right the first time. Its repeats were the defect.

## Decision

**A give-up is announced once per outage, and withdrawn by a recovery event.**

- The supervisor holds an in-memory latch of the entities whose give-up has been
  announced and not yet withdrawn. The first give-up of an outage publishes
  `adapter_gave_up` and sets the latch.
- A give-up that finds the latch already set publishes **nothing**. The watch is
  still marked `Failed`, and ADR-0011's retry still happens on the next sweep — the
  retry is unchanged, only its announcement is suppressed. Nothing about the state
  machine changes; the log records the silent re-give-up at `warn`.
- The latch is withdrawn when the watch **runs stably again** — when an adapter
  instance has been up for `RestartPolicy::reset_after`, the same threshold that
  already defines "stable" for resetting the crash streak. Withdrawing publishes
  `adapter_recovered`, whose subject is *"mailbox is watching this again: the
  adapter recovered"*.
- Teardown (last interest gone, TTL sweep, clean exit) drops the latch **silently**.
  The adapter did not recover, it stopped being wanted; a later watch of the same
  entity is a new outage and gets its own notice.

Recovery is detected by a timer armed at spawn, not by an exit. This is forced:
the adapter that recovers is the one that *stops* exiting, so no exit-driven check
can ever observe it. Deciding stability from anything cheaper — a successful
`start`, say — would recreate the storm inverted: while the network is down every
sweep retry starts an adapter that dies seconds later, so "started" would publish
one bogus "recovered" per interval.

The latch is in memory, not in SQLite. It describes *this daemon's* conversation
with its subscribers rather than a property of the watch, and a daemon restart is
genuinely new information — one fresh notice per restart is a defensible rate, and
it costs no schema migration.

## Consequences

- The observed incident becomes 2 wakes per topic instead of 13: one when the
  network drops, one when it returns. A fault lasting a week is still 2.
- An agent that hears "mailbox stopped watching this" can now trust it as *state*,
  not as a repeated alarm, because the retraction is guaranteed to arrive.
  Announce-once without the recovery event would be strictly worse than the storm:
  the agent would be told its watch went dark and never told otherwise, which is the
  ADR-0008 deafness this bus exists to prevent.
- The recovery event costs a model turn (AGENTS.md: "never put anything on that wire
  you would not spend a model turn on"). It earns it: it is the only signal that an
  idle agent's watch is live again, and an agent that was told to stop expecting news
  has to be told to start expecting it again. It fires at most once per outage.
- A daemon restart during an ongoing outage re-announces once, because the latch does
  not survive it. Accepted deliberately over a schema change; the pre-existing
  behaviour re-announced every sweep.
- `reset_after` acquires a second job: it was the stable-run threshold for resetting
  the crash streak, and it is now also how long a recovered adapter must stay up
  before its recovery is announced. Both readings are "this run lasted long enough to
  count", so one knob is right — but a test that shrinks `reset_after` to reach the
  recovery path also shortens the streak reset, and the supervision tests say so.
- One spawn now arms one extra timer task per adapter instance, which resolves after
  `reset_after` and is ignored if its instance is gone. Negligible, and it mirrors the
  existing detached backoff-restart task.
- This does not make a network drop harmless — it makes it quiet. Every watch still
  fails, gives up, and is marked `Failed` when the network goes, because a `gh`
  connection error is still classified fatal. Fixing *that* (treating unreachability
  as "no news" rather than "this watch is broken", so the adapter never dies at all)
  is a separate change in the adapter's error classification, deliberately not taken
  here.

## Alternatives considered

- **Classify connectivity errors in the github-pr adapter instead.** The deeper fix:
  a `gh` connection failure would skip the poll and keep the adapter alive, so a
  network drop would never reach a give-up. It addresses the *cause* of this
  incident, but not the *class* — any persistent fault (revoked auth, deleted PR, a
  bad adapter binary) still storms once per sweep, and that is a supervisor problem
  wherever the adapter's classification lands. Worth doing, on its own merits, as its
  own change; announce-once is what makes the storm impossible rather than merely
  unlikely.
- **Publish nothing at all after the first give-up, and no recovery event either.**
  Simplest, and wrong: an idle agent takes zero turns, so silence after "stopped
  watching this" is indistinguishable from a watch that came back. That is precisely
  the ADR-0010/ADR-0011 failure mode — a healthy watch nobody knows is healthy.
- **Persist the latch in the `watch` table.** Survives a daemon restart, at the cost
  of `SCHEMA_VERSION` 7 → 8 plus its migration and migration tests, to suppress one
  event per restart per broken watch. The rate that motivated this ADR was per-sweep,
  not per-restart. Revisit if daemon restarts during long outages prove noisy.
- **Exponential backoff on the sweep retry** (retry a long-`Failed` watch less
  often). ADR-0011 already considered and rejected this for the retry itself; as a
  fix for the *event* rate it is worse than a latch — it slows the storm rather than
  ending it, and it also slows recovery, which is the one thing that should stay
  prompt.
- **Deduplicate at the wake boundary** (suppress a wake whose subject repeats one the
  agent already saw). Fixes the symptom for every publisher at once, but puts
  content-inspection policy on the wake path, and a legitimately repeated subject —
  "CI failed on `build`", twice, for two different runs — is real news that would be
  swallowed. The supervisor knows it is repeating itself; the wake path would have to
  guess.
