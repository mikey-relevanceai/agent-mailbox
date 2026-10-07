# ADR-0027: Adapter failure wakes nobody

- Status: **Accepted**
- Date: 2026-10-07
- Supersedes: [ADR-0023](0023-one-give-up-notice-per-outage.md) (one give-up
  notice per outage, withdrawn by a recovery event).
- Amends: design/01 rule 7 ("give up and surface an error event after N
  failures"). The give-up itself stays: after N consecutive failures the watch is
  still marked `Failed`.
- Leaves unchanged: [ADR-0011](0011-retry-failed-watches-on-sweep.md). The sweep
  still retries a `Failed` watch once per interval while an interested session is
  alive.

## Context

ADR-0023 tried to make give-up notices quiet: announce the first give-up of an
outage, stay silent on the give-ups that follow, and withdraw the notice with an
`adapter_recovered` event once an adapter has stayed up for `reset_after` (60s).
It fixed the case it was written for, a network that is down and stays down. It
did not fix a network that keeps coming and going.

### Observed in production (2026-10-06)

The github-pr adapter's `gh` calls failed intermittently with `dial tcp` errors.
An adapter instance would come up, poll successfully, stay up past 60s and get
counted as recovered. Then a later poll would fail and the adapter would exit,
and the restart burst would end in another give-up. The daemon log for
`RelevanceAI/arg#7012` shows a run spawned at 01:26:59 announced as recovered at
01:27:59, then dying on a `dial tcp` error at 01:30:14.

On 2026-10-06 the supervisor put **73 events on 10 PR topics**: 39
`adapter_gave_up` and 34 `adapter_recovered`, with up to 10 on a single topic.
One topic's afternoon read gave-up 14:14, recovered 14:17, gave-up 14:19,
recovered 14:23, gave-up 14:23. Each event woke every subscriber of its topic.

In the topic read above, each give-up was followed by a recovery within minutes,
and each recovery by another give-up.

### Why there is nothing worth announcing

A wake costs a model turn (AGENTS.md: never put anything on that wire you would
not spend a model turn on). An adapter failure does not earn one:

- **It is not terminal.** Under ADR-0011 the supervisor never stops trying while a
  session wants the watch. So there is no moment at which "this watch is broken"
  becomes true and stays true. Any notice is provisional, and the next retry can
  undo it.
- **Nothing is lost.** The adapter's baseline is persisted through the bridge
  (ADR-0005), and the github-pr adapter diffs each observation against it. A PR
  that merged, conflicted, got a review or went red during the outage is reported
  on the first successful poll after it. An outage delays news. It does not drop
  it. One exception, inferred from the diff-based design: a state that changed and
  changed back within the outage is never seen. That is already true of any
  60-second poll.
- **The agent cannot act on it.** An idle agent woken to hear "the adapter failed
  5 times" can't fix the network or `gh` auth. Its only correct response is to do
  nothing, which costs a whole turn.

## Decision

**The supervisor publishes nothing about adapter failure.** It publishes nothing on
a give-up, on a retry that gives up again, or on a recovery. It authors no events
at all: only adapters publish on an entity topic.

- Crash handling is unchanged. Backoff-restart, give-up after N consecutive
  failures, `Failed`, and ADR-0011's sweep retry all still run.
- The give-up is logged at `warn`, so the daemon log still records every outage.
- `mailbox status` still shows the watch as `failed`. A human or agent that wants
  to know can ask.
- ADR-0023's latch (`announced_giveup`) and the per-spawn stability timer
  (`MarkStable`) are deleted. Their only job was to decide when to publish.
  `reset_after` goes back to one job: deciding when a run lasted long enough to
  reset the crash streak.

## Consequences

- The 2026-10-06 incident becomes zero wakes. By construction (nothing is
  published), so does a network that stays down for a week or flaps all day. That
  is inferred from the code, not measured in production.
- A **permanently** broken watch (revoked `gh` auth, a deleted PR, a bad adapter
  binary) is also silent. It is retried once per sweep for as long as its session
  lives, and no agent is told. This was chosen deliberately over a terminal state
  (stop retrying after a time budget and wake once). The person who owns the setup
  preferred silence to any notice, and the fault shows up in `mailbox status`
  and in the daemon log. Revisit this if a broken watch going unnoticed costs
  more than the wakes did.
- A woken agent can no longer tell "nothing happened on this PR" from "the poller
  has been failing for a while" without running `mailbox status`. On 2026-10-06 the
  notices that used to tell it alternated within minutes, so they were no reliable
  guide either.
- Old `adapter_gave_up` and `adapter_recovered` events stay in the durable log of
  every topic they were published on. Nothing new will be added.
- The supervisor no longer needs a `Bus` for its own publishes. It still holds
  one, because it hands it to each adapter's transport.

## Alternatives considered

- **A terminal state that wakes once.** Stop retrying after a budget (say, an hour
  of failing with no stable run), mark the watch terminally failed, and publish one
  "stopped watching; re-run `mailbox watch`" notice. That would make the notice
  true when sent and bound the noise to one wake per real fault. It was offered and
  declined in favour of never waking. It is also more machinery: a new terminal
  state for ADR-0011's retry to respect, and a clock for the budget.
- **Keep ADR-0023 and require a successful poll for recovery.** A real "the
  adapter observed the PR" signal would stop a 60-second survivor counting as
  recovered. But on a flapping network, successful polls are exactly what happens
  between failures, so the notices would still flap, only more slowly. That treats
  the symptom while keeping notices that are provisional by construction.
- **Classify `gh` connection errors as transient in the adapter.** The adapter
  would skip the poll and stay alive instead of exiting, so a network drop would
  rarely reach a give-up at all. ADR-0023 already named this as worth doing. It
  still is, as its own change: it cuts restart churn and log noise. It does not
  replace this decision, because any persistent fault would still reach give-up.
