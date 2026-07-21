# ADR-0012: Level-triggered wake at the turn boundary (the busy-window deafness fix)

- Status: Accepted
- Date: 2026-07-21
- Amends: [ADR-0008](0008-on-demand-wake-filechanged.md) (the `Stop`-liveness hook E′
  gains a second job). Nothing in ADR-0008 is reversed — the watcher, the sentinel,
  the `FileChanged` wake hook and its anti-loop all stand exactly as they are.

## Context

A watching agent sat idle on top of unread mailbox events and was never woken.

Reported from a real session (`2b60e384-…`, 2026-07-21) with
`mailbox watch github-pr RelevanceAI/arg#3943` armed. A reviewer approved the PR; the
poller detected it and published; the agent never woke, and only learned of the
approval when the human asked "you didn't get woken for the approval?".

**Publication worked end to end.** `mailbox status` showed the watch healthy
(`state=running interest=1`) and `unread: [github.pr.RelevanceAI/arg#3943] 2`;
`mailbox read` then returned both events. The failure was purely in wake DELIVERY.

### The mechanism, confirmed from `harness.log`

```text
01:43:47  watcher wrote the wake sentinel   topics="github.pr.RelevanceAI/arg#3943"
01:43:55  ensure-watcher: a live watcher already holds the lock; leaving it (no-op)
01:55:47  watcher wrote the wake sentinel   topics="github.pr.RelevanceAI/arg#3943"
```

Both real-mail bumps landed. **Neither produced a `FileChanged wake:` line at all** —
contrast 01:14:09 in the same session, where an idle-time bump did fire the hook 3.6 s
later. The session was mid-turn at 01:43:47 (its `Stop` arrived 8 s later), and the
hook simply never ran.

So the ADR-0008 wake is a pure **edge**, and the edge is only meaningful to an IDLE
session:

1. publish → kick → the watcher (a detached process, always live) bumps the sentinel;
2. `FileChanged` → `mailbox harness wake` → exit 2 → Claude Code wakes the session.

Step 2 has no effect on a session that is mid-turn: there is no idle session to
"rewake", and the hook is not deferred and re-delivered. The bump is spent. Nothing
ever bumps the sentinel again for that mail, because the watcher is strictly
kick-driven and the `Stop` hook only checked watcher LIVENESS — never whether the
session was sitting on unread mail. The agent then went idle, deaf, indefinitely.

This defeats the watch for exactly the agents most likely to use one: agents actively
working (and therefore busy) while waiting on a CI or review transition.

The original bug report's hypothesis was that the FIFO kick is dropped when no waiter
is blocked. That is NOT what happens — the watcher is detached and independent of turn
state, and it received and acted on both kicks. The drop is one layer up, at
`FileChanged` → wake.

## Decision

**Arm level-triggered, stay edge-triggered thereafter.** The `Stop` hook
(`mailbox harness ensure-watcher`) gains a second job: at every turn boundary, if the
session is sitting on unread mail it has not already been re-triggered for, **re-bump
the sentinel**.

The ordinary ADR-0008 path then does the rest: the re-bump fires `FileChanged` against
a session that is now idle, and the wake hook decides — from the store, as always —
whether to actually wake. `Stop` is the only signal the bridge receives that a turn
ENDED, so it is the only place a level check can run.

Deliberately unchanged:

- **The `Stop` hook still never wakes.** It exits 0 always; it triggers the existing
  wake wire rather than becoming one. A Stop hook that exits 2 was rejected (see
  Alternatives).
- **The sentinel is still a trigger, never authority.** The wake hook's per-session
  store re-check is untouched, so cross-session isolation and the anti-loop hold
  exactly as ADR-0008 specifies.
- **The watcher is untouched.** It has no notion of a turn boundary and gains none.

**Bounded by a watermark, so it cannot loop.** An agent that wakes and chooses not to
read would otherwise be re-triggered every turn forever. So a re-trigger is recorded by
[`WakeWatermark`] — the `event.event_row_id` of the newest unread event — in
`<sentinel-dir>/.mailbox-retriggered`, and fires only for mail strictly newer than the
last recorded one. **Each message buys at most one turn-boundary wake.** An agent that
ignores its mail is nudged once, not endlessly.

`event_row_id` is the store's single global monotonic sequence, so the comparison is
meaningful across topics — which per-topic offsets are not. The topics and the
watermark are read in ONE statement (`ReadOnlyStore::unread`), so a publish cannot land
between two reads and be recorded as already-seen. The predicate is shared with
`topics_with_unread`: two copies would be free to drift, and a wake that disagrees with
itself about "unread" is this module's classic bug.

**Where it lives.** The decision is `Waiter::retrigger_if_unread` in `wake.rs`, beside
`sync_sentinel_to_unread` — "read the unread state, then bump the sentinel" is the wake
domain's job, and both bump paths share one choke point rather than drifting apart. The
`Stop` hook only decides *when* to ask and how to log the answer, like every other hook
handler in `cli.rs`. The outcome is returned as a `RetriggerOutcome` rather than logged
in place, so the anti-loop branch is assertable in a test instead of inferable from a
file mtime, and the anti-loop rule itself is a pure `needs_retrigger` function with its
boundaries unit-tested.

**Ordering: bump first, record second.** A crash between them costs a duplicate nudge
(harmless — the wake hook re-checks the store); recording first would lose the wake.
Likewise a missing, unreadable, or corrupt record reads as "nothing re-triggered yet" —
a redundant wake, never a swallowed one. Every failure path in the re-trigger is a
logged no-op: it is a safety net on a per-turn hook, and it must never make a turn fail
or hang.

## Consequences

- **The busy-window deafness is closed.** Mail that arrives mid-turn is delivered at
  the end of that turn instead of never. Agents no longer need the fragile workaround
  of remembering to `gh pr checks` when they next act.
- **Steady-state behaviour is unchanged.** A session that is caught up at `Stop` — the
  overwhelmingly common case — touches nothing: no sentinel write, no `FileChanged`, no
  wake. Confirmed by test.
- **At most one extra wake per ignored message.** An agent woken for mail it does not
  read is nudged exactly once more at its next turn boundary, then not again until
  newer mail arrives. This is the accepted price of never defaulting to "assume it was
  delivered": the wake hook cannot know whether its exit 2 actually reached the agent,
  which is precisely why the hook does NOT record the watermark — only the `Stop` hook
  does.
- **One more small file per session**, `.mailbox-retriggered`, inside the sentinel
  directory `SessionEnd` already removes wholesale. Its basename is deliberately not a
  `.mailbox-wake…` name, so it can never be confused with — or matched alongside — the
  `FileChanged` matcher.
- **Residual: a re-bump that still lands while the session is busy is not retried for
  that same mail.** The `Stop` hook runs at the turn boundary and `FileChanged` fires
  100–500 ms later, by which time the session is idle, so this should not occur; if it
  ever did, the mail waits for the next publish (or the next NEWER message's
  re-trigger) rather than being re-nudged. Retrying on a timer was rejected — it
  reintroduces exactly the periodic cost ADR-0008 removed.
- **Residual: a resumed session (`claude --resume`, same session id) keeps its
  `.mailbox-retriggered`.** Mail that arrived while it was dead could therefore be
  skipped by the re-trigger — but the `SessionStart` watcher primes the sentinel from
  existing unread unconditionally (ADR-0008 step 4), which wakes it anyway. The record
  is left alone rather than cleared, to avoid a duplicate wake on every resume.
- **The idle-forever residual from ADR-0008 is unaffected.** A session that will never
  take another turn fires no `Stop`, so nothing here helps it. That limit stands.

## Alternatives considered

- **Make the `Stop` hook itself exit 2** (a blocking Stop feeds its stderr back to the
  model). More direct — it delivers at the turn boundary with no `FileChanged` race at
  all — but it changes `Stop` from a hook that can never wake into one that can, which
  ADR-0008 names as load-bearing, and it would need its own loop guard on top of the
  watermark. Rejected in favour of reusing the one smoke-tested wake wire.
- **Have the watcher re-check on a timer.** Reintroduces the periodic cost ADR-0008
  exists to eliminate, and the watcher still cannot see turn boundaries — it would wake
  a busy session just as ineffectively.
- **Have the `FileChanged` wake hook record the watermark when it exits 2.** Tighter in
  principle (the `Stop` hook would then only ever re-trigger genuinely-undelivered
  mail), but the hook CANNOT know whether its exit 2 actually woke the agent — that is
  the whole failure being fixed. It would have recorded the busy-window mail as
  delivered and preserved the bug.
- **Re-check unread inside the waiter's arm path only** (the original bug report's
  suggestion). The waiter/watcher already DOES check existing unread when it arms
  (ADR-0008 step 4, `Waiter::wait` step 4) — that path was never the gap. The gap is
  that a live watcher never re-arms, so nothing re-checks while a session stays up.

[`WakeWatermark`]: ../../crates/mailbox/src/storage/model.rs
