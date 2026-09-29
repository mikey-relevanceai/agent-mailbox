# ADR-0026: An ended session's watches are suspended, and a resume restores them

- Status: **Accepted**
- Date: 2026-09-29
- Amends: card 11's `SessionEnd` teardown ("a departing session must leave no
  subscription and no interest") and design/01 rule 5. Both still hold for the
  **live** tables, which is what the adapter refcount and the wake read. What changes
  is that the rows are set aside rather than deleted.
- Relates to: [ADR-0013](0013-re-register-inbox-and-watchpaths-on-resume.md) (the
  inbox half of a resume, which this completes),
  [ADR-0010](0010-resume-watches-on-restart.md) (resume on *daemon* restart, the
  other direction), [ADR-0007](0007-always-on-agent-inboxes.md) (the tombstone
  guard, reused here unchanged).

## Context

Restarting a desktop harness that hosts several Claude Code sessions brought the
terminals back and resumed the sessions, but not their mailbox watches. Each resumed
agent could be messaged again (ADR-0013 re-registers the inbox on every
`SessionStart`) but was deaf to every PR it had been watching. Nothing told it so.

`~/.agent-mailbox/harness.log` shows what happened. This is measured, from one machine:

- At 2026-09-21T11:21:32Z, five sessions ran the `SessionEnd` hook within one second
  of each other. That is the app quitting. Each `cleanup` reported
  `interests_dropped=1` or more.
- Between 11:27:07Z and 11:29:45Z, the **same five session ids** ran `session-start`
  again. That is the app reopening and resuming them.
- The same pattern appears on 2026-09-23 and 2026-09-27.

So a resumed session keeps its id (ADR-0013 had already observed this), and the
`SessionEnd` in between had deleted its `watch_interest` and `subscription` rows.
`session-start` only re-registers the inbox. Nothing recorded what the session had
been watching, so nothing could put it back. The daemon was up throughout (it had
been running since 2026-09-11), so this was not the daemon-restart case ADR-0010
handles.

The rule that caused it was reasonable when written: a `SessionEnd` looked like the
end of the session, and card 11 wanted no poller to outlive the session that asked for
it. The mistake was treating "this process ended" as "this session will never come
back". Claude Code can resume any ended session by id, and a harness that restores its
sessions on launch does it routinely.

## Decision

1. **`SessionEnd` suspends instead of deleting.** In the same transaction that removes
   the session's rows from `watch_interest` and `subscription`, it copies them into two
   new tables, `suspended_interest` and `suspended_subscription`, stamped with the end
   time. The adapters are still stopped when the last live interest leaves, exactly as
   before. No poller runs for a suspended session.

2. **The TTL sweep suspends too.** A session that dies without a `SessionEnd` (a
   crash, a force-quit) has its interests swept after an hour with no live process.
   Those are suspended the same way, stamped with their `last_seen` (the last moment
   anything proved the session alive). The sweep never touched subscriptions and
   still does not.

3. **`session-start` resumes.** After registering the inbox, the hook sends a new
   `ResumeSession` request. The daemon restores the session's suspended rows and then
   calls `ensure_running` on **every** watch the session has a live interest in, not
   only the restored ones. That also covers a daemon restart while the app was closed:
   the startup reconcile found no live session and stopped the watch, but left the
   interest in place, and nothing else would start it again.

4. **Suspended state expires after 30 days.** The periodic sweep deletes suspended rows
   older than the retention window. Sessions that are never resumed — abandoned, or
   ended by `/clear` — are forgotten eventually rather than kept forever. The window is
   long on purpose: a suspended row costs nothing, since no adapter runs for it, and
   losing one is exactly the bug this fixes. `MAILBOX_SUSPENSION_RETENTION_MS`
   overrides it, like the sweep's other timings.

5. **The resume honours the ADR-0007 tombstone guard**, the same one the inbox
   registration beside it uses. Both halves of `session-start` therefore agree on
   whether the session is back. A refused resume leaves the suspended rows in place for
   the next `SessionStart`.

### Why separate tables rather than a `suspended_at` column

The live tables have many readers: the interest refcount that keeps an adapter
running, the wake's subscriber lookup, `agents`, `topics`, `status`, the sweep. With a
column, every one of them would have to remember to skip suspended rows, and any one
that forgot would run a poller for a closed session or wake a dead one. A row that is
not in the live table cannot be counted by mistake. All the new behaviour lives in
three places: end, sweep, and resume.

### What a restored subscription sees

It keeps the delivery cursor it had. `end_session` never deleted cursors, so an event
published on the topic while the session was away is **unread** when it comes back,
not skipped. This is deliberately not baseline-on-subscribe: it is the same subscriber
returning, not a new one. For a `github-pr` watch whose only watcher was this session,
nothing was published while it was away, because the adapter was stopped. The
restarted adapter starts from its persisted baseline (design/01), so a transition that
happened while the app was closed should be reported on restart. That last point is
inferred from the baseline design and is not covered by a test here.

## Consequences

- **A resumed agent is watching what it was watching.** Through the real binaries, a
  test watches a stub, lets the TTL sweep suspend the dead session's interest and stop
  the adapter, runs `session-start`, and sees the watch `running` with interest 1
  again. In-process, a test ends a session, sees the adapter reaped, resumes, and sees a
  new adapter whose events the restored subscription receives.
- **`cleanup` now means "suspend".** The `SessionEnded` wire fields keep their
  `*_dropped` names: they still count what left the live tables, and renaming them
  would break an older client talking to a newer daemon. The human log line says
  "suspended".
- **`session-start` makes a second socket call.** It is fail-open like the first: a
  down bridge, an error, or an older daemon that does not know `ResumeSession` is
  logged to `harness.log` and the hook still exits 0. A daemon has to be restarted
  onto this version before resumes work. The suspended rows written by a newer daemon
  simply wait until then.
- **Two more tables and a migration (v7 → v8).** Additive. No existing row is touched,
  and nothing is suspended by the migration itself.
- **A resume within 10 seconds of the end is still refused.** This is a known residual,
  not fixed here. The log on the same machine shows the tombstone guard refusing an
  inbox registration 22 times. For example, session `c8ff5add` ended at
  2026-09-21T23:31:09Z, was refused at 23:31:17Z, and ended again at 23:54:23Z without
  ever re-registering. The ADR-0007 race the
  guard defends against came from a `Stop`-hook re-registration that ADR-0021 deleted.
  The guard may now only refuse genuine resumes, but proving that is its own decision.
  Until then, a refused resume keeps its suspended rows, and the next `SessionStart`
  restores them.

## Alternatives considered

- **Decide by the `SessionEnd` reason** (delete on `logout`, keep on `other`). Rejected:
  any ended session can be resumed by id whatever the reason, so the reason does not
  answer the question. We also have not measured which reason a harness quit
  produces.
- **Leave the rows in place on `SessionEnd` and rely on the TTL sweep.** Rejected: it
  keeps the adapters running for an hour after every session ends, which is the zombie
  poller card 11 exists to prevent. And a resume after the hour would lose the watch
  anyway.
- **Have the agent re-`watch` on resume** (a skill instruction, or `additionalContext`
  from the hook). Rejected for the same reason as ADR-0010: a resumed idle session takes
  no turn, so an agent-side step never runs. The bridge has to restore the watch itself.
- **Restore only the suspended watches, not every interested one.** Rejected: it misses
  the daemon-restart-while-closed case described in decision 3, which leaves an
  interest in place and a watch stopped with nothing to start it.
