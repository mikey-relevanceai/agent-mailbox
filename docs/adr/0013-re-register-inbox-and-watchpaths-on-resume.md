# ADR-0013: Re-register the inbox and watchPaths on session resume

- Status: Accepted
- Date: 2026-07-21
- Relates to: [ADR-0007](0007-always-on-agent-inboxes.md) (always-on inbox),
  [ADR-0008](0008-on-demand-wake-filechanged.md) (on-demand wake). Resolves the
  `watchPath`-persistence residual left [UNDOCUMENTED] in ADR-0008.
- Sibling: [ADR-0012](0012-level-triggered-wake-at-the-turn-boundary.md) also makes
  `ensure-watcher` do more at the turn boundary (re-bumping the sentinel for unread
  mail). The two compose and are independent: 0012 makes sure mail that arrived while
  the session was BUSY still wakes it; this one makes sure the session is *registered
  and watched at all* after a resume. Both landed as the same `Stop` hook grew from a
  pure watcher-liveness poke into the session's per-turn self-healing point — so
  `ensure-watcher` now registers the inbox (here) **before** it checks watcher liveness
  and then re-triggers (0012). That order is load-bearing; see below.

## Context

A long-lived, Superconductor-launched Claude Code session that goes idle waiting for
mailbox wakes (a peer's `mailbox send`, or a `github-pr` watch event like `pr_merged`)
**never woke** after the session was **resumed** — it only discovered mail by polling
`mailbox status` by hand. Live diagnosis on a resumed session showed:

```text
inbox: agent.<id> (NOT registered — peers cannot send to this session)
subscriptions: none
```

The session was both **unaddressable** (a peer's `send` failed) and **unwakeable**.

### Root cause: ADR-0008 dropped the register-on-every-lifecycle-hook invariant

ADR-0007 §2 registered the inbox "on every `SessionStart` **and** every `Stop`,
idempotently" — via the single `arm` hook that ran on both. ADR-0008 replaced that one
hook with two:

- `session-start` (`SessionStart`, matcher **`startup`**) — the ONLY handler that
  registers the inbox, prints the `watchPaths` sentinel registration, and spawns the
  watcher.
- `ensure-watcher` (`Stop`) — respawns a dead watcher, but **does not register the
  inbox** and (by design) cannot print `watchPaths` (Claude Code rejects a
  `SessionStart`-shaped `hookSpecificOutput` from a `Stop` hook — a bug ADR-0008 hit and
  fixed).

Claude Code fires `SessionStart` **again on resume**, but with `source: "resume"`, which
the `startup` matcher excludes. So on a resume:

- `session-start` never runs → the inbox is not re-registered (unaddressable) and the
  `watchPaths` are not re-registered in the fresh process (unwakeable — a resume is a new
  process, and `watchPath` registration is per-process, not persisted across a resume).
- `ensure-watcher` runs on the next `Stop`, but it can neither register the inbox nor
  re-emit `watchPaths` — so it cannot heal either gap.

This is exactly the `watchPath`-persistence residual ADR-0008 left [UNDOCUMENTED],
compounded by the inbox-registration regression against ADR-0007.

A secondary interaction: the ADR-0007 tombstone guard (`SUBSCRIBE_TOMBSTONE_GUARD_MS`,
10s) refuses an `AutoInbox` re-registration within 10s of a `SessionEnd`, and its own
comment says a genuine resume "re-registers on its next arm once the guard lapses" — but
after ADR-0008 there was no per-turn re-registration left to do so.

### The missing inbox was the SINGLE upstream blocker (observed, then confirmed)

On the live broken session it looked as though `ensure-watcher` never spawned a watcher
at all. It did. `ensure-watcher` does **not** gate the spawn on subscriptions — it spawns
whenever no live watcher exists — but the watcher it spawns re-checks `has_subscription`
after taking its lock and, finding none (the inbox was unregistered), self-exits
`Unsubscribed` and removes its own pidfile. From outside, "spawned then instantly
self-exited" is indistinguishable from "never spawned"; `harness.log` shows
`watcher found no subscriptions; exiting without arming a sentinel`.

So the deafness was **one** bug, not two: the unregistered inbox starved the watcher too.
That makes the ordering inside `ensure-watcher` load-bearing — **`register_inbox` must run
BEFORE the watcher-liveness check**, or the watcher spawned on that same turn would still
self-exit and the session would not recover until the following turn.

## Decision

Restore the ADR-0007 invariant — the inbox (and, on resume, the watchPaths) are
re-established on every lifecycle hook — with two changes:

1. **`session-start` fires on every `SessionStart` source.** Its hook matcher changes
   from `"startup"` to `""` (all sources: `startup`, `resume`, `clear`, `compact`). A
   resumed process therefore re-registers its inbox, re-prints its `watchPaths`, and
   re-spawns its watcher — the only place that CAN re-print `watchPaths`. Every step of
   `session-start` is already idempotent (an existing subscription's cursor is untouched;
   the watcher is single-instance; the watchPaths print is stateless), so firing on
   `clear`/`compact` mid-session is a safe no-op.

2. **`ensure-watcher` re-registers the inbox on every `Stop`** (best-effort, fail-open),
   restoring ADR-0007's register-on-every-`Stop` half and making the tombstone's
   self-heal real again: once the 10s guard lapses, the next `Stop` re-subscribes a
   session whose resume-time registration was refused. This is the one bridge socket call
   the `Stop` hook makes; a down/erroring bridge is logged and skipped, never failing the
   hook. `ensure-watcher` still **cannot** re-print `watchPaths` (a `Stop` may not emit
   SessionStart output) — that is change 1's job.

## Consequences

- **A resumed session is addressable and wakeable again** without any manual
  `mailbox harness session-start`. The reported symptom is fixed at its source.
- **`ensure-watcher` now makes one socket call per turn** on a working agent. This is not
  a new cost category — it is exactly what the pre-ADR-0008 `arm`-on-`Stop` did — and it
  is fail-open, so a down daemon does not block or fail the hook (the client fails fast on
  a missing socket). `ensure-watcher` is consequently dispatched on the async path rather
  than the synchronous one; its tracing still routes to `harness.log` and it still exits
  0 always (never a wake).
- **`watchPaths` are re-registered per process**, matching Claude Code's undocumented
  (assumed per-process) lifetime rather than relying on cross-resume persistence.
- **Re-running `mailbox harness install-hooks` after upgrading the binary** rewrites the
  `SessionStart` matcher from `startup` to `""` (the install merge recognises and
  replaces our own hook groups), so an existing install is healed by the documented
  upgrade step.
- **A resumed orchestrator is no longer a silent black hole.** `send` to an unregistered
  inbox DROPS rather than queues (baseline-on-subscribe — ADR-0007 §3), so every peer a
  resumed coordinator had told to "report back" lost its report. One peer burned ~20
  minutes retrying and only got through out-of-band. This ADR removes the common cause;
  the residual is now surfaced honestly rather than silently, via the reworded `send`
  error below.
- **The `send` refusal no longer reads as an identity error.** It was
  `SendError::UnknownAgent` — "unknown agent {id}" — which framed a *registration*
  failure as a *stale id*, and demonstrably misled a peer agent into concluding the
  coordinator had resumed with a new session id (it had not; the id is stable across a
  resume). Renamed `SendError::InboxNotRegistered`, and the text now leads with the
  registration, states outright that the id may be correct, and says the message was
  dropped rather than queued.
- **Hook handlers run by hand say so.** An empty stdin now yields a named
  `HookError::NoPayload` naming the exact `echo '{"session_id":…}' | mailbox harness
  session-start` incantation, instead of serde's "EOF while parsing a value at line 1
  column 0" — which is what an operator hit while hand-recovering a lapsed session.
  Manual recovery restores addressability and the watcher, but **not** the `watchPaths`
  registration (Claude Code only consumes that from a real `SessionStart` hook), so the
  recovered session does not auto-wake until a genuine `SessionStart` fires. That is
  documented in `docs/04-usage.md` and is a further argument for fix 1 being the real
  remedy rather than manual recovery.

## Alternatives considered

- **Widen the `SessionStart` matcher only (change 1, not 2).** Fixes the normal resume
  path, but leaves the <10s tombstone-window resume unaddressable until the next
  `SessionStart`, and does not restore ADR-0007's register-on-every-`Stop` invariant that
  the tombstone self-heal assumes. Rejected as incomplete.
- **Fold everything into `ensure-watcher` (change 2, not 1).** Recommended in the original
  bug report as "more robust" because a `Stop` is guaranteed on a working session. But a
  `Stop` hook **cannot** re-register `watchPaths` — so this makes a resumed session
  addressable yet still unwakeable (no watchPaths in the fresh process). Rejected as
  insufficient on its own; both changes are needed.
- **Register the inbox via the `Explicit` (tombstone-clearing) kind on resume.** Would
  bypass the 10s guard, but the guard exists precisely because a doomed post-teardown
  `AutoInbox` arm is indistinguishable from a legitimate resume at that layer (ADR-0007
  §2b). Keeping `AutoInbox` and letting the guard age out is the intended self-heal.
- **Detect `source` in the handler and branch.** Unnecessary: every `SessionStart` source
  wants the same idempotent setup, so the matcher (not handler logic) is the right lever.
