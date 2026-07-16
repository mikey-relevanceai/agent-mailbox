# ADR-0008: On-demand wake via a detached watcher + FileChanged hook

- Status: Accepted
- Date: 2026-07-16
- Supersedes: the **periodic re-arm** of [ADR-0006](0006-harness-self-respawn.md)
  (the `Stop` → `arm` → exit-2-at-`max_block` loop). ADR-0006's other decisions —
  the single-waiter lock, the waiter-owned pidfile, arm-iff-subscribed, the
  publish rules — still stand and are reused here.

## Context

ADR-0006 keeps an idle session wakeable with a blocked waiter (`mailbox wait`, an
`asyncRewake` hook process) that **exits 2 at `max_block`** to force a re-arm. Each
such exit-2 is surfaced to the agent as a wake — i.e. a **full model turn** — even
though there is no mail. On a fleet of parked-idle agents that periodic re-arm is a
heavy, recurring, uncached-token cost. It exists only because a hook process cannot
outlive its `timeout`, and nothing else fires on a timer during idle — so periodic
re-arm *seemed* unavoidable.

It is not. Claude Code's `FileChanged` hook gives us a wake that is driven by an
**external file change**, not a timer — so a background process can trigger a wake
exactly when there is real mail, and never otherwise.

### Empirically confirmed on this machine (the mechanism this ADR relies on)

1. **`FileChanged` fires on a truly-idle session** when an EXTERNAL process changes a
   watched file (an idle agent woke when its sentinel was touched from another
   process).
2. **A `FileChanged` hook with `asyncRewake: true` that exits 2 WAKES the idle
   session** (the idle agent woke and showed the reminder).
3. **`FileChanged` watches the cwd RECURSIVELY**, and the **matcher matches by
   BASENAME** (a file `<cwd>/.mailbox/wake` fired a matcher `wake`; a matcher
   `.mailbox/wake` did NOT).
4. **A `SessionStart` hook can register ABSOLUTE, out-of-cwd watch paths** by printing
   `{"hookSpecificOutput":{"hookEventName":"SessionStart","watchPaths":["<abs>"]}}` to
   stdout. An absolute central file fired its `FileChanged` (basename matcher) 3/3 when
   touched externally, from a session whose cwd did NOT contain it.
5. `FileChanged` `change_type` ∈ {create, modify, remove}; watcher latency 100–500 ms.

**Caveat (must be smoke-tested on a real agent).** Each link above was confirmed in
isolation. The **full chain end-to-end** — `watchPaths`-registered absolute sentinel
+ `asyncRewake` wake hook + a *truly-idle* session, all together — and **multi-session
isolation** (touching session A's sentinel wakes ONLY A) were NOT re-confirmed as one
flow headlessly. The integration tests here prove every link up to "the wake hook
WOULD exit 2 with the topic"; the harness-level wake itself needs a live agent. It is
also [UNDOCUMENTED] whether a `SessionStart`-registered `watchPath` persists for the
whole session or must be refreshed — see the design's defensive stance below.

## Decision

Replace the periodic re-arm with an **on-demand** wake: a detached per-session watcher
bumps a central sentinel file only on real mail, and a `FileChanged` hook turns that
bump into a wake iff there is genuinely unread mail.

**Central per-session sentinel.**
`<sentinel-root>/by-agent/<encoded-session>/.mailbox-wake`, where `<sentinel-root>`
defaults to `~/.mailbox` and is overridable by `MAILBOX_SENTINEL_ROOT` (tests point it
at a tempdir; `~` is expanded in-process, never by the shell). The basename is FIXED —
it is the static `FileChanged` matcher in `settings.json`. Per-session isolation comes
from the ABSOLUTE path each session registers via `watchPaths`. The sentinel holds
**topic names only** — payload-free, like the wake wire it triggers.

**Basename choice: `.mailbox-wake`, not `wake`.** Because the matcher matches by
basename over a recursively-watched cwd, a bare `wake` would be tripped by any file
literally named `wake` in a working tree. A dotted, mailbox-specific basename makes an
accidental collision vanishingly unlikely. The shared constant lives in
`mailbox-protocol` (`WAKE_SENTINEL_BASENAME`) so the bridge (which writes the sentinel
path) and the installer (which writes the matcher) can never drift.

**A) The detached watcher** (`mailbox harness watch`). Spawned by the `SessionStart`
hook and **fully detached**: it calls `setsid` on startup to leave the hook's process
group, and its stdio is redirected away, so the hook's exit — or a `killpg` on the
hook's group — cannot take it down, and no hook `timeout` applies (it is not a hook
child). It reuses the ADR-0006 single-waiter machinery in the same order: acquire the
per-session lock → write the pidfile under it → open the FIFO → re-check
`has_subscription` (self-exit `Unsubscribed`, no orphan, if none). Then it primes the
sentinel from any existing unread and **blocks on the mail FIFO forever**. On a
real-mail kick it writes the unread topic names into the sentinel (bumping its mtime);
a kick with nothing unread touches NOTHING (that is what would loop the wake hook).
There is **no `max_block`, no exit-2 re-arm** — it runs until `SIGTERM`'d at
`SessionEnd`. It survives a daemon restart exactly as the ADR-0006 waiter did (the
FIFO is re-openable; the kick reaches the already-blocked watcher). Single-instance via
the existing lock; a racing spawn loses and exits 0.

**B) The `SessionStart` hook** (`mailbox harness session-start`). Short-lived, NOT
`asyncRewake`, no exit-2: read `session_id`; ensure the always-on inbox subscription
(card 16); print the `watchPaths` registering this session's absolute sentinel; spawn
the detached watcher; exit 0. **Fail-open**: if the bridge is down, still print the
watchPaths and spawn the watcher (it self-validates under its lock).

**C) The `FileChanged` hook** (`mailbox harness wake`). Static matcher =
`.mailbox-wake`; `asyncRewake: true`. On fire, it opens the store **read-only** and
checks whether THIS session has genuine unread mail. **Anti-loop (the crux):** it exits
**2** with `mail on topic <X>` on stderr ONLY when there is genuine unread; otherwise it
exits **0**. A `FileChanged` fires on any change (the watcher's own write, a stray
touch, a create/remove at teardown), so an unconditional exit 2 would loop the agent —
which is exactly why the early prototype looped. The unread check is the store, not the
sentinel's contents, so a wake can never fire for mail that is not really there
(authored-by-self events are excluded too). If the store is unreadable, it exits 0
(anti-loop-safe) rather than risk a loop.

**D) The `SessionEnd` hook** (`mailbox harness cleanup`). Reap the watcher (SIGTERM via
its pidfile — the same `<session>.waiter.pid` the watcher writes), remove the session's
sentinel directory, and drop subscriptions/interests (unchanged).

**E) `install-hooks`.** Emits `SessionStart` (session-start), `FileChanged` (wake,
`asyncRewake`, matcher `.mailbox-wake`), and `SessionEnd` (cleanup). It no longer emits
the `SessionStart`/`Stop` → `arm` re-arm hooks, and an upgrade SWEEPS a stale `arm` hook
(the merge recognises the retired subcommand and removes it from every event, including
the `Stop` event the new snippet does not otherwise touch).

**No Stop-liveness hook.** We considered a minimal `Stop` hook that only re-ensures the
watcher is alive (cheap re-spawn, no wake, no model turn) as a safety net against a
watcher that died, and to re-register `watchPaths` in case they do not persist. We did
NOT ship it: a `Stop` hook fires on every turn boundary, which is a per-turn process
spawn on a working agent for a failure mode (watcher death) that the single-instance
lock already makes safe to recover from — and, more importantly, it does nothing for a
*truly idle* session (which fires no `Stop`), which is the only case that matters.
**Assumption, stated for the smoke test:** we assume a `SessionStart`-registered
`watchPath` persists for the session. If a real-agent smoke test shows it does not, the
right fix is a cheap `Stop` hook that only re-prints `watchPaths` and re-ensures the
watcher (no wake) — the design is structured so adding it is additive.

**F) Retained primitives.** `mailbox wait` and `mailbox harness arm` (and their
`max_block`/re-arm code) remain as working, tested primitives — they are simply no
longer wired into the hooks. The periodic-re-arm *mechanism* is superseded; the code is
retained because the watcher reuses most of it and removing it would churn a large,
green machinery test surface for no functional gain. A later card may delete them.

## Consequences

- **Eliminated:** the recurring re-arm model turn on idle. An idle subscribed session
  now costs **zero** turns until real mail arrives. The agent no longer sees a
  "re-arming the waiter" nudge — it wakes ONLY on mail.
- **New process:** one detached watcher per live session, reaped at `SessionEnd` and
  leak-checked in tests (the historically-buggy area — cards 07/08/11). It is
  single-instance and self-exits when unsubscribed, so it cannot orphan or duplicate.
- **New dependency on the harness:** a `FileChanged`-capable Claude Code. The wake wire
  is still a hook exit-2 (only a hook can wake an idle session — ADR-0006's rejected
  "bridge-side waiter" reasoning still holds); we simply trigger it on a file change
  instead of a timer.
- **Residual risk (be honest — the adversarial gate will probe this):**
  - The full `watchPaths` + `asyncRewake` + idle chain and multi-session isolation are
    proven only in parts headlessly; they need a real-agent smoke test.
  - `watchPath` persistence across a long session is [UNDOCUMENTED]; if it lapses, an
    idle session could stop waking until its next `SessionStart`. Mitigation is the
    additive `Stop`-liveness hook described above.
  - The watcher relies on the read-only store (WAL present) to name unread topics; the
    "bridge down" window degrades to "no bump" (the mail is still durable and surfaces
    on the next kick), never to a wrong wake.

## Alternatives considered

- **Keep the ADR-0006 periodic re-arm.** Correct, but pays a model turn per `max_block`
  of idle forever. The cost this ADR exists to remove.
- **A bare `wake` basename.** Simpler matcher, but a stray `wake` file in a repo's cwd
  (recursively watched) would fire the hook. Rejected for `.mailbox-wake`.
- **A `Stop`-liveness re-spawn hook.** Useful only if `watchPaths` do not persist or the
  watcher dies; costs a per-turn spawn on working agents and does nothing for a truly
  idle session. Deferred (additive) pending the smoke test.
- **Daemonize via double-fork.** Robust against re-acquiring a controlling terminal, but
  requires `unsafe`/`pre_exec` (the workspace denies `unsafe`). The watcher never opens
  a terminal, so `setsid`-on-startup (a safe `nix` call, no fork) is sufficient to
  detach from the hook's process group and outlive it.
