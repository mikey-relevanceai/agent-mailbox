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

**Smoke test: PASSED on real agents (2026-07-16).** Each link above was first
confirmed in isolation headlessly; the integration tests here prove every link up to
"the wake hook WOULD exit 2 with the topic." The remaining harness-level properties —
which need a live Claude Code session — were then confirmed end-to-end on real
interactive agents:

- **Zero idle cost.** An idle agent sat many minutes and took no turns — no spurious
  wakes.
- **Wakes on real mail.** A `mailbox send` to an idle agent woke it promptly with
  `mail on topic agent.<id>`, and `read` showed the sender's `from`.
- **Multi-session isolation.** Poking session A woke ONLY A and poking B woke ONLY B;
  no other session reacted. (A false wake is structurally impossible anyway — each
  session's wake hook re-checks its OWN store — but this confirms it in practice.)

The first live run also caught a real bug on contact: the `Stop` hook emitted a
`SessionStart`-shaped `watchPaths` output, which Claude Code rejects (a hook's
`hookEventName` must match the firing event). Fixed — the `Stop` hook prints nothing
and only re-ensures watcher liveness; `watchPaths` registration stays in `SessionStart`.

One item remains [UNDOCUMENTED]: whether a `SessionStart`-registered `watchPath`
persists for the whole session or must be refreshed. Because a `Stop` hook cannot
re-register it (see the bug above), the design relies on it persisting — an accepted
residual. **(Update, [ADR-0012](0012-re-register-inbox-and-watchpaths-on-resume.md):**
this residual bit on **resume** — a resumed session is a fresh process that never
re-registered its watchPaths, because `session-start` was gated to the `startup` matcher.
ADR-0012 widens the matcher so `session-start` re-fires on resume and re-prints them.)

> **Correction (2026-07-17, [ADR-0009](0009-interest-liveness-from-the-waiter-pidfile.md)).**
> This section used to name the `watchPath` residual as "the first suspect" if a
> long-idle session went deaf. The first real case of a session going deaf was NOT
> this: it was the card-08 TTL sweeper reaping a LIVE session's watch interest,
> because nothing ever refreshed `last_seen` and this ADR's zero-idle-turn design
> guarantees an idle session never would. The sweeper logs `swept stale watch
> interests` when it acts — **check the bridge log for that line before suspecting
> `watchPath`.** ADR-0009 fixes the sweeper; the `watchPath` residual above stands
> but is unproven and no longer the leading suspect.

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
sentinel from any existing unread and **blocks on the mail FIFO forever**. On EVERY kick
it writes the session's current unread topic set into the sentinel **unconditionally**,
bumping its mtime (`std::fs::write` truncates+rewrites, so the mtime always advances even
when the content is identical); a kick with nothing unread writes the empty set (a benign
`FileChanged` the wake hook answers with exit 0). There is **no coalescing** — see the
unconditional-bump section below. There is **no `max_block`, no exit-2 re-arm** — it runs until `SIGTERM`'d at
`SessionEnd`. It survives a daemon restart exactly as the ADR-0006 waiter did (the
FIFO is re-openable; the kick reaches the already-blocked watcher). Single-instance via
the existing lock; a racing spawn loses and exits 0.

*Resilience.* The block loop is deliberately SIMPLE, not bulletproof — the Stop-liveness
hook (E′) is the recovery mechanism, so the watcher does not need elaborate self-healing.
A signal-interrupted `poll` (EINTR) is retried (a signal is not an error). A normal kick
is a writer that writes and closes its end; because the watcher holds the FIFO `O_RDWR`
(so it is always its own writer), that close surfaces as a buffered read then `WouldBlock`
— i.e. a kick, never an EOF (verified: the watcher survives kick after kick). On a
genuinely unrecoverable error the loop exits and the watcher **removes its pidfile** (on
EVERY exit path now, not just the `Unsubscribed` one), leaving a clean "no live watcher"
state for the Stop hook to respawn from.

*Unconditional bump per kick (no coalescing — correctness over the optimization).* The
watcher writes the sentinel on **every** kick, unconditionally: it reads
`topics_with_unread` and writes that set (empty or not) via `write_topics`, which truncates
and rewrites so the mtime always advances — even when the content is identical. It does NOT
compare the set to what the sentinel holds, and it tracks no read-progress signal.

Correctness is then immediate and simple: **every kick writes the sentinel → every real
message (which always kicks a live watcher) advances the sentinel's mtime → `FileChanged`
fires → the wake hook exits 2 iff there is genuine unread. No message can be coalesced away
— a lost wake is structurally impossible.** The dead-window case (the watcher dies after a
read, a message on an already-read topic arrives while it is dead, the Stop-liveness hook
respawns it) is fixed for free: on respawn, arm writes the current unread set `{T}`
unconditionally → the mtime advances → the session wakes. An empty kick writes the empty
set — harmless; the wake hook re-checks the store, finds nothing, and exits 0.

The only cost is that a burst of N messages yields up to N `FileChanged` events. This is
**accepted and bounded**: the wake hook's anti-loop (exit 0 once the agent is caught up)
bounds actual *model* wakes to ~1–2 per burst regardless of how many times the sentinel is
written. That bounded, benign cost buys the elimination of an entire bug class.

**This replaced an earlier coalescing design.** The watcher used to bump only when the
unread SET changed, plus a read-progress proxy (the sum of `offset + 1` over delivery
cursors) to force a bump after a read. That optimization — collapsing a same-topic burst to
one wake — produced **three separate silent-deafness bugs** (multiple-wakes → a lost wake
from an empty kick consuming the read-progress edge → a lost wake from a dead-window
respawn seeing an unchanged set). It was removed deliberately: with the anti-loop already
bounding real wakes, the coalescing bought little and cost correctness. There is no longer
any coalescing logic that *can* be wrong.

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

**Isolation: the sentinel is a TRIGGER, never authority.** When a session's cwd is an
ancestor of `~/.mailbox` (e.g. `claude` launched from `$HOME`, whose cwd is watched
recursively), a bump to session B's `.mailbox-wake` fires session A's `FileChanged` too.
There is **no false wake**: A's wake hook (C) re-checks A's OWN unread from the store,
finds none, and exits 0. Isolation therefore comes from that **per-session store
re-check**, not from the sentinel path — a load-bearing property a future change must
preserve: it MUST NOT start trusting the sentinel's contents in place of the store
check, or B's mail could wake A. The only cost of the shared-ancestor case is efficiency
(A's wake hook process runs for an unrelated bump); launching `claude` from a project
directory rather than `$HOME` avoids it.

**E) `install-hooks`.** Emits `SessionStart` (session-start), `Stop` (ensure-watcher,
plain — the Stop-liveness hook E′), `FileChanged` (wake, `asyncRewake`, matcher
`.mailbox-wake`), and `SessionEnd` (cleanup). It no longer emits the ADR-0006
exit-2 `arm` re-arm hooks, and an upgrade SWEEPS a stale `arm` hook (the merge recognises
the retired subcommand and removes it from every event — so an old `Stop → arm` re-arm is
replaced by `Stop → ensure-watcher`, never left firing exit-2 alongside it). **Upgrading
the binary WITHOUT re-running `install-hooks` leaves the stale ADR-0006 `arm` hooks in
place** — see the residuals.

**E′) The `Stop`-liveness hook** (`mailbox harness ensure-watcher`). This is the
PRIMARY recovery mechanism and the pessimistic safety net. **NEVER `asyncRewake`** — it
exits **0 always**, so a `Stop` can never itself wake the session. On every turn boundary
it (a) **respawns the detached watcher iff it is missing or dead** — a live watcher is
left strictly alone (even a redundant spawn is free: the loser loses the single-instance
lock and exits `AlreadyWaiting`), and (b) **re-registers the inbox** (best-effort,
fail-open — ADR-0012). It does **not** re-print `watchPaths`: a `Stop` hook cannot emit a
`SessionStart`-shaped registration (Claude Code rejects the mismatched `hookEventName`).

> **Correction (ADR-0012).** This paragraph originally claimed (b) re-printed the
> `watchPaths` — it never could, and never did. The real gap was that neither the inbox
> nor the watchPaths were re-established on a **resume** (a fresh `SessionStart` whose
> `source: "resume"` the `startup` matcher excluded). ADR-0012 fixes both: `session-start`
> now fires on every `SessionStart` source (so a resumed process re-prints its own
> watchPaths), and `ensure-watcher` re-registers the inbox on every `Stop` (restoring the
> ADR-0007 invariant). The one bridge socket call `ensure-watcher` makes is that inbox
> re-registration; a down daemon still cannot block it (the client fails fast).

The cost is a per-turn process spawn on a working agent — but **no model turn**, since
it never wakes. That trade is deliberately accepted here (it was the reason ADR-0008
originally omitted a `Stop` hook): making the watcher recoverable from the one place
that also covers an OS/OOM kill is worth a cheap exit-0 hook, and it lets the watcher
itself stay simple (A). The **supervision split** is: the OS user-service (launchd /
systemd) supervises the **daemon** (`mailbox serve`); this Stop hook supervises the
**per-session watcher**. It does NOT help a session that goes idle *forever* (fires no
`Stop`) whose watcher then dies — that residual is stated below.

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
- **New per-turn cost:** the `Stop`-liveness hook (E′) spawns a short process at every
  turn boundary on a working agent. It costs **no model turn** (it always exits 0), and
  it is what makes a died watcher recoverable — an accepted trade.
- **Residual risk (be honest — the adversarial gate will probe this):**
  - **Idle-forever watcher death.** A session that goes idle FOREVER (never another
    `Stop`) whose watcher THEN dies stays deaf until it next takes a turn or is
    restarted — the Stop-liveness hook (E′) is what would respawn it, and a truly-idle
    session fires no `Stop`. This is the accepted limit of a zero-spurious-wake design:
    nothing can wake a session that will neither take a turn nor be poked. The
    supervision split (OS service supervises the daemon; the Stop hook supervises the
    watcher) covers every case except this one.
  - **Orphan window at cleanup.** `cleanup`'s reap can miss a watcher that has not yet
    written its pidfile if the session's subscription survives a bridge-down teardown.
    It is an **accepted residual**: the Stop-liveness hook reconciles it on the session's
    next turn (a live watcher is left alone, a dead/absent one is respawned under the
    lock), and the card-08 TTL sweeper ages out the stale subscription. The future fix,
    if the window ever bites in practice, is to extend the TTL sweeper to ALSO reap the
    orphaned watcher via its pidfile when it sweeps that session's stale subscription —
    not done here because it is cross-cutting (the sweeper lives in the daemon and would
    need the waiters-dir/pidfile scheme) and the Stop hook already covers the realistic
    cases. **(Update, ADR-0009: the sweeper now HAS the waiters-dir/pidfile scheme — it
    probes those pidfiles for liveness on every pass — so the cross-cutting objection is
    gone and this fix is now a small step if the window is ever observed biting.)**
  - **Exit-window respawn transient.** On exit the watcher removes its pidfile while it
    STILL holds the single-instance lock (deliberate: removing under the lock is what
    stops it from ever deleting a *different*, freshly-armed watcher's pidfile). In the
    brief window between that removal and the lock drop, a concurrent `ensure-watcher`
    sees no pidfile, spawns a replacement, and the replacement then loses the lock and
    exits without writing a pidfile — leaving zero watchers until the next `Stop`. It is
    an **accepted transient**: it self-heals at the session's next turn (the Stop-liveness
    hook respawns), and the mail stays durable meanwhile. The alternative — removing the
    pidfile *after* releasing the lock — was rejected because it opens a strictly worse
    race: another watcher could acquire the freed lock and write its own pidfile in the
    gap, which the exiting watcher would then delete, orphaning a LIVE watcher.
  - **Store-unreadable dropped wake.** The watcher and the wake hook read the store
    read-only (WAL present). A wake that fires during a "bridge down" / store-unreadable
    window exits 0 (anti-loop-safe), and the already-unread mail is NOT re-bumped until
    the next publish kicks the watcher — a degraded-state residual, never a wrong wake.
    The mail stays durable and surfaces on the next kick or the next `Stop` respawn.
  - **Half-upgraded install.** Upgrading the `mailbox` binary WITHOUT re-running
    `install-hooks` leaves the stale ADR-0006 `arm` hooks in `settings.json`; they still
    fire exit-2 re-arm wakes and contend for the single-waiter lock with the new watcher.
    **After upgrading, re-run `mailbox harness install-hooks`** (it sweeps the retired
    `arm` hooks and installs the ADR-0008 set). Documented in `docs/04-usage.md`.
  - The full `watchPaths` + `asyncRewake` + idle chain and multi-session isolation were
    **confirmed on real agents (2026-07-16)** — see the smoke-test note under Context.
  - `watchPath` persistence across a long session is [UNDOCUMENTED], and a `Stop` hook
    CANNOT re-register it (that is a `SessionStart`-only output — emitting it from a Stop
    fails Claude Code's event-name check, a bug we hit and fixed). **RESOLVED for the
    resume case by [ADR-0012](0012-re-register-inbox-and-watchpaths-on-resume.md):** the
    concrete failure was a *resumed* session (a fresh process) that never re-registered
    its watchPaths or inbox, because `session-start` was gated to the `startup` matcher
    and so did not fire on `source: "resume"`. ADR-0012 widens that matcher to `""` (all
    sources), so a resumed process re-prints its own watchPaths and re-registers its
    inbox; `ensure-watcher` also re-registers the inbox every `Stop`. The design no longer
    relies on a `SessionStart` registration persisting across a resume — each fresh process
    re-establishes it. (For a deaf session that was NOT resumed, ADR-0009 still applies —
    check the bridge log for `swept stale watch interests` first.)

## Alternatives considered

- **Keep the ADR-0006 periodic re-arm.** Correct, but pays a model turn per `max_block`
  of idle forever. The cost this ADR exists to remove.
- **A bare `wake` basename.** Simpler matcher, but a stray `wake` file in a repo's cwd
  (recursively watched) would fire the hook. Rejected for `.mailbox-wake`.
- **No `Stop`-liveness hook (the original ADR-0008 stance).** We first omitted it to
  avoid a per-turn spawn on working agents, reasoning it does nothing for a truly-idle
  session. **Revised: we SHIP it** (E′) as the PRIMARY recovery for a died watcher — the
  per-turn spawn costs no model turn, and pushing recovery to the one place that also
  covers an OS/OOM kill lets the watcher itself stay simple (no elaborate in-loop
  self-healing). The truly-idle-forever case remains uncovered and is documented as an
  accepted residual.
- **A bulletproof, never-die watcher** (reopen the FIFO on any error and loop forever).
  Rejected as gold-plating: with the Stop-liveness hook as the recovery path, the watcher
  only needs EINTR-retry and a clean pidfile-removing exit; an elaborate reopen loop adds
  risk (hot-spin modes) for a case the Stop hook already handles.
- **Daemonize via double-fork.** Robust against re-acquiring a controlling terminal, but
  requires `unsafe`/`pre_exec` (the workspace denies `unsafe`). The watcher never opens
  a terminal, so `setsid`-on-startup (a safe `nix` call, no fork) is sufficient to
  detach from the hook's process group and outlive it.
