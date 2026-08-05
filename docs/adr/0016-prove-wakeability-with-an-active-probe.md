# ADR-0016: Prove wakeability with an active probe and a hook ack

- Status: **Accepted — in force, with one clause overtaken.** `mailbox doctor` is
  exactly as decided here; decision 6's "the third such command after `wait` and
  `dashboard`" is not — neither command still exists, so `doctor` is now the ONLY
  read-only, socket-free command.
- Date: 2026-07-27
- Amends: [ADR-0008](0008-on-demand-wake-filechanged.md) (the `FileChanged` wake hook
  gains an ack side effect) and [ADR-0015](0015-dashboard-reads-the-store-read-only.md)
  (log-derived wake health stays, but is no longer the best signal available).
- Amended by: [ADR-0017](0017-daemon-bumps-the-sentinel.md).

> **Amendment (2026-08-05).** Two clauses below have been overtaken, and neither
> weakens the decision:
>
> - **`doctor` is now the only read-only socket-free command** (decision 6). `wait`
>   and `mailbox dashboard` were both deleted — in a simplification commit that
>   recorded no ADR of its own, so this note is the only place it is written down.
>   ADR-0015's reason for the exception — a health check must work when the daemon is
>   the broken thing — is what keeps it.
> - **"ADR-0015's log-derived view stays"** (last consequence) is no longer true: the
>   `dashboard` command was removed outright, on the measurement recorded in
>   ADR-0015's own Status. Nothing speaks about the *past* any more; `doctor` and
>   `harness.log` are what is left.
>
> Decision 4's liveness source — `doctor::live_claude_sessions`, added here — did the
> opposite of ageing out: ADR-0017 promoted it to the ONE liveness signal for the
> whole system, replacing the watcher pidfile everywhere.

## Context

ADR-0015 built the first view of the last hop in the wake path — whether Claude Code
actually runs the `FileChanged` hook — by counting wake-hook lines in `harness.log`.
It was careful to name its states for the evidence (`no_wake_observed`) rather than
the conclusion. That caution was justified. Measured against an active probe on a
fleet of 19 live, idle sessions, the log-derived signal was wrong in **both**
directions:

- **6 of 9** sessions it called suspect answered a probe in under five seconds. A
  session that has never *needed* waking leaves exactly the same trace as one whose
  watch is dead.
- **3 of 10** sessions it called verified did not answer at all. They had woken
  before; they could not be woken now.

That second number is the important one, and it overturns an assumption the
investigation had been running on. **Wakeability is perishable.** It is not a fixed
property established at `SessionStart` and kept for the life of the process: a
session can be reachable on Monday and unreachable on Wednesday with no visible
event in between. History therefore cannot answer the only question that matters for
a fleet that hands work between agents — *can this agent be woken now?*

Two further findings shaped the design:

**A dead process is not a deaf agent.** A session's watcher, subscriptions, and
sentinel all outlive the Claude Code process they belong to, because `SessionEnd`
does not run when a terminal is closed or a process is killed. On the measured
machine 322 of 347 known sessions had no live process at all, and every one of them
looked, from inside the bridge, exactly like a healthy idle agent waiting for mail.
An earlier investigation spent eleven refuted theories on a "deaf" population that
was mostly just gone. Any health check that cannot make this distinction will
mislead in the same way.

**Measuring one session at a time cannot separate a broken session from a broken
moment.** A per-session loop that finds silence has no way to know whether the fleet
was fine and this agent is not, or whether something global hiccuped for ten seconds.

**A busy session is silent too.** This one was found the hard way: the first
version of this probe reported 11 deaf sessions on a live fleet, and 4 of them were
simply mid-turn — including the session running the probe. A session executing a
turn cannot run its `FileChanged` hook, so it looks exactly like a deaf one, and it
is not a fault at all: it collects its mail at the turn boundary
([ADR-0012](0012-level-triggered-wake-at-the-turn-boundary.md)). A health check that
cannot make this distinction slanders every working agent that happens to be busy —
the same conflation that sent the original investigation chasing ghosts.

## Decision

**1. The `FileChanged` wake hook records that it ran.** On every invocation, before
it consults the store or decides anything, `mailbox harness wake` writes a
nanosecond stamp to `<sentinel-dir>/.mailbox-hook-ran`. It is stamped on *every*
exit path, including the exit-0 anti-loop path: the fact being recorded is not what
the hook decided but that Claude Code delivered the event at all, which is precisely
the hop the bridge has never been able to observe. Writing it only on wakes would
make a healthy, caught-up session indistinguishable from an unwatched one — the
confusion this record exists to end.

The basename follows the `.mailbox-retriggered` precedent: deliberately *not* a
`.mailbox-wake…` name, because the `FileChanged` matcher is the sentinel's basename
and a collision would make the hook trigger itself forever. It lives in the
per-session directory, so `SessionEnd` teardown removes it for free.

**2. `mailbox doctor` proves wakeability by asking.** For each session it reads the
ack stamp, bumps the sentinel, and waits for the stamp to change. A changed stamp is
positive proof the watch is live; an unchanged one after the budget is the absence
of that proof.

The bump is **content-preserving** (`Sentinel::bump_in_place` rewrites the file with
the bytes it already holds). A probe that recomputed the topic set could change what
the agent is told is unread — an observation that alters the thing observed. Only
the watcher decides sentinel content.

**3. Every session is bumped before any is polled.** The whole fleet is measured in
one window, which is what makes "this session is deaf" separable from "something was
wrong for ten seconds".

**3a. Turn boundaries are recorded, so "busy" is a verdict rather than a libel.**
The `UserPromptSubmit` hook stamps `.mailbox-turn-started`; the `Stop` hook — which
already fires at every turn boundary — stamps `.mailbox-turn-ended`. A session is
mid-turn exactly when the start is newer than the end. An unanswered probe against a
busy session reports `Busy` (not a fault); only an unanswered probe against an
**idle** session is `Deaf`.

A wake deliberately does not count as a turn start: a turn opened by the wake hook
exiting 2 has, by construction, already written its ack before the turn began, so the
probe already has its answer. Both stamps are best-effort — losing one costs accuracy
in `doctor`, and failing a prompt-path hook over bookkeeping would cost a turn.

**4. Liveness comes from the process table, and "gone" is a first-class verdict.**
Claude Code carries its session id in its own argv (`--session-id`, `--resume`), so
`ps` can answer a question no mailbox-owned state can. `Reachability::Gone` is
explicitly **not** a fault. Only `Deaf` — a live process, an armed sentinel, and no
answer — is, and only that sets the exit code.

**5. `doctor` exits 1 when any session is deaf.** It is a check, not a report: a
supervisor or cron must be able to notice an unreachable agent without parsing
prose.

**6. `doctor` is read-only and socket-free**, the third such command after `wait`
and `dashboard`, for ADR-0015's reason: a health check has to work when the daemon
is the thing that is broken. It needs nothing from the daemon anyway.

## Consequences

- Wake health has a signal that can be trusted at a point in time, and the
  perishability finding means it must be **re-run**, not consulted once. A single
  green result ages out.
- The ack is written by whichever binary the hook invokes, so an out-of-date install
  makes the entire fleet report deaf. Rather than let that read as a catastrophe,
  `FleetReport::looks_like_a_stale_install` recognises "nothing at all answered" and
  the CLI says to check the install first. No session needs restarting after an
  upgrade — the hook re-invokes the binary every time.
- The probe writes to sentinel files, so it is not purely observational. It is
  bounded: mtime moves, content does not, and a session with nothing unread answers
  the resulting `FileChanged` with exit 0 and no model turn. Probing a session that
  *does* have unread mail will wake it — correctly, since it should already have been
  woken.
- `mailbox harness install-hooks` must be re-run to wire the new `UserPromptSubmit`
  hook. Until it is, busy sessions keep reporting as `deaf` — over-reporting, in the
  safe direction, but over-reporting.
- Reading `ps` is a new platform dependency. It is best-effort: if it cannot be run,
  `doctor` says so and degrades to not distinguishing `Gone`, rather than inventing a
  liveness answer.
- ADR-0015's log-derived view stays. It is still the only thing that can speak about
  the *past*, and it needs no live process — but `doctor` is now the answer to "can
  this agent be woken", and the dashboard's states should be read as history.

## Alternatives considered

**Keep inferring from `harness.log`.** Rejected on measurement: 9 of 19 sessions
misclassified, in both directions. No amount of parsing fixes a signal that cannot
distinguish "never needed to wake" from "cannot wake".

**Have the probe watch `harness.log` for the hook's line instead of adding an ack.**
This is what the manual investigation did, and it works — but it makes a health check
depend on log retention, log format, and an 8 MiB tail window, and it cannot tell two
runs apart within the same second. A stamped record is a contract; a log line is a
side effect.

**Wake the agent and wait for it to reply.** The only truly end-to-end test, and far
too invasive to run across a fleet: every probe would cost a model turn on every
healthy agent.

**Have the bridge probe continuously and alarm on its own.** Deferred deliberately.
Continuous probing needs a policy for what to do about a deaf agent, and the decision
taken with the user is to **detect and alarm, not auto-recover** — the bridge must not
poke sessions by itself. `doctor` is the mechanism; scheduling and escalation are the
operator's, and can be built on its exit code and `--json`.

**Detect "busy" from child processes instead of turn stamps.** Tried first, and it
is a poor proxy: a long-lived MCP server child makes an idle session look busy, and a
session thinking with no tool call in flight looks idle while it is mid-turn. The two
hook stamps answer the question exactly, at the cost of one more hook.

**Report `Gone` as a fault.** Rejected: it is the normal end state of most sessions,
it is not actionable, and treating it as a fault is exactly what made a real
single-digit problem look like a fleet-wide collapse.
