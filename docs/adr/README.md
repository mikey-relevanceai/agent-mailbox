# Architecture Decision Records

ADRs capture **decisions that should stay true** until explicitly superseded.
They are not a substitute for design docs (see [`../design/`](../design/)).

## How the wake path actually works today

Read this before reading twenty-one ADRs, because most of what they describe has
been deleted.

```text
publish → write that subscriber's Claude Code inbox socket
        → the idle session takes a turn
```

**One hop, and that is the whole wake path.** No file, no watch, no hook, no exit
code. Claude Code binds a per-session Unix socket and starts a turn on an idle
session when something is written to it; the daemon writes it directly, in the
request that just committed the event.

The ADRs in force for that path are:

- [**0021**](0021-delete-the-sentinel-fallback.md) — the inbox socket is the ONLY wake
  wire; the sentinel + `FileChanged` path and everything propping it up is deleted.
  **Start here.**
- [**0020**](0020-peer-inbox-socket-is-the-wake-wire.md) — how the socket works: the
  frame, why we assert no permission class, and the inbound gate a
  `bypassPermissions` session has to opt past.
- [**0018**](0018-publish-has-one-rule.md) — a publish goes to the topic and wakes
  every subscriber, its author included. That is the whole contract.
- [**0022**](0022-the-wake-carries-a-subject.md) — what the wake actually SAYS: topic
  names, unread counts, and each event's one-line `subject` and link, behind an
  `[agent-mailbox]` tag. Never a body.
- [**0024**](0024-status-reports-the-wake-verdict.md) — `status` says whether this
  session can be woken, from the same read `watch` refuses on, so the self-check and
  the refusal cannot disagree.

Eight ADRs describe the wake path that was **deleted** in 0021 — 0008 and 0017 (the
sentinel), 0012 (the turn-boundary re-trigger), 0016 (the active probe), and the
liveness/resume work around them. Read them for the reasoning, not the mechanism:
they are the record of what it costs to build a wake wire out of parts the harness
never offered as one.

Anything below whose Status says superseded or amended is **history**: read it for
the reasoning, not for the mechanism.

## Index

| ADR | Title | Status |
|---|---|---|
| [0001](0001-rust-bridge-subprocess-adapters.md) | Rust bridge, subprocess adapters, local-only v0 | Accepted |
| [0002](0002-mvp-crate-stack.md) | MVP crate stack (serde, tracing, clap, rusqlite bundled, …) | Accepted |
| [0003](0003-single-writer-sqlite.md) | Single-writer SQLite; others queue via the bridge | Accepted |
| [0004](0004-cli-serve-daemon-and-socket.md) | `serve` daemon + Unix-socket CLI clients (fail loud when down) | Accepted; its `wait` carve-out is void — `mailbox wait` no longer exists, so the read-only exception is now `doctor` (0016) |
| [0005](0005-baseline-via-protocol.md) | Edge-triggered adapter baseline persists via the protocol (config in, `Baseline` out) | Accepted |
| [0006](0006-harness-self-respawn.md) | Harness wake loop: the re-arm exit (execv does NOT reset the hook timeout), waiter-owned pidfile after the lock, stale-pidfile reap, arm-iff-subscribed | **Fully superseded — by 0008 (the re-arm loop; `arm`/`wait` retired from the hooks, and later deleted outright), 0017 (the waiter and its pidfile) and 0018 (the publish rules)** |
| [0007](0007-always-on-agent-inboxes.md) | Always-on agent inboxes (`agent.<session-id>`), discovery, and why `send` to an unregistered agent is an error | Accepted; the inbox decision stands, but its registrar (`harness arm`) and its live-waiter liveness probe are gone — see 0013 and 0017 |
| [0008](0008-on-demand-wake-filechanged.md) | On-demand wake: the bridge bumps a per-session sentinel, a `FileChanged` hook wakes only on genuine mail — no periodic re-arm | Accepted; busy-window edge loss **fixed by 0012**; the detached watcher it introduced **deleted by 0017**; **demoted to the fallback channel by 0020**; **path deleted by 0021** |
| [0009](0009-interest-liveness-from-the-waiter-pidfile.md) | Interest liveness comes from the waiter pidfile, not a TTL clock — an idle session is silent by design, so silence cannot mean death | **Superseded by 0017**; the premise (silence ≠ death) stands, the pidfile is gone |
| [0010](0010-resume-watches-on-restart.md) | A daemon restart resumes the watches of live sessions — "never resume" stopped being fail-safe once an idle session could no longer re-`watch` | Accepted; the decision stands, its liveness probe is **amended by 0017** (process table, not pidfile) |
| [0011](0011-retry-failed-watches-on-sweep.md) | A give-up is not permanent — the sweep retries a `Failed` watch once per interval while its session lives, so a transient upstream outage self-heals | Accepted; the decision stands, its liveness probe is **amended by 0017** (process table, not pidfile) and its give-up event rate **by 0023** (announced once per outage, not once per retry) |
| [0012](0012-level-triggered-wake-at-the-turn-boundary.md) | Level-triggered wake at the turn boundary: `Stop` re-bumps the sentinel for unread mail, because a wake edge spent while the session was BUSY is lost forever | Accepted; the requirement stands, the `ensure-watcher` hook that carried it is **amended by 0017** (now `turn-end`; there is no watcher); **deleted by 0021** |
| [0013](0013-re-register-inbox-and-watchpaths-on-resume.md) | Re-register the inbox and watchPaths on resume: `session-start` fires on every `SessionStart` source (matcher `""`), and the `Stop` hook re-registers the inbox every turn — restoring ADR-0007's register-on-every-hook invariant that ADR-0008 dropped | Accepted; both decisions stand, **amended by 0017** (the hook is `turn-end`, and it ensures no watcher) |
| [0014](0014-self-authored-events-wake-their-author.md) | A self-authored event wakes its author: authorship is provenance, not evidence of knowledge, so it no longer suppresses a wake — reversing ADR-0006's "no self-wake" and giving `status` and the wake path one definition of "unread" | Accepted; **amended by 0018** (authorship is no longer recorded at all) |
| [0015](0015-dashboard-reads-the-store-read-only.md) | `mailbox dashboard` reads the store READ-ONLY (2nd exception to the socket-client rule) so a fleet health view still renders when the daemon is down; wake health is reported as three-valued evidence, never as a "deaf" verdict | **Superseded by 0016; command removed** |
| [0016](0016-prove-wakeability-with-an-active-probe.md) | Prove wakeability with an ACTIVE probe (`mailbox doctor`) plus a hook-ran ack, because log-derived wake health was wrong in both directions and wakeability turns out to be perishable; a session with no live process is `gone`, not a fault | Accepted; one clause overtaken — `wait` and `dashboard` no longer exist, so `doctor` is the ONLY read-only socket-free command; **deleted by 0021** |
| [0017](0017-daemon-bumps-the-sentinel.md) | The daemon bumps the sentinel directly: delete the per-session watcher, the FIFO, the single-waiter lock and the pidfile — wakeability was an emergent property of six components, each able to fail silently; liveness moves to the process table | Accepted; **demoted to the fallback channel by 0020**, and its session discovery superseded by Claude Code's own session registry; **path deleted by 0021** |
| [0018](0018-publish-has-one-rule.md) | `publish` has ONE rule: the event goes to the topic and wakes every subscriber, its author included. "Be caught up to speak", `--no-session` and `event.author_session` are deleted — the bridge no longer tries to know who is speaking | Accepted |
| [0019](0019-remove-the-observability-and-re-arm-surfaces.md) | Delete `harness arm`, `mailbox wait` and the whole max-block apparatus (retained primitives nothing invoked), and `mailbox dashboard` (inferred wake health, measured wrong in BOTH directions against 0016's probe). `doctor` is the only wake-health surface | Accepted |
| [0020](0020-peer-inbox-socket-is-the-wake-wire.md) | The peer inbox socket is the wake wire: publish writes Claude Code's per-session Unix socket directly and the idle session takes a turn, with the sentinel + `FileChanged` path retained as the fallback for sessions whose socket the `agents_cross_session_inbox` gate never bound. Claim no permission class; stay payload-free; never set `crossSessionInbound` for the operator | Accepted; **its fallback is deleted by 0021**, and its payload-free clause **amended by 0022** (the frame carries subjects, never bodies) |
| [0021](0021-delete-the-sentinel-fallback.md) | Delete the sentinel fallback: the inbox socket is the ONLY wake wire. Removes `sentinel.rs`, `watchPaths`, the `FileChanged`/`asyncRewake` exit-2 hook, ADR-0012's turn-boundary re-trigger and ADR-0016's active probe (the hook set drops from five to two, neither able to wake). `doctor` becomes a read; `subscribe`/`watch` refuse for a session nothing can wake. Written after watching the fallback go silently deaf for 6.3h with everything configured correctly | Accepted; its payload-free clause **amended by 0022** |
| [0022](0022-the-wake-carries-a-subject.md) | The wake carries a subject — **pointer, not payload**. An event may publish one bounded, single-line `subject` (plus an optional link); the wake renders the subjects of what is unread, prefixed `[agent-mailbox]`. Bodies still never cross the wake boundary. Replaces "mail on topic X", which cost every woken agent a re-derivation of the delta the adapter already knew | Accepted |
| [0023](0023-one-give-up-notice-per-outage.md) | One give-up notice per outage, withdrawn by an `adapter_recovered` event. 0011's sweep retry re-published `adapter_gave_up` every interval, so a laptop losing its network woke every agent watching each of eight PRs 13 times in an afternoon to say the same thing. The retry is unchanged; only the repeat announcement is suppressed, and recovery is detected by a stability timer because the adapter that recovers is the one that stops exiting | Accepted |
| [0024](0024-status-reports-the-wake-verdict.md) | `status` reports the wake verdict, derived locally from the same `doctor::reachability_of` that `watch` refuses on — so the command an agent checks itself with cannot contradict the command that refuses it. `unknown` stays an answer and never a verdict; the human label becomes `inbox topic:` while the `inbox` JSON key is left alone. Written after an agent distrusted a correct refusal and went back to polling | Accepted |
| [0025](0025-hooks-point-at-a-stable-path.md) | The hook path is absolute but never canonical: `abs_bin` anchors with `std::path::absolute` instead of resolving with `canonicalize`, so `settings.json` records Homebrew's stable `opt_bin` symlink rather than the versioned Cellar path `brew upgrade` deletes. Written while setting up the tap, on noticing that a routine upgrade would silently stop peers being able to address a session while topic wakes kept working | Accepted |
| [0026](0026-suspend-watches-on-session-end.md) | An ended session's watches are suspended, not deleted, and `session-start` restores them: Claude Code resumes a session under the same id, and a harness that quits and reopens its sessions left every resumed agent addressable but deaf. Suspended state expires after 30 days | Accepted |

## When to write one

Write an ADR when you choose something that later contributors (human or agent)
might reasonably reverse without noticing the cost — language, storage,
transport, security model, multi-subscriber semantics, harness wake strategy.

Skip ADRs for routine implementation detail that a design doc or code comment
covers.

## Template

Copy into `NNNN-short-title.md` (zero-padded, next free number):

```markdown
# ADR-NNNN: Title

- Status: Proposed | Accepted | Deprecated | Superseded by ADR-XXXX
- Date: YYYY-MM-DD

## Context

What forces the decision?

## Decision

What we will do.

## Consequences

What becomes easier, harder, or constrained.

## Alternatives considered

What we rejected and why (brief).
```
