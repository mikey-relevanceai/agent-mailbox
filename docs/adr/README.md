# Architecture Decision Records

ADRs capture **decisions that should stay true** until explicitly superseded.
They are not a substitute for design docs (see [`../design/`](../design/)).

## How the wake path actually works today

Read this before reading eighteen ADRs, because most of what they describe has
been deleted.

```text
publish → the `serve` daemon writes that subscriber's sentinel
        → Claude Code's `FileChanged` hook fires (even on an idle session)
        → `mailbox harness wake` exits 2 iff the store confirms unread mail
```

Three links, two live components and a file. There is **no per-session watcher
process, no FIFO, no waiter, no pidfile and no re-arm** — there were, and
wakeability being an emergent property of six components was why agents kept going
deaf.

The ADRs in force for that path are:

- [**0017**](0017-daemon-bumps-the-sentinel.md) — the daemon writes the sentinel
  itself, and session liveness comes from the process table. This is the current
  mechanism; start here.
- [**0012**](0012-level-triggered-wake-at-the-turn-boundary.md) — the turn boundary
  re-triggers for mail that landed while the session was BUSY. The *requirement*
  still holds; the `ensure-watcher` hook that implemented it is now `turn-end`.
- [**0016**](0016-prove-wakeability-with-an-active-probe.md) — `mailbox doctor`
  proves wakeability by asking, rather than inferring it from logs.
- [**0018**](0018-publish-has-one-rule.md) — a publish goes to the topic and wakes
  every subscriber, its author included. That is the whole contract.

[0008](0008-on-demand-wake-filechanged.md) is where the sentinel + `FileChanged`
design comes from and is still the best explanation of *why* the wake is a file
change rather than a timer — but everything it says about the machinery between the
daemon and that file has been deleted.

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
| [0008](0008-on-demand-wake-filechanged.md) | On-demand wake: the bridge bumps a per-session sentinel, a `FileChanged` hook wakes only on genuine mail — no periodic re-arm | Accepted; busy-window edge loss **fixed by 0012**; the detached watcher it introduced **deleted by 0017** |
| [0009](0009-interest-liveness-from-the-waiter-pidfile.md) | Interest liveness comes from the waiter pidfile, not a TTL clock — an idle session is silent by design, so silence cannot mean death | **Superseded by 0017**; the premise (silence ≠ death) stands, the pidfile is gone |
| [0010](0010-resume-watches-on-restart.md) | A daemon restart resumes the watches of live sessions — "never resume" stopped being fail-safe once an idle session could no longer re-`watch` | Accepted; the decision stands, its liveness probe is **amended by 0017** (process table, not pidfile) |
| [0011](0011-retry-failed-watches-on-sweep.md) | A give-up is not permanent — the sweep retries a `Failed` watch once per interval while its session lives, so a transient upstream outage self-heals | Accepted; the decision stands, its liveness probe is **amended by 0017** (process table, not pidfile) |
| [0012](0012-level-triggered-wake-at-the-turn-boundary.md) | Level-triggered wake at the turn boundary: `Stop` re-bumps the sentinel for unread mail, because a wake edge spent while the session was BUSY is lost forever | Accepted; the requirement stands, the `ensure-watcher` hook that carried it is **amended by 0017** (now `turn-end`; there is no watcher) |
| [0013](0013-re-register-inbox-and-watchpaths-on-resume.md) | Re-register the inbox and watchPaths on resume: `session-start` fires on every `SessionStart` source (matcher `""`), and the `Stop` hook re-registers the inbox every turn — restoring ADR-0007's register-on-every-hook invariant that ADR-0008 dropped | Accepted; both decisions stand, **amended by 0017** (the hook is `turn-end`, and it ensures no watcher) |
| [0014](0014-self-authored-events-wake-their-author.md) | A self-authored event wakes its author: authorship is provenance, not evidence of knowledge, so it no longer suppresses a wake — reversing ADR-0006's "no self-wake" and giving `status` and the wake path one definition of "unread" | Accepted; **amended by 0018** (authorship is no longer recorded at all) |
| [0015](0015-dashboard-reads-the-store-read-only.md) | `mailbox dashboard` reads the store READ-ONLY (2nd exception to the socket-client rule) so a fleet health view still renders when the daemon is down; wake health is reported as three-valued evidence, never as a "deaf" verdict | **Superseded by 0016; command removed** |
| [0016](0016-prove-wakeability-with-an-active-probe.md) | Prove wakeability with an ACTIVE probe (`mailbox doctor`) plus a hook-ran ack, because log-derived wake health was wrong in both directions and wakeability turns out to be perishable; a session with no live process is `gone`, not a fault | Accepted; one clause overtaken — `wait` and `dashboard` no longer exist, so `doctor` is the ONLY read-only socket-free command |
| [0017](0017-daemon-bumps-the-sentinel.md) | The daemon bumps the sentinel directly: delete the per-session watcher, the FIFO, the single-waiter lock and the pidfile — wakeability was an emergent property of six components, each able to fail silently; liveness moves to the process table | Accepted |
| [0018](0018-publish-has-one-rule.md) | `publish` has ONE rule: the event goes to the topic and wakes every subscriber, its author included. "Be caught up to speak", `--no-session` and `event.author_session` are deleted — the bridge no longer tries to know who is speaking | Accepted |

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
