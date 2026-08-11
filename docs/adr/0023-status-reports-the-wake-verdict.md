# ADR-0023: `status` reports the wake verdict, from the same read `watch` refuses on

- Status: Accepted
- Date: 2026-08-11
- Builds on: [ADR-0021](0021-delete-the-sentinel-fallback.md), which made `doctor` a
  daemon-free read and had `subscribe`/`watch` refuse a session nothing can wake.
- Relates to: [ADR-0007](0007-always-on-agent-inboxes.md) — the *other* thing called an
  inbox, and the one `status` used to report on its own.

## Context

An agent tried to watch a PR and was refused:

```
9156c9fe-… has no Claude Code inbox socket, so nothing can wake it.
```

The refusal was **correct**. That session genuinely had no socket, confirmed three
ways: no `messagingSocketPath` in `~/.claude/sessions/53258.json`, no socket file at
`/tmp/cc-socks/53258.sock`, and `mailbox doctor --session 9156c9fe-…` → `no-inbox`. The
cause was Claude Code's `agents_cross_session_inbox` gate simply not turning on for
that session — same binary, same launcher, same flags as socketed peers started minutes
apart. Nothing we can fix, and ADR-0021 accepted exactly that risk on the grounds that
it fails **loudly**.

It did not fail loudly enough, because the agent then ran the command an agent
naturally runs to check itself:

```
inbox: agent.9156c9fe-… (registered)
```

So one command said it could not be woken and the other said it was fine. The agent
read that as the system contradicting itself, distrusted the refusal, and fell back to
polling — the one habit `skills/agent-mailbox/SKILL.md` exists to remove. It missed the
review it was waiting for.

Nothing here was a lie. The word "inbox" was doing two unrelated jobs:

| Line | Means | A fact about |
|---|---|---|
| `status`'s `inbox: … (registered)` | this session is subscribed to its own topic | our SQLite store |
| `watch`'s "no Claude Code inbox socket" | Claude Code bound this process no socket | the process |

`StatusReport` carried no reachability at all, so `status` had nothing to say about the
question its reader thought it was answering.

## Decision

**`status` reports the wake verdict, derived locally, from the same
`doctor::reachability_of` that `watch` refuses on.**

```text
session: 9156c9fe-…
wake: no-inbox — Claude Code bound this session no inbox socket, so nothing can wake it. Restart the session. …
inbox topic: agent.9156c9fe-… (registered)
watches: none
```

Four things make this more than a new line of output:

1. **One read, so the two commands cannot disagree.** Both go through
   `doctor::wake_verdict`, a thin wrapper over `reachability_of`. The verdict `status`
   prints is the verdict `watch` would refuse on, in the same process, from the same
   registry read. There is no second definition to drift, and within `status` the read
   happens once and is handed to whichever renderer runs — the human and `--json`
   paths cannot describe the same session differently.
2. **Derived locally, so it survives a dead bridge.** `doctor` opens no store
   (ADR-0021), so the verdict belongs to `status`'s locally-derived identity half — the
   half that already answers "who am I" with `bridge: UNREACHABLE`. A session that
   cannot be woken learns so even when the daemon is the broken thing, which is
   precisely when it most needs to know.
3. **Unknown is an answer, not a verdict — and it is a type.** An unreadable registry
   reports `unknown` and never `no-inbox`. Absence of evidence is not evidence of
   absence (ADR-0009), and `subscribe`/`watch` already decline to refuse on it; a
   `status` that guessed `no-inbox` from a missing `~/.claude` would be the same bug
   pointed the other way. So the read returns `WakeVerdict::{Known(Reachability),
   Unknown}` rather than an `Option<Reachability>`: an `Option` invites exactly the
   collapse the rule forbids — `unwrap_or(NoInbox)`, or an `if let Some` that quietly
   skips the case — while a variant has to be matched. `unregistered` is likewise
   reported plainly and is not a fault: a harness that is not Claude Code looks exactly
   like it.
4. **The human label is renamed, the JSON key is not.** `inbox:` becomes
   `inbox topic:`, so the two senses stop colliding on screen. The `inbox` key in
   `--json` keeps its name — a Claude Code **status line** reads this object every
   prompt (the reason `subscription_count` exists), and renaming a key breaks that
   consumer silently. `wake` is purely additive.

The verdict is matched exhaustively where it is rendered, for the reason
`reachability_of` documents: a new variant must break compilation at every place that
decides something, and this is now one of them.

## Consequences

- **The self-check answers the question it is asked.** "Can I be woken?" is the load-
  bearing fact for an agent about to go idle, so it prints above the inbox topic: being
  addressable is worth nothing to a session nothing can wake.
- **`status` is no longer purely a bridge projection.** It now mixes a daemon snapshot
  with a local read of Claude Code's registry. That is the same split the identity half
  already had, and the alternative — asking the daemon — would have put the answer
  behind the component most likely to be broken.
- **Two names for one thing in `--json`, still.** The normal path emits `inbox` and the
  bridge-down path emits `inbox_topic`, a pre-existing wart this ADR deliberately does
  not fix: reconciling them means renaming a key some status line may read, and this
  change exists to stop silent breakage, not cause it.
- **A tiny cost per `status`.** One `readdir` of `~/.claude/sessions` plus a `kill(pid,
  0)` per entry. `doctor` and every `subscribe` already pay it.
- **It does not make anything wakeable.** The gate is Claude Code's. This makes the
  fault legible at the moment an agent is deciding whether to trust it.

## Alternatives considered

- **Have the daemon report reachability in `StatusReport`.** It already reads the
  registry to deliver wakes, so the data is there. Rejected twice over: it would make
  the verdict unavailable on the bridge-down path, which is where a stranded agent most
  needs it, and it would create a second derivation site — the exact shape ADR-0021
  collapsed into `reachability_of` when `subscribe`'s hand-rolled `if`-chain drifted
  from `doctor`'s.
- **Tell agents to run `doctor` as well as `status`.** A documentation fix for a
  wrong-default problem. An agent that has just been reassured by `status` has no reason
  to run a second command, and the one that misread it was following the skill.
- **Rename the `inbox` JSON key to `inbox_topic` for consistency.** Correct in the
  abstract, silently breaking in practice. See consequences.
- **Print the wake line only when it is a fault.** Cheaper output, but then `status`
  saying nothing means either "fine" or "old binary", and an agent cannot tell which. A
  verdict that is always present is one an agent can rely on.
