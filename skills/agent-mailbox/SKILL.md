---
name: agent-mailbox
description: >-
  Wake this idle Claude Code agent when the world changes — a watched GitHub PR
  gains a merge conflict, a review, or a CI failure, or a peer agent publishes to
  a shared topic. Replaces the older agent-ipc / agent-ipc-github skills. Use when
  the agent should go idle (or do other work) and be nudged to react to an
  external event through the mailbox bridge. The agent subscribes/watches once and
  reads on wake; it NEVER re-arms and NEVER spawns a background poller.
---

# agent-mailbox

Wake an idle agent when something it cares about changes, via the local
`mailbox` bridge. This **replaces** the old `agent-ipc` and `agent-ipc-github`
skills. If you are looking for `ipc-arm.sh` or `gh-watch.sh`, they are gone —
read the two hard rules below.

## Two hard rules (this is the whole point)

1. **NEVER re-arm.** Do not run any arm command after a wake. The
   `SessionStart` / `Stop` Claude Code hooks (`mailbox harness arm`) keep the
   waiter armed for you. If you catch yourself about to "re-arm listening,"
   stop — it is already armed.
2. **NEVER spawn a background poller.** Do not run `gh-watch.sh`, and do not
   background a `while true; gh …; sleep` loop or anything like it. To watch a
   PR, declare a `watch` — the bridge daemon owns and supervises the poller (one
   per PR, shared across sessions, torn down when no one is left watching).

Violating either rule recreates the exact failure the mailbox was built to kill:
zombie pollers and lost wakes.

## Prerequisites (assume already set up; do not do these yourself)

- `mailbox serve` is running (the bridge daemon).
- The Claude Code hooks are installed
  (`mailbox harness install-hooks`), so arming and cleanup are automatic.
- Your session id is available as `$MAILBOX_SESSION_ID` (the hooks set it; the
  `--session` flag overrides it if you need to be explicit).

If `mailbox` commands fail with `bridge not running; start it with 'mailbox
serve'`, tell the user — do not try to start a daemon or poll yourself.

## The loop: subscribe → idle → read → react → unsubscribe

### Watch a GitHub PR

```bash
mailbox watch github-pr OWNER/REPO#NUMBER --session "$MAILBOX_SESSION_ID"
```

This records your interest, subscribes you to the PR topic, and (via the daemon)
starts the shared edge-triggered poller. It baselines on its first poll and then
publishes only **transitions**: merge conflict, new review / review-thread / PR
comment, or CI rollup going red. Then **go idle or do other work** — do not poll.

### Subscribe to a peer / custom topic

```bash
mailbox subscribe TOPIC --session "$MAILBOX_SESSION_ID"
# a peer agent (or you) publishes with:
mailbox publish TOPIC --body '{"...":"..."}'
```

### On wake

When the world changes, the bridge kicks your armed waiter and Claude Code
surfaces a system reminder like `mail on topic github.pr.OWNER/REPO#NUMBER`. When
you see it:

```bash
mailbox read --session "$MAILBOX_SESSION_ID"
```

`read` returns the unread events and advances your cursor (exactly-once). React
to what you read — resolve the conflict, address the review, fix CI, reply to the
peer. Then just end your turn; the `Stop` hook re-arms the waiter. **You do
nothing to re-arm.**

### Check state (read-only)

```bash
mailbox status --session "$MAILBOX_SESSION_ID"
```

Shows your watches (and whether each poller is `running` with a pid), your
subscriptions, and per-topic unread counts. It does not consume events.

### When done

```bash
mailbox unwatch github-pr OWNER/REPO#NUMBER --session "$MAILBOX_SESSION_ID"
# or, for a plain topic:
mailbox unsubscribe TOPIC --session "$MAILBOX_SESSION_ID"
```

You do not have to clean up on exit — the `SessionEnd` hook drops your
subscriptions and interests and stops any poller you were the last to watch. Only
`unwatch`/`unsubscribe` when you want to stop caring *before* the session ends.

## Quick reference

| Verb | Command |
|---|---|
| watch a PR | `mailbox watch github-pr OWNER/REPO#N --session "$MAILBOX_SESSION_ID"` |
| subscribe to a topic | `mailbox subscribe TOPIC --session "$MAILBOX_SESSION_ID"` |
| read on wake | `mailbox read --session "$MAILBOX_SESSION_ID"` |
| check state | `mailbox status --session "$MAILBOX_SESSION_ID"` |
| stop watching a PR | `mailbox unwatch github-pr OWNER/REPO#N --session "$MAILBOX_SESSION_ID"` |
| unsubscribe | `mailbox unsubscribe TOPIC --session "$MAILBOX_SESSION_ID"` |

Never: `ipc-arm.sh`, `gh-watch.sh`, a background `gh` poll loop, or any re-arm
command. Arming and poller supervision are infrastructure, not your job.
