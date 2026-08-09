# Wake: how an idle session hears about the world

How an idle agent session gets woken when the world changes, and how it stays
wakeable for the whole session with the agent doing nothing at all — no arm
command, no re-arm, nothing on a timer.

The design in force is [ADR-0021](adr/0021-delete-the-sentinel-fallback.md) (the
inbox socket is the only wake wire) over
[ADR-0020](adr/0020-peer-inbox-socket-is-the-wake-wire.md) (how that socket works).

## The whole mechanism

```text
Adapters (detect world changes)
        ↓ publish
Bridge (durable events + subscriptions)
        ↓ write the subscriber's Claude Code inbox socket
Agent sessions (take a turn; never poll)
```

One hop. Claude Code binds a per-session Unix socket, and **when the receiving
session is idle it starts a turn with whatever is written there.** The daemon writes
it directly, inside the publish request that just committed the event.

There is no file, no watch, no hook, no exit code, and nothing per-session on our
side at all. Adapters only `publish`; the bridge owns durable logs, topics,
per-subscriber cursors and the wake delivery.

## What an agent does

Four verbs, and nothing else:

```bash
mailbox watch github-pr owner/repo#42   # subscribe + start the poller
# ... go idle. Nothing to arm, nothing to re-arm ...
mailbox read                            # on wake
mailbox unwatch github-pr owner/repo#42 # when done
```

An idle subscribed session costs **zero** model turns until real mail arrives.

## The frame

```json
{"type":"user","message":{"role":"user","content":"mail on topic github.pr.owner/repo#42"}}
```

Newline-terminated, written to the session's `messagingSocketPath`. Three properties
are deliberate, and each is pinned by a test:

- **Payload-free.** `content` is topic names only. The event body stays in the durable
  log until the agent's `read`. The socket *could* carry a body; it must not, or the
  agent would have two places to look for the truth.
- **No permission class asserted.** The richer envelope `SendMessage` writes carries a
  `from-mode` field claiming whether the sender bypasses permission prompts. It is
  believed without verification — and claiming `bypass` is exactly what makes an
  ordinary permission-prompting session HOLD the message. The honest frame is the more
  deliverable one.
- **Never empty.** Delivery *is* the turn; there is no second opinion between the
  socket and the model. A wake with nothing unread would spend a model turn announcing
  nothing, so it is not sent.

## Finding the session: Claude Code's registry

Every session registers itself at `~/.claude/sessions/<pid>.json`:

```json
{"pid":40741,"sessionId":"700a3bf5-…","cwd":"/…/arg","status":"idle",
 "messagingSocketPath":"/tmp/cc-socks/40741.sock","name":"arg-16"}
```

That is the whole lookup: **session id → inbox socket**. It also supplies session
liveness, replacing the old `ps` + argv scraping.

Two rules, both load-bearing:

- **A registry entry is not liveness.** It outlives the process that wrote it — this
  machine held nineteen entries spanning five days — so every entry is confirmed
  against the process table, and a socket path is checked for existence before use.
- **Names are derived and mutable.** Claude Code's `name` tracks the conversation and
  changes under you. It is fit for logs and never for addressing; key on `sessionId`.

Override the directory with `MAILBOX_CLAUDE_SESSIONS_DIR` (tests point it at a
tempdir), or `CLAUDE_CONFIG_DIR` if your whole Claude config lives elsewhere.

## A session with no inbox socket cannot be woken

Claude Code decides which sessions bind one, via a feature gate
(`agents_cross_session_inbox`). When it is off the session logs
`[uds-messaging] Skipped: cross-session messaging gate off` and binds nothing, and
**there is no setting, flag or environment variable that turns it on** —
`CLAUDE_CODE_MESSAGING_SOCKET` is an *output* Claude Code exports to hooks, not an
input.

Such a session is not degraded, it is unwakeable. So we say so at the only moment it
can still hear us:

```
$ mailbox subscribe github.pr.owner/repo#42
mailbox: 983eae5f-… has no Claude Code inbox socket, so nothing can wake it —
subscribing would leave you waiting on mail you would never be told about.
Claude Code bound this session no inbox socket… Restart the session.
```

`subscribe` and `watch` refuse; `publish` and `read` do not, so an unwakeable session
can still be sent to and can still read what it was sent. Requires Claude Code
**2.1.226+**.

## Receiving on a `--dangerously-skip-permissions` session

Claude Code decides delivery from **both** sessions' permission-mode classes, where
`bypassPermissions` is one class and everything else (default, `auto`, `acceptEdits`,
`dontAsk`) is the other. With nothing configured:

| Receiver | Sender claims | Outcome |
|---|---|---|
| prompting | nothing | **delivered** — nothing to configure |
| bypass | nothing | **held** for approval, dropped after `dialogExpiry` (default 5m) |

So an ordinary session wakes out of the box, and a `bypassPermissions` session does
not — its wakes are held behind a dialog nobody is there to answer, then dropped. The
fix is Claude Code's `crossSessionInbound: "accept"`, which
`mailbox harness install-inbound` sets **only when you ask it to**:

```bash
mailbox harness install-inbound --settings <file>   # preferred: just this fleet
mailbox harness install-inbound                     # user settings (every session)
```

Understand what it widens first. `accept` means that session takes messages from any
process running as you, without a prompt — which is what lets the bridge wake an agent
that acts without asking, and equally means anything else running as you can direct
it. Prefer a per-session `--settings` file over user settings. `install-hooks` never
does this, and never will: it is a separate decision with a separate consequence.

## The hooks: two, and neither can wake

| Hook | Command | What it does |
|---|---|---|
| `SessionStart` (matcher `""` — all sources) | `mailbox harness session-start` | Registers the always-on agent inbox (`agent.<session-id>`, ADR-0007) so peers can address this session. Matcher `""` so it re-fires on resume/clear/compact (ADR-0013); idempotent. Fail-open, exits 0. |
| `SessionEnd` | `mailbox harness cleanup` | Drops this session's subscriptions and interests, so no poller outlives the session that wanted it. |

Plus two setup commands: `install-hooks` (merges the snippet atomically, preserving
unrelated settings) and `install-inbound` (above, opt-in).

```json
{
  "hooks": {
    "SessionStart": [{ "matcher": "", "hooks": [{ "type": "command",
      "command": "/abs/path/to/mailbox harness session-start" }] }],
    "SessionEnd": [{ "matcher": "", "hooks": [{ "type": "command",
      "command": "/abs/path/to/mailbox harness cleanup" }] }]
  }
}
```

> **After upgrading the binary, re-run `mailbox harness install-hooks`.** It sweeps
> every hook name we have ever installed, so an upgrade cannot leave a
> `FileChanged → wake` firing against a subcommand the binary no longer has.

## Diagnosing a wake ("why didn't my agent wake?")

**Run `mailbox doctor`.** It is a read, not a probe: reachability is two facts —
is the process alive, and did Claude Code bind it a socket.

```
no-inbox   983eae5f-0b09-410e-a457-c9633b6f1a8a  agent-mailbox-30
           Claude Code bound this session no inbox socket, so nothing can wake it.
           Restart the session. …
3 session(s): 2 reachable, 1 cannot be woken, 0 gone
```

| Verdict | Meaning |
|---|---|
| `reachable` | Live process, bound socket. A publish to a subscribed topic reaches it. |
| `no-inbox` | Live process, no socket — **the fault**. Nothing can wake it; restart the session. |
| `gone` | No live process. Normal, and not a fault. |

Exits 1 if anything is a fault, so a supervisor can gate on it. Socket-free and
read-only, so it still answers when the daemon is the broken thing. Unlike the probe
it replaced, a session can measure itself.

The daemon logs its per-publish outcome at INFO:

| Line | Means |
|---|---|
| `woke subscribers after publish` (`delivered` / `no_inbox` / `failed`) | how each subscriber fared |
| `a subscriber has no Claude Code inbox socket, so it cannot be woken` | that session lost its socket after subscribing |
| `could not deliver a subscriber's wake` | the socket refused the frame; the event is still durable |

## Design notes

**No coalescing.** Every publish to a subscribed topic delivers a frame. A burst of N
costs up to N wakes. That is deliberate: ADR-0008 removed its own coalescing after it
produced three separate silent-deafness bugs, and the same reasoning applies here —
every attempt to be clever about which publishes "need" a wake produced a lost one
instead. Claude Code drops identical repeats arriving close together, which blunts the
cost; the bridge does not rely on that.

**Delivery is bounded.** The write carries a one-second deadline. It runs inside the
publish request on a Tokio worker, and Claude Code caps accepted messages at 50 per
session — so a session that stops draining its inbox is a real state, and an unbounded
write would hang the publishing adapter and burn the worker thread.

**Being busy delays a wake, it does not destroy one.** A message queues at the
receiver and is read between tool calls. This is why the old turn-boundary re-trigger
(ADR-0012), its watermark, and the turn-started/turn-ended stamps could all be deleted
rather than ported: the failure they existed for cannot happen here.

**Why the sentinel is gone.** The previous design wrote a per-session file that a
`FileChanged` hook turned into an `asyncRewake` exit-2 wake. It was kept as a fallback
by ADR-0020 and deleted by ADR-0021 after being watched fail: a session whose
`session-start` armed correctly and printed its `watchPaths` received **zero**
`FileChanged` events in 6.3 hours, while other sessions' hooks fired 6,898 times off
the same writes. Everything on our side was correct and it was silently deaf anyway.
See ADR-0021 for the full evidence.

## Other harnesses

| Capability | Claude Code | Codex CLI |
|---|---|---|
| Per-session inbox socket (external process → idle wake) | Yes, 2.1.226+ — feature-gated, and the gate cannot be turned on | **No** |
| Lifecycle hooks | Yes | [Yes](https://developers.openai.com/codex/hooks) |
| Native external-event → idle wake | Yes, via the inbox socket | [Requested](https://github.com/openai/codex/issues/20312), not shipped |

Codex has no wake primitive, and never had one this project could use — its hooks have
no `asyncRewake` and no `FileChanged`. agent-mailbox targets Claude Code; another
harness needs its own primitive before it can be woken. Related:
[monitor tool FR](https://github.com/openai/codex/issues/29922),
[bg tasks don't wake parent](https://github.com/openai/codex/issues/15723).

## Sequence

```mermaid
sequenceDiagram
    autonumber
    participant Agent as Agent (model)
    participant Claude as Claude Code
    participant Bridge as Mailbox bridge
    participant Adapter as Adapter (e.g. gh-watch)

    Note over Agent,Adapter: Subscribe once — the agent never arms or re-arms

    Agent->>Bridge: watch / subscribe
    Bridge-->>Agent: refuses if this session has no inbox socket
    Agent->>Claude: end turn / go idle

    Adapter->>Bridge: publish(topic, event)
    Bridge->>Bridge: append durable log; advance offset
    Bridge->>Bridge: look up subscribers in ~/.claude/sessions
    Bridge->>Claude: write the inbox socket ("mail on topic X")
    Claude->>Agent: start a turn with the message
    Agent->>Bridge: read(my cursors)
    Bridge-->>Agent: unread events (bodies live here, not on the wire)
    Agent->>Agent: react (tools, edits, replies)
```

## Contrast with `agent-ipc`

The earlier `agent-ipc` skill used a durable NDJSON inbox + FIFO kick + `ipc-arm.sh`
run by the agent in background mode. **One arm = one wake**; after reading, the agent
had to re-arm. Adapters were already just senders — that split stays. What is new is
that the agent arms nothing at all: the harness delivers, and the bridge only has to
know where to write.
