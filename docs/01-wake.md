# Wake: how an idle session hears about the world

How an idle agent session gets woken when the world changes, and how it stays
wakeable for the whole session with the agent doing nothing at all — no arm
command, no re-arm, nothing on a timer.

The design in force is [ADR-0021](adr/0021-delete-the-sentinel-fallback.md) (the
inbox socket is the only wake wire) over
[ADR-0020](adr/0020-peer-inbox-socket-is-the-wake-wire.md) (how that socket works),
plus [ADR-0022](adr/0022-the-wake-carries-a-subject.md) (what the wake says).

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
{"type":"user","message":{"role":"user","content":"<cross-session-message from-name=\"agent-mailbox\">\n[agent-mailbox] 3 new messages on 2 topics\n…\n</cross-session-message>"}}
```

Newline-terminated, written to the session's `messagingSocketPath`. The pane shows it
as one dim line ([ADR-0033](adr/0033-the-wake-rides-the-cross-session-envelope.md)):

```text
› Message from @agent-mailbox: [agent-mailbox] 3 new messages on 2 topics (ctrl+o to expand)
```

What the agent reads, inside the envelope, is:

```
[agent-mailbox] 3 new messages on 2 topics

github.pr.acme/web#42 — 2 unread
  · CI failed: build
    https://github.com/acme/web/actions/runs/9/job/2
  · new comment
    https://github.com/acme/web/pull/42#issuecomment-2145678

agent.983eae5f-0b09 — 1 unread
  · from 700a3bf5-1c4d: PR 42 review finished

Run `mailbox read` to read them.
```

A single event puts everything in the first line, because that is the line the pane
shows:

```
[agent-mailbox] one new message from github.pr.acme/web#42 — CI failed: build
https://github.com/acme/web/actions/runs/9/job/2

Run `mailbox read` to read it.
```

The headline names the session's own inbox as `your inbox`. Its topic is
`agent.<session id>`, and the 36-character id would push the subject off the preview.

Five properties are deliberate, and each is pinned by a test:

- **Pointer, not payload** ([ADR-0022](adr/0022-the-wake-carries-a-subject.md)).
  `content` carries topic names, unread counts, and each event's `subject` — one line
  its publisher wrote saying *what* changed, and a link to it. The event **body** stays
  in the durable log until the agent's `read`. The socket *could* carry a body; it must
  not, or the agent would have two places to look for the truth.
- **One line in the pane.** The `<cross-session-message>` tag is what Claude Code's
  own `SendMessage` uses, and the pane draws text that opens with it as a single
  collapsed line. Without it, Claude Code prints its whole peer wrapper around every
  wake — about 8 lines on top of ours. The model still receives the wrapper, the tag
  and the full text either way; only the pane changes.
- **Tagged `[agent-mailbox]`.** The first thing inside the envelope, so a woken agent
  knows which system started its turn and which skill to load. The envelope's opening
  tag before it is fixed bridge text. A `Subject` cannot contain a newline, so nothing
  after that line can forge another one — or a second bullet, or a second topic block.
  An envelope tag inside the text is broken (`<` becomes `<\`) the way Claude Code's
  own sender does it, so a subject cannot close the envelope early either.
- **No permission class asserted.** The envelope `SendMessage` writes also carries a
  `from-mode` attribute claiming whether the sender bypasses permission prompts. It
  is believed without verification — and claiming `bypass` is exactly what makes an
  ordinary permission-prompting session HOLD the message. We take the tag and
  `from-name` only: no `from-mode`, and no top-level `from` reply address, since there
  is no Claude session at our end to reply to.
- **Never empty.** Delivery *is* the turn; there is no second opinion between the
  socket and the model. A wake with nothing unread would spend a model turn announcing
  nothing, so it is not sent.

### Subjects

A subject is optional at every layer. An adapter that has nothing useful to say omits
it, and its subscribers are woken with the topic and a count — the pre-subject
behaviour, with a prefix.

| Rule | Why |
|---|---|
| One line, ≤120 chars, control characters collapsed | The frame's layout must be unforgeable by adapter text |
| Links must be `http(s)`, bounded, whitespace-free | It is rendered for a model to follow |
| Over-long text truncates; an unusable link is dropped | A wake degrades to a *worse* subject, never to no subject |
| Newest 3 per topic, 8 topics, overflow stated (`…and 4 earlier`) | A wake is a summary, not the read it prompts |
| The count is of EVERYTHING unread | The wake may under-describe what `read` returns, never over-describe |

Publishers set one with `mailbox publish --subject "…" [--link URL]`, or
`mailbox send --subject "…"` for a peer message — where the bridge composes
`from <sender>: <subject>` from the `from` it verified. **The message text is never
used as the subject**: a wake says what is waiting, not what it says.

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
`[uds-messaging] Skipped: cross-session messaging gate off` and binds nothing.

The gate is a gradual rollout, so same-version sessions on one machine disagree — but
it **can** be forced on with `CLAUDE_CODE_HARBOR_KITE=1` in *user* settings, which
short-circuits the remote flag entirely
([README](../README.md#setup-what-the-wake-depends-on) has the caveats; verified on
2.1.229). Do not confuse it with `CLAUDE_CODE_MESSAGING_SOCKET`, which is an *output*
Claude Code exports to hooks, not an input.

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
| `SessionStart` (matcher `""` — all sources) | `mailbox harness session-start` | Registers the always-on agent inbox (`agent.<session-id>`, ADR-0007) so peers can address this session, then resumes the session's suspended watches (ADR-0026). Matcher `""` so it re-fires on resume/clear/compact (ADR-0013); idempotent. Fail-open, exits 0. |
| `SessionEnd` | `mailbox harness cleanup` | Suspends this session's subscriptions and interests, so no poller outlives the session that wanted it; a resume of the same id restores them (ADR-0026). |

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

## Checking how a wake renders

The one-line pane rendering depends on undocumented Claude Code behaviour: the pane
draws text that opens with `<cross-session-message …>` through its compact peer
renderer ([ADR-0033](adr/0033-the-wake-rides-the-cross-session-envelope.md)). After a
Claude Code upgrade, or if wakes start showing as full blocks again, check it:

```bash
cargo build -p mailbox
scripts/probe-wake-render.py            # one event, default permission mode
scripts/probe-wake-render.py --bypass   # a bypassPermissions receiver, like an unattended fleet
```

It starts a private `mailbox serve` and a throwaway Claude Code session (no user
settings, so no hooks reach the shared bridge), publishes to it, and reports whether
the pane showed `Message from @agent-mailbox` or Claude Code's full wrapper. Each run
spends one small Haiku turn. ADR-0033 records how the renderer was found in the
Claude Code bundle, in case the tag changes and has to be found again.

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
| Per-session inbox socket (external process → idle wake) | Yes, 2.1.226+ — feature-gated; forced on with `CLAUDE_CODE_HARBOR_KITE=1` | **No** |
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
    Bridge->>Claude: write the inbox socket ("[agent-mailbox] one new message from X — what changed")
    Claude->>Agent: start a turn with the message
    Agent->>Bridge: read(my cursors)
    Bridge-->>Agent: unread events (bodies live here, never on the wake wire)
    Agent->>Agent: react (tools, edits, replies)
```

**`read` is the only step that consumes**, and it consumes in the same transaction
that returns the events — so a turn that dies *after* that commit has taken its mail
without ever seeing it, and the next `status` reports zero unread. `mailbox history`
reads the same durable log and advances nothing, which is how that mail is recovered
([ADR-0032](adr/0032-history-reads-past-the-cursor-without-moving-it.md),
[usage](04-usage.md#looking-back-through-mail)).

## Contrast with `agent-ipc`

The earlier `agent-ipc` skill used a durable NDJSON inbox + FIFO kick + `ipc-arm.sh`
run by the agent in background mode. **One arm = one wake**; after reading, the agent
had to re-arm. Adapters were already just senders — that split stays. What is new is
that the agent arms nothing at all: the harness delivers, and the bridge only has to
know where to write.
