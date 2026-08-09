# Wake: how an idle session hears about the world

How an idle agent session gets woken when the world changes, and how it stays
wakeable for the whole session with the agent doing nothing at all — no arm
command, no re-arm, nothing on a timer.

The design in force is [ADR-0020](adr/0020-peer-inbox-socket-is-the-wake-wire.md):
the daemon delivers straight onto the session's **Claude Code inbox socket**, and
falls back to the **sentinel + `FileChanged`** path of
[ADR-0017](adr/0017-daemon-bumps-the-sentinel.md) /
[ADR-0008](adr/0008-on-demand-wake-filechanged.md) for sessions that have no socket.
The **re-arm** this file used to be named for — ADR-0006's `Stop` → `arm` →
exit-2-at-`max_block` loop, one model turn per `max_block` of idle — no longer
exists in any form; it is described below only as the problem the current design
solves.

## Layers

```text
Adapters (detect world changes)
        ↓ publish
Bridge (durable events + subscriptions; delivers the wake)
        ↓ has this subscriber an inbox socket?
        ├─ yes → write it; the idle session takes a turn
        └─ no  → write its sentinel → FileChanged hook → exit 2 → wake
Agent sessions (react, never poll)
```

Adapters only `publish`. The bridge owns durable logs, topics, per-subscriber
cursors, **and the wake delivery**. Harness integrators own the hooks that serve the
fallback channel.

## Two channels, and why both exist

| | Peer inbox socket | Sentinel + `FileChanged` |
|---|---|---|
| Hops | 1 (daemon → session) | 4 (daemon → file → hook → exit 2 → session) |
| Needs hooks installed | No | Yes |
| Needs Claude Code ≥ 2.1.224 | Yes | No |
| Available | Only when Claude Code bound a socket | Always |
| Stray write costs | **a model turn** — the message IS the wake | a hook process; the hook re-checks the store and exits 0 |

The socket is preferred because it is one hop with nothing in between to fail
silently — which is what the sentinel path kept doing (ADR-0008's three coalescing
bugs, ADR-0009's sweeper, ADR-0012's spent edge, ADR-0013's un-re-registered
resume).

The sentinel stays because **whether a session binds a socket is not ours to
decide.** Claude Code gates it on `agents_cross_session_inbox`; when the gate is
off it logs `[uds-messaging] Skipped: cross-session messaging gate off` and binds
nothing. On the machine ADR-0020 was designed against, 2 of 19 live sessions had a
socket, across identical versions. There is no setting, flag or environment
variable that turns it on — `CLAUDE_CODE_MESSAGING_SOCKET` is an *output* Claude
Code exports to hooks, not an input.

**The asymmetry in the last row is load-bearing.** On the sentinel channel a write
is only a trigger, so a spurious one is harmless. On the peer channel delivery *is*
the turn, with no second opinion — so the daemon never sends a peer frame for an
empty unread set.

## The peer channel: the session's inbox socket

Since 2.1.224 a Claude Code session registers itself at
`~/.claude/sessions/<pid>.json` and — when the gate is on — binds a Unix socket
listed there as `messagingSocketPath`. Anything running as the same user can write
to it, and **if the session is idle, Claude Code starts a turn with the message.**
That is the wake primitive, delivered by the harness rather than assembled from
file watches.

On publish the daemon looks up each subscriber's `sessionId` in that registry and,
if it has a live socket, writes one newline-terminated frame:

```json
{"type":"user","message":{"role":"user","content":"mail on topic github.pr.o/r#42"}}
```

Three properties are deliberate, and each is pinned by a test:

- **We claim no permission class.** The richer envelope `SendMessage` writes carries
  `from-mode`, self-asserting whether the sender bypasses permission prompts. It is
  believed without verification, and setting it to `bypass` would reach a
  `bypassPermissions` receiver — but it is *also* what makes every ordinary
  permission-prompting receiver HOLD the message. The honest frame is the more
  deliverable one.
- **Payload-free, still.** `content` is the same topic-names-only reminder the
  exit-2 hook writes to stderr. The body stays in the durable log until `read`.
- **The registry is a lookup, not liveness.** A registry file outlives its process,
  exactly as ADR-0017 found for every other per-session artefact, so a socket that
  no longer exists reads as "no socket" and the subscriber falls back.

### Receiving on a `--dangerously-skip-permissions` session

Claude Code decides delivery from **both** sessions' permission classes, where
`bypassPermissions` is one class and everything else (default, `auto`,
`acceptEdits`, `dontAsk`) is the other. With nothing configured:

| Receiver | Sender claims | Outcome |
|---|---|---|
| prompting | nothing | **delivered** — nothing to configure |
| bypass | nothing | **held** for approval, dropped after `dialogExpiry` (default 5m) |

So an ordinary session wakes out of the box, and a `bypassPermissions` session
does not — its wakes are held behind a dialog nobody is there to answer, then
dropped. The fix is Claude Code's `crossSessionInbound: "accept"`, which
`mailbox harness install-inbound` sets **only when you ask it to**:

```bash
mailbox harness install-inbound                      # user settings (every session)
mailbox harness install-inbound --settings <file>    # preferred: just this fleet
```

Understand what it widens before running it. `accept` means that session takes
messages from any process running as you, without a prompt — which is what lets the
bridge wake it, and also means anything else running as you can direct an agent that
acts without asking. Prefer a per-session `--settings` file over user settings, so
it applies to the sessions that actually subscribe rather than every session you
run. `install-hooks` never does this, and never will: it is a separate decision with
a separate consequence.

Until you opt in, a `bypassPermissions` session is still served by the fallback
channel below — so it wakes, just through the hooks rather than the socket.

## The fallback channel: a sentinel file + a `FileChanged` wake

Claude Code can run a background hook with `asyncRewake: true`. When that process
exits with code **2**, the harness wakes an idle session and surfaces stderr as a
system reminder. Separately, a **`FileChanged`** hook fires — even on a truly-idle
session — when an external process changes a watched file, matched by **basename**.
ADR-0008 combines these so a wake happens **on demand** (when real mail arrives),
never on a timer:

1. Agent **subscribes** to topics once.
2. `SessionStart` runs `session-start`: it registers the inbox, **arms** this
   session's **sentinel** file (writes its current unread topics, creating the file),
   and prints the `watchPaths` registration for that absolute path.
3. An adapter publishes → the daemon appends the event and, in the same request,
   writes that subscriber's unread topic name(s) into its sentinel (bumping the mtime).
4. The sentinel change fires the `FileChanged` hook (`wake`), which exits **2** iff
   there is genuinely unread mail → the idle session wakes.
5. Agent **reads** unread events and reacts; ending the turn does nothing special —
   the sentinel is still watched and still armed for the next message.

The agent never runs an arm command, and **there is no periodic re-arm wake** — an
idle subscribed session costs zero model turns until real mail arrives.

**There is no per-session process anywhere in this.** There used to be: a detached
watcher blocked on a per-session FIFO, whose only job was step 3's file write. It was
deleted ([ADR-0017](adr/0017-daemon-bumps-the-sentinel.md)) because the process that
commits the event is the process that should write the file — and because wakeability
being an emergent property of six components was why agents kept going deaf.

Caveats:

- The wake wire is still a hook **exit 2** (only a hook can wake an idle session); we
  just trigger it on a file change rather than a timer.
- **Arm before you register the watch.** Claude Code watches a path; the daemon's
  writes are MODIFY events. If the file did not exist when the watch was registered,
  the daemon's first write is a CREATE — a different event the watch may not deliver.
- Remove the sentinel on `SessionEnd`.
- Wake payload is a short reminder (“mail on topic X”), never a body (payload-free).

### Implemented: `mailbox harness`

| Hook | Command | What it does |
|---|---|---|
| `SessionStart` (matcher `""` — all sources) | `mailbox harness session-start` (plain, synchronous) | Reads `session_id` from the hook stdin JSON, **registers the agent inbox** (`agent.<session-id>`, always-on — ADR-0007), **arms the sentinel** (writes the session's current unread topics, creating the file), then prints `{"hookSpecificOutput":{"hookEventName":"SessionStart","watchPaths":["<abs sentinel>"]}}`. Arming before printing is load-bearing (see the caveat above), and it doubles as the level-triggered arm: mail that landed while nothing owned this session is written here, so a starting or resuming agent wakes for it. Matcher `""` (not `startup`) so it **re-fires on resume/clear/compact** (ADR-0013); every step is idempotent. **Fail-open:** a down bridge arms with an empty topic set rather than not arming. Exits 0 — never asyncRewake, never a wake. |
| `Stop` (matcher `""`) | `mailbox harness turn-end` | The turn boundary — the session's per-turn self-healing point, three jobs. (1) **Close the turn** (ADR-0016) so `mailbox doctor` can tell busy from deaf. (2) **Re-register the inbox** (best-effort, fail-open — ADR-0013, restoring ADR-0007's register-on-every-`Stop` invariant). (3) **Re-arm and re-trigger:** if the sentinel has gone missing, write it again (the one deafness a per-turn hook can heal, and the honest remainder of the deleted watcher-respawn duty); then, if the session is sitting on unread mail newer than any it has already been re-triggered for, **re-bump the sentinel** so a `FileChanged` fires against the now-idle session — this rescues mail published while the agent was BUSY, whose wake edge was spent on a mid-turn session (ADR-0012). It does **not** re-print `watchPaths` (a Stop hook cannot emit a SessionStart registration — that is `session-start`'s job). Exits **0 always** — NEVER asyncRewake, so a Stop can never itself wake. |
| `UserPromptSubmit` (matcher `""`) | `mailbox harness turn-start` | Stamps that a turn has OPENED (ADR-0016). Paired with the `Stop` hook's turn-ended stamp, it is what lets `mailbox doctor` report a silent session as **busy** rather than deaf. Prints nothing, always exits 0. |
| `FileChanged` (matcher `.mailbox-wake`) | `mailbox harness wake` (`asyncRewake: true`, `timeout` 30s) | On any change to the sentinel, opens the store **read-only** and checks whether THIS session has genuine unread mail. Exits **2** with `mail on topic X` on stderr iff so; otherwise exits **0** (the anti-loop guard — a `FileChanged` fires on every change, so an unconditional exit 2 would loop the agent). It also stamps a hook-ran ack on every exit path (ADR-0016). Isolation: the store re-check, not the sentinel path, is authoritative — a bump to another session's sentinel (shared-ancestor cwd) exits 0 here. |
| `SessionEnd` | `mailbox harness cleanup` | **Removes the session's sentinel dir** and calls the bridge to drop this session's subscriptions **and** interests (feeds the card-08 refcount — no zombie poller outlives the session). It reaps nothing: there is no per-session process left to reap. |
| install | `mailbox harness install-hooks [--settings <path>]` | Merges the hooks snippet into the Claude Code `settings.json` *atomically*, preserving unrelated settings; an upgrade sweeps the retired ADR-0006 `arm` hooks. **Never touches `crossSessionInbound`.** |
| install (opt-in) | `mailbox harness install-inbound [--settings <path>]` | Sets `crossSessionInbound: "accept"` so a `bypassPermissions` session can RECEIVE a peer-channel wake instead of holding it for approval (ADR-0020). Run only when you have read what it widens; it prints that plainly. Idempotent, and reports what it found before changing it. |

**The sentinel.** `<sentinel-root>/by-agent/<encoded-session>/.mailbox-wake`, where
`<sentinel-root>` defaults to `~/.mailbox` (override with `MAILBOX_SENTINEL_ROOT`;
`~` expanded in-process). The basename is the static `FileChanged` matcher;
per-session isolation is the **absolute** path registered via `watchPaths`. It is
deliberately `.mailbox-wake`, not a bare `wake`, so a file named `wake` in a
recursively-watched cwd cannot trip the hook. The shared basename constant lives in
`mailbox-protocol` so the sentinel writer and the matcher can never drift. The
sentinel holds **topic names only** — payload-free.

**Session identity (settled).** Claude Code passes the hook payload as JSON on stdin,
including `session_id`. The hooks parse that into a branded `SessionId` (shared with
the bridge in `mailbox-protocol`).

**No coordination needed.** There is exactly one writer of a session's sentinel that
matters — the daemon, which is already single-instance behind its own `flock`
(ADR-0004) — plus the session's own hooks, which are serialized by Claude Code. The
ADR-0006 single-waiter lock and the pidfile it guarded were needed only because two
detached watchers sharing one FIFO would steal each other's kick bytes. No FIFO, no
lock, no pidfile.

**Session liveness comes from the process table.** Nothing the mailbox writes can
answer "is this agent still running?" — a session's subscriptions, sentinel and
interests all outlive the process they belong to. `doctor::live_claude_sessions` reads
Claude Code's own argv (`--session-id` / `--resume`) instead, and is the one signal
behind `mailbox agents`, the TTL sweep's interest refresh, and the supervisor's
resume/retry decisions (ADR-0017, superseding ADR-0009).

**Always-on agent inbox (card 16 / ADR-0007).** `session-start` subscribes the
session to `agent.<session-id>` before anything else, so an agent is addressable by
its peers (`mailbox send <session-id>`) with nothing for the agent to do.

**Payload-free wake.** The wake hook's stderr on exit 2 is only `mail on topic X`;
the body stays in the durable log for the agent's later `read`. To keep that wire
clean, the hooks route their `tracing` to `<db-dir>/harness.log`, never stderr.

### Why there is no per-session process (and no re-arm)

The ADR-0006 problem was that a hook process cannot outlive its `timeout`, and a
truly-idle session fires no further `Stop`, so the waiter had to **exit 2 at
`max_block`** to force a re-arm — one model turn per `max_block` of idle, forever.

ADR-0008 removed that by making the wake trigger a **file change** rather than a
timer. Its first implementation put a detached watcher process between the daemon and
that file; ADR-0017 removed the watcher too, because the daemon can write the file
itself, in the request that just committed the event:

- **The wake is synchronous with the publish.** `mailbox publish` does not return
  until every subscriber's sentinel is written. The missed-kick race that ADR-0008's
  open→check→block ordering existed to close cannot occur — there is no third process
  to race.
- **The daemon writes each subscriber's WHOLE unread topic set**, not just the topic
  being published, so its content agrees with what the `Stop`-hook re-trigger writes.
  Otherwise mail the agent was already sitting on would appear to vanish from the file
  on the next unrelated publish.
- **A publish only touches its own subscribers' sentinels.** A session that is not
  subscribed to the topic is not written to at all.

**Removed primitives.** `mailbox wait`, `mailbox harness arm`, `mailbox harness watch`
(the detached watcher), the per-session FIFO, the single-waiter `flock`, the waiter
pidfile and the whole `waiters/` directory are gone. On a machine upgraded from an
older build, clean up the leftovers once with `pkill -f 'mailbox harness watch'` and
`rm -rf ~/.agent-mailbox/waiters`.

**The wake edge only reaches an IDLE session, so the turn boundary re-arms it
level-triggered (ADR-0012).** `FileChanged` → exit 2 does nothing for a session that is
mid-turn: the hook does not even run, the bump is spent, and under the pure-edge design
nothing ever bumped again — so mail published while the agent was busy was never
delivered, and the agent went idle deaf on top of it (observed in the wild, 2026-07-21).
`turn-end` therefore also checks, at every turn boundary, whether the session has
unread mail NEWER than the last it re-triggered for, and re-bumps the sentinel if so.
The re-trigger is recorded by an `event_row_id` watermark in
`<sentinel-dir>/.mailbox-retriggered`, so each message buys **at most one**
turn-boundary wake — an agent that wakes and does not read is nudged once, not every
turn forever. The hook still never wakes the session itself, and the sentinel is still
never authority: the wake hook's store re-check decides, as always.

**The `Stop` hook (`turn-end`) is the recovery mechanism.** It fires per turn and
**never wakes** (always exits 0): it re-registers the inbox (best-effort — ADR-0013),
re-arms the sentinel if it has gone missing, and re-triggers for busy-window mail. That
re-arm is all that is left of the old watcher-respawn duty, and it is enough, because
the sentinel is now the only per-session artefact the wake path has.
**Supervision split:** the OS user-service (launchd/systemd) supervises the daemon
(`mailbox serve`); nothing needs supervising per session. The cases this cannot cover —
documented, not fixed — are a session that goes idle **forever** (fires no `Stop`) whose
sentinel is then deleted, and mail that arrives while the daemon is down (durable, but
nothing bumps for it until that session's next turn boundary or `SessionStart`).

**Resuming a session (ADR-0013).** A resume (Claude Code `--resume`/`--continue`, or an
app relaunching the CLI) is a **fresh process** that has lost its predecessor's inbox
registration, its `watchPaths` (per-process, not persisted across a resume). Claude Code fires `SessionStart` again on resume, but with `source: "resume"` —
which the old `startup` matcher excluded, leaving a resumed orchestrator session both
unaddressable (peers' `send` failed) and unwakeable (no watchPaths in the new process),
mail discoverable only by polling `mailbox status`. The fix is two-part: (1) the
`SessionStart` hook matcher is `""` (all sources), so `session-start` re-fires on resume
and re-establishes inbox + sentinel + watchPaths — the only hook that CAN re-print
watchPaths; (2) `turn-end` re-registers the inbox every `Stop`, so a resume that
raced the 10s ADR-0007 tombstone self-heals on the next turn once the guard lapses.
**After upgrading the binary, re-run `mailbox harness install-hooks`** to rewrite the
matcher from `startup` to `""`.

**Unconditional write per publish (no coalescing).** The daemon writes the sentinel on
**every** publish to a subscribed topic, unconditionally: it reads the subscriber's
current unread topic set and writes it (empty or not), always advancing the mtime. It
does not compare sets and tracks no read-progress signal. This makes a lost wake
structurally impossible — every real message writes the sentinel, so `FileChanged` always
fires and the wake hook exits 2 iff there is genuine unread. The only cost is that a burst
of N messages can fire up to N `FileChanged` events; the wake hook's anti-loop (exit 0
once the agent is caught up) bounds actual model wakes to ~1–2 per burst. This
deliberately replaced an earlier coalescing design that produced three silent-deafness
bugs — correctness over the optimization (see ADR-0008).

### install-hooks

`mailbox harness install-hooks` merges the snippet below into the Claude Code settings
file — `--settings <file>` if given (created if missing), else `~/.claude/settings.json`
when it exists — idempotently, preserving unrelated settings; with no such file it
only emits the snippet and says why. An upgrade over the old ADR-0006 `arm` hooks
sweeps them and installs these in their place.

```json
{
  "hooks": {
    "SessionStart": [{ "matcher": "", "hooks": [{ "type": "command",
      "command": "/abs/path/to/mailbox harness session-start" }] }],
    "Stop": [{ "matcher": "", "hooks": [{ "type": "command",
      "command": "/abs/path/to/mailbox harness turn-end" }] }],
    "UserPromptSubmit": [{ "matcher": "", "hooks": [{ "type": "command",
      "command": "/abs/path/to/mailbox harness turn-start" }] }],
    "FileChanged": [{ "matcher": ".mailbox-wake", "hooks": [{ "type": "command",
      "command": "/abs/path/to/mailbox harness wake",
      "asyncRewake": true, "timeout": 30 }] }],
    "SessionEnd": [{ "matcher": "", "hooks": [{ "type": "command",
      "command": "/abs/path/to/mailbox harness cleanup" }] }]
  }
}
```

> **After upgrading the `mailbox` binary, re-run `mailbox harness install-hooks`.** An
> upgrade that skips it leaves hooks pointing at subcommands the new binary no longer
> has (`harness arm`, `harness ensure-watcher`, `harness watch`). An unrecognised
> subcommand exits **2**, and on a `Stop` hook exit 2 blocks the turn from ending and
> surfaces stderr to the model — so the failure is loud and per-turn, not silent.
> Re-running sweeps every name we have ever installed and writes the current set.

### Diagnosing a wake ("why didn't my agent wake?")

**First, run `mailbox doctor`.** It PROVES wakeability by bumping the sentinel and
requiring the hook to answer, which is the only thing that can tell `wakeable` from
`deaf` from `busy` from `gone`. The logs below explain *why*; they cannot substitute
for the probe.

The hooks log at **INFO** (the default) to `<db-dir>/harness.log`, and the daemon logs
its per-publish wake counts at INFO on its own stderr:

| Line | Means |
|---|---|
| `armed the wake sentinel` | `session-start` created/refreshed the watched file, with the topics it wrote |
| `woke subscribers after publish` (`peer` / `sentinel` / `fell_back` / `failed`) | how each subscriber was reached. **`peer` is the socket channel, `sentinel` the fallback** — if you expected the socket and see `sentinel`, that session bound no socket (the gate), not that anything broke |
| `a subscriber's inbox socket refused the wake; fell back to its sentinel` | the session exited between the registry read and the write; the fallback carried it |
| `could not wake a subscriber on either channel` | that subscriber will not wake for THIS event (the event is still durable) |
| `FileChanged wake: genuine unread mail; exiting 2 to wake the session` | the wake hook fired a real wake |
| `FileChanged wake: nothing unread (stray sentinel change); exiting 0 (no wake)` | the anti-loop guard held |
| `turn boundary: the wake sentinel is missing … re-arming it` | the session had become unwakeable; the Stop hook healed it |
| `turn boundary: unread mail arrived while busy; re-bumped the wake sentinel` | the ADR-0012 re-trigger rescued mail whose wake edge was spent mid-turn |
| `turn boundary: this mail was already re-triggered; not nudging again (anti-loop)` | **benign** — the watermark bound one nudge per message |
| `turn boundary: session is caught up; nothing to re-trigger` | the ordinary quiet turn (no sentinel write) |

## Codex CLI: no equivalent yet

Codex has lifecycle hooks (`SessionStart`, `Stop`, …) but **not** an
`asyncRewake`-style idle wake, nor a `FileChanged` trigger:

| Capability | Claude Code | Codex CLI |
|---|---|---|
| Lifecycle hooks | Yes | [Yes](https://developers.openai.com/codex/hooks) |
| `async: true` background hooks | Yes | Parsed, then **skipped** |
| `asyncRewake` (bg exit → wake idle) | Yes | **No** |
| `FileChanged` (external file change → hook) | Yes | **No** |
| Per-session inbox socket (external process → idle wake) | Yes, 2.1.224+ — but **feature-gated**, and the gate cannot be turned on | **No** |
| Native external-event → idle wake | Yes, via the inbox socket (else hooks) | [Requested](https://github.com/openai/codex/issues/20312), not shipped |

Codex `Stop` can continue a turn (block stop) at turn boundaries; it does not wake a
truly idle session. For agent-mailbox, Claude Code is the first harness; Codex needs a
fallback (manual arm) until a wake primitive lands. Related:
[monitor tool FR](https://github.com/openai/codex/issues/29922),
[bg tasks don’t wake parent](https://github.com/openai/codex/issues/15723).

## Sequence: on-demand wake

```mermaid
sequenceDiagram
    autonumber
    participant Agent as Agent (model)
    participant Harness as Claude Code harness
    participant Hooks as Hooks (SessionStart / FileChanged)
    participant Sentinel as Sentinel file
    participant Bridge as Mailbox bridge
    participant Adapter as Adapter (e.g. gh-watch)

    Note over Agent,Adapter: Subscribe once — agent never re-arms

    Agent->>Bridge: subscribe(session, topics)
    Agent->>Harness: end turn / go idle
    Harness->>Hooks: SessionStart
    Hooks->>Sentinel: arm (write current unread; create the file)
    Hooks-->>Harness: print watchPaths(<abs sentinel>)
    Note over Sentinel: Watched by Claude Code; nothing else is running

    Adapter->>Bridge: publish(topic, event)
    Bridge->>Bridge: append durable log; advance offset
    Bridge->>Bridge: look up the subscriber in ~/.claude/sessions

    alt the subscriber has an inbox socket (ADR-0020)
        Bridge->>Harness: write {"type":"user", …"mail on topic X"} to its socket
        Harness->>Agent: start a turn (the message IS the wake)
    else no socket — the fallback channel
        Bridge->>Sentinel: write the subscriber's unread topic names (bump mtime)
        Sentinel-->>Harness: FileChanged fires (idle session)
        Harness->>Hooks: FileChanged → wake
        Hooks->>Bridge: read-only unread check
        alt genuine unread
            Hooks-->>Harness: exit 2, stderr "mail on topic X"
            Harness->>Agent: wake idle session (system reminder)
        else nothing unread (stray change)
            Hooks-->>Harness: exit 0 (no wake) — anti-loop
        end
    end

    Agent->>Bridge: read(my cursors)
    Bridge-->>Agent: unread events
    Agent->>Agent: react (tools, edits, replies)
    Note over Sentinel: Nothing to re-arm on either channel
```

## Contrast with `agent-ipc`

The earlier `agent-ipc` skill used a durable NDJSON inbox + FIFO kick +
`ipc-arm.sh` run by the agent in background mode. **One arm = one wake**; after
reading, the agent had to re-arm. Adapters (e.g. `agent-ipc-github`) were already
just senders — that split stays. What’s new is moving the arm loop into harness
hooks, adding multi-subscriber topics with per-subscriber cursors, and (ADR-0008)
making the wake on-demand so a long idle costs nothing.
