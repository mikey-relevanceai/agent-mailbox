# Wake (on-demand, ADR-0008)

How an idle agent session gets woken when the world changes — and how listening
stays armed for the whole session without the agent doing anything, and **without a
periodic re-arm** (the ADR-0006 re-arm is [superseded](adr/0008-on-demand-wake-filechanged.md)).

## Layers

```text
Adapters (detect world changes)
        ↓ publish
Bridge (durable events + subscriptions + wake kicks)
        ↓ watcher bumps a sentinel → FileChanged hook wakes
Agent sessions (react, never poll)
```

Adapters only `publish`. The bridge owns durable logs, topics, and per-subscriber
cursors. Harness integrators own the arm/wake loop.

## Claude Code: a detached watcher + a `FileChanged` wake

Claude Code can run a background hook with `asyncRewake: true`. When that process
exits with code **2**, the harness wakes an idle session and surfaces stderr as a
system reminder. Separately, a **`FileChanged`** hook fires — even on a truly-idle
session — when an external process changes a watched file, matched by **basename**.
ADR-0008 combines these so a wake happens **on demand** (when real mail arrives),
never on a timer:

1. Agent **subscribes** to topics once.
2. `SessionStart` runs `session-start`: it registers the inbox, prints a `watchPaths`
   registration for this session's **sentinel** file, and spawns a **detached
   watcher** that outlives the hook.
3. An adapter publishes → the bridge appends + kicks the watcher → the watcher writes
   the unread topic name(s) into the sentinel (bumping its mtime).
4. The sentinel change fires the `FileChanged` hook (`wake`), which exits **2** iff
   there is genuinely unread mail → the idle session wakes.
5. Agent **reads** unread events and reacts; ending the turn does nothing special —
   the watcher is still blocked and still armed for the next message.

The agent never runs an arm command, and **there is no periodic re-arm wake** — an
idle subscribed session costs zero model turns until real mail arrives.

Caveats:

- The wake wire is still a hook **exit 2** (only a hook can wake an idle session); we
  just trigger it on a file change rather than a timer.
- Tear down the watcher and remove the sentinel on `SessionEnd`.
- Wake payload is a short reminder (“mail on topic X”), never a body (payload-free).

### Implemented: `mailbox harness`

| Hook | Command | What it does |
|---|---|---|
| `SessionStart` (matcher `""` — all sources) | `mailbox harness session-start` (plain, synchronous) | Reads `session_id` from the hook stdin JSON, **registers the agent inbox** (`agent.<session-id>`, always-on — ADR-0007), prints `{"hookSpecificOutput":{"hookEventName":"SessionStart","watchPaths":["<abs sentinel>"]}}`, and spawns the detached watcher. Matcher `""` (not `startup`) so it **re-fires on resume/clear/compact** — a resumed process re-establishes its inbox, watchPaths, and watcher (ADR-0013); every step is idempotent. **Fail-open:** a down bridge does not stop it printing watchPaths or spawning the watcher (which self-validates). Exits 0 — never asyncRewake, never a wake. |
| `Stop` (matcher `""`) | `mailbox harness ensure-watcher` | The turn-boundary net — the session's per-turn self-healing point, three jobs, **in this order**. (1) **Re-register the inbox** (best-effort, fail-open — ADR-0013, restoring ADR-0007's register-on-every-`Stop` invariant). This runs FIRST on purpose: a watcher spawned while the inbox is unregistered self-exits `Unsubscribed`, so registering after the spawn would cost a turn. (2) **Stop-liveness** (ADR-0008 FIX 3): **respawn the detached watcher iff it is missing/dead** (a live watcher is left alone — the single-instance lock makes a redundant spawn a clean no-op). (3) **The level-triggered re-trigger** (ADR-0012): if the session is sitting on unread mail newer than any it has already been re-triggered for, **re-bump the sentinel** so a `FileChanged` fires against the now-idle session — this rescues mail published while the agent was BUSY, whose wake edge was spent on a mid-turn session. It does **not** re-print `watchPaths` (a Stop hook cannot emit a SessionStart registration — that is `session-start`'s job). Exits **0 always** — NEVER asyncRewake, so a Stop can never itself wake. Costs a per-turn process spawn + one fail-open socket call but **no model turn**. |
| `FileChanged` (matcher `.mailbox-wake`) | `mailbox harness wake` (`asyncRewake: true`, `timeout` 1h) | On any change to the sentinel, opens the store **read-only** and checks whether THIS session has genuine unread mail. Exits **2** with `mail on topic X` on stderr iff so; otherwise exits **0** (the anti-loop guard — a `FileChanged` fires on every change, so an unconditional exit 2 would loop the agent). Isolation: the store re-check, not the sentinel path, is authoritative — a bump to another session's sentinel (shared-ancestor cwd) exits 0 here. |
| `SessionEnd` | `mailbox harness cleanup` | Reaps the watcher (`SIGTERM` the pidfile PID), **removes the session's sentinel dir**, and calls the bridge to drop this session's subscriptions **and** interests (feeds the card-08 refcount — no zombie poller outlives the session). |
| install | `mailbox harness install-hooks [--settings <path>]` | Merges the hooks snippet into the Claude Code `settings.json` *atomically*, preserving unrelated settings; an upgrade sweeps the retired ADR-0006 `arm` hooks. |

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

**Coordination: the lock is the source of truth (reused from ADR-0006).** Exactly one
watcher may be live per session, guarded by an advisory lock. The **watcher owns the
pidfile**, written only *after* it takes the lock; a watcher that loses the lock
exits 0 without touching it. So the pidfile always names the one live watcher, and
`cleanup` reaps that stable PID. The pidfile, FIFO, and lock share one filename stem
via `SessionId::encode_filename`.

**Arm-iff-subscribed.** The watcher re-checks `has_subscription` after taking the
lock and self-exits cleanly (exit 0, pidfile removed) if there is none — catching a
`SessionStart` whose `SessionEnd`/unsubscribe then landed, so no orphan survives.

**Always-on agent inbox (card 16 / ADR-0007).** `session-start` subscribes the
session to `agent.<session-id>` before anything else, so an agent is addressable by
its peers (`mailbox send <session-id>`) with nothing for the agent to do — which also
means a live session always has ≥1 subscription, so the watcher never self-exits on a
live session.

**Payload-free wake.** The wake hook's stderr on exit 2 is only `mail on topic X`;
the body stays in the durable log for the agent's later `read`. To keep that wire
clean, `wake` (and the detached `watch`, and the retained `wait`/`arm`) route their
`tracing` to `<db-dir>/harness.log`, never stderr.

### The detached watcher (why it outlives the hook, and why there is no re-arm)

The ADR-0006 problem was that a hook process cannot outlive its `timeout`, and a
truly-idle session fires no further `Stop`, so the waiter had to **exit 2 at
`max_block`** to force a re-arm — one model turn per `max_block` of idle, forever.

ADR-0008 removes that entirely by making the wake trigger a **detached process**, not
a hook child:

- `session-start` spawns `mailbox harness watch`, which calls `setsid` on startup to
  leave the hook's process group. The hook's exit — or a `killpg` on its group —
  cannot take it down, and no hook `timeout` applies. (`setsid`-on-startup is a safe
  `nix` call; the workspace denies `unsafe`, so no `fork`/`pre_exec` double-fork. The
  watcher never opens a terminal, so `setsid` alone is sufficient.)
- The watcher **blocks on the mail FIFO forever** (the card-05 kick), with **no
  `max_block` and no exit-2**. On a real-mail kick it writes the unread topics into
  the sentinel; a kick with nothing unread touches nothing. It runs until `SIGTERM`'d
  at `SessionEnd`.
- It survives a daemon restart the same way the ADR-0006 waiter did (the FIFO is
  re-openable; the kick reaches the already-blocked watcher).

**Removed primitives.** `mailbox wait` and `mailbox harness arm`, and the max-block
timing knobs behind them, have been deleted. Nothing had invoked them since ADR-0008
replaced the re-arm loop with the detached watcher; they survived only as a
"retained primitive" exercised by their own tests.

**The wake edge only reaches an IDLE session, so the turn boundary re-arms it
level-triggered (ADR-0012).** `FileChanged` → exit 2 does nothing for a session that is
mid-turn: the hook does not even run, the bump is spent, and under the pure-edge design
nothing ever bumped again — so mail published while the agent was busy was never
delivered, and the agent went idle deaf on top of it (observed in the wild, 2026-07-21).
`ensure-watcher` therefore also checks, at every turn boundary, whether the session has
unread mail NEWER than the last it re-triggered for, and re-bumps the sentinel if so.
The re-trigger is recorded by an `event_row_id` watermark in
`<sentinel-dir>/.mailbox-retriggered`, so each message buys **at most one**
turn-boundary wake — an agent that wakes and does not read is nudged once, not every
turn forever. The hook still never wakes the session itself, and the sentinel is still
never authority: the wake hook's store re-check decides, as always.

**The `Stop`-liveness hook (`ensure-watcher`) is the recovery mechanism.** It fires per
turn and **never wakes** (always exits 0): it respawns the detached watcher iff it is
missing/dead (a live one is left alone via the single-instance lock) and re-registers the
inbox (best-effort — ADR-0013). This is what makes the simple watcher (no elaborate in-loop self-healing)
safe: a watcher killed by a crash, an OS/OOM kill, or an unrecoverable FIFO error is
respawned on the session's next turn. **Supervision split:** the OS user-service
(launchd/systemd) supervises the daemon (`mailbox serve`); the Stop hook supervises the
per-session watcher. The one case it cannot cover — documented, not fixed — is a session
that goes idle **forever** (fires no `Stop`) whose watcher then dies: nothing can wake a
session that will neither take a turn nor be poked.

**Resuming a session (ADR-0013).** A resume (Claude Code `--resume`/`--continue`, or an
app relaunching the CLI) is a **fresh process** that has lost its predecessor's inbox
registration, its `watchPaths` (per-process, not persisted across a resume), and its
watcher. Claude Code fires `SessionStart` again on resume, but with `source: "resume"` —
which the old `startup` matcher excluded, leaving a resumed orchestrator session both
unaddressable (peers' `send` failed) and unwakeable (no watchPaths in the new process),
mail discoverable only by polling `mailbox status`. The fix is two-part: (1) the
`SessionStart` hook matcher is `""` (all sources), so `session-start` re-fires on resume
and re-establishes inbox + watchPaths + watcher — the only hook that CAN re-print
watchPaths; (2) `ensure-watcher` re-registers the inbox every `Stop`, so a resume that
raced the 10s ADR-0007 tombstone self-heals on the next turn once the guard lapses.
**After upgrading the binary, re-run `mailbox harness install-hooks`** to rewrite the
matcher from `startup` to `""`.

**Unconditional bump per kick (no coalescing).** The watcher writes the sentinel on
**every** kick, unconditionally: it reads the current unread topic set and writes it
(empty or not), always advancing the mtime. It does not compare sets and tracks no
read-progress signal. This makes a lost wake structurally impossible — every real message
kicks a live watcher, every kick writes the sentinel, so `FileChanged` always fires and
the wake hook exits 2 iff there is genuine unread. The only cost is that a burst of N
messages can fire up to N `FileChanged` events; the wake hook's anti-loop (exit 0 once the
agent is caught up) bounds actual model wakes to ~1–2 per burst. This deliberately replaced
an earlier coalescing design that produced three silent-deafness bugs — correctness over
the optimization (see ADR-0008).

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
      "command": "/abs/path/to/mailbox harness ensure-watcher" }] }],
    "FileChanged": [{ "matcher": ".mailbox-wake", "hooks": [{ "type": "command",
      "command": "/abs/path/to/mailbox harness wake",
      "asyncRewake": true, "timeout": 3600 }] }],
    "SessionEnd": [{ "matcher": "", "hooks": [{ "type": "command",
      "command": "/abs/path/to/mailbox harness cleanup" }] }]
  }
}
```

> **After upgrading the `mailbox` binary, re-run `mailbox harness install-hooks`.** An
> upgrade that skips it leaves stale ADR-0006 `arm` hooks that still fire exit-2 re-arm
> wakes and contend for the single-waiter lock; re-running sweeps them.

### Diagnosing a wake ("why didn't my agent wake?")

The watcher lifecycle is logged at **INFO** (the default) to `<db-dir>/harness.log`,
and the daemon logs its kick counts at INFO on its own stderr:

| Line | Means |
|---|---|
| `watcher armed; blocking on the mail FIFO (no re-arm, no timer)` | the watcher is up and blocked |
| `watcher found no subscriptions; exiting without arming a sentinel` | arm-iff-subscribed said no |
| `watcher wrote the wake sentinel (FileChanged will fire; wake hook wakes iff unread)` | the watcher wrote the sentinel on this kick, with the topics |
| `FileChanged wake: genuine unread mail; exiting 2 to wake the session` | the wake hook fired a real wake |
| `FileChanged wake: nothing unread (stray sentinel change); exiting 0 (no wake)` | the anti-loop guard held |
| `kicked subscribed sessions after publish` (`delivered` / `no_reader`) | whether the publish reached a live watcher |
| `another watcher already holds this session's lock; exiting cleanly (single-instance)` | **benign** — a racing spawn lost the lock |
| `ensure-watcher: a live watcher already holds the lock; leaving it (no-op)` | the Stop hook found the watcher healthy |
| `ensure-watcher: no live watcher; respawning the detached watcher` | the Stop hook recovered a died watcher |
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
| Native external-event → idle wake | Via hooks / Monitor | [Requested](https://github.com/openai/codex/issues/20312), not shipped |

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
    participant Watcher as Detached watcher
    participant Sentinel as Sentinel file
    participant Bridge as Mailbox bridge
    participant Adapter as Adapter (e.g. gh-watch)

    Note over Agent,Adapter: Subscribe once — agent never re-arms

    Agent->>Bridge: subscribe(session, topics)
    Agent->>Harness: end turn / go idle
    Harness->>Hooks: SessionStart
    Hooks->>Watcher: spawn detached watcher (setsid; outlives the hook)
    Hooks-->>Harness: print watchPaths(<abs sentinel>)
    Note over Watcher: Blocks on the bridge kick (FIFO), forever — no timer

    Adapter->>Bridge: publish(topic, event)
    Bridge->>Bridge: append durable log; advance offset
    Bridge->>Watcher: kick (signal only)
    Watcher->>Sentinel: write topic names (bump mtime)
    Sentinel-->>Harness: FileChanged fires (idle session)
    Harness->>Hooks: FileChanged → wake
    Hooks->>Bridge: read-only unread check
    alt genuine unread
        Hooks-->>Harness: exit 2, stderr "mail on topic X"
        Harness->>Agent: wake idle session (system reminder)
        Agent->>Bridge: read(my cursors)
        Bridge-->>Agent: unread events
        Agent->>Agent: react (tools, edits, replies)
    else nothing unread (stray change)
        Hooks-->>Harness: exit 0 (no wake) — anti-loop
    end
    Note over Watcher: Still blocked and armed — no re-arm needed
```

## Contrast with `agent-ipc`

The earlier `agent-ipc` skill used a durable NDJSON inbox + FIFO kick +
`ipc-arm.sh` run by the agent in background mode. **One arm = one wake**; after
reading, the agent had to re-arm. Adapters (e.g. `agent-ipc-github`) were already
just senders — that split stays. What’s new is moving the arm loop into harness
hooks, adding multi-subscriber topics with per-subscriber cursors, and (ADR-0008)
making the wake on-demand so a long idle costs nothing.
