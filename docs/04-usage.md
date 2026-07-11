# Usage: install → hooks → the four-verb loop

This is the get-it-running guide for **agent-mailbox**: how to build and install
it, wire the Claude Code hooks, run the agent-facing loop, and check state as a
human. It assumes nothing beyond a stable Rust toolchain and (for GitHub
watching) the `gh` CLI.

New to the design? Read [01-wake-and-rearm](01-wake-and-rearm.md) for *why* the
loop is shaped this way. This doc is the *how*.

---

## 1. Install

### Build the binaries

From the repo root:

```bash
cargo build --release
```

That produces three binaries in `target/release/`:

| Binary | Role |
|---|---|
| `mailbox` | the bridge CLI + daemon (the one entry point) |
| `mailbox-stub-adapter` | reference adapter (synthetic edges; for the demo/tests) |
| `mailbox-github-pr-adapter` | the real GitHub PR poller |

### Put them on your PATH (co-located)

Install the three binaries **into the same directory** — e.g. `~/.local/bin` or
`/usr/local/bin`:

```bash
install -m755 target/release/mailbox \
              target/release/mailbox-stub-adapter \
              target/release/mailbox-github-pr-adapter \
              ~/.local/bin/
```

**Co-location matters.** When the daemon needs to spawn an adapter it resolves
the binary in this order (see `crates/mailbox/src/resolver.rs`):

1. an absolute-path env override —
   `MAILBOX_STUB_ADAPTER_BIN` / `MAILBOX_GH_ADAPTER_BIN` (mostly for tests/dev);
2. the adapter **beside the running `mailbox` binary** (the normal install);
3. the bare binary name on `PATH`.

So if `mailbox` and the two adapters live in the same directory, watches Just
Work with no extra configuration. The github-pr adapter also shells out to `gh`,
overridable via `MAILBOX_GH_BIN` (tests point it at a fake).

### Where state lives

The daemon keeps everything under one directory (default
`~/.agent-mailbox/`, override with `AGENT_MAILBOX_DB` for the full DB path or
`AGENT_MAILBOX_HOME` for the parent):

| File | What |
|---|---|
| `mailbox.db` | the durable SQLite topic log, subscriptions, cursors, watches |
| `mailbox.sock` | the user-scoped Unix socket clients connect to (`0600`) |
| `mailbox.lock` | the daemon's exclusive `flock` (single-writer guard) |
| `waiters/` | per-session wake FIFOs + pidfiles |
| `harness.log` | `wait` / `harness arm` logs (kept off stderr so wakes stay clean) |

### Start the daemon

Every command except `wait` is a client of the `mailbox serve` daemon. Start it
once (a login item, a `tmux` pane, or a user service):

```bash
mailbox serve
```

If the daemon is **down**, clients fail loudly — non-zero with
`bridge not running; start it with 'mailbox serve'` — rather than silently
opening the DB or spawning a second writer. A second `serve` on the same DB also
fails loudly (the lock is held for the daemon's whole life).

---

## 2. Wire the Claude Code hooks

The hooks are what make wake **infrastructure**, not something the agent does.
`mailbox harness install-hooks` prints the `settings.json` snippet and — with
`--settings <path>` — merges it into a file (atomically, preserving unrelated
settings, idempotent):

```bash
# Print the snippet (review it, paste it by hand):
mailbox harness install-hooks --mailbox-bin ~/.local/bin/mailbox

# ...or merge it straight into your Claude Code user settings:
mailbox harness install-hooks \
  --mailbox-bin ~/.local/bin/mailbox \
  --settings ~/.claude/settings.json
```

Pass an **absolute** `--mailbox-bin` so the hook works regardless of the
session's `PATH` (it defaults to the resolved path of the `mailbox` you ran).
The snippet wires three hooks:

```json
{
  "hooks": {
    "SessionStart": [{ "matcher": "startup", "hooks": [{ "type": "command",
      "command": "/abs/path/mailbox harness arm --max-block-ms 540000",
      "asyncRewake": true, "timeout": 600 }] }],
    "Stop": [{ "matcher": "", "hooks": [{ "type": "command",
      "command": "/abs/path/mailbox harness arm --max-block-ms 540000",
      "asyncRewake": true, "timeout": 600 }] }],
    "SessionEnd": [{ "matcher": "", "hooks": [{ "type": "command",
      "command": "/abs/path/mailbox harness cleanup" }] }]
  }
}
```

What each hook does:

- **`SessionStart` / `Stop` → `mailbox harness arm`** (`asyncRewake: true`).
  Reads the `session_id` from the hook's stdin JSON, asks the bridge whether the
  session has any subscriptions, and — **iff subscribed** — `exec`s the waiter.
  Not subscribed, or the bridge is down/erroring → exit 0, **no wake**
  (fail-safe). The armed waiter self-respawns before Claude Code's `timeout`
  would kill it, so a long idle stays armed. This is the only reason the agent
  never re-arms.
- **`SessionEnd` → `mailbox harness cleanup`.** Reaps the waiter and drops this
  session's subscriptions **and** watch interests, stopping any adapter whose
  last interested session it was (no zombie poller outlives the session).

The `--max-block-ms` (waiter self-respawn bound) is kept safely below the
async-hook `timeout`; both are install-time knobs (`--max-block-ms`,
`--timeout-secs`). See [01-wake-and-rearm](01-wake-and-rearm.md) § "Timeout
survival" for the reasoning.

---

## 3. The four-verb agent loop

This is the whole agent-facing contract. An agent does exactly four things:

```text
subscribe  ──►  (idle; hooks keep the waiter armed)  ──►  read  ──►  react
    ▲                                                                  │
    └──────────────────────  unsubscribe when done  ◄──────────────────┘
```

1. **subscribe** (once) — `mailbox subscribe <topic> --session <id>`, or
   `mailbox watch github-pr <owner>/<repo>#<n> --session <id>` (which subscribes
   *and* starts the shared poller). Baseline-on-subscribe: you only ever see
   events published *after* you subscribe.
2. **idle** — do other work, or nothing. The `SessionStart`/`Stop` hooks keep a
   waiter armed. **The agent does not arm anything and does not poll.**
3. **read** — on wake (a system reminder like `mail on topic X`), run
   `mailbox read --session <id>`. It returns unread events and advances your
   cursor (exactly-once, advance-on-read).
4. **react** — do the work: resolve the conflict, address the review, fix CI.
5. **unsubscribe** — `mailbox unsubscribe <topic> --session <id>` (or
   `mailbox unwatch …`) when you no longer care. `SessionEnd` does this for you
   if you just end the session.

**The two rules that make this different from the old skill loop:** the agent
**never re-arms** after a wake, and the agent **never spawns a background
poller**. Both are owned by infrastructure — the hooks arm; the bridge
supervises adapters. If you find yourself writing a `while true; gh … ; sleep`
loop, stop: declare a `watch` instead.

### The commands, precisely

Run `mailbox <cmd> --help` for the authoritative flags. The session-scoped ones
take `--session <id>` (or the `MAILBOX_SESSION_ID` env fallback; the flag wins).
Add global `--json` for machine-readable stdout.

| Command | What it does |
|---|---|
| `mailbox subscribe <topic> --session <id>` | Subscribe (baseline-on-subscribe). |
| `mailbox unsubscribe <topic> --session <id>` | Unsubscribe. |
| `mailbox read --session <id> [--limit <n>]` | Return unread events, advance the cursor. |
| `mailbox watch github-pr <owner>/<repo>#<n> [--interval <secs>] --session <id>` | Watch a PR: record interest, subscribe to the PR topic, and (via the daemon) spawn the shared edge-triggered `github-pr` poller. Default interval 60s. |
| `mailbox unwatch github-pr <owner>/<repo>#<n> --session <id>` | Drop this session's interest + unsubscribe; the poller stops only when the last interested session leaves. |
| `mailbox watch stub <label> [--interval-ms <n>] [--count <n>] --session <id>` | Watch the reference stub publisher (synthetic edges; for the demo/tests). |
| `mailbox unwatch stub <label> --session <id>` | Drop interest in the stub watch. |
| `mailbox publish <topic> [--body <json>] [--adapter <id>]` | Publish an event (normally an adapter's job; handy for testing). |

---

## 4. `mailbox status` for humans

`status` is the human-facing window into a session. It never consumes events (it
counts, it does not `read`):

```bash
mailbox status --session my-session
```

```text
session: my-session
watches:
  github-pr myrepo#42  state=running interest=1 interval=60s child=pid 51234
subscriptions:
  github.pr.me/myrepo#42
unread:
  [github.pr.me/myrepo#42] 1
```

- **watches** — each supervised watch, its `state`
  (`desired`/`running`/`stopped`/`failed`), how many sessions are `interest`ed,
  the poll `interval`, and the adapter's `child` pid when the supervisor is
  running one. `running` with a pid means the poller is live.
- **subscriptions** — the topics this session listens on.
- **unread** — per-topic count of events past this session's cursor. `read`
  drains these.

---

## 5. Try it now (no network)

`scripts/demo.sh` runs the whole loop — subscribe/read, an idle wake, a
supervised adapter, and teardown — in a throwaway tempdir with no GitHub and no
Claude Code. See [demo.md](demo.md) for the walkthrough and captured output:

```bash
scripts/demo.sh
```

Coming from the old `agent-ipc` / `agent-ipc-github` skills? See
[migration-from-agent-ipc.md](migration-from-agent-ipc.md).
