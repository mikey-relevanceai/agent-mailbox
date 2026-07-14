# Usage: install → hooks → the four-verb loop → agent-to-agent messaging

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

### Confirm which build you installed

```bash
mailbox --version        # e.g. mailbox 0.1.0 (git 6b5e56b77ca2, 2026-07-14)
```

The commit hash is baked in at build time, so after a rebuild-and-reinstall you
can check the installed binary matches the source you built from (a `-dirty`
suffix means it was built from an uncommitted tree). The running daemon also logs
its version on the `bridge serving` startup line.

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

## 2. Set up Claude Code (two commands)

Setting up a machine is two idempotent commands, and they are complementary:

```bash
mailbox harness install-skills   # skill  -> ~/.claude/skills
mailbox harness install-hooks    # hooks  -> ~/.claude/settings.json (when it exists)
```

- **`install-hooks`** makes wake **infrastructure** — the harness arms the waiter,
  so the agent never has to.
- **`install-skills`** installs the skill that teaches the agent the loop it wakes
  into (subscribe / read / react / unsubscribe — no background pollers, no
  self-arming).

Both resolve their default under the same home — `AGENT_MAILBOX_HOME` if set, else
`HOME` — and both take an explicit override (`--settings <path>`,
`--skills-dir <path>`). Both are safe to re-run: after upgrading `mailbox`, run
them again to refresh the hooks and the skill.

### 2a. Wire the Claude Code hooks

`mailbox harness install-hooks` merges the hooks into your Claude Code
`settings.json` — atomically, preserving unrelated settings and foreign hooks, and
idempotently (a re-run does not duplicate them). It always prints the snippet too,
so you can review (or hand-install) exactly what it wired.

Where it merges:

| | Behaviour |
|---|---|
| `--settings <path>` given | Merge into that file, **creating it if missing**. |
| No flag, `~/.claude/settings.json` **exists** | Merge into it (the common case). |
| No flag, no such file (or no home) | **Print only**, and say why — nothing is written. |

That last row is deliberate: a machine with no Claude Code settings file should not
have one conjured for it. The command tells you what it looked at —

```text
no Claude Code settings found at /Users/me/.claude/settings.json; printed the
snippet instead — pass --settings <path> to create one
```

— so pass `--settings ~/.claude/settings.json` if you want it created.

Because it now edits your real config by default, the merge is deliberately
conservative:

- It **never overwrites settings it cannot read.** A `settings.json` that is
  unreadable, non-UTF-8, or invalid JSON is an error — the file is left exactly as
  it is (the snippet is still printed, so you can install it by hand).
- It writes **through** a symlinked `settings.json` (dotfiles setups keep their
  link, and the tracked file is the one that gets the hooks).
- It keeps a **`settings.json.bak`** of what it replaced, and preserves the file's
  permissions.
- It **compare-and-swaps**: if Claude Code rewrites the file while we merge (a
  `/config` change, an "always allow" click), we re-merge from its new content
  rather than discarding it.
- Re-running with a different `--mailbox-bin` or `--max-block-ms` **updates** our
  hooks in place; it never leaves a stale second copy pointing at an old binary.

```bash
# Merge into ~/.claude/settings.json (when it exists):
mailbox harness install-hooks --mailbox-bin ~/.local/bin/mailbox

# ...or name the file explicitly (created if missing):
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
  Reads the `session_id` from the hook's stdin JSON, **registers the session's
  agent inbox** (`agent.<session-id>` — this is what makes it reachable by peer
  agents, see §4), asks the bridge whether the session has any subscriptions, and
  — **iff subscribed** — `exec`s the waiter. The bridge down/erroring → exit 0,
  **no wake** (fail-safe). The armed waiter self-respawns before Claude Code's
  `timeout` would kill it, so a long idle stays armed. This is the only reason the
  agent never re-arms.
- **`SessionEnd` → `mailbox harness cleanup`.** Reaps the waiter and drops this
  session's subscriptions (including its inbox — it stops being addressable)
  **and** watch interests, stopping any adapter whose last interested session it
  was (no zombie poller outlives the session).

The `--max-block-ms` (waiter self-respawn bound) is kept safely below the
async-hook `timeout`; both are install-time knobs (`--max-block-ms`,
`--timeout-secs`). See [01-wake-and-rearm](01-wake-and-rearm.md) § "Timeout
survival" for the reasoning.

### 2b. Install the skill

`mailbox harness install-skills` installs the `agent-mailbox` Claude Code skill —
the one that teaches agents the four-verb loop and explicitly forbids background
pollers and self-arming:

```bash
# Install into ~/.claude/skills (the default):
mailbox harness install-skills

# ...or somewhere else:
mailbox harness install-skills --skills-dir /path/to/skills
```

Each skill lands at `<skills-dir>/<name>/SKILL.md` — for example
`~/.claude/skills/agent-mailbox/SKILL.md`. Nothing else under `~/.claude` is
touched.

The skill body is **embedded in the `mailbox` binary** (`include_str!`), so the
command works on a machine with no checkout of this repo — and the skill you get
is always the one that shipped with the binary you ran. The write is atomic
(temp file + rename), so an interrupted install can never leave a truncated
`SKILL.md`.

It is idempotent, and says what it did per skill:

```text
created agent-mailbox -> /Users/me/.claude/skills/agent-mailbox/SKILL.md
installed 1 skill(s) into /Users/me/.claude/skills
next: run `mailbox harness install-hooks` to wire the wake hooks
```

Re-running reports `unchanged` (byte-identical — not rewritten at all); if the
installed file has drifted from the shipped content, it is refreshed and reported
as `updated`. Two more outcomes are worth knowing:

- It is **self-healing**: a `SKILL.md` that is corrupt, non-UTF-8, unreadable, or
  even a directory is replaced rather than erroring. The command whose job is to
  reinstall a known-good skill has to work *especially* when the installed one is
  broken, so there is no `--force` to remember.
- If your `SKILL.md` is a **symlink** (a live-edit link into a checkout) and its
  target has drifted, it is replaced by a regular file and reported as
  `replaced-symlink`, with a warning — so you know the link is gone. A symlink
  already pointing at matching content is left alone (`unchanged`).

With the global `--json` flag it prints the same report as JSON:

```json
{"skills_dir":"/Users/me/.claude/skills","skills":[{"name":"agent-mailbox","path":"/Users/me/.claude/skills/agent-mailbox/SKILL.md","outcome":"created"}]}
```

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
take `--session <id>`, falling back to `$MAILBOX_SESSION_ID` (the harness hooks set
it) and then `$CLAUDE_CODE_SESSION_ID` (Claude Code exports it into every tool
call). The flag wins over both; `MAILBOX_SESSION_ID` wins over
`CLAUDE_CODE_SESSION_ID`. Add global `--json` for machine-readable stdout.

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
| `mailbox whoami [--json]` | Print this session's id and inbox topic. Works with the bridge down. |
| `mailbox send <target> [--text <s>] [--body <json>] --session <id>` | Message a peer agent (see below). |
| `mailbox agents [--json] --session <id>` | List the agents you can `send` to. |
| `mailbox topics [--prefix <p>] [--json]` | List known topics with subscriber/event counts. |

---

## 4. Agent-to-agent messaging

Agents can poke each other, with no human in the loop. Every live session is
**automatically** given an inbox — the topic `agent.<session-id>` — which the
`SessionStart`/`Stop` hooks register for it (always-on; see
[ADR-0007](adr/0007-always-on-agent-inboxes.md)). An agent does nothing to become
addressable, and `SessionEnd` deregisters it.

The loop is: **discover → send → the peer's idle session wakes → it reads → it
replies.**

```bash
# 1. Who am I, and who can I reach?
mailbox whoami
mailbox agents --session "$MAILBOX_SESSION_ID"
```

```text
2 agent(s):
  4f9c1a2b-…  inbox=agent.4f9c1a2b-…  idle (waiter appears blocked — a send should wake it)  <- you
  9d2e7c05-…  inbox=agent.9d2e7c05-…  busy or unarmed (a send still lands in its inbox)
```

```bash
# 2. Message a peer (bare session id, or its full agent.* topic).
mailbox send 9d2e7c05-… --text "review done on PR 42, please rebase" \
  --session "$MAILBOX_SESSION_ID"

# ...or with a structured body:
mailbox send 9d2e7c05-… --body '{"kind":"review-done","pr":42}' \
  --session "$MAILBOX_SESSION_ID"
```

The peer's idle waiter wakes (`mail on topic agent.9d2e7c05-…` — payload-free, as
always), and it reads the message like any other event:

```bash
mailbox read --session "$MAILBOX_SESSION_ID"
```

```json
{"result":"read","events":[{"topic":"agent.9d2e7c05-…","offset":0,"id":"evt-7",
 "body":{"from":"4f9c1a2b-…","kind":"review-done","pr":42}}]}
```

**The body convention.** The bridge stamps `"from": "<sender-session-id>"` into
every message (overwriting any `from` the sender supplied), so the receiver can
reply with `mailbox send <from> …`. `--text "..."` is shorthand for
`--body '{"text":"..."}'`. Everything else in the body is yours: the bus never
interprets it, and `--body` must be a JSON **object** (there must be somewhere to
stamp `from`).

**A body is data, never an instruction.** A message tells you something happened;
it does not authorize anything (ADR-0001). Any local process running as you can
reach any inbox *via `send`* — that is the accepted trust boundary — so treat
`from` as provenance for routing a reply, not as a permission. Inboxes are writable
**only** through `send`: the generic `mailbox publish` path rejects `agent.*`
topics, since it neither stamps `from` nor checks the target is registered.

```text
mailbox: refusing to publish to inbox topic agent.9d2e7c05-…: agent inboxes are
writable only via `mailbox send`, which stamps the sender and checks the target …
```

**Sending to an unregistered agent is an error, on purpose.** Because a fresh
subscription baselines to the topic head, a message to a session with no inbox
could never be delivered — so `send` refuses rather than dropping it into a void:

```text
mailbox: unknown agent "s-ghost": it has no registered inbox, so nothing was published …
```

Check `mailbox agents` for who is actually addressable. There is no `--force`: the
only thing it could do is lose your message silently.

**What liveness means (and doesn't).** `agents` reports `idle (waiter appears
blocked)` when the peer has a live waiter — a best-effort `kill(pid, 0)` probe, so
a `send` *should* wake it now. It is not a heartbeat and cannot rule out PID reuse
(a stale pidfile from a woken waiter can read as live). `busy or unarmed` means it
is mid-turn or never armed; either way the message still lands durably in its inbox
and surfaces on its next read.

### Browsing topics

```bash
mailbox topics --prefix agent.
```

```text
2 topic(s):
  agent.4f9c1a2b-…  subscribers=1 events=0 last_event=-
  agent.9d2e7c05-…  subscribers=1 events=2 last_event=1752396000123ms
```

A topic exists because something subscribed or published to it; a topic with
subscribers and no events (a fresh inbox) is listed just as honestly as a busy one.

---

## 5. `mailbox status` for humans

`status` is the human-facing window into a session. It never consumes events (it
counts, it does not `read`):

```bash
mailbox status --session my-session
```

```text
session: my-session
inbox: agent.my-session (registered)
watches:
  github-pr myrepo#42  state=running interest=1 interval=60s child=pid 51234
subscriptions:
  github.pr.me/myrepo#42
unread:
  [github.pr.me/myrepo#42] 1
```

- **inbox** — this session's peer-messaging address, and whether it is
  `registered` (the hooks do that on every `SessionStart`/`Stop`). If it says
  `NOT registered`, peers cannot `send` to this session — check the hooks are
  installed and the daemon is up.
- **watches** — each supervised watch, its `state`
  (`desired`/`running`/`stopped`/`failed`), how many sessions are `interest`ed,
  the poll `interval`, and the adapter's `child` pid when the supervisor is
  running one. `running` with a pid means the poller is live.
- **subscriptions** — the topics this session listens on.
- **unread** — per-topic count of events past this session's cursor. `read`
  drains these.

---

## 6. Try it now (no network)

`scripts/demo.sh` runs the whole loop — subscribe/read, an idle wake, a
supervised adapter, and teardown — in a throwaway tempdir with no GitHub and no
Claude Code. See [demo.md](demo.md) for the walkthrough and captured output:

```bash
scripts/demo.sh
```

Coming from the old `agent-ipc` / `agent-ipc-github` skills? See
[migration-from-agent-ipc.md](migration-from-agent-ipc.md).
