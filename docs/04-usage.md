# Usage: install → hooks → the four-verb loop → agent-to-agent messaging

This is the get-it-running guide for **agent-mailbox**: how to build and install
it, wire the Claude Code hooks, run the agent-facing loop, and check state as a
human. It assumes nothing beyond a stable Rust toolchain and (for GitHub
watching) the `gh` CLI.

New to the design? Read [01-wake](01-wake.md) for *why* the
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

> **macOS `Killed: 9` after reinstalling?** On Apple Silicon a locally-built binary
> can be SIGKILLed on launch after an in-place reinstall (a code-signature-cache
> quirk, not a bug). Fix it with `codesign --force -s - ~/.local/bin/mailbox`, or
> avoid it by installing to a fresh inode (`rm -f` the target first). See
> [05-release](05-release.md#macos-killed-9-after-a-local-reinstall).

### Bump the version

The whole workspace shares one version (`[workspace.package] version` in the root
`Cargo.toml`, inherited by every crate). Bump it in one step:

```bash
scripts/bump-version.sh patch      # 0.1.0 -> 0.1.1  (also minor | major | X.Y.Z)
scripts/bump-version.sh minor --tag  # bump, sync Cargo.lock, commit + tag vX.Y.Z
```

Without `--tag` it edits `Cargo.toml`/`Cargo.lock` and prints the commit/tag
commands so you can review the diff first. The new number then shows in
`mailbox --version` after a rebuild.

### Where state lives

The daemon keeps everything under one directory (default
`~/.agent-mailbox/`, override with `AGENT_MAILBOX_DB` for the full DB path or
`AGENT_MAILBOX_HOME` for the parent):

| File | What |
|---|---|
| `mailbox.db` | the durable SQLite topic log, subscriptions, cursors, watches |
| `mailbox.sock` | the user-scoped Unix socket clients connect to (`0600`) |
| `mailbox.lock` | the daemon's exclusive `flock` (single-writer guard) |
| `harness.log` | the hooks' decisions (kept off stderr so wakes stay clean) |

Nothing else is written per session. The wake path reads Claude Code's own registry
(`~/.claude/sessions/*.json`) to find each subscriber's inbox socket, and writes to
that socket — it keeps no per-session state of its own.

> Upgrading from an older build? Earlier versions left a `~/.mailbox/by-agent/`
> directory of per-session wake sentinels, and before that a `waiters/` directory of
> FIFOs, lockfiles and pidfiles plus a detached `mailbox harness watch` process per
> session. None of it exists any more
> ([ADR-0021](adr/0021-delete-the-sentinel-fallback.md),
> [ADR-0017](adr/0017-daemon-bumps-the-sentinel.md)). Clean up once with
> `pkill -f 'mailbox harness watch'`, `rm -rf ~/.agent-mailbox/waiters` and
> `rm -rf ~/.mailbox`; nothing recreates them. **Re-run
> `mailbox harness install-hooks`** so the retired hooks are swept.

### Start the daemon

Every command except `mailbox doctor` is a client of the `mailbox serve` daemon
(`doctor` reads Claude Code's registry and the process table, not the store, so it
works when the daemon is the broken thing). Start the daemon once — a login item, a `tmux` pane, or a user
service:

```bash
mailbox serve
```

If the daemon is **down**, clients fail loudly — non-zero with
`bridge not running; start it with 'mailbox serve'` — rather than silently
opening the DB or spawning a second writer. A second `serve` on the same DB also
fails loudly (the lock is held for the daemon's whole life).

---

## 2. Set up Claude Code (two commands)

Setting up a machine is two idempotent commands (plus one opt-in third, below), and
they are complementary:

```bash
mailbox harness install-skills   # skill  -> ~/.claude/skills
mailbox harness install-hooks    # hooks  -> ~/.claude/settings.json (when it exists)
```

- **`install-hooks`** makes the session **addressable** — the `SessionStart` hook
  registers its always-on agent inbox, so peers can reach it. Waking needs no
  infrastructure at all: the daemon writes the inbox socket Claude Code already bound.
- **`install-skills`** installs the skill that teaches the agent the loop it wakes
  into (subscribe / read / react / unsubscribe — no background pollers, no
  self-arming).

Both resolve their default under the same home — `AGENT_MAILBOX_HOME` if set, else
`HOME` — and both take an explicit override (`--settings <path>`,
`--skills-dir <path>`). Both are safe to re-run: after upgrading `mailbox`, run
them again to refresh the hooks and the skill.

There is a **third, opt-in** command that neither of those two will ever do for you:

```bash
mailbox harness install-inbound --settings <file>   # crossSessionInbound: "accept"
```

You need it only if your sessions run `--dangerously-skip-permissions`. Claude Code
holds an inbox-socket wake arriving at such a session for human approval and drops it
after ~5 minutes, so those sessions are never woken at all.
`accept` fixes that — and in doing so lets **any** process running as you put a
prompt in front of an agent that acts without asking. Read
[docs/01-wake.md](01-wake.md#receiving-on-a---dangerously-skip-permissions-session)
before running it, and prefer a per-session `--settings` file over your user settings
so it applies to the fleet that subscribes rather than every session you start.

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
- Re-running with a different `--mailbox-bin` **updates** our hooks in place; it never
  leaves a stale second copy pointing at an old binary. An upgrade from the old
  ADR-0006 `arm` hooks also sweeps them.

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
The snippet wires **two** hooks, and neither can wake the session:

```json
{
  "hooks": {
    "SessionStart": [{ "matcher": "", "hooks": [{ "type": "command",
      "command": "/abs/path/mailbox harness session-start" }] }],
    "SessionEnd": [{ "matcher": "", "hooks": [{ "type": "command",
      "command": "/abs/path/mailbox harness cleanup" }] }]
  }
}
```

- **`SessionStart` (matcher `""`) → `mailbox harness session-start`.** Registers this
  session's always-on agent inbox (`agent.<session-id>`), so peer agents can address it
  (§4). The matcher is `""` — every source, not just `startup` — so a **resumed**
  session, which is a fresh process, re-registers itself. Idempotent, fail-open, and it
  exits 0 always.
- **`SessionEnd` → `mailbox harness cleanup`.** Drops this session's subscriptions and
  interests, so no poller outlives the session that wanted it.

That is the whole harness surface. Waking is not a hook: the daemon writes the
session's inbox socket directly, and Claude Code starts a turn on it.

> **Upgrading?** Older versions installed three more hooks — a `FileChanged`
> `asyncRewake` wake, a `Stop` turn-end re-trigger, and a `UserPromptSubmit` turn
> stamp — all of which existed to prop up a wake wire that could silently lose an edge
> ([ADR-0021](adr/0021-delete-the-sentinel-fallback.md)). Re-running `install-hooks`
> sweeps them.

## Recovering a session whose inbox lapsed

Symptom: `mailbox status` reports `inbox topic: agent.<id> (NOT registered)` and
`subscriptions: none`, peers' `mailbox send` to you fails, and you never wake. Since
[ADR-0013](adr/0013-re-register-inbox-and-watchpaths-on-resume.md) this should heal
itself on your next `SessionStart` **or** turn boundary — so first just **take a
turn**. If you are on an older binary (or need it back immediately), re-run the
`SessionStart` handler by hand with a synthetic payload:

```bash
# Claude Code exports CLAUDE_CODE_SESSION_ID into every tool call; `mailbox status`
# prints the same id if you want to eyeball it first.
echo "{\"session_id\":\"$CLAUDE_CODE_SESSION_ID\"}" | mailbox harness session-start
```

Two things to know about that manual path:

- **This makes you addressable; it does not make you wakeable.** Registering the inbox
  is all the hook does. Whether anything can *wake* you is Claude Code's decision — it
  binds the inbox socket, or it does not — and running a hook by hand cannot change
  that.
- **Confirm with `mailbox doctor`, not by reading logs.** It reads whether your process
  is alive and whether Claude Code bound you a socket, which is the whole answer.
  `mailbox status`'s `wake:` line is that same verdict for the calling session
  ([ADR-0023](adr/0023-status-reports-the-wake-verdict.md)), which is why the two lines
  it prints — `wake` and `inbox topic` — are exactly the addressable/wakeable split
  above.

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
subscribe  ──►  (idle; the hooks keep you armed)  ──►  read  ──►  react
    ▲                                                                  │
    └──────────────────────  unsubscribe when done  ◄──────────────────┘
```

1. **subscribe** (once) — `mailbox subscribe <topic>`, or
   `mailbox watch github-pr <owner>/<repo>#<n>` (which subscribes *and* starts the
   shared poller). Baseline-on-subscribe: you only ever see events published
   *after* you subscribe.
2. **idle** — do other work, or nothing. When mail lands, the daemon writes it to the
   inbox socket Claude Code bound for your session, and you take a turn. **The agent
   does not arm anything and does not poll.**
3. **read** — on wake (a message tagged `[agent-mailbox]`, naming each topic with
   what changed on it), run `mailbox read`. It returns unread events and advances your
   cursor (exactly-once, advance-on-read). The wake tells you *what* changed and links
   to it; `read` is still where the event bodies are.
4. **react** — do the work: resolve the conflict, address the review, fix CI.
5. **unsubscribe** — `mailbox unsubscribe <topic>` (or `mailbox unwatch …`) when
   you no longer care. `SessionEnd` does this for you if you just end the session.

**The two rules that make this different from the old skill loop:** the agent
**never re-arms** after a wake, and the agent **never spawns a background
poller**. Both are owned by infrastructure — the hooks arm; the bridge
supervises adapters. If you find yourself writing a `while true; gh … ; sleep`
loop, stop: declare a `watch` instead.

### The commands, precisely

Run `mailbox <cmd> --help` for the authoritative flags.

**Session identity is automatic, and there is no flag for it.** The session-scoped
commands resolve *you* from `$CLAUDE_CODE_SESSION_ID`, which Claude Code exports into
every tool call — that is the single source of a session's own identity. Just run
them; `mailbox status` confirms who you are.

**Not every command needs one.** The test is whether the command has to know *whose*:

| Session | Commands | Why |
|---|---|---|
| **required** | `read`, `status`, `subscribe`, `unsubscribe`, `watch`, `unwatch` | The caller IS the subject — "my unread", "my state", "my interest". They have no meaning without an identity, so they fail with an error naming the variable. |
| **optional** | `send`, `agents` | The identity is a courtesy added on the way: `send` stamps a reply address, `agents` marks which row is you. Both work without one — see [§4's human poke](#a-human-poking-an-agent-from-a-terminal). |
| **never** | `publish`, `topics`, `doctor` | `publish` resolves no caller at all (ADR-0018), `topics` is a global read, and `doctor` probes *named* sessions (it reads the ambient one only to warn that a caller cannot measure itself). |

To run a command **as a named session** from a script or by hand (there is nothing in
the agent loop that needs this), set the variable for that one command:

```bash
CLAUDE_CODE_SESSION_ID=some-session mailbox status
```

> The `--session` flag and the `MAILBOX_SESSION_ID` fallback are **gone**. Nothing in
> production set either, and the flag's main effect on agents was to let them break
> themselves: `--session "$MAILBOX_SESSION_ID"` — a shape this doc used to have to warn
> against — expands to `--session ""` in an agent's shell and bound a phantom empty
> session. `mailbox doctor --session <id>` is unrelated and survives: it names a
> session to *probe*, not an identity to act as.

Add global `--json` for machine-readable stdout.

| Command | What it does |
|---|---|
| `mailbox subscribe <topic>` | Subscribe (baseline-on-subscribe). |
| `mailbox unsubscribe <topic>` | Unsubscribe. |
| `mailbox read [--limit <n>]` | Return unread events, advance the cursor. |
| `mailbox watch github-pr <owner>/<repo>#<n> [--interval <secs>]` | Watch a PR: record interest, subscribe to the PR topic, and (via the daemon) spawn the shared edge-triggered `github-pr` poller. Default interval 60s. |
| `mailbox unwatch github-pr <owner>/<repo>#<n>` | Drop this session's interest + unsubscribe; the poller stops only when the last interested session leaves. |
| `mailbox watch stub <label> [--interval-ms <n>] [--count <n>]` | Watch the reference stub publisher (synthetic edges; for the demo/tests). |
| `mailbox unwatch stub <label>` | Drop interest in the stub watch. |
| `mailbox publish <topic> [--body <json>] [--adapter <id>] [--subject <s>] [--link <url>]` | Publish an event to a topic. It goes to the topic and wakes every subscriber, you included (see below). `--subject` is the one line subscribers see on WAKE. |
| `mailbox send <target> [--text <s>] [--body <json>] [--subject <s>] [--link <url>]` | Message a peer agent (see below). Works with no session; the message then carries no `from`. |
| `mailbox agents [--json]` | List the agents you can `send` to. Works with no session; no row is then marked as you. |
| `mailbox topics [--prefix <p>] [--json]` | List known topics with subscriber/event counts. |

### Publishing: one rule

**An event goes to the topic and wakes every subscriber — its author included.**
That is the whole of it ([ADR-0018](adr/0018-publish-has-one-rule.md)).

`publish` does not resolve who is calling, and nothing about a publish depends on it.
An adapter, an agent, and a build script the agent spawned all run the same command
with the same effect. There is no refusal, no exit code to handle, and no flag to
remember when something you spawned needs to publish:

```bash
# from the agent, from a git hook, from a subagent — identical
mailbox publish ci.builds --body '{"build":"failed"}'
```

**Say what it is** ([ADR-0022](adr/0022-the-wake-carries-a-subject.md)). A subscriber
is woken with your `--subject`, so it starts the turn knowing what happened instead of
diffing the world against its memory of it:

```bash
mailbox publish ci.builds \
  --body '{"build":"failed","job":"lint"}' \
  --subject 'the lint job failed on main' \
  --link 'https://ci.example.com/runs/9'
```

It is optional — without one, subscribers are woken with the topic and a count. Keep it
to a description and a pointer: it is collapsed to one line and truncated at 120
characters, and the body is what `read` is for.

**Your own message wakes you too**
([ADR-0014](adr/0014-self-authored-events-wake-their-author.md)). Publishing is not
evidence of what you know: the same PR transition arriving from a `github-pr` watch has
no author at all and has always woken you, even when you caused it by pushing the
commit. Your own event is ordinary mail — it stays unread, `mailbox read` returns it,
and `mailbox status` counts it. The wake stops as soon as you read, like any other.

**Publishing never marks anything read.** Only a `read` moves a cursor, for you or for
anyone else.

Two rules used to live here and are gone:

- *"Be caught up to speak"* refused a publish (exit 3) from a caller with unread mail on
  the topic. It blocked a **write** because of the writer's **read** state, and it
  decided who the writer was from `$CLAUDE_CODE_SESSION_ID` — which Claude Code exports
  into every process an agent spawns, so a build script was gagged by its parent
  agent's inbox.
- *`--no-session`* was the escape hatch from that mis-attribution (and, before
  ADR-0014, from not waking yourself). With neither rule left, it had nothing to opt
  out of.

---

## 4. Agent-to-agent messaging

Agents can poke each other, with no human in the loop. Every live session is
**automatically** given an inbox — the topic `agent.<session-id>` — which the
`SessionStart` hook registers for it (always-on; see
[ADR-0007](adr/0007-always-on-agent-inboxes.md)). An agent does nothing to become
addressable, and `SessionEnd` deregisters it.

The loop is: **discover → send → the peer's idle session wakes → it reads → it
replies.**

```bash
# 1. Who am I, and who can I reach?
mailbox status
mailbox agents
```

```text
2 agent(s):
  4f9c1a2b-…  inbox=agent.4f9c1a2b-…  running (a send reaches it; `mailbox doctor` proves it can be woken)  <- you
  9d2e7c05-…  inbox=agent.9d2e7c05-…  not running (a send still lands in its inbox)
```

```bash
# 2. Message a peer (bare session id, or its full agent.* topic).
mailbox send 9d2e7c05-… --text "review done on PR 42, please rebase"

# ...or with a structured body:
mailbox send 9d2e7c05-… --body '{"kind":"review-done","pr":42}'

# ...and say what it is ABOUT, which is what the peer sees on wake:
mailbox send 9d2e7c05-… --text "…" --subject "PR 42 is approved, please rebase"
```

The peer wakes with its inbox topic and who messaged it — `from 4f9c1a2b-…: PR 42 is
approved, please rebase`, or just `message from 4f9c1a2b-…` without a `--subject`. The
message TEXT never rides the wake wire: a wake says what is waiting, not what it says.
The peer reads it like any other event:

```bash
mailbox read
```

```json
{"result":"read","events":[{"id":"evt-7","offset":0,"topic":"agent.9d2e7c05-…",
 "timestamp":1785904651808,"body":{"from":"4f9c1a2b-…","kind":"review-done","pr":42}}]}
```

**The body convention.** When the sender is a session, the bridge stamps
`"from": "<sender-session-id>"` into the message (overwriting any `from` the sender
supplied), so the receiver can reply with `mailbox send <from> …`. `--text "..."` is
shorthand for `--body '{"text":"..."}'`. Everything else in the body is yours: the bus
never interprets it, and `--body` must be a JSON **object** (there must be somewhere to
stamp `from`).

`from` is a **reply address, not a requirement** — a message sent by a human has none,
and then the key is simply **absent** (see the next section). Read it as optional: key
present ⇒ a peer you can reply to; key missing ⇒ nobody to reply to.

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
mailbox: agent "s-ghost" has no registered inbox, so nothing was published. This does
NOT mean the session id is wrong or stale — a live session can have an unregistered
inbox … The message was DROPPED, not queued …
```

Check `mailbox agents` for who is actually addressable. There is no `--force`: the
only thing it could do is lose your message silently.

> **This error does not mean the id is wrong.** It says the *inbox* is
> unregistered, which a live session with a perfectly valid id can be. Do not
> conclude the peer "restarted with a new session id" — a resumed session keeps its
> id (`mailbox status`), and only its registration lapses. The earlier wording here
> read "unknown agent", and a real agent took it as an identity problem and burned
> ~20 minutes retrying a wrong theory while its report never arrived. If a peer is
> unreachable, the fix is on *their* side (their next `SessionStart`/`Stop` hook
> re-registers them — see the recovery note below), and you should re-send after
> that rather than assume a new address.

**What liveness means (and doesn't).** `agents` reports `running` when a Claude Code
process still carries that session id in its argv (read from the process table). That
says the agent EXISTS — not that it is idle, not that it is healthy, and **not that it
can be woken**: a running agent may be mid-turn, and only `mailbox doctor` proves
wakeability. `not running` means nobody is executing that session any more; the message
still lands durably in its inbox, it simply has nobody to collect it.

### A human poking an agent from a terminal

Peer-to-peer is the headline, but the same two commands are how **you** reach your own
agents from an ordinary shell — no Claude Code session, no `CLAUDE_CODE_SESSION_ID`,
nothing to set up. `agents` and `send` are the two commands that do not need a caller
identity, precisely so this works:

```console
$ env -u CLAUDE_CODE_SESSION_ID mailbox agents
2 agent(s):
  agent-bravo  inbox=agent.agent-bravo  running (a send reaches it; `mailbox doctor` proves it can be woken)
  agent-delta  inbox=agent.agent-delta  not running (a send still lands in its inbox)
```

Note there is no `<- you` marker: you are not one of these agents, and nothing is
marked. Then poke one:

```console
$ env -u CLAUDE_CODE_SESSION_ID mailbox send agent-bravo --text "stop and check CI on #42"
note: no CLAUDE_CODE_SESSION_ID, so this message carries no `from` and agent-bravo
cannot reply to it. That is normal when a human pokes an agent from a terminal — say
who you are in the text if you want an answer.
sent to agent-bravo on agent.agent-bravo (event evt-1 at offset 0)
```

The agent wakes exactly as it does for a peer's message, and reads it exactly the same
way — but **with no `from` key at all**:

```json
{"result":"read","events":[{"id":"evt-1","offset":0,"topic":"agent.agent-bravo",
 "timestamp":1785907052330,"body":{"text":"stop and check CI on #42"}}]}
```

Absent, not `null` and not a `"human"` placeholder: any placeholder would be a string
an agent could hand back to `mailbox send`, where it would either fail or reach the
wrong agent. The bridge also **strips** a `from` supplied in `--body` on this path, so
an unattributed message can never claim a sender the bridge did not verify.

> **What the agent should do:** act on the content — it is a real instruction from its
> human — and *not* try to reply. There is no address to reply to; it answers in its
> normal turn output, where you are reading. The shipped skill says exactly this.

`env -u` is only to make the point explicit; in a plain shell the variable is not set
anyway, so `mailbox agents` and `mailbox send …` are enough.

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

`status` is the human-facing window into a session — **and the answer to "who am
I"**, which is why there is no separate `whoami`. It never consumes events (it counts,
it does not `read`):

```bash
mailbox status
```

```text
session: my-session
wake: reachable
inbox topic: agent.my-session (registered)
watches:
  github-pr myrepo#42  state=running interest=1 interval=60s child=pid 51234
subscriptions (2):
  agent.my-session
  github.pr.me/myrepo#42
unread:
  [github.pr.me/myrepo#42] 1
```

- **session** — who this session is.
- **wake** — whether anything can wake this session, and the load-bearing line of the
  three. It is the SAME verdict `mailbox doctor` reports and `subscribe`/`watch` refuse
  on ([ADR-0023](adr/0023-status-reports-the-wake-verdict.md)), read from Claude Code's
  own registry rather than asked of the bridge:
  - `reachable` — Claude Code bound this session an inbox socket. Mail arrives while it
    is idle.
  - `no-inbox` — it did not, so **nothing can wake this session**. The one fault, and
    only a restart fixes it; the line carries the same remedy `doctor` prints.
  - `unregistered` — Claude Code has no record of this session (a non-Claude harness
    looks like this, and so does a Claude Code too old to register itself). Not a fault.
  - `unknown` — the session registry could not be read, which is not evidence either
    way. Never reported as a fault, for [ADR-0009](adr/0009-interest-liveness-from-the-waiter-pidfile.md)'s
    reason.
- **inbox topic** — this session's peer-messaging address, plus whether that address is
  `registered` (the `SessionStart` hook does that). If it says `NOT registered`, peers
  cannot `send` to this session — check the hooks are installed and the daemon is up.
  A different question from `wake`: this one is about the bus, that one is about the
  process. A `registered` topic on a `no-inbox` session means mail lands durably with
  nothing to announce it.
- **watches** — each supervised watch, its `state`
  (`desired`/`running`/`stopped`/`failed`), how many sessions are `interest`ed,
  the poll `interval`, and the adapter's `child` pid when the supervisor is
  running one. `running` with a pid means the poller is live.
- **subscriptions** — the topics this session listens on, headed by how many
  there are. The count includes this session's own `agent.<id>` inbox, so an
  armed session with no watches reads `1`, not `0` — the inbox is a real
  subscription, and the `inbox topic` line above says whether it is registered.
- **unread** — per-topic count of events past this session's cursor. `read`
  drains these.

**With the bridge down**, `status` still prints the three local lines — session, wake
and inbox topic are derived here, not fetched — and replaces the rest with an explicit
`bridge: UNREACHABLE`. It still exits non-zero
([ADR-0004](adr/0004-cli-serve-daemon-and-socket.md)): most of the report is genuinely
missing, so exiting 0 would report "fine" for a command whose main content is absent.
In `--json` that is one object — the usual `"result": "error"` shape with `session`,
`wake`, `inbox_topic` and `"bridge": "unreachable"` added.

> The wake verdict is deliberately in the local half. A session nothing can wake must
> be told so even when the daemon is the dead thing — that is the moment it is most
> likely to be waiting on mail nobody will announce.

In `--json` on the normal path the verdict is the `wake` key, alongside the existing
`inbox` one:

```bash
mailbox status --json | jq -r .wake      # reachable | no-inbox | unregistered | unknown
```

### Putting the subscription count in a Claude Code status line

`--json` carries the same number as a single `subscription_count` key, so a
status line reads one scalar rather than downloading the topic list to measure
it:

```bash
mailbox status --json | jq .subscription_count
```

Claude Code hands a status-line command the session id on stdin. Pass it through the
env var the CLI reads, so the line is about that session whether or not Claude Code
exported it into the status-line process:

```bash
#!/usr/bin/env bash
# ~/.claude/statusline.sh — "📬 3" when this session is on 3 topics.
session=$(jq -r .session_id)
count=$(CLAUDE_CODE_SESSION_ID="$session" mailbox status --json 2>/dev/null \
  | jq -r '.subscription_count // empty')
[ -n "$count" ] && printf '📬 %s' "$count"
```

Two things that script must survive, because a status line re-renders on every
prompt and must never become a red line in the UI:

- **The bridge being down.** `status` is a socket client, so with no
  `mailbox serve` it exits non-zero and `--json` prints an error object instead
  ([ADR-0004](adr/0004-cli-serve-daemon-and-socket.md)) — right for a human at a
  terminal, noise in a status line. Hence `2>/dev/null` and `// empty`: the
  latter is load-bearing, because a plain `jq -r .subscription_count` on that
  error object renders the string `null` into your status line.
- **The count being `0`.** That is an answer, not a failure: this session is on
  no topics — including no inbox — so peers cannot `send` to it and nothing will
  wake it. Worth showing rather than hiding, and `// empty` still yields it
  (jq's `//` only skips `null` and `false`, not `0`).

Note the count is *subscriptions*, not mail: it does not move when events arrive.
For "do I have unread?", sum `.unread[].unread` from the same report.

### Is the wake path actually working?

`status` answers "what does this session have?". It cannot answer "will this session
ever be told?", because whether a session can be woken at all is Claude Code's
decision: it binds a per-session inbox socket, or it does not, and the gate that
decides cannot be turned on from outside.

### `mailbox doctor` — can these agents be woken?

`doctor` reads the answer ([ADR-0021](adr/0021-delete-the-sentinel-fallback.md)). It is
two facts: is the session's process alive, and did Claude Code bind it an inbox socket.

```bash
mailbox doctor                    # every session; print only the faults
mailbox doctor --all              # list every session
mailbox doctor --json             # for a supervisor agent or a cron
mailbox doctor --session <id>     # just one
```

```text
no-inbox   983eae5f-0b09-410e-a457-c9633b6f1a8a  agent-mailbox-30
           Claude Code bound this session no inbox socket, so nothing can wake it.
           Restart the session. …
3 session(s): 2 reachable, 1 cannot be woken, 0 gone
```

- **Exit 1 when any session is a fault**, so a supervisor can notice without parsing
  prose.
- Three verdicts. `reachable` (live process, bound socket). `no-inbox` (live process,
  **no** socket — **the fault**; nothing can wake it, and only restarting the session
  can fix it). `gone` (no live process — normal, and not a fault).
- **A session can measure itself.** The probe this replaced could not: running it *was*
  a turn, so the caller was busy for the whole measurement and could only ever report
  itself as unmeasured. A read has no such blind spot.
- **There is no `busy` verdict any more**, because being busy no longer affects
  reachability: a message queues at the receiver and is read between tool calls, so a
  mid-turn session is just as reachable as an idle one.
- Reads no store and needs no daemon, so it still answers when the daemon is the
  broken thing.

You should rarely need it: `subscribe` and `watch` already refuse for a session
nothing can wake, so the common case is caught at the moment of asking rather than
discovered later.

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
