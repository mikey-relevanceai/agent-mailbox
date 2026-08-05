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

The per-session wake sentinels live elsewhere, under `~/.mailbox/by-agent/<session>/`
(override with `MAILBOX_SENTINEL_ROOT`), because Claude Code has to watch them by
absolute path.

> Upgrading from an older build? It left a `waiters/` directory of per-session FIFOs,
> lockfiles and pidfiles, plus one detached `mailbox harness watch` process per session
> that has ever started. Neither exists any more ([ADR-0017](adr/0017-daemon-bumps-the-sentinel.md)).
> Clean them up once with `pkill -f 'mailbox harness watch'` and
> `rm -rf ~/.agent-mailbox/waiters`; nothing recreates them.

### Start the daemon

Every command except `mailbox doctor` is a client of the `mailbox serve` daemon
(`doctor` reads sentinel files, not the store, so it works when the daemon is the
broken thing). Start the daemon once — a login item, a `tmux` pane, or a user
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

Setting up a machine is two idempotent commands, and they are complementary:

```bash
mailbox harness install-skills   # skill  -> ~/.claude/skills
mailbox harness install-hooks    # hooks  -> ~/.claude/settings.json (when it exists)
```

- **`install-hooks`** makes wake **infrastructure** — the `SessionStart` hook arms
  the session's wake sentinel and tells Claude Code to watch it, so the agent stays
  wakeable for the whole session with no periodic re-arm (ADR-0008) and nothing to
  run itself.
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
The snippet wires five hooks. Exactly ONE of them (`FileChanged`) can wake the
session; the rest always exit 0:

```json
{
  "hooks": {
    "SessionStart": [{ "matcher": "", "hooks": [{ "type": "command",
      "command": "/abs/path/mailbox harness session-start" }] }],
    "Stop": [{ "matcher": "", "hooks": [{ "type": "command",
      "command": "/abs/path/mailbox harness turn-end" }] }],
    "UserPromptSubmit": [{ "matcher": "", "hooks": [{ "type": "command",
      "command": "/abs/path/mailbox harness turn-start" }] }],
    "FileChanged": [{ "matcher": ".mailbox-wake", "hooks": [{ "type": "command",
      "command": "/abs/path/mailbox harness wake",
      "asyncRewake": true, "timeout": 30 }] }],
    "SessionEnd": [{ "matcher": "", "hooks": [{ "type": "command",
      "command": "/abs/path/mailbox harness cleanup" }] }]
  }
}
```

> **After upgrading the `mailbox` binary, re-run `mailbox harness install-hooks`.** An
> upgrade that skips it leaves hooks pointing at subcommands the new binary no longer
> has (`harness arm`, `harness ensure-watcher`, `harness watch`). Those do not fail
> quietly: an unrecognised subcommand exits **2**, and a `Stop` hook that exits 2
> *blocks the turn from ending* and feeds its stderr to the model — so a half-upgraded
> install nags the agent with clap usage text at every turn boundary, on top of having
> no working wake path. Re-running sweeps them and installs the current set; it
> recognises every name we have ever installed, so nothing is left behind.

What each hook does:

- **`SessionStart` (matcher `""`) → `mailbox harness session-start`** (plain,
  synchronous). Reads the `session_id` from the hook's stdin JSON, **registers the
  session's agent inbox** (`agent.<session-id>` — this is what makes it reachable by peer
  agents, see §4), **arms the wake sentinel** (writes the session's current unread topics
  into it, creating the file), and prints the `watchPaths` registration for that absolute
  path. The order matters: Claude Code watches a path, and the daemon's later writes are
  *modifications* — if the file did not exist first, the daemon's first write would be a
  *creation*, which the watch may not deliver. Arming also picks up mail that arrived
  while nothing owned this session, so a resumed agent wakes for it instead of waiting
  for the next publish. The matcher is `""` (all sources), so it re-fires on
  **resume**/clear/compact, and this is the only hook that can re-print watchPaths
  (ADR-0013). Every step is idempotent, and it never wakes the session itself.

  If the bridge is **down or erroring**, it still arms (with an empty topic set) and
  still prints the watchPaths — fail-open, because an unwatchable file is worse than an
  empty one. A down bridge never produces a *wake*; the first publish after the daemon
  returns writes the sentinel.
- **`Stop` → `mailbox harness turn-end`**. The turn boundary — the session's per-turn
  self-healing point, with three jobs. **(1) Close the turn** (ADR-0016), so
  `mailbox doctor` can tell a session that is merely mid-turn from one that cannot be
  woken. **(2) Re-register the inbox** (best-effort, fail-open — ADR-0013, restoring the
  register-on-every-`Stop` invariant; this is what self-heals a resume that raced the 10s
  tombstone). **(3) Re-arm and re-trigger:** if the sentinel has gone missing it is
  written again (the only deafness a per-turn hook can heal), and if the session is
  sitting on unread mail the sentinel is re-bumped so the `FileChanged` wake fires
  against the now-idle session (ADR-0012). That third part is what delivers mail which
  arrived while the agent was **busy** — a wake edge spent mid-turn reaches nothing, and
  without it the agent would go idle deaf on top of unread mail (a real bug, seen on a
  watched PR). It is bounded: each message earns at most one turn-boundary nudge, so an
  agent that wakes and does not read is not looped. It does **not** re-print
  `watchPaths` (a `Stop` cannot emit a SessionStart registration — that is
  `session-start`'s job). It **always exits 0** — a `Stop` can never itself wake the
  session. It cannot help a session that goes idle *forever* (which fires no `Stop`) —
  see the note below.
- **`UserPromptSubmit` → `mailbox harness turn-start`.** Stamps that a turn has opened.
  Paired with the `Stop` hook's turn-ended stamp it is what lets `mailbox doctor` report
  a silent session as **busy** rather than libelling it as deaf (ADR-0016). It prints
  nothing and always exits 0.
- **`FileChanged` (matcher `.mailbox-wake`) → `mailbox harness wake`**
  (`asyncRewake: true`). When the daemon writes the sentinel, this fires — even on an
  idle session — and exits **2** with `mail on topic X` **iff there is genuinely unread
  mail**, else exits **0**. That anti-loop guard is load-bearing: a `FileChanged` fires
  on every change to the sentinel, so waking unconditionally would loop the agent.
- **`SessionEnd` → `mailbox harness cleanup`.** **Removes the session's sentinel dir**
  and drops this session's subscriptions (including its inbox — it stops being
  addressable) **and** watch interests, stopping any adapter whose last interested
  session it was (no zombie poller outlives the session).

**No periodic re-arm (ADR-0008).** The old design kept a long idle armed by having a
waiter exit 2 at `--max-block-ms` to force a re-arm — one model turn per `max_block` of
idle. That is gone: nothing runs on a timer, so an idle subscribed session costs **zero**
model turns until real mail arrives, and **every wake is real mail**. See
[01-wake](01-wake.md),
[ADR-0008](adr/0008-on-demand-wake-filechanged.md) and
[ADR-0017](adr/0017-daemon-bumps-the-sentinel.md).

**Notes and limits.**

- **Launch `claude` from a project directory, not `$HOME`.** `FileChanged` watches the
  cwd recursively, so if the cwd is an ancestor of `~/.mailbox`, every *other* session's
  sentinel bump also fires this session's wake hook. There is **no false wake** (the hook
  re-checks this session's own unread and exits 0), but it does spawn an extra wake-hook
  process per unrelated bump. Launching from a project dir avoids the churn.
- **Idle-forever + a lost sentinel.** A session that goes idle *forever* (never takes
  another turn, so the `Stop` hook never fires) whose sentinel is then deleted stays deaf
  until it next takes a turn or is restarted. This is the accepted limit of a
  zero-spurious-wake design: nothing can poke a session that will neither act nor be
  poked. `mailbox doctor` is how you find one — it PROVES wakeability rather than
  assuming it.
- **Mail that arrives while the daemon is down** bumps nothing. It is not lost (the
  event is durable): the session's next turn boundary re-triggers for it, and its next
  `SessionStart` re-arms from unread. Restarting the daemon does not by itself wake
  anyone who missed a publish.

#### Recovering a session whose inbox lapsed

Symptom: `mailbox status` reports `inbox: agent.<id> (NOT registered)` and
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

- **The inbox comes back, but the wake does not (yet).** The handler prints a
  `watchPaths` registration on stdout, and that only means anything when **Claude Code**
  is the one reading it — i.e. when it runs as a real `SessionStart` hook. Run from a
  shell, the registration goes to your terminal and is discarded, so no watch is placed
  on the sentinel for the current process. You become *addressable* (peers can `send`,
  and you will see mail on your next `mailbox read`) but not *auto-wakeable* until a
  genuine `SessionStart` fires. **No `Stop` hook can fix this** — a `Stop` cannot emit a
  `SessionStart`-shaped registration.
- **Confirm with `mailbox doctor`, not by reading logs.** It bumps the sentinel and
  requires the `FileChanged` hook to answer, so it distinguishes "wakeable" from "deaf"
  from "busy" from "no process at all" — none of which the logs can tell apart.

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
2. **idle** — do other work, or nothing. The `SessionStart` hook armed your wake
   sentinel and told Claude Code to watch it; the daemon writes that file when mail
   lands for you. **The agent does not arm anything and does not poll.**
3. **read** — on wake (a system reminder like `mail on topic X`), run
   `mailbox read`. It returns unread events and advances your cursor
   (exactly-once, advance-on-read).
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
| `mailbox publish <topic> [--body <json>] [--adapter <id>]` | Publish an event to a topic. It goes to the topic and wakes every subscriber, you included (see below). |
| `mailbox send <target> [--text <s>] [--body <json>]` | Message a peer agent (see below). Works with no session; the message then carries no `from`. |
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
```

The peer wakes (`mail on topic agent.9d2e7c05-…` — payload-free, as always), and it
reads the message like any other event:

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
inbox: agent.my-session (registered)
watches:
  github-pr myrepo#42  state=running interest=1 interval=60s child=pid 51234
subscriptions (2):
  agent.my-session
  github.pr.me/myrepo#42
unread:
  [github.pr.me/myrepo#42] 1
```

- **session / inbox** — who this session is, and its peer-messaging address, plus
  whether that address is `registered` (the `SessionStart` hook does that). If it says
  `NOT registered`, peers cannot `send` to this session — check the hooks are
  installed and the daemon is up.
- **watches** — each supervised watch, its `state`
  (`desired`/`running`/`stopped`/`failed`), how many sessions are `interest`ed,
  the poll `interval`, and the adapter's `child` pid when the supervisor is
  running one. `running` with a pid means the poller is live.
- **subscriptions** — the topics this session listens on, headed by how many
  there are. The count includes this session's own `agent.<id>` inbox, so an
  armed session with no watches reads `1`, not `0` — the inbox is a real
  subscription, and the `inbox` line above says whether it is registered.
- **unread** — per-topic count of events past this session's cursor. `read`
  drains these.

**With the bridge down**, `status` still prints the two identity lines — they are
derived locally, not fetched — and replaces the rest with an explicit
`bridge: UNREACHABLE`. It still exits non-zero
([ADR-0004](adr/0004-cli-serve-daemon-and-socket.md)): most of the report is genuinely
missing, so exiting 0 would report "fine" for a command whose main content is absent.
In `--json` that is one object — the usual `"result": "error"` shape with `session`,
`inbox_topic` and `"bridge": "unreachable"` added.

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
ever be told?", because the last hop of the wake — Claude Code noticing the sentinel
file and running the `FileChanged` hook — happens outside the bridge. A session can
have a registered inbox and a sentinel being written on every publish, and still never
wake.

There used to be a `mailbox dashboard` that inferred this from `harness.log`. It was
removed: measured against an active probe on 19 live idle sessions it was wrong 9
times — 6 sessions it called suspect answered immediately, and 3 it called verified
could not be woken at all. History cannot answer a question about now, so ask
directly.

### `mailbox doctor` — can these agents be woken *right now*?

`doctor` runs the experiment
([ADR-0016](adr/0016-prove-wakeability-with-an-active-probe.md)): it bumps each
session's sentinel and requires the `FileChanged` hook to answer, which it does by
stamping `.mailbox-hook-ran` on every run.

```bash
mailbox doctor                    # probe every session; print only the faults
mailbox doctor --all              # list every session probed
mailbox doctor --json             # for a supervisor agent or a cron
mailbox doctor --session <id>     # just one
mailbox doctor --timeout-ms 20000 # longer budget on a loaded machine
```

```text
probed 19 session(s) in one window, 10000ms budget: 13 wakeable, 4 deaf, 2 UNMEASURED
  459ba64c-…  DEAF — its sentinel changed and Claude Code never ran the wake hook; mail will not reach this agent
  8ce450e7-…  DEAF — …
```

- **Exit 1 when any session is deaf**, so a supervisor can notice without parsing
  prose. Sessions with no live process do not affect the exit code.
- The verdicts are `wakeable` (the hook answered — positive proof), `deaf` (live,
  armed, **idle**, and silent — **the fault**), `busy`, `gone` (no live Claude Code
  process; normal, not a fault), `never_armed`, and `undetermined`.
- **`busy` is reported as `UNMEASURED`, and that is not "no fault found".** A
  mid-turn session could not have answered the probe, so the probe learned nothing
  about it — and a genuinely deaf session that happens to be busy looks exactly the
  same. It is an open question to re-probe while the session is idle, which is why
  the summary line counts it in its own bucket rather than folding it into the
  healthy total.
- **A session cannot measure itself.** Running `doctor` IS a turn, so the caller is
  busy for the whole probe and can only ever report itself `UNMEASURED`. An agent
  auditing its own fleet is therefore structurally blind to its own deafness;
  `doctor` warns about this on stderr. Probe a session from a *different* session
  (or a cron) to learn whether it can be woken.
- Busy-vs-deaf comes from a pair of turn-boundary stamps written by the
  `UserPromptSubmit` and `Stop` hooks, so **re-run `mailbox harness install-hooks`**
  after upgrading. Without the new hook every busy session reports as `deaf`.
- **Wakeability is perishable.** A session that answers today can be deaf tomorrow
  with no visible event in between, so re-run this rather than trusting an old
  result. That is the finding that motivates the command existing at all.
- Every session is bumped **before any is polled**, so the fleet is measured in one
  window — which is what makes "this session is deaf" distinguishable from "something
  global hiccuped".
- Probing does not change what an agent will be told is unread: the bump rewrites the
  sentinel with the bytes it already holds. A session with nothing unread answers with
  exit 0 and no model turn. A session that *does* have unread mail will be woken —
  which is correct, since it should already have been.
- Reads no store and needs no daemon.
- If **nothing at all** answers, suspect the install before the fleet: the ack is
  written by whichever `mailbox` binary the hook invokes, and an old one looks
  identical to a total blackout. `doctor` says so when it sees that pattern. No
  session needs restarting after an upgrade.

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
