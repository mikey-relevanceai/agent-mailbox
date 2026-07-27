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
| `waiters/` | per-session wake FIFOs + watcher pidfiles |
| `harness.log` | watcher / `wake` / `wait` logs (kept off stderr so wakes stay clean) |

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

- **`install-hooks`** makes wake **infrastructure** — the `SessionStart` hook starts
  a detached watcher that keeps the agent wakeable for the whole session (no periodic
  re-arm; ADR-0008), so the agent never has to.
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
The snippet wires four hooks (ADR-0008 — on-demand wake, no periodic re-arm *wake*; the
`Stop` hook is a plain exit-0 liveness poke, never a wake):

```json
{
  "hooks": {
    "SessionStart": [{ "matcher": "", "hooks": [{ "type": "command",
      "command": "/abs/path/mailbox harness session-start" }] }],
    "Stop": [{ "matcher": "", "hooks": [{ "type": "command",
      "command": "/abs/path/mailbox harness ensure-watcher" }] }],
    "FileChanged": [{ "matcher": ".mailbox-wake", "hooks": [{ "type": "command",
      "command": "/abs/path/mailbox harness wake",
      "asyncRewake": true, "timeout": 3600 }] }],
    "SessionEnd": [{ "matcher": "", "hooks": [{ "type": "command",
      "command": "/abs/path/mailbox harness cleanup" }] }]
  }
}
```

> **After upgrading the `mailbox` binary, re-run `mailbox harness install-hooks`.** An
> upgrade that skips it leaves the stale ADR-0006 `arm` hooks in your `settings.json`;
> they still fire exit-2 re-arm wakes and contend for the single-waiter lock with the new
> watcher. Re-running sweeps them and installs the current set.

What each hook does:

- **`SessionStart` (matcher `""`) → `mailbox harness session-start`** (plain,
  synchronous). Reads the `session_id` from the hook's stdin JSON, **registers the
  session's agent inbox** (`agent.<session-id>` — this is what makes it reachable by peer
  agents, see §4), prints a `watchPaths` registration for this session's sentinel file,
  and spawns a **detached watcher** that outlives the hook and blocks on the mail FIFO for
  the whole session. The matcher is `""` (all sources), so it re-fires on
  **resume**/clear/compact — a resumed session is a fresh process that must re-establish
  all three, and this is the only hook that can re-print watchPaths (ADR-0013). Every step
  is idempotent. It never wakes the session itself (it is not `asyncRewake`).

  If the bridge is **down or erroring**, it still prints the watchPaths and spawns the
  watcher (fail-open): the watcher needs no daemon and re-checks subscriptions itself
  under its lock, so an unsubscribed session's watcher simply self-exits. A down bridge
  never produces a *wake*; the watcher just blocks, and the first publish after the
  daemon returns kicks it.
- **`Stop` → `mailbox harness ensure-watcher`**. The turn-boundary net — the session's
  per-turn self-healing point, with three jobs, **in this order**. **(1) Re-register the
  inbox** (best-effort, fail-open — ADR-0013, restoring the register-on-every-`Stop`
  invariant; this is what self-heals a resume that raced the 10s tombstone). It runs
  first on purpose: a watcher spawned while the inbox is unregistered self-exits
  `Unsubscribed`, so registering after the spawn would cost a turn. **(2) Stop-liveness:**
  it respawns the detached watcher if it has died (a live watcher is left alone) — the
  primary recovery for a watcher killed by a crash or an OS/OOM kill. **(3) The
  level-triggered re-trigger (ADR-0012):** if the session is sitting on unread mail, it
  re-bumps the sentinel so the `FileChanged` wake fires against the now-idle session.
  That is what delivers mail which arrived while the agent was **busy** — a wake edge
  spent mid-turn reaches nothing, and without this the agent would go idle deaf on top of
  unread mail (a real bug, seen on a watched PR). It is bounded: each message earns at
  most one turn-boundary nudge, so an agent that wakes and does not read is not looped.
  It does **not** re-print `watchPaths` (a `Stop` cannot emit a SessionStart registration
  — that is `session-start`'s job). It **always exits 0** — a `Stop` can never itself
  wake the session; it only re-triggers the ordinary wake path. It costs a small per-turn
  process spawn plus one fail-open socket call but **no model turn**. It cannot help a
  session that goes idle *forever* (which fires no `Stop`) — see the note below.
- **`FileChanged` (matcher `.mailbox-wake`) → `mailbox harness wake`**
  (`asyncRewake: true`). When the watcher bumps the sentinel, this fires — even on an
  idle session — and exits **2** with `mail on topic X` **iff there is genuinely unread
  mail**, else exits **0**. That anti-loop guard is load-bearing: a `FileChanged` fires
  on every change to the sentinel, so waking unconditionally would loop the agent.
- **`SessionEnd` → `mailbox harness cleanup`.** Reaps the watcher, **removes the
  session's sentinel dir**, and drops this session's subscriptions (including its inbox
  — it stops being addressable) **and** watch interests, stopping any adapter whose last
  interested session it was (no zombie poller outlives the session).

**No periodic re-arm (ADR-0008).** The old design kept a long idle armed by having the
waiter exit 2 at `--max-block-ms` to force a re-arm — one model turn per `max_block` of
idle. That is gone: the detached watcher blocks indefinitely with no timer, so an idle
subscribed session costs **zero** model turns until real mail arrives, and **every wake
is real mail**. (`mailbox wait` / `mailbox harness arm` and their timing knobs survive
as retained primitives, but are no longer wired into the hooks.) See
[01-wake-and-rearm](01-wake-and-rearm.md) and
[ADR-0008](adr/0008-on-demand-wake-filechanged.md).

**Notes and limits.**

- **Launch `claude` from a project directory, not `$HOME`.** `FileChanged` watches the
  cwd recursively, so if the cwd is an ancestor of `~/.mailbox`, every *other* session's
  sentinel bump also fires this session's wake hook. There is **no false wake** (the hook
  re-checks this session's own unread and exits 0), but it does spawn an extra wake-hook
  process per unrelated bump. Launching from a project dir avoids the churn.
- **Idle-forever + watcher death.** A session that goes idle *forever* (never takes
  another turn, so the `Stop`-liveness hook never fires) whose watcher then dies stays
  deaf until it next takes a turn or is restarted. This is the accepted limit of a
  zero-spurious-wake design. The OS user-service supervises the *daemon*; the `Stop` hook
  supervises the per-session *watcher*; nothing can poke a session that will neither act
  nor be poked.

#### Recovering a session whose inbox lapsed

Symptom: `mailbox status` reports `inbox: agent.<id> (NOT registered)` and
`subscriptions: none`, peers' `mailbox send` to you fails, and you never wake. Since
[ADR-0013](adr/0013-re-register-inbox-and-watchpaths-on-resume.md) this should heal
itself on your next `SessionStart` **or** turn boundary — so first just **take a
turn**. If you are on an older binary (or need it back immediately), re-run the
`SessionStart` handler by hand with a synthetic payload:

```bash
# Claude Code exports CLAUDE_CODE_SESSION_ID into every tool call; `mailbox whoami`
# prints the same id if you want to eyeball it first.
echo "{\"session_id\":\"$CLAUDE_CODE_SESSION_ID\"}" | mailbox harness session-start
```

Two things to know about that manual path:

- **The inbox and the watcher come back, but the wake does not (yet).** The handler
  prints a `watchPaths` registration on stdout, and that only means anything when
  **Claude Code** is the one reading it — i.e. when it runs as a real `SessionStart`
  hook. Run from a shell, the registration goes to your terminal and is discarded, so
  the `FileChanged` sentinel is not re-armed for the current process. You become
  *addressable* (peers can `send`, and you will see mail on your next `mailbox read`)
  but not *auto-wakeable* until a genuine `SessionStart` fires. **No `Stop` hook can
  fix this** — `ensure-watcher` cannot emit a `SessionStart`-shaped registration.
- **Diagnosing a watcher that will not stay up.** A watcher spawned while the inbox is
  unregistered self-exits immediately (`watcher found no subscriptions; exiting without
  arming a sentinel` in `harness.log`) and removes its pidfile — so it *looks* like no
  watcher was ever spawned. Register the inbox first; the watcher then stays up. This
  is why `ensure-watcher` registers the inbox *before* it checks watcher liveness.

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

1. **subscribe** (once) — `mailbox subscribe <topic>`, or
   `mailbox watch github-pr <owner>/<repo>#<n>` (which subscribes *and* starts the
   shared poller). Baseline-on-subscribe: you only ever see events published
   *after* you subscribe.
2. **idle** — do other work, or nothing. The `SessionStart` hook starts a detached
   watcher that keeps you wakeable for the whole session (no re-arm). **The agent does
   not arm anything and does not poll.**
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

**Session identity is automatic.** The session-scoped commands resolve *you* from
`$CLAUDE_CODE_SESSION_ID` (which Claude Code exports into every tool call), so you
do **not** pass `--session` — just run them, and `mailbox whoami` confirms who you
are. Pass `--session <id>` only to act as a *different* session. Full resolution
order: `--session` > `$MAILBOX_SESSION_ID` > `$CLAUDE_CODE_SESSION_ID`.

> **Pitfall:** do not write `--session "$MAILBOX_SESSION_ID"`. In an agent's shell
> that variable is usually **empty** (the hooks set it only for the background
> waiter), so it expands to `--session ""`. An empty flag is treated as absent and
> falls through to `$CLAUDE_CODE_SESSION_ID` — but the clearer fix is to just omit
> the flag.

Add global `--json` for machine-readable stdout. Below, `--session` is shown only
where it is a genuine argument; the session-scoped commands take the optional
override but do not need it.

| Command | What it does |
|---|---|
| `mailbox subscribe <topic>` | Subscribe (baseline-on-subscribe). |
| `mailbox unsubscribe <topic>` | Unsubscribe. |
| `mailbox read [--limit <n>]` | Return unread events, advance the cursor. |
| `mailbox watch github-pr <owner>/<repo>#<n> [--interval <secs>]` | Watch a PR: record interest, subscribe to the PR topic, and (via the daemon) spawn the shared edge-triggered `github-pr` poller. Default interval 60s. |
| `mailbox unwatch github-pr <owner>/<repo>#<n>` | Drop this session's interest + unsubscribe; the poller stops only when the last interested session leaves. |
| `mailbox watch stub <label> [--interval-ms <n>] [--count <n>]` | Watch the reference stub publisher (synthetic edges; for the demo/tests). |
| `mailbox unwatch stub <label>` | Drop interest in the stub watch. |
| `mailbox publish <topic> [--body <json>] [--adapter <id>] [--no-session]` | Publish an event to a topic (see the rules below). `--no-session` publishes anonymously — use it from any script/hook/subagent the agent spawns. |
| `mailbox whoami [--json]` | Print this session's id and inbox topic. Works with the bridge down. |
| `mailbox send <target> [--text <s>] [--body <json>]` | Message a peer agent (see below). |
| `mailbox agents [--json]` | List the agents you can `send` to. |
| `mailbox topics [--prefix <p>] [--json]` | List known topics with subscriber/event counts. |

### Publishing: two rules for agents (adapters are unaffected)

`publish` resolves *you* the same way every other command does, so a publish from an
agent is attributed to that agent's session — the event carries your session id as its
**author**. Two rules follow, and they only apply to a **topic you subscribe to**:

1. **Be caught up to speak.** A publish is **refused** (exit **3**, nothing written) if
   you have unread events on that topic **that someone else wrote**. You would be
   talking past mail you have not read.

   ```text
   refused: you have 3 unread event(s) on gibson — run `mailbox read` first, then
   publish again (nothing was published)
   ```

   Run `mailbox read`, react to what is there, then publish. Nothing was lost — the
   event you tried to send was simply not written, so just publish it again.

   Exit **3** is the refusal's own code, so a script can tell it apart from a real
   failure (a bridge that is down is still exit 1). It is never exit 2 — that is the
   wake code.

2. **Your own message wakes you too.** The publish kicks every subscriber to the
   topic, you included ([ADR-0014](adr/0014-self-authored-events-wake-their-author.md)).
   Authorship records *who published*, not *what you already know*: the same PR
   transition arriving from a `github-pr` watch has no author at all and has always
   woken you, even when you caused it by pushing the commit. Waking on one and not the
   other was the inconsistency.

   Your own event is ordinary mail: it stays unread, `mailbox read` returns it, and
   `mailbox status` counts it — all three now agree. The one thing it does not do is
   *gag* you: rule 1 ignores what you wrote, so your own message can never block your
   next publish.

**Adapters are exempt from rule 1.** An adapter has no session, so no unread rule
applies to its publishes. That is the point of a mailbox — a poller must be able to
publish into a topic whose subscribers are far behind.

### `--no-session`: publishing from a script the agent spawned

Claude Code exports `$CLAUDE_CODE_SESSION_ID` into **every process an agent spawns** — a
build script, a git hook, a subagent. So a `mailbox publish` from any of them is
attributed to the *agent*, and by rule 2 it will not wake the agent.

Pass **`--no-session`** from any such process:

```bash
# in a build script / git hook / subagent the agent spawned
mailbox publish ci.builds --no-session --body '{"build":"failed"}'
```

The event is then authored by **nobody**: no unread rule applies to it, and it wakes
**every** subscriber — the agent included. This is the right flag for anything that
publishes *on the agent's behalf* rather than *as* the agent.

Without it, the message is still durable and still visible (it is unread for the agent,
and `mailbox read` returns it) — it just will not *wake* the session it was
mis-attributed to.

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
mailbox whoami
mailbox agents
```

```text
2 agent(s):
  4f9c1a2b-…  inbox=agent.4f9c1a2b-…  idle (waiter appears blocked — a send should wake it)  <- you
  9d2e7c05-…  inbox=agent.9d2e7c05-…  busy or unarmed (a send still lands in its inbox)
```

```bash
# 2. Message a peer (bare session id, or its full agent.* topic).
mailbox send 9d2e7c05-… --text "review done on PR 42, please rebase"

# ...or with a structured body:
mailbox send 9d2e7c05-… --body '{"kind":"review-done","pr":42}'
```

The peer's idle waiter wakes (`mail on topic agent.9d2e7c05-…` — payload-free, as
always), and it reads the message like any other event:

```bash
mailbox read
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
mailbox: agent "s-ghost" has no registered inbox, so nothing was published. This does
NOT mean the session id is wrong or stale — a live session can have an unregistered
inbox … The message was DROPPED, not queued …
```

Check `mailbox agents` for who is actually addressable. There is no `--force`: the
only thing it could do is lose your message silently.

> **This error does not mean the id is wrong.** It says the *inbox* is
> unregistered, which a live session with a perfectly valid id can be. Do not
> conclude the peer "restarted with a new session id" — a resumed session keeps its
> id (`mailbox whoami`), and only its registration lapses. The earlier wording here
> read "unknown agent", and a real agent took it as an identity problem and burned
> ~20 minutes retrying a wrong theory while its report never arrived. If a peer is
> unreachable, the fix is on *their* side (their next `SessionStart`/`Stop` hook
> re-registers them — see the recovery note below), and you should re-send after
> that rather than assume a new address.

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
  `registered` (the `SessionStart` hook does that). If it says
  `NOT registered`, peers cannot `send` to this session — check the hooks are
  installed and the daemon is up.
- **watches** — each supervised watch, its `state`
  (`desired`/`running`/`stopped`/`failed`), how many sessions are `interest`ed,
  the poll `interval`, and the adapter's `child` pid when the supervisor is
  running one. `running` with a pid means the poller is live.
- **subscriptions** — the topics this session listens on.
- **unread** — per-topic count of events past this session's cursor. `read`
  drains these.

### `mailbox dashboard` — is the wake path actually working?

`status` answers "what does this session have?". It cannot answer "will this session
ever be told?", because the last hop of the wake — Claude Code noticing the sentinel
file and running the `FileChanged` hook — happens outside the bridge. A session can
have a registered inbox, a live watcher, and a sentinel being bumped on every kick,
and still never wake.

`mailbox dashboard` is the fleet-wide view of exactly that
([ADR-0015](adr/0015-dashboard-reads-the-store-read-only.md)):

```bash
mailbox dashboard              # live view; [q]uit [r]efresh [d] no-wake only [a] incl. dead
mailbox dashboard --once       # one plain-text snapshot (what you paste into an issue)
mailbox dashboard --deaf-only  # just the sessions with no observed wake
```

```text
MAILBOX  daemon up  59 live / 306 known sessions  31 watches  250 events
WAKE     28 verified, 98 with no wake observed

SESSION    UNREAD   WAITER   INBOX   WATCH   WAKE
3800bebb   7        live     reg     1       NO WAKE OBSERVED (8 sentinel bumps, 0 hook runs)
5fd46282   0        live     reg     1       wake verified (12 hook runs, 8 wakes)
```

- **WAKE** is *evidence*, not a verdict, reconstructed from `harness.log`:
  - `wake verified` — a `FileChanged` hook demonstrably ran for this session, so the
    harness IS watching its sentinel. Positive proof; any hook run counts, including
    the ones that found nothing.
  - `NO WAKE OBSERVED` — its sentinel has been bumped and no hook has ever run. Very
    likely unwakeable. It is not called *deaf* because the log cannot prove that: it
    may have rotated, or every bump may have landed mid-turn (a lost edge —
    [ADR-0012](adr/0012-level-triggered-wake-at-the-turn-boundary.md)). The bump count
    is shown so you can check the claim.
  - `no evidence yet` — never bumped. A new session sits here.
- Rows sort **worst-first**: a session sitting on unread mail it cannot be woken for
  is the first thing on screen.
- **live / known** — subscriptions outlive a session whose `SessionEnd` never ran, so
  the store knows about many more sessions than exist. Dead ones are hidden (they
  cannot be woken and are not a fault); `--all` shows them.
- It reads the store **read-only**, so it still renders when the bridge is down —
  headed `daemon DOWN`, which is precisely when you want to look at it.

> **The dashboard's WAKE column is history, not a live verdict.** Measured against
> an active probe on 19 live idle sessions it was wrong 9 times: 6 sessions it called
> suspect answered immediately, and 3 it called verified could not be woken at all.
> To ask about *now*, use `mailbox doctor`.

### `mailbox doctor` — can these agents be woken *right now*?

The dashboard reconstructs what has happened. `doctor` runs the experiment
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
probed 19 session(s) in one window, 10000ms budget: 13 wakeable, 6 deaf
  459ba64c-…  DEAF — its sentinel changed and Claude Code never ran the wake hook; mail will not reach this agent
  8ce450e7-…  DEAF — …
```

- **Exit 1 when any session is deaf**, so a supervisor can notice without parsing
  prose. Sessions with no live process do not affect the exit code.
- The verdicts are `wakeable` (the hook answered — positive proof), `deaf` (live,
  armed, silent — **the fault**), `gone` (no live Claude Code process; normal, not a
  fault), `never_armed`, and `undetermined`.
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
- Reads no store and needs no daemon, like `dashboard`.
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
