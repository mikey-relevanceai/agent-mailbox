# Migrating from the `agent-ipc` / `agent-ipc-github` skills

**agent-mailbox replaces the old skill-based IPC loop.** If you (or your agents)
still use the `agent-ipc` and `agent-ipc-github` Claude Code skills — the ones
that ran `ipc-arm.sh` in the background and spawned a `gh-watch.sh` polling loop
per PR — this is how to move off them.

The headline change: **the agent stops owning the wake loop.** No more
`ipc-arm.sh` after every message, no more background `gh-watch.sh` pollers piling
up. The Claude Code *hooks* keep the waiter armed, and the *bridge daemon*
supervises the PR pollers (one per PR, refcounted, torn down when the last
interested session leaves). See [01-wake-and-rearm](01-wake-and-rearm.md) and
[design/01](design/01-mvp-github-watch.md) for the why.

---

## The old pattern vs the new one

| Old skill loop | New agent-mailbox flow |
|---|---|
| Agent runs `ipc-arm.sh` in the background after each turn to re-arm the wake | **Nothing.** `SessionStart`/`Stop` hooks run `mailbox harness arm`; the agent never re-arms |
| One arm = one wake; agent must re-arm after reading | Hooks keep the waiter armed across wakes; the waiter self-respawns before the hook timeout |
| Agent spawns `gh-watch.sh` per PR with `run_in_background`, must remember to kill it | `mailbox watch github-pr OWNER/REPO#N` — the **daemon** owns the poller; one per PR, shared across sessions |
| Watcher state in `~/.claude/agent-ipc/watchers/*.state` files | Baseline persists centrally in the bridge's SQLite (`adapter_baseline`), round-tripped via the protocol |
| Durable NDJSON inbox + FIFO kick, per agent | Durable topic log + per-subscriber cursors in the bridge; multi-subscriber topics |
| Dead session leaks a running `gh-watch.sh` | `SessionEnd` hook drops interest; supervisor stops any now-orphaned poller; TTL sweeper backstops a hard kill |

### Command-by-command map

| You used to… | Now run… |
|---|---|
| `ipc-arm.sh` (background, re-run after each turn) | *nothing* — install the hooks once (`mailbox harness install-hooks`) |
| Start `gh-watch.sh OWNER/REPO N` in the background | `mailbox watch github-pr OWNER/REPO#N --session <id>` |
| Read the NDJSON inbox / react to a kick | `mailbox read --session <id>` |
| `kill` the `gh-watch.sh` loop when done | `mailbox unwatch github-pr OWNER/REPO#N --session <id>` (or just end the session) |
| Check what you're watching | `mailbox status --session <id>` |
| Send a peer-agent message (`agent-ipc`) | Publish/subscribe on a shared topic: `mailbox publish <topic>` / `mailbox subscribe <topic> --session <id>` (peer chat is topics too; the MVP demo is GitHub PRs) |

The four-verb loop — **subscribe → read → react → unsubscribe** — is documented
in full in [04-usage.md § The four-verb agent loop](04-usage.md#3-the-four-verb-agent-loop).

---

## The two rules that changed

1. **Agents never re-arm.** Delete every `ipc-arm.sh` call. Arming is a
   `SessionStart`/`Stop` hook now (`mailbox harness arm`), installed once.
2. **Agents never spawn a background poller.** Delete every backgrounded
   `gh-watch.sh` (and any `while true; gh …; sleep` loop). Declare a `watch`
   instead; the bridge daemon runs and supervises the poller.

If an agent instruction anywhere still says "run `ipc-arm.sh` in the background"
or "start a `gh-watch.sh` loop," it is stale — replace it with the drop-in skill
below.

---

## Drop-in replacement skill

`skills/agent-mailbox/SKILL.md` (in this repo) is a ready-to-use Claude Code
skill that teaches agents the new loop and explicitly forbids background pollers
and self-arming. It is embedded in the `mailbox` binary, so one command installs
it in place of the old two skills:

```bash
mailbox harness install-skills
```

That writes `~/.claude/skills/agent-mailbox/SKILL.md` (override the location with
`--skills-dir`). It is atomic and idempotent — re-run it after an upgrade and it
reports `unchanged`, or refreshes a drifted file to the shipped content.

It is intentionally a single skill covering both peer-agent messaging (old
`agent-ipc`) and GitHub PR watching (old `agent-ipc-github`), because both are
now just topics on the same bus.

---

## One-time USER step: retire the external skills

> **Heads-up — this repo cannot do this for you.** The `agent-ipc` and
> `agent-ipc-github` skills live in your Claude Code config (`~/.claude/skills/`),
> **outside this repository**. Removing them is a manual, one-time step you run on
> each machine. Do it **after** installing the hooks and the replacement skill,
> so no session is left without a wake path.

1. **Install the new path first** — the two setup commands (hooks + replacement
   skill):

   ```bash
   mailbox harness install-hooks --mailbox-bin ~/.local/bin/mailbox \
     --settings ~/.claude/settings.json
   mailbox harness install-skills
   ```

2. **Confirm nothing else references the old skills.** Check your
   `~/.claude/settings.json` (and any project `.claude/settings.json`) for hooks
   or permissions that invoke `ipc-arm.sh` / `gh-watch.sh`, and remove them.

3. **Kill any still-running background pollers** left over from the old loop:

   ```bash
   pkill -f gh-watch.sh   # stop orphaned pollers
   pkill -f ipc-arm.sh    # stop orphaned arm loops
   ```

4. **Remove the old skill directories** (adjust the paths to wherever they were
   installed):

   ```bash
   rm -rf ~/.claude/skills/agent-ipc \
          ~/.claude/skills/agent-ipc-github
   ```

5. **Optionally archive the old inbox/watcher state** once you're satisfied the
   new path works (it is no longer read):

   ```bash
   mv ~/.claude/agent-ipc ~/.claude/agent-ipc.retired 2>/dev/null || true
   ```

That's it — from here on, agents subscribe/`watch`, idle, read on wake, and
react, with no arm command and no background poller anywhere.
