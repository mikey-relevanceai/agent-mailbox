# Agent Mailbox

Agent mailbox lets agents be **woken by asynchronous, real-world events** instead
of polling for them. It integrates with Claude Code (the harness we target
first): an agent subscribes to the things it cares about, goes idle, and gets
woken when one of them changes. Things worth waking on:

- changes in pull-request state (merge conflicts, reviews, CI)
- the deployment of your changes
- monitoring of those changes in production
- a peer agent handing off work

The key idea: **the agent never polls and never re-arms.** A local *bridge daemon*
supervises the pollers and, when mail lands, writes it straight to the session's
Claude Code **inbox socket** — which starts a turn on an idle session. The whole
agent-facing contract is four verbs — **subscribe → read → react → unsubscribe** —
and that's it.

```text
Adapters (detect world changes)
        ↓ publish
Bridge (durable events + subscriptions)
        ↓ write the subscriber's inbox socket
Agent sessions (take a turn; never poll)
```

One hop. No file, no watch, no hook, no exit code, nothing per-session on our side,
and nothing on a timer
([ADR-0021](docs/adr/0021-delete-the-sentinel-fallback.md),
[ADR-0020](docs/adr/0020-peer-inbox-socket-is-the-wake-wire.md)).

The woken agent is told what changed and where — never the event body, which stays in
the durable log until it runs `mailbox read`
([ADR-0022](docs/adr/0022-the-wake-carries-a-subject.md)):

```text
[agent-mailbox] mail on 1 topic — run `mailbox read`

github.pr.acme/web#42 — 2 unread
  · CI failed: build
    https://github.com/acme/web/actions/runs/9/job/2
  · new comment
    https://github.com/acme/web/pull/42#issuecomment-2145678
```

Requires **Claude Code 2.1.226+**, plus two settings that are not on by default —
see [Setup: what the wake depends on](#setup-what-the-wake-depends-on). A session
Claude Code gave no inbox socket cannot be woken by anything, so `mailbox watch` and
`mailbox subscribe` refuse up front rather than leaving an agent waiting on a wake
that will never come.

## Quickstart

```bash
# 1. Build the bridge + adapters
cargo build --release

# 2. See it work end to end, with no network and no Claude Code:
scripts/demo.sh
```

The demo starts a private daemon in a tempdir and walks the whole loop —
subscribe/read, an idle wake, a supervised poller, and teardown — asserting that
the daemon delivers the wake to the session's inbox. Captured
output is in [docs/demo.md](docs/demo.md).

To actually use it:

```bash
# mailbox + the two adapters, co-located on PATH, plus the `gh` the PR adapter needs.
brew install mikey-relevanceai/tap/mailbox

mailbox serve &                                                   # the bridge daemon

# The two setup commands: hooks make wake infrastructure, the skill teaches the
# agent the loop it wakes into. Neither is run for you by `brew`: both write to
# ~/.claude, outside Homebrew's prefix.
mailbox harness install-skills   # skill  -> ~/.claude/skills
mailbox harness install-hooks --mailbox-bin "$(brew --prefix)/opt/mailbox/bin/mailbox"
```

Pass `--mailbox-bin` exactly as shown — see the gotcha table below for why. Building
from source instead? Use `scripts/install-local.sh` (→ `~/.local/bin`, or
`DEST=/usr/local/bin`) rather than `cp`/`install`: on Apple Silicon, overwriting a
signed binary in place gets the new one SIGKILLed (`Killed: 9`), so the helper takes
a fresh inode and re-signs ([docs/05-release.md](docs/05-release.md)). With a
from-source install, plain `mailbox harness install-hooks` is correct.

Both are idempotent — re-run them after an upgrade to refresh the hooks and the
skill. Both default under the same home (`AGENT_MAILBOX_HOME`, else `HOME`), and
both take an override: `--skills-dir <path>` and `--settings <path>`.

Neither command touches the two Claude Code settings the wake depends on — that is
the next section, and it is not optional.

`install-hooks` merges into your existing `~/.claude/settings.json` (preserving
unrelated settings and foreign hooks). If that file does **not** exist, it prints
the snippet instead of conjuring a `settings.json` on a machine with no Claude
Code — pass `--settings <path>` to create one anyway.

Then, from an agent session:

```bash
mailbox watch github-pr owner/repo#42   # subscribe + start the poller
# ... go idle; the hooks keep you armed ...
mailbox read                            # on wake
mailbox unwatch github-pr owner/repo#42 # when done
```

No session argument, and no `--session` flag: the session-scoped commands resolve
*you* from the `$CLAUDE_CODE_SESSION_ID` Claude Code exports into every tool call.
(`publish` resolves no caller at all — it goes to the topic and wakes every
subscriber, its author included: [ADR-0018](docs/adr/0018-publish-has-one-rule.md).)

Full walkthrough (install, hooks, the four-verb loop, `mailbox status`):
**[docs/04-usage.md](docs/04-usage.md)**.

## Setup: what the wake depends on

Two Claude Code settings decide whether a wake ever lands, and **neither is on by
default**. Getting them wrong is silent — the agent subscribes, goes idle, and simply
never hears anything. `mailbox doctor` tells you which one you are missing.

### 1. `CLAUDE_CODE_HARBOR_KITE` — does this session have an inbox socket at all?

Claude Code only binds the per-session inbox socket — the thing the bridge writes to —
when an undocumented gate is on, and that gate is a **gradual rollout**. Same-version
sessions on the same machine disagree: before we found this, roughly half the live
sessions on one machine silently had no socket and could not be woken by anything.

Force it on, in **user** settings — `~/.claude/settings.json`:

```json
{
  "env": { "CLAUDE_CODE_HARBOR_KITE": "1" }
}
```

- **User scope only.** Claude Code strips this key from a repo's
  `.claude/settings.json` and `.claude/settings.local.json`, warning that
  "project-scoped settings can't set this key". It has to be `~/.claude/settings.json`
  or managed settings.
- **Restart the session.** The gate is read once at startup; a running session gains
  no socket.
- **Any non-empty value is truthy** — `"0"` turns it *on* too. Delete the key to turn
  it off.
- **It short-circuits the remote flag.** With it set, `DISABLE_TELEMETRY`,
  `DO_NOT_TRACK`, `DISABLE_GROWTHBOOK` and `CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC`
  stop mattering here. Without it, any of them blocks the flag fetch and you get no
  socket.

Confirm with `mailbox doctor`: `reachable` means the socket is bound, `no-inbox` means
this session cannot be woken by anything.

> Undocumented and unstable. Verified against Claude Code **2.1.229**, where the gate
> is `CLAUDE_CODE_HARBOR_KITE || tengu_harbor_kite`. Expect the variable to be retired
> once the rollout completes — check before trusting this.

### 2. `crossSessionInbound` — will this session accept what we write?

Only needed for sessions running **`--dangerously-skip-permissions`**. Those *hold* an
arriving wake for an approval nobody is there to give and drop it after five minutes,
and there is no second channel behind it — so they simply do not wake. Ordinary
prompting sessions need nothing here.

```bash
mailbox harness install-inbound --settings <file>   # preferred: just this fleet
mailbox harness install-inbound                     # user settings: every session
```

Read what it widens first: `accept` means that session takes messages from any process
running as you, without a prompt. That is what lets the bridge wake an agent that acts
without asking — and it means anything else running as you can direct it too.
`install-hooks` deliberately never sets this; the choice is yours to make.

- **A repo cannot grant it.** Claude Code lets a repo's settings only *tighten* this
  ("a repo may only tighten, so your own `accept` cannot override it"), so a committed
  `.claude/settings.json` is the one place `accept` will not work. Use `~/.claude/settings.json`,
  or a settings file you hand the session yourself.
- **Managed settings win.** An org policy of `hold` overrides your `accept`.
- **An existing `"hold"` or `"refuse"` blocks wakes on *any* session**, not just bypass
  ones. `install-inbound` reports what it found before overwriting it — read that line.
- **Restart the session.** Like the gate above, this is read at session start.

More detail: [docs/01-wake.md](docs/01-wake.md).

### Everything else that has to be true

Less exotic, but each one fails silently in its own way:

| Trick | What breaks without it |
|---|---|
| Run `install-hooks` from the **installed** binary, not `cargo run` or `target/debug` | The absolute path of whatever binary you ran is baked into `settings.json`. Point it at a build tree and the `SessionStart` hook dies after the next `cargo clean` — your inbox stops being registered, so peers cannot address you, while topic wakes keep working. `--mailbox-bin <abs path>` overrides. |
| On a Homebrew install, point `install-hooks` at `$(brew --prefix)/opt/mailbox/bin/mailbox` | The path you give it is the path baked into `settings.json`. Give it the versioned Cellar path — which is what `brew --prefix`'s plain `bin/` resolves to on Linux, and what an explicit Cellar path does anywhere — and the next `brew upgrade` deletes it. The `SessionStart` hook then dies exactly as in the row above: your inbox stops being registered, so peers cannot address you, while topic wakes keep working. The `opt` path is the one Homebrew re-points on upgrade ([ADR-0025](docs/adr/0025-hooks-point-at-a-stable-path.md)). |
| After upgrading the binary: re-run `install-hooks` **and** restart `mailbox serve` | The re-run sweeps every hook name ever shipped, so an upgrade cannot leave a retired hook firing at a subcommand that no longer exists. The running daemon is still the old code and holds the lock. |
| Start `mailbox serve` with the **same `HOME`** as your Claude sessions | The daemon resolves Claude Code's sessions directory **once, at startup, from its own environment**. Started by launchd/systemd with a different or missing `HOME` — or with `CLAUDE_CONFIG_DIR`/`MAILBOX_CLAUDE_SESSIONS_DIR` changed after it booted — it can wake nobody. Events still store durably; you get one `warn` line. Restart it after any such change. |
| `gh` on the **daemon's** `PATH`, authenticated as the **daemon's** user | Adapters are spawned by `serve` and inherit its environment, so `gh auth` is checked there, not in your shell. Missing auth is fatal: the adapter exits non-zero and the supervisor gives up on that watch. |
| Check `mailbox status` says `wake: reachable` | `watch`/`subscribe` refuse only on *positive* evidence of `no-inbox`. A session Claude Code never registered (older than 2.1.224, or a non-Claude harness) reads as `unregistered`, which is deliberately not a fault — it subscribes happily and then waits forever. |
| `AGENT_MAILBOX_HOME` does **not** redirect the Claude sessions registry | It moves our DB, hooks and skills; the registry follows `MAILBOX_CLAUDE_SESSIONS_DIR` → `CLAUDE_CONFIG_DIR` → `HOME`. Sandboxing with `AGENT_MAILBOX_HOME` alone leaves the wake path pointed at the real `~/.claude`. |

Upgrading from an older build, or migrating off `agent-ipc`, leaves live pollers and
stale skills that nothing cleans up — see
[docs/04-usage.md](docs/04-usage.md) and
[docs/migration-from-agent-ipc.md](docs/migration-from-agent-ipc.md).

## Layout

```text
crates/
  mailbox/            # bridge CLI + daemon binary
  mailbox-protocol/   # shared publish/subscribe types
  mailbox-harness/    # Claude Code integration: the hook set (session-start /
                      # cleanup) + hooks, skills and inbound-policy install
adapters/
  stub-adapter/       # reference adapter (synthetic edges; demo/tests)
  github-pr-adapter/  # the real GitHub PR poller (via `gh`)
  slack-adapter/      # Slack channel/thread poller (via `curl`, token in the Keychain)
docs/
scripts/demo.sh       # self-contained end-to-end demo
skills/agent-mailbox/ # drop-in Claude Code skill (replaces agent-ipc); embedded in
                      # the binary and installed by `mailbox harness install-skills`
```

## Develop

Requires a stable Rust toolchain (`rustup`). From the repo root:

```bash
cargo check --workspace
cargo test --workspace
cargo run -p mailbox -- --help    # the CLI surface
```

## Docs

See [docs/00-index.md](docs/00-index.md) for the full list. Agent-oriented repo
guidance lives in [AGENTS.md](AGENTS.md).

**Start here:**

- [Usage: install → hooks → the four-verb loop](docs/04-usage.md)
- [Demo](docs/demo.md) — runnable + captured output
- [Migrating from the agent-ipc skills](docs/migration-from-agent-ipc.md)

**Design:**

- [Wake](docs/01-wake.md) — the inbox socket, the two hooks, and why nothing re-arms
- [Tech stack](docs/02-tech-stack.md)
- [Working agreements](docs/03-working-agreements.md) — ADRs, designs, PRs, mikey-in-a-box install
- [ADRs](docs/adr/README.md) · [Designs](docs/design/README.md) · [MVP GitHub watch](docs/design/01-mvp-github-watch.md)
