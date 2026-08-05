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
supervises the pollers and, when mail lands, writes a per-session **sentinel file**
that Claude Code is watching; a `FileChanged` *hook* turns that write into a wake.
The whole agent-facing contract is four verbs — **subscribe → read → react →
unsubscribe** — and that's it.

```text
Adapters (detect world changes)
        ↓ publish
Bridge (durable events + subscriptions; writes each subscriber's wake sentinel)
        ↓ FileChanged hook → exit 2
Agent sessions (react, never poll)
```

Three links, two live components and a file. There is no per-session watcher
process and nothing on a timer ([ADR-0017](docs/adr/0017-daemon-bumps-the-sentinel.md)).

## Quickstart

```bash
# 1. Build the bridge + adapters
cargo build --release

# 2. See it work end to end, with no network and no Claude Code:
scripts/demo.sh
```

The demo starts a private daemon in a tempdir and walks the whole loop —
subscribe/read, an idle wake, a supervised poller, and teardown — asserting that
the daemon writes the sentinel and that the wake hook exits 2 on it. Captured
output is in [docs/demo.md](docs/demo.md).

To actually use it:

```bash
# Install mailbox + the two adapters co-located on PATH
install -m755 target/release/mailbox \
              target/release/mailbox-stub-adapter \
              target/release/mailbox-github-pr-adapter ~/.local/bin/

mailbox serve &                                                   # the bridge daemon

# The two setup commands: hooks make wake infrastructure, the skill teaches the
# agent the loop it wakes into.
mailbox harness install-skills   # skill  -> ~/.claude/skills
mailbox harness install-hooks    # hooks  -> ~/.claude/settings.json (when it exists)
```

Both are idempotent — re-run them after an upgrade to refresh the hooks and the
skill. Both default under the same home (`AGENT_MAILBOX_HOME`, else `HOME`), and
both take an override: `--skills-dir <path>` and `--settings <path>`.

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

## Layout

```text
crates/
  mailbox/            # bridge CLI + daemon binary
  mailbox-protocol/   # shared publish/subscribe types
  mailbox-harness/    # Claude Code integration: the hook set (session-start /
                      # turn-start / turn-end / wake / cleanup) + hooks & skills install
adapters/
  stub-adapter/       # reference adapter (synthetic edges; demo/tests)
  github-pr-adapter/  # the real GitHub PR poller (via `gh`)
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

- [Wake](docs/01-wake.md) — the sentinel, the hooks, and why nothing re-arms
- [Tech stack](docs/02-tech-stack.md)
- [Working agreements](docs/03-working-agreements.md) — ADRs, designs, PRs, mikey-in-a-box install
- [ADRs](docs/adr/README.md) · [Designs](docs/design/README.md) · [MVP GitHub watch](docs/design/01-mvp-github-watch.md)
