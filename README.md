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

Requires **Claude Code 2.1.226+**. A session Claude Code gave no inbox socket cannot
be woken by anything, so `mailbox watch` and `mailbox subscribe` refuse up front
rather than leaving an agent waiting on a wake that will never come.

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

**If your sessions run `--dangerously-skip-permissions`**, they will *hold* an
inbox-socket wake for an approval nobody is there to give, and drop it after five
minutes — and there is no second channel behind it, so those sessions simply do not
wake. There is a third, **opt-in** command that fixes it:

```bash
mailbox harness install-inbound --settings <file>   # crossSessionInbound: "accept"
```

Read what it widens first: `accept` means that session takes messages from any
process running as you without a prompt. That is what lets the bridge wake an agent
that acts without asking — and it means anything else running as you can direct it
too. `install-hooks` deliberately never sets this; the choice is yours to make.
See [docs/01-wake.md](docs/01-wake.md).

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
                      # cleanup) + hooks, skills and inbound-policy install
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

- [Wake](docs/01-wake.md) — the inbox socket, the two hooks, and why nothing re-arms
- [Tech stack](docs/02-tech-stack.md)
- [Working agreements](docs/03-working-agreements.md) — ADRs, designs, PRs, mikey-in-a-box install
- [ADRs](docs/adr/README.md) · [Designs](docs/design/README.md) · [MVP GitHub watch](docs/design/01-mvp-github-watch.md)
