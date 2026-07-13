# Agent Mailbox docs

Notes for the project. **New here and want to *use* it?** Start with
[04-usage](04-usage.md) and the [demo](demo.md). Reading to understand the
*design*? Go in number order.

**Get it running:**

| Doc | What it covers |
|---|---|
| [04-usage](04-usage.md) | **Start here.** Install, hooks, the four-verb agent loop, agent-to-agent messaging, `mailbox status` |
| [demo](demo.md) | Runnable end-to-end demo (`scripts/demo.sh`) + captured output; real-PR steps |
| [migration-from-agent-ipc](migration-from-agent-ipc.md) | Retire the old `agent-ipc` / `agent-ipc-github` skills; drop-in replacement skill |

**Design notes:**

| # | Doc | What it covers |
|---|---|---|
| 00 | [Index](00-index.md) | This page |
| 01 | [Wake and re-arm](01-wake-and-rearm.md) | Idle-session wake via Claude Code `asyncRewake`, delivery cursors, Codex gap |
| 02 | [Tech stack](02-tech-stack.md) | Rust bridge, subprocess adapters (WASI later), security process split, early test bar |
| 03 | [Working agreements](03-working-agreements.md) | ADRs, designs, branch/PR default, mikey-in-a-box install |

Also:

| Path | What it covers |
|---|---|
| [adr/](adr/README.md) | Architecture Decision Records |
| [design/](design/README.md) | Major system / subsystem designs |
| [design/01-mvp-github-watch.md](design/01-mvp-github-watch.md) | MVP: GitHub PR watch lifecycle (**Implemented**) |

## Mental model

```text
Adapters (detect world changes)          Peer agents (mailbox send)
        ↓ publish                                ↓ publish to agent.<session-id>
Bridge (durable events + subscriptions + wake kicks)
        ↓ harness wake
Agent sessions (react, never poll)
```

Adapters never know how wake works. The bridge owns topics and per-subscriber cursors. Harness integrators own arm/re-arm so the agent does not. Every live session is automatically subscribed to its own inbox topic (`agent.<session-id>`), so agents can wake each other with no human in the loop (ADR-0007).

## Repo map

| Path | Role |
|---|---|
| `crates/mailbox` | Bridge CLI (`cargo run -p mailbox`) |
| `crates/mailbox-protocol` | Shared wire/domain types |
| `crates/mailbox-harness` | Session wake / re-arm helpers |
| `adapters/` | External adapter processes |

Agent-facing working agreements: [AGENTS.md](../AGENTS.md).
