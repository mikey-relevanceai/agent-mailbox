# Agent Mailbox docs

Design notes for the experiment. Read in order if you're new; otherwise jump by topic.

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
| [design/01-mvp-github-watch.md](design/01-mvp-github-watch.md) | MVP: GitHub PR watch lifecycle |

## Mental model

```text
Adapters (detect world changes)
        ↓ publish
Bridge (durable events + subscriptions + wake kicks)
        ↓ harness wake
Agent sessions (react, never poll)
```

Adapters never know how wake works. The bridge owns topics and per-subscriber cursors. Harness integrators own arm/re-arm so the agent does not.

## Repo map

| Path | Role |
|---|---|
| `crates/mailbox` | Bridge CLI (`cargo run -p mailbox`) |
| `crates/mailbox-protocol` | Shared wire/domain types |
| `crates/mailbox-harness` | Session wake / re-arm helpers |
| `adapters/` | External adapter processes |

Agent-facing working agreements: [AGENTS.md](../AGENTS.md).
