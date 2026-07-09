# Agent Mailbox

Agent mailbox is a system designed to allow agents to be woken up via asynchronous actions. It integrates directly with Claude code, which is the most popular harness in use here, and allows you to have a number of background tasks trigger Claude to react to them. Examples of things that might be interesting to trigger would be:

- changes in pull request state
- the deployment of your changes
- monitoring of those changes in production

## Layout

```text
crates/
  mailbox/            # bridge CLI binary
  mailbox-protocol/   # shared publish/subscribe types
  mailbox-harness/    # harness wake helpers (Claude Code first)
adapters/             # subprocess adapters (protocol only; no bridge imports)
docs/
```

## Develop

Requires a stable Rust toolchain (`rustup`). From the repo root:

```bash
cargo check --workspace
cargo test --workspace
cargo run -p mailbox
```

## Docs

See [docs/00-index.md](docs/00-index.md) for the full list. Agent-oriented repo guidance lives in [AGENTS.md](AGENTS.md).

- [Wake and re-arm](docs/01-wake-and-rearm.md)
- [Tech stack](docs/02-tech-stack.md)
- [Working agreements](docs/03-working-agreements.md) — ADRs, designs, PRs, mikey-in-a-box install
- [ADRs](docs/adr/README.md) · [Designs](docs/design/README.md)
