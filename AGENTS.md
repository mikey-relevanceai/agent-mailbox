# Repository guidelines for AI coding agents

Orientation for agents working in this repo. **Read [docs/00-index.md](docs/00-index.md) first** — it lists the design docs and when to use each.

## Keeping this file updated

When you change architecture, crate boundaries, commands, or agent-facing conventions, update this file and/or the relevant `docs/*.md` in the same change.

## What this is

Local durable **topic bus** for coding agents: adapters publish world-change events; the bridge stores them and kicks waiters; harness integrators wake idle sessions (Claude Code first via `asyncRewake`). Agents react — they do not poll, and they should not re-arm themselves.

```text
Adapters → publish → Bridge (topics + cursors + kicks) → harness wake → Agent
```

## Hard boundaries

- **Adapters never import bridge internals.** They speak the protocol only (CLI / socket today; WASI later). Live under `adapters/`.
- **Protocol vs transport.** Wire/domain types live in `mailbox-protocol`. How an adapter is hosted (`SubprocessTransport` now, `WasiTransport` later) is a replaceable host boundary — do not leak transport into protocol or storage.
- **Payload-free wake.** Kicks carry no model-visible body; events are read from the durable log. Wake is ingress, not authority.
- **v0 network:** no TCP listen. CLI and/or user-scoped Unix socket only.
- **Language:** Rust for the bridge. Do not introduce Go.

## Crate map

| Path | Role |
|---|---|
| `crates/mailbox` | Bridge CLI binary |
| `crates/mailbox-protocol` | Shared publish/subscribe types |
| `crates/mailbox-harness` | Session wake / re-arm helpers |
| `adapters/` | External adapter processes |
| `docs/` | Numbered design notes (`00-`, `01-`, …) |

## Commands

Requires stable Rust via `rustup`. From the repo root:

```bash
cargo check --workspace
cargo test --workspace
cargo fmt --all
cargo clippy --workspace -- -D warnings
cargo run -p mailbox
```

Add crates with `cargo new` under `crates/` (or `adapters/` for adapter binaries) and register them in the workspace `Cargo.toml`. Add dependencies with `cargo add`, not by hand-editing version pins from memory.

## Coding conventions

Follow the mikey-in-a-box skills when they apply (architecture, type-driven design, testing, tooling, logging, coding workflow). In short:

- Comment on **why**, not what; link external constraints.
- Prefer types that make invalid states unrepresentable; business errors in `Result`, not panics/exceptions.
- Validate untrusted input at the edge (CLI args, adapter messages) before it reaches core logic.
- Push deterministic checks into tooling (`fmt`, `clippy`, tests) rather than relying on review.
- Keep PRs small and focused.

## Docs

| Doc | Use when |
|---|---|
| [docs/00-index.md](docs/00-index.md) | Starting point / repo map |
| [docs/01-wake-and-rearm.md](docs/01-wake-and-rearm.md) | Wake loop, delivery cursors, Claude vs Codex |
| [docs/02-tech-stack.md](docs/02-tech-stack.md) | Rust, subprocess→WASI, security process split |
