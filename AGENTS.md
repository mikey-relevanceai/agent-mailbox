# Repository guidelines for AI coding agents

Orientation for agents working in this repo. **Read [docs/00-index.md](docs/00-index.md) first** — it lists the design docs and when to use each.

## Keeping this file updated

When you change architecture, crate boundaries, commands, or agent-facing conventions, update this file and/or the relevant `docs/*.md` in the same change. Significant decisions also need an [ADR](docs/adr/README.md).

## What this is

Local durable **topic bus** for coding agents: adapters publish world-change events; the bridge stores them and kicks waiters; harness integrators wake idle sessions (Claude Code first via `asyncRewake`). Agents react — they do not poll, and they should not re-arm themselves.

```text
Adapters → publish → Bridge (topics + cursors + kicks) → harness wake → Agent
```

## Working agreements (must follow)

Full detail: [docs/03-working-agreements.md](docs/03-working-agreements.md).

1. **Branch + PR by default.** Do not push commits to `main` unless the human explicitly asks for that in the current request.
2. **ADRs** for durable decisions under `docs/adr/`.
3. **Design docs** for major systems/subsystems under `docs/design/`.
4. **mikey-in-a-box** — install and follow those skills (install steps in the working-agreements doc).

## Hard boundaries

- **Adapters never import bridge internals.** They speak the protocol only (CLI / socket today; WASI later). Live under `adapters/`.
- **Protocol vs transport.** Wire/domain types live in `mailbox-protocol`. How an adapter is hosted (`SubprocessTransport` now, `WasiTransport` later) is a replaceable host boundary — do not leak transport into protocol or storage.
- **Payload-free wake.** Kicks carry no model-visible body; events are read from the durable log. Wake is ingress, not authority.
- **Single-writer SQLite.** Only the `mailbox serve` daemon mutates the DB; other commands are Unix-socket clients ([ADR-0003](docs/adr/0003-single-writer-sqlite.md), [ADR-0004](docs/adr/0004-cli-serve-daemon-and-socket.md)). The daemon holds an exclusive `flock` (`<db-dir>/mailbox.lock`) for its whole life, so a second `serve` on the same DB fails loudly rather than becoming a second writer. `wait` is the sole read-only exception.
- **Bridge down fails loud.** Socket clients never auto-spawn the daemon and never open the DB directly; if `serve` is down they exit non-zero with "start it with `mailbox serve`" ([ADR-0004](docs/adr/0004-cli-serve-daemon-and-socket.md)).
- **Supervised adapters.** Long-running pollers are owned by the bridge (start/stop/idempotent, one per external entity, refcounted interest); agents must not leave naked background `gh` loops ([design/01](docs/design/01-mvp-github-watch.md)). Adapter *process* supervision is implemented (card 08): the `serve` daemon owns a `Supervisor` that starts an adapter when a watch gains interest and stops it on the last removal, backoff-restarts crashes, and does not resume orphan watches on restart. A finite adapter that exits **cleanly (code 0)** is treated as complete (marked `stopped`), NOT crash-restarted. The concrete adapter program is injected via a resolver; `serve`'s default `DefaultResolver` routes each watch kind to its adapter, so **both** `stub` (card 09) and `github-pr` (card 10) now spawn a real adapter. The stub binary is resolved via `MAILBOX_STUB_ADAPTER_BIN`, the github-pr poller via `MAILBOX_GH_ADAPTER_BIN` (absolute-path overrides), or the adapter co-installed beside the bridge binary. The `github-pr` adapter shells out to `gh` (overridable via `MAILBOX_GH_BIN` for tests) and is **edge-triggered**: it baselines on the first poll and fires only on transitions (conflict / reviews / CI). Its baseline persists **through the bridge** (never adapter-side SQLite, ADR-0001): the supervisor injects the last persisted baseline into the adapter's spawn config and relays the adapter's `Baseline` protocol messages back to `adapter_baseline`, so a restart does not re-fire ([design/01](docs/design/01-mvp-github-watch.md)).
- **v0 network:** no TCP listen. CLI and/or user-scoped Unix socket only.
- **Language:** Rust for the bridge. Do not introduce Go.

See [ADR-0001](docs/adr/0001-rust-bridge-subprocess-adapters.md), [ADR-0002](docs/adr/0002-mvp-crate-stack.md).

## Crate map

| Path | Role |
|---|---|
| `crates/mailbox` | Bridge CLI binary |
| `crates/mailbox-protocol` | Shared publish/subscribe types |
| `crates/mailbox-harness` | Session wake / re-arm helpers |
| `adapters/` | External adapter processes |
| `docs/` | Numbered notes, ADRs, designs |

## Commands

Requires stable Rust via `rustup`. From the repo root:

```bash
cargo fmt --all --check                                    # CI gate; drop --check to format
cargo clippy --workspace --all-targets -- -D warnings      # CI gate
cargo check --workspace                                    # CI gate
cargo test --workspace                                     # CI gate
cargo run -p mailbox -- --help                             # see the CLI surface
```

Add crates with `cargo new` under `crates/` (or `adapters/` for adapter binaries) and register them in the workspace `Cargo.toml`. Add dependencies with `cargo add`, not by hand-editing version pins from memory.

### `mailbox` CLI surface (settled in card 06 / [ADR-0004](docs/adr/0004-cli-serve-daemon-and-socket.md))

The `mailbox` binary is the single entry point. `serve` is the daemon; every
other command (except `wait`) is a one-shot Unix-socket client of it.

| Command | What it does |
|---|---|
| `mailbox serve` | Long-lived daemon: owns the single writer + waker, binds `<db-dir>/mailbox.sock` (0600). |
| `mailbox publish <topic> [--body <json>] [--adapter <id>]` | Publish an event (daemon stamps the timestamp). |
| `mailbox subscribe <topic> --session <id>` | Subscribe this session (baseline-on-subscribe). |
| `mailbox unsubscribe <topic> --session <id>` | Unsubscribe this session. |
| `mailbox read --session <id> [--limit <n>]` | Return unread events, advancing the cursor. |
| `mailbox watch github-pr <owner>/<repo>#<n> [--interval <secs>] --session <id>` | Record watch + this session's interest, subscribe to the PR topic, and spawn the edge-triggered `github-pr` poller (card 10). It polls via `gh` and publishes conflict / review / CI transitions; the watch reaches `running` with a pid. |
| `mailbox unwatch github-pr <owner>/<repo>#<n> --session <id>` | Drop this session's interest and unsubscribe. |
| `mailbox watch stub <label> [--interval-ms <n>] [--count <n>] --session <id>` | Record a `stub` watch + interest, subscribe to `stub.<label>`, and spawn the reference stub adapter (card 09), which publishes a synthetic event every `--interval-ms` (default 1000), `--count` times (`0`/default = forever). The one watch kind that spawns a real adapter today. |
| `mailbox unwatch stub <label> --session <id>` | Drop this session's interest in the stub watch and unsubscribe. |
| `mailbox status --session <id>` | Watches (interest counts, lifecycle state + child pid when the supervisor is running one) + this session's unread counts. Both `stub` and `github-pr` watches read `running` with a pid once their adapter is spawned. |
| `mailbox wait --session <id> [--max-block-ms <n>]` | Block until this session has mail, exit 2 (the `asyncRewake` contract). Direct read-only client — never uses the socket. With `--max-block-ms`, a block that elapses with no mail **re-execs a fresh waiter** (same PID) instead of returning — the self-respawn that keeps a long idle armed (card 11). |
| `mailbox harness arm [--max-block-ms <n>]` | The `SessionStart`/`Stop` hook target (`asyncRewake: true`). Reads `session_id` from the hook stdin JSON and launches the waiter **iff the session has subscriptions** — bridge-down/erroring or not-subscribed exit 0 without waking (fail-safe). It does NOT write the pidfile; the waiter does, after taking the single-waiter lock ([ADR-0006](docs/adr/0006-harness-self-respawn.md)). Logic lives in `mailbox-harness` (card 11). |
| `mailbox harness cleanup` | The `SessionEnd` hook target. Reaps the session's waiter (pidfile + `SIGTERM`) and drops its subscriptions + interests on the bridge, stopping any now-orphaned adapter (feeds the card-08 refcount — no zombie poller outlives the session). Retries a transient bridge failure, then defers to the card-08 TTL sweeper. |
| `mailbox harness install-hooks [--settings <path>] [--timeout-secs <n>] [--max-block-ms <n>] [--mailbox-bin <path>]` | Merge the Claude Code `settings.json` hooks snippet — `arm` (SessionStart/Stop) + `cleanup` (SessionEnd) — into `--settings <path>` if given (created if missing), else into `~/.claude/settings.json` **iff that file exists** (home from `AGENT_MAILBOX_HOME`, else `HOME` — the same resolution `install-skills` uses). With no such file (or no home) it **prints only** and says why: it must not conjure a `settings.json` on a machine with no Claude Code. The merge is atomic, idempotent and **non-destructive**: unrelated settings and foreign hooks are preserved; a settings file that cannot be read or parsed (permissions, non-UTF-8, bad JSON) is an **error**, never treated as absent and overwritten; a symlinked settings.json is written **through** (dotfiles link preserved); the publish is a compare-and-swap, so a concurrent Claude Code write is re-merged rather than lost; the file's mode is preserved; and the default path keeps a `.bak` of the pre-image. Re-running with a different `--mailbox-bin` / `--max-block-ms` **replaces** our hook groups (identified as `<bin> harness arm|cleanup`) instead of appending a stale second copy. The snippet is always printed too — in `--json` mode it IS stdout, and the note goes to stderr. One of the two setup commands. |
| `mailbox harness install-skills [--skills-dir <path>]` | Install the Claude Code skill(s) **embedded in the binary** (`include_str!` of `skills/agent-mailbox/SKILL.md`, so no checkout is needed at runtime) to `<skills-dir>/<name>/SKILL.md`, defaulting to `~/.claude/skills`. Atomic (temp + fsync + rename) and idempotent — reports `created` / `updated` / `unchanged` / `replaced-symlink` per skill. **Self-healing**: a corrupt, non-UTF-8, or unreadable `SKILL.md` is replaced rather than erroring (so no `--force` exists). The other setup command; it writes nothing else under `~/.claude`. |

Conventions: `--session <id>` wins over the `MAILBOX_SESSION_ID` env fallback.
Session identity is settled (card 11): it comes from the Claude Code hook stdin
JSON (`session_id`), which `mailbox harness arm`/`cleanup` read; `arm` then execs
`mailbox wait --session <id>` (which self-respawns carrying the same flag).
`--json` (global) makes any command emit
machine-readable JSON on stdout; logs always go to stderr so JSON stays clean.
When the daemon is down, socket clients fail loudly (non-zero, "start it with
`mailbox serve`") rather than auto-spawning or opening the DB.

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
| [docs/04-usage.md](docs/04-usage.md) | Install, hooks, the four-verb loop, `mailbox status` (getting started) |
| [docs/demo.md](docs/demo.md) | Runnable demo (`scripts/demo.sh`) + captured output; real-PR steps |
| [docs/migration-from-agent-ipc.md](docs/migration-from-agent-ipc.md) | Retiring the old `agent-ipc` skills; the replacement `skills/agent-mailbox` |
| [docs/01-wake-and-rearm.md](docs/01-wake-and-rearm.md) | Wake loop, delivery cursors, Claude vs Codex |
| [docs/02-tech-stack.md](docs/02-tech-stack.md) | Rust, subprocess→WASI, security process split |
| [docs/03-working-agreements.md](docs/03-working-agreements.md) | ADRs, designs, PRs, mikey-in-a-box install |
| [docs/adr/](docs/adr/README.md) | Decision log |
| [docs/design/](docs/design/README.md) | Subsystem designs |
| [docs/design/01-mvp-github-watch.md](docs/design/01-mvp-github-watch.md) | MVP GitHub watch / no zombie pollers (Implemented) |
