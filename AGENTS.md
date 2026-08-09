# Repository guidelines for AI coding agents

Orientation for agents working in this repo. **Read [docs/00-index.md](docs/00-index.md) first** — it lists the design docs and when to use each.

## Keeping this file updated

When you change architecture, crate boundaries, commands, or agent-facing conventions, update this file and/or the relevant `docs/*.md` in the same change. Significant decisions also need an [ADR](docs/adr/README.md).

**Durable learnings live in the repo, not in an agent's private memory.** A root cause, a constraint, a gotcha, or a decision that a future agent (or teammate) would benefit from goes in the relevant `docs/*.md` or an ADR — where everyone can see it — not in per-agent memory that no one else can read. If you learn something worth keeping, write it down here or under `docs/`.

## What this is

Local durable **topic bus** for coding agents: adapters publish world-change events; the bridge stores them and delivers each subscriber's wake — onto that session's Claude Code **inbox socket** where one exists, else by writing its **sentinel** for a `FileChanged` hook to turn into an `asyncRewake` exit 2. Agents react — they do not poll, and they should not re-arm themselves.

```text
Adapters → publish → Bridge (topics + cursors + wake delivery) ─┬→ inbox socket ────────────→ Agent
                                                                └→ sentinel → FileChanged ──→ Agent
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
- **The wake path is the daemon delivering, on one of two channels.** `publish` → the `serve` daemon looks the subscriber up in Claude Code's session registry (`~/.claude/sessions/*.json`) and either (a) writes its **inbox socket**, which starts a turn on an idle session, or (b) falls back to writing its **sentinel** (topic names only) for the `FileChanged` hook to answer with exit 2 ([ADR-0020](docs/adr/0020-peer-inbox-socket-is-the-wake-wire.md), over [ADR-0017](docs/adr/0017-daemon-bumps-the-sentinel.md)). There is **no per-session watcher process and no FIFO** — there was, and wakeability being an emergent property of six components was the reason agents kept going deaf. Do not reintroduce a hop between the daemon and either channel: the process that commits the event is the process that should deliver the wake.
- **The fallback is not dead code — do not delete it.** Whether a session binds an inbox socket is Claude Code's decision (the `agents_cross_session_inbox` gate), it differs between same-version sessions on one machine, and **nothing we can set turns it on**. Deleting the sentinel path would make wakeability depend on a flag the operator cannot control. It goes when the gate is universal, and not before.
- **The two channels are not equally forgiving, and the peer one has no anti-loop.** A sentinel write is only a TRIGGER — the woken hook re-reads the store and exits 0 when there is nothing unread, so a stray write costs a hook process and no model turn. On the peer channel **the message IS the turn**, with nothing in between to second-guess it. So never deliver a peer frame for an empty unread set, and never put anything on it you would not want to spend a turn on.
- **We claim no permission class on the peer channel.** The frame carries no `from-mode`. Claiming `bypass` is believed without verification and would reach a `bypassPermissions` receiver — but it is untrue, it makes the receiving agent believe a peer Claude session is speaking, and it is *also* what makes every permission-prompting receiver HOLD the message. The honest frame is the more deliverable one. A `bypassPermissions` fleet opts in with `mailbox harness install-inbound`; **`install-hooks` must never set `crossSessionInbound` implicitly.**
- **Wake arms level-triggered, then runs edge-triggered.** The steady-state wake is an edge (publish → sentinel write → `FileChanged` → exit 2), but **an edge only reaches an IDLE session** — one spent while the agent is mid-turn is lost, and the agent then goes idle deaf on top of unread mail. So every point that (re-)arms the wake path first checks the durable state instead of waiting for the next transition: `session-start` arms the sentinel from existing unread (and must do so BEFORE printing `watchPaths`, since a watch registered on a nonexistent path may never see its creation), and the `Stop` hook re-bumps the sentinel at the turn boundary when the session is sitting on unread mail ([ADR-0012](docs/adr/0012-level-triggered-wake-at-the-turn-boundary.md)). A re-trigger is bounded by an `event_row_id` watermark so each message buys at most one turn-boundary wake — the anti-loop. **Do not add a wake path that only reacts to a transition; it will be silently deaf to anything that happened while the agent was busy.**
- **Session liveness comes from the process table, never from a file — Claude Code's registry included.** A session's subscriptions, sentinel and interests all outlive the Claude Code process they belong to, and so does its `~/.claude/sessions/<pid>.json` entry (this machine had nineteen, spanning five days). The registry answers "where would I reach this session?", never "is it alive?" — treat a socket path from it as a delivery *address* whose existence is still worth checking, not as proof of anything. `doctor::live_claude_sessions` reads Claude Code's own argv (`--session-id` / `--resume`) and is the ONE liveness signal — used by `mailbox agents`, the TTL sweep's interest refresh, and the supervisor's resume/retry ([ADR-0017](docs/adr/0017-daemon-bumps-the-sentinel.md), superseding [ADR-0009](docs/adr/0009-interest-liveness-from-the-waiter-pidfile.md)). Read it ONCE per operation — it shells out to `ps`.
- **Single-writer SQLite.** Only the `mailbox serve` daemon mutates the DB; other commands are Unix-socket clients ([ADR-0003](docs/adr/0003-single-writer-sqlite.md), [ADR-0004](docs/adr/0004-cli-serve-daemon-and-socket.md)). The daemon holds an exclusive `flock` (`<db-dir>/mailbox.lock`) for its whole life, so a second `serve` on the same DB fails loudly rather than becoming a second writer. The read-only exceptions are `doctor` (a health check must work when the daemon is the broken thing — [ADR-0016](docs/adr/0016-prove-wakeability-with-an-active-probe.md)) and the `harness wake` hook; ADR-0004's original `wait` carve-out is void, because `wait` was deleted with the waiter.
- **Bridge down fails loud.** Socket clients never auto-spawn the daemon and never open the DB directly; if `serve` is down they exit non-zero with "start it with `mailbox serve`" ([ADR-0004](docs/adr/0004-cli-serve-daemon-and-socket.md)).
- **Supervised adapters.** Long-running pollers are owned by the bridge (start/stop/idempotent, one per external entity, refcounted interest); agents must not leave naked background `gh` loops ([design/01](docs/design/01-mvp-github-watch.md)). Adapter *process* supervision is implemented (card 08): the `serve` daemon owns a `Supervisor` that starts an adapter when a watch gains interest and stops it on the last removal, backoff-restarts crashes, and on daemon restart resumes a watch iff an interested session is still alive — proved by that session's live Claude Code process ([ADR-0010](docs/adr/0010-resume-watches-on-restart.md)); a watch whose sessions are all gone is not resumed (no zombie poller). After N consecutive crashes it gives up (marks the watch `failed`, surfaces an error event), but give-up is **not permanent**: the periodic sweep retries a `failed` watch once per interval while an interested session is still alive ([ADR-0011](docs/adr/0011-retry-failed-watches-on-sweep.md)), so a give-up caused by a transient upstream outage self-heals rather than needing a manual re-`watch`. A finite adapter that exits **cleanly (code 0)** is treated as complete (marked `stopped`), NOT crash-restarted. The concrete adapter program is injected via a resolver; `serve`'s default `DefaultResolver` routes each watch kind to its adapter, so **both** `stub` (card 09) and `github-pr` (card 10) now spawn a real adapter. The stub binary is resolved via `MAILBOX_STUB_ADAPTER_BIN`, the github-pr poller via `MAILBOX_GH_ADAPTER_BIN` (absolute-path overrides), or the adapter co-installed beside the bridge binary. The `github-pr` adapter shells out to `gh` (overridable via `MAILBOX_GH_BIN` for tests) and is **edge-triggered**: it baselines on the first poll and fires only on transitions (merge / conflict / reviews / CI). Its baseline persists **through the bridge** (never adapter-side SQLite, ADR-0001): the supervisor injects the last persisted baseline into the adapter's spawn config and relays the adapter's `Baseline` protocol messages back to `adapter_baseline`, so a restart does not re-fire ([design/01](docs/design/01-mvp-github-watch.md)).
- **v0 network:** no TCP listen. CLI and/or user-scoped Unix socket only.
- **Language:** Rust for the bridge. Do not introduce Go.

See [ADR-0001](docs/adr/0001-rust-bridge-subprocess-adapters.md), [ADR-0002](docs/adr/0002-mvp-crate-stack.md).

## Crate map

| Path | Role |
|---|---|
| `crates/mailbox` | Bridge CLI binary |
| `crates/mailbox-protocol` | Shared publish/subscribe types |
| `crates/mailbox-harness` | Claude Code integration: hooks + skills install, hook payload parse |
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
other command except `doctor` is a one-shot Unix-socket client of it (`doctor`
opens the sentinel files, not the store, and needs no daemon at all).

| Command | What it does |
|---|---|
| `mailbox serve` | Long-lived daemon: owns the single writer + waker, binds `<db-dir>/mailbox.sock` (0600). |
| `mailbox publish <topic> [--body <json>] [--adapter <id>]` | Publish an event (daemon stamps the timestamp). **One rule** ([ADR-0018](docs/adr/0018-publish-has-one-rule.md)): the event goes to the topic and wakes every subscriber, **its author included** ([ADR-0014](docs/adr/0014-self-authored-events-wake-their-author.md)). It resolves no caller — an adapter, an agent and a script an agent spawned are identical here — so there is no refusal, no `--no-session`, and no author recorded on the event. |
| `mailbox subscribe <topic>` | Subscribe this session (baseline-on-subscribe). |
| `mailbox unsubscribe <topic>` | Unsubscribe this session. |
| `mailbox read [--limit <n>]` | Return unread events, advancing the cursor. |
| `mailbox watch github-pr <owner>/<repo>#<n> [--interval <secs>]` | Record watch + this session's interest, subscribe to the PR topic, and spawn the edge-triggered `github-pr` poller (card 10). It polls via `gh` and publishes merge / conflict / review / CI transitions; the watch reaches `running` with a pid. |
| `mailbox unwatch github-pr <owner>/<repo>#<n>` | Drop this session's interest and unsubscribe. |
| `mailbox watch stub <label> [--interval-ms <n>] [--count <n>]` | Record a `stub` watch + interest, subscribe to `stub.<label>`, and spawn the reference stub adapter (card 09), which publishes a synthetic event every `--interval-ms` (default 1000), `--count` times (`0`/default = forever). The one watch kind that spawns a real adapter today. |
| `mailbox unwatch stub <label>` | Drop this session's interest in the stub watch and unsubscribe. |
| `mailbox send <target> [--text <s>] [--body <json>]` | Message a peer agent: publish to its `agent.<id>` inbox with `from` stamped by the bridge. `<target>` is a bare session id or the full `agent.*` topic. Refuses an unregistered inbox rather than dropping the message into a void ([ADR-0007](docs/adr/0007-always-on-agent-inboxes.md)); there is no `--force`. The ONLY writer of an inbox topic — generic `publish` refuses `agent.*`. **The caller's session is OPTIONAL**: it is only the reply address, so a HUMAN at a terminal can poke an agent, and that message is stored with the `from` key **absent** (never a placeholder, and any caller-supplied `from` is stripped). Receivers must treat `from` as optional — no `from`, no reply. |
| `mailbox agents` | List the sessions with a registered inbox (who you can `send` to), each with `running` / `not running` read from the process table. `running` means the agent EXISTS, not that it can be woken — only `doctor` proves that. **The caller's session is OPTIONAL**: it only decides which row is marked `is_self`, so with no session every agent is listed and no row is marked. |
| `mailbox topics [--prefix <p>]` | List known topics with subscriber and event counts. A topic exists because something subscribed or published to it. |
| `mailbox status` | **Who this session is** (id + inbox topic — there is no separate `whoami`; the identity half is derived locally, so it still answers with the bridge down, saying `bridge: UNREACHABLE` and still exiting non-zero), plus watches (interest counts, lifecycle state + child pid when the supervisor is running one), this session's subscriptions and unread counts. Both `stub` and `github-pr` watches read `running` with a pid once their adapter is spawned. The subscription list is summarised by a `subscription_count` scalar (`subscriptions (N):` on the human line) so a Claude Code **status line** can read one number per prompt instead of measuring the list; it counts the session's own inbox topic, so an armed session with no watches reads 1. |
| `mailbox doctor [--all] [--json] [--session ID] [--timeout-ms N]` | Actively PROVE which sessions can be woken right now ([ADR-0016](docs/adr/0016-prove-wakeability-with-an-active-probe.md)): bumps each sentinel content-preservingly and requires the `FileChanged` hook to answer (it stamps `.mailbox-hook-ran` on every run). Verdicts: `wakeable` (positive proof), `deaf` (live, armed, IDLE and silent — **the fault**), `busy` (mid-turn, so it could not have answered — reported as **UNMEASURED**, an open question, NOT a clean bill of health: a deaf session that happens to be busy looks exactly like this, so re-probe it while idle), `gone` (no live Claude Code process — normal, NOT a fault), `never_armed`, `undetermined`. The summary line counts three buckets — `N wakeable, N deaf, N UNMEASURED`. **Exits 1 if any session is deaf**, so a supervisor can gate on it. **A session cannot measure itself**: running the command IS a turn, so the caller is busy by construction and can only ever report itself UNMEASURED — `doctor` warns about this on stderr rather than letting an agent auditing its own fleet read its blind spot as health. Bumps the whole fleet before polling any of it, so one deaf session is distinguishable from one bad moment. Wakeability is PERISHABLE — a session that answers today can be deaf tomorrow — so re-run it rather than trusting an old result. Read-only and daemon-free. |
| `mailbox harness cleanup` | The `SessionEnd` hook target. Removes the session's wake sentinel and drops its subscriptions + interests on the bridge, stopping any now-orphaned adapter (feeds the card-08 refcount — no zombie poller outlives the session). Retries a transient bridge failure, then defers to the card-08 TTL sweeper. |
| `mailbox harness install-hooks [--settings <path>] [--timeout-secs <n>] [--mailbox-bin <path>]` | Merge the Claude Code `settings.json` hooks snippet — the on-demand-wake set: `session-start` (SessionStart, matcher `""` = **all sources**, so it re-fires on resume — ADR-0013), `turn-end` (Stop), `turn-start` (UserPromptSubmit), `wake` (FileChanged, `asyncRewake`), `cleanup` (SessionEnd) — into `--settings <path>` if given (created if missing), else into `~/.claude/settings.json` **iff that file exists** (home from `AGENT_MAILBOX_HOME`, else `HOME` — the same resolution `install-skills` uses). With no such file (or no home) it **prints only** and says why: it must not conjure a `settings.json` on a machine with no Claude Code. The merge is atomic, idempotent and **non-destructive**: unrelated settings and foreign hooks are preserved; a settings file that cannot be read or parsed (permissions, non-UTF-8, bad JSON) is an **error**, never treated as absent and overwritten; a symlinked settings.json is written **through** (dotfiles link preserved); the publish is a compare-and-swap, so a concurrent Claude Code write is re-merged rather than lost; the file's mode is preserved; and the default path keeps a `.bak` of the pre-image. Re-running with a different `--mailbox-bin` **replaces** our hook groups (recognised as `<bin> harness <our-subcommand>`, and an upgrade sweeps the retired ADR-0006 `arm` hooks) instead of appending a stale second copy. The snippet is always printed too — in `--json` mode it IS stdout, and the note goes to stderr. One of the two setup commands. |
| `mailbox harness install-skills [--skills-dir <path>]` | Install the Claude Code skill(s) **embedded in the binary** (`include_str!` of `skills/agent-mailbox/SKILL.md`, so no checkout is needed at runtime) to `<skills-dir>/<name>/SKILL.md`, defaulting to `~/.claude/skills`. Atomic (temp + fsync + rename) and idempotent — reports `created` / `updated` / `unchanged` / `replaced-symlink` per skill. **Self-healing**: a corrupt, non-UTF-8, or unreadable `SKILL.md` is replaced rather than erroring (so no `--force` exists). The other setup command; it writes nothing else under `~/.claude`. |
| `mailbox harness install-inbound [--settings <path>]` | **Opt-in, never implicit.** Set Claude Code's `crossSessionInbound: "accept"` so a `bypassPermissions` session can RECEIVE a peer-channel wake instead of holding it for an approval nobody is there to give (and dropping it after ~5 minutes). Same atomic, non-destructive settings merge as `install-hooks` — unrelated settings preserved, symlink written through, compare-and-swap publish, `.bak` on the default path — and it writes exactly one key, never a hook. Idempotent, and it reports what it found before changing it. It prints plainly what was widened: that session class then takes messages from any process running as the same user with no prompt, which is what lets the bridge wake an agent that acts without asking, and equally lets anything else running as you direct it. Prefer a per-session `--settings` file over user settings. **Do not run this on a user's behalf without them asking.** |

Conventions: **a session's own identity comes from `$CLAUDE_CODE_SESSION_ID` and
nowhere else** — no `--session` flag, no `MAILBOX_SESSION_ID`. Nothing in production
acts *as* another session, and the flag's main effect on agents was to let them bind a
phantom empty session via `--session "$MAILBOX_SESSION_ID"`. To run a command as a
named session by hand, prefix it: `CLAUDE_CODE_SESSION_ID=<id> mailbox status`.
(`mailbox doctor --session <id>` is a different thing and survives: it names a session
to *probe*.) The hooks are unaffected — they read `session_id` from the Claude Code
hook stdin JSON, which `mailbox harness session-start`/`turn-end`/`cleanup` parse;
`session-start` then arms that session's wake sentinel.

**Requiring a session is per-command, and the test is "does this command need to know
whose?"** — not "is it a socket client?".

- **REQUIRED** for the genuinely session-scoped commands, where the caller *is* the
  subject: `read` (my unread, advances my cursor), `status` (my state), `subscribe`,
  `unsubscribe`, `watch`, `unwatch`. Without an identity these have no meaning, so
  they fail with an error naming the variable.
- **OPTIONAL** where the identity is a courtesy the command adds on the way: `send`
  (stamps a reply address) and `agents` (marks which row is you). Both work with no
  session, which is what makes the **human manual-poke workflow** possible —
  `mailbox agents` to look, `mailbox send <id> --text …` to poke, from an ordinary
  terminal with no `CLAUDE_CODE_SESSION_ID` at all. Advising a human to invent one
  for these would be nonsense: they are not acting *as* anyone.
- **NOT RESOLVED AT ALL** by `publish` (every publisher is the same publisher,
  [ADR-0018](docs/adr/0018-publish-has-one-rule.md)), `topics` (a global read), and
  `doctor` (it probes *named* sessions; it reads the ambient one only to warn that a
  caller cannot measure itself).

A command that resolves a session it does not use is a bug of this class: it refuses
a workflow to enforce a field it then ignores. Adding one, check which bucket it is in.
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
| [docs/01-wake.md](docs/01-wake.md) | The wake path (inbox socket + sentinel fallback), the inbound permission gate, delivery cursors, Claude vs Codex |
| [docs/02-tech-stack.md](docs/02-tech-stack.md) | Rust, subprocess→WASI, security process split |
| [docs/03-working-agreements.md](docs/03-working-agreements.md) | ADRs, designs, PRs, mikey-in-a-box install |
| [docs/adr/](docs/adr/README.md) | Decision log |
| [docs/design/](docs/design/README.md) | Subsystem designs |
| [docs/design/01-mvp-github-watch.md](docs/design/01-mvp-github-watch.md) | MVP GitHub watch / no zombie pollers (Implemented) |
