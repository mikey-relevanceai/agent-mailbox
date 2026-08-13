# Repository guidelines for AI coding agents

Orientation for agents working in this repo. **Read [docs/00-index.md](docs/00-index.md) first** — it lists the design docs and when to use each.

## Keeping this file updated

When you change architecture, crate boundaries, commands, or agent-facing conventions, update this file and/or the relevant `docs/*.md` in the same change. Significant decisions also need an [ADR](docs/adr/README.md).

**Durable learnings live in the repo, not in an agent's private memory.** A root cause, a constraint, a gotcha, or a decision that a future agent (or teammate) would benefit from goes in the relevant `docs/*.md` or an ADR — where everyone can see it — not in per-agent memory that no one else can read. If you learn something worth keeping, write it down here or under `docs/`.

## What this is

Local durable **topic bus** for coding agents: adapters publish world-change events; the bridge stores them and writes each subscriber's wake to that session's Claude Code **inbox socket**, which makes an idle session take a turn. Agents react — they do not poll, and they do not arm anything.

```text
Adapters → publish → Bridge (topics + cursors + wake delivery) → inbox socket → Agent
```

Requires **Claude Code 2.1.226+**. A session Claude Code bound no inbox socket cannot be woken by anything.

## Working agreements (must follow)

Full detail: [docs/03-working-agreements.md](docs/03-working-agreements.md).

1. **Branch + PR by default.** Do not push commits to `main` unless the human explicitly asks for that in the current request.
2. **ADRs** for durable decisions under `docs/adr/`.
3. **Design docs** for major systems/subsystems under `docs/design/`.
4. **mikey-in-a-box** — install and follow those skills (install steps in the working-agreements doc).

## Hard boundaries

- **Adapters never import bridge internals.** They speak the protocol only (CLI / socket today; WASI later). Live under `adapters/`.
- **Protocol vs transport.** Wire/domain types live in `mailbox-protocol`. How an adapter is hosted (`SubprocessTransport` now, `WasiTransport` later) is a replaceable host boundary — do not leak transport into protocol or storage.
- **Pointer, not payload.** A wake carries topic names, unread counts, and each event's `subject` — one bounded, single-line description its publisher wrote, plus an optional link ([ADR-0022](docs/adr/0022-the-wake-carries-a-subject.md)). It carries **no body, ever**: bodies are read from the durable log. Wake is ingress, not authority. A subject NAMES the change and points at it (`new comment` + its permalink, `CI failed: build` + the failing run); it never quotes content. `Subject` is a parsed type for exactly this reason — the wake wire's shape must not depend on adapters behaving.
- **Every wake frame opens with `[agent-mailbox]`.** It is how a woken agent knows which system started its turn, and therefore which skill to load. Nothing after it may forge another one, which is why a subject may not contain a newline.
- **The wake path is one hop.** `publish` → the `serve` daemon looks the subscriber up in Claude Code's session registry (`~/.claude/sessions/*.json`) and writes its **inbox socket**; Claude Code starts a turn on an idle session ([ADR-0021](docs/adr/0021-delete-the-sentinel-fallback.md), [ADR-0020](docs/adr/0020-peer-inbox-socket-is-the-wake-wire.md)). There is **no sentinel, no `watchPaths`, no `FileChanged` hook, no exit-2 wire and nothing per-session on our side**. Do not reintroduce a hop between the daemon and the socket: the process that commits the event is the process that should deliver the wake.
- **Do not rebuild a fallback wake path.** One existed — a sentinel file plus a `FileChanged` hook — and it was deleted after being watched fail silently with everything configured correctly (ADR-0021 has the evidence). Every silent-deafness bug this project has had lived in it. If the socket is unavailable, the correct behaviour is to say so loudly, which is what `doctor` and the `subscribe`/`watch` refusal do.
- **Delivery IS the turn, so nothing may be sent idly.** There is no anti-loop between the socket and the model — no hook re-reads the store and exits 0 for you. Never deliver a wake for an empty unread set, and never put anything on that wire you would not spend a model turn on.
- **We claim no permission class.** The frame carries no `from-mode`. Claiming `bypass` is believed without verification and would reach a `bypassPermissions` receiver — but it is untrue, it makes the receiving agent believe a peer Claude session is speaking, and it is *also* what makes every permission-prompting receiver HOLD the message. The honest frame is the more deliverable one. A `bypassPermissions` fleet opts in with `mailbox harness install-inbound`; **`install-hooks` must never set `crossSessionInbound` implicitly.**
- **Tell an agent it cannot be woken while it can still hear you.** `subscribe` and `watch` refuse when Claude Code registered the calling session and gave it no socket. A warning on stderr in an unattended session is read by nobody, which is indistinguishable from the silent failure it replaces. Refuse only on unambiguous evidence: an unreadable registry, or a session Claude Code never registered, is not proof of anything.
- **Session liveness comes from the process table, never from a file — Claude Code's registry included.** A session's subscriptions and interests outlive the Claude Code process they belong to, and so does its `~/.claude/sessions/<pid>.json` entry (this machine had nineteen, spanning five days). The registry answers "where would I reach this session?", never "is it alive?" — treat a socket path from it as a delivery *address* whose existence is still worth checking, not as proof of anything. `doctor::live_claude_sessions` reads Claude Code's registry and confirms each entry against the process table; it is the ONE liveness signal, used by `mailbox agents`, the TTL sweep's interest refresh, and the supervisor's resume/retry ([ADR-0021](docs/adr/0021-delete-the-sentinel-fallback.md), over [ADR-0017](docs/adr/0017-daemon-bumps-the-sentinel.md) and [ADR-0009](docs/adr/0009-interest-liveness-from-the-waiter-pidfile.md)). Read it ONCE per operation.
- **Single-writer SQLite.** Only the `mailbox serve` daemon mutates the DB; other commands are Unix-socket clients ([ADR-0003](docs/adr/0003-single-writer-sqlite.md), [ADR-0004](docs/adr/0004-cli-serve-daemon-and-socket.md)). The daemon holds an exclusive `flock` (`<db-dir>/mailbox.lock`) for its whole life, so a second `serve` on the same DB fails loudly rather than becoming a second writer. The one read-only exception is `doctor`, and it opens no store at all any more — it reads Claude Code's registry and the process table ([ADR-0021](docs/adr/0021-delete-the-sentinel-fallback.md)), because a health check must work when the daemon is the broken thing. ADR-0004's original `wait` carve-out is void.
- **Bridge down fails loud.** Socket clients never auto-spawn the daemon and never open the DB directly; if `serve` is down they exit non-zero with "start it with `mailbox serve`" ([ADR-0004](docs/adr/0004-cli-serve-daemon-and-socket.md)).
- **Supervised adapters.** Long-running pollers are owned by the bridge (start/stop/idempotent, one per external entity, refcounted interest); agents must not leave naked background `gh` loops ([design/01](docs/design/01-mvp-github-watch.md)). Adapter *process* supervision is implemented (card 08): the `serve` daemon owns a `Supervisor` that starts an adapter when a watch gains interest and stops it on the last removal, backoff-restarts crashes, and on daemon restart resumes a watch iff an interested session is still alive — proved by that session's live Claude Code process ([ADR-0010](docs/adr/0010-resume-watches-on-restart.md)); a watch whose sessions are all gone is not resumed (no zombie poller). After N consecutive crashes it gives up (marks the watch `failed`, surfaces an error event), but give-up is **not permanent**: the periodic sweep retries a `failed` watch once per interval while an interested session is still alive ([ADR-0011](docs/adr/0011-retry-failed-watches-on-sweep.md)), so a give-up caused by a transient upstream outage self-heals rather than needing a manual re-`watch`. That give-up is announced **once per outage**, not once per retry ([ADR-0023](docs/adr/0023-one-give-up-notice-per-outage.md)): a retry that gives up again is the same outage continuing, so it publishes nothing, and the notice is withdrawn by one `adapter_recovered` event when an adapter has stayed up for the stable-run threshold. The two together are what stop a lost network — which fails every watch at once — waking every subscribed agent once per sweep interval for as long as it is down. A finite adapter that exits **cleanly (code 0)** is treated as complete (marked `stopped`), NOT crash-restarted. The concrete adapter program is injected via a resolver; `serve`'s default `DefaultResolver` routes each watch kind to its adapter, so **both** `stub` (card 09) and `github-pr` (card 10) now spawn a real adapter. The stub binary is resolved via `MAILBOX_STUB_ADAPTER_BIN`, the github-pr poller via `MAILBOX_GH_ADAPTER_BIN` (absolute-path overrides), or the adapter co-installed beside the bridge binary. The `github-pr` adapter shells out to `gh` (overridable via `MAILBOX_GH_BIN` for tests) and is **edge-triggered**: it baselines on the first poll and fires only on transitions (merge / conflict / reviews / CI). Its baseline persists **through the bridge** (never adapter-side SQLite, ADR-0001): the supervisor injects the last persisted baseline into the adapter's spawn config and relays the adapter's `Baseline` protocol messages back to `adapter_baseline`, so a restart does not re-fire ([design/01](docs/design/01-mvp-github-watch.md)).
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
reads Claude Code's registry and the process table, not the store, and needs no daemon at all).

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
| `mailbox status` | **Who this session is, and whether anything can wake it** (id + `wake:` verdict + inbox topic — there is no separate `whoami`; this identity half is derived locally, so it still answers with the bridge down, saying `bridge: UNREACHABLE` and still exiting non-zero), plus watches (interest counts, lifecycle state + child pid when the supervisor is running one), this session's subscriptions and unread counts. Both `stub` and `github-pr` watches read `running` with a pid once their adapter is spawned. The subscription list is summarised by a `subscription_count` scalar (`subscriptions (N):` on the human line) so a Claude Code **status line** can read one number per prompt instead of measuring the list; it counts the session's own inbox topic, so an armed session with no watches reads 1. The `wake:` line is the SAME `doctor::reachability_of` verdict `subscribe`/`watch` refuse on ([ADR-0024](docs/adr/0024-status-reports-the-wake-verdict.md)) — an unreadable registry reads `unknown`, never a verdict — so the self-check can never contradict the refusal. The human `inbox:` label became `inbox topic:` there; the `inbox` **JSON key is unchanged**, because that status line reads it. **The caller's session is OPTIONAL**: the watch table is bridge-global — no session predicate, interest summed across every session — so a human at a terminal gets it with no `CLAUDE_CODE_SESSION_ID` at all, and is told in one line why the rest is missing. With no session the session half is **absent**, never blanked: `session`, `wake`, `inbox`, `subscriptions`, `subscription_count` and `unread` are omitted from `--json` rather than emitted as null or `0`, because a zero there answers "how many topics am I on?" for a caller who is nobody. With a session the document is unchanged — the half is `#[serde(flatten)]`ed, so `.subscription_count` stays where that status line reads it. |
| `mailbox doctor [--all] [--json] [--session ID]` | Report which sessions can be woken ([ADR-0021](docs/adr/0021-delete-the-sentinel-fallback.md)). A **read**, not a probe: reachability is two readable facts — is the process alive, and did Claude Code bind it an inbox socket. Verdicts: `reachable`, `no-inbox` (live but nothing can wake it — **the fault**, and only a session restart fixes it), `gone` (no live process — normal, NOT a fault). **Exits 1 if any session is a fault**, so a supervisor can gate on it. Unlike the probe it replaced, a session CAN measure itself, and there is no `busy` verdict — a mid-turn session is just as reachable, because a message queues rather than needing an edge to land on. Read-only and daemon-free. |
| `mailbox harness cleanup` | The `SessionEnd` hook target. Drops the session's subscriptions + interests on the bridge, stopping any now-orphaned adapter (feeds the card-08 refcount — no zombie poller outlives the session). Retries a transient bridge failure, then defers to the card-08 TTL sweeper. |
| `mailbox harness install-hooks [--settings <path>] [--mailbox-bin <path>]` | Merge the Claude Code `settings.json` hooks snippet — **two** hooks, and neither can wake a session: `session-start` (SessionStart, matcher `""` = **all sources**, so it re-fires on resume — ADR-0013) and `cleanup` (SessionEnd) — into `--settings <path>` if given (created if missing), else into `~/.claude/settings.json` **iff that file exists** (home from `AGENT_MAILBOX_HOME`, else `HOME`). With no such file (or no home) it **prints only** and says why: it must not conjure a `settings.json` on a machine with no Claude Code. The merge is atomic, idempotent and **non-destructive**: unrelated settings and foreign hooks are preserved; a settings file that cannot be read or parsed is an **error**, never treated as absent and overwritten; a symlinked settings.json is written **through**; the publish is a compare-and-swap, so a concurrent Claude Code write is re-merged rather than lost; the mode is preserved; the default path keeps a `.bak`. A re-run **replaces** our hook groups and sweeps every hook name we have ever installed, including the retired `wake`/`turn-end`/`turn-start` set (ADR-0021), so an upgrade cannot leave an exit-2 wake firing at a subcommand that no longer exists. **Never touches `crossSessionInbound`.** |
| `mailbox harness install-skills [--skills-dir <path>]` | Install the Claude Code skill(s) **embedded in the binary** (`include_str!` of `skills/agent-mailbox/SKILL.md`, so no checkout is needed at runtime) to `<skills-dir>/<name>/SKILL.md`, defaulting to `~/.claude/skills`. Atomic (temp + fsync + rename) and idempotent — reports `created` / `updated` / `unchanged` / `replaced-symlink` per skill. **Self-healing**: a corrupt, non-UTF-8, or unreadable `SKILL.md` is replaced rather than erroring (so no `--force` exists). The other setup command; it writes nothing else under `~/.claude`. |
| `mailbox harness install-inbound [--settings <path>]` | **Opt-in, never implicit.** Set Claude Code's `crossSessionInbound: "accept"` so a `bypassPermissions` session can RECEIVE a peer-channel wake instead of holding it for an approval nobody is there to give (and dropping it after ~5 minutes). Same atomic, non-destructive settings merge as `install-hooks` — unrelated settings preserved, symlink written through, compare-and-swap publish, `.bak` on the default path — and it writes exactly one key, never a hook. Idempotent, and it reports what it found before changing it. It prints plainly what was widened: that session class then takes messages from any process running as the same user with no prompt, which is what lets the bridge wake an agent that acts without asking, and equally lets anything else running as you direct it. Prefer a per-session `--settings` file over user settings. **Do not run this on a user's behalf without them asking.** |

Conventions: **a session's own identity comes from `$CLAUDE_CODE_SESSION_ID` and
nowhere else** — no `--session` flag, no `MAILBOX_SESSION_ID`. Nothing in production
acts *as* another session, and the flag's main effect on agents was to let them bind a
phantom empty session via `--session "$MAILBOX_SESSION_ID"`. To run a command as a
named session by hand, prefix it: `CLAUDE_CODE_SESSION_ID=<id> mailbox status`.
(`mailbox doctor --session <id>` is a different thing and survives: it names a session
to *probe*.) The hooks are unaffected — they read `session_id` from the Claude Code
hook stdin JSON, which `mailbox harness session-start`/`cleanup` parse;
`session-start` then registers that session's always-on agent inbox.

**Requiring a session is per-command, and the test is "does this command need to know
whose?"** — not "is it a socket client?".

- **REQUIRED** for the genuinely session-scoped commands, where the caller *is* the
  subject: `read` (my unread, advances my cursor), `subscribe`, `unsubscribe`,
  `watch`, `unwatch`. Without an identity these have no meaning, so they fail with an
  error naming the variable.
- **OPTIONAL** where the identity buys an extra rather than the answer: `send`
  (stamps a reply address), `agents` (marks which row is you), and `status` (adds
  your own half to a bridge-global watch table). All three work with no session, which
  is what makes the **human workflow** possible — `mailbox status` to see what the
  bridge is doing, `mailbox agents` to look,
  `mailbox send <id> --text …` to poke, from an ordinary terminal with no
  `CLAUDE_CODE_SESSION_ID` at all. Advising a human to invent one for these would be
  nonsense: they are not acting *as* anyone, and for `status` an invented id also
  buys three confident answers about a session that has never existed.
- **NOT RESOLVED AT ALL** by `publish` (every publisher is the same publisher,
  [ADR-0018](docs/adr/0018-publish-has-one-rule.md)), `topics` (a global read), and
  `doctor` (it probes *named* sessions, and since [ADR-0021](docs/adr/0021-delete-the-sentinel-fallback.md)
  deleted the probe it does not read the ambient one at all — a session can measure
  itself).

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
| [docs/01-wake.md](docs/01-wake.md) | The wake path (the inbox socket, the registry, the two hooks), the inbound permission gate, delivery cursors, other harnesses |
| [docs/02-tech-stack.md](docs/02-tech-stack.md) | Rust, subprocess→WASI, security process split |
| [docs/03-working-agreements.md](docs/03-working-agreements.md) | ADRs, designs, PRs, mikey-in-a-box install |
| [docs/adr/](docs/adr/README.md) | Decision log |
| [docs/design/](docs/design/README.md) | Subsystem designs |
| [docs/design/01-mvp-github-watch.md](docs/design/01-mvp-github-watch.md) | MVP GitHub watch / no zombie pollers (Implemented) |
