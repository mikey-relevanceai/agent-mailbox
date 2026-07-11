# Design: MVP — GitHub PR watch (agent-ipc parity)

- Status: Draft
- Related ADRs: [0001](../adr/0001-rust-bridge-subprocess-adapters.md),
  [0002](../adr/0002-mvp-crate-stack.md),
  [0003](../adr/0003-single-writer-sqlite.md)
- Related: [01-wake-and-rearm](../01-wake-and-rearm.md), prior skill
  `agent-ipc` / `agent-ipc-github`

> **Card 09 status note.** The `stub` watch kind + its resolver landed as the
> **reference implementation** of this design: a trivial adapter (`stub.<label>`,
> `mailbox-stub-adapter`) that publishes a synthetic edge on an interval, driving
> the full supervised path (watch → interest → spawn → publish → wake → read) end
> to end.
>
> **Card 10 status note — IMPLEMENTED.** The real `github-pr` poller
> (`mailbox-github-pr-adapter`) now ships and `serve`'s `DefaultResolver` spawns it
> for `github-pr` watches. It polls a PR via `gh` (injectable via `MAILBOX_GH_BIN`
> for tests) and is **edge-triggered**: it baselines on the first poll and fires
> only on transitions — mergeable → `CONFLICTING` (ignoring transient `UNKNOWN`),
> new reviews / review threads / PR comments (diffed by highest-seen **id**, not a
> bare count, so an add is never missed when a concurrent delete cancels the
> count), and whole-PR **CI rollup** transitions **into failure** (event body lists
> the newly-failed check names). The baseline
> **persists through the bridge via the protocol** (never adapter-side SQLite —
> ADR-0001): the supervisor injects the last persisted baseline into the adapter's
> spawn config and relays the adapter's new `Baseline` protocol messages to the
> `adapter_baseline` table, so a restart does not re-fire. Delivery is
> **at-least-once across an ungraceful termination** (SIGKILL/OOM/power-loss in the
> window between publishing an edge and persisting the baseline) — a re-fired edge
> is a duplicate wake, tolerable for a wake bus; graceful shutdown flushes the
> baseline first. See the acceptance tests
> in `adapters/github-pr-adapter/tests/adapter_e2e.rs` (ac-10-1…4) and the
> round-trip-through-the-bridge test in `crates/mailbox/tests/github_pr_e2e.rs`.

## Goal

Replace the existing skill-based IPC loop for **GitHub PR watching**: wake idle
Claude Code sessions when a watched PR changes in ways agents care about —
without the agent re-arming, and **without zombie GitHub pollers**.

Parity with `agent-ipc-github`, plus CI:

- Edge-triggered (baseline on first poll; fire on transitions only)
- Watch mergeable → `CONFLICTING` (ignore transient `UNKNOWN`)
- Watch new reviews / review threads / PR comments
- Watch **CI / check-run status** transitions (e.g. pending → failure, or newly
  failing checks) so agents can react without a separate poll skill
- Persist watcher baseline so restarts don’t re-fire
- Default poll interval ~60s

## Non-goals (MVP)

- Multi-host / cloud bus
- WASI adapters
- Codex `asyncRewake` parity (manual arm fallback only)
- Full peer chat-room product (topics should allow it later; not the MVP demo)

## Shape

```text
┌─────────────────┐  publish(topic, event)   ┌──────────────────┐
│ github-pr       │ ───────────────────────► │ mailbox bridge   │
│ adapter         │                          │ (single SQLite   │
│ (subprocess)    │  supervised by bridge    │  writer)         │
└─────────────────┘                          └────────┬─────────┘
                                                      │ kick
                                                      ▼
                                             ┌──────────────────┐
                                             │ Claude harness   │
                                             │ asyncRewake      │
                                             │ waiter           │
                                             └──────────────────┘
```

- **Topic** (example): `github.pr.<owner>/<repo>#<n>` or a stable id derived
  from `(owner, repo, pr)`.
- **Adapter** only polls and publishes; it does not wake agents or touch SQLite.
- **Bridge** owns durability, subscriptions, delivery cursors, and **adapter
  process lifecycle**.
- **Harness** owns arm/re-arm via hooks ([01-wake-and-rearm](../01-wake-and-rearm.md)).

## Adapter lifecycle (no zombies)

> **Status (card 08): implemented.** The `serve` daemon owns a `Supervisor`
> (`crates/mailbox/src/supervisor.rs`) that realises the rules and state machine
> below: one adapter per `(kind, repo, pr)`, refcounted interest driving
> start/stop, backoff-restart with give-up, a TTL sweeper for hard-died sessions,
> and — via `reconcile_startup` — the rule-6 **no-resume-on-restart** fail-safe.
> The concrete adapter program is injected through a resolver, so the machinery
> is decoupled from any adapter. **Staged rollout:** card 08 ships no real
> adapter, so production uses an `UnavailableResolver` (every kind resolves to
> "none") — `watch` records intent + interest but no poller spawns yet; the real
> `github-pr` poller and its resolver arrive with cards 09/10. The full lifecycle
> is proven with a fixture adapter (`tests/supervision.rs`).

The failure mode in the old skill: `gh-watch.sh` is started with
`run_in_background` and **never exits**; if the session dies or the agent
forgets to kill it, pollers pile up.

MVP rules:

1. **Bridge-supervised adapters.** Starting a watch is
   `mailbox watch github-pr …` (name TBD), not “agent spawns a naked bash loop.”
   The bridge records the watch in SQLite and owns the child process.
2. **One adapter per external entity.** Keyed by `(kind, repo, pr)` (not by
   session). Many sessions may care about the same PR; they share one poller.
3. **Interest is refcounted.** `watch` / subscribe adds this session as an
   interested party. `unwatch` / unsubscribe / `SessionEnd` removes **only that
   session’s** interest. The adapter keeps running while **any** interested
   session remains. The first session ending must **not** kill a watcher other
   sessions still need.
4. **Idempotent start.** Same session watching the same entity again is a no-op
   (or returns the existing watch id). A second session watching the same entity
   attaches interest and reuses the running adapter.
5. **Stop only on last interest.** When the last interested session leaves
   (explicit unwatch or SessionEnd), tear down the child and mark the watch
   stopped.
6. **Bridge restart.** Resume a watch only if at least one interested session is
   still alive; otherwise mark stopped. Exact session-liveness probe is
   harness-specific; until we have one, default to **do not resume orphan
   watches** (fail safe: missed events > zombie API load).
7. **Adapter crash.** Bridge restarts with backoff **only while interest count
   > 0**; give up and surface an error event after N failures.
8. **No agent-owned infinite bash.** Agents declare intent; they do not hold the
   poll loop in a tool background task.

```mermaid
stateDiagram-v2
    [*] --> Desired: first session watches entity
    Desired --> Running: bridge spawns adapter
    Running --> Running: poll / publish edges
    Running --> Running: more sessions attach interest
    Running --> Desired: adapter crash (backoff restart)
    Running --> Stopped: last interest gone (unwatch / SessionEnd)
    Desired --> Stopped: last interest gone before spawn
    Stopped --> [*]
```

## Data the bridge stores (sketch)

| Record | Purpose |
|---|---|
| `watch` | id, kind=`github-pr`, repo, pr, interval, desired/stopped, child pid |
| `watch_interest` | `(watch_id, session_id)` — who still cares; drives refcount |
| `adapter_baseline` | last mergeable + review/thread/comment + CI check aggregates |
| `event` | durable topic log |
| `subscription` | session ↔ topics (may align with `watch_interest`) |
| `delivery_cursor` | per subscriber; advanced by harness/bridge on surface |

Adapter baseline lives in SQLite (`adapter_baseline`, centralized) instead of
`~/.claude/agent-ipc/watchers/*.state`. **The adapter never writes it directly**
(ADR-0001): it round-trips via the protocol (card 10). At spawn the supervisor
reads `storage.get_baseline(watch_id)` and injects it into the adapter's config
(`{ "baseline": … }`); after each poll that changes it the adapter emits a
`mailbox_protocol::Baseline` line, which the card-07 host relays to
`storage.set_baseline(watch_id, …)`. One-way both directions (storage → config →
adapter at startup; adapter → `Baseline` stdout → host → storage for updates), so
there is no request/response channel and a restart resumes from the persisted
snapshot without re-firing.

## Agent-facing loop (MVP)

1. Subscribe / `mailbox watch` for the PR (adds this session’s interest; starts
   the shared adapter if needed).
2. Work / idle — hooks keep `asyncRewake` armed.
3. On wake: read unread events; react (conflicts, reviews, CI).
4. Unwatch / unsubscribe when done — drops this session’s interest; stops the
   poller only if nobody else is watching.

No `ipc-arm.sh` step.

## Failure modes

| Failure | Behaviour |
|---|---|
| `gh` auth missing | Adapter exits non-zero; bridge records error, does not spin forever without surfacing |
| GitHub rate limit | Backoff inside adapter; still one process per watched entity |
| Bridge down | Publish/watch commands fail; no silent orphan writers |
| One of N sessions dies | That session’s interest dropped; adapter keeps running for the rest |
| Last session dies / hard kill | Reconcile / TTL sweeper drops dead interests; stop adapter when count hits 0 |
| Mid-turn publishes | Delivery cursor + Stop re-arm ([01](../01-wake-and-rearm.md)) |

## Test plan

1. Stub or recorded `gh` responses: conflict / review / CI transitions publish
   exactly once each.
2. Two sessions `watch` same PR → still one child; both receive events with
   independent delivery cursors.
3. First session `SessionEnd` / unwatch → child **still running**; second session
   still woken on new edges.
4. Last session unwatch / SessionEnd → child gone; no further API calls.
5. Kill adapter process → one restart while interest > 0; stop when interest is 0.
6. Bridge restart with no live interested sessions → watch not resumed (no zombie).

> **Card 12 status note — AUTOMATED.** All six scenarios above are now encoded as
> the cross-component suite `crates/mailbox/tests/e2e.rs` (`scenario_1…6_*`),
> driven end to end through the real `mailbox serve` daemon + `mailbox` CLI + a
> real supervised adapter (the `github-pr` poller against a recorded fake `gh`, and
> the reference stub) + the fake harness driver (hook JSON fed to `mailbox harness
> arm`/`cleanup`). The wake path (waiter wake, coalescing, mid-turn surfacing) is
> covered by the same suite's `wake_*` tests. The headline **no-zombie-pollers**
> guarantee is ENFORCED by a `LeakGuard` (`tests/common/mod.rs`): it fails the test
> if any adapter / waiter / serve process scoped to that test's daemon subtree +
> waiters dir survives teardown, and a dedicated test
> (`leak_guard_detects_a_surviving_process_and_clears_when_reaped`) proves the guard
> actually catches a leak. Runs in `cargo test --workspace` / CI with no network or
> real GitHub. Unit-level proofs still live in their own suites
> (`tests/supervision.rs`, `tests/wake.rs`, `adapters/github-pr-adapter/tests/adapter_e2e.rs`).

## Open questions

- ~~Exact CLI surface (`watch` vs `adapter start`).~~ **Settled (card 06,
  [ADR-0004](../adr/0004-cli-serve-daemon-and-socket.md)):** `mailbox watch
  github-pr <owner>/<repo>#<n> [--interval <secs>]` / `mailbox unwatch …`.
  `watch` records the watch (`upsert_watch`) + this session's interest
  (`add_interest`) and subscribes the session to the PR topic; `unwatch` reverses
  it. **Adapter process supervision (spawning the poller, populating child pids,
  refcount-driven start/stop) is implemented in card 08** via the `Supervisor`.
  With no real adapter yet (cards 09/10), the production `UnavailableResolver`
  resolves no poller, so a watch still sits `Desired` with no child pid until then.
- ~~How Claude session identity is named for interest rows.~~ **Settled (card
  06):** the `SessionId` comes from `--session <id>`, falling back to the
  `MAILBOX_SESSION_ID` env var (the harness hooks set the env — card 11).
- Whether peer agent→agent messages are in the same MVP slice or immediately
  after GitHub watch.
- ~~How finely to model CI edges (whole-PR rollup vs per-check) for the first
  cut.~~ **Settled (card 10):** whole-PR **rollup** (pending/success/failure); the
  event fires on a transition **into failure** (or the failed-check set gaining
  names while already failing) and carries the newly-failed check *names* in its
  body — not one event per check, and not on success/pending transitions (which
  would storm on a flapping check).
