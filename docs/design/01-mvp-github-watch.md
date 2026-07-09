# Design: MVP — GitHub PR watch (agent-ipc parity)

- Status: Draft
- Related ADRs: [0001](../adr/0001-rust-bridge-subprocess-adapters.md),
  [0002](../adr/0002-mvp-crate-stack.md),
  [0003](../adr/0003-single-writer-sqlite.md)
- Related: [01-wake-and-rearm](../01-wake-and-rearm.md), prior skill
  `agent-ipc` / `agent-ipc-github`

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

Adapter baseline can live in SQLite (preferred) instead of
`~/.claude/agent-ipc/watchers/*.state` so restart/idempotency is centralized.

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

## Open questions

- Exact CLI surface (`watch` vs `adapter start`).
- How Claude session identity is named for interest rows (session_id from hooks).
- Whether peer agent→agent messages are in the same MVP slice or immediately
  after GitHub watch.
- How finely to model CI edges (whole-PR rollup vs per-check) for the first cut.
