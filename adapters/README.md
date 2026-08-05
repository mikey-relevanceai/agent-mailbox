# Adapters

Reference and experimental adapters live here. They are **separate processes**
that speak the mailbox protocol (see `docs/02-tech-stack.md`). They must not
import bridge internals (ADR-0001) — protocol + NDJSON only, never SQLite or the
bridge crate.

## `mailbox-stub-adapter` (the reference stub)

`stub-adapter/` is the trivial reference adapter: it publishes a synthetic event
on a fixed interval. It proves the whole path — the protocol + framing, the
card-07 transport contract (config on stdin, `Publish` on stdout), card-08
supervision, wake, cursors, and multi-subscriber fan-out — **without any real
poller existing**. It is also the canonical happy-path adapter the supervision
tests drive.

It imports **only** `mailbox-protocol`.

### Config schema (first NDJSON line on stdin)

The transport delivers config as one JSON object on the first line of stdin, then
closes stdin. The object is opaque to the host; the stub defines its own schema:

```json
{ "topic": "stub.demo", "interval_ms": 500, "count": 10 }
```

| field         | required | meaning                                                        |
|---------------|----------|----------------------------------------------------------------|
| `topic`       | yes      | the topic to publish on (the bridge passes `stub.<label>`)      |
| `interval_ms` | no       | delay between publishes; absent or `0` ⇒ 1000 ms (never busy)   |
| `count`       | no       | how many events to publish; absent or `0` ⇒ **publish forever** |

Each publish is one `mailbox_protocol::Publish` NDJSON line on stdout, with a tiny
opaque body `{"source":"stub","seq":N}`. On **SIGTERM** the stub exits promptly
with code 0, so the supervisor's group teardown (SIGTERM → grace → SIGKILL) is
clean.

### Bridge-supervised (the normal path)

```sh
mailbox serve &                                   # the daemon
mailbox watch stub demo --interval-ms 500
mailbox read                          # see the synthetic events
mailbox unwatch stub demo             # tears the adapter down
```

`watch stub <label>` keys the watch by `(kind=stub, label)`, publishes on
`stub.<label>`, and reuses the same interest / supervision machinery as
`github-pr`. `serve` resolves the stub binary via `MAILBOX_STUB_ADAPTER_BIN`
(an absolute path, for tests/dev) or, unset, `mailbox-stub-adapter` on `PATH`.

Note: because the supervisor restarts any adapter that exits while a session is
still interested, a **finite `--count`** under the bridge means the stub
republishes that batch each time it is (re)started. Use `--count 0` (the default)
for a steady stream; `--count` is most useful for by-hand runs.

### By hand (for experiments)

The stub is runnable standalone — feed it a config line and read its stdout:

```sh
echo '{"topic":"stub.demo","interval_ms":500,"count":3}' | mailbox-stub-adapter
```

prints three `Publish` NDJSON lines, 500 ms apart, then exits 0. Pipe it into
`jq` to inspect, or hand its stdout to whatever is testing the wire format.

## `mailbox-github-pr-adapter` (the real MVP poller)

`github-pr-adapter/` is the MVP's real adapter: it polls one GitHub pull request
via the `gh` CLI and publishes **only the transitions agents care about** — parity
with the old `agent-ipc-github` skill, plus CI. It imports **only**
`mailbox-protocol` and reaches GitHub by shelling out to `gh` (never linking a
GitHub SDK or the bridge crate).

### Edge-triggered, baseline-via-protocol

On the **first poll** it *baselines* (records current state, publishes nothing).
Thereafter it fires only on **transitions**:

- **mergeable → `CONFLICTING`** — ignoring transient `UNKNOWN` (GitHub reports it
  while recomputing mergeability; we never baseline to it nor fire on it, so a
  flap produces zero events).
- **new reviews / review threads / PR comments** — diffed by a **monotonic
  highest-seen id cursor** (from the REST list endpoints), not a count: a new item
  surfaces even when a deletion cancels the count, and a pure decrease never
  false-fires.
- **CI into failure** — whole-PR rollup; the edge fires only on a transition
  *into* `Failure` (or when the failing-check set gains new names while already
  failing), **not** on transitions to pending/success. The body lists the
  newly-failed check **names** (we fire on the rollup, not per individual check).

Each poll makes four `gh` calls: `gh pr view` for mergeability + the CI rollup,
and three REST list endpoints (`.../pulls/N/reviews`, `.../issues/N/comments`,
`.../pulls/N/comments`) whose integer ids drive the cursors.

The baseline must survive a restart so an already-fired edge is not re-fired, and
it must not live in adapter-side SQLite (ADR-0001). So it round-trips **through the
bridge** ([ADR-0005](../docs/adr/0005-baseline-via-protocol.md)): the supervisor
injects the last persisted baseline into the adapter's config at spawn (`baseline`
field), and after each poll that changes it the adapter emits a
`mailbox_protocol::Baseline` line, which the host relays to `storage.set_baseline`.
A restart resumes exactly where it left off (at-least-once only across an
*ungraceful* kill — see ADR-0005). The host also binds each adapter to its entity
topic, rejecting any publish to a foreign topic (provenance).

### Config schema (first NDJSON line on stdin)

```json
{
  "topic": "github.pr.octocat/hello-world#42",
  "owner": "octocat", "repo": "hello-world", "number": 42,
  "interval_ms": 60000,
  "baseline": null,
  "max_polls": 0
}
```

| field         | required | meaning                                                             |
|---------------|----------|---------------------------------------------------------------------|
| `topic`       | yes      | the topic to publish edges on (`github.pr.<owner>/<repo>#<n>`)       |
| `owner`/`repo`/`number` | yes | the PR to poll (`gh --repo owner/repo`)                        |
| `interval_ms` | no       | poll cadence; absent or `0` ⇒ 60 000 ms (never a busy loop)          |
| `baseline`    | injected | the persisted snapshot (supervisor-injected); `null` ⇒ baseline next |
| `max_polls`   | no       | stop cleanly after N polls; `0`/absent ⇒ poll forever (until SIGTERM)|

### Failure modes (design/01)

- **`gh` auth missing** → the adapter exits **non-zero**, so the supervisor records
  and surfaces the error via its give-up path (no silent spin).
- **GitHub rate limit** → the adapter **backs off and retries inside itself** (still
  one process per watched PR), rather than a tight loop — bounded by a retry
  budget, after which it exits non-zero so a *persistent* rate limit surfaces.
- **Malformed / incomplete `gh` response** (a missing/null required field) → strict
  fail-CLOSED parsing rejects it as **transient**: the poll is skipped and the
  baseline is left untouched (never fail-open to zeros, which would false-fire a
  storm). A persistent streak eventually exits non-zero so schema drift surfaces.
- **SIGTERM/SIGINT** → exits promptly with code 0 (clean supervisor teardown).

### Injecting `gh` (tests)

The `gh` binary is overridable via **`MAILBOX_GH_BIN`** so tests point it at a fake
`gh` emitting recorded JSON — the whole poll→diff→publish loop then runs with no
network or auth. `serve` resolves the adapter binary via **`MAILBOX_GH_ADAPTER_BIN`**
(absolute path, for tests/dev) or, unset, `mailbox-github-pr-adapter` on `PATH`.

### Bridge-supervised (the normal path)

```sh
mailbox serve &
mailbox watch github-pr octocat/hello-world#42 --interval 60
mailbox read      # conflict / review / CI edges as they happen
mailbox unwatch github-pr octocat/hello-world#42
```
