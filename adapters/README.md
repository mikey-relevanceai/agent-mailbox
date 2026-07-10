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
mailbox watch stub demo --interval-ms 500 --session s1
mailbox read --session s1                          # see the synthetic events
mailbox unwatch stub demo --session s1             # tears the adapter down
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
