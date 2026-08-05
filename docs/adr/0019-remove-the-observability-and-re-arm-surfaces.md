# ADR-0019: Remove the re-arm primitives and the inferred wake dashboard

- Status: Accepted
- Date: 2026-08-05
- Supersedes: [ADR-0006](0006-harness-self-respawn.md) (the `arm`/`wait` re-arm loop
  and its timing knobs are deleted, not merely unused) and
  [ADR-0015](0015-dashboard-reads-the-store-read-only.md) (the command it describes
  is deleted; the read-only-store principle it established survives in
  [ADR-0016](0016-prove-wakeability-with-an-active-probe.md)).

## Context

This ADR is written **after** the change it records. The deletion shipped without
one, which is a gap in the record: the two later removals on the same branch
([ADR-0017](0017-daemon-bumps-the-sentinel.md),
[ADR-0018](0018-publish-has-one-rule.md)) each got an ADR, and a reader walking the
numbering forward would otherwise find two ADRs marked superseded with no entry
saying when or why. Recording it late is worth more than leaving the gap; nothing
here is reconstructed guesswork, but the reasoning is stated by the author of the
change rather than captured at the time.

Two surfaces had outlived their decisions.

**`mailbox harness arm` and `mailbox wait`.** ADR-0008 replaced the periodic re-arm
with the detached watcher and the `FileChanged` hook. `arm` and `wait` were kept
"as retained primitives" — but no installed hook invoked either one from that point
on, and nothing else did. They were exercised only by their own tests, which is the
definition of code that is passing rather than working. Behind them sat a whole
apparatus that existed only to serve them: `max_block`, the hook-timeout margin,
`MaxBlockDecision`, `resolve_max_block`, `HookInstallSpec::validate`, the exit-2
re-arm boundary, `REARM_NOTICE`.

**`mailbox dashboard`.** It reconstructed wake health from `harness.log` — had a
`FileChanged` hook ever run for this session? That is real evidence, but it answers
a question about the past. Measured against ADR-0016's active probe on 19 live idle
sessions it was wrong nine times, in **both** directions: six sessions it called
suspect answered a probe in under five seconds, and three it called verified could
not be woken at all. Wakeability is perishable; history cannot report it.

## Decision

Delete all of it: `mailbox harness arm`, `mailbox wait`, the max-block/timeout
apparatus, `Waiter::wait`, `WaitOutcome`, `REARM_NOTICE`, `mailbox dashboard`,
`dashboard::wake_health`, and `ReadOnlyStore::fleet` with its `Fleet*` types.

`mailbox doctor` is the only wake-health surface. It asks directly.

## Consequences

- **A retained primitive is a liability, not an option.** Both of these were kept
  against a future that never came, and in the meantime they had to keep compiling,
  keep being tested, and keep being described in the docs. Retention is not free.
- **A view that is wrong in both directions is worse than no view.** The dashboard
  did not merely fail to help; it told operators that healthy sessions were deaf and
  that deaf ones were fine. Keeping it beside `doctor` would have invited reading the
  wrong one.
- **The read-only-store exception survives its command.** ADR-0015's real decision —
  a health check must not depend on the component most likely to be broken — is why
  `doctor` needs no daemon. That principle is inherited, not withdrawn.
- **One test was passing for the wrong reason and this exposed it.** `stub_e2e`
  spawned `mailbox wait` and asserted exit 2. Clap returns exit 2 for an unknown
  subcommand, so after the deletion it stayed green while testing nothing. It was
  rewritten to drive the real wire. Any test asserting an exit code that a *usage
  error* also produces is not asserting what it appears to.
- **`cargo check` was not a sufficient gate.** The deletion left `cargo fmt --check`
  and `cargo clippy -D warnings` failing, and left four intra-doc links pointing at
  deleted items — including the whole `run_wait` doc block orphaned onto
  `mailbox doctor`, so `doctor`'s documentation opened by describing `--max-block-ms`.
  `clippy` does not check intra-doc links; `cargo doc` does.
