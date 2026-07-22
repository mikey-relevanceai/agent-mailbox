# ADR-0015: `mailbox dashboard` reads the store read-only

- Status: Accepted
- Date: 2026-07-21
- Amends: [ADR-0003](0003-single-writer-sqlite.md) / [ADR-0004](0004-cli-serve-daemon-and-socket.md).
  The single-writer rule is untouched. The "every command except `wait` is a socket
  client" rule gains a **second** read-only exception.

## Context

The wake path can fail in a way that no existing view can see.

A session whose harness never established its `watchPaths` registration looks
completely healthy from inside the bridge: its inbox is registered, its watcher
process is alive, its sentinel file is being bumped on every kick, and `mailbox
status` reports it as fine. It simply is never woken, because the last hop —
Claude Code noticing the sentinel and running the `FileChanged` hook — is outside
our process and unobservable from it.

Measured on one developer machine, **27 of 124 sessions had ever had a wake hook run
for them**; the rest had sentinel bumps and no hook runs at all. Nothing in the
product surfaced that. It was found by hand-grepping `harness.log`.

Two things follow. First, that log grep is the diagnosis, and it should be a command
rather than folklore. Second, the view has to be **fleet-wide**: the ratio is the
finding, and no per-session view could have shown it.

### Why not a socket client

Every command except `wait` is a one-shot socket client, and a client that cannot
reach the daemon fails loud (ADR-0004). That rule is right for anything that mutates
or that needs the daemon's authority. It is wrong here, for one reason:

**A health view must work when things are broken, and "the daemon is down" is one of
the things that breaks.** A dashboard routed through the socket goes blank in one of
the failure modes it exists to diagnose, and the person looking at it learns nothing
except that something is wrong — which they already knew.

There is also a plain engineering argument. Assembling a fleet view over the socket
needs either a new `Fleet` request or N per-session `Status` round trips; the latter
is a torn snapshot (a session's unread from before a publish beside a watch state
from after), and a dashboard whose rows disagree with each other looks exactly like a
bug in the thing being diagnosed. Reading the store directly gets all of it from
**one SQL transaction**, with less code than either.

## Decision

**`mailbox dashboard` opens the database read-only, as the second exception to the
socket-client rule.** It follows the same terms `wait` already has under ADR-0003:
`SQLITE_OPEN_READ_ONLY`, no create flag, no mutation, and no delivery cursor ever
advanced. `ReadOnlyStore::fleet` is the single read; it runs inside one transaction
so every row describes one instant.

The daemon is **probed and reported**, not required: a `DaemonState::Down` header,
with every row still rendered.

**The evidence stays evidence.** Wake health is reconstructed from `harness.log` and
reported three-valued — `Verified` (a hook demonstrably ran), `NoWakeObserved` (bumps
with no hook run) and `Unproven` (no bumps yet). The middle case is deliberately NOT
called "deaf": absence of a wake line is strong evidence, not proof, since the log may
have rotated or the bumps may all have landed mid-turn (a lost edge — ADR-0012). A
tool built to catch a system that overstates its own health must not overstate what it
knows. The bounded log read discloses when it truncated, for the same reason.

**Live sessions by default.** Subscriptions outlive a session whose `SessionEnd` never
ran, so the store knows about far more sessions than exist (306 known, 59 live on the
machine above). Dead rows are inert — a dead session cannot be woken and is not a
fault — so they are hidden behind `--all`, and the count is always shown, never
silently dropped.

## Consequences

- **The failure is visible.** "27 verified, 97 with no wake observed" is one command,
  and sessions sitting on mail they cannot be woken for sort to the top.
- **The view survives the outage.** With the bridge down the dashboard still renders,
  headed `daemon DOWN`. Asserted by test.
- **A second read-only reader exists.** The single-writer invariant is unaffected —
  this connection cannot write — but "everything except `wait` goes through the
  socket" is no longer literally true, and a future reader should point at this ADR
  rather than quietly becoming a third.
- **The log is now load-bearing for a user-facing feature.** Its wording was already
  a debugging surface; it is now parsed. That coupling is guarded by
  `tests/dashboard.rs`, which drives real hook runs and classifies their real output,
  so rewording a wake message fails a test instead of silently turning the fleet red.
- **`harness.log` grows unbounded and is read on a timer.** Bounded to an 8 MiB tail,
  which the view discloses. Rotation is not solved here.

## Alternatives considered

- **A new `Fleet` control request over the socket.** Keeps the boundary exactly as
  written, and gives the daemon's own consistent snapshot. Rejected because it takes
  the dashboard away when the daemon is down, and costs a protocol addition plus a
  serve dispatch to end up with strictly less capability than the read-only open.
- **Socket first, read-only fallback.** Strictly more code for no capability the
  read-only path lacks: with WAL, a read-only reader already sees every committed
  write immediately, so "fresher via the daemon" is not a real advantage.
- **N per-session `Status` calls.** A torn snapshot, and O(sessions) round trips per
  refresh.
- **Report `NoWakeObserved` as "deaf".** Shorter and more decisive, and wrong: the
  log cannot distinguish "never watched" from "rotated away" or "every bump landed
  mid-turn". Overstating here would reproduce the exact defect — a confident health
  signal that is not backed by evidence — that motivated the tool.
