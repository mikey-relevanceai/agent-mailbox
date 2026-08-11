# ADR-0021: Delete the sentinel fallback — the inbox socket is the only wake wire

- Status: Accepted
- Date: 2026-08-09
- Supersedes: [ADR-0008](0008-on-demand-wake-filechanged.md) (the sentinel +
  `FileChanged` design), [ADR-0012](0012-level-triggered-wake-at-the-turn-boundary.md)
  (the turn-boundary re-trigger), [ADR-0016](0016-prove-wakeability-with-an-active-probe.md)
  (the active probe) and [ADR-0017](0017-daemon-bumps-the-sentinel.md) (the daemon's
  sentinel write). Their *mechanisms* are deleted; the principles they established are
  inherited and named below.
- Amends: [ADR-0013](0013-re-register-inbox-and-watchpaths-on-resume.md) — the inbox
  half stands, the `watchPaths` half is void.
- Builds on: [ADR-0020](0020-peer-inbox-socket-is-the-wake-wire.md), which made the
  inbox socket the primary wire and kept this fallback.

## Context

ADR-0020 shipped the inbox socket as the wake wire and **kept the sentinel path as a
fallback**, because Claude Code's `agents_cross_session_inbox` gate decides which
sessions bind a socket and cannot be turned on from outside Claude Code. That was the
cautious call, and one day of running it disproved it.

### What we watched happen

A real PR comment, published to a topic two sessions subscribed to — one on each
channel:

```
11:04:18  woke subscribers after publish  topic="…agent-mailbox#33" peer=1 sentinel=1
11:04:22  delivered unread events…        session="983eae5f…"  count=1
```

The **peer** subscriber woke and read four seconds later. The **sentinel** subscriber
never woke at all. Its diagnosis:

- `session-start` ran correctly at 04:51, armed the sentinel, and printed the
  `watchPaths` registration. Claude Code accepted it.
- The daemon wrote that sentinel correctly — right content, right mtime.
- The session's `FileChanged` hook then ran **zero times in 6.3 hours**, while other
  sessions' hooks fired **6,898 times** off the same writes.

Everything on our side was correct. The session was silently deaf anyway, and the
component that failed was the one being kept *as the safety net*.

The cause is the residual ADR-0008 flagged as `[UNDOCUMENTED]` on day one — whether a
`watchPaths` registration persists for the whole session — and neither we nor anyone
else has ever got past "Claude Code has a bug" on it. Fleet-wide, 744 sessions appear
in `harness.log` and only 205 have ever had a `FileChanged` fire.

### Why this settles it rather than being one more bug to chase

The sentinel path has produced a silent-deafness bug on roughly every contact with
reality: three coalescing bugs (ADR-0008), a sweeper reaping live interest (ADR-0009),
a wake edge spent mid-turn (ADR-0012), an un-re-registered resume (ADR-0013), and now
a `watchPaths` registration that is accepted and then ignored. ADR-0016 exists solely
because none of it could be observed from outside.

Every one of those is a failure of a wake mechanism assembled out of parts Claude Code
never offered as a wake mechanism. There is now a part that *is* one.

## Decision

**Delete the sentinel path entirely. The inbox socket is the only wake wire.**

Deleted: `sentinel.rs` and the per-session sentinel directory; the `watchPaths`
registration; the `FileChanged` matcher and its `asyncRewake` exit-2 hook; the
anti-loop store re-check; ADR-0012's turn-boundary re-trigger and its watermark;
ADR-0016's active probe, hook-ran ack and turn-started/turn-ended stamps; the
read-only store that existed to serve them; and the hook-timeout apparatus that
bounded a hook nothing runs.

**The hook set drops from five to two**, and neither can wake a session:

| Hook | Job |
|---|---|
| `SessionStart` (matcher `""`) | register the always-on agent inbox (ADR-0007) |
| `SessionEnd` | drop interests/subscriptions, so no poller outlives its session |

**`doctor` becomes a read, not an experiment.** Reachability is two readable facts —
is the process alive, and did Claude Code bind it a socket — so there is no bump, no
ack, no budget, and no "a session cannot measure itself" blind spot. One verdict is a
fault: `no-inbox`, a live session nothing can wake.

**Session liveness comes from Claude Code's registry plus a pid check**, replacing the
`ps` + argv parsing. The registry is still **not** liveness — an entry outlives its
process — so every entry is confirmed against the process table.

**`subscribe` and `watch` refuse when the caller cannot be woken.** Those commands
mean "tell me when this changes"; an agent that runs one and goes idle waits forever
if no socket exists. The one moment to say so is while it is still awake and asking.
Two carve-outs, because refusing on weak evidence is worse than not refusing: an
unreadable registry is not evidence, and a session Claude Code never registered may be
a harness that is not Claude Code. Only *registered, and given no socket* is
unambiguous. `publish` and `read` are ungated — an unwakeable session can still be
sent to and can still read.

**Claude Code 2.1.226+ is the floor**, set by the maintainer. Older sessions do not
register themselves, so they read as gone and cannot be woken.

## Consequences

- **−4,000 lines, and the bug-prone ones.** Every silent-deafness bug this project has
  had lived in the deleted code.
- **We now depend entirely on a third-party feature flag with no safety net.** If
  `agents_cross_session_inbox` regresses, agent-mailbox stops waking anybody. This is
  the deliberate trade: it stops **loudly** — `doctor` reports `no-inbox`, and
  `subscribe`/`watch` refuse at the moment of asking — where the fallback stopped
  silently. A mechanism that fails loudly and rarely beats one that fails quietly and
  often.
- **Wake is no longer edge-triggered, so a whole failure class is gone.** A message
  queues at the receiver and is read between tool calls, so a busy agent *delays* a
  wake rather than destroying it. That is why ADR-0012's re-trigger, its watermark and
  the turn-boundary stamps could all go rather than being ported.
- **No anti-loop between the socket and the model.** The sentinel was a trigger the
  hook could second-guess; a frame IS the turn. So the daemon sends nothing for an
  empty unread set, and a burst of N publishes costs up to N wakes — deliberately not
  coalesced, for the reason ADR-0008 removed its own coalescing: every attempt to be
  clever about which publishes "need" a wake produced a lost one instead. Claude Code
  drops identical repeats arriving close together, which blunts the cost; the bridge
  does not rely on that.
- **Non-Claude-Code harnesses have no wake at all.** Codex never had one (no
  `FileChanged`, no `asyncRewake`), so nothing is lost in practice — but the sentinel
  was at least a shape a future harness could have implemented against. That door is
  now closed until such a harness offers its own primitive.
- **Upgrading sweeps the old hooks.** A re-run of `install-hooks` removes every hook
  name we have ever installed, so an upgrade cannot leave a `FileChanged → wake`
  pointing at a subcommand the binary no longer has.

### Principles inherited from the ADRs this supersedes

Their mechanisms are gone; these survive them and still bind:

- **Payload-free wake** (ADR-0001): the frame carries topic names only. The socket
  *could* carry a body and must not. *(Amended by
  [ADR-0022](0022-the-wake-carries-a-subject.md) to **pointer, not payload**: the
  frame also carries counts and each event's one-line `subject`. Bodies still never
  cross.)*
- **A health check must not depend on the component most likely to be broken**
  (ADR-0015/0016): `doctor` still needs no daemon.
- **Silence is not death** (ADR-0009): an idle session is silent by design, so
  liveness comes from the process table and never from our own artefacts — now
  including Claude Code's registry file.
- **A retained primitive is a liability** (ADR-0019): this ADR is that rule applied to
  the fallback ADR-0020 retained, and to the hook-timeout apparatus it left behind.

## Alternatives considered

- **Keep the fallback until the gate is universal.** ADR-0020's position, and the one
  this reverses. Rejected on evidence: the session it was protecting was the deaf one.
  Keeping a safety net that silently does not catch is worse than having none, because
  it is the reason nobody looks.
- **Fix the `watchPaths` residual.** We could not, and neither could anyone: many
  hours across several sessions produced no explanation beyond "Claude Code has a
  bug". Building on a mechanism we cannot debug is what got us here.
- **Warn instead of refusing on `subscribe`/`watch`.** Rejected: a warning on stderr in
  an unattended session is read by nobody, which is indistinguishable from the silent
  failure being replaced.
- **Coalesce bursts on the peer channel** to recover the anti-loop's cost saving.
  Rejected on ADR-0008's evidence — its three lost-wake bugs all lived in coalescing
  logic — and because Claude Code already dedups identical repeats.
