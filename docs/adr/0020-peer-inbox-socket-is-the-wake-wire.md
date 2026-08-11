# ADR-0020: The peer inbox socket is the wake wire; the sentinel becomes the fallback

- Status: Accepted
- Date: 2026-08-09
- Amends: [ADR-0008](0008-on-demand-wake-filechanged.md) and
  [ADR-0017](0017-daemon-bumps-the-sentinel.md) — the sentinel + `FileChanged` path
  they describe is no longer the primary wake wire, but it is **retained, not
  deleted**, and everything they say about it still holds when it runs.
  [ADR-0012](0012-level-triggered-wake-at-the-turn-boundary.md),
  [ADR-0013](0013-re-register-inbox-and-watchpaths-on-resume.md) and
  [ADR-0016](0016-prove-wakeability-with-an-active-probe.md) continue to govern the
  fallback path unchanged.

## Context

Claude Code 2.1.224 shipped **cross-session messaging**: every session binds a
per-session Unix socket, registers itself in `~/.claude/sessions/<pid>.json`, and
Claude Code delivers anything arriving on that socket to the model. The documented
behaviour is exactly the primitive this project has spent eighteen ADRs
constructing out of file watches:

> "When the receiving session is idle, Claude Code starts a new turn with the message."

The docs name our case explicitly as a supported reason to read the socket section:
*"when you want a script or hook to post into a session."* The socket path is also
exported to hooks and Bash as `CLAUDE_CODE_MESSAGING_SOCKET` before any hook runs.

Two forces make this worth acting on now rather than noting for later.

**The `FileChanged` path is unreliable in practice.** That is the human maintainer's
call, made against lived experience of this fleet, and it is the reason this ADR
exists. The record supports it: the wake wire has produced silent-deafness bugs
repeatedly (ADR-0008's three coalescing bugs, ADR-0009's sweeper reaping live
interest, ADR-0012's spent-edge window, ADR-0013's un-re-registered resume), and
ADR-0016 exists solely because we could not otherwise tell a wakeable session from
a deaf one. Every one of those is a failure of a wake mechanism assembled from
parts Claude Code never intended as a wake mechanism.

**The new path is one hop.** `publish` → write the subscriber's socket → the session
takes a turn. No sentinel, no `watchPaths`, no basename matcher, no `FileChanged`,
no exit-2 hook, no anti-loop store re-check, no turn-boundary re-trigger.

### What we verified, rather than assumed

Everything below was confirmed on this machine on 2026-08-09 against live sessions,
not read off a docs page. The delivery decision depends on **both** sessions'
permission-mode classes, where `bypassPermissions` is one class and everything else
(default, `auto`, `acceptEdits`, `dontAsk`) is the other:

| Receiver class | Sender claims | Outcome |
|---|---|---|
| bypass | nothing | **Held** → approval dialog → dropped at `dialogExpiry` (default 5m) |
| bypass | `bypass` | **Delivered** |
| prompting (`-p`, default mode) | nothing | **Delivered**, no configuration |
| prompting (`auto` mode) | nothing, external process | **Delivered**, no configuration |
| prompting (`auto` mode) | `bypass` | **Held** |

The wire format, captured by standing up a decoy peer (a socket plus a registry
file — which was enough to appear in `ListAgents`, so discovery is unauthenticated
same-user file registration):

```json
{"msgV":1,"msg_id":"<uuid>","type":"user",
 "message":{"role":"user","content":"<cross-session-message from-name=\"…\" from-mode=\"bypass\">\n…\n</cross-session-message>"},
 "priority":"next"}
```

Claude Code's own startup log publishes a minimal equivalent, and it is the one we
use: `{"type":"user","message":{"role":"user","content":"…"}}`, newline-terminated.

Three further findings shape the decision:

1. **The permission class is self-asserted in the frame and unverified.** A sender
   that writes `from-mode="bypass"` is believed. This is what makes row 2 above
   work, and it is why we do not use it — see Alternatives.
2. **Peer pid verification requires a live sender.** A held message from a
   still-running sender is annotated `[verified pid N]`; one from a script that had
   already exited is not. This matches the documented macOS limitation, and it means
   a long-running daemon lands in a strictly better trust position than a
   spawn-and-exit writer.
3. **Socket binding is feature-gated and cannot be forced on.** The gate is
   `agents_cross_session_inbox`; when off, Claude Code logs
   `[uds-messaging] Skipped: cross-session messaging gate off` and binds nothing.
   Of 19 live sessions on this machine, 2 had a socket, across identical versions.
   `CLAUDE_CODE_MESSAGING_SOCKET` is an **output** Claude Code exports, not an input:
   setting it changes nothing. **There is no setting, flag, or environment variable
   that turns the socket on.**

Finding 3 is the whole reason this ADR does not simply delete the sentinel.

## Decision

**The wake path becomes two channels, tried in order.**

1. **Peer inbox (primary).** On publish, for each subscriber, look up its socket in
   Claude Code's session registry (`~/.claude/sessions/*.json`, keyed by
   `sessionId`). If it has one, connect and write the minimal frame.
2. **Sentinel (fallback).** If the subscriber has no registry entry or no bound
   socket, write the sentinel exactly as ADR-0017 specifies. The `FileChanged` hook,
   the turn-boundary re-trigger and `doctor`'s probe all keep working for that
   session, unchanged.

Neither channel may fail a publish. The event is durable first; delivery is
best-effort after, and a failure on either channel is logged and skipped.

**We assert no permission class.** The frame carries no `from-mode`. This is not
only the honest choice, it is the *more deliverable* one: asserting `bypass` is
precisely what causes a hold against every prompting-class receiver (rows 4 and 5
above). The honest option and the effective option are the same option.

**Wake stays payload-free.** The frame's `content` is `wake::reminder(&topics)` —
the same topic-names-only string the exit-2 hook already writes to stderr, and
already pinned payload-free by its own test. The body stays in the durable log for
the agent's `read`. This invariant (ADR-0001, `docs/01-wake.md`) is preserved
deliberately: the socket *could* carry a body, and must not.

> **Amended by [ADR-0022](0022-the-wake-carries-a-subject.md).** The frame is no
> longer topic names alone: it also carries unread counts and each event's bounded,
> single-line `subject` and link. The half of this clause that matters is untouched —
> the socket still may not carry a body, and `read` is still the only way to one.

**The daemon holds the connection open across the write** so peer-pid verification
can succeed (finding 2). A fire-and-forget write is unverifiable on macOS.

**Session discovery moves to the registry.** `doctor::live_claude_sessions` scrapes
`ps` output for `--session-id` / `--resume`. The registry publishes `sessionId`,
`pid`, `cwd`, `name` and socket path directly. The registry is authoritative where
it exists; the process table remains the liveness check, because a registry file
outlives its process exactly as ADR-0017 warned about every other artefact.

**`crossSessionInbound` is never set by us.** A `bypassPermissions` session holds
our wakes by default, and the fix is the documented `crossSessionInbound: "accept"`.
That is a real widening of trust — any same-user process could then drive an agent
that acts without prompting — so it is offered as an explicit setup step
(`mailbox harness install-inbound`), in the shape of `install-hooks`, run only when
the operator asks for it. We detect the situation and say so; we do not decide it.

## Consequences

- **The unreliable path stops being the only path.** For a session with a bound
  socket, wakeability no longer depends on `watchPaths` surviving, a basename
  matcher not colliding, an edge not being spent mid-turn, or a hook exiting 2.
- **The sentinel is retained, and that is a cost.** ADR-0019 is emphatic that a
  retained primitive is a liability. The difference here is that this one is
  *invoked*: it serves every session the gate has not reached, which today is most
  of them. It should be deleted when the gate is universal, and not before.
- **Two channels means two ways to be wrong.** `doctor` must report which channel a
  session is on, or the next silent-deafness bug will be "we assumed it was on the
  socket". This is the main new obligation.
- **A wake now costs a turn immediately, not a hook round-trip.** Delivery counts
  toward usage like a typed prompt — the same as the exit-2 wake it replaces.
- **Message loops are throttled by Claude Code.** It rate-limits per sender, drops
  identical repeats within a short window, and caps unread accepted messages at 50.
  This is coalescing, in a layer we do not control, and ADR-0008 removed our own
  coalescing because it caused three lost-wake bugs. The mitigation is that the
  store stays authoritative: the reminder names topics, the agent's `read` is what
  actually consumes, and a dropped duplicate cannot lose mail that a later `read`
  will still find. **A reminder must never be the only record that mail exists.**
- **The agent is told the wake came from "another Claude session".** That is Claude
  Code's own framing for anything arriving on the socket; a GitHub PR event will be
  announced that way. We do not assert it and cannot suppress it.
- **Peer identity is same-user file registration.** Anything that can write
  `~/.claude/sessions/*.json` and bind a socket is an addressable peer. This is
  inside the trust boundary Claude Code already draws (the socket is `0600`), but it
  means the registry is not evidence of anything beyond "same user".

### Residual risks

- **The gate can leave a session unreachable by socket**, with no way to turn it on.
  The fallback covers it; what it does not cover is an operator who assumes the
  socket path is in use when it is not. Hence the `doctor` obligation above.
- **A `bypassPermissions` fleet is deaf on the socket until the operator opts in.**
  Held wakes are dropped after `dialogExpiry` with no sender-side retry. Until
  `install-inbound` is run, such a session is served only by the fallback — so the
  fallback's removal is gated on this, too.
- **The frame shape is not a published contract.** The minimal form comes from
  Claude Code's own startup log, which is a strong signal but not a documented API.
  It may change. The failure mode is a rejected write, which is visible and falls
  back, rather than a silent drop.

## Alternatives considered

- **Assert `from-mode="bypass"` so bypass sessions accept us.** Verified to work.
  Rejected: the field is undocumented, it claims a permission class we do not hold,
  it makes the receiving agent believe a peer Claude session is speaking, and it is
  *worse at delivery* than claiming nothing because it triggers holds on every
  prompting-class receiver. It would also ship that assertion to every user's machine
  with nobody opting into anything. It is strictly dominated.
- **Set `crossSessionInbound: "accept"` in `install-hooks`.** Rejected: it re-opens
  unattended delivery in exactly the configuration the guard was added for — an agent
  that executes without asking. It is the operator's call, so it gets its own command.
- **Channels** (`--channels`, an MCP server pushing `notifications/claude/channel`).
  Conceptually the closest fit — it is built for pushing CI and monitoring events
  into a live session. Rejected for now: research preview with a contract that "may
  change", requires a launch flag so it cannot be enabled via `settings.json` the way
  `install-hooks` can, requires an Anthropic-allowlisted plugin, and spawns a
  per-session stdio subprocess — reintroducing exactly the per-session process
  ADR-0017 deleted. Worth revisiting if it leaves preview.
- **Delete the sentinel and require the socket.** Rejected on finding 3: we would be
  making wakeability depend on a flag the operator cannot set.
- **Keep `FileChanged` alone.** Rejected by the maintainer on reliability grounds;
  the ADR record above is consistent with that judgement.
