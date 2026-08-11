# ADR-0007: Always-on agent inboxes (inter-agent messaging)

- Status: **Accepted — the decision stands; two of its mechanisms are superseded.**
  The always-on `agent.<session-id>` inbox, the tombstone guard, the hard `send`
  refusal and discovery-as-a-read are all in force. What changed is *who registers
  the inbox* — `mailbox harness arm` is gone, and `session-start` / `turn-end` do it
  now ([ADR-0008](0008-on-demand-wake-filechanged.md),
  [ADR-0013](0013-re-register-inbox-and-watchpaths-on-resume.md)) — and *what proves
  liveness*: decision 5's live-waiter pidfile probe is replaced by the process table
  ([ADR-0017](0017-daemon-bumps-the-sentinel.md)). See the note on decision 5.
- Date: 2026-07-13

## Context

Until card 16 the mailbox could only wake an agent about the *world* (a watched
PR, a stub publisher, a topic someone happened to share). Agents could not
address each other: there was no name to send to. The concrete want is a reviewer
agent telling a builder agent "your review is done" — and the builder, idle in a
different session, waking up — **with no human in the loop and nobody switching
sessions to arrange it beforehand**.

Everything needed already exists. Topics are arbitrary validated strings; the bus
already does multi-subscriber fan-out with independent per-subscriber cursors and
exactly-once delivery; a publish already kicks every subscribed session's waiter.
The only missing piece is an *address*: a well-known topic per session, and
something that registers it.

Two forces shape the decision:

1. **Opt-in registration cannot work.** If an agent had to subscribe to its own
   inbox before peers could reach it, then reaching an agent that had not thought
   to do so would require entering its session and asking it to — which is exactly
   the human-in-the-loop step this feature exists to remove.
2. **Baseline-on-subscribe.** A fresh subscription starts at the topic head
   (docs/01), so events published before a session subscribed are never delivered.
   A message sent to an unregistered agent is therefore *guaranteed* undeliverable,
   not merely "delivered late".

## Decision

**1. A per-session inbox topic: `agent.<session-id>`.** Minted and parsed in one
place (`mailbox_protocol::inbox_topic` / `Topic::as_agent_inbox`), alongside the
`github.pr.*` and `stub.*` schemes — parse, don't validate, so producer and
consumer cannot drift. It is fallible, not total: a `SessionId` is an opaque
harness label whose grammar is *not* a subset of the topic grammar (whitespace,
control characters, `/`, `#`), so a pathological id is refused rather than mangled.
The address space is also **injective by construction**: a session id that itself
begins with the `agent.` prefix is refused (`TopicError::SessionLooksLikeInbox`),
because minting `agent.agent.<id>` would collide under the single-strip parse that
discovery and `parse_send_target` use — it would misroute to a *different*
session. Real harness ids are UUIDs, so this only rejects a pathological id; such a
session simply gets no inbox (logged, and now visible at default verbosity).

**2. Registration is ALWAYS-ON, done by the harness.** `mailbox harness arm`
subscribes the session to its own inbox before it probes and arms — on every
`SessionStart` *and* every `Stop`, idempotently (subscribe is a no-op when already
subscribed and leaves the delivery cursor untouched, so a re-arm can never skip
unread mail). `harness cleanup` (SessionEnd) already drops every subscription, so
the inbox deregisters when the session ends. An agent does nothing to be
addressable.

**2a. A writer-enforced tombstone closes the resurrection race.** Because `arm`
re-registers on *every* `Stop` — including the final one — an arm's inbox
`Subscribe` can reach the single writer *just after* `cleanup`'s `EndSession`
delete. Subscriptions have no TTL (only watch interest is swept), so in the order
[EndSession delete] → [arm Subscribe] the inbox would be resurrected
**permanently**: an orphan waiter outlives the session, `agents` lists a corpse,
and `send`'s registration check passes for a session that no longer exists —
inverting the guarantee in decision 3. The fix is a
`session_tombstone(session_id, ended_at_ms)` table (schema v4): `EndSession`
records the end instant in the *same transaction* as the delete, and the
`Subscribe` writer path refuses to create a subscription for a session whose
tombstone is younger than `SUBSCRIBE_TOMBSTONE_GUARD_MS` (10s), returning an honest
`RefusedSessionRecentlyEnded` outcome (logged, never a silent success). A tombstone
*older* than the guard is a genuine resume long after the end: it is deleted and
the subscribe proceeds (self-healing). The 10s window is chosen because the
arm/cleanup race is sub-second while a real resume of the same id happens far
later — it covers the race with vast head room without depending on Claude Code's
unverifiable SessionStart-on-resume matcher behaviour. In BOTH interleavings the
subscription ends up deleted and tombstoned; the card-11 waiter still self-exits on
its post-lock `has_subscription` re-check, so no orphan survives.

**2b. The guard is scoped to the automatic inbox path only — explicit subscribes
are exempt.** The tombstone must NOT refuse an explicit `mailbox subscribe` /
`watch` issued by a genuinely-resumed session within the 10s window. Only ONE path
can be the doomed racing command: the automatic `register_inbox` that `arm` fires
asynchronously after a turn ends. An explicit `subscribe`/`watch`, by contrast, is
issued *synchronously from a live turn*, which by construction completes before that
turn's `SessionEnd` — so it can never be the post-teardown arm the guard defends
against. It is therefore legitimate proof-of-life. The `Subscribe` writer command
carries a `SubscribeKind` (`AutoInbox` vs `Explicit`), threaded from the request
edge exactly like `now_ms` (never inferred inside the writer). `register_inbox` — the
only caller that races teardown — is the sole `AutoInbox` (guarded) path; the
explicit `mailbox subscribe` command and the `watch`/`stub` subscribe (via
`bus.subscribe`) are `Explicit`: they proceed AND clear any tombstone, so a resumed
session's inbox is restored rather than silently left with a running poller and
interest row but no subscription (zero deliveries). Without this scoping the guard
was over-broad: an explicit re-subscribe within 10s of a prior end returned an
overall-success `Watched { subscribe: Refused }` (exit 0) yet delivered nothing. The
resurrection guarantee (2a) is untouched — the racing arm's path is still fully
guarded — because the exemption applies only to commands that cannot be that arm.

**2c. Tombstone growth, honestly.** The `session_tombstone` table holds one row per
DISTINCT session id ever ended over the daemon's lifetime (`PRIMARY KEY`, `INSERT OR
REPLACE` dedupes re-ends of the same id). A row is cleared only when that id
subscribes again — an aged `AutoInbox` re-registration past the guard window, or any
`Explicit` subscribe/watch (which clears it as proof-of-life). Most ended sessions
never resume, so their rows simply persist. Rows are tiny (a short id string + an
`i64`) and the daemon is a local, single-user process, so the unbounded-in-principle
growth is negligible in practice; there is deliberately **no dedicated sweeper in the
MVP**. If a long-lived shared deployment ever makes this matter, add a periodic sweep
of tombstones older than the guard window (they can never refuse anything once aged).

**3. `send` to an unregistered agent is a hard error.** Because of
baseline-on-subscribe, publishing to a session with no inbox subscription would
durably store a message that can never be read, while telling the sender it
worked. `mailbox send` therefore fails loudly (non-zero exit, naming the unknown
target) and publishes nothing. There is deliberately **no `--force`**: the only
thing a forced publish could produce is a message that is silently lost.

**4. Discovery is a read over existing tables.** `mailbox agents` lists the
sessions subscribed to their *own* inbox topic; `mailbox topics` lists known
topics with subscriber/event counts. No schema migration: a topic exists precisely
because something subscribed or published to it.

**5. Liveness is a live-waiter probe, not a heartbeat.** `agents` reports whether
a session's waiter pidfile names a live PID. Post-ADR-0006 that pidfile is written
by the waiter itself, only after it takes the single-waiter lock, so it reliably
names the one blocked waiter — meaning "this agent is idle and a send wakes it
now". It does **not** mean the agent is healthy, and `false` does not mean the
message will be lost (it lands durably and surfaces on the agent's next read). We
did not invent a heartbeat we do not have, and the CLI says exactly this.

> **Superseded ([ADR-0017](0017-daemon-bumps-the-sentinel.md), 2026-08-05).** The
> *shape* of decision 5 stands — liveness is a probe, never a heartbeat, and it
> answers "does this agent exist", not "is it healthy". The *signal* does not: there
> is no waiter and no pidfile. `agents` now asks the process table
> (`doctor::live_claude_sessions`, which reads Claude Code's own `--session-id` /
> `--resume` argv), and the answer is strictly better — an orphaned waiter used to
> outlive the agent it belonged to, so a dead session read as alive. It reports
> `running` / `not running`, and even `running` does not mean *wakeable*: only
> `mailbox doctor` proves that ([ADR-0016](0016-prove-wakeability-with-an-active-probe.md)).

## Consequences

- **Every live session now arms a waiter.** Registration means a live session
  always has ≥1 subscription, so the arm-iff-subscribed rule (ADR-0006) now always
  says "arm" while the bridge is up. The cost is one blocked `mailbox wait` process
  per live session — a few hundred KB of RSS, blocked in `poll`, costing no CPU.
  This is accepted, and is the price of agents being addressable by default.
- **Every fail-safe is unchanged.** A bridge that is down or erroring still means
  exit 0 and no wake (registration fails the same way the probe does, and is
  best-effort — it never fails the hook). The waiter still re-checks
  `has_subscription` after taking its lock and self-exits if a `SessionEnd` raced
  it. A session id that cannot form a topic simply gets no inbox, logged.
- **Trust model: any local same-user process can reach any inbox — but only
  through `send`.** This is the accepted boundary — one user, one machine, no TCP
  (ADR-0001/0004), and every peer agent is already running with that user's full
  authority. What is *not* accepted is bypassing provenance: the generic
  `mailbox publish` / `Request::Publish` path **refuses `agent.*` topics** (tested
  via `Topic::as_agent_inbox().is_ok()`, reusing the grammar rather than
  string-matching), because it stamps no `from` and runs no registration check —
  allowing it would let a caller forge a `from` into a victim's inbox, or write into
  an unregistered inbox where baseline-on-subscribe guarantees the message is
  unreadable. Inboxes are therefore writable *only* via `send`, which stamps the
  sender and checks the target is registered. Message bodies remain **untrusted
  data, never authority** (ADR-0001): a body may inform an agent, never instruct it.
  The `from` stamp (which the bridge writes, overwriting any caller-supplied value)
  is *provenance* — good enough to route a reply, not to authorize an action.
- **The wake never carries the message.** A peer message wakes the recipient with
  its inbox topic and who sent it — and, if the sender passed `--subject`, that one
  line ([ADR-0022](0022-the-wake-carries-a-subject.md)). The message text itself is
  read afterwards through `mailbox read`, exactly like every other event.

## Alternatives considered

- **Opt-in registration (`mailbox register`).** Rejected: an agent nobody can reach
  until it opts in is unreachable exactly when you need it, and arranging the opt-in
  requires the human step this feature removes.
- **A dedicated `inbox`/`message` table + delivery machinery.** Rejected: the bus
  already gives durable fan-out, exactly-once, cursors, and wake. A second delivery
  path would be a second set of bugs.
- **Publish to an unregistered agent and let it "catch up" on registration.**
  Rejected: baseline-on-subscribe means it never catches up. Changing baselining to
  replay history would break the property every other subscriber depends on (a new
  subscriber must not be flooded with a backlog).
- **A real heartbeat / presence protocol.** Deferred. It would need an agent-side
  periodic action, which is exactly what "agents never run background loops"
  forbids. The live-waiter probe answers the one question discovery actually needs
  ("will a send wake it right now?") using state the wake loop already maintains.
