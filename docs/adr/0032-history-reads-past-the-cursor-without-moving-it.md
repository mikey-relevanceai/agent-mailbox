# ADR-0032: History reads past the cursor without moving it

- Status: Accepted
- Date: 2026-10-20

## Context

`read` is the only way to see an event body, and `read` consumes. Delivery and
cursor advance commit in one transaction per topic
(`read_topic_unread`) — correct exactly-once delivery, right up until the turn
carrying the result dies.

That is the failure this ADR is written for. An agent is woken, runs `mailbox
read`, the daemon commits the cursor, and then the model turn is lost — an API
error, a dropped connection, a context compaction that eats the tool result. The
events are now read and the agent never saw them. `mailbox status` reports zero
unread, so the agent looks caught up while having acted on nothing. Nothing in
the system says otherwise, and nothing can hand the mail back.

The mail itself is not lost. The `event` table is append-only and nothing prunes
it — no retention, no compaction, no `DELETE FROM event` anywhere in the tree.
Only the *read-state* is lost, and read-state is recoverable by looking.

Three things already point at the surface that closes this:

- **The principle is ours already.** [ADR-0016](0016-prove-wakeability-with-an-active-probe.md)
  rejected a probe that recomputed the topic set as "an observation that alters
  the thing observed". `unread_digest` and `list_topics` are pure reads for the
  same reason, and `mailbox topics` already reports every topic's full historical
  event count to a caller with no session at all. The *volume* of past mail is
  already a public, non-consuming read; only the rows were missing.
- **A non-consuming read of the log already exists internally.**
  `Storage::read_events` is a complete, tested, cursor-addressed read that
  advances nothing. Its callers are all tests, but unlike the `wait`/`arm`
  primitives [ADR-0019](0019-remove-the-observability-and-re-arm-surfaces.md)
  deleted, its semantics are load-bearing: the durability assertions across the
  bus, supervision and storage suites are written against it, including the
  "a cursor past the end must not replay the log" invariant. It is the proof
  that reading the log without consuming it is already a settled idea here.
- **No ADR forbids it.** [ADR-0007](0007-always-on-agent-inboxes.md) rejects
  making *subscription* replay implicitly, because a new subscriber must not be
  flooded with a backlog. An explicit, opt-in read that moves no cursor is the
  opposite mechanism and is untouched by it.

## Decision

Add `mailbox history` — a read-only view over the durable log that **never moves
a delivery cursor**.

```bash
mailbox history                       # newest events across my subscribed topics
mailbox history --topic <topic>       # one topic, subscribed or not
mailbox history --since 2h            # only events newer than this
mailbox history --limit 20
```

Four properties make it safe to add:

1. **It advances nothing.** Not the delivery cursor, not anything else. Recovering
   mail through `history` leaves it read, so nothing re-wakes and nothing is
   delivered twice.
2. **It is a view, not a delivery path.** ADR-0007 rejected "a dedicated
   inbox/message table + delivery machinery" because "a second delivery path
   would be a second set of bugs". `history` reads the same rows the same bus
   already stored. It is not addressed, not subscribed to, not woken on, and it
   returns its own wire type rather than `Delivery`, whose stated invariant is
   "a completed, ordered read whose cursors have already advanced" — reusing that
   type would lie in the type system.
3. **It reports rows, never inferences.** ADR-0019's other surviving lesson is
   that a view wrong in both directions is worse than no view. Every field
   `history` emits is a stored value or a comparison of two stored values.
4. **It marks read-state without owning it.** Each entry carries `read`: whether
   that offset is at or before the session's cursor. Present only when the caller
   is a session subscribed to that topic — a caller with no subscription has no
   read-state, and the field is omitted rather than guessed.

**Mailboxes are not private.** `--topic agent.<other-session>` is allowed and
documented as allowed. Every session runs as the same user against the same
SQLite file; a refusal here would be advice, not a boundary, and could be
stepped around with `sqlite3`. This project does not claim a permission class it
cannot enforce ([ADR-0020](0020-peer-inbox-socket-is-the-wake-wire.md)) — the
honest surface is the one that says what it is.

**The session is optional**, in the bucket with `status`, `agents` and `send`.
With a session, `history` defaults to that session's subscribed topics and marks
read-state. With none, `--topic` is required and `read` is omitted from the
output. Naming neither a session nor a topic is refused, because "newest events
across the topics belonging to nobody" has no answer.

## Consequences

- **A lost turn stops being lost mail.** The recovery is `mailbox history`, and
  the read-state marking is what tells the agent which of those events it had
  already consumed when the turn died.
- **This helps two neighbouring cases for free**: a context compaction that ate
  the bodies, and a fresh session wanting to see what its predecessor was told.
- **`read` is unchanged.** Advance-on-read stays exactly what it was; `history`
  adds a way to look, never a way to consume differently. An agent that browses
  with `history` and never calls `read` accumulates unread and keeps being woken
  — that is correct, and the docs say so.
- **Two read primitives now sit side by side**, deliberately.
  `Storage::read_events` pages *forward* from a cursor and stays the substrate
  the durability tests assert against; `recent_events` reads *backwards* from the
  head with read-state marking, which is the shape a recovery question needs
  ("what are the last N things, and which had I already seen?"). Neither is
  expressible as the other without contorting it, and a future
  `history --before <offset>` is where the forward primitive would surface to
  users. If that never arrives, collapsing the two is a fair later call.
- **Cross-topic ordering needs the global row id.** Offsets are per-topic and
  cannot order a cross-topic list, so the default view orders by
  `event.event_row_id` — the global monotonic sequence the schema already mints
  ids from. The `(topic, offset)` index does not serve that scan; at local mail
  volumes a bounded backwards scan is the right trade, and an index is a later
  change if a log ever grows enough to matter.
- **Unbounded log growth becomes slightly more visible**, since a command now
  reads old rows on purpose. It remains the same posture the `session_tombstone`
  comment already records: no sweeper in the MVP, add one if a long-lived
  deployment ever makes it matter.

## Alternatives considered

- **Rewind the cursor** (`read --rewind N`, or a settable cursor). Rejected: it
  would re-wake the session, multiplying the exact failure it is meant to fix,
  and `storage/mod.rs` deliberately exposes no `advance_cursor` because "no
  caller may nudge it independently (that could silently skip events)". History
  gives read access without touching delivery semantics at all.
- **Two-phase read: `read` returns, `ack` advances.** This is the honest
  root-cause fix — at-least-once with an explicit commit — and it is rejected for
  now. It inverts advance-on-read everywhere, and a forgotten `ack` means a
  session woken forever about mail it has already handled. That is a worse
  failure than the one being fixed, and it is a failure of the *delivery* path
  rather than a recoverable gap beside it. Recorded here so it is not
  relitigated as a bug.
- **A `--replay-last` slot: the daemon remembers each session's last delivered
  batch.** Rejected as new state that a newest-first `history` already subsumes.
  "What did I just get" is a limit, not a mechanism.
- **Extend `read` with a `--history` flag.** Rejected: `read`'s contract is
  "unread events, advancing the cursor", and a flag that suspends the second
  half of a verb's contract is how a verb stops meaning anything. A separate
  command keeps `read` sacred.
- **Do nothing; agents can use `sqlite3`.** Rejected: it works, which is the
  problem. Every command except `doctor` is a socket client precisely so nothing
  else opens the DB ([ADR-0003](0003-single-writer-sqlite.md)), and an escape
  hatch the docs have to teach is a surface with no tests, no JSON contract and
  no refusals.
