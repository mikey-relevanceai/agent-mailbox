# ADR-0029: A watch's filters belong to the watch, and its adapter applies them before publishing

- Status: Proposed
- Date: 2026-10-09
- Amends: [design/02](../design/02-slack-watch.md)'s "Authorship is not filtered".
  Nothing is filtered by default; a watch can now opt in.

## Context

Mikey's chief-of-staff agent watches `#team-arg-agent-watercooler` and several
threads in it. It posts to Slack through the claude.ai connector, and the connector
posts **as Mikey's own user**. So every post and reply the agent makes comes back
as "new message from Mikey Hudson" and wakes it. On 2026-10-09 he estimated that was
half its wakes; that figure is his, not measured here. He still wants to be woken by
what he types himself, because he talks to his agent in that channel. A filter on
his user alone would silence him too.

### What tells the two apart, measured

All 141 messages in C0C83CXLUL8 on 2026-10-09 (47 top-level from
`conversations.history`, 94 replies from `conversations.replies` across 36
threads), read with the adapter's bot token:

| Messages | `app_id` | `client_msg_id` | "Sent using Claude" footer |
|---|---|---|---|
| 109, by two users | `A08SF47R6P4` | absent | present |
| 22, by the same two users | absent | present | absent |
| 10 (joins, giphy, the Arg bot) | absent or another app | absent | absent |

The footer is what independently marks a post as the connector's, and it agreed
with `app_id` on all 109. That the 22 were typed by hand is inferred from their
`client_msg_id`, which Slack clients attach. Only the two Mikey named as his own
typing are confirmed. The connector posts have no `bot_id`, `bot_profile` or
`subtype`. Mikey's named examples fit: 1791502779.408819 and 1791502844.447549
carry `app_id: A08SF47R6P4`, and 1791502526.986199 and 1791502873.184069 carry a
`client_msg_id` and no `app_id`.

This is one channel in one workspace (tryrelevance) on one day. Whether the
connector's app id is the same in every workspace was not checked.

`app_id` names the app positively. A missing `client_msg_id` only says "not typed in
a Slack client", which other integrations share. The footer is text, which the
adapter deliberately never deserializes (design/02). So the filter matches on
`user` and `app_id`.

### Where a filter can run

- **In the bridge, per subscriber.** Store every event and skip it at wake time for
  the sessions whose filter matches. The right shape for a bus, since each
  subscriber chooses for itself. But the bridge never interprets an event body
  (ADR-0001). It would have to start reading adapter-defined bodies, or adapters
  would need a new channel of bridge-readable attributes plus a per-subscription
  filter table that the unread count, `read`, the wake digest and session
  suspend/resume all honour. A text filter could never run there, because the text
  never reaches the bridge.
- **In the adapter, per watch.** The adapter already decides which messages wake
  (joins, edits and thread parents are skipped there). A filter is one more rule in
  that same place. The cost is that one adapter serves every session watching an
  entity, so its filters are shared by all of them.

## Decision

1. **Filters are part of a watch's configuration, passed to its adapter in the
   spawn config, and applied by the adapter before it publishes.** A matched
   message is skipped the way a join is: read, passed by the cursor, never
   published. The bridge stores no event for it, so nothing reaches `mailbox
   read`, and no wake goes out for it. Each skip is logged by the adapter at `info`
   with the message `ts` and the filter that matched.

2. **Dropped, not stored.** For a Slack watch the message itself lives in Slack. The
   mailbox event is only a pointer to it, so dropping the pointer loses nothing an
   agent cannot read from Slack. Storing filtered events would buy durability for
   copies of data that is already durable, and it would need the bridge-side
   machinery above.

3. **The first filter vocabulary is Slack's**, `--skip key=value[,key=value]` on
   `watch slack-channel` and `watch slack-thread`, repeatable. A filter matches when
   every condition holds, and a message is skipped when any filter matches. The keys
   built are `user=<U…>` and `app=<A…>`. Mikey's filter is:

   ```bash
   mailbox watch slack-channel C0C83CXLUL8 --skip user=U0AB7RJSQBE,app=A08SF47R6P4
   ```

   Filters are typed (`SlackFilter` in `mailbox-protocol`), parsed at the CLI edge,
   stored as canonical JSON in a new `watch.filters` column (schema v9), and parsed
   again when the row is read, so a corrupt one fails the watch rather than running
   it unfiltered. Another watch kind adds its own vocabulary the same way. There is
   no generic key/value predicate language over event bodies.

4. **The flag states the whole set.** A re-watch with no `--skip` clears the
   filters, the same way a re-watch resets `--interval`. The daemon echoes the
   filters it recorded, and the CLI fails if they differ from what it sent. A
   daemon that predates this ADR ignores the unknown field, records the watch
   unfiltered and echoes nothing, so success would otherwise be reported for a
   filter that is not running.

5. **Filters are shared, so changing them under another session is refused.**
   `watch` with a filter set different from the stored one:
   - another session holds live interest in the watch → **refused**. Nothing is
     written, and the error names the current filters, so the caller can share
     them or ask the other session.
   - otherwise → the filters are replaced and the running adapter is **respawned**:
     stopped, waited for until it exits (so its final baseline is flushed and two
     adapters never poll one entity), then started with the new config. Without
     the respawn the change would wait for the adapter's next crash.

   The check and the write run in one writer transaction, so two sessions racing
   to watch one entity with different filters cannot both pass the check.

## Consequences

- Mikey's chief of staff stops being woken by its own posts and keeps being woken
  by his typing. Inferred from the measurement above plus the tests. Not observed
  in production at the time of writing: this version was not yet deployed.
- **The filter also silences peers that post the same way.** Any other agent of
  Mikey's that posts through the same connector posts as the same user through the
  same app, so a chief of staff with this filter no longer hears it in that channel
  either. That is design/02's identity problem, unchanged: a filter can only see
  who posted and through what.
- Two sessions that want different filters on one entity cannot both watch it.
  On 2026-10-09, `mailbox status` on Mikey's machine showed every C0C83CXLUL8
  watch (the channel and seven threads) with an interest of 1. If that changes, the
  per-subscriber design above is the upgrade path.
- "Another session" is one holding a live `watch_interest` row, not one proved
  alive. A hard-killed session keeps its row until the TTL sweep suspends it, so
  until then it still blocks a change to the watch's filters.
- The filters' serde form is persisted in `watch.filters`, so changing
  `SlackFilter`'s wire shape is a schema migration. A row that no longer parses
  fails the watch as corrupt rather than running it unfiltered.
- A suspended session's interest (ADR-0026) does not count as "another session"
  for the refusal. A session that comes back gets the watch's filters as they are
  by then.
- A text filter is possible later, in the adapter, without the bridge ever seeing
  the text. Not built.
- `mailbox status` shows each watch's filters (`skip=[…]`, and `skip` in `--json`).

## Alternatives considered

- **Filter in the bridge, per subscriber** (above). Correct for shared watches, but
  it breaks body opacity or needs a second attribute channel, it touches every
  read path, and it can never filter on text. Rejected for now.
- **Filter identity in the topic** (a filtered watch is a different watch, with its
  own adapter and topic). No sharing conflict, but topic names would carry filter
  hashes, `unwatch` would need the filter to find the watch, and the poll count
  doubles. Rejected.
- **Last writer wins**, like `--interval`. Simplest, but a second session's plain
  re-watch would silently remove the first session's filter and bring its noise
  back. Rejected.
- **A config file of filters.** It would be a second source of truth beside the
  watch row, and every other watch parameter is a flag. Rejected.
- **Match the footer, or the absence of `client_msg_id`.** See Context. Rejected.
