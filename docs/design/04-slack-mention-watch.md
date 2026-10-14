# Design: Slack @mention watch

- Status: Implemented
- Related ADRs: [0005](../adr/0005-baseline-via-protocol.md),
  [0022](../adr/0022-the-wake-carries-a-subject.md),
  [0027](../adr/0027-adapter-secrets-live-in-the-keychain.md),
  [0029](../adr/0029-watch-filters-run-in-the-adapter.md),
  [0031](../adr/0031-mentions-arrive-over-socket-mode.md)
- Builds on: [design/02](02-slack-watch.md) (the Slack app, the adapter, `--skip`)

## Goal

Let a person summon an agent from Slack by @mentioning the mailbox's Slack bot. A
session runs `mailbox watch slack-mentions` once. After that, a mention of the bot in
any conversation the bot is in wakes the session within seconds, thread replies
included.

```bash
mailbox watch slack-mentions
mailbox watch slack-mentions --skip user=U0AB7RJSQBE   # not woken by that person's mentions
mailbox unwatch slack-mentions
```

## Non-goals

- **Posting.** The bot stays read-only here. The app has since been granted
  `chat:write` and friends for later work; this watch uses none of them.
- **Direct messages.** `im:history` is granted, but a DM wake is a separate watch
  (`message.im`), not built.
- **Routing.** Every session watching mentions is woken by every mention. Which
  agent answers is the agents' business, as for every other topic.
- **Catching up after a long disconnect.** See [the offline gap](#the-offline-gap).

## How a mention arrives

**Slack's `app_mention` event over Socket Mode.** The adapter opens an outbound
WebSocket to Slack with an app-level token, and Slack pushes each event down it.
No public URL is involved, so it works from a laptop. [ADR-0031](../adr/0031-mentions-arrive-over-socket-mode.md)
records why this route and not polling.

Measured on 2026-10-14 against the tryrelevance workspace, in Mikey's private
channel `#mikeys-private-test-arena`:

| Mention | Arrived | `event_time` → published |
|---|---|---|
| Top-level message | yes | 0 s |
| Reply in a thread | yes, with `thread_ts` set | 1 s |
| Through the bridge, top-level | yes; the session was woken through its inbox socket | 0 s |

So `app_mention` fires for thread replies (measured; Slack's event page does not say)
and in a private channel the bot was invited to (measured).

### What the app needs

All four were done by Mikey on 2026-10-13/14; the scopes are as `auth.test` reported
on 2026-10-14.

| Setting | Where |
|---|---|
| Bot scope `app_mentions:read` | Approved by the workspace admin |
| Socket Mode on | App config → Socket Mode |
| Bot event `app_mention` | App config → Event Subscriptions |
| App-level token with `connections:write` | App config → Basic Information → App-Level Tokens |

The app-level token (`xapp-…`) is a second Keychain item beside the bot token:

```bash
security add-generic-password -U -s agent-mailbox.slack-app-token -a "$USER" -w
```

`-w` last, with no value, makes `security` prompt for the token, so it never sits
on a command line or in shell history. The bot must also be **in** a conversation to
hear mentions there: `/invite @<bot>` (Slack's `app_mention` docs).

## Shape

```text
mailbox watch slack-mentions [--skip …]
  → daemon records the one mention watch (kind slack-mentions, key "bot"), subscribes
  → supervisor → SlackResolver → mailbox-slack-adapter
       config {kind: "slack-mentions", topic: "slack.mentions", skip, baseline}
  → adapter: both tokens from the Keychain
       → auth.test (bot token, via curl): workspace URL for permalinks
       → apps.connections.open (app token, via curl): a one-time wss:// URL
       → WebSocket: hello, then envelopes
  → per app_mention: Publish, then Baseline, then ack
```

- **One mode of the existing Slack adapter**, chosen by the config's `kind`. A config
  with no `kind` is still a channel or thread watch, so a daemon that predates this
  watch can drive the new binary (the same rule [ADR-0030](../adr/0030-review-requests-are-a-mode-of-the-github-adapter.md)
  set for the GitHub adapter).
- **One mention watch per bridge.** A bridge reaches one Slack app, whose tokens are
  in the Keychain, so there is one bot to be mentioned. The topic is `slack.mentions`
  and names no entity. The watch row stores `bot` in its key column.
- **No interval.** Nothing is polled. `status` shows `interval=push`.

### Frames

Slack sends three kinds of text frame ([Socket Mode](https://docs.slack.dev/apis/events-api/using-socket-mode)),
parsed once in `socket.rs`:

| Frame | The adapter |
|---|---|
| `hello` | Logs `num_connections`; warns when it is above 1 (below) |
| `disconnect`, reason `refresh_requested` / `warning` / other | Closes and connects again at once |
| `disconnect`, reason `link_disabled` | Exits non-zero: Socket Mode was turned off |
| An envelope (`events_api`, `slash_commands`, `interactive`, …) | Acknowledges it, whatever it carries |
| An `events_api` envelope carrying `app_mention` | Disposes of the mention (below), then acknowledges |
| Anything else | Logged by type and ignored |

The payload carries the message text and a legacy verification token. Neither is
deserialized. `AppMention` reads the channel, `ts`, `thread_ts`, `user`, `bot_id`,
`app_id`, `event_id` and `event_time`.

### One mention, one wake

Slack redelivers an event it thinks was not acknowledged, with `retry_attempt` above
0. The adapter:

1. Drops a mention whose `event_id` it has already published.
2. Otherwise applies the `--skip` filters.
3. Publishes what is left.
4. Emits the published `event_id`s as its baseline, `{"seen_event_ids": […]}`, keeping
   the newest 256.
5. Acknowledges the envelope.

Publishing before acknowledging means a crash between the two gets the mention
redelivered and woken twice, rather than lost. The persisted set means a redelivery
that lands after a restart is still dropped. A retry is **not** dropped for being a
retry. The first attempt may have gone to a connection that died before the adapter
read it.

That a redelivery carries the original `event_id` is inferred. It was secondhand,
from a library's docs, when the previous agent looked. Slack's Events API page calls
`event_id` "globally unique" but does not say whether a retry keeps it. A real
duplicate was not observed in testing.

### Subject and body

The subject is `mention from <name> in #<channel>` or `mention from <name> in a thread
in #<channel>`, linked to the message (with `?thread_ts=…&cid=…` for a reply). Names
come from `users.info` and `conversations.info`, cached per process, and bounded to 2
seconds per mention so the ack is not held up. A lookup that fails or runs out of
time falls back to the id.

The body is `{kind: "slack_mention", channel, ts, thread_ts?, user?, bot_id?, app_id?,
event_id, permalink}`. **It never carries the text.** The woken agent reads the
message through its own Slack access.

### Connections end

- **Refresh.** Slack recycles connections "every few hours" (its docs). The adapter
  reconnects at once, and a refresh does not count as a failure.
- **Dead socket.** The adapter pings every 30 s. A connection with no frame for 90 s
  is dropped and replaced. This is how a laptop waking from sleep notices that its
  socket is gone. The interval is a chosen value; Slack documents no keepalive.
- **Failures.** A lost connection, or one that will not open, is retried after 1 s,
  doubling to 60 s. After 10 in a row (`MAILBOX_SLACK_MAX_TRANSIENT_FAILURES`), the
  adapter exits non-zero. The supervisor backs off, gives up, and its sweep retries
  the watch while a session still wants it (ADR-0011). A connection that said `hello`
  resets the count.

## The offline gap

**Mentions made while no connection is open are redelivered for a few minutes, then
lost. The gap is accepted and documented; there is no polling backfill.**

What is known:

- **Measured** (2026-10-14): with the adapter stopped for 57 s, Mikey posted
  two mentions. Both arrived after it reconnected, each 60–61 s after it was posted,
  with `retry_attempt` 2. They did not arrive when the socket opened; they arrived on
  Slack's retry schedule.
- **Documented** for the Events API in general: Slack retries a failed delivery
  "nearly immediately", after 1 minute, and after 5 minutes, then stops
  ([Events API](https://docs.slack.dev/apis/events-api/)). The Socket Mode page says
  only that "you may lose events until you establish a connection".
- **Inferred:** a mention is redelivered if a connection is back within about 5
  minutes of it, and lost after that. The 5-minute retry was not measured; Mikey
  stopped the measurement there.

So a laptop that sleeps through a mention (a lunch break, overnight) misses it.
Nothing tells the agent; the person who mentioned the bot sees no reply.

Why no backfill:

- A backfill has to find mentions in Slack history. The bot token cannot call
  `search.messages` (user tokens only, per Slack's method page). So it would read
  `conversations.history` for every conversation the bot is in, plus
  `conversations.replies` for every thread with new replies, and match `<@bot>` in
  the text. That is the most expensive read pattern Slack has, and it is new code that
  reads message text.
- The use is interactive: someone summons an agent and waits for it. A mention found
  hours later is better answered by the person asking again.

Two things could close the gap later, both unmeasured:

- **Slack's "Delayed Events"** setting: "hourly retries for 24 hours" after the three
  retries (Events API page). Whether it applies to Socket Mode is not documented. It
  is an app setting Mikey can turn on; then a mention made during sleep would be
  measured again.
- **A backfill pass on reconnect** as described above, if missed mentions turn out to
  matter.

Slack disables event delivery for an app failing over 95% of deliveries in an hour,
but "apps receiving less than 1,000 events per hour will not be automatically
disabled" (Events API page). A sleeping laptop fails every delivery, but this bot's
volume is far below that floor (inferred from use, not measured).

## One consumer per app

Slack allows 10 connections per app and sends each event to **one** of them: "each
payload may be sent to any of the connections" (Socket Mode page). So two bridges
running a mention watch for the same app, on two machines or a test daemon beside the
real one, each get only some of the mentions. The adapter warns when `hello` reports
more than one connection. It cannot fix this; the remedy is one mention watch per app.

## Failure modes

| Failure | Behaviour |
|---|---|
| No app token, or no bot token, in the Keychain | Exit non-zero at start, naming the `security add-generic-password` command |
| A token stored under the wrong item (`xoxb-` as the app token) | Exit non-zero at start: malformed |
| `apps.connections.open` or `auth.test` rejects its token | Exit non-zero |
| Socket Mode turned off (`link_disabled`) | Exit non-zero |
| Offline, Slack 5xx, rate limited, WebSocket will not open | Back off and retry; exit non-zero after 10 in a row |
| Silent socket (sleep, network change) | Replaced after 90 s |
| A malformed `app_mention` | Acknowledged, logged by field, dropped |
| Another consumer of the same app | Warned at `hello`; mentions are split |

## Test plan

- **Frames** (`socket.rs`): hello, each disconnect reason, an `app_mention` with and
  without a thread, other envelopes keep their id for the ack, a malformed mention is
  still acked, garbage quotes nothing.
- **Disposition** (`mentions.rs`, in-memory fake Slack): subject and link, thread
  replies, a redelivery dropped only once published, filters, cached and failed
  lookups, the body never carries text, the seen set is bounded and round-trips.
- **Adapter e2e** (`tests/mention_e2e.rs`): the real binary against a fake Socket Mode
  server on loopback, a fake `curl` and a fake `security`. Covered: every envelope
  acked, a redelivery dropped, a refresh reconnects, a redelivery after a restart
  dropped by the injected baseline, a silent socket replaced, `link_disabled` fatal,
  a missing or rejected app token fatal, and no token or ticket in argv or logs.
- **Bridge e2e** (`crates/mailbox/tests/slack_mentions_e2e.rs`): `watch
  slack-mentions --skip` through the daemon reaches a fake adapter with the right
  config, its publish reaches `read`, another session's different filters are
  refused, and `unwatch` stops it.
- **Live**, by hand on 2026-10-14: the table under [How a mention arrives](#how-a-mention-arrives)
  and the offline measurement above.
