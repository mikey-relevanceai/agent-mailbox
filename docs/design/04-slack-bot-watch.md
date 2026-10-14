# Design: Slack bot watch (@mentions and DMs)

- Status: Implemented
- Related ADRs: [0005](../adr/0005-baseline-via-protocol.md),
  [0022](../adr/0022-the-wake-carries-a-subject.md),
  [0027](../adr/0027-adapter-secrets-live-in-the-keychain.md),
  [0029](../adr/0029-watch-filters-run-in-the-adapter.md),
  [0031](../adr/0031-the-bot-watch-listens-over-socket-mode.md)
- Builds on: [design/02](02-slack-watch.md) (the Slack app, the adapter, `--skip`)

## Goal

Let a person summon an agent from Slack by @mentioning the mailbox's Slack bot or
sending it a DM. A session runs `mailbox watch slack-bot` once. After that, either
one wakes the session within seconds:

- a mention of the bot, in any conversation the bot is in, thread replies included;
- a DM to the bot.

```bash
mailbox watch slack-bot
mailbox watch slack-bot --skip user=U0AB7RJSQBE   # not woken by that person
mailbox unwatch slack-bot
```

## Non-goals

- **Posting.** This watch never posts. The bot does post now, for Dispatch through
  its own code (measured by the orchestrator on 2026-10-15); this watch only makes
  sure those posts do not wake it (below).
- **Group DMs** (`message.mpim`) and **reactions**. The scopes are granted; neither
  is subscribed or built.
- **Routing.** Every session watching the bot is woken by every message to it.
  Which agent answers is the agents' business, as for every other topic.
- **Catching up after a long disconnect.** See [the offline gap](#the-offline-gap).

## How a message arrives

**Slack's `app_mention` and `message.im` events, over Socket Mode.** The adapter
opens an outbound WebSocket to Slack with an app-level token, and Slack pushes each
event down it. No public URL is involved, so it works from a laptop.
[ADR-0031](../adr/0031-the-bot-watch-listens-over-socket-mode.md) records why this
route and not polling.

Measured against the tryrelevance workspace. The mentions were in Mikey's private
channel `#mikeys-private-test-arena`; the DMs were in his DM with the bot:

| Date | Message | Result | Published after `event_time` |
|---|---|---|---|
| 2026-10-14 | Top-level mention | woke | 0 s |
| 2026-10-14 | Mention in a thread reply, `thread_ts` set | woke | 1 s |
| 2026-10-14 | Top-level mention, through the bridge | woke this session through its inbox socket | 0 s |
| 2026-10-15 | DM, through the bridge | woke as `DM from Mikey Hudson` | 0 s |
| 2026-10-15 | An edit of that DM | two `hidden` events, both skipped | — |
| 2026-10-15 | Mention, on the same connection as the DM | woke | 0 s |

So `app_mention` fires for thread replies, which Slack's event page does not say,
and in a private channel the bot was invited to. The claude.ai Slack connector read
the DM by its channel id (measured), so a woken agent can follow the link.

### What the app needs

All of these were done by Mikey on 2026-10-13 to 15. The scopes are as `auth.test`
reported them on 2026-10-14.

| Setting | Where |
|---|---|
| Bot scopes `app_mentions:read`, `im:history`, `im:read` | Approved by the workspace admin |
| Socket Mode on | App config → Socket Mode |
| Bot events `app_mention` and `message.im` | App config → Event Subscriptions |
| Messages Tab, with "Allow users to send … messages" | App config → App Home |
| App-level token with `connections:write` | App config → Basic Information → App-Level Tokens |

The app-level token (`xapp-…`) is a second Keychain item beside the bot token:

```bash
security add-generic-password -U -s agent-mailbox.slack-app-token -a "$USER" -w
```

`-w` comes last with no value, so `security` prompts for the token. It never sits
on a command line or in shell history. To hear mentions in a conversation, the bot
must also be **in** it: `/invite @<bot>` (Slack's `app_mention` docs).

## Shape

```text
mailbox watch slack-bot [--skip …]
  → daemon records the one bot watch (kind slack-bot, key "bot"), subscribes
  → supervisor → SlackResolver → mailbox-slack-adapter
       config {kind: "slack-bot", topic: "slack.bot", skip, baseline}
  → adapter: both tokens from the Keychain
       → auth.test (bot token, via curl): workspace URL, the bot's user and bot ids
       → apps.connections.open (app token, via curl): a one-time wss:// URL
       → WebSocket: hello, then envelopes
  → per message to the bot: Publish, then Baseline, then ack
```

- **One watch for mentions and DMs, because one connection must carry both.** Slack
  sends each event to only one of an app's open connections (Socket Mode page). A
  separate DM watch would open a second connection, and each adapter would then get
  about half of both kinds and drop the half it does not handle. For the same
  reason, there is one bot watch per bridge.
- **A third mode of the Slack adapter**, chosen by `kind: "slack-bot"` in the config.
  A config with no `kind` is still a channel or thread watch. So a daemon that
  predates this watch can still drive the new binary, the rule
  [ADR-0030](../adr/0030-review-requests-are-a-mode-of-the-github-adapter.md) set for
  the GitHub adapter. The `kind` string is `SLACK_BOT_CONFIG_KIND` in
  `mailbox-protocol`, shared by the resolver and the adapter.
- **Topic `slack.bot`, key `bot`.** The topic names no entity. A bridge reaches one
  Slack app, whose tokens are in the Keychain. The watch row stores `bot` in its key
  column and a zero interval. A row with anything else is corrupt.
- **No interval.** Nothing is polled. `status` shows `interval=push`.

### Frames

Slack sends three kinds of text frame
([Socket Mode](https://docs.slack.dev/apis/events-api/using-socket-mode)). They are
parsed once, in `socket.rs`:

| Frame | The adapter |
|---|---|
| `hello` | Logs `num_connections`, and whether this is a reconnect and how long the gap was. Warns when `num_connections` is above 1 (below) |
| `disconnect`, reason `refresh_requested` / `warning` / other | Closes and connects again. At once if this connection said `hello`; otherwise it counts as a failure and backs off |
| `disconnect`, reason `link_disabled` | Exits non-zero: Socket Mode was turned off |
| Any envelope (`events_api`, `slash_commands`, `interactive`, …) | Acknowledged, whatever it carries |
| `events_api` with `app_mention`, or `message` with `channel_type: "im"` | Disposed of (below), then acknowledged |
| Either of those with an unparseable channel, `ts` or `thread_ts` | Logged with its envelope id, dropped, acknowledged |

The payload carries the message text and a legacy verification token. Neither is
deserialized. A message is read as channel, `ts`, `thread_ts`, `user`, `bot_id`,
`app_id`, `subtype`, `hidden`, `event_id` and `event_time`.

### What wakes

The rules run in this order, and the first that matches decides:

| Message | Wakes? |
|---|---|
| Already published (same `<conversation>/<ts>`) | no: a redelivery |
| `hidden` (an edit's event) | no |
| Subtype `message_changed`, `message_deleted`, `bot_message`, or any other | no: nobody said anything to the bot |
| Subtype `file_share`, `me_message`, `thread_broadcast`, or none | continue |
| Posted by the bot itself (its `user` or its `bot_id`, from `auth.test`) | no |
| Matches a `--skip` filter | no |
| Otherwise | **wakes** |

The bot's own DM posts were seen in two forms once Dispatch began posting as it
(measured by the orchestrator on 2026-10-15). One has the bot's user and no subtype;
the other has subtype `bot_message` and no user. Both carry `bot_id` B0C74MURRK5 and
app `A0C7AJ9AMUJ`. The first is skipped as the bot's own post and the second as a
`bot_message`. Each form has a unit test.

### One message, one wake

Slack redelivers an event it thinks was not acknowledged, with `retry_attempt` above
0. For each message, the adapter:

1. Applies the table above. A message already published is dropped.
2. Publishes it.
3. Emits the published messages as its baseline, `{"seen_messages":
   ["<conversation>/<ts>", …]}`, keeping the newest 256.
4. Acknowledges the envelope.

Publishing before acknowledging means a crash between the two gets the message
redelivered and woken twice, rather than lost. The persisted set means a redelivery
after a restart is still dropped. A retry is **not** dropped for being a retry,
because the first attempt may have gone to a connection that died before the adapter
read it. Messages are keyed by where they are, not by `event_id`. That needs nothing
from Slack's retry semantics, which its docs do not state. It also stops one message
that arrives as two events (a DM that @mentions the bot) from waking twice; whether
Slack sends both for that was not checked.

### Subject and body

| Message | Subject |
|---|---|
| Mention | `mention from <name> in #<channel>` |
| Mention in a thread | `mention from <name> in a thread in #<channel>` |
| DM | `DM from <name>` |
| DM in a thread | `DM from <name> in a thread` |

Each subject links to the message, with `?thread_ts=…&cid=…` for a reply. Names come
from `users.info` and `conversations.info`, cached per process. The lookups for one
message are bounded to 2 seconds so they do not hold up the ack. A lookup that fails
or runs out of time falls back to the id, and the publish log line says
`names_resolved=false`.

The body is `{kind: "slack_mention" | "slack_dm", channel, ts, thread_ts?, user?,
bot_id?, app_id?, subtype?, event_id?, permalink}`. **It never carries the text.**
The woken agent reads the message through its own Slack access.

### Connections end

- **Refresh.** Slack recycles connections "every few hours" (its docs). The adapter
  reconnects at once after a refresh on a connection that said `hello`. It closes
  the old connection before opening the new one. A message Slack sends in that gap
  goes unacknowledged and should arrive on its 1-minute retry (inferred from the
  offline measurement below); opening the new one first would avoid even that, and
  was not built.
- **Dead socket.** The adapter pings every 30 s and drops a connection that has
  carried nothing for 90 s. It measures that silence on the wall clock as well as
  the monotonic one. On macOS the monotonic clock is not expected to advance while
  the machine sleeps (inferred from how Rust's `Instant` is built there; not
  measured). After a sleep, the wall clock shows the gap at the first ping tick,
  within about 30 s of waking. The intervals are chosen values; Slack documents no
  keepalive.
- **Failures.** A connection that fails to open or is lost is retried after 1 s,
  with the wait doubling up to 60 s, or longer if Slack sent a `Retry-After`. After
  10 in a row (`MAILBOX_SLACK_MAX_TRANSIENT_FAILURES`) the adapter exits non-zero.
  A connection that said `hello` resets the count. In practice, while offline, that
  exit comes after about four minutes. That is longer than the supervisor's 60 s
  `reset_after`, so the supervisor treats each run as stable and restarts it at once.
  An offline adapter is therefore retried indefinitely and never marked `failed`.
  Only a fast exit leads to `failed`: a missing or rejected token, or Socket Mode
  turned off. This is inferred from the constants in the code, not measured.
- **Orphaned.** On each ping tick, the adapter checks its parent pid. If the daemon
  that started it is gone (killed with SIGKILL, say), it exits. Otherwise it would
  hold a connection and take a share of the bot's events, which it can no longer
  publish.

## The offline gap

**A message sent while no connection is open is redelivered for a few minutes, and
lost after that. The gap is accepted and documented; there is no polling backfill.**

What is known:

- **Measured** (2026-10-14): Mikey posted two mentions while the adapter was stopped
  for 57 s. Both arrived after it reconnected, each 60–61 s after it was posted, with
  `retry_attempt` 2. They did not arrive when the socket opened; they arrived on
  Slack's retry schedule.
- **Documented** for the Events API in general: Slack retries a failed delivery
  "nearly immediately", after 1 minute, and after 5 minutes, then stops
  ([Events API](https://docs.slack.dev/apis/events-api/)). The Socket Mode page says
  only that "you may lose events until you establish a connection".
- **Inferred:** a message is redelivered if a connection is back within about 5
  minutes of it, and lost after that. The 5-minute retry was not measured; Mikey
  stopped the measurement there. DMs were assumed to behave as mentions do; that was
  not measured separately.

So a laptop that sleeps through a mention or DM (a lunch break, overnight) misses
it. Nothing tells the agent, and the person sees no reply.

Why no backfill:

- A backfill has to find missed messages in Slack history. The bot token cannot call
  `search.messages`: its method page lists a user-token scope only. So it would read
  `conversations.history` for every conversation the bot is in, plus
  `conversations.replies` for every thread with new replies, and match `<@bot>` in
  the text. That is the most expensive read pattern Slack has, and it is new code
  that reads message text.
- The use is interactive: someone summons an agent and waits for it. A message found
  hours later is better answered by the person asking again.

Two things could close the gap later. Neither has been tried:

- **Slack's "Delayed Events"** setting adds "hourly retries for 24 hours" after the
  three retries (Events API page). Whether it applies to Socket Mode is not
  documented. It is an app setting Mikey can turn on, after which the gap would be
  measured again.
- **A backfill pass on reconnect**, as described above, if missed messages turn out
  to matter.

Slack disables event delivery for an app that fails over 95% of deliveries in an
hour. But "apps receiving less than 1,000 events per hour will not be automatically
disabled" (Events API page). A sleeping laptop fails every delivery. This bot is
addressed far less often than that floor (inferred from use, not measured).

## Logging

Adapters run with tracing's default `ERROR` filter unless `RUST_LOG` says otherwise.
On 2026-10-15 every one of the 2,123 Slack adapter lines in the live daemon log was
`ERROR` (measured by a reviewer). So the two signs that wakes are being lost are
logged at `error`, where they reach the bridge log: another Socket Mode consumer on
the same app, and a message dropped as malformed. Everything else (connects,
reconnects, each publish and skip) is `info` or `debug`, for a run with `RUST_LOG`.

## One consumer per app

Slack allows 10 connections per app and sends each event to **one** of them: "each
payload may be sent to any of the connections" (Socket Mode page). So two bridges
running a bot watch for the same app split the messages between them. That could be
two machines, or a test daemon beside the real one. The adapter logs an error when `hello`
reports more than one connection. It cannot fix this; the remedy is one bot watch per
app. Posting through the Web API, as Dispatch does, opens no Socket Mode connection
and takes nothing (inferred: `num_connections` read 1 on 2026-10-15, after Dispatch
had started posting as the bot).

## Failure modes

| Failure | Behaviour |
|---|---|
| No app token, or no bot token, in the Keychain | Exit non-zero at start, naming the `security add-generic-password` command |
| A token stored under the wrong item (`xoxb-` as the app token) | Exit non-zero at start: malformed |
| A user token (`xoxp-`) as the bot token, which the poll modes accept | Exit non-zero at start: `auth.test` returned no `bot_id`. Run with it, the bot watch would take that person's own messages for the bot's |
| `apps.connections.open` or `auth.test` rejects its token | Exit non-zero |
| Socket Mode turned off (`link_disabled`) | Exit non-zero |
| Offline, Slack 5xx, rate limited, WebSocket will not open, disconnect before `hello` | Back off and retry; exit after 10 in a row, and the supervisor restarts it |
| Silent socket (sleep, network change) | Replaced, within about 30 s of waking |
| The daemon died | The adapter exits at its next ping tick |
| A malformed message | Acknowledged, logged at `error` with its envelope id, dropped |
| Another consumer of the same app | Logged at `error` at `hello`; messages are split |

## Test plan

- **Frames** (`socket.rs`): hello and each disconnect reason; a mention, with and
  without a thread; a DM, an edited DM, and a `message` outside a DM; other
  envelopes keep their id for the ack; malformed fields; odd `event_id` and
  `event_time` keep the message; garbage quotes nothing; conversation ids.
- **Wake rules** (`bot_messages.rs`, in-memory fake Slack): subjects and links for
  both kinds, in and out of threads; a redelivery is dropped only once published;
  edits, deletions, `bot_message`, and the bot's own posts in both measured forms;
  filters; cached and failed lookups; the body never carries text; the seen set is
  bounded and round-trips.
- **Loop** (`socket_loop.rs`): the failure streak (doubling, `Retry-After`, give-up,
  reset); publish before remember, and nothing written for a redelivery or a
  non-message; URLs stripped from errors; the config topic.
- **Adapter e2e** (`tests/slack_bot_e2e.rs`): the real binary against a fake Socket
  Mode server on loopback, a fake `curl` and a fake `security`. Covered:
  - every envelope is acked;
  - a redelivery and a DM edit are dropped;
  - a refresh reconnects;
  - a redelivery after a restart is dropped by the injected baseline;
  - a silent socket is replaced;
  - filtered and malformed messages are acked but not published;
  - failed opens are retried;
  - disconnects before `hello` spend the budget;
  - `link_disabled` is fatal;
  - a missing or rejected app token is fatal;
  - no token or ticket reaches argv or the logs.
- **Bridge e2e** (`crates/mailbox/tests/slack_bot_e2e.rs`): `watch slack-bot --skip`
  goes through the daemon and reaches a fake adapter with the right config. Its
  publish reaches `read`. Another session's different filters are refused, and
  `unwatch` stops the adapter.
- **Not covered** by an automated test for this kind: a filter change respawning
  the adapter, and suspend and resume across `SessionEnd`. Both run through the same
  kind-agnostic code the other Slack kinds test.
- **Live**, by hand, on 2026-10-14 and 15: the table under [How a message
  arrives](#how-a-message-arrives) and the offline measurement above.
