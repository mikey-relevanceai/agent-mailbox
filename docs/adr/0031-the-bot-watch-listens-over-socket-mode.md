# ADR-0031: The bot watch listens for mentions and DMs over Socket Mode, in one mode of the Slack adapter, and a long disconnect loses them

- Status: Proposed
- Date: 2026-10-15
- Design: [design/04](../design/04-slack-bot-watch.md)
- Amends: [ADR-0027](0027-adapter-secrets-live-in-the-keychain.md) (a second Keychain
  item; a TLS stack in the Slack adapter after all) and [design/02](../design/02-slack-watch.md)'s
  "Later" (Socket Mode is built, without the polling backfill it assumed)

## Context

Mikey wants a person to be able to summon an agent from Slack: by @mentioning the
mailbox's Slack bot (the original ask), or by sending it a DM (added on 2026-10-15).
The existing Slack watches poll a channel's top level or one thread
([design/02](../design/02-slack-watch.md)). Neither fits:

- A mention can be anywhere the bot is, and most of a channel is in threads. Of the
  141 messages in `#team-arg-agent-watercooler` on 2026-10-09, 94 were thread replies
  (measured, ADR-0029), and `conversations.history` does not return replies. Finding
  mentions by polling means reading every thread of every channel.
- A summons wants an answer in seconds. A poll a minute is the floor design/02 set.

Slack pushes an `app_mention` event, and a `message.im` event for a DM, to an app
subscribed to them. Over HTTP that needs a public Request URL. A laptop does not have
one, and v0 does not listen for one. Over Socket Mode the app opens an outbound
WebSocket, and Slack pushes events down it.

Slack sends each event to **one** of an app's open Socket Mode connections ("each
payload may be sent to any of the connections", Socket Mode page). Every consumer
of the app's events must therefore share one connection.

Measured on 2026-10-14 and 15, once Mikey had set the app up (design/04 has the
table):

- A top-level mention and a thread-reply mention in a private channel both arrived.
- A DM woke a session through the bridge, and an edit of it did not.
- Each was published 0–1 s after it was posted.

## Decision

1. **Mentions and DMs come from Slack's `app_mention` and `message.im` events over
   Socket Mode.** No polling.

2. **They are one watch, `mailbox watch slack-bot`, with one topic, `slack.bot`**,
   because one connection must carry both. Two watches would mean two connections,
   and each would get about half of both kinds of event. There is one bot watch per
   bridge, and every subscribed session wakes on every message to the bot. Mikey
   chose the name over keeping `slack-mentions` (2026-10-15).

3. **It is a third mode of `mailbox-slack-adapter`**, picked by `kind: "slack-bot"`
   in the config. A config with no `kind` is still a channel or thread watch. The
   mode shares the adapter's Keychain reading, `curl` transport, name lookups and
   `--skip` filters, and the resolver already routes Slack watches there. This is
   the same choice [ADR-0030](0030-review-requests-are-a-mode-of-the-github-adapter.md)
   made for the GitHub adapter.

4. **The app-level token is a second Keychain item, `agent-mailbox.slack-app-token`**,
   read by the adapter itself, as ADR-0027 says. It is kept apart from the bot token
   because Slack issues and rotates the two separately. Each token has a type of its
   own in the adapter, and each item's prefix is checked (`xapp-`, `xoxb-`/`xoxp-`),
   so a token stored under the wrong name fails at start.

5. **The WebSocket is `tokio-tungstenite` with `native-tls`.** That puts a TLS stack
   in the Slack adapter, which ADR-0027 chose `curl` to avoid. `curl` cannot carry a
   WebSocket here: the `curl` 8.7.1 that macOS ships lists no `ws`/`wss` protocol
   (measured, `curl --version`). `native-tls` uses the platform's own TLS rather than
   a bundled one: Security.framework on macOS, OpenSSL on Linux. Web API calls,
   including `apps.connections.open`, still go through `curl` with the token on
   stdin, so no token reaches argv. The one-time connection URL is held in process
   and never logged.

6. **Only someone saying something to the bot wakes.** Edits, deletions, `hidden`
   events, `bot_message` and other subtypes do not wake. Neither do the bot's own
   posts, identified by its user and bot id from `auth.test`. The bot does post, for
   Dispatch. Its DM posts were seen as the bot's user with no subtype, and as a
   `bot_message` with no user (measured by the orchestrator on 2026-10-15). Then the
   `--skip` filters apply.

7. **Publish, persist, then acknowledge; dedupe by message.** Each message is
   published, the set of published `<conversation>/<ts>` keys (the newest 256) is
   persisted as the baseline, and then its envelope is acknowledged. A message
   already published is dropped. A crash before the ack therefore wakes twice at
   worst, never zero times. A retry is not dropped for being a retry. Keying on the
   message rather than `event_id` needs nothing from Slack's retry semantics, which
   its docs do not state.

8. **The offline gap is accepted, not backfilled.**
   - Measured: mentions posted during a 57 s disconnect were redelivered 60 s after
     posting, with `retry_attempt` 2.
   - Documented for the Events API: retries "nearly immediately", at 1 minute and at
     5 minutes, then stop.
   - Inferred: a message sent while the laptop sleeps longer than about 5 minutes is
     lost. The 5-minute retry was not measured.

   A backfill would have to read every conversation and thread the bot is in, and
   match mention text, because `search.messages` takes user tokens only (Slack's
   method page). That cost is not worth it for an interactive summons, where the
   person can ask again.

9. **One consumer per app.** The adapter warns when `hello` reports more than one
   connection. It exits when the daemon that started it is gone, so an orphan does
   not keep a connection and take a share of the events. It does not try to
   coordinate beyond that.

## Consequences

- A mention or DM wakes in seconds, thread replies included (measured).
- The bot must be invited to each channel whose mentions it should hear (Slack's
  `app_mention` docs). A private channel works once it is (measured).
- A message sent while no connection has been open for more than about 5 minutes is
  lost silently (inferred above). Slack's "Delayed Events" setting promises hourly
  retries for 24 hours. Its docs do not say whether that covers Socket Mode, and it
  was not tried. It is the first thing to try if the gap matters.
- Running a second bot watch for the same app splits the messages between them. That
  could be on another machine, or a test daemon beside the real one. The warning is
  the only signal.
- **Linux** now links OpenSSL into the Slack adapter, where before it needed only
  `curl`. The Slack adapter still cannot work on Linux, because there is no Keychain
  (ADR-0027). The release build runs on `ubuntu-22.04`, which is expected to have the
  OpenSSL headers. That is inferred, not seen in a release run; CI builds on
  `ubuntu-latest` and will show it first.
- While offline, the adapter exits after about four minutes of failed connections.
  That is longer than the supervisor's 60 s `reset_after`, so it is restarted
  indefinitely rather than marked `failed` (inferred from the constants). Only a fast,
  fatal exit, such as a bad token or Socket Mode turned off, leads to `failed`.
- Slack auto-disables an app that fails over 95% of deliveries in an hour, but not
  one under 1,000 events an hour (Events API page). A sleeping laptop fails every
  delivery. This bot is addressed far less often than that (inferred from use).
- The adapter now holds a WebSocket and pings every 30 s, and it reconnects every few
  hours. That replaces a poll a minute.

## Alternatives considered

- **Poll for mentions and DMs.** `conversations.history` plus `conversations.replies`
  for every conversation the bot is in, matching `<@bot>` in the text. Slow, it is the
  most expensive read pattern Slack has, and it reads text. Rejected, and also
  rejected as a backfill (above).
- **`search.messages` for `<@bot>`.** One call instead of many, but it takes user
  tokens only, so it would need a person's token. Rejected.
- **The Events API over HTTP.** Needs a public URL and a listener; v0 has neither.
  Rejected, as in design/02.
- **Separate `slack-mentions` and `slack-dms` watches.** Each would open its own
  connection and lose about half of its events to the other. Rejected.
- **Keep the name `slack-mentions` and let it cover DMs.** Less churn, but the name
  would undersell what wakes. Mikey chose `slack-bot`.
- **A separate adapter binary.** It would duplicate the Keychain, `curl`, lookups and
  filter code. Rejected for a mode, as ADR-0030 did.
- **A WebSocket through a helper binary** (`websocat`), to keep TLS out of the
  adapter. Not on macOS by default, so one more thing to install for one watch.
  Rejected.
- **Acknowledge first, then publish.** It never duplicates, but a crash between the
  two loses the message. Rejected; the project re-fires rather than drops.
- **Dedupe by `event_id`.** It needs a redelivery to keep its `event_id`, which
  Slack's docs do not say. It would also miss one message delivered as two events.
  Rejected for the message's `<conversation>/<ts>`.
