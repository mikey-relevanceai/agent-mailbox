# ADR-0031: Mentions arrive over Socket Mode, in a mode of the Slack adapter, and a long disconnect loses them

- Status: Proposed
- Date: 2026-10-14
- Design: [design/04](../design/04-slack-mention-watch.md)
- Amends: [ADR-0027](0027-adapter-secrets-live-in-the-keychain.md) (a second Keychain
  item; a TLS stack in the Slack adapter after all) and [design/02](../design/02-slack-watch.md)'s
  "Later" (Socket Mode is built, without the polling backfill it assumed)

## Context

Mikey wants a person to be able to summon an agent from Slack by @mentioning the
mailbox's Slack bot. The existing Slack watches poll a channel's top level or one
thread ([design/02](../design/02-slack-watch.md)). Neither fits:

- A mention can be anywhere the bot is, and most of a channel is in threads. Of 141
  messages in `#team-arg-agent-watercooler` on 2026-10-09, 94 were thread replies,
  which `conversations.history` does not return (measured, ADR-0029). Finding
  mentions by polling means reading every thread of every channel.
- A summons wants an answer in seconds. A poll a minute is the floor design/02 set.

Slack pushes an `app_mention` event to an app subscribed to it. Over HTTP that needs a
public Request URL, which a laptop does not have and v0 does not listen for. Over
Socket Mode the app opens an outbound WebSocket and Slack pushes events down it.

Measured on 2026-10-14, once Mikey had set the app up (design/04 has the table): a
top-level mention and a thread-reply mention in a private channel both arrived, 0–1 s
after posting, and one woke a session through the bridge end to end.

## Decision

1. **Mentions come from Slack's `app_mention` event over Socket Mode.** No polling.
   `mailbox watch slack-mentions` records the bridge's one mention watch. Its topic
   is `slack.mentions`. Every subscribed session wakes on every mention.

2. **It is a third mode of `mailbox-slack-adapter`**, picked by `kind:
   "slack-mentions"` in the config. A config with no `kind` is still a channel or
   thread watch. The mode shares the adapter's Keychain reading, `curl` transport,
   name lookups and `--skip` filters, and the resolver already routes Slack watches
   there. This is the same choice [ADR-0030](0030-review-requests-are-a-mode-of-the-github-adapter.md)
   made for the GitHub adapter.

3. **The app-level token is a second Keychain item, `agent-mailbox.slack-app-token`**,
   read by the adapter itself, as ADR-0027 says. It is separate from the bot token
   because Slack issues and rotates the two separately. The adapter checks each
   item's prefix (`xapp-`, `xoxb-`/`xoxp-`), so a token stored under the wrong
   name fails at start.

4. **The WebSocket is `tokio-tungstenite` with `native-tls`.** That puts a TLS stack
   in the Slack adapter, which ADR-0027 chose `curl` to avoid. `curl` cannot carry it:
   the `curl` 8.7.1 that macOS ships lists no `ws`/`wss` protocol (measured, `curl
   --version`). `native-tls` uses the platform's own TLS (Security.framework on macOS,
   OpenSSL on Linux), the same TLS `curl` uses, rather than a bundled one. Web API calls,
   including `apps.connections.open`, still go through `curl` with the token on
   stdin. So no token reaches argv. The one-time connection URL is held in process
   and never logged.

5. **Publish, persist, then acknowledge.** Each mention is published, its
   `event_id` is persisted as the baseline (the newest 256), and then its envelope is
   acknowledged. A redelivery whose `event_id` was already published is dropped. A
   crash before the ack therefore wakes twice at worst, never zero times. A retry is
   not dropped for being a retry.

6. **The offline gap is accepted, not backfilled.** Measured: mentions posted during
   a 57 s disconnect were redelivered 60 s after posting, with `retry_attempt` 2.
   Documented for the Events API: retries "nearly immediately", at 1 minute and at 5
   minutes, then stop. So mentions made while the laptop sleeps longer than about 5
   minutes are lost (inferred; the 5-minute retry was not measured). A backfill would
   have to read every conversation and thread the bot is in and match mention text,
   because `search.messages` takes user tokens only (Slack's method page). That cost
   is not worth it for an interactive summons, where the person can ask again.

7. **One consumer per app.** Slack sends each event to one of an app's connections
   (Socket Mode page). The adapter warns when `hello` reports more than one connection.
   It does not try to coordinate.

## Consequences

- A mention wakes in seconds, thread replies included (measured).
- The bot must be invited to each conversation it should hear (Slack's `app_mention`
  docs). A private channel works once it is (measured).
- A mention made while no connection is open for longer than about 5 minutes is lost
  silently (inferred above). Slack's "Delayed Events" setting promises hourly retries
  for 24 hours. Its docs do not say whether that covers Socket Mode, and it was not
  tried. It is the first thing to try if the gap matters.
- Running a second mention watch for the same app (another machine, a test daemon
  beside the real one) splits mentions between them. The warning is the only signal.
- **Linux** now links OpenSSL into the Slack adapter for this mode, where before it
  needed only `curl`. The Slack adapter still cannot work on Linux, because there is
  no Keychain (ADR-0027). The release build runs on `ubuntu-22.04`, whose image is
  expected to have the OpenSSL headers. That is inferred, not seen in a release run;
  the first release after this merges will show it.
- The dedupe depends on a redelivery keeping its `event_id`. That is inferred: Slack's
  docs do not say so, and no real duplicate was seen in testing. If it is wrong, a
  redelivered mention wakes twice. No mention is lost either way.
- Slack auto-disables an app failing over 95% of deliveries in an hour, but not one
  under 1,000 events an hour (Events API page). A sleeping laptop fails every delivery.
  This bot is mentioned far less often than that (inferred from use).
- The adapter now spends a WebSocket, a ping every 30 s and a reconnect every few
  hours, instead of a poll a minute.

## Alternatives considered

- **Poll for mentions.** `conversations.history` plus `conversations.replies` for
  every conversation the bot is in, matching `<@bot>` in the text. Slow, the most
  expensive read pattern Slack has, and it reads text. Rejected, and also rejected as
  a backfill (above).
- **`search.messages` for `<@bot>`.** One call instead of many, but user tokens only:
  it would need a person's token. Rejected.
- **The Events API over HTTP.** Needs a public URL and a listener; v0 has neither.
  Rejected, as in design/02.
- **A separate `mailbox-slack-mentions-adapter` binary.** Duplicates the Keychain,
  `curl`, lookups and filter code. Rejected for the mode, as ADR-0030 did.
- **A WebSocket through a helper binary** (`websocat`) to keep TLS out of the adapter.
  It is not on macOS by default, so it is one more thing to install for one watch.
  Rejected.
- **Acknowledge first, then publish.** Never duplicates, but a crash between the two
  loses the mention. Rejected; the project re-fires rather than drops.
