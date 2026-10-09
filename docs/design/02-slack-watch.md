# Design: Slack channel and thread watches

- Status: Implemented
- Related ADRs: [0001](../adr/0001-rust-bridge-subprocess-adapters.md),
  [0005](../adr/0005-baseline-via-protocol.md),
  [0014](../adr/0014-self-authored-events-wake-their-author.md),
  [0022](../adr/0022-the-wake-carries-a-subject.md),
  [0027](../adr/0027-adapter-secrets-live-in-the-keychain.md),
  [0029](../adr/0029-watch-filters-run-in-the-adapter.md)

## Goal

Wake a Claude Code session when a message lands in a Slack channel or thread it is
watching, the way `watch github-pr` wakes it when a PR changes. The motivating case is
a channel where the team's agents post status updates and talk to each other. Agents
can already read and post to Slack through the claude.ai connector; nothing could wake
them when someone else posted.

```bash
mailbox watch slack-channel C0C83CXLUL8
mailbox watch slack-thread  C0C83CXLUL8/1791349480.652779
mailbox watch slack-thread  https://tryrelevance.slack.com/archives/C0C83CXLUL8/p1791349480652779
mailbox watch slack-channel C0C83CXLUL8 --skip user=U0AB7RJSQBE,app=A08SF47R6P4
```

## Non-goals

- **Posting.** The adapter is read-only; agents post through their own Slack access.
- **Direct messages.** The app is granted no `im:history`.
- **Real-time delivery.** v1 polls. Socket Mode is the upgrade path (below).
- **Linux.** The token lives in the macOS Keychain ([ADR-0027](../adr/0027-adapter-secrets-live-in-the-keychain.md)).

## How Slack is reached, and why

**Polling `conversations.history` and `conversations.replies`, with a bot token from
an internal Slack app.** The options considered, and what each needs:

| Option | Credential | Works from a laptop? | Verdict |
|---|---|---|---|
| Own internal app, polling | A bot token; the workspace's app approval | Yes: outbound HTTPS only | **Chosen** |
| Arg's Slack app via `arg action run slack_fetch_channel_history` | None new; the Arg bot must be in the channel | Yes | Probably rate-limited (below); not measured |
| Events API over HTTP | Same app, plus a public Request URL | No: needs a public URL, and v0 has no listener | Rejected |
| Socket Mode | Same app, plus an `xapp-` token with `connections:write` | Yes: outbound WebSocket | Later; still needs polling to backfill after downtime |
| The claude.ai Slack connector | Its token cannot be exported | — | Not an option |

The deciding fact is the rate limit. Slack's `conversations.history` page says
internal customer-built apps keep Tier 3 (50+ requests per minute, `limit` up to
1,000), counted per method, per workspace, per app. One poll a minute is about 2% of
that. From 29 May 2025, commercially distributed apps outside the Slack Marketplace get
1 request per minute and 15 messages per call
([method page](https://docs.slack.dev/reference/methods/conversations.history),
[changelog](https://docs.slack.dev/changelog/2025/05/29/rate-limit-changes-for-non-marketplace-apps)).
Arg's Slack app is not on the Marketplace, per its own README, so the Arg route
probably falls under the 1/min limit, shared with every Arg Slack trigger in the
workspace. That is inferred, not measured.

The tryrelevance workspace requires admin approval for custom apps; the app used here
was approved with these bot scopes: `channels:history`, `channels:read`,
`channels:join`, `users:read`, `groups:history`, `groups:read`, `reactions:read`. The
adapter uses the first four. The rest were requested up front because a scope change
needs re-approval.

## Shape

```text
mailbox watch slack-thread <link>
  → CLI parses the link into SlackTarget{channel, thread_ts}
  → daemon parses it into SlackWatch, records the watch + interest, subscribes
  → supervisor → SlackResolver → mailbox-slack-adapter
       config {topic, channel, thread_ts, interval_ms, skip, baseline}
  → adapter: token from Keychain → curl → Slack → Publish / Baseline lines
  → host relays; the bridge wakes subscribers
```

- **Targets and topics** (`mailbox-protocol::slack`). `SlackWatch` is
  `Channel(SlackChannelId)` or `Thread { channel, thread_ts: SlackTs }`. Topics are
  `slack.channel.<C>` and `slack.thread.<C>/<ts>`. A channel is named by id because
  names can change. `SlackTs` is held as numbers and renders canonically, so two
  spellings of one timestamp are one watch. All three types parse on deserialize,
  and `SlackWatch` crosses the control socket as a tagged value
  (`{"kind": "thread", "channel": …, "thread_ts": …}`), so the daemon decodes a
  parsed target rather than re-checking strings.
- **Storage.** Two watch kinds, `slack-channel` and `slack-thread`, on the existing
  flat row: the watch key (`<C>` or `<C>/<ts>`) in `repo`, `0` in `pr` and
  `publish_count`, the same way a stub stores its label. `build_watch` parses the
  key back, and an unparseable one is corrupt. The watch's `--skip` filters are
  canonical JSON in `filters` (schema v9, [ADR-0029](../adr/0029-watch-filters-run-in-the-adapter.md)),
  parsed back the same way.
- **One adapter for both kinds**, `adapters/slack-adapter`, resolved like the others:
  `MAILBOX_SLACK_ADAPTER_BIN`, else beside the bridge binary, else `PATH`.

### What wakes

| Message | Channel watch | Thread watch |
|---|---|---|
| Ordinary message, `bot_message`, `file_share`, `me_message` | wakes | wakes (if a reply) |
| Reply in a thread | — (not in history) | wakes |
| `thread_broadcast` (reply also sent to channel) | wakes | wakes |
| The thread's parent | — | skipped |
| `channel_join`, `channel_leave`, topic/purpose/name changes | skipped | skipped |
| `message_changed`, `message_deleted`, `hidden` | skipped | skipped |
| Anything above that wakes, but matches a `--skip` filter | skipped | skipped |

**Authorship is not filtered by default.** The setup this was built for has agents
posting through one person's claude.ai Slack connector, so they post as that
person's Slack user. Then a session's own post is indistinguishable from a peer's by
author, and a session is woken by its own post: the trade
[ADR-0014](../adr/0014-self-authored-events-wake-their-author.md) made for `publish`.

**A watch can opt in to filters** ([ADR-0029](../adr/0029-watch-filters-run-in-the-adapter.md)).
`--skip key=value[,key=value]` (repeatable) skips a message that meets every
condition of any filter. The keys are `user=<U…>` and `app=<A…>`. A connector post
carries the connector's `app_id`, and the typed messages measured carried none
(ADR-0029 has the measurement and its limits). So `--skip user=U0AB7RJSQBE,app=A08SF47R6P4`
skips that person's agent posts in the tryrelevance workspace and keeps what they
type. It also skips any peer agent posting through the same connector as that person.

The adapter applies filters after the rows above, so a join is still logged as a
join. A filtered message advances the cursor and is logged at `info` with its `ts` and
the filter; it is never published, so the bridge stores nothing for it. The filters
are the watch's, shared by every session on it: a `watch` whose `--skip` set differs
from the stored one is refused while another session is interested, and otherwise
replaces it and respawns the adapter.

### Cursor and baseline

The baseline is `{"last_ts": "<ts>"}`, the newest message seen. With none injected, the
first poll records the newest message (one `limit=1` call; for a thread, the parent's
`latest_reply`) and publishes nothing. Each later poll reads messages newer than the
cursor, oldest first; publishes the ones that wake; then emits the cursor advanced past
everything it read, including skipped messages. Publishing before emitting the cursor
means a crash re-fires a message rather than dropping one.

A poll reads at most 10 pages of 200. History pages newest first, so a channel taking
more than 2,000 messages between polls loses the oldest, with a warning. Replies page
oldest first, so a thread past the cap loses nothing: the next poll reads the rest.

### Subject and body

The subject is `new message from <name> in #<channel>` or `new reply from <name> in a
thread in #<channel>`, linked to the message's permalink, built locally from
`auth.test`'s workspace URL. Names come from `bot_profile.name` or `users.info`,
cached per process; a failed lookup falls back to the id.

Each message is parsed once, where it leaves the API, into a typed `SlackMessage`; a
message whose `ts` or `thread_ts` is malformed is dropped with a warning rather than
read as if the field were absent. The text is never deserialized.

The body is `{kind, channel, ts, thread_ts?, user?, bot_id?, app_id?, subtype?, permalink}`.
**It never carries the text.** The woken agent reads the message through its own Slack
access. That keeps third-party text, which is a prompt-injection surface, out of the
bridge's log and out of `mailbox read`.

### Credential

The adapter reads its token from the Keychain itself; see
[ADR-0027](../adr/0027-adapter-secrets-live-in-the-keychain.md).

## Failure modes

| Failure | Behaviour |
|---|---|
| No token in the Keychain | Exit non-zero, naming the `security add-generic-password` command |
| Token rejected (`invalid_auth`, `token_revoked`, …) | Exit non-zero |
| Bot not in the channel | Join once (`conversations.join`), retry; if that fails, exit non-zero saying to `/invite` it |
| `channel_not_found`, `thread_not_found`, `missing_scope` | Exit non-zero |
| A thread watch given a reply's ts | Exit non-zero: watch the parent |
| HTTP 429 / `ratelimited` | Wait `Retry-After` (default 30 s), retry up to 12 times, then exit non-zero |
| Offline, curl failure, 5xx, non-JSON reply | Skip the poll, keep the cursor; exit non-zero after 10 in a row |

Every non-zero exit goes through the supervisor's backoff, give-up and once-per-outage
notice ([ADR-0023](../adr/0023-one-give-up-notice-per-outage.md)), and the sweep
retries a failed watch while someone still wants it.

## Test plan

- **Protocol:** id, ts and link parsing; numeric ts order; key and topic round trip.
- **Filters:** grammar, matching and the canonical set (protocol); the stored
  column, the refusal and the replace (storage); the respawn waiting for the old
  adapter (supervisor); a connector post skipped and a typed one woken (adapter
  unit and e2e); and through the bridge, `--skip` reaching the adapter, another
  session refused, and a change respawning it.
- **Adapter unit tests:** the wake table, row by row, over typed messages; and an
  in-memory fake Slack (history newest-first, replies parent-first, both paged by
  `limit` and `cursor`) covering the cursor, an empty channel, pagination, the page
  cap's documented loss, a malformed message dropped rather than misread,
  join-then-read, a reply ts used as a thread, and name caching.
- **Adapter e2e:** the real binary against a fake `curl` and a fake `security`:
  baseline then one publish, the token never in argv or logs, resume from an injected
  cursor, thread links, no token, a rejected token, a rate limit that lifts, one
  that never does, an outage that outlasts the skipped-poll budget, and SIGTERM.
- **Resolver:** the config each Slack kind gets, and that every resolver refuses
  the kinds that are not its own.
- **Bridge e2e:** `watch slack-thread <link>` through the daemon; a reply reaches
  `mailbox read`; `unwatch` stops the adapter.
- **Storage:** channel and thread watches are distinct rows that round-trip; a bad
  key is corrupt.
- Measured once by hand against the real channel: a run with an older cursor
  published Ben Skinner's message and skipped the bot's own `channel_join`.

## Later

- **Socket Mode**, for seconds rather than a minute. It needs an app-level token and
  an event subscription on the app, which may need re-approval, and it still needs
  this polling to backfill after the laptop sleeps.
- **Reactions** (`reactions:read` is granted) if agents start signalling with them.
