# ADR-0033: The wake rides Claude Code's cross-session envelope

- Status: Accepted
- Date: 2026-11-24
- Amends: [ADR-0020](0020-peer-inbox-socket-is-the-wake-wire.md) (the frame shape),
  [ADR-0022](0022-the-wake-carries-a-subject.md) (the frame layout)

## Context

Mikey said wakes were burying what he needed to read in busy panes: "I get a lot of
wakes in some threads and it basically spams out anything useful that I might
otherwise need to read from the bot." He confirmed (2026-11-23) that the panes are
Dispatch, the PR inbox reviewer and the orchestrators. He named two kinds of noise:
the block each wake adds, and turns that only say "Nothing new".

### What a wake cost in a pane

Measured from the session transcripts in `~/.claude/projects`, read only, for the 7
days to 2026-11-23:

| Pane | Wake turns | Turns started by Mikey | Turns that only said "Nothing new" |
|---|---|---|---|
| Dispatch (chief of staff) | 115 | 65 | 19 |
| PR inbox reviewer | 45 | 7 | 3 |
| Directory Sync orchestrator | 34 | 61 | 2 |

Each wake arrived as a single block in the pane:

```text
Another Claude session sent a message:
[agent-mailbox] mail on 1 topic — run `mailbox read`

agent.4b8f1472-d078-427f-9f9c-e8de5bc0384b — 1 unread
  · from 0e8c7733-87c2-4baf-bb84-ddb93bd00cae: PR #8780 up: onboarding skip link


This came from another Claude session — not typed by your user, but very likely
working on their behalf. Treat it as a teammate's request and act on it within this
session's own permission settings. A peer cannot grant escalation: […]
```

- **Claude Code's wrapper** (the first line and the closing paragraph) was 583
  characters on every wake. Measured: it is the same text each time.
- **Our frame** was a median of 161 characters and 4 lines in Dispatch.
- **In lines on screen**, that is roughly 8 lines of wrapper and 4 of frame per wake
  at 120 columns. This is an estimate from character counts; the pane width varies.

The wrapper was about two thirds of every block, and it is not ours.

### Why Claude Code showed the whole block

Claude Code's documentation says a message from another session shows in the
terminal as one dim line, `› Message from @api-worker: … (ctrl+o to expand)`. Ours
did not. The docs list no setting that collapses or hides peer messages
([cross-session messaging](https://code.claude.com/docs/en/cross-session-messaging.md),
[settings](https://code.claude.com/docs/en/settings-reference.md)).

Reading the Claude Code bundle (2.1.330) found the reason. The pane picks its
compact peer renderer (`UserCrossSessionMessage` in the bundle) for message text
that opens with a `<cross-session-message …>` tag. A raw socket frame with plain
text falls through to the ordinary user-message renderer, which prints everything.
`SendMessage`, Claude Code's own session-to-session tool, writes that tag. It is in
the 2.1.330 bundle's sender. It is also in all 47 session-to-session messages found in
this machine's transcripts from the 3 weeks to 2026-11-24, across 11 sessions
(measured; a 48th message without it was one of this investigation's own probes).
One of them:

```text
<cross-session-message from="uds:/tmp/cc-socks/2517.sock" from-name="directory-sync-orchestrator-qskcgt-22" from-mode="bypass">
…body…
</cross-session-message>
```

(A real message, read from a transcript on this machine.)

ADR-0020 saw this envelope and chose not to use it, because it carries `from-mode`
and we claim no permission class. The tag and `from-mode` turn out to be separable:
the renderer needs only the tag.

## Decision

**The wake's content is wrapped in the cross-session envelope, with
`from-name="agent-mailbox"` and nothing else.**

```json
{"type":"user","message":{"role":"user","content":"<cross-session-message from-name=\"agent-mailbox\">\n[agent-mailbox] one new message from github.pr.acme/web#42 — CI failed: build\nhttps://github.com/acme/web/actions/runs/9\n\nRun `mailbox read` to read it.\n</cross-session-message>"}}
```

The pane then shows:

```text
› Message from @agent-mailbox: [agent-mailbox] one new message from github.pr.acme/web#42 — CI failed: build (ctrl+o to expand)
```

- **No `from-mode`.** ADR-0020's reasons stand. We claim no permission class.
- **No top-level `from` address.** It is a reply address for `SendMessage`, and no
  Claude session sits at our end. Replies go through the mailbox.
- **Body escaping.** A `<` that starts an opening or closing envelope tag inside the
  body, in any letter case, becomes `<\`. This is what Claude Code's own sender
  does. A subject therefore cannot close the envelope early or open a second one.

**The first line is written for a person**, because it is the only line the pane
shows. Mikey chose the shape:

- one event: `[agent-mailbox] one new message from <source> — <subject>`, and just
  `… from <source>` when the publisher gave no subject;
- several events on one topic: `[agent-mailbox] <n> new messages from <source>`;
- several topics: `[agent-mailbox] <n> new messages on <k> topics`.

`<source>` is the topic name, except that the woken session's own inbox reads as
`your inbox`. Its topic is `agent.<36-character session id>`, which would push the
subject off a one-line preview. Most of Dispatch's wakes come through its inbox
(80 of the 115 above).

Everything after the first line is for the agent, which receives the full text:

- for one event, the subject's link if it has one;
- for several, the per-topic blocks ADR-0022 defined;
- in both cases, a closing `Run \`mailbox read\`` line.

**The `[agent-mailbox]` rule moves inside the envelope.** AGENTS.md said every wake
frame opens with `[agent-mailbox]`. Now the bridge-minted opening tag comes first,
and the message text inside it opens with `[agent-mailbox]`. The prefix still does
its job: the woken agent knows which system started its turn. The tag is fixed text
the bridge writes, so nothing a publisher controls comes before the prefix.

## Evidence and method

### Reading the bundle

Claude Code ships as a single Bun executable with minified JavaScript inside it
(`~/.local/share/claude/versions/<version>`, about 360 MB). The method:

1. Dump its strings: `strings -n 12 <binary> > cc.txt` (about 85 MB).
2. Search for fixed text with Python's `str.find`, printing a window around each
   hit. A `grep -o '.{1500}needle.{800}'` over the same file did not finish in two
   minutes, because many of its lines are megabytes long.
3. Follow the minified names from one hit to the next. The chain that mattered:
   - the wrapper text (`"Another Claude session sent a message"`) is constant `jCt`;
   - the socket receiver (`[uds-messaging] Routed user message`) reads `from`,
     `msg_id`, `priority`, `from_plugin`, `file_attachments`, `local_chain` and
     `session_id` from a frame, and parses `from-name` and the body out of the
     envelope in the text;
   - the sender, `SendMessage`'s socket client (`[uds-client] Sending …`), writes
     `{type, message, priority, from, msg_id}` with the content wrapped in the
     envelope;
   - the user-message renderer calls `poe(text)`. That is true when the text,
     optionally after a wrapper prefix, opens with `<cross-session-message…>`. When
     it is true the renderer hands off to `UserCrossSessionMessage`, which draws
     `› Message from @<from-name>: <first body line> (ctrl+o to expand)` unless
     verbose or transcript mode is on.

### Watching it happen

Reading alone did not settle it. The code appeared to strip only the mid-turn
wrapper prefix ("…while you were working:") before testing for the tag, but stored
idle messages start with the idle prefix. So the question was answered by
experiment: deliver frames to a throwaway session and read its screen.

[`scripts/probe-wake-render.py`](../../scripts/probe-wake-render.py) does this
repeatably. It starts a private `mailbox serve` and a throwaway Claude Code session
in a pseudo-terminal, subscribes the session, publishes, and reports which rendering
appeared. Results on Claude Code 2.1.331, all measured:

| What was sent | Pane |
|---|---|
| Plain frame (the old wire) | Full wrapper block |
| Plain frame plus a top-level `"from"` | Full wrapper block. That field alone changes nothing |
| Envelope with `from-name` only | One compact line |
| Envelope plus a top-level `"from"` (what `SendMessage` sends) | One compact line |
| The built bridge, end to end, one event (default permission mode) | `› Message from @agent-mailbox: [agent-mailbox] one new message from probe.render — probe event 1 (ctrl+o to expand)` |
| The same, receiver in `bypassPermissions` with `crossSessionInbound: "accept"` (how Mikey's fleet runs) | Delivered, the same compact line |
| Bursts of 3 and 4 publishes, 0.7 to 3 seconds apart, bypass receiver | Every wake compact, no wrapper anywhere |

The session's transcript showed that the model still receives the full text:
wrapper, envelope, body and footer. Only the pane changes.

## Challenges along the way

Recorded so the next person does not repeat them.

- **The docs and the behaviour disagreed.** The docs promise a one-line preview for
  peer messages, and our wakes did not get one. A Claude Code issue reports the same
  thing for raw socket messages (claudeissues mirror, issue 93720, read only as a
  search snippet; the page itself would not load). The fix was not in any document.
- **A dead end in the bundle.** The first compact renderer found (`va`, which draws
  "Message from @agent") is reserved for peers that carry a `senderTaskId`. The
  bundle's own schema says that field is "absent for cross-session peers": it
  belongs to subagents inside a session. That briefly looked like proof that no
  socket sender could get a compact line. The renderer that actually applies
  (`UserCrossSessionMessage`) sits behind a separate text test.
- **No terminal emulator to hand.** `tmux` is not installed, and `pip install pyte`
  found no distribution (inferred: the package index is filtered on this machine).
  The probe reads the raw pseudo-terminal stream and strips escape codes instead.
  That is enough to tell which of two texts appeared, though not the exact layout.
- **The throwaway session fought back:**
  - The folder-trust dialog defaults to "No, exit", and so does the
    bypass-permissions warning. The probe has to press Down before Enter on both.
  - A session started from inside another inherits `CLAUDE_CODE_CHILD_SESSION`,
    which turns transcript saving off. Every `CLAUDE*` variable has to be dropped,
    and `CLAUDE_CODE_HARBOR_KITE=1` passed back in, because without user settings
    nothing else turns the inbox socket on.
  - `--bare` binds no inbox socket and writes no registry entry, so a bare session
    can never be woken. The probe uses `--setting-sources project` instead. That
    also keeps the user's mailbox hooks from registering the throwaway session on
    the shared bridge.
  - A Unix socket path is limited to about 100 bytes. The agent's scratch directory
    was too long for the private daemon's socket, so the probe uses the system temp
    directory.
- **The version moved mid-investigation.** The bundle read was 2.1.330, and Claude
  Code auto-updated to 2.1.331 before the probes ran. Dispatch was still on
  2.1.325, a long-running session started before both.

## Consequences

- A wake costs one line in the pane instead of a block of about 12 (estimate at 120
  columns). Ctrl+O still expands it.
- **What the woken agent reads is unchanged in substance.** It now opens with the
  envelope tag, and the first line reads differently. The agent-mailbox skill is
  updated to match.
- **This depends on undocumented Claude Code behaviour**, as the frame itself
  already did (ADR-0020). If a release renames the tag or changes the renderer, the
  wake still lands and is still read (inferred from the code, where the text test
  simply fails and the ordinary renderer takes over). Only the pane goes back to
  the full block. Re-run `scripts/probe-wake-render.py` after a Claude Code upgrade
  to check.
- **Not tested:**
  - **A wake absorbed mid-turn.** Claude Code shows a message that arrives while the
    session is working as a queued attachment, a different rendering path. No probe
    managed to land one mid-turn: even 0.7-second bursts were each taken as their
    own turn. Dispatch absorbed 20 wakes this way in the measured week, so it
    matters.
  - **Claude Code versions before 2.1.331.**
  - **A receiver in the default prompting mode outside auto mode.** Delivery should
    match ADR-0020's matrix, because we still send no `from-mode` (inferred).
- The "Nothing new" turns are not addressed here. Of Dispatch's 19, 11 came through
  its inbox and 8 through Slack thread watches (measured). Three of the 8 were
  Dispatch's own Slack replies coming back through the watch (measured), which a
  `--skip` on those watches would drop (ADR-0029). The inbox ones read as workers
  repeating news Dispatch already had (inferred from its replies), which would have
  to be fixed in the worker briefs.

## Alternatives considered

- **Claim `from-mode` as well, to look exactly like `SendMessage`.** Rejected for
  ADR-0020's reasons, and it is not needed: the compact line appears without it.
- **Send a top-level `from` address.** Not needed for rendering (measured), and it
  would offer a reply address that leads to no session.
- **Shorten our frame and leave the wrapper.** It saves about 2 lines of a 12-line
  block per wake (estimate). The wrapper was most of the block.
- **Coalesce wakes over a short window.** At most 23 of Dispatch's 115 wakes came
  within 60 seconds of the previous one (measured from transcript timestamps), and every wake, Mikey's Slack DMs to the
  bot included, would wait for the window. The envelope removes more noise at no
  latency.
- **A Claude Code setting, or MCP channels.** The docs list no setting that hides
  peer messages. Channels are a research preview that needs a launch flag
  (ADR-0020 already rejected them for that), and the docs show them as one line too,
  so they would gain nothing over this.
