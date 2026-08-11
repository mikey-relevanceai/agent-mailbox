# ADR-0022: The wake carries a subject — pointer, not payload

- Status: Accepted
- Date: 2026-08-11
- Amends: [ADR-0001](0001-rust-bridge-subprocess-adapters.md) — the payload-free wake
  rule becomes **pointer, not payload**. Bodies still never cross the wake boundary.
- Amends: [ADR-0020](0020-peer-inbox-socket-is-the-wake-wire.md) §"Wake stays
  payload-free" and [ADR-0021](0021-delete-the-sentinel-fallback.md)'s restatement of
  it — the frame is no longer topic names alone.
- Builds on: [ADR-0021](0021-delete-the-sentinel-fallback.md), which made the inbox
  socket the only wake wire.

## Context

A wake said one thing, and it said it about everything:

```
mail on topic github.pr.acme/web#42
```

An agent that receives it knows something changed on a PR it is watching. It does not
know **what**, so it reconstructs the delta itself: re-read the PR, compare against
what it remembers from an hour ago, work out which of the comments, reviews, CI runs
or merge states is the new one. That is most of a turn spent recovering information
the adapter already had and threw away — the poller *knew* it was comment 2145678,
because a strictly higher comment id is exactly what made it fire.

Two smaller problems came with it:

- **Nothing identified the sender.** A message arriving on a session's inbox socket is
  indistinguishable, at a glance, from anything else Claude Code might inject there.
  An agent could not tell "this is the mailbox waking me, so load that skill" from any
  other prompt.
- **A peer message named its topic and no more**, so being woken by a teammate looked
  exactly like being woken by CI.

The payload-free rule (ADR-0001) is what stopped us fixing this earlier, and it was
written for a real reason: an event body is untrusted adapter output, potentially
large, and duplicating it onto the wake wire would give the agent two places to look
for the truth and let a wake become a second, unversioned copy of the event.

That reason does not extend to *naming* the change. "New comment, here is its
permalink" is not the event; it is a pointer to it.

## Decision

**A published event may carry a `subject`: one line describing what changed, and an
optional link to it. The wake renders the subjects of what is unread. Bodies still
never cross.** And every wake frame opens with `[agent-mailbox]`.

A wake now reads:

```
[agent-mailbox] mail on 2 topics — run `mailbox read`

github.pr.acme/web#42 — 2 unread
  · CI failed: build
    https://github.com/acme/web/actions/runs/9/job/2
  · new comment
    https://github.com/acme/web/pull/42#issuecomment-2145678

agent.983eae5f-0b09 — 1 unread
  · from 700a3bf5-1c4d: PR 42 review finished
```

Five things make this safe to put in front of a model, and each is load-bearing:

1. **`Subject` is a parsed type, not a string** (`mailbox-protocol/src/subject.rs`).
   Text is collapsed to a single line and bounded at 120 characters; a link must be
   `http(s)`, bounded, and whitespace-free. Deserialization goes through the same
   constructor, so there is no way — including from the wire — to hold a `Subject`
   that breaks those rules.
2. **It normalizes rather than rejects.** Subject text is assembled from things
   GitHub owns (a check name, a review title). Refusing a subject over a stray newline
   would cost the agent the whole CI signal over a character it never chose, so
   whitespace collapses, over-long text truncates, and an unusable link is dropped
   while its text survives. A wake degrades to a worse subject, never to no subject.
3. **The layout is unforgeable.** Because a subject cannot contain a newline, adapter
   text can fill its own bullet and nothing else: not a second bullet, not a topic
   block, not another `[agent-mailbox]` header.
4. **It is a separate protocol field, not a well-known key in the body.** The bridge
   never reaches into opaque content to find it, and an adapter has to say explicitly
   "this line is fit to show a model" rather than have that decided by a key name.
5. **It is optional everywhere.** An adapter with nothing useful to say omits it and
   its subscribers are woken with the topic and a count — exactly the old behaviour,
   with a prefix.

The wake stays a summary, not a feed: **at most 3 subjects per topic** (newest first)
and **8 topics**, with what is left out stated (`…and 4 earlier`, `…and 3 more
topics`) rather than silently dropped. The count is always of *everything* unread, so
the wake can only ever under-describe what `read` will hand over.

For peer messages, `mailbox send --subject` lets a sender say what a message is about;
the **bridge** composes the delivered line as `from <sender>: <subject>` from the
`from` it verified. The message text is never used as the subject by default: a wake
describes what is waiting, and folding the text in would make the mail readable
without `read` and spend the recipient's turn on a peer's words before it chose to.

## Consequences

- **Agents start where the change is.** "CI failed: build" plus the failing run's own
  URL replaces a re-derivation of the PR's history from memory.
- **`[agent-mailbox]` is a skill trigger.** An agent can recognise the mailbox on
  sight and load the right skill without guessing.
- **Adapter text now reaches a model's turn start.** That is a genuine widening of the
  trust surface, and it is bounded by construction rather than by adapter good
  behaviour (points 1–3 above). A subject names things — ids, check names, URLs — and
  never quotes content, so there is no design in which it carries a comment body.
- **Schema v7** adds nullable `event.subject` / `event.subject_link`. A v6 database
  migrates forward with its mail intact; those events simply have nothing to say.
- **One unread predicate, one query.** The count and the subjects come out of a single
  `unread_digest`, because two queries could disagree — "3 unread" followed by four
  descriptions.
- **`status` is unchanged.** It asks for zero subjects and prints counts; what the
  mail is *about* is what `read` is for.

## Alternatives considered

- **Leave it payload-free.** Cheapest, and it keeps costing an agent the first half of
  every woken turn. The rule's purpose — don't duplicate the body — survives intact
  under this ADR, so the cost bought nothing.
- **Put the summary in the body under a well-known key.** No protocol change, but the
  bridge would have to reach into untrusted opaque content and trust what it found
  there, which is exactly what ADR-0001 keeps it from doing.
- **Let the bridge derive a summary from the body.** Same objection, plus the bridge
  would need a schema per adapter — the coupling ADR-0001's opaque body exists to
  prevent.
- **Put the whole event body on the wire.** The thing ADR-0001 forbids, and rightly:
  the agent would have two sources of truth, one of them unversioned and unbounded.
- **Send the peer message's text as its subject.** Most informative for peer chat, and
  it makes the wake wire a delivery channel — the one thing it is not. `--subject` is
  the opt-in for a sender who wants to say more.
