# ADR-0014: A self-authored event wakes its author

- Status: Accepted — and now the ONLY publish rule.
- Date: 2026-07-21
- Amends: [ADR-0006](0006-harness-self-respawn.md) (the publish rules). Rule 1, "be
  caught up to speak", is unchanged. Rule 2, "no self-wake", is **reversed**.
- Amended by: [ADR-0018](0018-publish-has-one-rule.md). The decision below stands
  unchanged — an event wakes every subscriber, its author included — but everything
  this ADR says about rule 1 surviving, about `event.author_session` remaining as
  provenance, and about `--no-session` narrowing in meaning is **out of date**: rule 1,
  the column and the flag are all deleted. Authorship is no longer recorded at all, so
  "it no longer participates in the wake decision" is now true by construction.

## Context

Publishing carried two rules for an agent-authored event. Rule 1: a publisher with
unread mail on the topic is refused ("be caught up to speak"). Rule 2: the publisher
is never woken by its own event — implemented in two halves, the kick filter
(`bus::kick_targets`) and the durable unread predicate (`storage::reader::query_unread`).

Rule 2's stated justification was that the publisher "is mid-turn (it just ran a
command) and already knows what it said". Both clauses turn out to be unreliable.

**Authorship is not a proxy for knowledge.** The overwhelmingly common wake in
practice is a `github-pr` transition the agent itself caused: it opened the PR, it
pushed the commit that turned CI red, it requested the review. Those events are
published by an adapter, carry **no author session at all**, and have always woken the
agent — correctly, because "I did the thing that caused this" is not the same as "I
know the outcome". We only suppressed the one case we happen to be able to attribute,
which made the rule inconsistent rather than principled.

**Authorship is ambient and therefore untrustworthy.** The publisher is inferred from
`$CLAUDE_CODE_SESSION_ID`, which Claude Code exports into every process an agent
spawns. A script, hook, or subagent that publishes is stamped with its parent agent's
session id whether or not that agent knows anything about the event. ADR-0006 already
recognised this for the cursor (an earlier version advanced the publisher's cursor past
its own event, which was silent mail loss) and added `--no-session` as the escape
hatch. The wake suppression had the same defect and no such correction.

**It made "unread" mean two different things.** `mailbox status` has always counted a
session's own events; the wake predicate excluded them. So a session could truthfully
report `unread: [topic] 1` and never be woken for it — which reads exactly like the
wake being broken, and cost real time during the ADR-0012 wake investigation before it
was identified as intended behaviour. `query_unread`'s own doc comment warns that "a
wake that disagrees with itself about what 'unread' means is the exact bug class this
module exists to prevent". Disagreeing with `status` is the same bug class.

## Decision

**An event wakes every subscriber to its topic, including the session that published
it.** Authorship stays recorded as provenance (`event.author_session`) and is still
reported, but it no longer participates in the wake decision.

Concretely:

- `bus::kick_subscribers` kicks every subscriber. The `author` parameter and the
  `kick_targets` filter are deleted rather than left inert — a filter that always
  passes is a place for the rule to silently grow back.
- `storage::reader::query_unread` drops its `author_session` clause. This is the one
  unread predicate behind both `topics_with_unread` and `unread`, so the wake path and
  the ADR-0012 watermark move together, and both now agree with `status`.

**Rule 1 keeps its author exclusion.** `storage::writer` still ignores the publisher's
own events when deciding whether it is caught up. Being *woken* by your own message is
harmless; being required to `read` it before you may speak again would make a second
publish impossible — an agent's first publish would be its last. These two are
deliberately not symmetric, and the asymmetry is the point: rule 1 is about not talking
over *other people*, rule 2 is about not missing *anything*.

## Consequences

- **One definition of "unread".** `status`, `read`, the wake predicate and the ADR-0012
  watermark now agree. A non-zero unread count means "you will be woken for this".
- **Consistent with the dominant wake path.** An agent is woken by consequences of its
  own actions whether they arrive from a peer's `send`, its own `publish`, or a
  `github-pr` watch. No rule to remember about which of those are silent.
- **An agent that publishes to a topic it subscribes to now wakes once for it.** This
  is the intended behaviour change. It is bounded: the wake hook wakes iff unread, so
  the wake stops as soon as the agent reads. An agent that publishes on every wake
  would re-wake itself, but that is a loop the agent authors, not one the bus imposes —
  and the ADR-0012 watermark still bounds the turn-boundary nudge to one per message.
- **`--no-session` narrows in meaning.** It used to do two things: exempt the event
  from rule 1 *and* let it wake the ambient session. Only the first remains, since
  every publish now wakes every subscriber. It is still the correct call for a spawned
  script — an event that process publishes should not be attributed to, or gagged by,
  whichever agent happened to be its parent.
- **`event.author_session` is now purely informational.** No behaviour reads it except
  rule 1. If a future change wants "who published this" to affect delivery again, it
  should say so here first.

## Alternatives considered

- **Keep the suppression and make `status` agree with it** (hide self-authored events
  from the unread count). Restores one definition of "unread", but the wrong one: it
  would hide an event the agent may genuinely need, and it doubles down on treating an
  ambient env var as evidence of knowledge. It also would not fix the inconsistency
  with adapter-published consequences of the agent's own actions.
- **Suppress only when the publisher is provably mid-turn.** The bus has no reliable
  view of turn state — that is the whole difficulty ADR-0012 documents — and a rule
  that depends on unobservable state fails silently in exactly the busy window where
  wakes already go missing.
- **Suppress for `publish` but not for `send`.** Arbitrary: both are the same durable
  append, and it would leave the `status`/wake disagreement in place for one of them.
