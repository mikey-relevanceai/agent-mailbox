# ADR-0018: `publish` has one rule — the event goes to the topic and wakes everyone

- Status: Accepted
- Date: 2026-08-05
- Supersedes: [ADR-0006](0006-harness-self-respawn.md) §8 ("the publish rules").
  Rule 1, "be caught up to speak", is **deleted**.
- Amends: [ADR-0014](0014-self-authored-events-wake-their-author.md). Its decision —
  a self-authored event wakes its author — **stands**, and is now the only rule.
  Everything it says about rule 1, `event.author_session` and `--no-session` is moot:
  all three are gone.

## Context

Three interacting special cases sat on `publish`, and every one of them existed
because the bridge tried to know **who** was speaking.

**Rule 1, "be caught up to speak."** A publisher subscribed to the topic with unread
events on it *that it did not itself write* was REFUSED: nothing written, its own exit
code (3), and a message telling the agent to `mailbox read` and retry.

**`--no-session`.** Claude Code exports `$CLAUDE_CODE_SESSION_ID` into every process an
agent spawns — a build script, a git hook, a subagent. So such a process's `publish`
was attributed to the AGENT. `--no-session` published anonymously, so that process's
event was not gagged by (and, before ADR-0014, was not hidden from) whichever session
it happened to inherit.

**`event.author_session`** (schema v5) existed solely so rule 1 could tell "your own
words" from "someone else's". ADR-0014 had already removed the only other reader of
it.

Four things are wrong with this arrangement.

1. **It blocks a write because of the writer's read state.** Nothing else in the
   system does that. `send` — the actual peer-to-peer path — was explicitly exempt,
   because it writes to a topic the sender does not subscribe to, so the same message
   was refused or permitted depending on which command carried it.
2. **It decides on an author it cannot trust.** ADR-0014 already established that
   authorship is ambient and routinely wrong. Rule 1 kept deciding on it anyway, and
   `--no-session` was the manual correction an agent had to remember to apply to
   everything it spawned. A rule that needs an opt-out flag in the common case is not
   a rule, it is a tax.
3. **It is a politeness norm in the transport.** "Do not talk over mail you have not
   read" is good advice to an agent. Enforcing it in the bus means an agent that hits
   it must go read before it may speak — including when what it was about to say was
   time-critical and unrelated. The bus's job is to deliver, not to arbitrate turn
   taking.
4. **It cost a documented exit code, a wire response, a storage command, an enum, a
   schema column and a section of every agent-facing document** — for a rule whose
   entire effect was to make an agent run `mailbox read` at a moment it had not chosen.

## Decision

**An event goes to the topic and wakes every subscriber, its author included. That is
the whole publish contract.**

Concretely, deleted rather than left inert:

- The refusal: `PublishAttempt` (both variants), `Storage::publish_as_session`,
  `Command::PublishAsSession`, `Bus::publish_as_session`, `Response::PublishRefused`,
  and the `PUBLISH_REFUSED_EXIT` (3) exit code.
- The caller on the wire: `Request::Publish` no longer carries a `session`, and the
  CLI's `publish` resolves none.
- `--no-session`, which had nothing left to opt out of.
- `event.author_session` (schema **v6** drops the column). Nothing read it, nothing
  displayed it — `Event` never carried it on the wire, and `send` never stamped it,
  putting its provenance in the body's `from` field instead. A column recording a
  routinely-wrong answer to a question nobody asks is an invitation to grow a new rule
  on bad data.

Provenance that survives, because it is honest: the `adapter` label on every event (a
name, not authority — ADR-0001), and the `from` field the bridge stamps into every
`send` body.

## Consequences

- **One publisher.** An adapter, an agent, and a script an agent spawned are
  indistinguishable to the bridge, and there is exactly one code path for all three.
  The `publish` half of the skill drops from two rules plus a flag to one sentence.
- **A publish has two outcomes: it worked, or the bridge is down.** No third
  "serviced but refused" state for a script to tell apart from a failure.
- **Being caught up is now the agent's judgement.** The skill still says to read what
  peers wrote; it no longer pretends the transport can enforce it. An agent CAN now
  publish over unread mail — that is the accepted cost, and it is the same latitude
  every `send` has always had.
- **Migration is one-way.** Schema v6 drops the author column, so a v6 database cannot
  be opened by a build that predates this ADR (`UnsupportedSchemaVersion`, refused
  loudly rather than guessed at). Events, offsets and bodies survive the rewrite.
- **If delivery should ever depend on who published again**, it needs a new column, a
  new ADR, and an answer to the question this one could not answer: how do you know
  who published, given the id is inherited by every process the agent spawns?

## Alternatives considered

- **Keep rule 1, delete only `--no-session`.** The flag exists *because* of the rule;
  removing the escape hatch and keeping the trap is strictly worse.
- **Keep the author column as pure provenance.** Tempting — a durable log recording
  who wrote each row sounds valuable. But the value it would record is the ambient
  session id of whatever process ran the command, which for everything an agent spawns
  is the wrong agent. Keeping an unread, unreliable field is how the rule grows back.
- **Move "be caught up to speak" into the skill as advice.** Done — that is what the
  skill now says. What is rejected is enforcing it in the transport.
