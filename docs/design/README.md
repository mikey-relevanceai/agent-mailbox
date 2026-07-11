# Design docs

Designs describe **how a major system or subsystem works** (or will work):
responsibilities, data flow, failure modes, and how we will test it.

ADRs ([`../adr/`](../adr/)) record the durable *choice*. Designs record the
*shape*.

## Index

| Design | Status | Notes |
|---|---|---|
| [01-mvp-github-watch](01-mvp-github-watch.md) | Implemented | GitHub PR watch (conflicts, reviews, CI); refcounted multi-session interest; no zombie pollers |

Likely next designs:

- Bridge core (topics, publish, subscriptions, delivery vs processed cursors)
- Adapter host / subprocess transport
- Claude Code harness integrator (`asyncRewake` waiter + Stop re-arm)

## When to write one

Write or update a design when you are about to implement (or substantially
change) a subsystem that other crates or adapters will depend on. Prefer a short
design in the same PR as the first vertical slice, not a large speculative doc
with no code path.

## Suggested outline

```markdown
# Design: <name>

- Status: Draft | Active | Implemented | Superseded
- Related ADRs: …

## Goal

## Non-goals

## Shape

Components, ownership, data flow (diagrams welcome).

## Failure modes

## Test plan

What the early test bar is for this subsystem.
```
