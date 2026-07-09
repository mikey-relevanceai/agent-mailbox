# Architecture Decision Records

ADRs capture **decisions that should stay true** until explicitly superseded.
They are not a substitute for design docs (see [`../design/`](../design/)).

## Index

| ADR | Title | Status |
|---|---|---|
| [0001](0001-rust-bridge-subprocess-adapters.md) | Rust bridge, subprocess adapters, local-only v0 | Accepted |

## When to write one

Write an ADR when you choose something that later contributors (human or agent)
might reasonably reverse without noticing the cost — language, storage,
transport, security model, multi-subscriber semantics, harness wake strategy.

Skip ADRs for routine implementation detail that a design doc or code comment
covers.

## Template

Copy into `NNNN-short-title.md` (zero-padded, next free number):

```markdown
# ADR-NNNN: Title

- Status: Proposed | Accepted | Deprecated | Superseded by ADR-XXXX
- Date: YYYY-MM-DD

## Context

What forces the decision?

## Decision

What we will do.

## Consequences

What becomes easier, harder, or constrained.

## Alternatives considered

What we rejected and why (brief).
```
