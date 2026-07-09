# Working agreements

How we make and record decisions in this repo. Agents must follow these unless
the human explicitly overrides them for a given task.

## Branching and pull requests

**Default: branch off `main`, open a PR, do not push commits straight to `main`.**

Only push directly to `main` when the human explicitly asks for that in the
current request (e.g. “commit and push to main”).

Typical flow:

```bash
git checkout main && git pull
git checkout -b <type>/<short-description>
# … work …
git push -u origin HEAD
gh pr create
```

Prefer small, focused PRs (see mikey-in-a-box `coding-workflow`).

## Architecture Decision Records (ADRs)

Significant, durable choices go in [`docs/adr/`](adr/). Examples: language,
storage engine, transport model, security boundaries, harness strategy.

- One decision per ADR; use the next free `NNNN` number.
- Status is `Proposed` → `Accepted` → (`Deprecated` / `Superseded by ADR-NNNN`).
- Link related design docs and PRs.
- When an ADR changes how agents should work, update [`AGENTS.md`](../AGENTS.md)
  in the same change.

Template and index: [`docs/adr/README.md`](adr/README.md).

## Design docs

Major systems and subsystems get a design note under [`docs/design/`](design/)
**before or alongside** substantial implementation — not a post-hoc essay.

Use designs for “how this area works” (components, data flow, failure modes,
test plan). Use ADRs for “what we chose and why we rejected alternatives.”

Index and expectations: [`docs/design/README.md`](design/README.md).

Numbered overview docs (`01-wake-and-rearm`, `02-tech-stack`, …) stay as the
high-level narrative; designs go deeper per subsystem.

## mikey-in-a-box

This repo follows the [mikey-in-a-box](https://gitlab.com/huddo121/mikey-in-a-box)
philosophy skills. Install the plugin in your harness so those skills and
`/mikey-code-review` are available.

### Claude Code

```text
/plugin marketplace add https://gitlab.com/huddo121/mikey-in-a-box.git
/plugin install mikey-in-a-box@mikey-in-a-box
```

Update later with `/plugin marketplace update mikey-in-a-box`.

### Codex

Add `https://gitlab.com/huddo121/mikey-in-a-box.git` as a Codex plugin
marketplace, then install `mikey-in-a-box` from `/plugins` or the Codex app
Plugins UI.

### Cursor

Experimental. When ready, add the same git URL as a Cursor plugin marketplace
(plugin source: `harnesses/cursor` in that repo).

### Skills to apply here

| Skill | Use when |
|---|---|
| `architecture-and-layout` | Crate/module boundaries, layering |
| `type-driven-design` | Types, errors, parsing at edges |
| `testing-strategy` | Tests, fakes vs mocks |
| `tooling-and-ci` | fmt/clippy/CI gates |
| `logging-and-observability` | Logs after decisions |
| `coding-workflow` | Comments, PRs, using real CLIs |
| `/mikey-code-review` | Reviewing a change against the above |
