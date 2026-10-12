# Design: GitHub review-request watch

- Status: Implemented
- Related ADRs: [0001](../adr/0001-rust-bridge-subprocess-adapters.md),
  [0005](../adr/0005-baseline-via-protocol.md),
  [0022](../adr/0022-the-wake-carries-a-subject.md),
  [0028](../adr/0028-adapter-failure-wakes-nobody.md),
  [0030](../adr/0030-review-requests-are-a-mode-of-the-github-adapter.md)

## Goal

Wake a Claude Code session when a GitHub user is newly asked to review a pull
request, so an agent can start on the review without the user having to start it.

```bash
mailbox watch github-review-requests mikey-relevanceai
mailbox unwatch github-review-requests mikey-relevanceai
```

The motivating use (Mikey, 2026-10-12): people request his review, and he wants an
agent to pick a request up as it lands. The shape he has in mind is a long-lived
"dispatcher" session that holds this watch and, on each wake, starts a separate
reviewer agent with his own reviewing skills. **That is context, not part of this
project.** Mikey decided that agent-mailbox only delivers the wake. What the woken
agent does, and what it may do on GitHub without asking, is decided by whatever
receives the wake. Nothing here encodes a dispatch recipe or an autonomy policy.

## Non-goals

- **The backlog.** Requests pending when the watch starts never wake. An agent that
  wants them runs the search itself: `gh search prs --review-requested=<login> --state=open`.
- **Exits.** A PR leaving the set (reviewed, closed, request withdrawn) wakes nobody.
- **Content.** The event names the PR. Title, author and body stay on GitHub.
- **`@me`.** Refused: see [ADR-0030](../adr/0030-review-requests-are-a-mode-of-the-github-adapter.md).

## How GitHub is reached

Through `gh`, like the PR watch. One search per poll:

```text
gh api search/issues --method GET \
  -f q='is:pr is:open archived:false review-requested:<login>' \
  -f sort=created -f order=asc -f per_page=100 -f page=<n>
```

`review-requested:` matches a direct request and a request to a team the login is
on. Mikey chose that over `user-review-requested:` (direct only). For him, on
2026-10-12, both matched the same 13 PRs, and `team-review-requested:` matched 0
PRs, open or closed, for either of his two teams (measured;
[ADR-0030](../adr/0030-review-requests-are-a-mode-of-the-github-adapter.md) has the
numbers). So the difference has not shown up in his data. `archived:false` drops PRs nobody can review.

The notifications API was the alternative. It was measured and rejected: it had a
thread for 1 of the 13 pending requests, and that thread's reason was `mention`.

### Rate limit

Search allows 30 authenticated requests a minute (GitHub REST docs), in its own
bucket, not the `core` or `graphql` buckets the `github-pr` pollers spend (measured
with `gh api rate_limit`). A poll costs one search request for each 100 pending
requests. At the default `--interval 60` that is 1 of 30 a minute. Nothing enforces
a floor. Below about 2s, one watch alone exceeds the budget. Any other `gh search`
run as the same user shares it. When search is exhausted the adapter backs off and
retries like the PR mode, and exits non-zero once 12 retries in a row are rate-limited too.

## Shape

```text
mailbox watch github-review-requests <login>
  → CLI parses the login (clap value parser → GithubLogin; refuses @me)
  → daemon: Request::WatchReviewRequests{login} → record the watch + interest, subscribe
  → supervisor → GithubResolver → mailbox-github-pr-adapter
       config {kind: "github-review-requests", topic, login, interval_ms, baseline}
  → adapter: gh search → diff against baseline → Publish / Baseline lines
  → host relays; the bridge wakes subscribers
```

- **Target and topic** (`mailbox-protocol::github_review`). `GithubLogin` accepts
  1–39 ASCII letters, digits and hyphens, not starting with a hyphen, and stores
  them lowercased. The character rule matters beyond tidiness. The login is spliced
  into a search query, so it must not be able to carry a space or `:` that adds a
  qualifier. It parses on deserialize, so the control frame, the stored row and the
  adapter's config are each validated by decoding them. Topic:
  `github.review-requests.<login>`.
- **Storage.** Kind `github-review-requests` on the existing flat row: the login in
  `repo`, `0` in `pr` and `publish_count`, `[]` in `filters`. No migration: `kind`
  is free text. `build_watch` parses the login back. An invalid login or a non-zero
  `pr` is corrupt.
- **One adapter program** ([ADR-0030](../adr/0030-review-requests-are-a-mode-of-the-github-adapter.md)).
  `GithubResolver` serves both GitHub kinds and puts `kind` in the config. The
  adapter's poll loop is generic over a `Watcher` (`adapters/github-pr-adapter/src/watcher.rs`).
  The review-request mode is `review_requests.rs` (the set and the diff, pure) and
  `review_search.rs` (the search, paging and parsing). A config with no `kind` is a PR
  watch, so a daemon still running the old resolver can spawn the new binary.

### What wakes

| Poll sees | Fires |
|---|---|
| First poll (no baseline) | nothing; everything pending is recorded |
| A PR not in the baseline | one `review_requested` event |
| A PR already in the baseline | nothing |
| A PR missing for one poll, then back | nothing |
| A PR missing for two consecutive polls, then back (a re-request) | one `review_requested` event |
| `incomplete_results`, a malformed item, the set changing mid-paging | the poll is skipped; baseline unchanged |
| More than 1,000 pending requests | the adapter exits non-zero; the watch shows `failed` |

### Baseline

```json
{"requested": ["acme/web#1", "acme/api#7"], "leaving": []}
```

`requested` is the last poll's set. `leaving` holds PRs that were in the set one poll
ago and missing from the last. They are still "known", so reappearing does not wake.
A PR missing from two consecutive polls is dropped from both sets, and its next
appearance wakes. The guard exists because a search result that briefly omitted PRs
that are still requested would otherwise re-fire all of them on the next poll. That
failure has not been observed; the guard is cheap insurance against a burst of
wasted turns. Both sets are bounded by the 1,000-result cap.

The baseline is emitted after the poll's events, as in the PR mode, so a crash
re-fires an event rather than dropping one.

### Paging

Results are sorted by PR creation time (`sort=created`, oldest first), 100 to a
page. That gives a stable order, not an append-only one: a new request on an old PR
lands mid-list and shifts later results across a page boundary. The poll keeps
fetching pages until the distinct PRs it has collected equal the page's
`total_count`. A short page, an overshoot, or the 10-page cap before that point
means the set changed under it, and the poll is skipped. A removal and a shift that
cancel can pass that check with one PR missing. That PR is then absent for one
poll, which the two-poll forget absorbs, so it does not fire when it reappears.
This is reasoned from the paging, not observed; a queue of one page (the one
measured had 13–14 PRs) cannot hit it.

### Subject and body

```json
{"subject": {"text": "review requested: RelevanceAI/arg#7353",
             "link": "https://github.com/RelevanceAI/arg/pull/7353"},
 "body": {"source": "github-review-requests", "edge": "review_requested",
          "repo": "RelevanceAI/arg", "pr": 7353,
          "url": "https://github.com/RelevanceAI/arg/pull/7353"}}
```

The link is the search item's `html_url`, GitHub's own URL for the PR, so a GitHub
Enterprise host stays correct.

## Failure modes

| Failure | Behaviour |
|---|---|
| `gh` not authenticated | adapter exits non-zero → supervisor backoff → `failed`, retried each sweep (ADR-0011); nothing is published (ADR-0028) |
| Search rate limit | in-adapter backoff, 12 retries, then exit non-zero (as above) |
| Partial or malformed result | poll skipped, baseline kept; exit non-zero after 10 in a row |
| Daemon restart | the watch resumes iff an interested session is alive; the persisted baseline means nothing re-fires (ADR-0010, 0005) |
| Request made and withdrawn between two polls | never seen; nothing fires |
| Re-request within one interval of the review | not seen as an exit; nothing fires |

## Test plan

- `mailbox-protocol`: `GithubLogin` lowercases, refuses `@me` with the way to find a
  login, and refuses everything that could change the query.
- Adapter unit tests (`review_requests.rs`): entry fires once; exit fires nothing;
  re-request after two absent polls fires; a one-poll gap does not; the baseline
  round-trips and a foreign one does not parse; subject and body carry no title.
- Adapter unit tests (`review_search.rs`): paging over canned pages, including the
  over-1,000 refusal, a short page, an overshoot and the page cap; fail-closed
  parsing of every malformed shape.
- Adapter e2e (`tests/adapter_e2e.rs`, fake `gh`): silent baseline then one wake;
  the exact query sent; re-request and restart-without-refire; `incomplete_results`
  skipped; a 101-request queue paged as one observation.
- Bridge e2e (`crates/mailbox/tests/github_pr_e2e.rs`): `watch` → real daemon →
  real adapter → one `review_requested` event in `read`, baseline in storage, watch
  in `status`, `unwatch` stops the adapter; `@me` refused at the CLI.
- Storage, control-frame and resolver unit tests for the new kind.
