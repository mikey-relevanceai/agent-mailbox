# ADR-0030: Review requests are watched through GitHub search, by a second mode of the GitHub adapter

- Status: Proposed
- Date: 2026-10-12
- Design: [design/03](../design/03-review-request-watch.md)

## Context

Mikey asked for a session to be woken when someone requests his review on a PR, so
an agent can start on the review without him kicking it off. Every GitHub watch
before this one was `github-pr <owner>/<repo>#<n>`, tied to a PR somebody already
knew about. A review request arrives on a PR nobody is watching yet. Before this
change no adapter or bridge code mentioned review requests (measured:
`grep -rniE 'review_?request|requested_?reviewer|search prs' adapters crates`
matched nothing).

Three decisions had to be made: where the set of requests comes from, what counts as
a wake, and which program polls it.

### Where the requests come from, measured

All read-only `gh` calls as `mikey-relevanceai` on 2026-10-12:

- **Search** (`search/issues`, `is:pr is:open review-requested:<login>`) matched 13
  open PRs, all on `RelevanceAI/arg`. Every one lists `mikey-relevanceai` by name in
  its `requested_reviewers`. A later run the same day matched 14.
- **The notifications API** had a thread for **1** of those 13 PRs, and that
  thread's `reason` was `mention`, not `review_requested`. The newest
  `review_requested` notification was dated 2026-10-07, though requests were made on
  10-08, 10-11 and 10-12. 142 unread `review_requested` notifications from July and
  August were still in the list. Why the recent ones are missing was not
  investigated. A notification thread carries one `reason`, and its existence
  depends on the user's subscription and read state, so it reports what happened to
  Mikey's inbox, not which reviews are pending.

### Which qualifier

GitHub's search docs say `review-requested:<login>` also matches a request made to
a team the login is on, and `user-review-requested:<login>` matches only direct
requests. For Mikey both matched the same 13 PRs. He is on two teams
(`RelevanceAI/arg`, `RelevanceAI/arg-core`), and `team-review-requested:` matched 0
PRs for either, open or closed. So the difference has never shown up in his data.
**Mikey chose direct plus team** (`review-requested:`).

### The budget

GitHub's REST docs give authenticated search 30 requests a minute, across all search
endpoints except code search. `gh api rate_limit` reports `search` as its own
bucket, separate from `core` and `graphql` (measured). A `search/issues` call raised
`search.used` by one. A `gh pr view` and a `gh api …/reviews` call, the PR poller's
own calls, left it unchanged (measured). One poll is one search request for each 100
pending requests. At the default 60s interval that is 1 of the 30 a minute, and
none of the budget the PR pollers spend. Anything else that runs `gh search` as the
same user shares the 30.

## Decision

1. **Search is the source.** The adapter asks
   `is:pr is:open archived:false review-requested:<login>` every interval. It does
   not use the notifications API.
2. **Wake on entry into the set, never on exit.** The first poll records what is
   already pending and publishes nothing. A PR that appears in a later poll publishes
   one `review_requested` event. A PR that leaves (reviewed, closed, request
   withdrawn) publishes nothing. A re-request after a review is a fresh entry and
   wakes. That rests on GitHub's REST docs ("Review requests"): "Once a requested
   reviewer submits a review, they are no longer considered a requested reviewer."
   Mikey's data agrees: 13 open PRs he has reviewed were all absent from his
   `review-requested:` search, and 0 matched both (measured 2026-10-12). No open PR
   had been re-requested at the time, so a re-request reappearing in search has not
   been observed. The set is the baseline,
   persisted through the bridge as every adapter's is ([ADR-0005](0005-baseline-via-protocol.md)).
3. **One watch per person, named by login.** `mailbox watch github-review-requests
   <login>`, topic `github.review-requests.<login>`. The login is lowercased, because
   GitHub logins are case-insensitive. `@me` is refused. It means whoever the
   daemon's `gh` is logged in as, which can change under a running watch, and a
   topic has to name one person.
4. **The same program as `github-pr`, in a second mode.** The resolver puts a `kind`
   in the adapter's config, and `mailbox-github-pr-adapter` runs the matching mode.
   A config with no `kind` is a PR watch.

### Why one program

Everything except the observe-and-diff step is identical between the two: `gh`
invocation and its auth/rate-limit/parse classification, the bounded rate-limit
backoff, the transient-failure budget, the baseline read/emit, and signal handling.
Inside the adapter, that is now one poll loop generic over a `Watcher` trait. A
second binary would have needed that code twice, or a new shared crate. It would
also have meant a fifth entry in every list that names the shipped binaries: the
release workflow, the Homebrew formula renderer and its tests, `install-local.sh`
and the test harness. The Slack adapter is the precedent: one program serves both
Slack kinds.

The cost is the name. `mailbox-github-pr-adapter` now polls something that is not
one PR. Renaming it would touch those same packaging lists and break
`MAILBOX_GH_ADAPTER_BIN` overrides, for a cosmetic gain, so it keeps its name.

The `kind` default exists for rollout. The upgraded binary can be installed while
the old daemon is still running, and that daemon's resolver sends no `kind`. With a
required `kind`, every PR watch it spawned in that window would crash.

## Consequences

- An agent can be woken by a review request it did not know was coming.
- What the woken agent does is not this project's concern. Mikey decided that
  whatever receives the wake decides what it may do. The watch encodes no policy
  about posting, reviewing or dispatching.
- **Requests that predate the watch never wake.** That is the edge-trigger, and it
  is deliberate: starting a watch must not deliver a backlog. An agent that wants
  the backlog can run the same search itself.
- **A PR is forgotten only after two consecutive polls without it.** A search result
  that briefly drops PRs that are still requested would otherwise make them all
  "enter" again on the next poll, one wake each. We have not observed such a drop;
  the cost if it happened is what motivates the guard. The price is that a re-request
  within one interval of the review does not wake.
- **A partial search result is skipped, not diffed.** `incomplete_results: true`, a
  malformed item, or a set that changed while paging is a transient failure. The
  baseline is kept, and the poll is retried next interval.
- **More than 1,000 pending requests is fatal.** Search returns at most 1,000
  results, so a larger set cannot be told apart from churn. The adapter exits
  non-zero and the watch shows `failed` rather than waking on PRs that rotate in and
  out of view.
- `status` and the status JSON show the login in the existing `repo` field, as a
  Slack watch shows its key there.
