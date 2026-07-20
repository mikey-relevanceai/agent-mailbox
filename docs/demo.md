# Demo: watch a change, wake an idle session

Two demos:

- **[Local demo](#local-demo-no-network)** — `scripts/demo.sh`, fully
  self-contained (no GitHub, no Claude Code). Proves every command in the loop
  and the idle-wake mechanic on your machine. **Run this first.**
- **[Real PR demo](#real-pr-demo-human-driven)** — the end-to-end story a human
  runs: an idle Claude Code session woken by a real edge on a real pull request.
  Needs `gh` auth, a PR you can push to, and a Claude Code session.

---

## Local demo (no network)

```bash
scripts/demo.sh          # builds if needed, then runs
SKIP_BUILD=1 scripts/demo.sh   # skip the build if the binaries are current
```

The script starts a private `mailbox serve` in a throwaway tempdir (it never
touches your real `~/.agent-mailbox` or `~/.claude`), then walks four steps:

0. **start the daemon** — `mailbox serve`, wait for its socket.
1. **four-verb core** — `subscribe` → `publish` (standing in for an adapter) →
   `read`.
2. **wake an idle waiter** — start `mailbox wait` with no mail pending so it
   *blocks* (exactly what the `SessionStart`/`Stop` hook does), then `publish`
   and watch it exit **2** with a payload-free `mail on topic X` reminder. This
   is the asyncRewake contract that the harness turns into a session wake.
3. **supervised adapter** — `watch stub` records interest and the daemon spawns
   the reference stub poller; `status` shows it `running` with a child pid;
   `read` drains its synthetic edges. The agent never launched this loop.
4. **teardown** — `unwatch` drops the last interest and the supervisor stops the
   adapter; `status` shows `stopped`, no child. **No zombie poller.**

The script asserts the idle waiter woke with exit 2 and fails loudly otherwise,
so it doubles as a smoke test of the whole path.

### Expected output

Captured from a clean run on macOS (`SKIP_BUILD=1 scripts/demo.sh`; paths and
pids vary, ANSI colour stripped):

```text
demo workdir: /var/folders/.../mailbox-demo.EzyeKX

== 0. start the bridge daemon (mailbox serve) ==
daemon up (pid 23379), socket at /var/folders/.../mailbox-demo.EzyeKX/mailbox.sock

== 1. four-verb core: subscribe, publish, read ==
$ mailbox subscribe demo.hello --session demo-session
subscribed to demo.hello (new, empty topic (no baseline))
$ mailbox publish demo.hello --body '{"msg":"first"}'
published event evt-1 at offset 0
$ mailbox read --session demo-session
1 unread event(s):
  [demo.hello] offset=0 id=evt-1 body={"msg":"first"}

== 2. wake an idle waiter (the asyncRewake contract) ==
starting a blocking waiter (no mail yet) ...
publishing while the waiter is idle ...
$ mailbox publish demo.hello --body '{"msg":"wake up"}'
published event evt-2 at offset 1
waiter exit code: 2   (2 = woken; the harness turns this into a wake)
waiter reminder (stderr, payload-free):
    mail on topic demo.hello
    wake reason: kicked
$ mailbox read --session demo-session
1 unread event(s):
  [demo.hello] offset=1 id=evt-2 body={"msg":"wake up"}

== 3. supervised adapter: watch stub, see it running, read its edges ==
$ mailbox watch stub demo --interval-ms 500 --session demo-session
watching stub.demo (interest=1, subscription: new, empty topic (no baseline))
$ mailbox status --session demo-session
session: demo-session
inbox: agent.demo-session (NOT registered — peers cannot send to this session)
watches:
  stub demo  state=running interest=1 interval=500ms child=pid 23407
subscriptions:
  demo.hello
  stub.demo
unread:
  [stub.demo] 2
$ mailbox read --session demo-session --limit 5
2 unread event(s):
  [stub.demo] offset=0 id=evt-3 body={"seq":0,"source":"stub"}
  [stub.demo] offset=1 id=evt-4 body={"seq":1,"source":"stub"}

== 4. unwatch -> the supervisor stops the adapter (no zombie poller) ==
$ mailbox unwatch stub demo --session demo-session
unwatched stub.demo (remaining interest=0)
$ mailbox status --session demo-session
session: demo-session
inbox: agent.demo-session (NOT registered — peers cannot send to this session)
watches:
  stub demo  state=stopped interest=0 interval=500ms child=stopped
subscriptions:
  demo.hello
unread: none

OK — subscribe/read, idle-wake, supervised watch, and teardown all worked.
```

(The `inbox: … (NOT registered)` line is expected here: the demo drives the CLI
by hand with no Claude Code hooks, and it is `mailbox harness arm` — the
`SessionStart`/`Stop` hook — that registers a session's inbox. In a real session
with the hooks installed it reads `inbox: agent.<id> (registered)`, and peers can
`mailbox send` to it; see [04-usage §4](04-usage.md#4-agent-to-agent-messaging).)

(The `wake reason: kicked` line is a diagnostic the script opts into via
`MAILBOX_WAIT_DEBUG=1`; a real wake surfaces only the payload-free
`mail on topic …` reminder.)

---

## Real PR demo (human-driven)

This is the acceptance story: an **idle Claude Code session** woken by a **real
edge** on a **real pull request**. It needs a human because it requires `gh`
auth, a PR you can mutate, and a live Claude Code session — none of which the
local stack can stand in for.

### Prerequisites

- `mailbox`, `mailbox-github-pr-adapter`, and `mailbox-stub-adapter` installed
  **co-located** on `PATH` (see [04-usage.md § Install](04-usage.md#1-install)).
- `gh` authenticated (`gh auth status` is clean).
- A pull request you can push to — call it `OWNER/REPO#N`.
- Claude Code hooks installed (`mailbox harness install-hooks`, which merges into
  `~/.claude/settings.json` when it exists).

### Steps

1. **Start the bridge** (once, in its own pane/service):

   ```bash
   mailbox serve
   ```

2. **In a Claude Code session**, have the agent watch the PR. With the hooks
   installed the session id comes from Claude Code; from a shell you pass it
   explicitly:

   ```bash
   mailbox watch github-pr OWNER/REPO#N --interval 60 --session "$MAILBOX_SESSION_ID"
   ```

   `status` should now show the watch `running` with a child pid:

   ```bash
   mailbox status --session "$MAILBOX_SESSION_ID"
   #   github-pr REPO#N  state=running interest=1 interval=60s child=pid <...>
   ```

   The adapter **baselines on its first poll** — it publishes nothing for state
   that already exists, only for *transitions* after it starts watching.

3. **Let the session go idle.** The `Stop` hook arms a waiter. Do not run any
   arm command — that is the whole point.

4. **Trigger an edge** on the PR (any one of these):
   - **Merge** — merge the PR, so GitHub flips its state to `MERGED` (the edge
     fires once and is terminal).
   - **Merge conflict** — push a commit to the PR's base branch that conflicts
     with the PR, so GitHub flips it to `CONFLICTING`.
   - **Review** — request changes / leave a review or a review-thread comment.
   - **CI failure** — push a commit that makes a required check fail (the edge
     fires on the rollup transitioning *into* failure and names the failed
     checks).

5. **Watch the idle session wake.** Within ~one poll interval the adapter
   publishes the edge, the bridge kicks the waiter, and Claude Code surfaces a
   system reminder (`mail on topic github.pr.OWNER/REPO#N`). The agent then:

   ```bash
   mailbox read --session "$MAILBOX_SESSION_ID"
   #   1 unread event(s):
   #     [github.pr.OWNER/REPO#N] offset=… id=… body={"source":"github-pr","edge":"mergeable_conflicting","repo":"OWNER/REPO","pr":N}
   ```

   The body is a small, opaque label plus the deltas that fired it — e.g.
   `"edge":"pr_merged"` when the PR merges, `"edge":"mergeable_conflicting"` for a
   conflict, `"edge":"new_reviews"` with `previous_max_id`/`current_max_id` for a
   review, or `"edge":"ci_failure"` with `"rollup":"failure"` and a `newly_failed`
   list of check names for CI.

   and reacts (react to the merge, fix the conflict, address the review, fix CI).

6. **Done — clean up:**

   ```bash
   mailbox unwatch github-pr OWNER/REPO#N --session "$MAILBOX_SESSION_ID"
   ```

   or just end the session — `SessionEnd` drops the interest and stops the poller
   if no other session is watching that PR.

### Rehearse the mechanics without a PR

Every command above is exercised by the automated suite against a **recorded
fake `gh`** (`crates/mailbox/tests/github_pr_e2e.rs`,
`adapters/github-pr-adapter/tests/adapter_e2e.rs`), so the `watch github-pr` →
baseline → conflict-edge → `read` chain is proven with no network in
`cargo test --workspace`. The only thing a human adds in the real demo is a
*live* PR, `gh` auth, and Claude Code turning the exit-2 wake into a visible
session reminder. If you want to see a conflict edge flow without a real PR, run
that test with output:

```bash
cargo test -p mailbox --test github_pr_e2e -- --nocapture
```
