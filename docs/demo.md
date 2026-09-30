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
2. **wake an idle session** — stand in for Claude Code by binding an inbox socket and
   registering the session the way Claude Code does, then `publish` and watch the
   DAEMON write that socket with the topic NAME only — the
   asyncRewake contract the harness turns into a session wake. There are no sleeps in
   this step: the daemon writes the inbox socket before it answers the publish.
3. **supervised adapter** — `watch stub` records interest and the daemon spawns
   the reference stub poller; `status` shows it `running` with a child pid;
   `read` drains its synthetic edges. The agent never launched this loop.
4. **teardown** — `unwatch` drops the last interest and the supervisor stops the
   adapter; `status` shows `stopped`, no child. **No zombie poller.**

The script asserts a wake actually reached the inbox, and that it carried no event
body, and fails loudly otherwise — so it doubles as a smoke test of the
whole path.

### Expected output

Captured from a clean run on macOS (`SKIP_BUILD=1 scripts/demo.sh`, ANSI colour
stripped). Three things legitimately differ run to run, so do not read them as a
contract: the **tempdir path and pids**; the **absolute binary path** the script
echoes on each `$` line, abbreviated to `mailbox` below; and the **number of stub
events** in step 3, since the stub publishes on a 500 ms timer and how many have
landed by the time `read` runs is a race (2 and 3 are both normal).

```text
demo workdir: /var/folders/.../mailbox-demo.wFPR9y

== 0. start the bridge daemon (mailbox serve) ==
daemon up (pid 47854), socket at /var/folders/.../mailbox-demo.wFPR9y/mailbox.sock

== 1. four-verb core: subscribe, publish, read ==
$ mailbox subscribe demo.hello
subscribed to demo.hello (new, empty topic (no baseline))
$ mailbox publish demo.hello --body '{"msg":"first"}'
published event evt-1 at offset 0
$ mailbox read
1 unread event(s):
  [demo.hello] offset=0 id=evt-1 body={"msg":"first"}

== 2. wake an idle session (the inbox-socket contract) ==
running the SessionStart hook (registers this session's agent inbox) ...
publishing while the session is idle ...
$ mailbox publish demo.hello --body '{"msg":"wake up"}' --subject 'the demo said hello' --link 'https://example.com/demo'
published event evt-2 at offset 1
the wake delivered to the session's inbox (what changed and where — never the body):
    {"message":{"content":"[agent-mailbox] mail on 1 topic — run `mailbox read`\n\ndemo.hello — 1 unread\n  · the demo said hello\n    https://example.com/demo\n","role":"user"},"type":"user"}
$ mailbox read
1 unread event(s):
  [demo.hello] offset=1 id=evt-2 body={"msg":"wake up"}
      the demo said hello — https://example.com/demo

== 3. supervised adapter: watch stub, see it running, read its edges ==
$ mailbox watch stub demo --interval-ms 500
watching stub.demo (interest=1, subscription: new, empty topic (no baseline))
$ mailbox status
session: demo-session
wake: reachable
inbox topic: agent.demo-session (registered)
watches:
  stub demo  state=running interest=1 interval=500ms child=pid 47934
subscriptions (3):
  agent.demo-session
  demo.hello
  stub.demo
unread:
  [stub.demo] 3
$ mailbox read --limit 5
3 unread event(s):
  [stub.demo] offset=0 id=evt-3 body={"seq":0,"source":"stub"}
      stub event 0
  [stub.demo] offset=1 id=evt-4 body={"seq":1,"source":"stub"}
      stub event 1
  [stub.demo] offset=2 id=evt-5 body={"seq":2,"source":"stub"}
      stub event 2

== 4. unwatch -> the supervisor stops the adapter (no zombie poller) ==
$ mailbox unwatch stub demo
unwatched stub.demo (remaining interest=0)
$ mailbox status
session: demo-session
wake: reachable
inbox topic: agent.demo-session (registered)
watches:
  stub demo  state=stopped interest=0 interval=500ms child=stopped
subscriptions (2):
  agent.demo-session
  demo.hello
unread: none

OK — subscribe/read, idle-wake, supervised watch, and teardown all worked.
For the real GitHub-PR wake on a live Claude Code session, see docs/demo.md.
```

(The inbox is registered from step 2 onward because that is where the demo runs
`mailbox harness session-start` by hand — the same `SessionStart` hook Claude Code
runs for you. Once it is registered, peers can `mailbox send` to the session; see
[04-usage §4](04-usage.md#4-agent-to-agent-messaging).)

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

2. **In a Claude Code session**, have the agent watch the PR. The session id comes
   from `$CLAUDE_CODE_SESSION_ID`, which Claude Code sets for every command it runs;
   from a plain shell, set it yourself (`CLAUDE_CODE_SESSION_ID=<id> mailbox …`):

   ```bash
   mailbox watch github-pr OWNER/REPO#N --interval 60
   ```

   `status` should now show the watch `running` with a child pid:

   ```bash
   mailbox status
   #   github-pr REPO#N  state=running interest=1 interval=60s child=pid <...>
   ```

   The adapter **baselines on its first poll** — it publishes nothing for state
   that already exists, only for *transitions* after it starts watching.

3. **Let the session go idle.** The hooks keep it armed. Do not run any arm
   command — that is the whole point.

4. **Trigger an edge** on the PR (any one of these):
   - **Merge** — merge the PR, so GitHub flips its state to `MERGED` (the edge
     fires once and is terminal).
   - **Merge conflict** — push a commit to the PR's base branch that conflicts
     with the PR, so GitHub flips it to `CONFLICTING`.
   - **Review** — request changes / leave a review or a review-thread comment.
   - **CI failure** — push a commit that makes a required check fail (the edge
     fires on the rollup transitioning *into* failure and names the failed
     checks).

5. **Watch the idle session wake.** Within ~one poll interval the adapter publishes
   the edge, the daemon writes the session's inbox socket, and Claude Code starts a
   turn with a message like:

   ```text
   [agent-mailbox] mail on 1 topic — run `mailbox read`

   github.pr.OWNER/REPO#N — 1 unread
     · CI failed: build
       https://github.com/OWNER/REPO/actions/runs/…
   ```

   The agent then:

   ```bash
   mailbox read
   #   1 unread event(s):
   #     [github.pr.OWNER/REPO#N] offset=… id=… body={"source":"github-pr","edge":"mergeable_conflicting","repo":"OWNER/REPO","pr":N}
   ```

   The body is a small, opaque label plus the deltas that fired it — e.g.
   `"edge":"pr_merged"` when the PR merges, `"edge":"mergeable_conflicting"` for a
   conflict, `"edge":"new_reviews"` with `previous_max_id`/`current_max_id` for a
   review, or `"edge":"ci_failure"` with `"rollup":"failure"` and a `newly_failed`
   list of `{name, url}` checks for CI. Each event also carries the one-line
   `subject` the wake showed, with a link to the comment, review or failing run
   itself ([ADR-0022](adr/0022-the-wake-carries-a-subject.md)).

   and reacts (react to the merge, fix the conflict, address the review, fix CI).

6. **Done — clean up:**

   ```bash
   mailbox unwatch github-pr OWNER/REPO#N
   ```

   or just end the session — `SessionEnd` suspends the interest and stops the poller
   if no other session is watching that PR. Resuming the session restarts it.

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
