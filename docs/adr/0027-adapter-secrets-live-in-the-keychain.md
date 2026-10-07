# ADR-0027: An adapter's secret lives in the Keychain, read by that adapter alone

- Status: Proposed
- Date: 2026-10-07

## Context

The Slack adapter ([design/02](../design/02-slack-watch.md)) is the first adapter that
needs a credential of its own. `github-pr` never held one: it shells out to `gh`,
which keeps the user's GitHub token in `gh`'s own store. Slack has no equivalent CLI
that a member can log in to; a token comes from a Slack app, as a `xoxb-` string
someone has to keep somewhere.

Nothing in this repo said where. Two facts about the bridge narrow the answer:

- **Every adapter inherits the daemon's whole environment.** The subprocess host
  spawns children with no `env_clear` (`crates/mailbox/src/host/subprocess.rs`), and
  the e2e tests depend on that to hand fakes to adapters. A token in the daemon's
  environment would therefore reach the stub and `gh` adapters too, and anything
  they spawn.
- **Everything the bridge passes to an adapter can end up on disk.** The spawn
  config is built from the watch row, and baselines are stored in SQLite. Neither
  is a place for a secret.

## Decision

1. **The token is stored in the macOS Keychain** under the service name
   `agent-mailbox.slack`, by the user:
   `security add-generic-password -s agent-mailbox.slack -a "$USER" -w`.
2. **The adapter that needs it reads it itself**, once at start, through
   `security find-generic-password -s agent-mailbox.slack -w`. The bridge never
   sees it: it is not in the watch row, the spawn config, the baseline, an
   environment variable, `settings.json`, or a log. The adapter's `Token` type
   redacts its `Debug`.
3. **It reaches Slack through `curl`, with the token on curl's stdin** (`-K -`, a
   config file read from stdin), never as an argument, so it is not visible in the
   process table. The adapter refuses a Keychain value that is not a bare
   `xoxb-`/`xoxp-` word rather than escaping it into that config.
4. Tests swap the binaries, not the secret: `MAILBOX_SLACK_SECURITY_BIN` and
   `MAILBOX_SLACK_CURL_BIN` point at fakes, the same way `MAILBOX_GH_BIN` does.
   There is deliberately no `MAILBOX_SLACK_TOKEN` variable, because it would be
   the environment route under another name.

A future adapter that needs a secret follows the same pattern under its own
`agent-mailbox.<name>` service.

## Consequences

- The security posture matches `github-pr`'s: the credential lives in an OS or
  vendor store, and the bridge only ever starts a process that knows how to use it.
- **macOS only, for now.** `security` does not exist on Linux, and there the
  adapter fails at start with "no Slack token in the Keychain". A Linux user would
  need a `secret-tool` (libsecret) equivalent; nobody has asked for one yet.
- The Keychain must be unlocked, which it is for a logged-in user. A daemon started
  before login would fail its Slack watches until the supervisor's retry runs after
  login.
- `curl` becomes a runtime dependency of the Slack adapter. It ships with macOS.
- Rotating the token means re-running `add-generic-password` (with `-U`) and
  waiting for the adapter's next restart; it reads the token once per process.

## Alternatives considered

- **An environment variable on the daemon** (`SLACK_TOKEN`). Simplest, and leaks to
  every adapter because of the inherited environment. Rejected.
- **A file under `~/.agent-mailbox`** with `0600`. Works on Linux too, but it is a
  plaintext secret in a directory that also holds the database and is easy to copy
  or back up by accident. The Keychain is the store macOS already gives us.
- **Scrubbing the adapter environment** so an env var only reaches the Slack
  adapter. The right change eventually, but it alters what every existing adapter
  and test inherits, and it still leaves the secret in the daemon's own environment
  for its whole life. Out of scope here.
- **An HTTP client crate** (`reqwest`/`ureq`) instead of `curl`. Adds a TLS stack to
  a small adapter and turns the test fake from a shell script into an HTTP server.
  `curl` keeps the adapter as thin as `github-pr`'s use of `gh`.
