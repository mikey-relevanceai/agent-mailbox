# ADR-0034: `brew services` runs the daemon

- Status: **Accepted**
- Date: 2026-11-25
- Builds on: [ADR-0004](0004-cli-serve-daemon-and-socket.md) (clients never spawn
  the daemon) and [ADR-0025](0025-hooks-point-at-a-stable-path.md) (point at the
  `opt` path, never the Cellar).

## Context

Every command except `doctor` needs `mailbox serve` running, and clients never start
it ([ADR-0004](0004-cli-serve-daemon-and-socket.md)). Until now the docs said "start
it once: a login item, a `tmux` pane, or a user service" and left the user to build
one. On the maintainer's machine that is a hand-written LaunchAgent. A Homebrew user
had nothing.

The ask was that `brew install` register the daemon with the OS: launchd on macOS,
systemd on Linux.

What Homebrew allows decides the shape. These facts were read from the installed
Homebrew 7.0.14 source (`Library/Homebrew/service.rb`, `formula_installer.rb`,
`caveats.rb`, `upgrade.rb`) and its `docs/Formula-Cookbook.md`:

- **A formula cannot start its own service.** `post_install` is deprecated and
  rejected by audits. Its replacement, `post_install_steps`, is a fixed list of
  sandboxed file operations with no service step.
- **A `service do` block is the supported route.** `brew install` writes the
  generated plist or unit into the keg. `brew services start` copies it into
  `~/Library/LaunchAgents` (macOS) or `~/.config/systemd/user` (Linux), and loads it.
  Homebrew then adds the `brew services start` command to the formula's caveats.
- **`brew upgrade` does not restart a running service.** `upgrade.rb` contains no
  service handling. Homebrew's caveat then reads "To restart … after an upgrade:
  `brew services restart`".
- **The two platforms read different knobs.** `throttle_interval` is written only to
  the plist. `restart_delay` becomes `RestartSec` in the unit, and the plist's
  `TimeOut` key, which `launchd.plist(5)` documents as "no longer implemented".
- **The default names changed.** 7.0.14 names both files `sh.brew.<formula>`. Older
  versions used `homebrew.mxcl.<formula>` (plist) and `homebrew.<formula>` (unit),
  and 7.0.14 still recognises them.

## Decision

**The formula declares a `service do` block, and the user starts it with one
command, `brew services start mikey-relevanceai/tap/mailbox`.** That one command is
as far as "`brew install` registers the daemon" can go.

The block, in `scripts/render-formula.sh`:

| Setting | Why |
|---|---|
| `run [opt_bin/"mailbox", "serve"]` | The `opt` path survives `brew upgrade`. The Cellar path does not ([ADR-0025](0025-hooks-point-at-a-stable-path.md)). |
| `keep_alive true` | A crash restarts it. On Linux this is `Restart=on-failure`, so `systemctl --user stop` still stops it, and so does a `SIGTERM`, which the daemon answers with a clean exit. |
| `throttle_interval 10` + `restart_delay 10` | At most one restart per 10 s on both platforms. Without `RestartSec`, systemd retries after 100 ms, hits its default limit of 5 starts in 10 s, and leaves the unit failed for good. A daemon refused by another daemon's lock would do exactly that. |
| `environment_variables PATH: std_service_path_env` | launchd and systemd start jobs with a `PATH` that lacks Homebrew's `bin`, so the github-pr adapter could not find `gh`. |
| `log_path` / `error_log_path` → `var/"log/mailbox.log"` | Homebrew's convention, created by `brew install`, and the same file on both platforms. The daemon logs to stderr. |

It runs as the user, never as root: the daemon reads that user's Claude Code session
registry and writes their sessions' inbox sockets.

The caveats tell the user four more things: the log path, that only one daemon can
own `~/.agent-mailbox`, that an upgrade needs `brew services restart`, and, on Linux,
`loginctl enable-linger`.

## Consequences

- **A Homebrew user gets a supervised daemon in one command.** It starts at login,
  restarts 10 s after a crash, and logs to a known file.
- **An upgrade is two commands, not one.** `brew upgrade` leaves the old daemon
  running the old code, holding the lock. The docs and caveats say
  `brew services restart mailbox`. On Linux the old daemon then runs from a deleted
  file: `brew upgrade` removed the old Cellar directory immediately, and
  `/proc/<pid>/exe` read `…/Cellar/mailbox/0.3.0/bin/mailbox (deleted)` (measured,
  below). It can still spawn adapters, but they come from the new version. Its
  lookup beside its own executable finds nothing, so `resolver.rs` falls back to
  the bare name on `PATH`. In the test, a watch started after the upgrade ran
  `…/Cellar/mailbox/0.3.2/bin/mailbox-stub-adapter` under a daemon still running
  0.3.1. So nothing fails, but bridge and adapters run different versions until
  the restart. On macOS `current_exe` keeps the `opt` path the daemon was started
  through, so its adapters are the new version too (inferred, not measured).
- **Linux needs lingering.** A systemd user manager runs only while the user has a
  session, unless `loginctl enable-linger` is set, and `brew services` does not set
  it. Without it, logging out of the last session stops the daemon, while Claude
  Code sessions in a surviving `tmux` live on with nothing to wake them.
- **Linux has no Slack.** Nothing changes from
  [ADR-0027](0027-adapter-secrets-live-in-the-keychain.md): the adapter reads its
  tokens through the macOS `security` CLI, which Linux lacks. GitHub watches work.
- **The service does not see the user's shell environment.** `CLAUDE_CONFIG_DIR` or
  `MAILBOX_CLAUDE_SESSIONS_DIR` exported in a shell never reaches it, and a daemon
  looking in the wrong sessions directory wakes nobody. Homebrew merges
  `~/.homebrew/services/mailbox.env` into the service definition, so that file is
  where such overrides go.
- **Two daemons cannot run.** A service started while a hand-made daemon holds the
  lock exits and is restarted every 10 s until the other one stops. The flock
  ([ADR-0003](0003-single-writer-sqlite.md)) keeps the database safe, but nothing
  else announces the conflict. `docs/04-usage.md` gives the migration order.

## How it was tested

On 2026-11-25, against the v0.3.0 release.

- **Linux, end to end through Homebrew.** This ran in an Ubuntu 24.04 container with
  systemd as PID 1 and Homebrew 7.0.16. The formula was installed from a local tap
  with `brew install`, and the service was started with `brew services start`. The
  container was arm64, because systemd in an amd64 container under Docker Desktop's
  emulation failed every unit with `255/EXCEPTION`. The tap ships no Linux arm64
  tarball, so the test formula added one `on_arm` url, a local v0.3.0 build. Its
  service block is the shipped one. Measured:
  - The unit matched the generated one above, and `mailbox status` reached the
    daemon.
  - `SIGKILL` → restarted 10 s later (`NRestarts=1`).
  - `SIGTERM` → `inactive`, `Result=success`, not restarted.
  - Reboot (`docker restart`) with lingering → running.
  - Reboot without lingering → not running until a login. Then it ran, and it
    stopped some seconds after the last session ended.
  - With lingering, a login and logout left it running.
  - `brew upgrade` (a re-versioned formula) → the old daemon was not restarted, as
    above. A stub watch started then ran the new version's adapter.
    `brew services restart` moved the daemon to the new Cellar path.
- **macOS, launchd without Homebrew's install step.** The plist came from Homebrew's
  own `Service#to_plist` for the rendered formula, and passed `plutil -lint`. A copy
  ran under a throwaway label, pointed at the v0.3.0 macOS binary with
  `AGENT_MAILBOX_HOME` in a temp directory. `mailbox status` reached it, and a
  `SIGKILL` was followed by a new pid 9 s later. The formula was not
  `brew install`ed on the test Mac, because that would link a second `mailbox`
  onto a `PATH` other agents share.
- **The Keychain** was not tested through the brew service. The evidence is the
  maintainer's hand-written LaunchAgent, in the same `gui/<uid>` domain
  `brew services` uses. It runs Slack bot and thread watches with no Keychain
  errors in its log. Over SSH, `brew services` loads into `user/<uid>` instead,
  and whether the Keychain is readable there is unmeasured.

## Alternatives considered

- **Start the service at install time.** Not possible (above). Even if it were,
  silently starting a second daemon on a machine that already has one would put it
  straight into the lock loop.
- **Ship our own plist and unit, installed by a `mailbox` subcommand.** This
  duplicates what Homebrew generates, and puts a second registration mechanism
  beside `brew services` that the user would have to keep in step with upgrades.
  A source install can still copy the plist shape from `docs/04-usage.md`.
- **Log to `~/.agent-mailbox/daemon.log`**, beside the database, as the
  hand-written LaunchAgent did. Rejected because a path fixed in the formula cannot
  follow an `AGENT_MAILBOX_HOME` override, so the log would sit apart from a
  relocated database anyway, while `var/log` is where Homebrew puts every other
  service's log.
- **`keep_alive crashed: true`.** On macOS it would not restart a daemon that exited
  non-zero without crashing, which is how a lock refusal or a bad database exits.
