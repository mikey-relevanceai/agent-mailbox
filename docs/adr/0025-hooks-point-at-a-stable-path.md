# ADR-0025: The hook path is absolute but never canonical

- Status: **Accepted**
- Date: 2026-08-14
- Amends: [ADR-0013](0013-re-register-inbox-and-watchpaths-on-resume.md) and
  [ADR-0021](0021-delete-the-sentinel-fallback.md) — neither changes, but both rest
  on a `SessionStart` hook that still resolves to a binary, which is what this ADR
  is about.

## Context

`mailbox harness install-hooks` writes an absolute path to the `mailbox` binary into
`~/.claude/settings.json`, because a hook fires with a `PATH` we do not control:

> Absolute path to the `mailbox` binary the hooks invoke. Absolute so the hook works
> regardless of the session's `PATH`. — `HookInstallSpec::mailbox_bin`

Absolute was the requirement. **Canonical** is what the code did: `abs_bin` called
`std::fs::canonicalize`, which resolves every symlink in the path. Until now that
distinction was invisible, because every documented install put the real binary at
the path the user typed — `~/.local/bin/mailbox` via `scripts/install-local.sh`, or
a `target/release` tree.

Shipping through a Homebrew tap makes the distinction load-bearing. Homebrew installs
into a **versioned Cellar** directory and exposes it through two stable indirections:

```text
/opt/homebrew/bin/mailbox           -> ../Cellar/mailbox/0.1.0/bin/mailbox   (symlink)
/opt/homebrew/opt/mailbox           -> ../Cellar/mailbox/0.1.0               (symlink)
/opt/homebrew/Cellar/mailbox/0.1.0/ ← deleted by the next `brew upgrade`
```

`brew upgrade` deletes the Cellar directory and re-points the links. So the two
stable paths keep working forever and the canonical one is destroyed on every
upgrade. Canonicalising picks exactly the wrong one.

The resulting failure is the silent-and-partial kind this repo treats as the cardinal
sin, and it is already documented for the `cargo clean` case at `README.md`:

> The absolute path of whatever binary you ran is baked into `settings.json`. Point
> it at a build tree and the `SessionStart` hook dies after the next `cargo clean` —
> your inbox stops being registered, so peers cannot address you, **while topic wakes
> keep working**.

An upgrade would reproduce that in a fleet that had done nothing wrong. Nothing
errors: `brew upgrade mailbox` reports success, `mailbox --version` prints the new
version, topic wakes keep landing, and the only broken thing is the half nobody
checks — `mailbox send` to that session, from a peer, silently has no one to address.

`std::env::current_exe()` deserves a note, because it is the *other* source of this
path and it is not uniformly fixable. On macOS it returns the path the binary was
invoked through, so a default `install-hooks` under Homebrew already records
`/opt/homebrew/bin/mailbox` and is safe. On Linux it reads `/proc/self/exe`, which
the kernel resolves — so the default there records the Cellar path no matter what we
do at our end. That asymmetry is why the fix has to be reachable through an explicit
flag rather than through the default alone.

## Decision

**`abs_bin` anchors a path; it does not resolve one.** It uses
`std::path::absolute`, which makes a relative path absolute against the working
directory without following symlinks.

The documented Homebrew install therefore passes the stable link explicitly, and the
formula's `caveats` prints exactly this:

```bash
mailbox harness install-hooks --mailbox-bin "$(brew --prefix)/opt/mailbox/bin/mailbox"
```

`opt/mailbox/bin` (Homebrew's `opt_bin`) is preferred over `bin/` because it is the
prefix Homebrew documents as stable-by-contract for exactly this purpose, and it is
per-formula, so it stays correct if the plain `bin` link is ever shadowed by another
package.

This does not change what `--mailbox-bin` means, and it does not change the
`PATH`-independence requirement that motivated an absolute path in the first place.
It changes only *which* absolute path a symlinked argument produces.

## Consequences

- A Homebrew install survives `brew upgrade` with no re-run of `install-hooks`, on
  both macOS and Linux, provided the documented `--mailbox-bin` was used.
- Re-running `install-hooks` after an upgrade remains good advice and remains
  idempotent — hooks are matched by shape, not by command string, so a re-run with a
  different binary path replaces ours rather than appending. That safety net is
  unchanged; it is simply no longer load-bearing.
- Two unit tests now pin this: one asserts a symlinked path survives `abs_bin`, one
  asserts a relative path is still anchored. The first is a genuine guard — with
  `canonicalize` restored it fails, and it fails with the Cellar path in the message.
  The assumption cannot be expressed in the type system (both functions return an
  absolute `PathBuf`), which is precisely why it is a test.
- `abs_bin` no longer touches the filesystem, so it no longer silently falls back to
  the raw input when the path does not exist yet. Behaviourally this is a small
  improvement — a typo'd `--mailbox-bin` is now recorded as the absolute form of the
  typo rather than the relative form, which is easier to read in `settings.json` —
  but neither form is validated, and `install-hooks` still does not check that the
  binary exists. Unchanged, and out of scope.
- We inherit Homebrew's guarantee about `opt_bin`. If that layout ever changes, the
  caveats and `docs/05-release.md` change with it; the code does not.
- The Linux default path (`current_exe()` → `/proc/self/exe` → Cellar) is **not**
  fixed by this ADR and cannot be fixed at our end. It is handled by documentation:
  the formula's caveats give the flag, and `README.md`'s gotcha table names the
  failure. An installer that ignores the caveats gets the pre-existing behaviour.

## Alternatives considered

- **Leave `canonicalize` and tell users to re-run `install-hooks` after every
  upgrade.** The formula's `caveats` would carry it. Rejected because it makes a
  silent, partial failure the default outcome of a routine `brew upgrade` and defends
  against it with a message users read once, at install time, and not on the upgrade
  where it matters. ADR-0021 exists because a fallback that fails silently is worse
  than no fallback.
- **Have the formula run `install-hooks` itself** in `post_install`. Removes the
  manual step entirely. Rejected twice over: it writes to `~/.claude/settings.json`,
  outside the Homebrew prefix, which `brew audit` flags and which no package manager
  should do; and it contradicts `AGENTS.md`'s rule that we never edit a user's Claude
  settings implicitly. The same rule already forbids `install-inbound` being implicit,
  and hooks are the same family of edit.
- **Install a wrapper script at `bin/mailbox` that `exec`s the Cellar binary.**
  A common Homebrew idiom, and actively harmful here: adapter discovery resolves
  siblings from `current_exe().parent()`, so a wrapper would make `mailbox` look like
  it lives wherever the wrapper is and break co-location for
  `mailbox-stub-adapter` / `mailbox-github-pr-adapter`. `PATH` lookup would mask the
  breakage in the normal case, which makes it worse, not better.
- **Resolve the path at hook-fire time instead of install time** — write
  `mailbox harness session-start` and let `PATH` find it. This is what the absolute
  path was chosen to avoid: a `SessionStart` hook runs with an environment Claude Code
  composes, and a login-shell `PATH` is not guaranteed. Reversing that is a bigger
  decision than this one and would need its own ADR.
- **Canonicalize, then walk back up to a stable prefix** (detect a Cellar path and
  rewrite it to `opt`). Teaches the binary about one package manager's directory
  layout so it can undo a resolution it chose to perform. Not resolving in the first
  place is the same outcome with none of the knowledge.
