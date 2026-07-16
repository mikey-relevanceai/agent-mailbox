# Release & distribution

How `mailbox` is versioned and shipped, and the constraints that shape those
choices. Read this before changing versioning, the `--version` output, or the
install flow.

## Versioning

The whole workspace shares one version — `[workspace.package] version` in the root
`Cargo.toml`, inherited by every crate via `version.workspace = true`. Bump it with
`scripts/bump-version.sh <patch|minor|major|X.Y.Z> [--tag]` (see
[04-usage](04-usage.md#bump-the-version)).

`mailbox --version` prints the crate version plus the commit it was built from
(baked in by `crates/mailbox/build.rs`), e.g. `mailbox 0.1.0 (git 6b5e56b, 2026-07-14)`,
with `-dirty` for an uncommitted tree. **Keep the bare semver as an unadorned
leading token** — a Homebrew formula `test do` block asserts the semver appears in
`--version`, so reformatting it (e.g. `v0.2.0-<hash>`) would make that test brittle.
The git-hash suffix is fine; it's a substring match.

## Distribution: a self-owned Homebrew tap

The route is our own tap at `mikey-relevanceai/homebrew-tap`, giving users
`brew install mikey-relevanceai/tap/mailbox`. It needs no approval — only a real
git tag. homebrew/core is out of reach (self-submitted projects need ≥90 forks /
≥90 watchers / ≥225 stars).

Hard constraints this imposes — **do not violate without revisiting the route**:

1. **Ship an annotated `vX.Y.Z` git tag** on a versioned, checksummed tarball, never
   a branch. `bump-version.sh --tag` produces exactly this (`git tag -a vX.Y.Z`), and
   `cargo-dist` (the likely release automation) keys its workflow off tags in that
   shape.
2. **Never add a self-update command** (`mailbox self-update` etc.). Homebrew rejects
   software that upgrades itself — it conflicts with `brew upgrade`.
3. **Keep the `--version` format** as above.

**Binary name stays `mailbox`** (decided 2026-07-14). It is generic and risks a
collision if ever pushed to homebrew/core, but the tap install path is unambiguous
and homebrew/core is out of reach anyway. Do not rename unless homebrew/core
becomes realistic — it is cheaper before the first tag than after.

## macOS: `Killed: 9` after a local reinstall

On Apple Silicon a locally-built (ad-hoc-signed, un-notarized) binary can be
SIGKILLed on launch — `zsh: killed  mailbox --version`, exit 137 — after you
reinstall it over the same path. It is an intermittent code-signature-cache issue on
in-place replacement, not a bug in the binary: the file is byte-identical and
`codesign -v` reports it valid, yet the kernel kills it until the signature is
refreshed.

**Best: use the install helper**, which does the safe thing every time (removes the
old file for a fresh inode, then re-signs ad-hoc), so this never recurs:

```bash
scripts/install-local.sh              # build --release + install to ~/.local/bin, safely
SKIP_BUILD=1 scripts/install-local.sh # install an already-built target/release
DEST=/usr/local/bin scripts/install-local.sh
```

If you already hit it with a hand-installed binary, either fix works on its own:

```bash
codesign --force -s - ~/.local/bin/mailbox                # re-sign in place
rm -f ~/.local/bin/mailbox && cp target/release/mailbox ~/.local/bin/mailbox  # fresh inode
```

Once we ship through the Homebrew tap, releases are properly signed and this does
not affect end users — it is only a local dev-install wrinkle.
