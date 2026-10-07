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
leading token** — the Homebrew formula's `test do` block asserts the semver appears
in `--version`, so reformatting it (e.g. `v0.2.0-<hash>`) would make that test
brittle. The git-hash suffix is fine; it's a substring match. The release workflow
asserts the same thing before it publishes anything, so a break shows up in CI
rather than in a user's `brew install`.

## Distribution: a self-owned Homebrew tap

The route is our own tap at `mikey-relevanceai/homebrew-tap`, giving users
`brew install mikey-relevanceai/tap/mailbox`. It needs no approval — only a real
git tag. homebrew/core is out of reach (self-submitted projects need ≥90 forks /
≥90 watchers / ≥225 stars).

Hard constraints this imposes — **do not violate without revisiting the route**:

1. **Ship an annotated `vX.Y.Z` git tag** on a versioned, checksummed tarball, never
   a branch. `bump-version.sh --tag` produces exactly this (`git tag -a vX.Y.Z`), and
   `.github/workflows/release.yml` triggers on tags in that shape.
2. **Never add a self-update command** (`mailbox self-update` etc.). Homebrew rejects
   software that upgrades itself — it conflicts with `brew upgrade`.
3. **Keep the `--version` format** as above.

**Binary name stays `mailbox`** (decided 2026-07-14). It is generic and risks a
collision if ever pushed to homebrew/core, but the tap install path is unambiguous
and homebrew/core is out of reach anyway. Do not rename unless homebrew/core
becomes realistic — it is cheaper before the first tag than after.

### Supported platforms

Prebuilt tarballs cover **macOS arm64, macOS x86_64, and Linux x86_64**. Windows is
excluded by construction, not by policy: Unix domain sockets, `flock` and `nix`
run through the whole bridge. Linux arm64 has no bottle yet — those users build from
source. Both macOS slices are built on the arm64 runner, the Intel one as a
cross-compile, so the matrix does not depend on Actions keeping an Intel macOS image
alive.

### Why a hand-rolled workflow and not cargo-dist

cargo-dist is the obvious tool here and was the original plan. It does not fit,
for one structural reason: **it maps one "App" to one Cargo package.** Our four
shipped binaries live in four packages (`crates/mailbox`, `adapters/stub-adapter`,
`adapters/github-pr-adapter`, `adapters/slack-adapter`), so cargo-dist would emit four
tarballs and four formulae, and a user would have to know to `brew install` all four to
get a working watch. There is no supported way to merge packages into one artifact. It would also
ship `crates/mailbox`'s `test_adapter` fixture, which is a second bin target in the
same package and therefore inseparable from `mailbox` in its model.

Co-location is not a preference we could give up to make the tool fit — the bridge
resolves an adapter by looking beside its own executable
([04-usage](04-usage.md#from-source)), so one tarball holding all of them is the
artifact this project actually needs.

Reconsider cargo-dist if the binaries ever collapse into one package.

## Cutting a release

```bash
scripts/bump-version.sh minor --tag   # edits Cargo.toml + lock, commits, tags vX.Y.Z
git push && git push origin vX.Y.Z
```

Pushing the tag is the whole trigger. `.github/workflows/release.yml` then:

1. **builds** each target, checks the tag matches `Cargo.toml`, checks each binary's
   architecture actually matches its target name, and smoke-tests `--version` on the
   two natively-built ones;
2. **publishes** a GitHub Release with the three tarballs and a combined
   `SHA256SUMS`, with notes generated from the commit log;
3. **updates the tap** — renders `Formula/mailbox.rb` from that same `SHA256SUMS`
   and pushes it to `mikey-relevanceai/homebrew-tap`.

The formula and the release read their checksums from one file, so they cannot
disagree about what was shipped. That file covers all three platforms, so verifying
a single download needs `shasum -a 256 -c --ignore-missing SHA256SUMS` — without
`--ignore-missing` the two tarballs you did not download are reported as failures.

**If the `release` job fails partway**, `gh release create` is not idempotent: a
re-run fails with "release already exists". Delete the release (the tag can stay)
and re-run, or cut the next patch tag. The `tap` job has no such problem — it reads
the checksums back off the published release, so it can be re-run freely.

To see the formula without cutting a release:

```bash
mkdir -p Formula
scripts/render-formula.sh 0.2.0 SHA256SUMS >Formula/mailbox.rb   # prints to stdout
brew style Formula/mailbox.rb                                    # Homebrew's own lint
```

The renderer has its own suite in `scripts/test-render-formula.sh` — a pure transform,
so every refusal is reachable from a fixture. `cargo test` runs it (via
`crates/mailbox/tests/release_tooling.rs`), so it is covered by the same command as
everything else; run the script directly when you want its per-case output.

Lint it from a path ending in `Formula/`. `brew style` switches rule sets on the
directory name, and on a bare `mailbox.rb` it falls back to generic Ruby rules and
reports three offences (Sorbet sigils, a frozen-string-literal comment) that no
formula ever satisfies.

### The one piece of manual setup

The tap lives in a different repository, and `GITHUB_TOKEN` is scoped to this one.
Pushing the formula needs a **fine-grained PAT with `Contents: read and write` on
`mikey-relevanceai/homebrew-tap`**, stored here as the repository secret
`HOMEBREW_TAP_TOKEN`:

```bash
gh secret set HOMEBREW_TAP_TOKEN --repo mikey-relevanceai/agent-mailbox
```

The `tap` job checks for it and fails with that instruction rather than a bare 404.
It runs after the release is published, so a missing secret costs you a re-run of one
job, never a half-published release.

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

**Homebrew users do not hit this**, but not because the binaries are notarized — they
are not, they carry the same ad-hoc signature the linker applies. They avoid it
because `brew upgrade` never replaces a binary in place: it unpacks the new version
into a fresh, versioned Cellar directory and re-points a symlink, so every install
lands on a new inode. That is the same property `install-local.sh` reproduces by
hand.

Notarizing would need a paid Apple Developer account and a signing identity in CI.
Not done, and not needed for a `brew`-delivered formula — revisit only if we ever
ship a `.dmg` or a direct download users fetch with a browser, since it is the
browser's quarantine attribute, not the signature alone, that triggers Gatekeeper.
