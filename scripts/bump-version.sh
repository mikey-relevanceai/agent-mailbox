#!/usr/bin/env bash
#
# bump-version.sh — bump the workspace version in one place.
#
# The whole workspace shares a single version: `[workspace.package] version` in
# the root Cargo.toml, which every crate inherits via `version.workspace = true`.
# That number is what `mailbox --version` prints (alongside the build's git hash,
# baked in by crates/mailbox/build.rs). This script edits that one field, keeps
# Cargo.lock in sync, and — with --tag — commits and tags the release.
#
# Usage:
#   scripts/bump-version.sh patch            # 0.1.0 -> 0.1.1
#   scripts/bump-version.sh minor            # 0.1.3 -> 0.2.0
#   scripts/bump-version.sh major            # 0.2.5 -> 1.0.0
#   scripts/bump-version.sh 1.4.0            # set an explicit version
#   scripts/bump-version.sh minor --tag      # also: commit + annotated tag vX.Y.Z
#
# Without --tag it only edits the files and prints the git commands to run next,
# so you can review the diff first.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
MANIFEST="${REPO_DIR}/Cargo.toml"

die() { echo "error: $*" >&2; exit 1; }

[[ $# -ge 1 ]] || die "usage: bump-version.sh <patch|minor|major|X.Y.Z> [--tag]"
BUMP="$1"; shift
TAG=0
for arg in "$@"; do
  case "${arg}" in
    --tag) TAG=1 ;;
    *) die "unknown option: ${arg}" ;;
  esac
done

# --- read the current version from [workspace.package] -------------------------
# Only the version line *inside* the [workspace.package] table — not the ones in
# [workspace.dependencies] — so we track sections as we scan.
current="$(awk '
  /^\[/            { in_pkg = ($0 == "[workspace.package]") }
  in_pkg && /^version[[:space:]]*=/ {
    match($0, /"[^"]+"/); print substr($0, RSTART+1, RLENGTH-2); exit
  }
' "${MANIFEST}")"
[[ -n "${current}" ]] || die "could not find [workspace.package] version in ${MANIFEST}"
[[ "${current}" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || die "current version is not X.Y.Z: ${current}"

IFS='.' read -r major minor patch <<<"${current}"

# --- compute the new version ---------------------------------------------------
case "${BUMP}" in
  major) new="$((major + 1)).0.0" ;;
  minor) new="${major}.$((minor + 1)).0" ;;
  patch) new="${major}.${minor}.$((patch + 1))" ;;
  *)
    [[ "${BUMP}" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] \
      || die "not a bump keyword or X.Y.Z version: ${BUMP}"
    new="${BUMP}"
    ;;
esac

[[ "${new}" != "${current}" ]] || die "new version equals current (${current}); nothing to do"

# --- write it back (only the [workspace.package] version line) ------------------
tmp="$(mktemp)"
awk -v new="${new}" '
  /^\[/            { in_pkg = ($0 == "[workspace.package]") }
  in_pkg && /^version[[:space:]]*=/ && !done {
    sub(/"[^"]+"/, "\"" new "\""); done = 1
  }
  { print }
' "${MANIFEST}" >"${tmp}"
mv "${tmp}" "${MANIFEST}"

# --- keep Cargo.lock in sync (workspace members only) --------------------------
( cd "${REPO_DIR}" && cargo update --workspace --quiet )

echo "bumped ${current} -> ${new}"

if [[ "${TAG}" -eq 1 ]]; then
  ( cd "${REPO_DIR}" \
      && git add Cargo.toml Cargo.lock \
      && git commit -m "Release v${new}" -q \
      && git tag -a "v${new}" -m "v${new}" )
  echo "committed and tagged v${new} (push with: git push && git push origin v${new})"
else
  echo "review the change, then:"
  echo "    git add Cargo.toml Cargo.lock && git commit -m \"Release v${new}\""
  echo "    git tag -a v${new} -m v${new}"
  echo "(or re-run with --tag to do both automatically)"
fi
