#!/usr/bin/env bash
#
# install-local.sh — build (optionally) and install the mailbox binaries into a
# local bin dir, the SAFE way for local dev on macOS.
#
# Why this exists: on Apple Silicon, replacing a code-signed binary IN PLACE
# (a plain `cp`/`install` over the same path) can leave the kernel's
# code-signature cache stale for that inode, so the new binary is SIGKILLed on
# launch — `zsh: killed  mailbox`, exit 137 — even though it is byte-identical
# and validly signed. See docs/05-release.md. This script sidesteps it two ways:
# it removes the old file first (fresh inode) AND re-signs ad-hoc after copying.
#
# Usage:
#   scripts/install-local.sh                 # build --release, install to ~/.local/bin
#   DEST=/usr/local/bin scripts/install-local.sh
#   SKIP_BUILD=1 scripts/install-local.sh    # install already-built target/release
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
DEST="${DEST:-${HOME}/.local/bin}"
BINS=(mailbox mailbox-stub-adapter mailbox-github-pr-adapter mailbox-slack-adapter)

if [[ "${SKIP_BUILD:-0}" != "1" ]]; then
  echo "==> cargo build --release (set SKIP_BUILD=1 to skip)"
  ( cd "${REPO_DIR}" && cargo build --release )
fi

REL="${REPO_DIR}/target/release"
for b in "${BINS[@]}"; do
  [[ -x "${REL}/${b}" ]] || { echo "error: ${REL}/${b} not built" >&2; exit 1; }
done

mkdir -p "${DEST}"
for b in "${BINS[@]}"; do
  # Fresh inode: remove the old file before copying so the kernel never reuses a
  # cached signature for the replaced inode.
  rm -f "${DEST}/${b}"
  cp "${REL}/${b}" "${DEST}/${b}"
  chmod 755 "${DEST}/${b}"
  # Belt and suspenders: re-sign ad-hoc so the on-disk signature is fresh
  # regardless of how the copy landed. No-op / harmless on non-macOS.
  if command -v codesign >/dev/null 2>&1; then
    codesign --force -s - "${DEST}/${b}" >/dev/null 2>&1 || true
  fi
done

echo "==> installed to ${DEST}:"
"${DEST}/mailbox" --version
echo ""
echo "Next: re-run 'mailbox harness install-hooks' if you changed the hook wiring,"
echo "and restart 'mailbox serve' to pick up the new daemon."
