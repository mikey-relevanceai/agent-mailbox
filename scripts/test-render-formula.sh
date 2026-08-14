#!/usr/bin/env bash
#
# test-render-formula.sh — tests for render-formula.sh.
#
# render-formula.sh is a pure transform (version + SHA256SUMS in, formula text
# out) with no network and no git, so every one of its refusals is reachable from
# a fixture. Without this, the first time a `die` path runs is during a real
# release — on a tag that is already public and immutable.
#
# Usage: scripts/test-render-formula.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RENDER="${SCRIPT_DIR}/render-formula.sh"
WORK="$(mktemp -d)"
trap 'rm -rf "${WORK}"' EXIT

PASS=0
FAIL=0

ok()   { PASS=$((PASS + 1)); echo "  ok   — $1"; }
bad()  { FAIL=$((FAIL + 1)); echo "  FAIL — $1"; }
check() { if [[ "$1" == "$2" ]]; then ok "$3"; else bad "$3 (got '$1', want '$2')"; fi; }

VERSION="1.2.3"
SUMS="${WORK}/SHA256SUMS"
sum_line() { printf '%s  mailbox-%s-%s.tar.gz\n' "$2" "${VERSION}" "$1"; }
{
  sum_line aarch64-apple-darwin      "$(printf 'a%.0s' {1..64})"
  sum_line x86_64-apple-darwin       "$(printf 'b%.0s' {1..64})"
  sum_line x86_64-unknown-linux-gnu  "$(printf 'c%.0s' {1..64})"
} >"${SUMS}"

echo "render-formula.sh"

# --- it renders every platform, with the right checksum against the right url ---
out="${WORK}/formula.rb"
"${RENDER}" "${VERSION}" "${SUMS}" >"${out}"

check "$(grep -c '^      url ' "${out}")" "3" "renders one url per shipped platform"
check "$(grep -c '^      sha256 ' "${out}")" "3" "renders one sha256 per shipped platform"
check "$(grep -c "version \"${VERSION}\"" "${out}")" "1" "renders the version"
check "$(grep -c 'github.com/mikey-relevanceai/agent-mailbox/releases' "${out}")" "3" \
  "defaults to this project's releases"

# The pairing is the part worth pinning: a formula that renders three correct
# checksums against three swapped urls still passes a naive count check, and
# fails only on a user's machine with a checksum mismatch.
for pair in "aarch64-apple-darwin:a" "x86_64-apple-darwin:b" "x86_64-unknown-linux-gnu:c"; do
  target="${pair%%:*}"; letter="${pair##*:}"
  line="$(grep -A1 "mailbox-${VERSION}-${target}.tar.gz" "${out}" | tail -1)"
  expected="$(printf "${letter}%.0s" {1..64})"
  check "${line}" "      sha256 \"${expected}\"" "${target} url is followed by its own checksum"
done

# --- it is valid Ruby -------------------------------------------------------
if command -v ruby >/dev/null 2>&1; then
  if ruby -c "${out}" >/dev/null 2>&1; then ok "output parses as Ruby"; else bad "output parses as Ruby"; fi
else
  echo "  skip — output parses as Ruby (no ruby on PATH)"
fi

# --- the repo the urls point at is overridable ------------------------------
MAILBOX_RELEASE_REPO="someone/elsewhere" "${RENDER}" "${VERSION}" "${SUMS}" >"${WORK}/forked.rb"
check "$(grep -c 'github.com/someone/elsewhere/releases' "${WORK}/forked.rb")" "3" \
  "MAILBOX_RELEASE_REPO redirects every url"

# --- input shapes that are valid but not what shasum happens to emit ---------
# A sums file is external input to this script, and the script is documented as
# runnable by hand. Two forms it must not choke on:

# CRLF. Every filename would carry an invisible \r, so nothing matches and the
# script blames a missing platform rather than the line endings.
sed 's/$/\r/' "${SUMS}" >"${WORK}/crlf"
if "${RENDER}" "${VERSION}" "${WORK}/crlf" >"${WORK}/crlf.rb" 2>/dev/null; then
  check "$(grep -c '^      sha256 ' "${WORK}/crlf.rb")" "3" "a CRLF sums file renders every platform"
else
  bad "a CRLF sums file renders every platform (refused a valid file)"
fi

# GNU coreutils binary mode writes "<hash> *<file>". Both the lookup and the
# drift guard claim to handle it; nothing proved that until now.
sed 's/  mailbox-/  *mailbox-/' "${SUMS}" >"${WORK}/binary-mode"
"${RENDER}" "${VERSION}" "${WORK}/binary-mode" >"${WORK}/binary.rb" 2>/dev/null || true
check "$(grep -c '^      sha256 ' "${WORK}/binary.rb" 2>/dev/null || echo 0)" "3" \
  "a binary-mode (*file) sums file renders every platform"

# --- every refusal, and each one writes NOTHING -----------------------------
# A `die` that still emits a partial formula is the dangerous shape: the caller
# redirects stdout to Formula/mailbox.rb, so bytes written before the refusal
# become a published formula.
refuses() {
  local description="$1"; shift
  local stdout="${WORK}/refused.rb"
  if "$@" >"${stdout}" 2>/dev/null; then
    bad "${description} (exited 0)"
  elif [[ -s "${stdout}" ]]; then
    bad "${description} (refused but wrote $(wc -c <"${stdout}" | tr -d ' ') bytes)"
  else
    ok "${description}"
  fi
}

refuses "refuses a non-semver version"            "${RENDER}" "1.2" "${SUMS}"

# A repo with no `/` would leave OWNER and REPO as the same string, rendering
# plausible URLs that 404 rather than failing.
if MAILBOX_RELEASE_REPO="justaname" "${RENDER}" "${VERSION}" "${SUMS}" >/dev/null 2>&1; then
  bad "refuses a MAILBOX_RELEASE_REPO with no owner"
else
  ok "refuses a MAILBOX_RELEASE_REPO with no owner"
fi

refuses "refuses a version with a v prefix"       "${RENDER}" "v${VERSION}" "${SUMS}"
refuses "refuses an unreadable sums file"         "${RENDER}" "${VERSION}" "${WORK}/absent"
refuses "refuses the wrong argument count"        "${RENDER}" "${VERSION}"

grep -v aarch64 "${SUMS}" >"${WORK}/missing-platform"
refuses "refuses a sums file missing a platform"  "${RENDER}" "${VERSION}" "${WORK}/missing-platform"

# Corrupt exactly ONE line, so this pins "one bad checksum among good ones".
# `s/^a*/` would match a zero-width string on every other line too, corrupting all
# three and passing even if only the first line's check worked.
sed '1s/^a\{64\}/notasha/' "${SUMS}" >"${WORK}/malformed-sha"
refuses "refuses a malformed checksum"            "${RENDER}" "${VERSION}" "${WORK}/malformed-sha"

# The drift guard: a platform the build matrix ships but the formula has no url
# block for must stop the render, not vanish from the formula.
{ cat "${SUMS}"; sum_line aarch64-unknown-linux-gnu "$(printf 'd%.0s' {1..64})"; } >"${WORK}/extra-platform"
refuses "refuses a platform it has no url block for" "${RENDER}" "${VERSION}" "${WORK}/extra-platform"

# A sums file for a DIFFERENT version must not be silently accepted.
sed "s/${VERSION}/9.9.9/" "${SUMS}" >"${WORK}/other-version"
refuses "refuses a sums file for another version"  "${RENDER}" "${VERSION}" "${WORK}/other-version"

echo ""
echo "${PASS} passed, ${FAIL} failed"
[[ "${FAIL}" -eq 0 ]]
