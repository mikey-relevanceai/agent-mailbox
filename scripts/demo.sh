#!/usr/bin/env bash
#
# demo.sh — a self-contained, no-network walkthrough of the agent-mailbox wake
# loop. It proves, on your machine with no GitHub and no Claude Code, that:
#
#   1. the four-verb agent loop works: subscribe -> (publish) -> read;
#   2. an *idle* waiter wakes when mail lands (the asyncRewake contract:
#      `mailbox wait` exits 2 with a payload-free "mail on topic X" reminder);
#   3. a bridge-supervised adapter (the reference `stub` poller) publishes edges
#      on its own and wakes the same way — no agent-owned background poller;
#   4. `unwatch` / end-session tears the adapter down (no zombie pollers).
#
# Everything runs in a throwaway tempdir against a private `mailbox serve`
# daemon, so it never touches your real ~/.agent-mailbox or ~/.claude.
#
# The REAL github-pr walkthrough (watch an actual PR, push a conflict, watch an
# idle Claude Code session wake) is a human step — see docs/demo.md § "Real PR".
#
# Usage:  scripts/demo.sh            # builds if needed, then runs
#         SKIP_BUILD=1 scripts/demo.sh
set -euo pipefail

# --- locate the repo + binaries -----------------------------------------------
# Resolve relative to this script so the demo runs from any cwd.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_DIR}"

if [[ "${SKIP_BUILD:-0}" != "1" ]]; then
  echo "==> building release binaries (set SKIP_BUILD=1 to skip)"
  cargo build --release --quiet
fi

BIN_DIR="${REPO_DIR}/target/release"
MAILBOX="${BIN_DIR}/mailbox"
STUB_ADAPTER="${BIN_DIR}/mailbox-stub-adapter"
for bin in "${MAILBOX}" "${STUB_ADAPTER}"; do
  if [[ ! -x "${bin}" ]]; then
    echo "error: ${bin} not found — run 'cargo build --release' first" >&2
    exit 1
  fi
done

# --- private, throwaway environment -------------------------------------------
WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/mailbox-demo.XXXXXX")"
export AGENT_MAILBOX_DB="${WORK_DIR}/mailbox.db"
# Point the supervisor's stub resolver at the binary we just built (the normal
# install co-locates it beside `mailbox`, so this override is a dev convenience).
export MAILBOX_STUB_ADAPTER_BIN="${STUB_ADAPTER}"
# Ask `mailbox wait` to append its wake reason to stderr, so the demo can show
# *why* it woke. This is a diagnostic-only opt-in; the reminder itself stays
# payload-free.
export MAILBOX_WAIT_DEBUG=1
export RUST_LOG="${RUST_LOG:-error}"

SESSION="demo-session"
SERVE_PID=""

cleanup() {
  # Best-effort teardown so a re-run starts clean and no daemon is left behind.
  # A plain SIGTERM (not SIGKILL) is deliberate: it runs the daemon's graceful
  # shutdown, which tears down every supervised adapter's process group — so if
  # the demo errors out after `watch stub` but before `unwatch`, the stub adapter
  # child is still reaped with the daemon rather than orphaned.
  if [[ -n "${SERVE_PID}" ]] && kill -0 "${SERVE_PID}" 2>/dev/null; then
    kill "${SERVE_PID}" 2>/dev/null || true
    wait "${SERVE_PID}" 2>/dev/null || true
  fi
  rm -rf "${WORK_DIR}"
}
trap cleanup EXIT

step() { printf '\n\033[1;34m== %s ==\033[0m\n' "$*"; }
run()  { printf '\033[2m$ %s\033[0m\n' "$*"; eval "$*"; }

echo "demo workdir: ${WORK_DIR}"

# --- 0. start the bridge daemon -----------------------------------------------
step "0. start the bridge daemon (mailbox serve)"
"${MAILBOX}" serve &
SERVE_PID=$!
# Wait for the socket to appear so clients don't race the daemon's bind.
SOCK="${WORK_DIR}/mailbox.sock"
for _ in $(seq 1 100); do
  [[ -S "${SOCK}" ]] && break
  sleep 0.05
done
if [[ ! -S "${SOCK}" ]]; then
  echo "error: daemon socket never appeared at ${SOCK}" >&2
  exit 1
fi
echo "daemon up (pid ${SERVE_PID}), socket at ${SOCK}"

# --- 1. the four-verb loop, by hand -------------------------------------------
# subscribe -> publish (stands in for an adapter) -> read.
step "1. four-verb core: subscribe, publish, read"
run "${MAILBOX} subscribe demo.hello --session ${SESSION}"
run "${MAILBOX} publish demo.hello --body '{\"msg\":\"first\"}'"
run "${MAILBOX} read --session ${SESSION}"

# --- 2. wake an IDLE waiter ----------------------------------------------------
# This is the load-bearing mechanic: start `mailbox wait` with NO mail pending,
# so it blocks (exactly what the SessionStart/Stop hook does in a real session),
# then publish and watch it exit 2 with a payload-free reminder.
step "2. wake an idle waiter (the asyncRewake contract)"
echo "starting a blocking waiter (no mail yet) ..."
set +e
"${MAILBOX}" wait --session "${SESSION}" --max-block-ms 10000 >"${WORK_DIR}/wait.out" 2>"${WORK_DIR}/wait.err" &
WAIT_PID=$!
set -e
sleep 0.5   # let the waiter reach its blocking read
echo "publishing while the waiter is idle ..."
run "${MAILBOX} publish demo.hello --body '{\"msg\":\"wake up\"}'"
set +e
wait "${WAIT_PID}"; WAIT_RC=$?
set -e
echo "waiter exit code: ${WAIT_RC}   (2 = woken; the harness turns this into a wake)"
echo "waiter reminder (stderr, payload-free):"
sed 's/^/    /' "${WORK_DIR}/wait.err"
if [[ "${WAIT_RC}" -ne 2 ]]; then
  echo "error: expected the waiter to wake with exit 2, got ${WAIT_RC}" >&2
  exit 1
fi
run "${MAILBOX} read --session ${SESSION}"

# --- 3. a bridge-SUPERVISED adapter (no agent-owned poller) -------------------
# `watch stub` records interest, subscribes the session, and the daemon spawns
# the stub adapter, which publishes a synthetic edge every --interval-ms. The
# agent NEVER launches this loop itself.
step "3. supervised adapter: watch stub, see it running, read its edges"
run "${MAILBOX} watch stub demo --interval-ms 500 --session ${SESSION}"
sleep 1.2   # let the supervisor spawn the adapter and it publish a couple edges
run "${MAILBOX} status --session ${SESSION}"
run "${MAILBOX} read --session ${SESSION} --limit 5"

# --- 4. teardown: no zombie pollers -------------------------------------------
# Dropping the last interest stops the adapter. `status` shows no running child.
step "4. unwatch -> the supervisor stops the adapter (no zombie poller)"
run "${MAILBOX} unwatch stub demo --session ${SESSION}"
sleep 0.5
run "${MAILBOX} status --session ${SESSION}"

echo
echo "OK — subscribe/read, idle-wake, supervised watch, and teardown all worked."
echo "For the real GitHub-PR wake on a live Claude Code session, see docs/demo.md."
