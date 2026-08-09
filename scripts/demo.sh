#!/usr/bin/env bash
#
# demo.sh — a self-contained, no-network walkthrough of the agent-mailbox wake
# loop. It proves, on your machine with no GitHub and no Claude Code, that:
#
#   1. the four-verb agent loop works: subscribe -> (publish) -> read;
#   2. an *idle* session wakes when mail lands (the daemon writes that session's
#      Claude Code inbox socket as part of the publish, delivering a payload-free
#      "mail on topic X"
#      reminder);
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
# Keep the fake Claude Code session registry inside the throwaway dir: without this
# the demo would read your REAL ~/.claude/sessions and could deliver its wake onto a
# live session of yours.
export MAILBOX_CLAUDE_SESSIONS_DIR="${WORK_DIR}/claude-sessions"
export RUST_LOG="${RUST_LOG:-error}"

SESSION="demo-session"
# The session every client command below runs as. There is no `--session` flag:
# a command learns whose session it is from `$CLAUDE_CODE_SESSION_ID`, which Claude
# Code exports into every tool call — so exporting it here is exactly the shape an
# agent's own shell has. (`serve` and the harness hooks ignore it: the daemon has no
# session, and a hook reads `session_id` from its payload on stdin.)
export CLAUDE_CODE_SESSION_ID="${SESSION}"
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

# Poll a condition with a bounded deadline (never a fixed sleep waiting on a state).
wait_for() {
  local what="$1"; shift
  for _ in $(seq 1 100); do
    if "$@"; then return 0; fi
    sleep 0.05
  done
  echo "error: timed out waiting for ${what}" >&2
  exit 1
}
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
run "${MAILBOX} subscribe demo.hello"
run "${MAILBOX} publish demo.hello --body '{\"msg\":\"first\"}'"
run "${MAILBOX} read"

# --- 2. wake an IDLE session ---------------------------------------------------
# This is the load-bearing mechanic, and it is exactly what Claude Code does. A
# session binds an inbox socket and registers itself in ~/.claude/sessions; a publish
# makes the DAEMON write that socket as part of serving the publish; Claude Code then
# starts a turn on the idle session with whatever arrived. Here we stand in for Claude
# Code with a socket of our own — no sleeps for a third process to be scheduled,
# because the write happens inside the publish request.
step "2. wake an idle session (the inbox-socket contract)"

mkdir -p "${MAILBOX_CLAUDE_SESSIONS_DIR}"
INBOX="${WORK_DIR}/inbox.sock"
FRAME="${WORK_DIR}/frame.json"

# A stand-in for Claude Code: accept one connection and record the frame.
cat >"${WORK_DIR}/listen.py" <<'LISTENER'
import os
import socket
import sys

sock, out = sys.argv[1], sys.argv[2]
if os.path.exists(sock):
    os.unlink(sock)
srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
srv.bind(sock)
srv.listen(1)
conn, _ = srv.accept()
open(out, "wb").write(conn.recv(65536))
LISTENER

python3 "${WORK_DIR}/listen.py" "${INBOX}" "${FRAME}" &
LISTENER_PID=$!
trap 'kill "${LISTENER_PID}" 2>/dev/null || true' EXIT
wait_for "the demo inbox socket" test -S "${INBOX}"

# Register the session the way Claude Code does, naming that socket.
cat >"${MAILBOX_CLAUDE_SESSIONS_DIR}/${SESSION}.json" <<REGISTRY
{"pid":$$,"sessionId":"${SESSION}","cwd":"${WORK_DIR}","status":"idle","name":"demo","updatedAt":1786000000000,"messagingSocketPath":"${INBOX}"}
REGISTRY

echo "running the SessionStart hook (registers this session's agent inbox) ..."
echo "{\"session_id\":\"${SESSION}\",\"hook_event_name\":\"SessionStart\"}" \
  | "${MAILBOX}" harness session-start

echo "publishing while the session is idle ..."
run "${MAILBOX} publish demo.hello --body '{\"msg\":\"wake up\"}'"

wait_for "the wake to reach the session's inbox" test -s "${FRAME}"
echo "the wake delivered to the session's inbox (topic NAMES only, never a body):"
sed 's/^/    /' "${FRAME}"
if grep -q "wake up" "${FRAME}"; then
  echo "error: the wake carried the event body; it must be payload-free" >&2
  exit 1
fi

run "${MAILBOX} read"

# --- 3. a bridge-SUPERVISED adapter (no agent-owned poller) -------------------
# `watch stub` records interest, subscribes the session, and the daemon spawns
# the stub adapter, which publishes a synthetic edge every --interval-ms. The
# agent NEVER launches this loop itself.
step "3. supervised adapter: watch stub, see it running, read its edges"
run "${MAILBOX} watch stub demo --interval-ms 500"
sleep 1.2   # let the supervisor spawn the adapter and it publish a couple edges
run "${MAILBOX} status"
run "${MAILBOX} read --limit 5"

# --- 4. teardown: no zombie pollers -------------------------------------------
# Dropping the last interest stops the adapter. `status` shows no running child.
step "4. unwatch -> the supervisor stops the adapter (no zombie poller)"
run "${MAILBOX} unwatch stub demo"
sleep 0.5
run "${MAILBOX} status"

echo
echo "OK — subscribe/read, idle-wake, supervised watch, and teardown all worked."
echo "For the real GitHub-PR wake on a live Claude Code session, see docs/demo.md."
