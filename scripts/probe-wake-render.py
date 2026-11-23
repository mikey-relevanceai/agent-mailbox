#!/usr/bin/env python3
"""See how Claude Code shows a mailbox wake in a pane (ADR-0033).

Starts a private `mailbox serve`, starts a throwaway Claude Code session in a
pseudo-terminal, subscribes that session to one topic, publishes to it, and
reports what the session's screen showed.

Re-run it after a Claude Code upgrade: the compact rendering depends on an
undocumented Claude Code tag, and this is how to find out that it changed.

Isolation, and why each piece matters:

- The daemon is private (`AGENT_MAILBOX_DB` in a temp dir), so nothing is
  published to the shared bridge and no other agent is woken.
- The session loads no user settings (`--setting-sources project`), so neither
  the mailbox hooks nor any plugin hook runs. Otherwise its SessionStart hook
  would register an inbox on the shared bridge.
- Inherited `CLAUDE*` variables are dropped. Claude Code sets
  `CLAUDE_CODE_CHILD_SESSION` for anything a session spawns, and a session that
  sees it turns transcript saving off. `CLAUDE_CODE_HARBOR_KITE=1` is passed
  explicitly, because without user settings nothing else turns the inbox socket on.
- `--bare` is NOT used: a bare session binds no inbox socket and writes no
  registry entry, so it can never be woken.

Each run spends a small model turn (Haiku): the woken session reacts. Bash is
disabled in the session so it cannot run anything.

The screen is read from the raw terminal stream with escape codes stripped. That
is crude but sufficient: the checks only ask which of two texts appeared.

Usage:
    cargo build -p mailbox
    scripts/probe-wake-render.py                 # one event, default mode
    scripts/probe-wake-render.py --bypass        # a bypassPermissions receiver
    scripts/probe-wake-render.py --count 3 --gap 2   # a burst; later ones land mid-turn
"""

import argparse
import glob
import json
import os
import pty
import re
import select
import shutil
import signal
import struct
import subprocess
import sys
import tempfile
import time
import fcntl
import termios

COMPACT = "Message from @agent-mailbox"
WRAPPER = "This came from another Claude session"
ESCAPES = re.compile(rb"\x1b\[[0-9;?<>]*[A-Za-z~]|\x1b\][^\x07]*\x07|\x1b[=>()][0-9A-Za-z]?")


def main():
    args = parse_args()
    mailbox = os.path.abspath(args.mailbox)
    if not os.access(mailbox, os.X_OK):
        sys.exit(f"no mailbox binary at {mailbox}: run `cargo build -p mailbox` first")
    if shutil.which("claude") is None:
        sys.exit("no `claude` on PATH")

    # The temp dir must be short: the daemon's socket lives in it, and a Unix
    # socket path is limited to about 100 bytes.
    work = tempfile.mkdtemp(prefix="mbx-probe-")
    os.mkdir(os.path.join(work, "cwd"))
    raw_path = os.path.join(work, "screen.raw")
    mailbox_env = dict(os.environ, AGENT_MAILBOX_DB=os.path.join(work, "mailbox.db"), RUST_LOG="error")
    mailbox_env.pop("CLAUDE_CODE_SESSION_ID", None)

    daemon = subprocess.Popen([mailbox, "serve"], env=mailbox_env, stderr=subprocess.DEVNULL)
    child, fd = start_claude(os.path.join(work, "cwd"), args.bypass)
    raw = open(raw_path, "wb")
    try:
        pump(fd, raw, 6)
        # Two start-up dialogs can appear, and both default to "No, exit": the
        # folder-trust check, and (with --bypass) the bypass-mode warning.
        for marker in ("trust this folder", "Yes, I accept"):
            if marker.replace(" ", "") in screen_text(raw_path).replace(" ", ""):
                os.write(fd, b"\x1b[B")
                pump(fd, raw, 1)
                os.write(fd, b"\r")
                pump(fd, raw, 6)

        session = wait_for_registration(child, fd, raw)
        if session is None:
            sys.exit("the throwaway session never registered an inbox socket")
        print(f"throwaway session {session['sessionId']} (pid {child}), socket {session['messagingSocketPath']}")

        as_probe = dict(mailbox_env, CLAUDE_CODE_SESSION_ID=session["sessionId"])
        run([mailbox, "subscribe", args.topic], as_probe)
        for n in range(args.count):
            publish = [mailbox, "publish", args.topic, "--body", json.dumps({"probe": n}), "--subject", f"{args.subject} {n + 1}"]
            if args.link:
                publish += ["--link", args.link]
            run(publish, mailbox_env)
            pump(fd, raw, args.gap)
        pump(fd, raw, args.settle)
    finally:
        os.kill(child, signal.SIGTERM)
        pump(fd, raw, 2)
        daemon.terminate()
        daemon.wait(timeout=10)

    text = screen_text(raw_path)
    print(f"compact line ({COMPACT!r}): {text.count(COMPACT)}")
    print(f"full wrapper ({WRAPPER!r}): {text.count(WRAPPER)}")
    for match in re.finditer(re.escape(COMPACT) + r"[^\n]*?\(ctrl\+o to expand\)", text):
        print(f"  › {match.group(0)}")
    print(f"raw terminal stream: {raw_path}")


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--mailbox", default="target/debug/mailbox", help="the mailbox binary to test")
    parser.add_argument("--bypass", action="store_true", help="run the receiver with --dangerously-skip-permissions")
    parser.add_argument("--count", type=int, default=1, help="events to publish")
    parser.add_argument("--gap", type=float, default=1.0, help="seconds between publishes")
    parser.add_argument("--settle", type=float, default=25.0, help="seconds to watch after the last publish")
    parser.add_argument("--topic", default="probe.render")
    parser.add_argument("--subject", default="probe event")
    parser.add_argument("--link", default=None)
    return parser.parse_args()


def start_claude(cwd, bypass):
    env = {k: v for k, v in os.environ.items() if not k.startswith("CLAUDE")}
    env["CLAUDE_CODE_HARBOR_KITE"] = "1"
    argv = [
        "claude",
        "--setting-sources", "project",
        "--settings", json.dumps({"crossSessionInbound": "accept"}),
        "--model", "haiku",
        "--disallowed-tools", "Bash",
    ]
    if bypass:
        argv.append("--dangerously-skip-permissions")
    child, fd = pty.fork()
    if child == 0:
        os.chdir(cwd)
        os.execvpe("claude", argv, env)
    # 120 columns: the width the measurements in ADR-0033 assume.
    fcntl.ioctl(fd, termios.TIOCSWINSZ, struct.pack("HHHH", 45, 120, 0, 0))
    return child, fd


def wait_for_registration(child, fd, raw):
    for _ in range(20):
        for path in glob.glob(os.path.expanduser("~/.claude/sessions/*.json")):
            try:
                entry = json.load(open(path))
            except (OSError, ValueError):
                continue
            if entry.get("pid") == child and entry.get("messagingSocketPath"):
                return entry
        pump(fd, raw, 1)
    return None


def run(argv, env):
    result = subprocess.run(argv, env=env, capture_output=True, text=True)
    if result.returncode != 0:
        sys.exit(f"{' '.join(argv[1:3])} failed: {result.stderr.strip()}")


def pump(fd, raw, seconds):
    end = time.time() + seconds
    while time.time() < end:
        ready, _, _ = select.select([fd], [], [], 0.2)
        if ready:
            try:
                data = os.read(fd, 65536)
            except OSError:
                return
            raw.write(data)
            raw.flush()


def screen_text(raw_path):
    return ESCAPES.sub(b"", open(raw_path, "rb").read()).decode("utf8", "replace")


if __name__ == "__main__":
    main()
