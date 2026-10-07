//! The Slack watch through the whole bridge (design/02): `mailbox watch
//! slack-thread <link>` makes the daemon spawn the REAL Slack adapter, which
//! baselines against a FAKE Slack (`curl`) with a token from a FAKE Keychain
//! (`security`); a reply posted after that reaches the session's `mailbox read`
//! with its subject and link, and `unwatch` stops the adapter.
//!
//! Which messages wake is covered by the adapter's own tests; this proves the
//! plumbing between them: CLI parsing, the control request, storage, the
//! resolver's config, the injected baseline and the host relay.

use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Output, Stdio};
use std::time::Duration;

use serde_json::{Value, json};
use tempfile::TempDir;

mod common;

use common::{mailbox_command, poll_until, slack_adapter_bin};

const SESSION: &str = "slack-e2e-session";
const CHANNEL: &str = "C0C83CXLUL8";
const PARENT: &str = "1791349480.652779";
const TOKEN: &str = "xoxb-e2e";

/// A fake `curl` serving `$FAKE_SLACK_DIR/<method>`. `conversations.replies`
/// answers from `replies.later` once that file exists, so the test decides when
/// the reply is "posted".
const FAKE_CURL: &str = r#"#!/usr/bin/env bash
set -euo pipefail
dir="${FAKE_SLACK_DIR:?}"
config="$(cat)"
url="$(printf '%s\n' "$config" | sed -n 's/^url = "\(.*\)"$/\1/p')"
method="${url##*/}"
f="$dir/$method"
if [ "$method" = "conversations.replies" ] && [ -f "$dir/replies.later" ]; then f="$dir/replies.later"; fi
cat "$f"
printf '\n@@mailbox-slack-status 200 \n' >&2
"#;

const FAKE_SECURITY: &str = "#!/usr/bin/env bash\necho \"$FAKE_KEYCHAIN_TOKEN\"\n";

struct Bridge {
    child: Child,
    dir: TempDir,
}

impl Bridge {
    fn start() -> Self {
        let dir = TempDir::new().unwrap();
        let slack = dir.path().join("slack");
        std::fs::create_dir(&slack).unwrap();
        let fixture = |name: &str, body: Value| {
            std::fs::write(slack.join(name), body.to_string()).unwrap();
        };
        fixture(
            "auth.test",
            json!({"ok": true, "url": "https://tryrelevance.slack.com/"}),
        );
        fixture(
            "conversations.info",
            json!({"ok": true, "channel": {"name": "team-arg-agent-watercooler"}}),
        );
        fixture(
            "users.info",
            json!({"ok": true, "user": {"profile": {"display_name": "Ben Skinner"}}}),
        );
        fixture(
            "conversations.replies",
            json!({"ok": true, "has_more": false, "messages": [
                {"ts": PARENT, "thread_ts": PARENT, "user": "U1"}]}),
        );
        let curl = script(dir.path(), "fake-curl.sh", FAKE_CURL);
        let security = script(dir.path(), "fake-security.sh", FAKE_SECURITY);

        let child = mailbox_command()
            .arg("serve")
            .env("AGENT_MAILBOX_DB", dir.path().join("mailbox.db"))
            // Never a real Claude Code registry: a test must not wake a real session.
            .env(
                "MAILBOX_CLAUDE_SESSIONS_DIR",
                dir.path().join("claude-sessions"),
            )
            .env("MAILBOX_SLACK_ADAPTER_BIN", slack_adapter_bin())
            // The adapter inherits serve's environment, so it finds the fakes.
            .env("MAILBOX_SLACK_CURL_BIN", &curl)
            .env("MAILBOX_SLACK_SECURITY_BIN", &security)
            .env("FAKE_SLACK_DIR", &slack)
            .env("FAKE_KEYCHAIN_TOKEN", TOKEN)
            .env("RUST_LOG", "error")
            .stdin(Stdio::null())
            .spawn()
            .unwrap();
        let bridge = Bridge { child, dir };
        let socket = bridge.dir.path().join("mailbox.sock");
        poll_until("daemon socket", Duration::from_secs(10), || {
            UnixStream::connect(&socket).ok()
        });
        bridge
    }

    fn run(&self, args: &[&str]) -> Output {
        let output = mailbox_command()
            .args(args)
            .env("AGENT_MAILBOX_DB", self.dir.path().join("mailbox.db"))
            .env("CLAUDE_CODE_SESSION_ID", SESSION)
            .env("RUST_LOG", "error")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "mailbox {args:?} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        output
    }

    fn json(&self, args: &[&str]) -> Value {
        let mut all = vec!["--json"];
        all.extend_from_slice(args);
        serde_json::from_slice(&self.run(&all).stdout).unwrap()
    }

    fn post_reply(&self) {
        std::fs::write(
            self.dir.path().join("slack/replies.later"),
            json!({"ok": true, "has_more": false, "messages": [
                {"ts": PARENT, "thread_ts": PARENT, "user": "U1"},
                {"ts": "1791349600.000001", "thread_ts": PARENT, "user": "U2", "text": "on it"}
            ]})
            .to_string(),
        )
        .unwrap();
    }

    fn baseline(&self) -> Option<String> {
        let conn = rusqlite::Connection::open(self.dir.path().join("mailbox.db")).ok()?;
        conn.query_row("SELECT baseline FROM adapter_baseline", [], |row| {
            row.get(0)
        })
        .ok()
    }
}

impl Drop for Bridge {
    fn drop(&mut self) {
        // SIGTERM lets serve tear down and reap the adapter it spawned.
        let pid = nix::unistd::Pid::from_raw(self.child.id() as i32);
        let _ = nix::sys::signal::kill(pid, nix::sys::signal::Signal::SIGTERM);
        let _ = common::wait_within(&mut self.child, Duration::from_secs(5));
    }
}

fn script(dir: &Path, name: &str, body: &str) -> PathBuf {
    let path = dir.join(name);
    std::fs::write(&path, body).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    path
}

#[test]
fn a_reply_in_a_watched_thread_reaches_the_session() {
    let bridge = Bridge::start();
    let link = format!("https://tryrelevance.slack.com/archives/{CHANNEL}/p1791349480652779");
    let watched = bridge.json(&["watch", "slack-thread", &link, "--interval", "1"]);
    let topic = format!("slack.thread.{CHANNEL}/{PARENT}");
    assert_eq!(watched["topic"], topic);

    // The first poll baselines on the existing thread and publishes nothing.
    poll_until("the adapter baselines", Duration::from_secs(15), || {
        bridge.baseline()
    });
    let status = bridge.json(&["status"]);
    let watch = &status["watches"][0];
    assert_eq!(watch["kind"], "slack-thread");
    assert_eq!(watch["repo"], format!("{CHANNEL}/{PARENT}"));
    assert_eq!(watch["state"], "running");

    bridge.post_reply();
    let events = poll_until("the reply is published", Duration::from_secs(15), || {
        let events = bridge.json(&["read"])["events"].as_array().cloned()?;
        let replies: Vec<Value> = events.into_iter().filter(|e| e["topic"] == topic).collect();
        (!replies.is_empty()).then_some(replies)
    });
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["body"]["kind"], "slack_reply");
    assert_eq!(events[0]["body"]["ts"], "1791349600.000001");
    assert!(events[0]["body"].get("text").is_none());

    let unwatched = bridge.json(&["unwatch", "slack-thread", &format!("{CHANNEL}/{PARENT}")]);
    assert_eq!(unwatched["topic"], topic);
    poll_until("the adapter stops", Duration::from_secs(10), || {
        let status = bridge.json(&["status"]);
        (status["watches"][0]["state"] == "stopped").then_some(())
    });
}
