//! The mention watch through the bridge (design/04): `mailbox watch
//! slack-bot` records the watch, the daemon spawns the Slack adapter with
//! the mention config, and what the adapter publishes on `slack.bot`
//! reaches the session's `mailbox read`.
//!
//! The adapter here is a FAKE: a script that records the config it was given and
//! publishes one mention. The real adapter needs a Socket Mode server, and its
//! own e2e tests (`adapters/slack-adapter/tests/mention_e2e.rs`) run it against
//! one. This proves the plumbing between them: CLI parsing, the control request,
//! storage, the resolver's config, the host relay, and `--skip` behaving as it
//! does for the other Slack kinds (ADR-0029).

use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Output, Stdio};
use std::time::Duration;

use serde_json::{Value, json};
use tempfile::TempDir;

mod common;

use common::{mailbox_command, poll_until};

const SESSION: &str = "mention-e2e-session";
const MIKEY: &str = "U0AB7RJSQBE";

/// A fake Slack adapter: saves its config line, publishes one mention, then
/// waits for SIGTERM the way the real one holds its connection.
const FAKE_ADAPTER: &str = r#"#!/usr/bin/env bash
set -euo pipefail
IFS= read -r config
printf '%s\n' "$config" > "${FAKE_ADAPTER_DIR:?}/config.json"
printf '%s\n' '{"version":1,"type":"publish","topic":"slack.bot","adapter":"slack-adapter","body":{"kind":"slack_mention","channel":"C0AEL6NV7GC","ts":"1791935731.212069","user":"U0AB7RJSQBE","event_id":"Ev1","permalink":"https://tryrelevance.slack.com/archives/C0AEL6NV7GC/p1791935731212069"},"subject":{"text":"mention from Mikey Hudson in #mikeys-private-test-arena","link":"https://tryrelevance.slack.com/archives/C0AEL6NV7GC/p1791935731212069"}}'
trap 'exit 0' TERM
while true; do sleep 0.1; done
"#;

struct Bridge {
    child: Child,
    dir: TempDir,
}

impl Bridge {
    fn start() -> Self {
        let dir = TempDir::new().unwrap();
        let adapter = script(dir.path(), "fake-slack-adapter.sh", FAKE_ADAPTER);
        let child = mailbox_command()
            .arg("serve")
            .env("AGENT_MAILBOX_DB", dir.path().join("mailbox.db"))
            // Never a real Claude Code registry: a test must not wake a real session.
            .env(
                "MAILBOX_CLAUDE_SESSIONS_DIR",
                dir.path().join("claude-sessions"),
            )
            .env("MAILBOX_SLACK_ADAPTER_BIN", &adapter)
            .env("FAKE_ADAPTER_DIR", dir.path())
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

    fn run_as(&self, session: &str, args: &[&str]) -> Output {
        mailbox_command()
            .args(args)
            .env("AGENT_MAILBOX_DB", self.dir.path().join("mailbox.db"))
            .env("CLAUDE_CODE_SESSION_ID", session)
            .env("RUST_LOG", "error")
            .output()
            .unwrap()
    }

    fn json(&self, args: &[&str]) -> Value {
        let mut all = vec!["--json"];
        all.extend_from_slice(args);
        let output = self.run_as(SESSION, &all);
        assert!(
            output.status.success(),
            "mailbox {args:?} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        serde_json::from_slice(&output.stdout).unwrap()
    }

    fn adapter_config(&self) -> Option<Value> {
        let raw = std::fs::read_to_string(self.dir.path().join("config.json")).ok()?;
        serde_json::from_str(&raw).ok()
    }
}

impl Drop for Bridge {
    fn drop(&mut self) {
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
fn a_mention_reaches_the_watching_session() {
    let bridge = Bridge::start();
    let skip = format!("user={MIKEY}");
    let watched = bridge.json(&["watch", "slack-bot", "--skip", &skip]);
    assert_eq!(watched["topic"], "slack.bot");
    assert_eq!(watched["skip"], json!([{"user": MIKEY}]));

    let config = poll_until("the adapter starts", Duration::from_secs(10), || {
        bridge.adapter_config()
    });
    assert_eq!(
        config,
        json!({"kind": "slack-bot", "topic": "slack.bot",
            "skip": [{"user": MIKEY}], "baseline": null}),
        "no interval and no credential: the adapter reads its tokens itself"
    );

    let events = poll_until("the mention is published", Duration::from_secs(10), || {
        let events = bridge.json(&["read"])["events"].as_array().cloned()?;
        (!events.is_empty()).then_some(events)
    });
    assert_eq!(events[0]["topic"], "slack.bot");
    assert_eq!(events[0]["body"]["kind"], "slack_mention");
    assert_eq!(
        events[0]["subject"]["text"],
        "mention from Mikey Hudson in #mikeys-private-test-arena"
    );

    let watch = bridge.json(&["status"])["watches"][0].clone();
    assert_eq!(watch["kind"], "slack-bot");
    assert_eq!(watch["repo"], "bot");
    assert_eq!(watch["state"], "running");

    // One mention watch per bridge, shared: another session asking for it with
    // other filters would change what wakes this one, so it is refused.
    let refused = bridge.run_as("another-session", &["watch", "slack-bot"]);
    assert!(!refused.status.success());
    assert!(String::from_utf8_lossy(&refused.stderr).contains(&format!("--skip {skip}")));

    let unwatched = bridge.json(&["unwatch", "slack-bot"]);
    assert_eq!(unwatched["topic"], "slack.bot");
    poll_until("the adapter stops", Duration::from_secs(10), || {
        let status = bridge.json(&["status"]);
        (status["watches"][0]["state"] == "stopped").then_some(())
    });
}
