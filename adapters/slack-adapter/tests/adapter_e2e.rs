//! End-to-end tests for the Slack adapter: the REAL built binary against a FAKE
//! `curl` (a scripted Slack) and a FAKE `security` (the Keychain), so the whole
//! token → poll → publish → baseline path runs with no network and no token.
//!
//! The in-memory fake in `watcher.rs` covers which messages wake. These cover
//! what only the process can show: the token is read from the Keychain and sent
//! as a header on stdin, never as an argument; the NDJSON it writes; the cursor
//! it resumes from; and that a missing token or a rate limit behave as designed.
//!
//! Every run is bounded by `max_polls`, so the adapter exits on its own.

use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use tempfile::TempDir;

const CHANNEL: &str = "C0C83CXLUL8";
const TOKEN: &str = "xoxb-test-token";

fn adapter_bin() -> &'static str {
    env!("CARGO_BIN_EXE_mailbox-slack-adapter")
}

/// A fake `curl`: reads the `-K -` config from stdin, refuses a wrong token the
/// way Slack does, logs the method and form data (never the header), and answers
/// from `$FAKE_SLACK_DIR/<method>.<n>`, clamping to the last fixture. A fixture
/// whose first line is `@429` answers with HTTP 429 and a `Retry-After: 0`;
/// `@503` answers with an HTTP 503.
const FAKE_CURL: &str = r#"#!/usr/bin/env bash
set -euo pipefail
dir="${FAKE_SLACK_DIR:?}"
config="$(cat)"
for arg in "$@"; do echo "arg:$arg" >> "$dir/args.log"; done
url="$(printf '%s\n' "$config" | sed -n 's/^url = "\(.*\)"$/\1/p')"
auth="$(printf '%s\n' "$config" | sed -n 's/^header = "\(.*\)"$/\1/p')"
data="$(printf '%s\n' "$config" | sed -n 's/^data = "\(.*\)"$/\1/p')"
method="${url##*/}"
echo "$method $data" >> "$dir/calls.log"
if [ "$auth" != "Authorization: Bearer ${FAKE_SLACK_TOKEN:?}" ]; then
  printf '{"ok":false,"error":"invalid_auth"}'
  printf '\n@@mailbox-slack-status 200 \n' >&2
  exit 0
fi
ctr="$dir/counter.$method"
i=0; [ -f "$ctr" ] && i="$(cat "$ctr")"
echo $((i + 1)) > "$ctr"
max=0
for f in "$dir/$method".*; do
  [ -e "$f" ] || continue
  n="${f##*.}"; case "$n" in (*[!0-9]*) continue ;; esac
  [ "$n" -gt "$max" ] && max="$n"
done
use="$i"; [ "$i" -gt "$max" ] && use="$max"
f="$dir/$method.$use"
[ -f "$f" ] || { echo "fake-curl: no fixture $f" >&2; exit 3; }
if [ "$(head -n1 "$f")" = "@429" ]; then
  printf '{"ok":false,"error":"ratelimited"}'
  printf '\n@@mailbox-slack-status 429 0\n' >&2
  exit 0
fi
if [ "$(head -n1 "$f")" = "@503" ]; then
  printf '\n@@mailbox-slack-status 503 \n' >&2
  exit 0
fi
cat "$f"
printf '\n@@mailbox-slack-status 200 \n' >&2
"#;

/// A fake `security`: prints the token for the adapter's service, else fails the
/// way the real one does when the item is missing.
const FAKE_SECURITY: &str = r#"#!/usr/bin/env bash
if [ "$*" = "find-generic-password -s agent-mailbox.slack -w" ] && [ -n "${FAKE_KEYCHAIN_TOKEN:-}" ]; then
  echo "$FAKE_KEYCHAIN_TOKEN"; exit 0
fi
echo "security: SecKeychainSearchCopyNext: The specified item could not be found in the keychain." >&2
exit 44
"#;

struct FakeSlack {
    dir: TempDir,
    curl: PathBuf,
    security: PathBuf,
}

impl FakeSlack {
    fn new() -> Self {
        let dir = TempDir::new().unwrap();
        let curl = script(dir.path(), "fake-curl.sh", FAKE_CURL);
        let security = script(dir.path(), "fake-security.sh", FAKE_SECURITY);
        let fake = Self {
            dir,
            curl,
            security,
        };
        fake.fixture(
            "auth.test",
            0,
            json!({"ok": true, "url": "https://tryrelevance.slack.com/", "user": "watch"}),
        );
        fake.fixture(
            "conversations.info",
            0,
            json!({"ok": true, "channel": {"name": "team-arg-agent-watercooler"}}),
        );
        fake.fixture(
            "users.info",
            0,
            json!({"ok": true, "user": {"profile": {"display_name": "Ben Skinner"}}}),
        );
        fake
    }

    fn fixture(&self, method: &str, i: usize, body: Value) {
        std::fs::write(
            self.dir.path().join(format!("{method}.{i}")),
            body.to_string(),
        )
        .unwrap();
    }

    fn raw_fixture(&self, method: &str, i: usize, body: &str) {
        std::fs::write(self.dir.path().join(format!("{method}.{i}")), body).unwrap();
    }

    fn log(&self, name: &str) -> String {
        std::fs::read_to_string(self.dir.path().join(name)).unwrap_or_default()
    }

    /// Run the adapter with `config`, the Keychain holding `keychain_token`.
    fn run(&self, config: Value, keychain_token: Option<&str>) -> Output {
        self.run_with(config, keychain_token, &[])
    }

    fn run_with(
        &self,
        config: Value,
        keychain_token: Option<&str>,
        env: &[(&str, &str)],
    ) -> Output {
        self.spawn(config, keychain_token, env)
            .wait_with_output()
            .unwrap()
    }

    fn spawn(&self, config: Value, keychain_token: Option<&str>, env: &[(&str, &str)]) -> Child {
        let mut command = Command::new(adapter_bin());
        command
            .env("MAILBOX_SLACK_CURL_BIN", &self.curl)
            .env("MAILBOX_SLACK_SECURITY_BIN", &self.security)
            .env("FAKE_SLACK_DIR", self.dir.path())
            .env("FAKE_SLACK_TOKEN", TOKEN)
            .env("MAILBOX_SLACK_RATE_LIMIT_BACKOFF_MS", "10")
            .env("RUST_LOG", "info")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        match keychain_token {
            Some(token) => command.env("FAKE_KEYCHAIN_TOKEN", token),
            None => command.env_remove("FAKE_KEYCHAIN_TOKEN"),
        };
        command.envs(env.iter().copied());
        let mut child = command.spawn().unwrap();
        let mut stdin = child.stdin.take().unwrap();
        writeln!(stdin, "{config}").unwrap();
        drop(stdin);
        child
    }
}

fn script(dir: &Path, name: &str, body: &str) -> PathBuf {
    let path = dir.join(name);
    std::fs::write(&path, body).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    path
}

fn channel_config(baseline: Value, max_polls: u64) -> Value {
    json!({
        "topic": format!("slack.channel.{CHANNEL}"),
        "channel": CHANNEL,
        "interval_ms": 1,
        "baseline": baseline,
        "max_polls": max_polls,
    })
}

fn lines(output: &Output) -> Vec<Value> {
    String::from_utf8(output.stdout.clone())
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

fn of_type<'a>(lines: &'a [Value], kind: &str) -> Vec<&'a Value> {
    lines.iter().filter(|line| line["type"] == kind).collect()
}

fn history(messages: Value) -> Value {
    json!({"ok": true, "messages": messages, "has_more": false})
}

#[test]
fn baselines_then_publishes_each_new_message_once() {
    let slack = FakeSlack::new();
    // Poll 0 baselines (limit=1); poll 1 sees a join and a message; poll 2 nothing.
    slack.fixture(
        "conversations.history",
        0,
        history(json!([{"ts": "1791349480.652779", "user": "U1"}])),
    );
    slack.fixture(
        "conversations.history",
        1,
        history(json!([
            {"ts": "1791349500.000002", "user": "U2", "text": "secret plans"},
            {"ts": "1791349500.000001", "user": "U2", "subtype": "channel_join"},
        ])),
    );
    slack.fixture("conversations.history", 2, history(json!([])));

    let output = slack.run(channel_config(Value::Null, 3), Some(TOKEN));
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );

    let lines = lines(&output);
    let publishes = of_type(&lines, "publish");
    assert_eq!(
        publishes.len(),
        1,
        "the join does not wake; the message does, once"
    );
    let publish = publishes[0];
    assert_eq!(publish["topic"], format!("slack.channel.{CHANNEL}"));
    assert_eq!(
        publish["subject"]["text"],
        "new message from Ben Skinner in #team-arg-agent-watercooler"
    );
    assert_eq!(
        publish["subject"]["link"],
        format!("https://tryrelevance.slack.com/archives/{CHANNEL}/p1791349500000002")
    );
    assert!(
        !publish.to_string().contains("secret plans"),
        "message text never leaves Slack"
    );

    let baselines: Vec<_> = of_type(&lines, "baseline")
        .iter()
        .map(|b| b["value"]["last_ts"].clone())
        .collect();
    assert_eq!(
        baselines,
        [json!("1791349480.652779"), json!("1791349500.000002")]
    );

    // The second poll asked for messages after the baseline, not the whole channel.
    let polls: Vec<String> = slack
        .log("calls.log")
        .lines()
        .filter(|call| call.starts_with("conversations.history"))
        .map(str::to_string)
        .collect();
    assert!(polls[1].contains("oldest=1791349480.652779"), "{polls:?}");
    assert!(polls[1].contains("limit=200"), "{polls:?}");
}

#[test]
fn the_token_goes_on_stdin_never_in_arguments() {
    let slack = FakeSlack::new();
    slack.fixture("conversations.history", 0, history(json!([])));
    let output = slack.run(channel_config(Value::Null, 1), Some(TOKEN));
    assert!(output.status.success());
    let args = slack.log("args.log");
    assert!(args.contains("arg:-K"), "{args}");
    assert!(
        !args.contains(TOKEN),
        "the token must not be visible in the process table"
    );
    assert!(
        !String::from_utf8_lossy(&output.stderr).contains(TOKEN),
        "nor in logs"
    );
}

#[test]
fn a_restart_resumes_from_the_injected_cursor() {
    let slack = FakeSlack::new();
    slack.fixture("conversations.history", 0, history(json!([])));
    let output = slack.run(
        channel_config(json!({"last_ts": "1791349500.000002"}), 1),
        Some(TOKEN),
    );
    assert!(output.status.success());
    assert!(of_type(&lines(&output), "publish").is_empty());
    assert!(
        slack.log("calls.log").contains("oldest=1791349500.000002"),
        "resumes from the persisted cursor instead of re-baselining"
    );
}

#[test]
fn a_thread_watch_publishes_replies_with_thread_links() {
    let slack = FakeSlack::new();
    let parent = json!({"ts": "1791349480.652779", "user": "U1", "thread_ts": "1791349480.652779"});
    slack.fixture(
        "conversations.replies",
        0,
        history(
            json!([{"ts": "1791349480.652779", "thread_ts": "1791349480.652779",
            "latest_reply": "1791349481.000001"}]),
        ),
    );
    slack.fixture(
        "conversations.replies",
        1,
        history(json!([parent, {"ts": "1791349600.000001", "user": "U2",
            "thread_ts": "1791349480.652779"}])),
    );
    let config = json!({
        "topic": format!("slack.thread.{CHANNEL}/1791349480.652779"),
        "channel": CHANNEL,
        "thread_ts": "1791349480.652779",
        "interval_ms": 1,
        "baseline": null,
        "max_polls": 2,
    });
    let output = slack.run(config, Some(TOKEN));
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let lines = lines(&output);
    let publishes = of_type(&lines, "publish");
    assert_eq!(publishes.len(), 1, "the parent is not a new reply");
    assert_eq!(
        publishes[0]["subject"]["link"],
        format!(
            "https://tryrelevance.slack.com/archives/{CHANNEL}/p1791349600000001?thread_ts=1791349480.652779&cid={CHANNEL}"
        )
    );
}

#[test]
fn no_token_in_the_keychain_exits_non_zero_and_says_how_to_add_one() {
    let slack = FakeSlack::new();
    let output = slack.run(channel_config(Value::Null, 1), None);
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("security add-generic-password"), "{stderr}");
    assert!(
        slack.log("calls.log").is_empty(),
        "no Slack call without a token"
    );
}

#[test]
fn a_rejected_token_exits_non_zero() {
    let slack = FakeSlack::new();
    let output = slack.run(channel_config(Value::Null, 1), Some("xoxb-wrong"));
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("rejected the token"));
}

#[test]
fn a_rate_limit_waits_and_retries_instead_of_failing() {
    let slack = FakeSlack::new();
    slack.raw_fixture("conversations.history", 0, "@429");
    slack.fixture("conversations.history", 1, history(json!([])));
    let output = slack.run(channel_config(Value::Null, 1), Some(TOKEN));
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(of_type(&lines(&output), "baseline").len(), 1);
    assert_eq!(
        slack
            .log("calls.log")
            .matches("conversations.history")
            .count(),
        2,
        "one rate-limited call, one retry"
    );
}

#[test]
fn a_persistent_outage_skips_polls_then_exits_non_zero() {
    let slack = FakeSlack::new();
    slack.raw_fixture("conversations.history", 0, "@503");
    let output = slack.run_with(
        channel_config(Value::Null, 0),
        Some(TOKEN),
        &[("MAILBOX_SLACK_MAX_TRANSIENT_FAILURES", "3")],
    );
    assert!(
        !output.status.success(),
        "an outage that never ends must surface"
    );
    assert!(
        of_type(&lines(&output), "baseline").is_empty(),
        "the cursor never moved"
    );
    assert_eq!(
        slack
            .log("calls.log")
            .matches("conversations.history")
            .count(),
        3,
        "one skipped poll per tick, up to the budget"
    );
}

#[test]
fn a_rate_limit_that_never_lifts_exits_non_zero() {
    let slack = FakeSlack::new();
    slack.raw_fixture("conversations.history", 0, "@429");
    let output = slack.run_with(
        channel_config(Value::Null, 0),
        Some(TOKEN),
        &[("MAILBOX_SLACK_MAX_RATE_LIMIT_RETRIES", "2")],
    );
    assert!(!output.status.success());
    assert_eq!(
        slack
            .log("calls.log")
            .matches("conversations.history")
            .count(),
        3,
        "the first call and two retries"
    );
}

#[test]
fn sigterm_exits_cleanly_mid_watch() {
    let slack = FakeSlack::new();
    slack.fixture("conversations.history", 0, history(json!([])));
    let mut config = channel_config(Value::Null, 0);
    config["interval_ms"] = json!(60_000);
    let child = slack.spawn(config, Some(TOKEN), &[]);
    // Wait until it has baselined, so the signal lands in the idle wait.
    let deadline = Instant::now() + Duration::from_secs(10);
    while !slack.log("calls.log").contains("conversations.history") {
        assert!(Instant::now() < deadline, "the adapter never polled");
        std::thread::sleep(Duration::from_millis(20));
    }
    let pid = i32::try_from(child.id()).unwrap();
    assert!(
        Command::new("kill")
            .args(["-TERM", &pid.to_string()])
            .status()
            .unwrap()
            .success()
    );
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

/// A `--skip` filter (ADR-0029) reaches the binary through its config: a post by
/// the user through the app is read and passed by the cursor but never published,
/// while the same user's typed message still is.
#[test]
fn a_skip_filter_drops_app_posts_and_keeps_typed_ones() {
    let slack = FakeSlack::new();
    slack.fixture(
        "conversations.history",
        0,
        history(json!([{"ts": "1791349480.652779", "user": "U1"}])),
    );
    slack.fixture(
        "conversations.history",
        1,
        history(json!([
            {"ts": "1791349500.000003", "user": "U0AB7RJSQBE", "app_id": "A08SF47R6P4"},
            {"ts": "1791349500.000002", "user": "U0AB7RJSQBE", "client_msg_id": "x"},
            {"ts": "1791349500.000001", "user": "U0AB7RJSQBE", "app_id": "A08SF47R6P4"},
        ])),
    );
    let mut config = channel_config(Value::Null, 2);
    config["skip"] = json!([{"user": "U0AB7RJSQBE", "app": "A08SF47R6P4"}]);

    let output = slack.run(config, Some(TOKEN));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "{stderr}");

    let lines = lines(&output);
    let published: Vec<&Value> = of_type(&lines, "publish")
        .into_iter()
        .map(|p| &p["body"]["ts"])
        .collect();
    assert_eq!(
        published,
        [&json!("1791349500.000002")],
        "only the typed one"
    );
    let last = of_type(&lines, "baseline").last().unwrap()["value"]["last_ts"].clone();
    assert_eq!(
        last, "1791349500.000003",
        "the cursor passes the skipped posts"
    );
    assert_eq!(
        stderr
            .matches("skipped a message a --skip filter matched")
            .count(),
        2,
        "each skip is logged: {stderr}"
    );
}

/// A filter the adapter cannot parse stops it at start: running unfiltered
/// would wake the session for exactly what it asked not to hear.
#[test]
fn an_unparseable_skip_filter_fails_the_config() {
    let slack = FakeSlack::new();
    let mut config = channel_config(Value::Null, 1);
    config["skip"] = json!([{}]);
    let output = slack.run(config, Some(TOKEN));
    assert!(!output.status.success());
    assert!(
        slack.log("calls.log").is_empty(),
        "nothing was polled with a broken filter"
    );
}
