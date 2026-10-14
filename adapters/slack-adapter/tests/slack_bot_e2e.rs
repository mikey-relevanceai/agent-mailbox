//! End-to-end tests for the Slack adapter's bot mode (design/04): the REAL
//! built binary against a FAKE Socket Mode server (a WebSocket on loopback), a
//! FAKE `curl` (the Web API, including `apps.connections.open`) and a FAKE
//! `security` (the Keychain, holding both tokens).
//!
//! The unit tests in `socket.rs`, `bot_messages.rs` and `socket_loop.rs` cover
//! parsing, which messages wake and the publish-then-remember order. These cover
//! what only the process shows: each envelope is acknowledged, one message wakes
//! once across a redelivery, a refresh and a restart, a dead connection is
//! replaced, failures are retried within a budget, and the failures that cannot
//! be retried end the process.

use std::io::Write;
use std::net::SocketAddr;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message as WsMessage;

const BOT_TOKEN: &str = "xoxb-test-bot";
const APP_TOKEN: &str = "xapp-1-A0-test-app";
const CHANNEL: &str = "C0AEL6NV7GC";
const DM: &str = "D0C8ABCDEF";

fn adapter_bin() -> &'static str {
    env!("CARGO_BIN_EXE_mailbox-slack-adapter")
}

/// A fake `curl` for the methods the bot mode calls. `apps.connections.open`
/// must carry the app token and answers with the fake server's URL, after
/// failing with a 503 the first `$FAKE_OPEN_FAILURES` times; everything else
/// must carry the bot token. Logs each method and every argument.
const FAKE_CURL: &str = r#"#!/usr/bin/env bash
set -euo pipefail
dir="${FAKE_SLACK_DIR:?}"
config="$(cat)"
for arg in "$@"; do echo "arg:$arg" >> "$dir/args.log"; done
url="$(printf '%s\n' "$config" | sed -n 's/^url = "\(.*\)"$/\1/p')"
auth="$(printf '%s\n' "$config" | sed -n 's/^header = "\(.*\)"$/\1/p')"
method="${url##*/}"
echo "$method" >> "$dir/calls.log"
want="Authorization: Bearer ${FAKE_BOT_TOKEN:?}"
[ "$method" = "apps.connections.open" ] && want="Authorization: Bearer ${FAKE_APP_TOKEN:?}"
if [ "$auth" != "$want" ]; then
  printf '{"ok":false,"error":"invalid_auth"}'
elif [ "$method" = "apps.connections.open" ]; then
  n=0; [ -f "$dir/opens" ] && n="$(cat "$dir/opens")"
  echo $((n + 1)) > "$dir/opens"
  if [ "$n" -lt "${FAKE_OPEN_FAILURES:-0}" ]; then
    printf '\n@@mailbox-slack-status 503 \n' >&2
    exit 0
  fi
  printf '{"ok":true,"url":"%s"}' "${FAKE_SOCKET_URL:?}"
else
  cat "$dir/$method.json"
fi
printf '\n@@mailbox-slack-status 200 \n' >&2
"#;

/// A fake `security` holding the bot token and, when set, the app token, each
/// under its own service.
const FAKE_SECURITY: &str = r#"#!/usr/bin/env bash
case "$*" in
  "find-generic-password -s agent-mailbox.slack -w") token="${FAKE_KEYCHAIN_BOT:-}" ;;
  "find-generic-password -s agent-mailbox.slack-app-token -w") token="${FAKE_KEYCHAIN_APP:-}" ;;
  *) token="" ;;
esac
if [ -n "$token" ]; then echo "$token"; exit 0; fi
echo "security: SecKeychainSearchCopyNext: The specified item could not be found in the keychain." >&2
exit 44
"#;

/// What the fake server does on one connection, in order.
#[derive(Clone)]
enum Step {
    Send(String),
    /// Read frames until this many acks have arrived on this connection.
    AwaitAcks(usize),
    /// Stop reading, so pings go unanswered, and hold the socket open.
    GoSilent,
}

/// A Socket Mode server on loopback, playing one script per connection and
/// recording every ack it receives.
struct FakeSocketMode {
    addr: SocketAddr,
    acks: Arc<Mutex<Vec<String>>>,
    connections: Arc<Mutex<usize>>,
    _runtime: tokio::runtime::Runtime,
}

impl FakeSocketMode {
    fn start(scripts: Vec<Vec<Step>>) -> Self {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let listener = runtime.block_on(TcpListener::bind("127.0.0.1:0")).unwrap();
        let addr = listener.local_addr().unwrap();
        let acks = Arc::new(Mutex::new(Vec::new()));
        let connections = Arc::new(Mutex::new(0));
        let (server_acks, server_connections) = (acks.clone(), connections.clone());
        runtime.spawn(async move {
            let mut scripts = scripts.into_iter();
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                *server_connections.lock().unwrap() += 1;
                // Past the last script, a connection just says hello.
                let script = scripts.next().unwrap_or_else(|| vec![hello()]);
                let acks = server_acks.clone();
                tokio::spawn(async move {
                    let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
                    let mut seen = 0;
                    for step in script {
                        match step {
                            Step::Send(text) => {
                                if socket.send(WsMessage::Text(text.into())).await.is_err() {
                                    return;
                                }
                            }
                            Step::AwaitAcks(n) => {
                                while seen < n {
                                    match socket.next().await {
                                        Some(Ok(WsMessage::Text(text))) => {
                                            let ack: Value = serde_json::from_str(&text).unwrap();
                                            let id = ack["envelope_id"].as_str().unwrap();
                                            acks.lock().unwrap().push(id.to_string());
                                            seen += 1;
                                        }
                                        Some(Ok(_)) => {}
                                        _ => return,
                                    }
                                }
                            }
                            Step::GoSilent => {
                                tokio::time::sleep(Duration::from_secs(60)).await;
                                return;
                            }
                        }
                    }
                    // Keep answering pings and recording acks until the adapter goes.
                    while let Some(Ok(message)) = socket.next().await {
                        if let WsMessage::Text(text) = message {
                            let ack: Value = serde_json::from_str(&text).unwrap();
                            let id = ack["envelope_id"].as_str().unwrap().to_string();
                            acks.lock().unwrap().push(id);
                        }
                    }
                });
            }
        });
        Self {
            addr,
            acks,
            connections,
            _runtime: runtime,
        }
    }

    fn url(&self) -> String {
        format!("ws://{}/link/?ticket=secret-ticket", self.addr)
    }

    fn acks(&self) -> Vec<String> {
        self.acks.lock().unwrap().clone()
    }

    fn connections(&self) -> usize {
        *self.connections.lock().unwrap()
    }
}

fn hello() -> Step {
    Step::Send(
        json!({"type": "hello", "num_connections": 1,
            "debug_info": {"approximate_connection_time": 18060},
            "connection_info": {"app_id": "A0"}})
        .to_string(),
    )
}

fn disconnect(reason: &str) -> Step {
    Step::Send(json!({"type": "disconnect", "reason": reason}).to_string())
}

/// An `events_api` envelope, shaped like the ones measured live (design/04).
fn envelope(envelope_id: &str, retry_attempt: u64, event: Value, event_id: &str) -> Step {
    Step::Send(
        json!({
            "envelope_id": envelope_id, "type": "events_api",
            "accepts_response_payload": false, "retry_attempt": retry_attempt,
            "retry_reason": if retry_attempt > 0 { "timeout" } else { "" },
            "payload": {
                "token": "legacy-verification-token", "team_id": "T0", "api_app_id": "A0",
                "type": "event_callback", "event_id": event_id, "event_time": 1_791_935_308,
                "event": event,
            },
        })
        .to_string(),
    )
}

fn mention(envelope_id: &str, event_id: &str, ts: &str) -> Step {
    mention_retry(envelope_id, event_id, ts, 0)
}

fn dm(envelope_id: &str, event_id: &str, ts: &str, extra: Value) -> Step {
    let mut event = json!({"type": "message", "channel_type": "im", "channel": DM,
        "user": "U0AB7RJSQBE", "ts": ts, "text": "secret plans"});
    for (key, value) in extra.as_object().unwrap() {
        event[key] = value.clone();
    }
    envelope(envelope_id, 0, event, event_id)
}

fn mention_retry(envelope_id: &str, event_id: &str, ts: &str, retry_attempt: u64) -> Step {
    envelope(
        envelope_id,
        retry_attempt,
        json!({"type": "app_mention", "user": "U0AB7RJSQBE", "ts": ts,
            "channel": CHANNEL, "event_ts": ts, "text": "<@U0C6VHAA4LX> secret plans"}),
        event_id,
    )
}

struct Harness {
    dir: TempDir,
    curl: PathBuf,
    security: PathBuf,
}

impl Harness {
    fn new() -> Self {
        let dir = TempDir::new().unwrap();
        let curl = script(dir.path(), "fake-curl.sh", FAKE_CURL);
        let security = script(dir.path(), "fake-security.sh", FAKE_SECURITY);
        let fixture = |method: &str, body: Value| {
            std::fs::write(dir.path().join(format!("{method}.json")), body.to_string()).unwrap();
        };
        fixture(
            "auth.test",
            json!({"ok": true, "url": "https://tryrelevance.slack.com/", "user_id": "U0C6VHAA4LX", "bot_id": "B0C74MURRK5"}),
        );
        fixture(
            "conversations.info",
            json!({"ok": true, "channel": {"name": "mikeys-private-test-arena"}}),
        );
        fixture(
            "users.info",
            json!({"ok": true, "user": {"profile": {"display_name": "Mikey Hudson"}}}),
        );
        Self {
            dir,
            curl,
            security,
        }
    }

    fn spawn(&self, config: Value, server: &FakeSocketMode, env: &[(&str, &str)]) -> Child {
        let mut command = Command::new(adapter_bin());
        command
            .env("MAILBOX_SLACK_CURL_BIN", &self.curl)
            .env("MAILBOX_SLACK_SECURITY_BIN", &self.security)
            .env("FAKE_SLACK_DIR", self.dir.path())
            .env("FAKE_BOT_TOKEN", BOT_TOKEN)
            .env("FAKE_APP_TOKEN", APP_TOKEN)
            .env("FAKE_KEYCHAIN_BOT", BOT_TOKEN)
            .env("FAKE_KEYCHAIN_APP", APP_TOKEN)
            .env("FAKE_SOCKET_URL", server.url())
            .env("MAILBOX_SLACK_RECONNECT_BACKOFF_MS", "10")
            .env("RUST_LOG", "info")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        command.envs(env.iter().copied());
        let mut child = command.spawn().unwrap();
        let mut stdin = child.stdin.take().unwrap();
        writeln!(stdin, "{config}").unwrap();
        drop(stdin);
        child
    }

    fn log(&self, name: &str) -> String {
        std::fs::read_to_string(self.dir.path().join(name)).unwrap_or_default()
    }
}

fn script(dir: &Path, name: &str, body: &str) -> PathBuf {
    let path = dir.join(name);
    std::fs::write(&path, body).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    path
}

fn config(baseline: Value) -> Value {
    json!({"kind": "slack-bot", "topic": "slack.bot", "baseline": baseline})
}

/// Wait until `done` holds, or fail naming what never happened.
fn wait_until(what: &str, done: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(15);
    while !done() {
        assert!(Instant::now() < deadline, "timed out waiting: {what}");
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn terminate(child: Child) -> Output {
    let status = Command::new("kill")
        .args(["-TERM", &child.id().to_string()])
        .status()
        .unwrap();
    assert!(status.success());
    child.wait_with_output().unwrap()
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

fn event_ids(publishes: &[&Value]) -> Vec<String> {
    publishes
        .iter()
        .map(|p| p["body"]["event_id"].as_str().unwrap().to_string())
        .collect()
}

fn publishes(output: &Output) -> Vec<String> {
    event_ids(&of_type(&lines(output), "publish"))
}

#[test]
fn each_message_wakes_once_and_every_envelope_is_acknowledged() {
    let server = FakeSocketMode::start(vec![
        vec![
            hello(),
            mention("env-1", "Ev1", "1791935308.038199"),
            envelope("env-2", 0, json!({"type": "reaction_added"}), "Ev9"),
            // Slack redelivers Ev1, as if the first ack had been lost.
            mention_retry("env-3", "Ev1", "1791935308.038199", 1),
            Step::AwaitAcks(3),
            disconnect("refresh_requested"),
        ],
        vec![
            hello(),
            dm("env-4", "Ev2", "1791935316.656669", json!({})),
            // An edit of that DM: nobody said anything new.
            dm(
                "env-5",
                "Ev3",
                "1791935320.000001",
                json!({"subtype": "message_changed", "hidden": true}),
            ),
            Step::AwaitAcks(2),
        ],
    ]);
    let harness = Harness::new();
    let child = harness.spawn(config(Value::Null), &server, &[]);
    wait_until("all five envelopes acknowledged", || {
        server.acks().len() == 5
    });
    let output = terminate(child);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "{stderr}");

    assert_eq!(server.acks(), ["env-1", "env-2", "env-3", "env-4", "env-5"]);
    assert_eq!(server.connections(), 2, "a refresh opens a new connection");

    let lines = lines(&output);
    let published = of_type(&lines, "publish");
    assert_eq!(
        event_ids(&published),
        ["Ev1", "Ev2"],
        "the redelivery and the edit wake nobody"
    );
    assert_eq!(published[0]["topic"], "slack.bot");
    assert_eq!(
        published[0]["subject"]["text"],
        "mention from Mikey Hudson in #mikeys-private-test-arena"
    );
    assert_eq!(
        published[0]["subject"]["link"],
        format!("https://tryrelevance.slack.com/archives/{CHANNEL}/p1791935308038199")
    );
    assert_eq!(published[1]["body"]["kind"], "slack_dm");
    assert_eq!(published[1]["subject"]["text"], "DM from Mikey Hudson");
    assert!(
        !output_text(&output).contains("secret plans"),
        "message text never leaves Slack"
    );
    assert!(
        !output_text(&output).contains("secret-ticket"),
        "the connection URL's ticket is never logged"
    );

    let last_baseline = of_type(&lines, "baseline").last().unwrap()["value"].clone();
    assert_eq!(
        last_baseline,
        json!({"seen_messages": [
            format!("{CHANNEL}/1791935308.038199"),
            format!("{DM}/1791935316.656669"),
        ]})
    );
}

#[test]
fn a_redelivery_after_a_restart_is_dropped_by_the_injected_baseline() {
    let server = FakeSocketMode::start(vec![vec![
        hello(),
        mention_retry("env-1", "Ev1", "1791935308.038199", 2),
        Step::AwaitAcks(1),
    ]]);
    let harness = Harness::new();
    let baseline = json!({"seen_messages": [format!("{CHANNEL}/1791935308.038199")]});
    let child = harness.spawn(config(baseline), &server, &[]);
    wait_until("the redelivery acknowledged", || server.acks().len() == 1);
    let output = terminate(child);
    assert!(publishes(&output).is_empty());
}

/// A laptop that sleeps keeps a socket that looks open and carries nothing. The
/// adapter notices the silence and connects again.
#[test]
fn a_silent_connection_is_replaced() {
    let server = FakeSocketMode::start(vec![
        vec![hello(), Step::GoSilent],
        vec![
            hello(),
            mention("env-1", "Ev1", "1791935308.038199"),
            Step::AwaitAcks(1),
        ],
    ]);
    let harness = Harness::new();
    let child = harness.spawn(
        config(Value::Null),
        &server,
        &[("MAILBOX_SLACK_SOCKET_PING_MS", "100")],
    );
    wait_until("the mention on the second connection", || {
        server.acks().len() == 1
    });
    let output = terminate(child);
    assert_eq!(publishes(&output), ["Ev1"]);
    assert!(String::from_utf8_lossy(&output.stderr).contains("no frame from Slack"));
}

/// A filtered message and a malformed one are acknowledged like any other, so
/// Slack does not redeliver them, and neither is published.
#[test]
fn filtered_and_malformed_messages_are_acknowledged_and_not_published() {
    let server = FakeSocketMode::start(vec![vec![
        hello(),
        dm("env-1", "Ev1", "1791935316.656669", json!({})),
        mention("env-2", "Ev2", "not-a-ts"),
        dm(
            "env-3",
            "Ev3",
            "1791935320.000001",
            json!({"user": "U0OTHERPERSON"}),
        ),
        Step::AwaitAcks(3),
    ]]);
    let harness = Harness::new();
    let config = json!({"kind": "slack-bot", "topic": "slack.bot",
        "skip": [{"user": "U0AB7RJSQBE"}]});
    let child = harness.spawn(config, &server, &[]);
    wait_until("all three acknowledged", || server.acks().len() == 3);
    let output = terminate(child);
    assert_eq!(
        publishes(&output),
        ["Ev3"],
        "the filtered DM and the malformed mention are dropped"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("malformed field"), "{stderr}");
}

/// The network being down is retried, and a connection that works afterwards
/// carries on as if nothing happened.
#[test]
fn failed_connections_are_retried_until_one_works() {
    let server = FakeSocketMode::start(vec![vec![
        hello(),
        mention("env-1", "Ev1", "1791935308.038199"),
        Step::AwaitAcks(1),
    ]]);
    let harness = Harness::new();
    let child = harness.spawn(config(Value::Null), &server, &[("FAKE_OPEN_FAILURES", "2")]);
    wait_until("the mention after two failures", || {
        server.acks().len() == 1
    });
    let output = terminate(child);
    assert_eq!(publishes(&output), ["Ev1"]);
    assert_eq!(harness.log("opens").trim(), "3");
}

/// A connection Slack drops before saying hello never worked, so it counts
/// against the budget like any other failure instead of reconnecting at once,
/// forever.
#[test]
fn disconnects_before_hello_spend_the_failure_budget() {
    let refused = || vec![disconnect("refresh_requested")];
    let server = FakeSocketMode::start(vec![refused(), refused(), refused(), refused()]);
    let harness = Harness::new();
    let output = harness
        .spawn(
            config(Value::Null),
            &server,
            &[("MAILBOX_SLACK_MAX_TRANSIENT_FAILURES", "3")],
        )
        .wait_with_output()
        .unwrap();
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("disconnected before hello"), "{stderr}");
    assert_eq!(server.connections(), 3);
}

#[test]
fn socket_mode_turned_off_ends_the_adapter() {
    let server = FakeSocketMode::start(vec![vec![hello(), disconnect("link_disabled")]]);
    let harness = Harness::new();
    let output = harness
        .spawn(config(Value::Null), &server, &[])
        .wait_with_output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("Socket Mode was turned off"));
}

#[test]
fn a_missing_app_token_fails_at_start_naming_the_command_that_stores_it() {
    let server = FakeSocketMode::start(Vec::new());
    let harness = Harness::new();
    let output = harness
        .spawn(config(Value::Null), &server, &[("FAKE_KEYCHAIN_APP", "")])
        .wait_with_output()
        .unwrap();
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("security add-generic-password -U -s agent-mailbox.slack-app-token"),
        "{stderr}"
    );
    assert_eq!(server.connections(), 0);
}

#[test]
fn a_rejected_app_token_ends_the_adapter_and_no_token_reaches_argv_or_logs() {
    let server = FakeSocketMode::start(Vec::new());
    let harness = Harness::new();
    let output = harness
        .spawn(
            config(Value::Null),
            &server,
            &[("FAKE_APP_TOKEN", "xapp-the-one-slack-wants")],
        )
        .wait_with_output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("invalid_auth"));
    let args = harness.log("args.log");
    for secret in [BOT_TOKEN, APP_TOKEN, "secret-ticket"] {
        assert!(!args.contains(secret), "{secret} reached curl's argv");
        assert!(
            !output_text(&output).contains(secret),
            "{secret} reached the output"
        );
    }
}

fn output_text(output: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

/// The bot's own DM posts, in both forms measured on 2026-10-15, never wake:
/// the ids that identify them come from the fake `auth.test`, so this also
/// pins that they are read from it.
#[test]
fn the_bots_own_posts_are_acknowledged_and_never_published() {
    let server = FakeSocketMode::start(vec![vec![
        hello(),
        dm(
            "env-1",
            "Ev1",
            "1791935500.000001",
            json!({"user": "U0C6VHAA4LX", "bot_id": "B0C74MURRK5", "app_id": "A0C7AJ9AMUJ"}),
        ),
        dm(
            "env-2",
            "Ev2",
            "1791935500.000002",
            json!({"user": null, "subtype": "bot_message", "bot_id": "B0C74MURRK5",
                "app_id": "A0C7AJ9AMUJ"}),
        ),
        dm("env-3", "Ev3", "1791935500.000003", json!({})),
        Step::AwaitAcks(3),
    ]]);
    let harness = Harness::new();
    let child = harness.spawn(config(Value::Null), &server, &[]);
    wait_until("all three acknowledged", || server.acks().len() == 3);
    let output = terminate(child);
    assert_eq!(publishes(&output), ["Ev3"], "only the person's DM wakes");
}

#[test]
fn a_rejected_bot_token_ends_the_adapter_before_it_connects() {
    let server = FakeSocketMode::start(Vec::new());
    let harness = Harness::new();
    let output = harness
        .spawn(
            config(Value::Null),
            &server,
            &[("FAKE_BOT_TOKEN", "xoxb-the-one-slack-wants")],
        )
        .wait_with_output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("invalid_auth"));
    assert_eq!(server.connections(), 0);
}
