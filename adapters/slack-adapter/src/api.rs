//! The Slack Web API seam: one call is one HTTPS POST, made by `curl`.
//!
//! # Why `curl` (ADR-0027)
//!
//! It keeps the adapter free of an HTTP and TLS stack, the same way `github-pr`
//! leaves HTTP to `gh`, and it keeps the test fake a shell script. The token goes
//! to `curl` on **stdin** as a config file (`-K -`), never as an argument, so it
//! cannot be read from the process table.
//!
//! # Error classes
//!
//! A [`SlackError`] says how the run loop should react, not just what failed:
//! [`SlackError::RateLimited`] backs off and retries, [`SlackError::Transient`]
//! skips the poll, [`SlackError::NotInChannel`] is the watcher's cue to join, and
//! everything else is fatal so the supervisor surfaces it.

use std::time::Duration;

use serde_json::Value;
use tokio::io::AsyncWriteExt;

use crate::token::Token;

/// Env var overriding the `curl` binary, so tests can point at a fake Slack.
pub const ENV_CURL_BIN: &str = "MAILBOX_SLACK_CURL_BIN";

const DEFAULT_CURL_BIN: &str = "curl";

const API_BASE: &str = "https://slack.com/api/";

/// A Slack call that hangs must not stall the poll loop indefinitely.
const REQUEST_TIMEOUT_SECS: &str = "30";

/// Marks the line `curl` writes to stderr after the transfer, carrying the HTTP
/// status and any `Retry-After`, so it cannot be confused with a curl error.
const STATUS_MARKER: &str = "@@mailbox-slack-status";

#[derive(Debug, thiserror::Error)]
pub enum SlackError {
    /// The token is wrong, revoked, or its account is gone. Fatal.
    #[error("Slack rejected the token ({0}); store a valid one in the Keychain")]
    Auth(String),
    /// The bot is not a member of the channel. Not fatal by itself: the watcher
    /// tries to join, and only a failed join becomes [`SlackError::Access`].
    #[error("the Slack bot is not a member of the channel")]
    NotInChannel,
    /// The token cannot read this channel or thread. Fatal.
    #[error("Slack refused access: {0}")]
    Access(String),
    /// Slack said slow down. `retry_after` is its `Retry-After`, when it gave one.
    #[error("Slack rate-limited the call")]
    RateLimited { retry_after: Option<Duration> },
    /// A network failure, a 5xx, or a reply that is not the JSON Slack sends.
    /// Skips the poll; only a persistent streak is fatal.
    #[error("Slack call failed transiently: {0}")]
    Transient(String),
    /// Any other Slack error. Fatal, so a broken watch surfaces instead of spinning.
    #[error("Slack call failed: {0}")]
    Failed(String),
    #[error("could not run {bin:?} to call Slack: {source}")]
    Spawn {
        bin: String,
        #[source]
        source: std::io::Error,
    },
}

/// One Slack Web API method call. Generic over the transport so the poll logic
/// runs against an in-memory fake in unit tests.
pub(crate) trait SlackApi {
    /// Call `method` with form `params`, returning the reply when Slack said
    /// `"ok": true`.
    async fn call(&self, method: &str, params: &[(&str, &str)]) -> Result<Value, SlackError>;
}

/// The real transport: `curl` against slack.com.
pub struct CurlSlack {
    bin: String,
    token: Token,
}

impl CurlSlack {
    pub fn new(token: Token) -> Self {
        let bin = std::env::var(ENV_CURL_BIN)
            .ok()
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| DEFAULT_CURL_BIN.to_string());
        Self { bin, token }
    }
}

impl SlackApi for CurlSlack {
    async fn call(&self, method: &str, params: &[(&str, &str)]) -> Result<Value, SlackError> {
        let mut child = tokio::process::Command::new(&self.bin)
            .args(["-sS", "--max-time", REQUEST_TIMEOUT_SECS, "-K", "-"])
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .map_err(|source| SlackError::Spawn {
                bin: self.bin.clone(),
                source,
            })?;
        let config = curl_config(method, params, &self.token);
        if let Some(mut stdin) = child.stdin.take() {
            stdin
                .write_all(config.as_bytes())
                .await
                .map_err(|err| SlackError::Transient(format!("could not write to curl: {err}")))?;
        }
        let output = child
            .wait_with_output()
            .await
            .map_err(|err| SlackError::Transient(format!("curl did not finish: {err}")))?;
        let stderr = String::from_utf8_lossy(&output.stderr);
        if !output.status.success() {
            // curl's own failures (DNS, connect, timeout) are the laptop being
            // offline far more often than anything wrong with the watch.
            return Err(SlackError::Transient(first_line(&stderr)));
        }
        let (status, retry_after) = parse_status(&stderr)
            .ok_or_else(|| SlackError::Transient("curl reported no HTTP status".to_string()))?;
        interpret(status, retry_after, &output.stdout)
    }
}

/// The curl config for one call. Every value is form-encoded, and the token is
/// header-safe by construction ([`crate::token`]), so nothing here needs quoting
/// beyond the surrounding `"`.
fn curl_config(method: &str, params: &[(&str, &str)], token: &Token) -> String {
    let form = params
        .iter()
        .map(|(key, value)| format!("{}={}", form_encode(key), form_encode(value)))
        .collect::<Vec<_>>()
        .join("&");
    format!(
        "url = \"{API_BASE}{method}\"\n\
         header = \"Authorization: Bearer {}\"\n\
         data = \"{form}\"\n\
         write-out = \"%{{stderr}}\\n{STATUS_MARKER} %{{http_code}} %header{{retry-after}}\\n\"\n",
        token.expose()
    )
}

/// Percent-encode for `application/x-www-form-urlencoded`. Slack cursors are
/// base64 and can carry `=` and `+`, which must not be read as delimiters.
fn form_encode(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            out.push(byte as char);
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

/// Find the status line curl wrote after the transfer.
fn parse_status(stderr: &str) -> Option<(u16, Option<Duration>)> {
    let line = stderr
        .lines()
        .rev()
        .find_map(|line| line.strip_prefix(STATUS_MARKER))?;
    let mut fields = line.split_whitespace();
    let status = fields.next()?.parse().ok()?;
    let retry_after = fields
        .next()
        .and_then(|secs| secs.parse().ok())
        .map(Duration::from_secs);
    Some((status, retry_after))
}

/// Turn an HTTP reply into the JSON or the error class it represents.
fn interpret(status: u16, retry_after: Option<Duration>, body: &[u8]) -> Result<Value, SlackError> {
    if status == 429 {
        return Err(SlackError::RateLimited { retry_after });
    }
    if status >= 500 {
        return Err(SlackError::Transient(format!(
            "Slack returned HTTP {status}"
        )));
    }
    if status != 200 {
        return Err(SlackError::Failed(format!("Slack returned HTTP {status}")));
    }
    let reply: Value = serde_json::from_slice(body)
        .map_err(|err| SlackError::Transient(format!("Slack reply was not JSON: {err}")))?;
    if reply.get("ok").and_then(Value::as_bool) == Some(true) {
        return Ok(reply);
    }
    let code = reply
        .get("error")
        .and_then(Value::as_str)
        .unwrap_or("unknown_error");
    Err(classify(code, retry_after))
}

/// Classify a Slack `error` code. The lists are Slack's documented codes for the
/// methods this adapter calls; an unlisted code is fatal so it gets noticed.
pub(crate) fn classify(code: &str, retry_after: Option<Duration>) -> SlackError {
    match code {
        "not_authed" | "invalid_auth" | "account_inactive" | "token_revoked" | "token_expired"
        | "no_permission" => SlackError::Auth(code.to_string()),
        "not_in_channel" => SlackError::NotInChannel,
        "missing_scope"
        | "channel_not_found"
        | "thread_not_found"
        | "method_not_supported_for_channel_type"
        | "is_archived" => SlackError::Access(code.to_string()),
        "ratelimited" => SlackError::RateLimited { retry_after },
        "internal_error" | "fatal_error" | "service_unavailable" | "request_timeout" => {
            SlackError::Transient(code.to_string())
        }
        other => SlackError::Failed(other.to_string()),
    }
}

/// The first non-empty line of `text`, so an error never dumps curl's whole stderr.
fn first_line(text: &str) -> String {
    text.lines()
        .map(str::trim)
        .find(|line| !line.is_empty() && !line.starts_with(STATUS_MARKER))
        .unwrap_or("curl exited with an error")
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn form_encoding_protects_cursor_delimiters() {
        assert_eq!(
            form_encode("dXNlcjpVMDYxTkZUVDI="),
            "dXNlcjpVMDYxTkZUVDI%3D"
        );
        assert_eq!(form_encode("1791349480.652779"), "1791349480.652779");
        assert_eq!(form_encode("a b&c"), "a%20b%26c");
    }

    #[test]
    fn config_carries_the_token_in_a_header_and_params_in_the_body() {
        let token = crate::token::tests_support::token("xoxb-test");
        let config = curl_config(
            "conversations.history",
            &[("channel", "C1"), ("oldest", "1.000001")],
            &token,
        );
        assert!(config.contains("url = \"https://slack.com/api/conversations.history\""));
        assert!(config.contains("header = \"Authorization: Bearer xoxb-test\""));
        assert!(config.contains("data = \"channel=C1&oldest=1.000001\""));
    }

    #[test]
    fn status_line_is_found_after_other_stderr() {
        let stderr = "curl: (52) warning\n@@mailbox-slack-status 429 30\n";
        assert_eq!(
            parse_status(stderr),
            Some((429, Some(Duration::from_secs(30))))
        );
        assert_eq!(
            parse_status("\n@@mailbox-slack-status 200 \n"),
            Some((200, None))
        );
        assert_eq!(parse_status("nothing here"), None);
    }

    #[test]
    fn replies_map_onto_error_classes() {
        assert!(interpret(200, None, br#"{"ok":true,"messages":[]}"#).is_ok());
        assert!(matches!(
            interpret(429, Some(Duration::from_secs(5)), b""),
            Err(SlackError::RateLimited {
                retry_after: Some(_)
            })
        ));
        assert!(matches!(
            interpret(503, None, b""),
            Err(SlackError::Transient(_))
        ));
        assert!(matches!(
            interpret(200, None, b"<html>"),
            Err(SlackError::Transient(_))
        ));
        assert!(matches!(
            interpret(200, None, br#"{"ok":false,"error":"invalid_auth"}"#),
            Err(SlackError::Auth(_))
        ));
        assert!(matches!(
            interpret(200, None, br#"{"ok":false,"error":"not_in_channel"}"#),
            Err(SlackError::NotInChannel)
        ));
        assert!(matches!(
            interpret(200, None, br#"{"ok":false,"error":"channel_not_found"}"#),
            Err(SlackError::Access(_))
        ));
        assert!(matches!(
            interpret(200, None, br#"{"ok":false,"error":"something_new"}"#),
            Err(SlackError::Failed(_))
        ));
    }
}
