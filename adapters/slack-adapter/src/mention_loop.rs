//! The mention watch's process: hold a Socket Mode connection, publish each
//! `app_mention`, acknowledge every envelope, and reconnect (design/04).
//!
//! Unlike the channel and thread watches this mode does not poll. Slack pushes
//! events down a WebSocket the adapter opened, so a mention wakes in about the
//! time it takes to cross the socket. The connection is outbound only, which is
//! why it works from a laptop with no public URL.
//!
//! # Order: publish, remember, acknowledge
//!
//! A mention is published, then its event id is persisted as the baseline,
//! then its envelope is acknowledged. A crash before the ack makes Slack
//! redeliver, which at worst wakes twice; acknowledging first would let a
//! crash lose the mention outright. The same trade as the poll modes:
//! re-fire rather than drop.
//!
//! # Connections end
//!
//! Slack refreshes a connection every few hours, says so with a `disconnect`
//! frame, and documents that events "may" be lost while none is open. The
//! adapter reconnects at once on a refresh and after a short backoff on a lost
//! connection. It pings every [`DEFAULT_PING_MS`] and treats a connection with
//! no frame for three intervals as dead, which is how a laptop waking from sleep
//! notices that its old socket is gone.

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures_util::{SinkExt, StreamExt};
use mailbox_protocol::{
    AdapterId, Baseline as BaselineMsg, Message, Publish, SlackFilters, Topic, slack_mentions_topic,
};
use serde::Deserialize;
use serde_json::Value;
use tokio::io::Stdout;
use tokio::net::TcpStream;
use tokio::signal::unix::Signal;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use tracing::{debug, info, warn};

use crate::api::{CurlSlack, SlackApi, SlackError};
use crate::mentions::{Disposition, Mention, Mentions, Seen, identify};
use crate::socket::{DisconnectReason, Frame, FrameError, ack};
use crate::token::{self, Credential};
use crate::{ADAPTER_ID, AdapterError, Limits, env_u64, write_message};

/// How often to ping Slack. Slack's docs name no keepalive; this is a chosen
/// value, short enough that a dead socket is noticed within a couple of minutes.
const DEFAULT_PING_MS: u64 = 30_000;
const ENV_PING_MS: &str = "MAILBOX_SLACK_SOCKET_PING_MS";

/// A connection that sends nothing, not even a pong, for this many ping
/// intervals is treated as dead.
const IDLE_PINGS: u32 = 3;

/// First wait before reconnecting after a failure; doubled per consecutive
/// failure up to [`MAX_BACKOFF`].
const DEFAULT_RECONNECT_BACKOFF_MS: u64 = 1_000;
const ENV_RECONNECT_BACKOFF_MS: &str = "MAILBOX_SLACK_RECONNECT_BACKOFF_MS";
const MAX_BACKOFF: Duration = Duration::from_secs(60);

/// Opening the WebSocket must not hang the adapter.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

/// The mention watch's config line (`kind: "mentions"`).
#[derive(Debug, Deserialize)]
pub struct MentionConfig {
    pub topic: String,
    #[serde(default)]
    pub skip: SlackFilters,
    #[serde(default)]
    pub baseline: Value,
}

impl MentionConfig {
    /// The topic, checked against the one this mode publishes on.
    pub fn topic(&self) -> Result<Topic, AdapterError> {
        let expected = slack_mentions_topic();
        if self.topic != expected.as_str() {
            return Err(AdapterError::TopicMismatch {
                topic: self.topic.clone(),
                expected: expected.as_str().to_string(),
            });
        }
        Ok(expected)
    }
}

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// How one connection ended.
enum End {
    Signalled(&'static str),
    /// Slack asked for a new connection. Not a failure.
    Refresh(String),
    /// The connection died. `healthy` when Slack had said hello on it, so the
    /// failure streak restarts.
    Lost {
        reason: String,
        healthy: bool,
    },
}

/// Why opening a connection failed.
enum OpenError {
    /// Worth retrying: the network, a Slack 5xx, a rate limit, a WebSocket that
    /// would not open.
    Transient(String),
    Fatal(SlackError),
}

pub async fn run(
    config: MentionConfig,
    topic: Topic,
    sigterm: &mut Signal,
    sigint: &mut Signal,
) -> Result<(), AdapterError> {
    let limits = Limits::from_env();
    let ping = Duration::from_millis(env_u64(ENV_PING_MS, DEFAULT_PING_MS).max(1));
    let first_backoff = Duration::from_millis(env_u64(
        ENV_RECONNECT_BACKOFF_MS,
        DEFAULT_RECONNECT_BACKOFF_MS,
    ));
    let mut stdout = tokio::io::stdout();

    // Both tokens are read up front, so a missing one fails the watch at start
    // with the command that stores it.
    let opener = CurlSlack::new(token::load(Credential::App).await?);
    let web = CurlSlack::new(token::load(Credential::Bot).await?);

    let seen = injected_seen(config.baseline);
    let skip = config.skip.to_string();
    let skip_count = config.skip.iter().count();
    info!(
        remembered = seen.as_ref().map_or(0, Seen::remembered),
        skip_count,
        skip = %skip,
        ping_ms = ping.as_millis() as u64,
        "slack mention watch started"
    );

    let mut mentions: Option<Mentions<CurlSlack>> = None;
    let mut seen = seen.unwrap_or_default();
    let mut failures: u64 = 0;
    loop {
        let opened = tokio::select! {
            _ = sigterm.recv() => return stopped("SIGTERM"),
            _ = sigint.recv() => return stopped("SIGINT"),
            opened = open(&opener, &web, mentions.is_none()) => opened,
        };
        let failure = match opened {
            Ok((socket, identity)) => {
                if let Some(identity) = identity {
                    info!(
                        workspace = %identity.workspace_url,
                        bot_user = identity.bot_user.as_deref().unwrap_or(""),
                        "identified the bot"
                    );
                    mentions = Some(Mentions::new(
                        web.clone(),
                        identity.workspace_url,
                        config.skip.clone(),
                        std::mem::take(&mut seen),
                    ));
                }
                let watch = mentions
                    .as_mut()
                    .expect("identified before the first connection");
                match serve(socket, watch, &mut stdout, &topic, ping, sigterm, sigint).await? {
                    End::Signalled(name) => return stopped(name),
                    End::Refresh(reason) => {
                        info!(reason = %reason, "Slack refreshed the connection; reconnecting");
                        failures = 0;
                        continue;
                    }
                    End::Lost { reason, healthy } => {
                        if healthy {
                            failures = 0;
                        }
                        reason
                    }
                }
            }
            Err(OpenError::Fatal(err)) => return Err(err.into()),
            Err(OpenError::Transient(reason)) => reason,
        };
        failures += 1;
        let wait = backoff(first_backoff, failures);
        warn!(
            consecutive_failures = failures,
            max = limits.max_transient,
            wait_ms = wait.as_millis() as u64,
            reason = %failure,
            "lost the Socket Mode connection; reconnecting"
        );
        if failures >= limits.max_transient {
            return Err(SlackError::Transient(format!(
                "no Socket Mode connection after {failures} tries: {failure}"
            ))
            .into());
        }
        tokio::select! {
            _ = sigterm.recv() => return stopped("SIGTERM"),
            _ = sigint.recv() => return stopped("SIGINT"),
            _ = tokio::time::sleep(wait) => {}
        }
    }
}

fn stopped(signal: &str) -> Result<(), AdapterError> {
    info!(signal, "received signal; exiting cleanly");
    Ok(())
}

fn backoff(first: Duration, failures: u64) -> Duration {
    let doublings = u32::try_from(failures.saturating_sub(1))
        .unwrap_or(u32::MAX)
        .min(16);
    first.saturating_mul(1 << doublings).min(MAX_BACKOFF)
}

/// Ask Slack for a connection URL with the app token and open it. On the first
/// connection, also check the bot token and learn the workspace URL.
async fn open(
    opener: &CurlSlack,
    web: &CurlSlack,
    identify_bot: bool,
) -> Result<(Socket, Option<crate::mentions::Identity>), OpenError> {
    let identity = if identify_bot {
        Some(identify(web).await.map_err(classify)?)
    } else {
        None
    };
    let reply = opener
        .call("apps.connections.open", &[])
        .await
        .map_err(classify)?;
    let url = reply
        .get("url")
        .and_then(Value::as_str)
        .ok_or_else(|| OpenError::Transient("apps.connections.open returned no url".into()))?;
    // The URL carries a one-time ticket, so it is never logged.
    if !acceptable_url(url) {
        return Err(OpenError::Fatal(SlackError::Failed(
            "apps.connections.open returned a URL that is not wss://".into(),
        )));
    }
    let (socket, _response) =
        match tokio::time::timeout(CONNECT_TIMEOUT, tokio_tungstenite::connect_async(url)).await {
            Ok(Ok(connected)) => connected,
            Ok(Err(err)) => {
                return Err(OpenError::Transient(format!(
                    "could not open the WebSocket: {}",
                    without_url(&err.to_string())
                )));
            }
            Err(_) => {
                return Err(OpenError::Transient(
                    "timed out opening the WebSocket".into(),
                ));
            }
        };
    debug!("opened a Socket Mode WebSocket");
    Ok((socket, identity))
}

fn classify(err: SlackError) -> OpenError {
    match err {
        SlackError::Transient(reason) => OpenError::Transient(reason),
        SlackError::RateLimited { .. } => OpenError::Transient("rate-limited".into()),
        // Listed, not wildcarded, as in the poll loop.
        err @ (SlackError::Auth(_)
        | SlackError::NotInChannel
        | SlackError::Access(_)
        | SlackError::Failed(_)
        | SlackError::Spawn { .. }) => OpenError::Fatal(err),
    }
}

/// Slack hands out `wss://` URLs. Plain `ws://` is accepted only to loopback,
/// which is where the tests' fake Slack listens.
fn acceptable_url(url: &str) -> bool {
    url.starts_with("wss://")
        || url.starts_with("ws://127.0.0.1:")
        || url.starts_with("ws://localhost:")
}

/// An error string with any URL cut out, so a ticket cannot reach a log.
fn without_url(text: &str) -> String {
    text.split_whitespace()
        .map(|word| if word.contains("://") { "<url>" } else { word })
        .collect::<Vec<_>>()
        .join(" ")
}

/// Read one connection until it ends.
async fn serve<A: SlackApi>(
    mut socket: Socket,
    mentions: &mut Mentions<A>,
    stdout: &mut Stdout,
    topic: &Topic,
    ping: Duration,
    sigterm: &mut Signal,
    sigint: &mut Signal,
) -> Result<End, AdapterError> {
    let mut healthy = false;
    let mut last_frame = Instant::now();
    let mut pinger = tokio::time::interval(ping);
    pinger.tick().await;
    loop {
        let incoming = tokio::select! {
            _ = sigterm.recv() => return Ok(close(socket, End::Signalled("SIGTERM")).await),
            _ = sigint.recv() => return Ok(close(socket, End::Signalled("SIGINT")).await),
            _ = pinger.tick() => {
                if last_frame.elapsed() > ping * IDLE_PINGS {
                    return Ok(End::Lost {
                        reason: format!("no frame from Slack in {}s", last_frame.elapsed().as_secs()),
                        healthy,
                    });
                }
                if let Err(err) = socket.send(WsMessage::Ping(Vec::new().into())).await {
                    return Ok(End::Lost { reason: format!("could not ping: {err}"), healthy });
                }
                continue;
            }
            incoming = socket.next() => incoming,
        };
        last_frame = Instant::now();
        let text = match incoming {
            None => {
                return Ok(End::Lost {
                    reason: "Slack closed the connection".into(),
                    healthy,
                });
            }
            Some(Err(err)) => {
                return Ok(End::Lost {
                    reason: format!("the WebSocket failed: {}", without_url(&err.to_string())),
                    healthy,
                });
            }
            Some(Ok(WsMessage::Text(text))) => text,
            Some(Ok(WsMessage::Close(frame))) => {
                return Ok(End::Lost {
                    reason: format!(
                        "Slack closed the connection ({})",
                        frame.map_or_else(|| "no reason".to_string(), |f| f.code.to_string())
                    ),
                    healthy,
                });
            }
            // Pings are answered by the WebSocket library; pongs only prove the
            // connection is alive, which `last_frame` has recorded.
            Some(Ok(_)) => continue,
        };
        let envelope = match Frame::parse(text.as_str()) {
            Ok(Frame::Hello { num_connections }) => {
                healthy = true;
                info!(
                    num_connections = num_connections.unwrap_or(0),
                    "connected to Slack over Socket Mode"
                );
                // Slack sends each event to ONE of an app's connections, so a
                // second consumer of this app takes some of its mentions.
                if num_connections.is_some_and(|n| n > 1) {
                    warn!(
                        num_connections = num_connections.unwrap_or(0),
                        "another Socket Mode connection is open for this Slack app; each mention goes to only one of them"
                    );
                }
                continue;
            }
            Ok(Frame::Disconnect {
                reason: DisconnectReason::LinkDisabled,
            }) => {
                return Err(SlackError::Failed(
                    "Socket Mode was turned off for the Slack app (disconnect: link_disabled)"
                        .into(),
                )
                .into());
            }
            Ok(Frame::Disconnect {
                reason: DisconnectReason::Refresh(reason),
            }) => return Ok(close(socket, End::Refresh(reason)).await),
            Ok(Frame::Other { kind }) => {
                debug!(
                    kind = kind.as_deref().unwrap_or(""),
                    "ignored a Socket Mode frame"
                );
                continue;
            }
            Ok(Frame::Envelope(envelope)) => envelope,
            Err(FrameError::MalformedMention { envelope_id, field }) => {
                warn!(field, "dropped an app_mention with a malformed field");
                if let Err(err) = socket.send(WsMessage::Text(ack(&envelope_id).into())).await {
                    return Ok(End::Lost {
                        reason: format!("could not acknowledge: {err}"),
                        healthy,
                    });
                }
                continue;
            }
            Err(err) => {
                warn!(error = %err, "ignored a Socket Mode frame that would not parse");
                continue;
            }
        };

        match envelope.mention {
            Some(event) => {
                let event_id = event.event_id.clone().unwrap_or_default();
                let delay_secs = event.event_time.and_then(seconds_since);
                match mentions.dispose(event).await {
                    Disposition::Wake(mention) => {
                        publish(stdout, topic, &mention).await?;
                        mentions.published(&mention);
                        write_message(
                            stdout,
                            &Message::Baseline(BaselineMsg {
                                value: mentions.seen().to_json(),
                            }),
                        )
                        .await?;
                        info!(
                            event_id = %event_id,
                            retry_attempt = envelope.retry_attempt,
                            delay_secs,
                            "published a Slack mention"
                        );
                    }
                    Disposition::Duplicate => info!(
                        event_id = %event_id,
                        retry_attempt = envelope.retry_attempt,
                        "dropped a redelivered mention that was already published"
                    ),
                    Disposition::Filtered(filter) => info!(
                        event_id = %event_id,
                        filter = %filter,
                        "skipped a mention matching a --skip filter"
                    ),
                }
            }
            None => debug!(
                kind = %envelope.kind,
                event = envelope.other_event.as_deref().unwrap_or(""),
                "acknowledged an envelope that is not a mention"
            ),
        }
        if let Err(err) = socket
            .send(WsMessage::Text(ack(&envelope.envelope_id).into()))
            .await
        {
            return Ok(End::Lost {
                reason: format!("could not acknowledge: {err}"),
                healthy,
            });
        }
    }
}

/// Close politely; the connection is being abandoned either way.
async fn close(mut socket: Socket, end: End) -> End {
    let _ = tokio::time::timeout(Duration::from_secs(2), socket.close(None)).await;
    end
}

async fn publish(
    stdout: &mut Stdout,
    topic: &Topic,
    mention: &Mention,
) -> Result<(), AdapterError> {
    write_message(
        stdout,
        &Message::Publish(Publish {
            topic: topic.clone(),
            adapter: AdapterId(ADAPTER_ID.to_string()),
            body: mention.body(),
            subject: mention.subject(),
        }),
    )
    .await
}

/// The remembered event ids the supervisor injected. One this mode cannot read
/// is dropped with a warning: the cost is that a redelivery of a mention
/// published before the restart wakes again.
fn injected_seen(value: Value) -> Option<Seen> {
    if value.is_null() {
        return None;
    }
    let parsed = Seen::from_json(value);
    if parsed.is_none() {
        warn!("could not parse the injected baseline; starting with no remembered mentions");
    }
    parsed
}

fn seconds_since(event_time: u64) -> Option<u64> {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).ok()?.as_secs();
    now.checked_sub(event_time)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_doubles_to_a_cap() {
        let first = Duration::from_secs(1);
        assert_eq!(backoff(first, 1), Duration::from_secs(1));
        assert_eq!(backoff(first, 3), Duration::from_secs(4));
        assert_eq!(backoff(first, 40), MAX_BACKOFF);
    }

    #[test]
    fn only_slack_shaped_urls_are_opened() {
        assert!(acceptable_url("wss://wss-primary.slack.com/link/?ticket=x"));
        assert!(acceptable_url("ws://127.0.0.1:4000/"));
        assert!(!acceptable_url("ws://evil.example/"));
        assert!(!acceptable_url("https://slack.com/"));
    }

    #[test]
    fn a_logged_error_never_carries_the_ticket() {
        assert_eq!(
            without_url("HTTP error at wss://wss-primary.slack.com/link/?ticket=secret refused"),
            "HTTP error at <url> refused"
        );
    }

    #[test]
    fn the_config_topic_must_be_the_mentions_topic() {
        let config = |topic: &str| MentionConfig {
            topic: topic.to_string(),
            skip: SlackFilters::default(),
            baseline: Value::Null,
        };
        assert!(config("slack.mentions").topic().is_ok());
        assert!(matches!(
            config("slack.channel.C1").topic(),
            Err(AdapterError::TopicMismatch { .. })
        ));
    }
}
