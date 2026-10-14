//! The bot watch's process: hold a Socket Mode connection, publish each message
//! addressed to the bot, acknowledge every envelope, and reconnect (design/04).
//!
//! Unlike the channel and thread watches this mode does not poll. Slack pushes
//! events down a WebSocket the adapter opened, so a mention or a DM wakes in
//! about the time it takes to cross the socket. The connection is outbound only,
//! which is why it works from a laptop with no public URL.
//!
//! # Order: publish, remember, acknowledge
//!
//! A message is published, then the published set is persisted as the
//! baseline, then its envelope is acknowledged. A crash before the ack makes
//! Slack redeliver, which at worst wakes twice; acknowledging first would let a
//! crash lose the message outright. The same trade as the poll modes: re-fire
//! rather than drop.
//!
//! # Connections end
//!
//! Slack refreshes a connection every few hours, says so with a `disconnect`
//! frame, and documents that events "may" be lost while none is open. The
//! adapter reconnects at once after a refresh on a connection that said
//! `hello`, and backs off after anything else. It pings every
//! [`DEFAULT_PING_MS`] and drops a connection that has carried nothing for three
//! intervals, by the wall clock as well as the monotonic one: on macOS the
//! monotonic clock stops while the machine sleeps, so only the wall clock shows
//! that a socket held across a sleep has been silent for hours.

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures_util::{Sink, SinkExt, Stream, StreamExt};
use mailbox_protocol::{
    AdapterId, Baseline as BaselineMsg, Message, Publish, SlackFilters, Topic, slack_bot_topic,
};
use serde::Deserialize;
use serde_json::Value;
use tokio::io::AsyncWrite;
use tokio::signal::unix::Signal;
use tokio_tungstenite::tungstenite::{Error as WsError, Message as WsMessage};
use tracing::{debug, info, warn};

use crate::api::{CurlSlack, SlackApi, SlackError};
use crate::bot_messages::{BotMessages, Disposition, Seen, Skip, Wake};
use crate::error::AdapterError;
use crate::identity::{Identity, identify};
use crate::limits::{Limits, env_u64};
use crate::output::{ADAPTER_ID, write_message};
use crate::socket::{Content, DisconnectReason, Envelope, Frame, ack};
use crate::token::{self, App, Bot};

/// How often to ping Slack. Slack's docs name no keepalive; this is a chosen
/// value, short enough that a dead socket is noticed within a couple of minutes.
const DEFAULT_PING_MS: u64 = 30_000;
const ENV_PING_MS: &str = "MAILBOX_SLACK_SOCKET_PING_MS";

/// A connection that carries nothing, not even a pong, for this many ping
/// intervals is treated as dead.
const IDLE_PINGS: u32 = 3;

/// First wait before reconnecting after a failure; doubled per consecutive
/// failure up to [`MAX_BACKOFF`].
const DEFAULT_RECONNECT_BACKOFF_MS: u64 = 1_000;
const ENV_RECONNECT_BACKOFF_MS: &str = "MAILBOX_SLACK_RECONNECT_BACKOFF_MS";
const MAX_BACKOFF: Duration = Duration::from_secs(60);

/// Opening the WebSocket, or sending one frame on it, must not hang the adapter.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
const SEND_TIMEOUT: Duration = Duration::from_secs(10);

/// The bot watch's config line (`kind: "slack-bot"`).
#[derive(Debug, Deserialize)]
pub struct BotConfig {
    pub topic: String,
    #[serde(default)]
    pub skip: SlackFilters,
    #[serde(default)]
    pub baseline: Value,
}

impl BotConfig {
    /// The topic, checked against the one this mode publishes on.
    pub fn topic(&self) -> Result<Topic, AdapterError> {
        let expected = slack_bot_topic();
        if self.topic != expected.as_str() {
            return Err(AdapterError::TopicMismatch {
                topic: self.topic.clone(),
                expected: expected.as_str().to_string(),
            });
        }
        Ok(expected)
    }
}

/// How one connection ended. `healthy` is whether Slack said `hello` on it.
enum End {
    Signalled(&'static str),
    /// Slack asked for a new connection.
    Refresh {
        reason: String,
        healthy: bool,
    },
    /// The connection died.
    Lost {
        reason: String,
        healthy: bool,
    },
    /// The daemon that started this adapter is gone (design/04).
    Orphaned,
}

/// Why opening a connection, or identifying the bot, failed.
enum OpenError {
    /// Worth retrying: the network, a Slack 5xx, a WebSocket that would not
    /// open. `wait` is Slack's `Retry-After` when it rate-limited us.
    Transient {
        reason: String,
        wait: Option<Duration>,
    },
    Fatal(SlackError),
}

/// Consecutive failures and the backoff they earn. A pure counter, so the
/// give-up rule is testable without a socket.
struct Streak {
    failures: u64,
    max: u64,
    first: Duration,
}

impl Streak {
    fn new(max: u64, first: Duration) -> Self {
        Self {
            failures: 0,
            max,
            first,
        }
    }

    /// Count a failure: how long to wait before trying again, or `None` once the
    /// budget is spent. A wait Slack asked for is never cut short.
    fn fail(&mut self, asked: Option<Duration>) -> Option<Duration> {
        self.failures += 1;
        if self.failures >= self.max {
            return None;
        }
        let doublings = u32::try_from(self.failures - 1).unwrap_or(u32::MAX).min(16);
        let backoff = self.first.saturating_mul(1 << doublings).min(MAX_BACKOFF);
        Some(asked.map_or(backoff, |asked| asked.max(backoff)))
    }

    fn reset(&mut self) {
        self.failures = 0;
    }
}

pub async fn run(
    config: BotConfig,
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
    let parent = std::os::unix::process::parent_id();
    let mut stdout = tokio::io::stdout();

    // Both tokens are read up front, so a missing one fails the watch at start
    // with the command that stores it.
    let opener = CurlSlack::new(token::load::<App>().await?);
    let web = CurlSlack::new(token::load::<Bot>().await?);

    let seen = injected_seen(config.baseline);
    info!(
        remembered = seen.as_ref().map_or(0, Seen::remembered),
        skip_count = config.skip.iter().count(),
        skip = %config.skip,
        ping_ms = ping.as_millis() as u64,
        "slack bot watch started"
    );

    let mut streak = Streak::new(limits.max_transient, first_backoff);
    let identity = loop {
        let failure = tokio::select! {
            _ = sigterm.recv() => return stopped("SIGTERM"),
            _ = sigint.recv() => return stopped("SIGINT"),
            identified = identify(&web) => match identified.map_err(classify) {
                Ok(identity) => break identity,
                Err(OpenError::Fatal(err)) => return Err(err.into()),
                Err(OpenError::Transient { reason, wait }) => (reason, wait),
            },
        };
        if let Some(name) =
            back_off(&mut streak, failure, "identify the bot", sigterm, sigint).await?
        {
            return stopped(name);
        }
    };
    log_identity(&identity);
    let mut bot = BotMessages::new(web, identity, config.skip, seen.unwrap_or_default());

    streak.reset();
    let mut disconnected_at: Option<Instant> = None;
    loop {
        let opened = tokio::select! {
            _ = sigterm.recv() => return stopped("SIGTERM"),
            _ = sigint.recv() => return stopped("SIGINT"),
            opened = open(&opener) => opened,
        };
        let failure = match opened {
            Err(OpenError::Fatal(err)) => return Err(err.into()),
            Err(OpenError::Transient { reason, wait }) => (reason, wait),
            Ok(socket) => {
                let connection = Connection {
                    ping,
                    parent,
                    disconnected_for: disconnected_at.map(|at| at.elapsed()),
                };
                let end = serve(
                    socket,
                    &mut bot,
                    &mut stdout,
                    &topic,
                    &connection,
                    sigterm,
                    sigint,
                )
                .await?;
                disconnected_at = Some(Instant::now());
                match end {
                    End::Signalled(name) => return stopped(name),
                    End::Orphaned => {
                        info!(
                            parent,
                            "the daemon that started this adapter is gone; exiting"
                        );
                        return Ok(());
                    }
                    // A refresh on a connection that worked is routine. One
                    // before `hello` is a connection that never worked, and
                    // reconnecting at once would spin.
                    End::Refresh {
                        reason,
                        healthy: true,
                    } => {
                        info!(reason = %reason, "Slack refreshed the connection; reconnecting");
                        streak.reset();
                        continue;
                    }
                    End::Refresh {
                        reason,
                        healthy: false,
                    } => (format!("Slack disconnected before hello ({reason})"), None),
                    End::Lost { reason, healthy } => {
                        if healthy {
                            streak.reset();
                        }
                        (reason, None)
                    }
                }
            }
        };
        if let Some(name) = back_off(&mut streak, failure, "connect", sigterm, sigint).await? {
            return stopped(name);
        }
    }
}

fn log_identity(identity: &Identity) {
    info!(
        workspace = %identity.workspace,
        bot_user = identity.bot_user.as_ref().map_or("", |user| user.as_str()),
        bot_id = identity.bot_id.as_deref().unwrap_or(""),
        "identified the bot"
    );
}

/// Count a failure and wait it out, or give up once the budget is spent.
/// Returns the signal that cut the wait short, if one did.
async fn back_off(
    streak: &mut Streak,
    (reason, asked): (String, Option<Duration>),
    doing: &'static str,
    sigterm: &mut Signal,
    sigint: &mut Signal,
) -> Result<Option<&'static str>, AdapterError> {
    let Some(wait) = streak.fail(asked) else {
        return Err(SlackError::Transient(format!(
            "could not {doing} after {} tries: {reason}",
            streak.failures
        ))
        .into());
    };
    warn!(
        doing,
        consecutive_failures = streak.failures,
        max = streak.max,
        wait_ms = wait.as_millis() as u64,
        reason = %reason,
        "Socket Mode attempt failed; retrying after a wait"
    );
    Ok(tokio::select! {
        _ = sigterm.recv() => Some("SIGTERM"),
        _ = sigint.recv() => Some("SIGINT"),
        _ = tokio::time::sleep(wait) => None,
    })
}

fn stopped(signal: &str) -> Result<(), AdapterError> {
    info!(signal, "received signal; exiting cleanly");
    Ok(())
}

/// Ask Slack for a connection URL with the app token and open it.
async fn open(
    opener: &CurlSlack<App>,
) -> Result<
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    OpenError,
> {
    let reply = opener
        .call("apps.connections.open", &[])
        .await
        .map_err(classify)?;
    let url = reply
        .get("url")
        .and_then(Value::as_str)
        .ok_or_else(|| OpenError::Transient {
            reason: "apps.connections.open returned no url".into(),
            wait: None,
        })?;
    // The URL carries a one-time ticket, so it is never logged.
    if !acceptable_url(url) {
        return Err(OpenError::Fatal(SlackError::Failed(
            "apps.connections.open returned a URL that is not wss://".into(),
        )));
    }
    match tokio::time::timeout(CONNECT_TIMEOUT, tokio_tungstenite::connect_async(url)).await {
        Ok(Ok((socket, _response))) => {
            debug!("opened a Socket Mode WebSocket");
            Ok(socket)
        }
        Ok(Err(err)) => Err(OpenError::Transient {
            reason: format!("could not open the WebSocket: {}", without_url(&err)),
            wait: None,
        }),
        Err(_) => Err(OpenError::Transient {
            reason: "timed out opening the WebSocket".into(),
            wait: None,
        }),
    }
}

fn classify(err: SlackError) -> OpenError {
    match err {
        SlackError::Transient(reason) => OpenError::Transient { reason, wait: None },
        SlackError::RateLimited { retry_after } => OpenError::Transient {
            reason: "rate-limited".into(),
            wait: retry_after,
        },
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

/// An error's text with any URL cut out, so a ticket cannot reach a log. Every
/// WebSocket error goes through this, whichever variant it is.
fn without_url(err: &dyn std::fmt::Display) -> String {
    err.to_string()
        .split_whitespace()
        .map(|word| if word.contains("://") { "<url>" } else { word })
        .collect::<Vec<_>>()
        .join(" ")
}

/// What one connection needs to know beyond its socket.
struct Connection {
    ping: Duration,
    /// The daemon's pid when this adapter started.
    parent: u32,
    /// How long no connection was open before this one, for the log.
    disconnected_for: Option<Duration>,
}

/// Read one connection until it ends.
async fn serve<S, A, W>(
    mut socket: S,
    bot: &mut BotMessages<A>,
    out: &mut W,
    topic: &Topic,
    connection: &Connection,
    sigterm: &mut Signal,
    sigint: &mut Signal,
) -> Result<End, AdapterError>
where
    S: Stream<Item = Result<WsMessage, WsError>> + Sink<WsMessage, Error = WsError> + Unpin,
    A: SlackApi,
    W: AsyncWrite + Unpin,
{
    let mut healthy = false;
    let mut last_frame = Instant::now();
    let mut last_frame_wall = SystemTime::now();
    let idle = connection.ping * IDLE_PINGS;
    let mut pinger = tokio::time::interval(connection.ping);
    pinger.tick().await;
    loop {
        let incoming = tokio::select! {
            _ = sigterm.recv() => return Ok(close(socket, End::Signalled("SIGTERM")).await),
            _ = sigint.recv() => return Ok(close(socket, End::Signalled("SIGINT")).await),
            _ = pinger.tick() => {
                // An adapter whose daemon died would otherwise hold its
                // connection and take a share of the bot's events, which it can
                // never publish (design/04).
                if std::os::unix::process::parent_id() != connection.parent {
                    return Ok(close(socket, End::Orphaned).await);
                }
                let silent_wall = last_frame_wall.elapsed().unwrap_or_default();
                let silent = last_frame.elapsed().max(silent_wall);
                if silent > idle {
                    return Ok(End::Lost {
                        reason: format!("no frame from Slack in {}s", silent.as_secs()),
                        healthy,
                    });
                }
                if let Err(reason) = send(&mut socket, WsMessage::Ping(Vec::new().into())).await {
                    return Ok(End::Lost { reason: format!("could not ping: {reason}"), healthy });
                }
                continue;
            }
            incoming = socket.next() => incoming,
        };
        last_frame = Instant::now();
        last_frame_wall = SystemTime::now();
        let text = match incoming {
            None => {
                return Ok(End::Lost {
                    reason: "Slack closed the connection".into(),
                    healthy,
                });
            }
            Some(Err(err)) => {
                return Ok(End::Lost {
                    reason: format!("the WebSocket failed: {}", without_url(&err)),
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
                    reconnect = connection.disconnected_for.is_some(),
                    disconnected_ms = connection
                        .disconnected_for
                        .map_or(0, |gap| gap.as_millis() as u64),
                    "connected to Slack over Socket Mode"
                );
                // Slack sends each event to ONE of an app's connections, so a
                // second consumer of this app takes some of its events.
                if num_connections.is_some_and(|n| n > 1) {
                    warn!(
                        num_connections = num_connections.unwrap_or(0),
                        "another Socket Mode connection is open for this Slack app; each event goes to only one of them"
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
            }) => return Ok(close(socket, End::Refresh { reason, healthy }).await),
            Ok(Frame::Other { kind }) => {
                debug!(
                    kind = kind.as_deref().unwrap_or(""),
                    "ignored a Socket Mode frame"
                );
                continue;
            }
            Ok(Frame::Envelope(envelope)) => envelope,
            Err(err) => {
                warn!(error = %err, "ignored a Socket Mode frame that would not parse");
                continue;
            }
        };
        let id = envelope.id.clone();
        handle_envelope(*envelope, bot, out, topic).await?;
        if let Err(reason) = send(&mut socket, WsMessage::Text(ack(&id).into())).await {
            return Ok(End::Lost {
                reason: format!("could not acknowledge: {reason}"),
                healthy,
            });
        }
    }
}

/// Everything an envelope does before it is acknowledged: publish a message
/// that wakes, then persist what was published. Kept apart from the socket so
/// that order is testable on its own.
async fn handle_envelope<A: SlackApi, W: AsyncWrite + Unpin>(
    envelope: Envelope,
    bot: &mut BotMessages<A>,
    out: &mut W,
    topic: &Topic,
) -> Result<(), AdapterError> {
    let message = match envelope.content {
        Content::Message(message) => *message,
        Content::Malformed { field } => {
            warn!(
                envelope_id = %envelope.id,
                field,
                "dropped a message to the bot with a malformed field"
            );
            return Ok(());
        }
        Content::OtherEvent(event) => {
            debug!(envelope_id = %envelope.id, event = %event, "acknowledged an event that is not for the bot");
            return Ok(());
        }
        Content::NotAnEvent => {
            debug!(envelope_id = %envelope.id, kind = %envelope.kind, "acknowledged an envelope that is not an event");
            return Ok(());
        }
    };
    let via = message.via.as_str();
    let event_id = message
        .event_id
        .as_ref()
        .map_or_else(String::new, ToString::to_string);
    let conversation = message.conversation.to_string();
    let delay_secs = message.event_time.and_then(seconds_since);
    match bot.dispose(message).await {
        Disposition::Wake(wake) => {
            publish(out, topic, &wake).await?;
            bot.published(&wake);
            write_message(
                out,
                &Message::Baseline(BaselineMsg {
                    value: bot.seen().to_json(),
                }),
            )
            .await?;
            info!(
                via,
                envelope_id = %envelope.id,
                event_id = %event_id,
                channel = %conversation,
                retry_attempt = envelope.retry_attempt,
                delay_secs,
                names_resolved = wake.names_resolved,
                "published a message to the bot"
            );
        }
        Disposition::Skip(skip) => {
            let reason = skip.reason();
            let log = |reason: &str| {
                info!(
                    via,
                    envelope_id = %envelope.id,
                    event_id = %event_id,
                    channel = %conversation,
                    retry_attempt = envelope.retry_attempt,
                    reason,
                    "skipped a message to the bot"
                );
            };
            match skip {
                // Routine, so not worth a line each at the default level.
                Skip::Subtype(_) | Skip::Hidden | Skip::Own => {
                    debug!(via, envelope_id = %envelope.id, reason = %reason, "skipped a message to the bot");
                }
                Skip::Duplicate | Skip::Filtered(_) => log(&reason),
            }
        }
    }
    Ok(())
}

/// Send one frame, bounded so a stalled socket cannot hang the adapter.
async fn send<S>(socket: &mut S, frame: WsMessage) -> Result<(), String>
where
    S: Sink<WsMessage, Error = WsError> + Unpin,
{
    match tokio::time::timeout(SEND_TIMEOUT, socket.send(frame)).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(err)) => Err(without_url(&err)),
        Err(_) => Err("timed out".to_string()),
    }
}

/// Close politely; the connection is being abandoned either way.
async fn close<S>(mut socket: S, end: End) -> End
where
    S: Sink<WsMessage, Error = WsError> + Unpin,
{
    let _ = tokio::time::timeout(Duration::from_secs(2), socket.close()).await;
    end
}

async fn publish<W: AsyncWrite + Unpin>(
    out: &mut W,
    topic: &Topic,
    wake: &Wake,
) -> Result<(), AdapterError> {
    write_message(
        out,
        &Message::Publish(Publish {
            topic: topic.clone(),
            adapter: AdapterId(ADAPTER_ID.to_string()),
            body: wake.body(),
            subject: wake.subject(),
        }),
    )
    .await
}

/// The published set the supervisor injected. One this mode cannot read is
/// dropped with a warning: the cost is that a redelivery of a message
/// published before the restart wakes again.
fn injected_seen(value: Value) -> Option<Seen> {
    if value.is_null() {
        return None;
    }
    let parsed = Seen::from_json(value);
    if parsed.is_none() {
        warn!("could not parse the injected baseline; starting with no remembered messages");
    }
    parsed
}

fn seconds_since(event_time: u64) -> Option<u64> {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).ok()?.as_secs();
    now.checked_sub(event_time)
}

#[cfg(test)]
mod tests {
    use mailbox_protocol::{SlackTs, SlackUserId};
    use serde_json::json;

    use super::*;
    use crate::identity::WorkspaceUrl;
    use crate::socket::{BotMessage, ConversationId, EnvelopeId, Via};

    #[test]
    fn the_streak_doubles_its_wait_honours_slack_and_gives_up() {
        let mut streak = Streak::new(5, Duration::from_secs(1));
        assert_eq!(streak.fail(None), Some(Duration::from_secs(1)));
        assert_eq!(streak.fail(None), Some(Duration::from_secs(2)));
        assert_eq!(
            streak.fail(Some(Duration::from_secs(30))),
            Some(Duration::from_secs(30)),
            "Slack's Retry-After is never cut short"
        );
        assert_eq!(streak.fail(None), Some(Duration::from_secs(8)));
        assert_eq!(
            streak.fail(None),
            None,
            "the fifth failure spends the budget"
        );
        streak.reset();
        assert_eq!(streak.fail(None), Some(Duration::from_secs(1)));
        let mut long = Streak::new(100, Duration::from_secs(1));
        for _ in 0..40 {
            assert!(long.fail(None).unwrap() <= MAX_BACKOFF);
        }
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
            without_url(&"HTTP error at wss://wss-primary.slack.com/link/?ticket=secret refused"),
            "HTTP error at <url> refused"
        );
    }

    #[test]
    fn the_config_topic_must_be_the_bot_topic() {
        let config = |topic: &str| BotConfig {
            topic: topic.to_string(),
            skip: SlackFilters::default(),
            baseline: Value::Null,
        };
        assert!(config("slack.bot").topic().is_ok());
        assert!(matches!(
            config("slack.channel.C1").topic(),
            Err(AdapterError::TopicMismatch { .. })
        ));
    }

    /// Slack's lookups never matter here: they only decorate a subject.
    struct Offline;

    impl SlackApi for Offline {
        async fn call(&self, _: &str, _: &[(&str, &str)]) -> Result<Value, SlackError> {
            Err(SlackError::Transient("offline".into()))
        }
    }

    fn bot() -> BotMessages<Offline> {
        let identity = Identity {
            workspace: WorkspaceUrl::parse("https://w.slack.com/"),
            bot_user: None,
            bot_id: None,
        };
        BotMessages::new(Offline, identity, SlackFilters::default(), Seen::default())
    }

    fn envelope(content: Content) -> Envelope {
        Envelope {
            id: EnvelopeId::for_tests("env-1"),
            kind: "events_api".to_string(),
            retry_attempt: 0,
            content,
        }
    }

    fn dm() -> Content {
        Content::Message(Box::new(BotMessage {
            via: Via::Dm,
            event_id: None,
            event_time: None,
            conversation: ConversationId::parse("D0C8ABCDEF").unwrap(),
            ts: SlackTs::parse("1791935400.000100").unwrap(),
            thread_ts: None,
            user: Some(SlackUserId::parse("U0AB7RJSQBE").unwrap()),
            bot_id: None,
            app_id: None,
            subtype: None,
            hidden: false,
        }))
    }

    fn lines(out: &[u8]) -> Vec<Value> {
        String::from_utf8(out.to_vec())
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }

    /// The order the module header promises: the publish, then the baseline
    /// that records it. The ack comes after both, in `serve`.
    #[tokio::test(flavor = "current_thread")]
    async fn a_message_is_published_before_it_is_remembered() {
        let (mut bot, mut out) = (bot(), Vec::new());
        let topic = slack_bot_topic();
        handle_envelope(envelope(dm()), &mut bot, &mut out, &topic)
            .await
            .unwrap();
        let written = lines(&out);
        let types: Vec<&str> = written
            .iter()
            .map(|l| l["type"].as_str().unwrap())
            .collect();
        assert_eq!(types, ["publish", "baseline"]);
        assert_eq!(
            written[1]["value"],
            json!({"seen_messages": ["D0C8ABCDEF/1791935400.000100"]})
        );

        out.clear();
        handle_envelope(envelope(dm()), &mut bot, &mut out, &topic)
            .await
            .unwrap();
        assert!(out.is_empty(), "a redelivery writes nothing");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn envelopes_that_are_not_messages_write_nothing() {
        let (mut bot, mut out) = (bot(), Vec::new());
        let topic = slack_bot_topic();
        for content in [
            Content::Malformed { field: "ts" },
            Content::OtherEvent("reaction_added".into()),
            Content::NotAnEvent,
        ] {
            handle_envelope(envelope(content), &mut bot, &mut out, &topic)
                .await
                .unwrap();
        }
        assert!(out.is_empty());
    }
}
