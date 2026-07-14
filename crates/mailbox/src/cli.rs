//! The clap command layer: argument parsing, session resolution, output
//! formatting, and the thin edge that turns each command into a socket request
//! (or, for `serve`/`wait`, a direct call).
//!
//! This is the binary edge, so `anyhow` lives here (context-rich CLI errors);
//! the library modules it calls use typed `thiserror` errors. Logs go to stderr
//! (configured in `main`), so `--json` stdout stays clean for agents to parse.

use std::convert::Infallible;
use std::process::ExitCode;

use anyhow::Context;
use clap::{Args, Parser, Subcommand};
use tracing::{error, info, warn};

use mailbox::storage::{SessionId, StorageConfig};
use mailbox::wake::{WaitOutcome, Waiter, WakeOutcome};
use mailbox_harness::arm::{ArmDecision, SubscriptionProbe};
use mailbox_harness::hook::HookInput;
use mailbox_protocol::{AdapterId, GithubPr, Topic, inbox_topic, stub_topic};

use crate::client;
use crate::control::{
    AgentSummary, GithubPrTarget, Request, Response, StatusReport, SubscribeState, TopicStatus,
    UnwatchResultWire, WatchKindWire, WatchStateWire,
};
use crate::serve;

/// Opt-in env var: when `1`, `wait` appends its wake reason to stderr as a second
/// diagnostic line. TEST-only — the default payload-free reminder is unaffected.
const WAIT_DEBUG_ENV: &str = "MAILBOX_WAIT_DEBUG";

/// How a command renders its result on stdout.
///
/// A named enum rather than a bare `bool` threaded through every handler, so a
/// call site reads `OutputFormat::Json` instead of an unlabelled `true`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OutputFormat {
    /// Machine-readable JSON (for agents).
    Json,
    /// Human-readable text.
    Human,
}

impl OutputFormat {
    fn from_json_flag(json: bool) -> Self {
        if json {
            OutputFormat::Json
        } else {
            OutputFormat::Human
        }
    }

    fn is_json(self) -> bool {
        matches!(self, OutputFormat::Json)
    }
}

/// The `mailbox` bridge CLI: the single user/agent/adapter entry point.
///
/// All mutating/reading commands are clients of the `mailbox serve` daemon over
/// a user-scoped Unix socket (no TCP). When the daemon is down they fail loudly
/// (ADR-0004). `wait` is the sole read-only exception: it opens the store
/// read-only and blocks on its wake FIFO, never touching the socket.
#[derive(Parser, Debug)]
#[command(name = "mailbox", version, about, long_about = None)]
pub struct Cli {
    /// Emit machine-readable JSON on stdout (for agents). Human-readable
    /// otherwise. Logs always go to stderr, so JSON output is never polluted.
    #[arg(long, global = true)]
    pub json: bool,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Run the long-lived bridge daemon (owns the single writer + waker + socket).
    Serve,
    /// Publish an event to a topic.
    Publish(PublishArgs),
    /// Subscribe this session to a topic (baseline-on-subscribe).
    Subscribe(TopicArgs),
    /// Unsubscribe this session from a topic.
    Unsubscribe(TopicArgs),
    /// Read this session's unread events, advancing its cursor.
    Read(ReadArgs),
    /// Watch an entity: record interest, subscribe to its topic, and (via the
    /// daemon) spawn the shared supervised poller.
    Watch(WatchArgs),
    /// Drop interest in a watch.
    Unwatch(UnwatchArgs),
    /// Show watches (interest + child pid) and this session's unread counts.
    Status(SessionOpt),
    /// Print this session's own id and inbox topic (card 16). Needs no bridge.
    Whoami(SessionOpt),
    /// Message a peer agent: publish to its inbox, stamped with your session id.
    Send(SendArgs),
    /// List the agents with a registered inbox (who you can `send` to).
    Agents(SessionOpt),
    /// List known topics with their subscriber and event counts.
    Topics(TopicsArgs),
    /// Block until this session has mail, then exit 2 (the asyncRewake contract).
    Wait(WaitArgs),
    /// Claude Code hook handlers and setup (arm / cleanup / install-hooks /
    /// install-skills). The harness owns the wake loop so the agent never re-arms
    /// (card 11).
    Harness(HarnessArgs),
}

/// Env var the harness hooks export for a session (card 11).
const ENV_MAILBOX_SESSION: &str = "MAILBOX_SESSION_ID";
/// Env var **Claude Code itself** exports into every tool invocation. It carries
/// the same id the hooks receive on stdin, so it is the fallback that lets an
/// agent learn its own identity with nothing installed but the binary (card 16).
const ENV_CLAUDE_SESSION: &str = "CLAUDE_CODE_SESSION_ID";

/// The session identity every session-scoped command needs. Parsed ONCE here at
/// the clap edge into a branded [`SessionId`] (parse, don't validate), so the
/// handlers never re-mint it from a bare `String`.
///
/// Resolution order: `--session` > `MAILBOX_SESSION_ID` > `CLAUDE_CODE_SESSION_ID`
/// (see [`resolve_session`]). The env fallbacks are read here rather than through
/// clap's `env =` because clap supports only ONE env var per argument, and the
/// precedence between the two is a rule we want stated (and tested) explicitly.
#[derive(Args, Debug)]
pub struct SessionOpt {
    /// This session's id. Defaults to `$MAILBOX_SESSION_ID`, else
    /// `$CLAUDE_CODE_SESSION_ID` (which Claude Code exports into every tool call).
    #[arg(long, value_parser = parse_session)]
    pub session: Option<SessionId>,
}

impl SessionOpt {
    /// Resolve the session from the flag and the environment, or fail with an
    /// actionable message naming every place we looked.
    pub fn resolve(&self) -> anyhow::Result<SessionId> {
        resolve_session(
            self.session.clone(),
            env_session(ENV_MAILBOX_SESSION),
            env_session(ENV_CLAUDE_SESSION),
        )
    }
}

/// A non-empty environment variable, trimmed of surrounding whitespace. An empty
/// or whitespace-only value names no session, so it is treated as absent rather
/// than resolving to a phantom session id.
fn env_session(key: &str) -> Option<String> {
    let value = std::env::var(key).ok()?;
    let trimmed = value.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_string())
}

/// The session-resolution rule, as a pure function of its three inputs so the
/// precedence is unit-testable without touching process env.
fn resolve_session(
    flag: Option<SessionId>,
    mailbox_env: Option<String>,
    claude_env: Option<String>,
) -> anyhow::Result<SessionId> {
    flag.or_else(|| mailbox_env.map(SessionId::new))
        .or_else(|| claude_env.map(SessionId::new))
        .context(
            "no session id: pass --session <id>, or set MAILBOX_SESSION_ID \
             (the harness hooks do) or CLAUDE_CODE_SESSION_ID (Claude Code does)",
        )
}

/// Wrap a raw session label into a [`SessionId`]. Infallible — the harness owns
/// the label's grammar — but expressed as a `value_parser` so clap brands it.
fn parse_session(raw: &str) -> Result<SessionId, Infallible> {
    Ok(SessionId::new(raw))
}

/// Default waiter max-block before self-respawn (9 minutes). Kept below Claude
/// Code's default async-hook `timeout` (10 min) so the waiter re-execs a fresh
/// image before the harness would kill it (card 11, docs/01-wake-and-rearm.md).
const DEFAULT_ARM_MAX_BLOCK_MS: u64 = 540_000;

/// Default async-hook `timeout` the install snippet writes, in **seconds** (10
/// minutes — Claude Code's documented default for command hooks).
const DEFAULT_HOOK_TIMEOUT_SECS: u64 = 600;

/// Arguments to `wait`: the session plus an optional self-respawn bound.
#[derive(Args, Debug)]
pub struct WaitArgs {
    #[command(flatten)]
    pub session: SessionOpt,
    /// If set, block at most this long before re-execing a fresh waiter (the
    /// self-respawn that keeps a long idle armed, card 11). Absent = block forever
    /// (the card-05 default). The harness passes this; a bare `mailbox wait` does
    /// not, preserving the original blocking contract.
    #[arg(long)]
    pub max_block_ms: Option<u64>,
}

/// The `harness` command group: Claude Code hook targets.
#[derive(Args, Debug)]
pub struct HarnessArgs {
    #[command(subcommand)]
    pub command: HarnessCommand,
}

#[derive(Subcommand, Debug)]
pub enum HarnessCommand {
    /// SessionStart / Stop hook: launch a waiter IFF the session is subscribed.
    Arm(ArmArgs),
    /// SessionEnd hook: reap the waiter and drop this session's interests/subs.
    Cleanup,
    /// Merge the hooks into the Claude Code settings.json (and print the snippet).
    InstallHooks(InstallHooksArgs),
    /// Install the embedded agent-mailbox skill into the Claude Code skills dir.
    InstallSkills(InstallSkillsArgs),
}

#[derive(Args, Debug)]
pub struct ArmArgs {
    /// Max block the armed waiter uses before self-respawn (see [`WaitArgs`]).
    ///
    /// `arm` always reads the session id from the hook's stdin JSON — the
    /// self-respawn re-execs `mailbox wait` directly (carrying `--session`), never
    /// `harness arm`, so `arm` needs no `--session` flag.
    #[arg(long, default_value_t = DEFAULT_ARM_MAX_BLOCK_MS)]
    pub max_block_ms: u64,
}

#[derive(Args, Debug)]
pub struct InstallHooksArgs {
    /// settings.json to merge the snippet into, created if missing. Defaults to
    /// `~/.claude/settings.json` (home from `AGENT_MAILBOX_HOME`, else `HOME`) IF
    /// that file exists — if it does not, the snippet is only printed. Unrelated
    /// settings and foreign hooks are always preserved; a re-run does not duplicate.
    #[arg(long)]
    pub settings: Option<std::path::PathBuf>,
    /// Absolute path to the `mailbox` binary the hooks invoke. Defaults to this
    /// executable's resolved path.
    #[arg(long)]
    pub mailbox_bin: Option<std::path::PathBuf>,
    /// Claude Code async-hook timeout to write, in seconds.
    #[arg(long, default_value_t = DEFAULT_HOOK_TIMEOUT_SECS)]
    pub timeout_secs: u64,
    /// Waiter max-block to write into the arm command, in milliseconds.
    #[arg(long, default_value_t = DEFAULT_ARM_MAX_BLOCK_MS)]
    pub max_block_ms: u64,
}

#[derive(Args, Debug)]
pub struct InstallSkillsArgs {
    /// Directory to install the skill(s) into. Defaults to `~/.claude/skills`
    /// (home from `AGENT_MAILBOX_HOME`, else `HOME`). Each skill lands at
    /// `<skills-dir>/<name>/SKILL.md`; nothing else is touched.
    #[arg(long)]
    pub skills_dir: Option<std::path::PathBuf>,
}

#[derive(Args, Debug)]
pub struct PublishArgs {
    /// Topic to publish to (e.g. `github.pr.owner/repo#42`).
    pub topic: String,
    /// JSON event body, stored verbatim and never interpreted.
    #[arg(long, default_value = "{}")]
    pub body: String,
    /// Publisher provenance label (a name, not authority).
    #[arg(long, default_value = "cli")]
    pub adapter: String,
}

#[derive(Args, Debug)]
pub struct TopicArgs {
    /// Topic to (un)subscribe.
    pub topic: String,
    #[command(flatten)]
    pub session: SessionOpt,
}

/// Arguments to `send`: who to message, and what to say.
///
/// `--text` and `--body` are mutually exclusive: `--text` IS the shorthand for
/// `--body '{"text": "..."}'`, so accepting both would only raise the question of
/// which wins. Neither is also fine — a bare `send <target>` is a poke, and the
/// receiver still learns who it came from (the `from` stamp is always present).
#[derive(Args, Debug)]
pub struct SendArgs {
    /// The agent to message: a bare session id, or its full `agent.<id>` topic.
    pub target: String,
    /// Message text. Shorthand for `--body '{"text": "<s>"}'`.
    #[arg(long, conflicts_with = "body")]
    pub text: Option<String>,
    /// A JSON **object** body (stored verbatim; the bridge only adds `from`).
    #[arg(long)]
    pub body: Option<String>,
    #[command(flatten)]
    pub session: SessionOpt,
}

#[derive(Args, Debug)]
pub struct TopicsArgs {
    /// Only list topics starting with this prefix (e.g. `agent.`, `github.pr.`).
    #[arg(long)]
    pub prefix: Option<String>,
}

#[derive(Args, Debug)]
pub struct ReadArgs {
    /// Maximum events per topic to return (bridge default if omitted).
    #[arg(long)]
    pub limit: Option<u32>,
    #[command(flatten)]
    pub session: SessionOpt,
}

#[derive(Args, Debug)]
pub struct WatchArgs {
    #[command(subcommand)]
    pub target: WatchTargetCmd,
}

#[derive(Subcommand, Debug)]
pub enum WatchTargetCmd {
    /// Watch a GitHub pull request.
    GithubPr(GithubPrWatchArgs),
    /// Watch a stub publisher (the reference adapter; card 09). Publishes a
    /// synthetic event on an interval to prove the whole path end to end.
    Stub(StubWatchArgs),
}

#[derive(Args, Debug)]
pub struct GithubPrWatchArgs {
    /// PR reference: `owner/repo#number`.
    pub spec: String,
    /// Poll interval in seconds (the supervised adapter's poll cadence).
    #[arg(long, default_value_t = 60)]
    pub interval: u64,
    #[command(flatten)]
    pub session: SessionOpt,
}

#[derive(Args, Debug)]
pub struct StubWatchArgs {
    /// Stub label; the topic is `stub.<label>`.
    pub label: String,
    /// Interval between synthetic publishes, in milliseconds.
    #[arg(long, default_value_t = 1000)]
    pub interval_ms: u64,
    /// How many events to publish; `0` (the default) means publish forever.
    #[arg(long, default_value_t = 0)]
    pub count: u64,
    #[command(flatten)]
    pub session: SessionOpt,
}

#[derive(Args, Debug)]
pub struct UnwatchArgs {
    #[command(subcommand)]
    pub target: UnwatchTargetCmd,
}

#[derive(Subcommand, Debug)]
pub enum UnwatchTargetCmd {
    /// Stop watching a GitHub pull request.
    GithubPr(GithubPrUnwatchArgs),
    /// Stop watching a stub publisher.
    Stub(StubUnwatchArgs),
}

#[derive(Args, Debug)]
pub struct GithubPrUnwatchArgs {
    /// PR reference: `owner/repo#number`.
    pub spec: String,
    #[command(flatten)]
    pub session: SessionOpt,
}

#[derive(Args, Debug)]
pub struct StubUnwatchArgs {
    /// Stub label previously passed to `watch stub`.
    pub label: String,
    #[command(flatten)]
    pub session: SessionOpt,
}

/// Run an async command (everything except `wait`). Returns `Ok(())` on success;
/// the caller maps `Err` to a non-zero exit.
pub async fn run(format: OutputFormat, command: Command) -> anyhow::Result<()> {
    match command {
        Command::Serve => run_serve().await,
        Command::Publish(args) => run_publish(format, args).await,
        Command::Subscribe(args) => run_subscribe(format, args).await,
        Command::Unsubscribe(args) => run_unsubscribe(format, args).await,
        Command::Read(args) => run_read(format, args).await,
        Command::Watch(args) => run_watch(format, args).await,
        Command::Unwatch(args) => run_unwatch(format, args).await,
        Command::Status(args) => run_status(format, args).await,
        Command::Whoami(args) => run_whoami(format, args),
        Command::Send(args) => run_send(format, args).await,
        Command::Agents(args) => run_agents(format, args).await,
        Command::Topics(args) => run_topics(format, args).await,
        // `wait` is dispatched synchronously by `main` and never reaches here.
        Command::Wait(_) => unreachable!("wait is handled synchronously in main"),
        Command::Harness(args) => run_harness(format, args).await,
    }
}

async fn run_serve() -> anyhow::Result<()> {
    let config = StorageConfig::from_env().context("resolving storage path for serve")?;
    serve::run(config).await
}

async fn run_publish(format: OutputFormat, args: PublishArgs) -> anyhow::Result<()> {
    let topic = parse_topic(&args.topic)?;
    let body: serde_json::Value =
        serde_json::from_str(&args.body).context("--body must be valid JSON")?;
    request(
        format,
        Request::Publish {
            topic,
            adapter: AdapterId(args.adapter),
            body,
        },
    )
    .await
}

async fn run_subscribe(format: OutputFormat, args: TopicArgs) -> anyhow::Result<()> {
    let topic = parse_topic(&args.topic)?;
    request(
        format,
        Request::Subscribe {
            session: resolve_session_or_fail(format, &args.session)?,
            topic,
        },
    )
    .await
}

async fn run_unsubscribe(format: OutputFormat, args: TopicArgs) -> anyhow::Result<()> {
    let topic = parse_topic(&args.topic)?;
    request(
        format,
        Request::Unsubscribe {
            session: resolve_session_or_fail(format, &args.session)?,
            topic,
        },
    )
    .await
}

async fn run_read(format: OutputFormat, args: ReadArgs) -> anyhow::Result<()> {
    request(
        format,
        Request::Read {
            session: resolve_session_or_fail(format, &args.session)?,
            limit: args.limit,
        },
    )
    .await
}

/// `whoami`: this session's id and its inbox topic — the address a peer uses to
/// `send` to it. Deliberately NOT a socket call: identity does not depend on the
/// bridge, so an agent can always answer "who am I" even when the daemon is down.
fn run_whoami(format: OutputFormat, args: SessionOpt) -> anyhow::Result<()> {
    let session = resolve_session_or_fail(format, &args)?;
    let inbox = inbox_topic(&session)
        .with_context(|| format!("session {:?} cannot form an inbox topic", session.as_str()))?;

    if format.is_json() {
        println!(
            "{}",
            serde_json::json!({ "session": session.as_str(), "inbox_topic": inbox.as_str() })
        );
    } else {
        println!("session: {}", session.as_str());
        println!("inbox:   {}", inbox.as_str());
    }
    Ok(())
}

/// `send`: message a peer agent. The body the bridge publishes is
/// `{"from": "<sender>", ...}` — see [`mailbox::agents`] for the convention and
/// for why an unregistered target is a hard error rather than a silent publish.
async fn run_send(format: OutputFormat, args: SendArgs) -> anyhow::Result<()> {
    let from = resolve_session_or_fail(format, &args.session)?;
    let to = parse_send_target(&args.target)?;
    let body = send_body(args.text, args.body)?;
    request(format, Request::Send { from, to, body }).await
}

/// Build the message body from the mutually-exclusive `--text` / `--body` flags.
/// `--body` must be a JSON **object**: the bridge stamps `from` into it, and there
/// is nowhere to stamp it on a bare scalar or array.
fn send_body(
    text: Option<String>,
    body: Option<String>,
) -> anyhow::Result<serde_json::Map<String, serde_json::Value>> {
    match (text, body) {
        (Some(text), _) => {
            let mut map = serde_json::Map::new();
            map.insert("text".to_string(), serde_json::Value::String(text));
            Ok(map)
        }
        (None, Some(raw)) => {
            let value: serde_json::Value =
                serde_json::from_str(&raw).context("--body must be valid JSON")?;
            match value {
                serde_json::Value::Object(map) => Ok(map),
                _ => anyhow::bail!(
                    "--body must be a JSON object (e.g. '{{\"text\":\"hi\"}}'), so the bridge can \
                     stamp the sender's id into it"
                ),
            }
        }
        // A bare poke: the receiver still learns who sent it (the `from` stamp).
        (None, None) => Ok(serde_json::Map::new()),
    }
}

/// Resolve a `send` target — a bare session id, or a full `agent.<id>` topic —
/// into the [`SessionId`] the request addresses.
///
/// The three cases are kept apart rather than guessed at: a topic in the `agent.`
/// namespace with a bad session segment is an ERROR (the user meant an inbox and
/// got it wrong), while a string that is not in that namespace at all is treated
/// as the bare session id it looks like.
fn parse_send_target(raw: &str) -> anyhow::Result<SessionId> {
    let topic = Topic::parse(raw).with_context(|| format!("invalid send target {raw:?}"))?;
    match topic.as_agent_inbox() {
        Ok(session) => Ok(session),
        Err(mailbox_protocol::TopicError::NotAgentInbox) => {
            let session = SessionId::new(raw);
            // Validate through the same grammar the daemon will use, so a target
            // that could never form an inbox fails locally with a clear message.
            inbox_topic(&session)
                .with_context(|| format!("{raw:?} cannot be used as an agent address"))?;
            Ok(session)
        }
        Err(err) => Err(anyhow::Error::new(err))
            .with_context(|| format!("invalid agent inbox topic {raw:?}")),
    }
}

async fn run_agents(format: OutputFormat, args: SessionOpt) -> anyhow::Result<()> {
    request(
        format,
        Request::Agents {
            session: resolve_session_or_fail(format, &args)?,
        },
    )
    .await
}

async fn run_topics(format: OutputFormat, args: TopicsArgs) -> anyhow::Result<()> {
    request(
        format,
        Request::Topics {
            prefix: args.prefix,
        },
    )
    .await
}

async fn run_watch(format: OutputFormat, args: WatchArgs) -> anyhow::Result<()> {
    let req = match args.target {
        WatchTargetCmd::GithubPr(gh) => Request::Watch {
            session: resolve_session_or_fail(format, &gh.session)?,
            target: parse_pr_spec(&gh.spec)?,
            interval_secs: gh.interval,
        },
        WatchTargetCmd::Stub(stub) => Request::WatchStub {
            session: resolve_session_or_fail(format, &stub.session)?,
            // Validate the label at the edge (same as the daemon) so a bad label
            // is a clean local error, not a round-trip.
            label: parse_stub_label(&stub.label)?,
            interval_ms: stub.interval_ms,
            count: stub.count,
        },
    };
    request(format, req).await
}

async fn run_unwatch(format: OutputFormat, args: UnwatchArgs) -> anyhow::Result<()> {
    let req = match args.target {
        UnwatchTargetCmd::GithubPr(gh) => Request::Unwatch {
            session: resolve_session_or_fail(format, &gh.session)?,
            target: parse_pr_spec(&gh.spec)?,
        },
        UnwatchTargetCmd::Stub(stub) => Request::UnwatchStub {
            session: resolve_session_or_fail(format, &stub.session)?,
            label: parse_stub_label(&stub.label)?,
        },
    };
    request(format, req).await
}

async fn run_status(format: OutputFormat, args: SessionOpt) -> anyhow::Result<()> {
    request(
        format,
        Request::Status {
            session: resolve_session_or_fail(format, &args)?,
        },
    )
    .await
}

/// Send one request to the daemon and render the reply.
///
/// Three outcomes, kept distinct so an agent (and a human) can tell them apart:
/// - success → render the typed response (JSON or human text) to stdout;
/// - a serviced request that failed ([`Response::Error`]) → render the message;
/// - the bridge is down / unreachable ([`client::ClientError`]) → the actionable
///   "start it with `mailbox serve`" error, tagged with the op/session it was
///   trying to run (F13). Both failure paths exit non-zero.
async fn request(format: OutputFormat, request: Request) -> anyhow::Result<()> {
    let config = StorageConfig::from_env().context("resolving storage path")?;
    let socket = config.socket_path();

    let response = match client::send(&socket, &request).await {
        Ok(response) => response,
        // `ClientError::BridgeDown`'s Display is already actionable; add the op +
        // session so stderr names what was being attempted.
        Err(err) => {
            return Err(fail(format, &err.to_string()).context(request_context(&request)));
        }
    };

    if let Response::Error { message } = &response {
        return Err(fail(format, message));
    }

    if format.is_json() {
        // The typed response is already the machine-readable contract.
        println!("{}", serde_json::to_string(&response)?);
    } else {
        render_human(&response);
    }
    Ok(())
}

/// Build the edge error. In JSON mode it first prints the TYPED
/// [`Response::Error`] to stdout (so `--json` consumers always get JSON even on
/// failure, and its shape can't drift from the enum's serde). `main` additionally
/// prints the message to stderr, so human-mode stdout stays empty.
fn fail(format: OutputFormat, message: &str) -> anyhow::Error {
    if format.is_json()
        && let Ok(json) = serde_json::to_string(&Response::error(message))
    {
        println!("{json}");
    }
    anyhow::anyhow!("{message}")
}

/// Resolve the session for a session-scoped command, routing a failure through
/// [`fail`] so `--json` mode still emits a typed [`Response::Error`] on stdout —
/// the same contract every serviced failure honours. Without this, a missing
/// session (no `--session`, no env) failed *before* any `fail`/`request` call, so
/// `mailbox --json <cmd>` printed nothing on stdout and only a plain-text stderr
/// line. Exits non-zero (via the returned `Err` → `ExitCode::FAILURE`), never
/// exit 2 — a resolution failure is an error, not a wake.
fn resolve_session_or_fail(
    format: OutputFormat,
    session: &SessionOpt,
) -> anyhow::Result<SessionId> {
    session
        .resolve()
        .map_err(|err| fail(format, &format!("{err:#}")))
}

/// A short "what was being attempted" label for a failed request, for the stderr
/// context chain (the only thing the user sees when the bridge is down).
fn request_context(request: &Request) -> String {
    match request {
        Request::Publish { topic, .. } => format!("publishing to {}", topic.as_str()),
        Request::Subscribe { session, topic } => {
            format!("subscribing {} to {}", session.as_str(), topic.as_str())
        }
        Request::Unsubscribe { session, topic } => {
            format!("unsubscribing {} from {}", session.as_str(), topic.as_str())
        }
        Request::Read { session, .. } => format!("reading for {}", session.as_str()),
        Request::Watch {
            session, target, ..
        } => format!(
            "watching {}/{}#{} for {}",
            target.owner,
            target.repo,
            target.number,
            session.as_str()
        ),
        Request::Unwatch { session, target } => format!(
            "unwatching {}/{}#{} for {}",
            target.owner,
            target.repo,
            target.number,
            session.as_str()
        ),
        Request::WatchStub { session, label, .. } => {
            format!("watching stub {label} for {}", session.as_str())
        }
        Request::UnwatchStub { session, label } => {
            format!("unwatching stub {label} for {}", session.as_str())
        }
        Request::Status { session } => format!("status for {}", session.as_str()),
        Request::Send { from, to, .. } => {
            format!("sending from {} to {}", from.as_str(), to.as_str())
        }
        Request::Agents { .. } => "listing agents".to_string(),
        Request::Topics { prefix } => match prefix {
            Some(prefix) => format!("listing topics under {prefix:?}"),
            None => "listing topics".to_string(),
        },
        Request::EndSession { session } => format!("ending session {}", session.as_str()),
    }
}

/// Render a successful response as human-readable text on stdout.
fn render_human(response: &Response) {
    match response {
        Response::Published { id, offset } => {
            println!("published event {} at offset {}", id.0, offset.0);
        }
        Response::Subscribed { topic, outcome } => {
            println!(
                "subscribed to {} ({})",
                topic.as_str(),
                describe_sub(outcome)
            );
        }
        Response::Unsubscribed { topic } => {
            println!("unsubscribed from {}", topic.as_str());
        }
        Response::Read { events } => {
            if events.is_empty() {
                println!("no unread events");
            } else {
                println!("{} unread event(s):", events.len());
                for event in events {
                    // The body is what `read` exists to surface, so showing it
                    // here is correct (unlike wake, which is payload-free).
                    println!(
                        "  [{}] offset={} id={} body={}",
                        event.topic.as_str(),
                        event.offset.0,
                        event.id.0,
                        event.body
                    );
                }
            }
        }
        Response::Watched {
            topic,
            interest,
            subscribe,
        } => {
            println!(
                "watching {} (interest={}, subscription: {})",
                topic.as_str(),
                interest,
                describe_sub(subscribe)
            );
        }
        Response::Unwatched { topic, outcome } => match outcome {
            UnwatchResultWire::Dropped { remaining_interest } => println!(
                "unwatched {} (remaining interest={})",
                topic.as_str(),
                remaining_interest
            ),
            UnwatchResultWire::NoSuchWatch => {
                println!(
                    "no watch existed for {}; unsubscribed anyway",
                    topic.as_str()
                );
            }
        },
        Response::Status(report) => render_status(report),
        Response::Sent {
            to,
            topic,
            id,
            offset,
        } => println!(
            "sent to {} on {} (event {} at offset {})",
            to.as_str(),
            topic.as_str(),
            id.0,
            offset.0
        ),
        Response::Agents { agents } => render_agents(agents),
        Response::Topics { topics } => render_topics(topics),
        Response::SessionEnded {
            subscriptions_dropped,
            interests_dropped,
            adapters_stopped,
        } => println!(
            "ended session (subscriptions dropped={subscriptions_dropped}, interests dropped={interests_dropped}, adapters stopped={adapters_stopped})"
        ),
        // Error is handled before rendering; nothing to print here.
        Response::Error { message } => eprintln!("error: {message}"),
    }
}

fn describe_sub(state: &SubscribeState) -> String {
    match state {
        SubscribeState::Subscribed {
            baseline: Some(offset),
        } => format!("new, baselined at offset {}", offset.0),
        SubscribeState::Subscribed { baseline: None } => {
            "new, empty topic (no baseline)".to_string()
        }
        SubscribeState::AlreadySubscribed => "already subscribed".to_string(),
        SubscribeState::RefusedSessionRecentlyEnded => {
            "refused: session recently ended (not resurrecting its inbox)".to_string()
        }
    }
}

/// Render the registered agent inboxes.
///
/// The liveness column is stated in full rather than as a bare `live`/`idle`
/// flag, because it is easy to over-read: at best it means "a waiter *appears*
/// blocked for this session", not "this agent is healthy". It is a best-effort
/// probe (`kill(pid, 0)` on a pidfile) that cannot rule out PID reuse, so the
/// wording hedges. A `send` to an agent with no live waiter still lands durably —
/// so the footer says so instead of leaving the reader to guess (there is no
/// heartbeat here, and we do not pretend otherwise).
fn render_agents(agents: &[AgentSummary]) {
    if agents.is_empty() {
        println!("no agents registered (nobody is addressable yet)");
        return;
    }
    println!("{} agent(s):", agents.len());
    for agent in agents {
        let waiter = if agent.live_waiter {
            "idle (waiter appears blocked — a send should wake it)"
        } else {
            "busy or unarmed (a send still lands in its inbox)"
        };
        let me = if agent.is_self { "  <- you" } else { "" };
        println!(
            "  {}  inbox={}  {}{}",
            agent.session.as_str(),
            agent.inbox.as_str(),
            waiter,
            me
        );
    }
}

fn render_topics(topics: &[TopicStatus]) {
    if topics.is_empty() {
        println!("no topics");
        return;
    }
    println!("{} topic(s):", topics.len());
    for topic in topics {
        // A topic with no events has no last-event time; say "-" rather than
        // inventing an epoch timestamp.
        let last = match topic.last_event_ms {
            Some(ms) => format!("{ms}ms"),
            None => "-".to_string(),
        };
        println!(
            "  {}  subscribers={} events={} last_event={}",
            topic.topic.as_str(),
            topic.subscribers,
            topic.events,
            last
        );
    }
}

fn render_status(report: &StatusReport) {
    println!("session: {}", report.session.as_str());
    // The inbox line answers "can peers reach me?" — the topic AND whether the
    // session is actually subscribed to it (registration is what makes a `send`
    // deliverable; see ADR-0007).
    match &report.inbox {
        Some(inbox) => {
            let registered = if report.subscriptions.contains(inbox) {
                "registered"
            } else {
                "NOT registered — peers cannot send to this session"
            };
            println!("inbox: {} ({registered})", inbox.as_str());
        }
        None => println!("inbox: none (this session id cannot form an inbox topic)"),
    }
    if report.watches.is_empty() {
        println!("watches: none");
    } else {
        println!("watches:");
        for watch in &report.watches {
            let (state, child) = match watch.state {
                WatchStateWire::Desired => ("desired", "not running".to_string()),
                WatchStateWire::Running { pid } => ("running", format!("pid {pid}")),
                WatchStateWire::Stopped => ("stopped", "stopped".to_string()),
                WatchStateWire::Failed => ("failed", "gave up after repeated crashes".to_string()),
            };
            // The github entity is `repo#pr`; a stub is just its label (pr is an
            // unused 0 sentinel there, so showing `#0` would be noise).
            let entity = match watch.kind {
                WatchKindWire::GithubPr => format!("{}#{}", watch.repo, watch.pr),
                WatchKindWire::Stub => watch.repo.clone(),
            };
            println!(
                "  {} {}  state={} interest={} interval={} child={}",
                kind_label(watch.kind),
                entity,
                state,
                watch.interest,
                format_interval(watch.interval_ms),
                child
            );
        }
    }
    if report.subscriptions.is_empty() {
        println!("subscriptions: none");
    } else {
        println!("subscriptions:");
        for topic in &report.subscriptions {
            println!("  {}", topic.as_str());
        }
    }
    if report.unread.is_empty() {
        println!("unread: none");
    } else {
        println!("unread:");
        for topic in &report.unread {
            println!("  [{}] {}", topic.topic.as_str(), topic.unread);
        }
    }
}

fn kind_label(kind: WatchKindWire) -> &'static str {
    match kind {
        WatchKindWire::GithubPr => "github-pr",
        WatchKindWire::Stub => "stub",
    }
}

/// Render an interval given in milliseconds as `<n>s` when it is a whole number
/// of seconds (the github case), else `<n>ms` (so a sub-second stub interval is
/// shown honestly rather than truncated to `0s`).
fn format_interval(interval_ms: u64) -> String {
    if interval_ms != 0 && interval_ms.is_multiple_of(1000) {
        format!("{}s", interval_ms / 1000)
    } else {
        format!("{interval_ms}ms")
    }
}

/// Parse a topic at the CLI edge so a bad topic is a clean usage error, not a
/// round-trip to the daemon.
fn parse_topic(raw: &str) -> anyhow::Result<Topic> {
    Topic::parse(raw).with_context(|| format!("invalid topic {raw:?}"))
}

/// Parse an `owner/repo#number` PR spec into a [`GithubPrTarget`], validating it
/// through the same [`GithubPr`] grammar the daemon uses so the error is caught
/// early and identically.
fn parse_pr_spec(spec: &str) -> anyhow::Result<GithubPrTarget> {
    let (owner, rest) = spec
        .split_once('/')
        .with_context(|| format!("PR spec {spec:?} must be owner/repo#number"))?;
    let (repo, number) = rest
        .rsplit_once('#')
        .with_context(|| format!("PR spec {spec:?} must be owner/repo#number"))?;
    let number: u64 = number
        .parse()
        .with_context(|| format!("PR number {number:?} must be a positive integer"))?;
    // Validate through the domain grammar (rejects empty/delimiter-bearing
    // segments and zero) so the CLI error matches the daemon's.
    GithubPr::new(owner, repo, number).with_context(|| format!("invalid PR spec {spec:?}"))?;
    Ok(GithubPrTarget {
        owner: owner.to_string(),
        repo: repo.to_string(),
        number,
    })
}

/// Validate a stub label through the same `stub.<label>` grammar the daemon uses,
/// so a bad label is caught early and identically. Returns the label unchanged on
/// success (the daemon rebuilds the topic from it).
fn parse_stub_label(label: &str) -> anyhow::Result<String> {
    stub_topic(label).with_context(|| format!("invalid stub label {label:?}"))?;
    Ok(label.to_string())
}

/// Dispatch a `harness` subcommand. `arm`/`cleanup` are socket clients; the two
/// `install-*` setup commands touch no bridge.
async fn run_harness(format: OutputFormat, args: HarnessArgs) -> anyhow::Result<()> {
    match args.command {
        HarnessCommand::Arm(args) => run_harness_arm(args).await,
        HarnessCommand::Cleanup => run_harness_cleanup().await,
        HarnessCommand::InstallHooks(args) => run_harness_install(format, args),
        HarnessCommand::InstallSkills(args) => run_harness_install_skills(format, args),
    }
}

/// The `SessionStart` / `Stop` hook: register this session's agent inbox, then
/// launch a waiter IFF the session is subscribed. Session identity comes from the
/// hook's stdin JSON (settled decision, card 11). Fail-safe: a down/erroring
/// bridge or a session with no subscriptions all skip arming (no wake).
///
/// **Always-on inbox (card 16 / ADR-0007).** Registration happens here, before the
/// probe, on every arm — so a session is addressable by its peers from its first
/// `SessionStart` with nothing for the agent to do. The consequence is intended:
/// every live session now has ≥1 subscription, so every live session arms a waiter
/// while the bridge is up. Every fail-safe is untouched: a bridge that is down or
/// erroring still skips arming (registration fails the same way the probe does),
/// and the waiter still re-checks `has_subscription` after taking its lock.
///
/// On the `Arm` path this **execs** `mailbox wait` (it does NOT write the
/// pidfile — the waiter writes it after taking the single-waiter lock, so a
/// doomed second arm can never overwrite the live waiter's pidfile; card 11
/// HIGH#1). A failure to exec exits **2** (a wake → the harness re-runs Stop and
/// re-arms) rather than exit 1 (a silent un-arm), first clearing any stale
/// pidfile (item E / ADR-0006).
async fn run_harness_arm(args: ArmArgs) -> anyhow::Result<()> {
    let config = StorageConfig::from_env().context("resolving storage path for harness arm")?;
    let session = HookInput::from_reader(std::io::stdin().lock())
        .context("reading the SessionStart/Stop hook payload from stdin")?
        .session_id;

    register_inbox(&config, &session).await;
    let probe = probe_subscription(&config, &session).await;
    match mailbox_harness::arm::decide(probe) {
        ArmDecision::Arm => {
            info!(
                session = %session.as_str(),
                max_block_ms = args.max_block_ms,
                "armed session (subscribed); exec-ing the waiter"
            );
            let exe = std::env::current_exe()
                .context("resolving the mailbox binary path to exec the waiter")?;
            // Never returns on success — the image becomes `mailbox wait`, which
            // writes the pidfile itself after acquiring the single-waiter lock.
            let err = mailbox_harness::arm::exec_waiter(&exe, session.as_str(), args.max_block_ms);
            error!(
                session = %session.as_str(),
                error = %err,
                "could not exec the waiter; waking to force a re-arm rather than silently un-arming"
            );
            let _ =
                std::fs::remove_file(mailbox::wake::pidfile_path(&config.waiters_dir(), &session));
            // Exit 2 (wake) so the harness re-runs Stop; std::process::exit skips
            // the anyhow→exit-1 mapping this async path would otherwise apply.
            std::process::exit(i32::from(mailbox::wake::WakeOutcome::EXIT_CODE));
        }
        ArmDecision::Skip(reason) => {
            info!(
                session = %session.as_str(),
                reason = reason.as_str(),
                "did not arm a waiter"
            );
            Ok(())
        }
    }
}

/// Ensure `session` is subscribed to its own inbox topic, over the socket
/// (always-on agent inboxes, ADR-0007).
///
/// Idempotent by construction: `arm` runs on every `Stop`, and `subscribe` is an
/// idempotent no-op that leaves an existing delivery cursor untouched — so a
/// re-arm can neither duplicate the subscription nor skip mail the agent has not
/// read yet. Baseline-on-subscribe applies on the FIRST registration, which is
/// exactly right: an agent is not shown messages sent before it existed.
///
/// Best-effort and silent on failure by design: this must never fail the hook. If
/// the bridge is down or errors, the probe that follows sees the same thing and
/// takes the fail-safe path (skip arming, no wake).
async fn register_inbox(config: &StorageConfig, session: &SessionId) {
    let topic = match inbox_topic(session) {
        Ok(topic) => topic,
        Err(err) => {
            // Permanent and actionable: this session id will NEVER be addressable,
            // so peers can never `send` to it. Logged at error so it survives the
            // harness.log default filter (there is no transient retry that fixes it).
            error!(
                session = %session.as_str(),
                error = %err,
                "session id cannot form an inbox topic; not registering an inbox (peers cannot address this session)"
            );
            return;
        }
    };
    let request = Request::Subscribe {
        session: session.clone(),
        topic: topic.clone(),
    };
    match client::send(&config.socket_path(), &request).await {
        // The guard refused this registration: the session ended within the
        // tombstone window (the arm-vs-cleanup race, ADR-0007). Honest, not a
        // silent success — logged at warn so it is visible in harness.log.
        Ok(Response::Subscribed {
            outcome: SubscribeState::RefusedSessionRecentlyEnded,
            ..
        }) => warn!(
            session = %session.as_str(),
            topic = %topic.as_str(),
            "did not register the agent inbox: session recently ended (tombstone guard); \
             it will re-register on a later arm if the session genuinely resumes"
        ),
        Ok(Response::Subscribed { outcome, .. }) => info!(
            session = %session.as_str(),
            topic = %topic.as_str(),
            outcome = %describe_sub(&outcome),
            "registered the session's agent inbox"
        ),
        Ok(Response::Error { message }) => warn!(
            session = %session.as_str(),
            error = %message,
            "bridge could not register the agent inbox; continuing (arming stays fail-safe)"
        ),
        Ok(other) => warn!(
            session = %session.as_str(),
            reply = ?other,
            "unexpected bridge reply while registering the agent inbox; continuing"
        ),
        Err(err) => warn!(
            session = %session.as_str(),
            error = %err,
            "bridge unreachable while registering the agent inbox; continuing (arming stays fail-safe)"
        ),
    }
}

/// Probe whether `session` has any subscriptions, over the socket. A clean
/// `Status` with subscriptions → `Subscribed`; a clean `Status` with none →
/// `NotSubscribed`; a serviced error/unexpected reply → `BridgeError`; an
/// unreachable bridge → `BridgeUnreachable`. The two failure cases are logged
/// distinctly (honest diagnostics) but both keep arming fail-safe (no wake).
async fn probe_subscription(config: &StorageConfig, session: &SessionId) -> SubscriptionProbe {
    let request = Request::Status {
        session: session.clone(),
    };
    match client::send(&config.socket_path(), &request).await {
        Ok(Response::Status(report)) if !report.subscriptions.is_empty() => {
            SubscriptionProbe::Subscribed
        }
        Ok(Response::Status(_)) => SubscriptionProbe::NotSubscribed,
        Ok(Response::Error { message }) => {
            warn!(session = %session.as_str(), error = %message, "bridge errored on the subscription probe; not arming");
            SubscriptionProbe::BridgeError
        }
        Ok(other) => {
            warn!(session = %session.as_str(), reply = ?other, "unexpected bridge reply to the subscription probe; not arming");
            SubscriptionProbe::BridgeError
        }
        Err(err) => {
            warn!(session = %session.as_str(), error = %err, "bridge unreachable on the subscription probe; not arming");
            SubscriptionProbe::BridgeUnreachable
        }
    }
}

/// The `SessionEnd` hook: reap the waiter (process half) and drop the session's
/// subscriptions + interests on the bridge (durable half, which stops any adapter
/// whose last interest this session held). Best-effort: a down bridge must not
/// fail the hook, and the waiter is reaped regardless.
///
/// A transient bridge failure is RETRIED a few times (brief backoff) so a
/// momentary blip does not leak the session's interest. If every attempt fails,
/// the card-08 TTL sweeper is the ultimate backstop: an un-torn-down interest
/// ages out via its `last_seen` (exactly the hard-died-session case it exists for)
/// — so we log and exit 0 rather than build a durable pending-end queue for MVP.
async fn run_harness_cleanup() -> anyhow::Result<()> {
    let config = StorageConfig::from_env().context("resolving storage path for harness cleanup")?;
    let session = HookInput::from_reader(std::io::stdin().lock())
        .context("reading the SessionEnd hook payload from stdin")?
        .session_id;

    let reap = mailbox_harness::cleanup::reap_waiter(&config.waiters_dir(), &session);
    info!(session = %session.as_str(), outcome = reap.as_str(), "reaped session waiter");

    end_session_with_retry(&config, &session).await;
    Ok(())
}

/// Number of `EndSession` attempts before deferring to the TTL sweeper.
const CLEANUP_END_SESSION_ATTEMPTS: u32 = 4;
/// Base backoff between `EndSession` retries (doubles each attempt).
const CLEANUP_END_SESSION_BACKOFF: std::time::Duration = std::time::Duration::from_millis(200);

/// Ask the bridge to end the session, retrying transient failures with backoff.
/// A serviced `Error` reply is NOT retried (it is deterministic); only an
/// unreachable/transport failure is. Never returns an error — cleanup is
/// best-effort, backstopped by the TTL sweeper.
async fn end_session_with_retry(config: &StorageConfig, session: &SessionId) {
    let request = Request::EndSession {
        session: session.clone(),
    };
    let mut backoff = CLEANUP_END_SESSION_BACKOFF;
    for attempt in 1..=CLEANUP_END_SESSION_ATTEMPTS {
        match client::send(&config.socket_path(), &request).await {
            Ok(Response::SessionEnded {
                subscriptions_dropped,
                interests_dropped,
                adapters_stopped,
            }) => {
                info!(
                    session = %session.as_str(),
                    attempt,
                    subscriptions_dropped,
                    interests_dropped,
                    adapters_stopped,
                    "ended session on the bridge (dropped interests/subscriptions)"
                );
                return;
            }
            Ok(Response::Error { message }) => {
                // Deterministic business failure — retrying will not help.
                warn!(session = %session.as_str(), error = %message, "bridge could not end session cleanly; TTL sweeper will reconcile");
                return;
            }
            Ok(_) => {
                warn!(session = %session.as_str(), "unexpected bridge reply to end-session; TTL sweeper will reconcile");
                return;
            }
            Err(err) if attempt < CLEANUP_END_SESSION_ATTEMPTS => {
                warn!(session = %session.as_str(), attempt, error = %err, backoff_ms = backoff.as_millis(), "bridge unreachable during cleanup; retrying");
                tokio::time::sleep(backoff).await;
                backoff *= 2;
            }
            Err(err) => {
                warn!(session = %session.as_str(), attempts = CLEANUP_END_SESSION_ATTEMPTS, error = %err, "bridge unreachable during cleanup; leaving interest for the TTL sweeper (waiter already reaped)");
                return;
            }
        }
    }
}

/// `install-hooks`: validate the timing, then merge the snippet into the Claude
/// Code settings file — `--settings <path>` if given, else `~/.claude/settings.json`
/// **when it exists** — and always print the snippet too.
///
/// The three outcomes come from ONE resolved
/// [`SettingsTarget`](mailbox_harness::install::SettingsTarget), matched
/// exhaustively below, so a print-only run always arrives with its reason. With no
/// settings file (and no `--settings`) this writes nothing at all: conjuring a
/// `settings.json` on a machine with no Claude Code is not ours to do.
fn run_harness_install(format: OutputFormat, args: InstallHooksArgs) -> anyhow::Result<()> {
    use mailbox_harness::install::{BackupPolicy, SettingsTarget};

    let mailbox_bin = match args.mailbox_bin {
        Some(path) => mailbox_harness::install::abs_bin(&path),
        None => mailbox_harness::install::default_mailbox_bin(std::env::current_exe()),
    };
    let spec = mailbox_harness::install::HookInstallSpec {
        mailbox_bin,
        timeout_secs: args.timeout_secs,
        max_block_ms: args.max_block_ms,
    };
    // Reject a max-block that would let Claude Code kill the waiter before it can
    // self-respawn (the load-bearing invariant, ADR-0006).
    spec.validate().context("invalid hook timing")?;
    let snippet = mailbox_harness::install::hooks_snippet(&spec);

    // Resolve the destination ONCE (env at this edge; the decision itself is pure).
    let target = mailbox_harness::install::settings_target(args.settings);

    // Emit the snippet BEFORE attempting the merge. It does not depend on the merge,
    // and a failed merge is precisely when the user needs it: hand-installation is
    // then their only route, so the fallback must not be suppressed by the failure
    // it exists for. In `--json` mode the snippet IS the stdout contract, so every
    // human note goes to stderr and never pollutes parseable stdout.
    if format.is_json() {
        println!("{}", serde_json::to_string(&snippet)?);
    } else {
        println!("{}", serde_json::to_string_pretty(&snippet)?);
    }

    match &target {
        SettingsTarget::Explicit(path) | SettingsTarget::DefaultFound(path) => {
            // The default path edits the user's real config with no confirmation, so
            // it keeps a `.bak`; an explicitly named file does not get littered.
            let backup = match &target {
                SettingsTarget::DefaultFound(_) => BackupPolicy::Keep,
                _ => BackupPolicy::Skip,
            };
            let report = mailbox_harness::install::merge_hooks_file(path, &snippet, backup)
                .with_context(|| {
                    format!(
                        "merging the agent-mailbox hooks into {} (your settings were NOT modified; the snippet above can be installed by hand)",
                        path.display()
                    )
                })?;
            note(format, &merged_note(&report));
        }
        SettingsTarget::NoDefault {
            looked_at: Some(path),
        } => note(
            format,
            &format!(
                "no Claude Code settings found at {}; printed the snippet instead — pass --settings <path> to create one",
                path.display()
            ),
        ),
        SettingsTarget::NoDefault { looked_at: None } => note(
            format,
            "no home to resolve Claude Code settings under (neither AGENT_MAILBOX_HOME nor HOME is set, or it is not absolute); printed the snippet instead — pass --settings <path> to choose one",
        ),
    }

    eprintln!("next: run `mailbox harness install-skills` to install the agent-mailbox skill");
    Ok(())
}

/// What a successful merge tells the user: where the hooks actually landed —
/// naming the *resolved* file when a symlink was followed, since a dotfiles user's
/// hooks land in their tracked repo, not at the path they typed — and the backup.
fn merged_note(report: &mailbox_harness::install::MergeReport) -> String {
    let mut message = format!(
        "merged agent-mailbox hooks into {}",
        report.written.display()
    );
    if let Some(link) = &report.via_symlink {
        message.push_str(&format!(
            " (via the symlink {}, which was followed, not replaced)",
            link.display()
        ));
    }
    if let Some(backup) = &report.backup {
        message.push_str(&format!(
            "; your previous settings are at {}",
            backup.display()
        ));
    }
    message
}

/// A human note that belongs on stdout — unless `--json` is on, where stdout is a
/// machine contract and the note would corrupt it, so it goes to stderr instead
/// (the same split `install-skills` makes for its pointer and warnings).
fn note(format: OutputFormat, message: &str) {
    if format.is_json() {
        eprintln!("{message}");
    } else {
        println!("{message}");
    }
}

/// `install-skills`: write every embedded skill to `<skills-dir>/<name>/SKILL.md`
/// (atomically), reporting per skill whether it was created / updated / unchanged
/// / replaced-symlink.
///
/// The skill body is compiled into this binary, so the command works with no repo
/// checked out — see `mailbox_harness::skills` for why it is embedded, why an
/// unreadable existing skill is repaired rather than fatal, and why the write is
/// atomic.
///
/// A failure still renders what DID install before returning non-zero: with more
/// than one skill, "it failed" without naming what landed is not actionable.
fn run_harness_install_skills(format: OutputFormat, args: InstallSkillsArgs) -> anyhow::Result<()> {
    let skills_dir = match args.skills_dir {
        Some(dir) => dir,
        None => mailbox_harness::skills::default_skills_dir()
            .context("resolving the default skills directory")?,
    };

    match mailbox_harness::skills::install_skills(&skills_dir) {
        Ok(report) => {
            render_skill_report(format, &report)?;
            eprintln!("next: run `mailbox harness install-hooks` to wire the wake hooks");
            Ok(())
        }
        Err(err) => {
            // Report the skills that DID land before failing, so a partial install
            // is never invisible (this is empty today, and won't be once a second
            // skill exists). Each failure is logged with its own cause, because the
            // error chain only carries the first.
            render_skill_report(format, &err.installed)?;
            for failure in &err.failures {
                error!(
                    skill = %failure.name,
                    error = %failure.error,
                    "could not install skill"
                );
            }
            Err(anyhow::Error::from(err))
                .with_context(|| format!("installing skills into {}", skills_dir.display()))
        }
    }
}

/// Render an install report.
///
/// In `--json` mode the report IS the stdout contract, so it is always printed
/// (even when empty) and nothing else goes to stdout. In human mode an empty
/// report prints nothing — on a failed install the error is the message, and
/// "installed 0 skill(s)" would just be noise above it.
///
/// A replaced symlink is warned about on stderr in both modes: a user who
/// deliberately symlinked their SKILL.md into a checkout needs to be told it is
/// now a plain copy.
fn render_skill_report(
    format: OutputFormat,
    report: &mailbox_harness::skills::InstallReport,
) -> anyhow::Result<()> {
    use mailbox_harness::skills::SkillOutcome;

    for skill in &report.skills {
        info!(
            skill = %skill.name,
            path = %skill.path.display(),
            outcome = skill.outcome.as_str(),
            "installed skill"
        );
    }

    if format.is_json() {
        println!("{}", serde_json::to_string(report)?);
    } else if !report.skills.is_empty() {
        for skill in &report.skills {
            println!(
                "{} {} -> {}",
                skill.outcome.as_str(),
                skill.name,
                skill.path.display()
            );
        }
        println!(
            "installed {} skill(s) into {}",
            report.skills.len(),
            report.skills_dir.display()
        );
    }

    for skill in &report.skills {
        if skill.outcome == SkillOutcome::ReplacedSymlink {
            eprintln!(
                "warning: {} was a symlink and is now a regular file ({}); any live-edit link into a checkout is gone",
                skill.name,
                skill.path.display()
            );
        }
    }
    Ok(())
}

/// Run `wait` synchronously (no tokio runtime): open the read-only store and
/// block on the FIFO. Finalizes the card-05 PROVISIONAL command into its real
/// shape — `mailbox wait --session <id>` — with the same exit-code contract.
///
/// Exit codes: `2` = the session has mail (wake it; reminder on stderr);
/// `1` = a waiter error, including an unresolvable session (no `--session` and no
/// session env var — a waiter with no identity has nothing to wait on).
///
/// With `--max-block-ms`, a block that elapses with no mail re-execs a FRESH
/// waiter (same PID) rather than returning — the self-respawn that keeps a long
/// idle armed without Claude Code's per-hook timeout ever killing a live wait
/// (card 11). Without it, `wait` blocks indefinitely (the card-05 contract).
pub fn run_wait(args: &WaitArgs) -> ExitCode {
    let config = match StorageConfig::from_env() {
        Ok(config) => config,
        Err(err) => {
            eprintln!("mailbox wait: {err}");
            return ExitCode::FAILURE;
        }
    };

    let session = match args.session.resolve() {
        Ok(session) => session,
        Err(err) => {
            eprintln!("mailbox wait: {err:#}");
            return ExitCode::FAILURE;
        }
    };
    let session = &session;
    let waiter = Waiter::new(
        config.waiters_dir(),
        config.path().to_path_buf(),
        session.clone(),
    );
    let max_block = args.max_block_ms.map(std::time::Duration::from_millis);

    match waiter.wait(max_block) {
        Ok(WaitOutcome::Woken(outcome)) => {
            // Payload-free reminder — topic names only — is what the harness
            // surfaces as a system reminder (docs/01-wake-and-rearm.md).
            eprintln!("{}", outcome.reminder());
            if wait_debug_enabled() {
                eprintln!("wake reason: {}", outcome.reason().as_str());
            }
            ExitCode::from(WakeOutcome::EXIT_CODE)
        }
        // The session unsubscribed (or a SessionEnd raced this arm): nothing to
        // wake about. The waiter already dropped its pidfile; exit 0, no wake.
        Ok(WaitOutcome::Unsubscribed) => ExitCode::SUCCESS,
        Ok(WaitOutcome::TimedOut { budget }) => {
            // Re-exec a fresh waiter. exec preserves the PID (so the pidfile stays
            // valid) and gives the harness a fresh process to reset its async-hook
            // timeout against; the fresh waiter's check-then-block catches any
            // publish that landed during the exec gap. `budget` carries the same
            // max-block forward.
            let ms = u64::try_from(budget.as_millis()).unwrap_or(u64::MAX);
            reexec_or_wake(&waiter, session, ms)
        }
        Err(err) => {
            error!(session = %session.as_str(), error = %err, "waiter failed");
            eprintln!("mailbox wait: {err}");
            ExitCode::FAILURE
        }
    }
}

/// Re-exec a fresh waiter (self-respawn). On a re-exec FAILURE we exit **2**, not
/// 1: exit 2 is a wake, so the harness re-runs `Stop` and re-arms — a silent
/// un-arm (exit 1, no wake) would be worse than a spurious wake. Any stale pidfile
/// is removed first so a later arm/cleanup does not chase a dead pid (card 11,
/// item E / ADR-0006).
fn reexec_or_wake(waiter: &Waiter, session: &SessionId, max_block_ms: u64) -> ExitCode {
    let exe = match std::env::current_exe() {
        Ok(exe) => exe,
        Err(err) => {
            error!(session = %session.as_str(), error = %err, "could not resolve binary to re-exec");
            let _ = std::fs::remove_file(waiter.pidfile_path());
            eprintln!("mailbox wait: could not resolve binary to re-exec: {err}");
            return ExitCode::from(WakeOutcome::EXIT_CODE);
        }
    };
    // exec only returns on failure.
    let err = mailbox_harness::arm::exec_waiter(&exe, session.as_str(), max_block_ms);
    error!(session = %session.as_str(), error = %err, "could not re-exec waiter; waking instead of silently un-arming");
    let _ = std::fs::remove_file(waiter.pidfile_path());
    eprintln!("mailbox wait: could not re-exec waiter: {err}");
    ExitCode::from(WakeOutcome::EXIT_CODE)
}

fn wait_debug_enabled() -> bool {
    std::env::var(WAIT_DEBUG_ENV).is_ok_and(|v| v == "1")
}

/// Convenience for `main`: turn the `--json` flag into an [`OutputFormat`].
pub fn output_format(json: bool) -> OutputFormat {
    OutputFormat::from_json_flag(json)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_flag_beats_both_env_vars() {
        let resolved = resolve_session(
            Some(SessionId::new("from-flag")),
            Some("from-mailbox-env".to_string()),
            Some("from-claude-env".to_string()),
        )
        .unwrap();
        assert_eq!(resolved, SessionId::new("from-flag"));
    }

    #[test]
    fn mailbox_env_beats_claude_env() {
        // The harness hooks set MAILBOX_SESSION_ID deliberately; Claude Code's own
        // CLAUDE_CODE_SESSION_ID is the LAST resort, so it must not win.
        let resolved = resolve_session(
            None,
            Some("from-mailbox-env".to_string()),
            Some("from-claude-env".to_string()),
        )
        .unwrap();
        assert_eq!(resolved, SessionId::new("from-mailbox-env"));
    }

    #[test]
    fn claude_env_is_the_last_fallback() {
        let resolved = resolve_session(None, None, Some("from-claude-env".to_string())).unwrap();
        assert_eq!(resolved, SessionId::new("from-claude-env"));
    }

    #[test]
    fn no_session_anywhere_is_an_actionable_error() {
        let err = resolve_session(None, None, None).unwrap_err();
        let message = format!("{err}");
        // The error must name every place we looked, or the agent cannot fix it.
        assert!(message.contains("--session"), "{message}");
        assert!(message.contains("MAILBOX_SESSION_ID"), "{message}");
        assert!(message.contains("CLAUDE_CODE_SESSION_ID"), "{message}");
    }

    #[test]
    fn send_target_accepts_a_bare_session_or_a_full_inbox_topic() {
        assert_eq!(parse_send_target("s-b").unwrap(), SessionId::new("s-b"));
        assert_eq!(
            parse_send_target("agent.s-b").unwrap(),
            SessionId::new("s-b")
        );
        // A dotted session id survives both spellings identically.
        assert_eq!(parse_send_target("a.b").unwrap(), SessionId::new("a.b"));
        assert_eq!(
            parse_send_target("agent.a.b").unwrap(),
            SessionId::new("a.b")
        );
    }

    #[test]
    fn send_target_rejects_an_unaddressable_target() {
        // A malformed inbox topic is an error, not a session id literally named
        // "agent." — the user clearly meant an inbox.
        assert!(parse_send_target("agent.").is_err());
        // A session id that cannot form a topic at all.
        assert!(parse_send_target("has space").is_err());
        assert!(parse_send_target("a/b").is_err());
    }

    #[test]
    fn send_body_builds_from_text_or_a_json_object() {
        let from_text = send_body(Some("hello".to_string()), None).unwrap();
        assert_eq!(from_text["text"], serde_json::json!("hello"));

        let from_body = send_body(None, Some(r#"{"kind":"review-done"}"#.to_string())).unwrap();
        assert_eq!(from_body["kind"], serde_json::json!("review-done"));

        // A bare poke is allowed: the `from` stamp the bridge adds is enough.
        assert!(send_body(None, None).unwrap().is_empty());
    }

    #[test]
    fn send_body_rejects_a_non_object_body() {
        // There would be nowhere to stamp `from` on a scalar or an array.
        for bad in ["[1,2]", "\"just a string\"", "7", "not json at all"] {
            assert!(
                send_body(None, Some(bad.to_string())).is_err(),
                "expected {bad:?} to be rejected"
            );
        }
    }
}
