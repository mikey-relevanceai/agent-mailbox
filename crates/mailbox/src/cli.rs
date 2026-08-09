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

use mailbox::storage::{SessionId, StorageConfig, SubscribeKind};
use mailbox_harness::hook::HookInput;
use mailbox_protocol::{AdapterId, GithubPr, Topic, inbox_topic, stub_topic};

use crate::client;
use crate::control::{
    AgentSummary, GithubPrTarget, Request, Response, StatusReport, SubscribeState, TopicStatus,
    UnwatchResultWire, WatchKindWire, WatchStateWire,
};
use crate::serve;

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
/// The version `mailbox --version` reports: the crate version plus the commit it
/// was built from (and `-dirty` for an uncommitted tree), baked in by `build.rs`.
/// Lets an installed binary be traced to a commit — a plain crate version cannot
/// distinguish a fresh build from a stale one.
pub const LONG_VERSION: &str = env!("MAILBOX_LONG_VERSION");

#[derive(Parser, Debug)]
#[command(name = "mailbox", version = LONG_VERSION, about, long_about = None)]
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
    /// Run the long-lived bridge daemon (owns the single writer, the socket, and
    /// the sentinel writes that wake idle sessions).
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
    /// Show this session's identity (id + inbox topic), its watches, its
    /// subscriptions and its unread counts. The identity half needs no bridge.
    Status,
    /// Message a peer agent: publish to its inbox, stamped with your session id if
    /// you are a session (a human's message carries no reply address).
    Send(SendArgs),
    /// List the agents with a registered inbox (who you can `send` to). Marks which
    /// one is you, when you are one of them.
    Agents,
    /// List known topics with their subscriber and event counts.
    Topics(TopicsArgs),
    /// Actively prove which sessions can be woken right now, by bumping each
    /// sentinel and requiring the wake hook to answer.
    Doctor(DoctorArgs),
    /// Claude Code hook handlers and setup (session-start / wake / turn-end /
    /// turn-start / cleanup / install-hooks / install-skills). The harness owns the
    /// wake loop so the agent never re-arms.
    Harness(HarnessArgs),
}

/// The env var **Claude Code exports into every tool invocation**, carrying the id of
/// the session that ran the command. It is the SINGLE source of a session's own
/// identity, so an agent learns who it is with nothing installed but the binary, and
/// there is exactly one answer to "who am I" rather than a precedence order.
///
/// There used to be a `--session` flag and a `MAILBOX_SESSION_ID` fallback ahead of
/// this. Nothing in production set either: the harness hooks read `session_id` from
/// the hook payload on stdin, adapters have no session at all, and no command acts
/// *as* another session. What the flag did do is let an agent break itself — the skill
/// carried a whole section warning against `--session "$MAILBOX_SESSION_ID"`, which in
/// an agent's shell expands to `--session ""` and bound a phantom empty session.
///
/// `mailbox doctor --session <id>` survives and is a different thing: it names another
/// session to PROBE, not an identity to act as.
const ENV_CLAUDE_SESSION: &str = "CLAUDE_CODE_SESSION_ID";

/// Resolve the calling session's own id from the environment, branding it into a
/// [`SessionId`] once here at the edge (parse, don't validate), or fail with a message
/// naming where it looked.
///
/// An empty or whitespace-only value names no session, so it is treated as absent
/// rather than binding a phantom session id.
fn resolve_session() -> anyhow::Result<SessionId> {
    resolve_session_optional().context(
        "no session id: this command must run inside a Claude Code session, which sets \
         CLAUDE_CODE_SESSION_ID. To run it by hand, set that variable yourself \
         (CLAUDE_CODE_SESSION_ID=<id> mailbox ...)",
    )
}

/// Resolve the calling session if the environment names one, WITHOUT failing when
/// it does not.
///
/// For the commands where a session is a nicety rather than the point. `send` uses
/// it to stamp a reply address; `agents` uses it to mark which row is the caller;
/// `doctor` uses it to warn that the caller cannot measure itself. None of the three
/// is *about* the caller, so refusing to run without one would refuse the human
/// manual-poke workflow — look at the fleet, poke an agent — for the sake of a field
/// that command does not need. Commands that genuinely are about the caller (`read`,
/// `status`, `subscribe`, `unsubscribe`, `watch`, `unwatch`) use
/// [`resolve_session_or_fail`] instead, because "whose?" is their whole content.
fn resolve_session_optional() -> Option<SessionId> {
    session_from_env_value(&std::env::var(ENV_CLAUDE_SESSION).unwrap_or_default())
}

/// The identity rule itself, as a pure function of the raw env value so it is
/// unit-testable without mutating process env. An empty or whitespace-only value
/// names NO session — it must never bind a phantom empty session id.
fn session_from_env_value(raw: &str) -> Option<SessionId> {
    let trimmed = raw.trim();
    (!trimmed.is_empty()).then(|| SessionId::new(trimmed))
}

/// Wrap a raw session label into a [`SessionId`]. Infallible — the harness owns
/// the label's grammar — but expressed as a `value_parser` so clap brands it.
fn parse_session(raw: &str) -> Result<SessionId, Infallible> {
    Ok(SessionId::new(raw))
}

/// The `harness` command group: Claude Code hook targets.
#[derive(Args, Debug)]
pub struct HarnessArgs {
    #[command(subcommand)]
    pub command: HarnessCommand,
}

#[derive(Subcommand, Debug)]
pub enum HarnessCommand {
    /// SessionStart hook: register this session's always-on agent inbox so peers can
    /// address it (ADR-0007). Exits 0 always; it can never wake the session.
    SessionStart,
    /// SessionEnd hook: drop this session's interests/subscriptions, so no poller
    /// outlives the session that wanted it.
    Cleanup,
    /// Merge the hooks into the Claude Code settings.json (and print the snippet).
    InstallHooks(InstallHooksArgs),
    /// Install the embedded agent-mailbox skill into the Claude Code skills dir.
    InstallSkills(InstallSkillsArgs),
    /// Let a bypassPermissions session RECEIVE mailbox wakes on its inbox socket, by
    /// setting `crossSessionInbound: "accept"` (ADR-0020). Opt-in, never implicit.
    InstallInbound(InstallInboundArgs),
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
}

/// Arguments to `install-inbound`.
///
/// Deliberately minimal: there is exactly one thing this command does, and no flag
/// to make it do something weaker. If you do not want `accept`, do not run it.
#[derive(Args, Debug)]
pub struct InstallInboundArgs {
    /// settings.json to set the policy in, created if missing. Defaults to
    /// `~/.claude/settings.json` (home from `AGENT_MAILBOX_HOME`, else `HOME`) IF
    /// that file exists. Unrelated settings are always preserved.
    ///
    /// Prefer a per-session `--settings` file over your user settings: a user-level
    /// `accept` applies to EVERY session you run, not only the ones that subscribe.
    #[arg(long)]
    pub settings: Option<std::path::PathBuf>,
}

#[derive(Args, Debug)]
pub struct InstallSkillsArgs {
    /// Directory to install the skill(s) into. Defaults to `~/.claude/skills`
    /// (home from `AGENT_MAILBOX_HOME`, else `HOME`). Each skill lands at
    /// `<skills-dir>/<name>/SKILL.md`; nothing else is touched.
    #[arg(long)]
    pub skills_dir: Option<std::path::PathBuf>,
}

/// Arguments to `publish`.
///
/// It takes no session, by design (ADR-0018). `publish` has ONE rule — the event goes
/// to the topic and wakes every subscriber, its author included — so who is calling
/// changes nothing, and an adapter, an agent and a script an agent spawned all use
/// the same command with the same effect.
///
/// This is why there is no `--no-session` flag any more. It existed because the
/// caller's identity WAS load-bearing: Claude Code exports `$CLAUDE_CODE_SESSION_ID`
/// into every process an agent spawns, so a build script's publish was attributed to
/// the agent — which (under the old rules) gagged it behind the agent's unread mail
/// and did not wake the agent. Neither rule survives, so neither does the escape
/// hatch from them.
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
}

/// Arguments to `send`: who to message, and what to say.
///
/// `--text` and `--body` are mutually exclusive: `--text` IS the shorthand for
/// `--body '{"text": "..."}'`, so accepting both would only raise the question of
/// which wins. Neither is also fine — a bare `send <target>` is a poke, and a peer
/// sending one is still identified by the `from` the bridge stamps. A HUMAN's poke
/// carries no `from` at all, so an empty body says genuinely nothing; give it a
/// `--text` if the agent is meant to act on something in particular.
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
}

#[derive(Args, Debug)]
pub struct TopicsArgs {
    /// Only list topics starting with this prefix (e.g. `agent.`, `github.pr.`).
    #[arg(long)]
    pub prefix: Option<String>,
}

#[derive(Args, Debug)]
pub struct DoctorArgs {
    /// Report only this session, instead of every session Claude Code has registered.
    #[arg(long, value_parser = parse_session)]
    pub session: Option<SessionId>,
    /// List every session, not just the faults.
    #[arg(long)]
    pub all: bool,
}

#[derive(Args, Debug)]
pub struct ReadArgs {
    /// Maximum events per topic to return (bridge default if omitted).
    #[arg(long)]
    pub limit: Option<u32>,
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
    /// Watch a stub publisher (a built-in test adapter). Publishes a synthetic
    /// event on an interval to prove the whole path end to end.
    Stub(StubWatchArgs),
}

#[derive(Args, Debug)]
pub struct GithubPrWatchArgs {
    /// PR reference: `owner/repo#number`.
    pub spec: String,
    /// Poll interval in seconds (the supervised adapter's poll cadence).
    #[arg(long, default_value_t = 60)]
    pub interval: u64,
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
}

#[derive(Args, Debug)]
pub struct StubUnwatchArgs {
    /// Stub label previously passed to `watch stub`.
    pub label: String,
}

/// Run an async command (everything except `wait`). Returns the process exit code on
/// success — [`ExitCode::SUCCESS`] for almost everything, but `publish` has a REFUSED
/// outcome that is neither success nor failure and gets [`PUBLISH_REFUSED_EXIT`]. The
/// caller maps `Err` to a plain non-zero exit.
pub async fn run(format: OutputFormat, command: Command) -> anyhow::Result<ExitCode> {
    match command {
        Command::Serve => run_serve().await,
        Command::Publish(args) => run_publish(format, args).await,
        Command::Subscribe(args) => run_subscribe(format, args).await,
        Command::Unsubscribe(args) => run_unsubscribe(format, args).await,
        Command::Read(args) => run_read(format, args).await,
        Command::Watch(args) => run_watch(format, args).await,
        Command::Unwatch(args) => run_unwatch(format, args).await,
        Command::Status => run_status(format).await,
        Command::Send(args) => run_send(format, args).await,
        Command::Agents => run_agents(format).await,
        Command::Topics(args) => run_topics(format, args).await,
        // `doctor` is dispatched synchronously by `main` (it is socket-free and
        // read-only) and never reaches here.
        Command::Doctor(_) => unreachable!("doctor is handled synchronously in main"),
        Command::Harness(args) => run_harness(format, args).await,
    }
}

async fn run_serve() -> anyhow::Result<ExitCode> {
    let config = StorageConfig::from_env().context("resolving storage path for serve")?;
    serve::run(config).await?;
    Ok(ExitCode::SUCCESS)
}

/// `publish`: append an event to a topic, waking every subscriber to it.
///
/// One rule, no caller-aware behaviour (ADR-0018): the publish does not resolve a
/// session, so it cannot be refused, gagged, or filtered by who ran it.
async fn run_publish(format: OutputFormat, args: PublishArgs) -> anyhow::Result<ExitCode> {
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

async fn run_subscribe(format: OutputFormat, args: TopicArgs) -> anyhow::Result<ExitCode> {
    let topic = parse_topic(&args.topic)?;
    request(
        format,
        Request::Subscribe {
            session: resolve_session_or_fail(format)?,
            topic,
            // An explicit `mailbox subscribe` from a live turn: unguarded, and it
            // clears any tombstone (proof-of-life, ADR-0007). Only the automatic
            // inbox re-registration below takes the guarded AutoInbox path.
            kind: SubscribeKind::Explicit,
        },
    )
    .await
}

async fn run_unsubscribe(format: OutputFormat, args: TopicArgs) -> anyhow::Result<ExitCode> {
    let topic = parse_topic(&args.topic)?;
    request(
        format,
        Request::Unsubscribe {
            session: resolve_session_or_fail(format)?,
            topic,
        },
    )
    .await
}

async fn run_read(format: OutputFormat, args: ReadArgs) -> anyhow::Result<ExitCode> {
    request(
        format,
        Request::Read {
            session: resolve_session_or_fail(format)?,
            limit: args.limit,
        },
    )
    .await
}

/// `send`: message a peer agent. The body the bridge publishes is
/// `{"from": "<sender>", ...}` — see [`mailbox::agents`] for the convention and
/// for why an unregistered target is a hard error rather than a silent publish.
///
/// **The session is optional here** (see [`resolve_session_optional`]). `send`'s job
/// is to deliver; `from` is only the reply address stamped on the way. A human in an
/// ordinary terminal has no session id and so no address to be replied to — and a
/// human poking an agent is a workflow this bridge exists to support, so the message
/// goes with no `from` and the sender is told that on stderr.
async fn run_send(format: OutputFormat, args: SendArgs) -> anyhow::Result<ExitCode> {
    let from = resolve_session_optional();
    let to = parse_send_target(&args.target)?;
    let body = send_body(args.text, args.body)?;
    if from.is_none() {
        // stderr in BOTH modes: `--json` stdout is a machine contract, and in human
        // mode this is a caveat about the send, not its result.
        eprintln!(
            "note: no CLAUDE_CODE_SESSION_ID, so this message carries no `from` and \
             {} cannot reply to it. That is normal when a human pokes an agent from a \
             terminal — say who you are in the text if you want an answer.",
            to.as_str()
        );
    }
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
        // A bare poke. From a peer it still carries the `from` stamp; from a human it
        // carries nothing at all, which is a legitimate "go look" nudge.
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

/// `agents`: who has a registered inbox, i.e. who can be `send` to.
///
/// **The session is optional here** (see [`resolve_session_optional`]). The caller's
/// only effect on this listing is which row is marked `<- you`; with no session,
/// every agent is still listed and no row is marked. Refusing to answer "who exists"
/// to a human at a terminal would be refusing the question, not protecting anything.
async fn run_agents(format: OutputFormat) -> anyhow::Result<ExitCode> {
    request(
        format,
        Request::Agents {
            session: resolve_session_optional(),
        },
    )
    .await
}

async fn run_topics(format: OutputFormat, args: TopicsArgs) -> anyhow::Result<ExitCode> {
    request(
        format,
        Request::Topics {
            prefix: args.prefix,
        },
    )
    .await
}

async fn run_watch(format: OutputFormat, args: WatchArgs) -> anyhow::Result<ExitCode> {
    let req = match args.target {
        WatchTargetCmd::GithubPr(gh) => Request::Watch {
            session: resolve_session_or_fail(format)?,
            target: parse_pr_spec(&gh.spec)?,
            interval_secs: gh.interval,
        },
        WatchTargetCmd::Stub(stub) => Request::WatchStub {
            session: resolve_session_or_fail(format)?,
            // Validate the label at the edge (same as the daemon) so a bad label
            // is a clean local error, not a round-trip.
            label: parse_stub_label(&stub.label)?,
            interval_ms: stub.interval_ms,
            count: stub.count,
        },
    };
    request(format, req).await
}

async fn run_unwatch(format: OutputFormat, args: UnwatchArgs) -> anyhow::Result<ExitCode> {
    let req = match args.target {
        UnwatchTargetCmd::GithubPr(gh) => Request::Unwatch {
            session: resolve_session_or_fail(format)?,
            target: parse_pr_spec(&gh.spec)?,
        },
        UnwatchTargetCmd::Stub(stub) => Request::UnwatchStub {
            session: resolve_session_or_fail(format)?,
            label: parse_stub_label(&stub.label)?,
        },
    };
    request(format, req).await
}

/// `status`: this session's identity, plus everything the bridge knows about it.
///
/// **The identity half never depends on the bridge.** A session's id and its inbox
/// topic are derivable locally, so when the daemon is down `status` still answers
/// "who am I, and what is my address" — and says plainly that the rest (watches,
/// subscriptions, unread counts) is unknown because the bridge is unreachable. That is
/// what the separate `whoami` command used to be for; it was otherwise a strict subset
/// of this output, so it is gone.
///
/// It still exits NON-ZERO when the bridge is down (ADR-0004: socket clients fail
/// loud). The degradation is in what it can tell you, not in whether it admits the
/// failure — most of what `status` reports is genuinely missing, and exiting 0 would
/// report "fine" for a command whose primary content is absent.
async fn run_status(format: OutputFormat) -> anyhow::Result<ExitCode> {
    let session = resolve_session_or_fail(format)?;
    let request = Request::Status {
        session: session.clone(),
    };
    let config = StorageConfig::from_env().context("resolving storage path")?;

    let response = match client::send(&config.socket_path(), &request).await {
        Ok(response) => response,
        Err(err) => return Err(status_without_bridge(format, &session, &err.to_string())),
    };

    if let Response::Error { message } = &response {
        return Err(fail(format, message));
    }
    if format.is_json() {
        println!("{}", serde_json::to_string(&response)?);
    } else {
        render_human(&response);
    }
    Ok(ExitCode::SUCCESS)
}

/// Render the bridge-down `status`: the identity fields that are always knowable, and
/// an explicit statement that the bridge could not be reached.
///
/// In JSON mode this is ONE object carrying both — the `result: "error"` shape a
/// `--json` consumer already expects, with the identity keys added — rather than an
/// identity object followed by an error object, which would make stdout two documents.
fn status_without_bridge(
    format: OutputFormat,
    session: &SessionId,
    message: &str,
) -> anyhow::Error {
    let inbox = inbox_topic(session).ok();
    if format.is_json() {
        println!(
            "{}",
            serde_json::json!({
                "result": "error",
                "message": message,
                "session": session.as_str(),
                "inbox_topic": inbox.as_ref().map(Topic::as_str),
                "bridge": "unreachable",
            })
        );
    } else {
        println!("session: {}", session.as_str());
        match &inbox {
            Some(inbox) => println!("inbox: {}", inbox.as_str()),
            None => println!("inbox: none (this session id cannot form an inbox topic)"),
        }
        println!(
            "bridge: UNREACHABLE — watches, subscriptions and unread counts are unknown \
             (start it with `mailbox serve`)"
        );
    }
    anyhow::anyhow!("{message}").context(format!("status for {}", session.as_str()))
}

/// Send one request to the daemon and render the reply.
///
/// Three outcomes, kept distinct so an agent (and a human) can tell them apart:
/// - success → render the typed response (JSON or human text) to stdout;
/// - a serviced request that failed ([`Response::Error`]) → render the message;
/// - the bridge is down / unreachable ([`client::ClientError`]) → the actionable
///   "start it with `mailbox serve`" error, tagged with the op/session it was
///   trying to run (F13). Both failure paths exit non-zero.
async fn request(format: OutputFormat, request: Request) -> anyhow::Result<ExitCode> {
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
    Ok(ExitCode::SUCCESS)
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
fn resolve_session_or_fail(format: OutputFormat) -> anyhow::Result<SessionId> {
    resolve_session().map_err(|err| fail(format, &format!("{err:#}")))
}

/// A short "what was being attempted" label for a failed request, for the stderr
/// context chain (the only thing the user sees when the bridge is down).
fn request_context(request: &Request) -> String {
    match request {
        Request::Publish { topic, .. } => format!("publishing to {}", topic.as_str()),
        Request::Subscribe { session, topic, .. } => {
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
        // An unattributed send is a real case (a human at a terminal), so it says so
        // rather than printing an empty "sending from  to X".
        Request::Send { from, to, .. } => match from {
            Some(from) => format!("sending from {} to {}", from.as_str(), to.as_str()),
            None => format!("sending to {} (no sender session)", to.as_str()),
        },
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
/// The liveness column is stated in full rather than as a bare `live`/`idle` flag,
/// because it is easy to over-read. It means "a Claude Code process is still running
/// this session", which is NOT "this agent is idle" and NOT "this agent can be
/// woken" — a live agent may be mid-turn, and `mailbox doctor` is the only thing
/// that proves wakeability. A `send` to an agent that is not running still lands
/// durably, so the line says so rather than leaving the reader to guess.
///
/// The `<- you` marker is simply absent when the caller is not a session (a human at
/// a terminal), which is honest: there is no row to mark.
fn render_agents(agents: &[AgentSummary]) {
    if agents.is_empty() {
        println!("no agents registered (nobody is addressable yet)");
        return;
    }
    println!("{} agent(s):", agents.len());
    for agent in agents {
        let liveness = if agent.live {
            "running (a send reaches it; `mailbox doctor` proves it can be woken)"
        } else {
            "not running (a send still lands in its inbox)"
        };
        let me = if agent.is_self { "  <- you" } else { "" };
        println!(
            "  {}  inbox={}  {}{}",
            agent.session.as_str(),
            agent.inbox.as_str(),
            liveness,
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
        // The count leads the list so the human line carries the same number
        // `--json`'s `subscription_count` does, rather than making a reader tally
        // the rows themselves.
        println!("subscriptions ({}):", report.subscription_count);
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

/// Dispatch a `harness` subcommand. `session-start`, `cleanup`, and `turn-end` are
/// socket clients (the last two re-register the inbox / end the session); the two
/// `install-*` setup commands touch no bridge.
async fn run_harness(format: OutputFormat, args: HarnessArgs) -> anyhow::Result<ExitCode> {
    match args.command {
        HarnessCommand::SessionStart => run_harness_session_start().await,
        HarnessCommand::Cleanup => run_harness_cleanup().await,
        HarnessCommand::InstallHooks(args) => run_harness_install(format, args),
        HarnessCommand::InstallSkills(args) => run_harness_install_skills(format, args),
        HarnessCommand::InstallInbound(args) => run_harness_install_inbound(format, args),
        // `wake` is dispatched synchronously by `main` (a read-only peek needs no tokio
        // runtime) and never reaches here. `turn-end` re-registers the inbox over the
        // socket, so it IS dispatched here.
    }
}

/// The `SessionStart` hook: make this session addressable by its peers.
///
/// Wired with matcher `""`, so it fires on EVERY SessionStart source — `startup` AND
/// `resume`/`clear`/`compact` (ADR-0013). A resume is a fresh process that has lost
/// its predecessor's inbox registration, and running this on resume re-establishes
/// it. Idempotent, so re-running on a live session (a `compact`, say) is a no-op.
///
/// # What it no longer does
///
/// It used to also arm a wake sentinel and print a `watchPaths` registration for it.
/// Both are gone with the sentinel channel (ADR-0021): a session is woken through the
/// inbox socket Claude Code binds for it, which needs nothing armed, nothing watched
/// and nothing registered. That also removes the ordering constraint this hook was
/// built around — the file had to exist before the watch went on it, or the first
/// write would be a CREATE the watch might never deliver.
///
/// **Fail-open, and never a wake.** A down bridge skips the registration and still
/// exits 0; there is no exit-2 anywhere on this path.
async fn run_harness_session_start() -> anyhow::Result<ExitCode> {
    let config =
        StorageConfig::from_env().context("resolving storage path for harness session-start")?;
    let session = HookInput::from_reader(std::io::stdin().lock())
        .context("reading the SessionStart hook payload from stdin")?
        .session_id;

    register_inbox(&config, &session, "session-start").await;
    Ok(ExitCode::SUCCESS)
}

/// Ensure `session` is subscribed to its own inbox topic, over the socket
/// (always-on agent inboxes, ADR-0007).
///
/// `source` names the hook that called us (`"session-start"`, `"turn-end"`) and
/// rides every log line: since ADR-0013 both `session-start` (on a resume) and
/// `turn-end` (every `Stop`) re-register the inbox, and an operator diagnosing "why
/// didn't my resumed agent wake?" needs to tell a resume's SessionStart
/// re-registration apart from a Stop healing a lapsed one.
///
/// Idempotent by construction: `turn-end` runs on every `Stop`, and
/// `subscribe` is an idempotent no-op that leaves an existing delivery cursor
/// untouched — so a re-registration can neither duplicate the subscription nor
/// skip mail the agent has not read yet. Baseline-on-subscribe applies on the
/// FIRST registration, which is exactly right: an agent is not shown messages sent
/// before it existed.
///
/// Best-effort and silent on failure by design: this must never fail the hook. If
/// the bridge is down or errors, the probe that follows sees the same thing and
/// takes the fail-safe path (skip arming, no wake).
async fn register_inbox(config: &StorageConfig, session: &SessionId, source: &'static str) {
    let topic = match inbox_topic(session) {
        Ok(topic) => topic,
        Err(err) => {
            // Permanent and actionable: this session id will NEVER be addressable,
            // so peers can never `send` to it. Logged at error so it survives the
            // harness.log default filter (there is no transient retry that fixes it).
            error!(
                session = %session.as_str(),
                source,
                error = %err,
                "session id cannot form an inbox topic; not registering an inbox (peers cannot address this session)"
            );
            return;
        }
    };
    let request = Request::Subscribe {
        session: session.clone(),
        topic: topic.clone(),
        // The automatic inbox re-registration — the ONLY guarded subscribe. This is
        // the exact path that can race `SessionEnd`; the tombstone refuses it within
        // the window so a doomed post-teardown arm cannot resurrect the inbox
        // (ADR-0007). An explicit subscribe/watch instead takes the Explicit path.
        kind: SubscribeKind::AutoInbox,
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
            source,
            topic = %topic.as_str(),
            "did not register the agent inbox: session recently ended (tombstone guard); \
             the next Stop's turn-end re-registers it once the guard lapses (ADR-0013)"
        ),
        Ok(Response::Subscribed { outcome, .. }) => info!(
            session = %session.as_str(),
            source,
            topic = %topic.as_str(),
            outcome = %describe_sub(&outcome),
            "registered the session's agent inbox"
        ),
        Ok(Response::Error { message }) => warn!(
            session = %session.as_str(),
            source,
            error = %message,
            "bridge could not register the agent inbox; continuing (arming stays fail-safe)"
        ),
        Ok(other) => warn!(
            session = %session.as_str(),
            source,
            reply = ?other,
            "unexpected bridge reply while registering the agent inbox; continuing"
        ),
        Err(err) => warn!(
            session = %session.as_str(),
            source,
            error = %err,
            "bridge unreachable while registering the agent inbox; continuing (arming stays fail-safe)"
        ),
    }
}

/// The `SessionEnd` hook: remove the session's wake sentinel and drop its
/// subscriptions + interests on the bridge (which stops any adapter whose last
/// interest this session held). Best-effort: a down bridge must not fail the hook.
///
/// It no longer reaps anything. There is no per-session process to reap — the
/// daemon writes the sentinel itself (ADR-0017) — so teardown is two file/socket
/// operations rather than a signal to a detached child that may or may not still
/// exist.
///
/// A transient bridge failure is RETRIED a few times (brief backoff) so a
/// momentary blip does not leak the session's interest. If every attempt fails,
/// the card-08 TTL sweeper is the ultimate backstop: an un-torn-down interest
/// ages out via its `last_seen` (exactly the hard-died-session case it exists for)
/// — so we log and exit 0 rather than build a durable pending-end queue for MVP.
async fn run_harness_cleanup() -> anyhow::Result<ExitCode> {
    let config = StorageConfig::from_env().context("resolving storage path for harness cleanup")?;
    let session = HookInput::from_reader(std::io::stdin().lock())
        .context("reading the SessionEnd hook payload from stdin")?
        .session_id;

    end_session_with_retry(&config, &session).await;
    Ok(ExitCode::SUCCESS)
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
fn run_harness_install(format: OutputFormat, args: InstallHooksArgs) -> anyhow::Result<ExitCode> {
    use mailbox_harness::install::{BackupPolicy, SettingsTarget};

    let mailbox_bin = match args.mailbox_bin {
        Some(path) => mailbox_harness::install::abs_bin(&path),
        None => mailbox_harness::install::default_mailbox_bin(std::env::current_exe()),
    };
    let spec = mailbox_harness::install::HookInstallSpec { mailbox_bin };
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
    Ok(ExitCode::SUCCESS)
}

/// `install-inbound`: set `crossSessionInbound: "accept"` so a `bypassPermissions`
/// session can RECEIVE mailbox wakes on its inbox socket (ADR-0020).
///
/// # Why this is a separate, opt-in command
///
/// With no value set, Claude Code decides per message from both sessions' permission
/// classes. A session running `--dangerously-skip-permissions` HOLDS an arriving
/// message for human approval and drops it after ~5 minutes — so the bridge's wakes
/// never land, silently. `accept` fixes that, and in doing so re-opens unattended
/// delivery in exactly the configuration the guard exists for: an agent that acts
/// without asking, now taking direction from any process running as the same user.
///
/// That trade is the operator's to make. `install-hooks` therefore never does this,
/// and this command exists so that saying yes is explicit, reversible, and reported.
fn run_harness_install_inbound(
    format: OutputFormat,
    args: InstallInboundArgs,
) -> anyhow::Result<ExitCode> {
    use mailbox_harness::install::{BackupPolicy, InboundState, SettingsTarget};
    use std::cell::RefCell;

    let target = mailbox_harness::install::settings_target(args.settings);

    let (path, backup) = match &target {
        SettingsTarget::Explicit(path) => (path, BackupPolicy::Skip),
        SettingsTarget::DefaultFound(path) => (path, BackupPolicy::Keep),
        SettingsTarget::NoDefault { looked_at } => {
            // Nothing to edit, and conjuring a settings.json on a machine with no
            // Claude Code is not ours to do (same stance as `install-hooks`).
            let where_we_looked = match looked_at {
                Some(path) => format!("no Claude Code settings found at {}", path.display()),
                None => "no home to resolve Claude Code settings under (neither \
                         AGENT_MAILBOX_HOME nor HOME is set, or it is not absolute)"
                    .to_string(),
            };
            note(
                format,
                &format!(
                    "{where_we_looked}; nothing was changed — pass --settings <path> to choose one"
                ),
            );
            return Ok(ExitCode::SUCCESS);
        }
    };

    // What the successful merge actually read, recorded by the merge itself rather
    // than by a second read: the closure re-runs on every compare-and-swap retry, so
    // this ends up holding what the winning attempt saw, with no race to misreport.
    let seen = RefCell::new(InboundState::Unset);
    let report = mailbox_harness::install::merge_settings_file(path, backup, |existing| {
        *seen.borrow_mut() = mailbox_harness::install::inbound_state(&existing);
        mailbox_harness::install::set_inbound_accept(existing)
    })
    .with_context(|| {
        format!(
            "setting {} in {} (your settings were NOT modified)",
            mailbox_harness::install::INBOUND_SETTING_KEY,
            path.display()
        )
    })?;

    let before = seen.into_inner();
    let what_changed = match &before {
        InboundState::Accept => format!(
            "{} was already \"accept\" in {}; nothing changed",
            mailbox_harness::install::INBOUND_SETTING_KEY,
            report.written.display()
        ),
        InboundState::Unset => format!(
            "set {} = \"accept\" in {}",
            mailbox_harness::install::INBOUND_SETTING_KEY,
            report.written.display()
        ),
        InboundState::Other(previous) => format!(
            "changed {} from \"{previous}\" to \"accept\" in {}",
            mailbox_harness::install::INBOUND_SETTING_KEY,
            report.written.display()
        ),
    };
    note(format, &what_changed);
    if let Some(backup) = &report.backup {
        note(
            format,
            &format!("previous settings saved to {}", backup.display()),
        );
    }

    // Say plainly what was just widened. A setup command that quietly loosens a
    // security default and prints only "done" is how an operator ends up not knowing.
    if before != InboundState::Accept {
        eprintln!(
            "note: this session class now accepts messages from any process running as you, \
             without a prompt. That is what lets the bridge wake a --dangerously-skip-permissions \
             session; it also means anything else running as you can direct that agent. Undo by \
             removing the key, or setting it to \"hold\" or \"refuse\"."
        );
    }
    Ok(ExitCode::SUCCESS)
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
fn run_harness_install_skills(
    format: OutputFormat,
    args: InstallSkillsArgs,
) -> anyhow::Result<ExitCode> {
    let skills_dir = match args.skills_dir {
        Some(dir) => dir,
        None => mailbox_harness::skills::default_skills_dir()
            .context("resolving the default skills directory")?,
    };

    match mailbox_harness::skills::install_skills(&skills_dir) {
        Ok(report) => {
            render_skill_report(format, &report)?;
            eprintln!("next: run `mailbox harness install-hooks` to wire the wake hooks");
            Ok(ExitCode::SUCCESS)
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

/// `doctor`: report which sessions can be woken, and which cannot.
///
/// Socket-free and read-only, so it still answers when the daemon is the broken
/// thing. It no longer *probes*: there is nothing to bump and nothing to wait for.
/// Reachability is now two readable facts — is the process alive, and did Claude Code
/// bind it an inbox socket — so this is a read, not an experiment (ADR-0021).
///
/// That also removes the caveat the probe carried: a session could not measure itself,
/// because running the command made it busy. A read has no such blind spot, so
/// `doctor` now reports honestly on the session that invoked it.
///
/// **Exits 1 if any session is a fault**, so a supervisor can gate on it.
pub fn run_doctor(format: OutputFormat, args: &DoctorArgs) -> ExitCode {
    let registry = match mailbox::claude_registry::ClaudeRegistry::open() {
        Ok(registry) => registry,
        Err(err) => {
            eprintln!("mailbox doctor: {err}");
            return ExitCode::FAILURE;
        }
    };

    let sessions = match &args.session {
        Some(session) => vec![session.clone()],
        None => mailbox::doctor::registered_sessions(&registry),
    };
    if sessions.is_empty() {
        note(
            format,
            "no Claude Code sessions are registered on this machine",
        );
        return ExitCode::SUCCESS;
    }

    let live = mailbox::doctor::live_from(&registry);
    let report = mailbox::doctor::report(&sessions, &registry, &live);

    if format.is_json() {
        println!("{}", doctor_json(&report));
    } else {
        render_doctor(&report, args.all);
    }

    if report.has_fault() {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}

/// Human output: the faults, and a one-line count. `--all` lists everything.
fn render_doctor(report: &mailbox::doctor::FleetReport, show_all: bool) {
    for entry in &report.sessions {
        if !show_all && !entry.reachability.is_fault() {
            continue;
        }
        let name = entry.name.as_deref().unwrap_or("-");
        println!(
            "{:<10} {}  {}",
            entry.reachability.label(),
            entry.session.as_str(),
            name
        );
        if let Some(remedy) = entry.reachability.remedy() {
            println!("           {remedy}");
        }
    }
    println!(
        "{} session(s): {} reachable, {} cannot be woken, {} gone",
        report.sessions.len(),
        report.reachable(),
        report.no_inbox(),
        report.gone()
    );
}

/// `--json`: the whole report, stable field names for a supervisor to gate on.
fn doctor_json(report: &mailbox::doctor::FleetReport) -> String {
    let sessions: Vec<serde_json::Value> = report
        .sessions
        .iter()
        .map(|entry| {
            serde_json::json!({
                "session": entry.session.as_str(),
                "reachability": entry.reachability.label(),
                "name": entry.name,
                "fault": entry.reachability.is_fault(),
            })
        })
        .collect();
    serde_json::json!({
        "result": "doctor",
        "sessions": sessions,
        "reachable": report.reachable(),
        "no_inbox": report.no_inbox(),
        "gone": report.gone(),
    })
    .to_string()
}

/// Convenience for `main`: turn the `--json` flag into an [`OutputFormat`].
pub fn output_format(json: bool) -> OutputFormat {
    OutputFormat::from_json_flag(json)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_session_id_comes_from_the_env_value_verbatim_but_trimmed() {
        assert_eq!(
            session_from_env_value("from-claude-env"),
            Some(SessionId::new("from-claude-env"))
        );
        // Trimmed for the same reason the value is validated at all: a shell can
        // hand us a trailing newline, and that is the same session.
        assert_eq!(
            session_from_env_value("  real  "),
            Some(SessionId::new("real"))
        );
    }

    /// An empty `$CLAUDE_CODE_SESSION_ID` names NO session, so it must fail loudly
    /// rather than bind an anonymous empty one. This was the sharp edge of the old
    /// `--session` flag: `--session "$MAILBOX_SESSION_ID"` (which the skill had to warn
    /// agents away from) expanded to `--session ""` in an agent's shell and, being an
    /// explicit flag, won the precedence — binding a phantom session the agent then
    /// could not be woken on.
    #[test]
    fn an_empty_or_blank_session_env_value_names_nobody() {
        assert_eq!(session_from_env_value(""), None);
        assert_eq!(session_from_env_value("   "), None);
        assert_eq!(session_from_env_value("\n"), None);
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

        // A bare poke is allowed: from a peer, the `from` stamp is enough on its own.
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
