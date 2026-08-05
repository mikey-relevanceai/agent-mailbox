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

use mailbox::sentinel::Sentinel;
use mailbox::storage::{SessionId, StorageConfig, SubscribeKind};
use mailbox::wake::{
    RetriggerOutcome, Waiter, WakeError, WakeOutcome, WatchOutcome,
};
use mailbox_harness::hook::HookInput;
use mailbox_harness::install::DEFAULT_HOOK_TIMEOUT_SECS;
use mailbox_protocol::{AdapterId, GithubPr, Topic, inbox_topic, stub_topic};

use crate::client;
use crate::control::{
    AgentSummary, GithubPrTarget, Request, Response, StatusReport, SubscribeState, TopicStatus,
    UnwatchResultWire, WatchKindWire, WatchStateWire,
};
use crate::serve;


/// Exit code for a REFUSED publish ("you have unread mail on this topic; read first").
///
/// Its own code, because it is not a failure: nothing was written, nothing is broken,
/// and the remedy is defined (`mailbox read`, then retry). It used to exit 1 — the
/// same code as "the bridge is down" — so a scripted publisher could not tell "retry
/// after reading" from a real error without string-matching stderr.
///
/// **Not 2**: 2 is the wake code (`wait`/`arm`), and Claude Code treats an exit 2 from
/// an asyncRewake hook as "wake this session". Reusing it here would be a category
/// error. Clap's own usage errors also exit 2, which is another reason to keep away.
const PUBLISH_REFUSED_EXIT: u8 = 3;

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
    /// Print this session's own id and inbox topic. Needs no bridge.
    Whoami(SessionOpt),
    /// Message a peer agent: publish to its inbox, stamped with your session id.
    Send(SendArgs),
    /// List the agents with a registered inbox (who you can `send` to).
    Agents(SessionOpt),
    /// List known topics with their subscriber and event counts.
    Topics(TopicsArgs),
    /// Actively prove which sessions can be woken right now, by bumping each
    /// sentinel and requiring the wake hook to answer.
    Doctor(DoctorArgs),
    /// Claude Code hook handlers and setup (arm / cleanup / install-hooks /
    /// install-skills). The harness owns the wake loop so the agent never re-arms.
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

    /// Resolve the session where having none is LEGAL — the `publish` path, whose
    /// caller may be an adapter or a plain script with no session anywhere. `None`
    /// is the adapter contract (kick every subscriber; no caller-aware rules), so it
    /// must not be an error the way it is for a session-scoped command.
    pub fn resolve_optional(&self) -> Option<SessionId> {
        self.resolve().ok()
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

/// The user-facing diagnostic for an ignored empty `--session`. A real stderr line
/// (see [`resolve_session`]), not a filtered log event.
const EMPTY_SESSION_WARNING: &str = "mailbox: warning: ignoring an empty --session (it names no session); \
     falling back to MAILBOX_SESSION_ID / CLAUDE_CODE_SESSION_ID — omit the flag instead";

/// The session-resolution rule, as a pure function of its three inputs so the
/// precedence is unit-testable without touching process env.
fn resolve_session(
    flag: Option<SessionId>,
    mailbox_env: Option<String>,
    claude_env: Option<String>,
) -> anyhow::Result<SessionId> {
    // An empty/whitespace `--session` is treated as absent, not as a real (empty)
    // session id, so it falls through to the env fallbacks. This is the common
    // trap: `--session "$MAILBOX_SESSION_ID"` with that var unset expands to
    // `--session ""`, and an explicit flag wins the precedence — so without this
    // it would bind a phantom empty session instead of resolving via
    // `CLAUDE_CODE_SESSION_ID`. (The env sources are already emptiness-filtered by
    // `env_session`.)
    let flag = flag.and_then(|s| {
        let trimmed = s.as_str().trim();
        if trimmed.is_empty() {
            // Never silent: the caller believes they named a session and did not, so
            // whichever session we DO bind is not the one they typed. (This is the
            // `--session "$MAILBOX_SESSION_ID"` trap.)
            //
            // A DIRECT stderr line, not a `tracing` event: at the default filter
            // (ERROR) a `warn!` was swallowed for exactly the commands where the trap
            // bites — `mailbox publish --session ""` printed nothing at all — so the
            // one diagnostic that names the trap never reached the agent that walked
            // into it. It is also duplicated into `tracing` so it lands in harness.log
            // for `wait`/`arm`, whose stderr goes elsewhere.
            eprintln!("{EMPTY_SESSION_WARNING}");
            warn!("{EMPTY_SESSION_WARNING}");
            return None;
        }
        Some(SessionId::new(trimmed))
    });
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

/// The `harness` command group: Claude Code hook targets.
#[derive(Args, Debug)]
pub struct HarnessArgs {
    #[command(subcommand)]
    pub command: HarnessCommand,
}

#[derive(Subcommand, Debug)]
pub enum HarnessCommand {
    /// SessionStart hook (ADR-0008): register the inbox, print the `watchPaths`
    /// registering this session's sentinel, and spawn the detached mail watcher.
    SessionStart,
    /// FileChanged hook (ADR-0008): wake (exit 2) IFF this session has genuine
    /// unread mail, else exit 0 — the anti-loop guard against a stray sentinel touch.
    Wake,
    /// The detached per-session mail watcher (ADR-0008). Spawned by `session-start`;
    /// blocks on the FIFO and bumps the wake sentinel on real mail. Not run by hand.
    Watch(WatchSentinelArgs),
    /// Stop hook (ADR-0008 Stop-liveness): re-register this session's inbox (best-effort,
    /// ADR-0013) and respawn the detached watcher IFF it is missing/dead. NEVER wakes
    /// (exit 0 always).
    EnsureWatcher,
    /// UserPromptSubmit hook (ADR-0016): record that a turn has opened, so a health
    /// probe can tell a busy session apart from an unreachable one. Never wakes.
    TurnStart,
    /// SessionEnd hook: reap the watcher, remove the sentinel, and drop this
    /// session's interests/subscriptions.
    Cleanup,
    /// Merge the hooks into the Claude Code settings.json (and print the snippet).
    InstallHooks(InstallHooksArgs),
    /// Install the embedded agent-mailbox skill into the Claude Code skills dir.
    InstallSkills(InstallSkillsArgs),
}

/// Arguments to the detached watcher (`harness watch`). It is spawned by
/// `session-start` with an explicit `--session`, so unlike the hooks it does not
/// read the session from a hook stdin payload.
#[derive(Args, Debug)]
pub struct WatchSentinelArgs {
    #[command(flatten)]
    pub session: SessionOpt,
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
    /// Claude Code hook timeout to write, in seconds.
    #[arg(long, default_value_t = DEFAULT_HOOK_TIMEOUT_SECS)]
    pub timeout_secs: u64,
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
/// The session is resolved like every other session-scoped command, but is
/// **optional**: an adapter (or any script outside a Claude Code session) has no
/// session id anywhere, and publishing must keep working exactly as it always has
/// for it. A resolved session turns on the two caller-aware rules — be caught up to
/// speak, and never wake yourself — in [`crate::serve`].
///
/// `--no-session` publishes ANONYMOUSLY on purpose. It matters because Claude Code
/// exports `$CLAUDE_CODE_SESSION_ID` into every process an agent spawns — a build
/// script, a git hook, a subagent — so such a process's `publish` is otherwise
/// attributed to the AGENT, and an agent is never woken by its own message. Any
/// process the agent spawned that publishes on the agent's behalf should pass
/// `--no-session`, so the event has no author and wakes every subscriber, the agent
/// included.
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
    /// Publish with NO authoring session, ignoring `--session` and the ambient
    /// `$CLAUDE_CODE_SESSION_ID` / `$MAILBOX_SESSION_ID`. The event then belongs to
    /// nobody, so the "be caught up to speak" rule does not apply to it. Use it from
    /// any script/hook/subagent an agent spawns, so that process's work is not
    /// attributed to whichever session id it happened to inherit.
    #[arg(long, conflicts_with = "session")]
    pub no_session: bool,
    #[command(flatten)]
    pub session: SessionOpt,
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
pub struct DoctorArgs {
    /// Probe only this session, instead of every session with a sentinel.
    #[arg(long, value_parser = parse_session)]
    pub session: Option<SessionId>,
    /// How long a session has to answer before it is reported deaf.
    #[arg(long, default_value_t = 10_000)]
    pub timeout_ms: u64,
    /// List every session probed, not just the faults.
    #[arg(long)]
    pub all: bool,
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
        Command::Status(args) => run_status(format, args).await,
        Command::Whoami(args) => run_whoami(format, args),
        Command::Send(args) => run_send(format, args).await,
        Command::Agents(args) => run_agents(format, args).await,
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

/// `publish`: append an event to a topic.
///
/// The caller's session is resolved when there IS one (an agent's shell always has
/// `$CLAUDE_CODE_SESSION_ID`), and carried on the wire. That is what lets the bridge
/// apply the two caller-aware rules — refuse a publish from a caller with unread on
/// that topic, and never kick the publisher for its own event (ADR-0006 / §Publish
/// in docs/04-usage.md). With no session (an adapter, a cron script) the publish
/// behaves exactly as it always did: no unread rule, kick every subscriber.
async fn run_publish(format: OutputFormat, args: PublishArgs) -> anyhow::Result<ExitCode> {
    let topic = parse_topic(&args.topic)?;
    let body: serde_json::Value =
        serde_json::from_str(&args.body).context("--body must be valid JSON")?;
    // `--no-session` deliberately drops the ambient identity: the caller is a script
    // an agent spawned, not the agent, and it must NOT publish as it (see PublishArgs).
    let session = if args.no_session {
        None
    } else {
        args.session.resolve_optional()
    };
    request(
        format,
        Request::Publish {
            topic,
            adapter: AdapterId(args.adapter),
            body,
            session,
        },
    )
    .await
}

async fn run_subscribe(format: OutputFormat, args: TopicArgs) -> anyhow::Result<ExitCode> {
    let topic = parse_topic(&args.topic)?;
    request(
        format,
        Request::Subscribe {
            session: resolve_session_or_fail(format, &args.session)?,
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
            session: resolve_session_or_fail(format, &args.session)?,
            topic,
        },
    )
    .await
}

async fn run_read(format: OutputFormat, args: ReadArgs) -> anyhow::Result<ExitCode> {
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
fn run_whoami(format: OutputFormat, args: SessionOpt) -> anyhow::Result<ExitCode> {
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
    Ok(ExitCode::SUCCESS)
}

/// `send`: message a peer agent. The body the bridge publishes is
/// `{"from": "<sender>", ...}` — see [`mailbox::agents`] for the convention and
/// for why an unregistered target is a hard error rather than a silent publish.
async fn run_send(format: OutputFormat, args: SendArgs) -> anyhow::Result<ExitCode> {
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

async fn run_agents(format: OutputFormat, args: SessionOpt) -> anyhow::Result<ExitCode> {
    request(
        format,
        Request::Agents {
            session: resolve_session_or_fail(format, &args)?,
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

async fn run_unwatch(format: OutputFormat, args: UnwatchArgs) -> anyhow::Result<ExitCode> {
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

async fn run_status(format: OutputFormat, args: SessionOpt) -> anyhow::Result<ExitCode> {
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
    // A refused publish is the ONE serviced, non-error outcome with its own exit code:
    // "you are not caught up; read and retry" is not a failure, and a scripted
    // publisher must be able to tell it from one (FIX 6). Everything else is success.
    Ok(match &response {
        Response::PublishRefused { .. } => ExitCode::from(PUBLISH_REFUSED_EXIT),
        _ => ExitCode::SUCCESS,
    })
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
        // Be caught up to speak. Named counts, the topic, and the ONE command that
        // fixes it: an agent must be able to act on this without guessing. Nothing was
        // written, so it can simply read and retry. Printed on stdout (it is this
        // command's result, not an error), and the exit code says so too.
        Response::PublishRefused { topic, unread } => println!(
            "refused: you have {unread} unread event(s) on {} — run `mailbox read` first, then \
             publish again (nothing was published)",
            topic.as_str()
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

/// Dispatch a `harness` subcommand. `session-start`, `arm`, `cleanup`, and
/// `ensure-watcher` are socket clients (the last re-registers the inbox — ADR-0013);
/// the two `install-*` setup commands touch no bridge.
async fn run_harness(format: OutputFormat, args: HarnessArgs) -> anyhow::Result<ExitCode> {
    match args.command {
        HarnessCommand::SessionStart => run_harness_session_start().await,
        HarnessCommand::Cleanup => run_harness_cleanup().await,
        HarnessCommand::EnsureWatcher => Ok(run_ensure_watcher_hook().await),
        HarnessCommand::TurnStart => Ok(run_turn_start_hook()),
        HarnessCommand::InstallHooks(args) => run_harness_install(format, args),
        HarnessCommand::InstallSkills(args) => run_harness_install_skills(format, args),
        // `wake` and `watch` are dispatched synchronously by `main` (they need no tokio
        // runtime — `wake` is a read-only peek, `watch` is a blocking loop) and never
        // reach here. `ensure-watcher` is async now (it re-registers the inbox over the
        // socket — ADR-0013), so it IS dispatched here.
        HarnessCommand::Wake => unreachable!("harness wake is handled synchronously in main"),
        HarnessCommand::Watch(_) => {
            unreachable!("harness watch is handled synchronously in main")
        }
    }
}

/// The `SessionStart` hook (ADR-0008): the short-lived, non-asyncRewake setup that
/// arms on-demand wake for this session.
///
/// It is wired with matcher `""`, so it fires on EVERY SessionStart source — `startup`
/// AND `resume`/`clear`/`compact` (ADR-0013). A resume is a fresh process that has lost
/// its predecessor's inbox registration, watchPaths, and watcher; running this on resume
/// re-establishes all three. Every step below is idempotent, so re-running it on a live
/// session (e.g. a `compact` mid-session) is a safe no-op. It:
///
/// 1. reads `session_id` from the hook's stdin JSON;
/// 2. ensures the always-on agent inbox subscription (card 16 / ADR-0007);
/// 3. prints the `watchPaths` JSON registering this session's ABSOLUTE sentinel
///    path, so Claude Code watches it even though it lives outside the cwd;
/// 4. spawns the detached watcher (`harness watch`), fully daemonized so it outlives
///    this hook;
/// 5. exits 0.
///
/// **Fail-open.** If the bridge is down the inbox registration is skipped, but the
/// watchPaths are still printed and the watcher is still spawned: the watcher
/// self-validates `has_subscription` under its lock, so an unsubscribed session's
/// watcher simply self-exits, whereas NOT arming would leave an idle session
/// permanently unwakeable. There is no exit-2 anywhere on this path — waking is the
/// `FileChanged` hook's job, not this one's.
async fn run_harness_session_start() -> anyhow::Result<ExitCode> {
    let config =
        StorageConfig::from_env().context("resolving storage path for harness session-start")?;
    let session = HookInput::from_reader(std::io::stdin().lock())
        .context("reading the SessionStart hook payload from stdin")?
        .session_id;

    // Always-on inbox first (best-effort; a down bridge does not fail the hook).
    register_inbox(&config, &session, "session-start").await;

    // Print the watchPaths registration (stdout is this hook's contract) and spawn
    // the detached watcher. Both are best-effort-but-loud: a failure to resolve the
    // sentinel root is logged, but we still exit 0 (the hook must never fail).
    match Sentinel::for_session(&session) {
        Ok(sentinel) => {
            print_watch_paths(&sentinel);
            spawn_detached_watcher(&session);
        }
        Err(err) => error!(
            session = %session.as_str(),
            error = %err,
            "could not resolve the wake sentinel path; on-demand wake is NOT armed for this session \
             (set MAILBOX_SENTINEL_ROOT or a home). The session still receives mail durably; it \
             just will not wake on it"
        ),
    }
    Ok(ExitCode::SUCCESS)
}

/// Print the `SessionStart` `watchPaths` registration to stdout: it tells Claude
/// Code to watch this session's ABSOLUTE sentinel file (which lives outside the
/// cwd), so a bump to it fires the `FileChanged` hook even on a truly-idle session.
/// Per-session isolation comes from this absolute path — the static matcher is the
/// shared basename.
fn print_watch_paths(sentinel: &Sentinel) {
    // ONE path: this session's own sentinel.
    //
    // The shared `by-agent` root was registered here too for a while, so that a
    // session which forks (new id, new directory) would still be watching something
    // its mail lands under. It was withdrawn: the matcher is the shared sentinel
    // BASENAME, so registering the root made every session's bump fire every other
    // session's hook — a measured 16:1 stray-to-genuine wake ratio, and one ~40ms
    // process per live session per bump. It also did not buy what it was for, which
    // was a session whose FileChanged servicing had died.
    //
    // The fork case is therefore unhandled by design rather than by accident: a
    // forked session registers its own inbox on SessionStart like any other, and
    // mail addressed to the pre-fork id is lost the same way mail to any ended
    // session is. `send` fails loudly for an unregistered agent, so the peer learns
    // it rather than being silently dropped. Linking a fork to its parent needs a
    // parent id the hook payload does not give us.
    let registration = serde_json::json!({
        "hookSpecificOutput": {
            "hookEventName": "SessionStart",
            "watchPaths": [sentinel.path().display().to_string()],
        }
    });
    // stdout is the hook contract; a serialization failure is not possible for this
    // fixed shape, but if it somehow were, printing nothing is better than a panic.
    if let Ok(line) = serde_json::to_string(&registration) {
        println!("{line}");
    }
}

/// Spawn the detached mail watcher (`mailbox harness watch --session <id>`) so it
/// OUTLIVES this hook process (ADR-0008).
///
/// Daemonization is two-part: here we detach the child's stdio (so it holds no
/// pipe back to the hook), and the watcher process itself calls `setsid` on
/// startup to leave the hook's process group — so a `killpg` on the hook's group,
/// or the hook's own exit, cannot take the watcher down. It is deliberately NOT
/// waited on: this hook exits immediately, the child is reparented to init, and no
/// hook `timeout` applies to it (it is not an asyncRewake hook child).
///
/// Best-effort: a spawn failure is logged, never fatal to the hook. The watcher is
/// single-instance (it takes the per-session lock), so a redundant spawn — e.g. a
/// SessionStart racing a still-live watcher — has one winner and the loser exits.
fn spawn_detached_watcher(session: &SessionId) {
    let exe = match std::env::current_exe() {
        Ok(exe) => exe,
        Err(err) => {
            error!(session = %session.as_str(), error = %err, "could not resolve the mailbox binary to spawn the watcher");
            return;
        }
    };
    let spawn = std::process::Command::new(exe)
        .args(["harness", "watch", "--session", session.as_str()])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn();
    match spawn {
        Ok(child) => {
            info!(session = %session.as_str(), pid = child.id(), "spawned the detached mail watcher")
        }
        Err(err) => {
            error!(session = %session.as_str(), error = %err, "could not spawn the detached mail watcher; the session will not wake on mail")
        }
    }
}


/// Ensure `session` is subscribed to its own inbox topic, over the socket
/// (always-on agent inboxes, ADR-0007).
///
/// `source` names the hook that called us (`"session-start"`, `"ensure-watcher"`,
/// `"arm"`) and rides every log line: since ADR-0013 both `session-start` (on a
/// resume) and `ensure-watcher` (every `Stop`) re-register the inbox, and an
/// operator diagnosing "why didn't my resumed agent wake?" needs to tell a
/// resume's SessionStart re-registration apart from a Stop healing a lapsed one.
///
/// Idempotent by construction: `ensure-watcher` runs on every `Stop`, and
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
             the next Stop's ensure-watcher re-registers it once the guard lapses (ADR-0013)"
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
async fn run_harness_cleanup() -> anyhow::Result<ExitCode> {
    let config = StorageConfig::from_env().context("resolving storage path for harness cleanup")?;
    let session = HookInput::from_reader(std::io::stdin().lock())
        .context("reading the SessionEnd hook payload from stdin")?
        .session_id;

    // Reap the watcher (its pidfile is the same `<session>.waiter.pid` the waiter
    // uses, so this SIGTERMs whichever detached process is live for this session).
    let reap = mailbox_harness::cleanup::reap_waiter(&config.waiters_dir(), &session);
    info!(session = %session.as_str(), outcome = reap.as_str(), "reaped session watcher");

    // Remove the session's wake sentinel dir (ADR-0008), so no `by-agent/<id>`
    // directory outlives the session. Best-effort: a never-created sentinel or an
    // unresolvable root must not fail the hook.
    match Sentinel::for_session(&session) {
        Ok(sentinel) => match sentinel.remove_dir() {
            Ok(()) => {
                info!(session = %session.as_str(), dir = %sentinel.dir().display(), "removed the session's wake sentinel")
            }
            Err(err) => {
                warn!(session = %session.as_str(), error = %err, "could not remove the session's wake sentinel dir")
            }
        },
        Err(err) => {
            warn!(session = %session.as_str(), error = %err, "could not resolve the wake sentinel to remove it")
        }
    }

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
    let spec = mailbox_harness::install::HookInstallSpec {
        mailbox_bin,
        timeout_secs: args.timeout_secs,
    };
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

/// Run `wait` synchronously (no tokio runtime): open the read-only store and
/// block on the FIFO. Finalizes the card-05 PROVISIONAL command into its real
/// shape — `mailbox wait --session <id>` — with the same exit-code contract.
///
/// Exit codes: `2` = wake the session (mail, OR the benign re-arm boundary — both
/// carry their reason on stderr); `1` = a waiter error, including an unresolvable
/// session (a waiter with no identity has nothing to wait on); `0` = there is nothing
/// to wake about (the session has no subscriptions, or no store exists at all).
///
/// With `--max-block-ms`, a block that elapses with no mail exits **2** with
/// [`REARM_NOTICE`]. That is the whole re-arm design (ADR-0006): the waiter cannot
/// outlive its hook process (Claude Code kills it at the `timeout`, and `execv` does
/// not reset that clock), and a truly idle session fires no further `Stop` — so a
/// killed waiter would never be re-armed. Waking *before* the deadline is what
/// guarantees a `Stop`, and therefore a fresh `arm` with a fresh timeout. Without
/// `--max-block-ms`, `wait` blocks indefinitely (the card-05 contract).
/// `mailbox dashboard`: the live fleet health view (ADR-0015).
///
/// Synchronous and socket-free, like `wait`: it opens the store READ-ONLY, so it
/// still renders when the `serve` daemon is down — the state it reports as
/// `daemon DOWN` rather than refusing to draw.
///
/// `--once` prints a plain-text snapshot instead of taking over the terminal. It is
/// also the automatic fallback when the terminal cannot be driven (piped output, no
/// TTY, CI): a health view that fails because it is being piped to a file would be
/// useless in exactly the situation where someone is capturing evidence.
/// `mailbox doctor` — actively prove which sessions can be woken right now
/// (ADR-0016).
///
/// Socket-free and synchronous: a health check has to work when the daemon is down,
/// and this one needs nothing from it — it bumps sentinel files and reads the hook's
/// acks.
///
/// **Exit 1 when any session is deaf.** This is a check, not a report: a fleet with
/// an unreachable agent is a fleet that will silently drop work, and a caller
/// scripting it (a cron, a supervisor agent) must be able to notice without parsing
/// prose. Sessions with no live process are NOT faults and do not affect the code.
pub fn run_doctor(format: OutputFormat, args: &DoctorArgs) -> ExitCode {
    let root = match mailbox::sentinel::Sentinel::for_session(&SessionId::new("probe")) {
        // `for_session` is the one place the root rule lives; we only want the root,
        // so resolve a throwaway session and walk up from its directory.
        Ok(sentinel) => match sentinel.dir().parent().and_then(|p| p.parent()) {
            Some(root) => root.to_path_buf(),
            None => {
                eprintln!("mailbox doctor: could not resolve the sentinel root");
                return ExitCode::FAILURE;
            }
        },
        Err(err) => {
            eprintln!("mailbox doctor: {err}");
            return ExitCode::FAILURE;
        }
    };

    let sessions = match &args.session {
        Some(session) => vec![session.clone()],
        None => mailbox::doctor::sessions_with_sentinels(&root),
    };
    if sessions.is_empty() {
        println!(
            "no sessions to probe (no sentinel directories under {})",
            root.display()
        );
        return ExitCode::SUCCESS;
    }

    // Without a process table we cannot tell "deaf" from "not running". Say so and
    // keep going rather than reporting confident nonsense: the probe still proves
    // who IS reachable, which is the half that never lies.
    let live = mailbox::doctor::live_claude_sessions();
    if live.is_none() {
        eprintln!(
            "warning: could not read the process table, so sessions that have exited \
             cannot be told apart from sessions that are deaf"
        );
    }
    let live = live.unwrap_or_default();

    let probe = mailbox::doctor::Probe {
        budget: std::time::Duration::from_millis(args.timeout_ms),
    };
    let report = probe.run(&sessions, &live);

    // A session cannot measure itself. Running this command IS a turn, so the caller
    // is busy by construction for the whole probe and can only ever report itself as
    // UNMEASURED — which reads as "no fault found" to anyone skimming. An agent
    // auditing its own fleet is therefore structurally blind to its own deafness, and
    // that blind spot has to be stated rather than left for the reader to deduce.
    if let Some(caller) =
        env_session(ENV_MAILBOX_SESSION).or_else(|| env_session(ENV_CLAUDE_SESSION))
        && report.sessions.iter().any(|r| r.session.as_str() == caller)
    {
        eprintln!(
            "warning: {caller} is the session running this command, so it is busy for the \
             whole probe and cannot be measured here. Probe it from another session (or a \
             cron) to learn whether it can be woken."
        );
    }

    match format {
        OutputFormat::Json => println!("{}", doctor_json(&report)),
        OutputFormat::Human => render_doctor(&report, args.all),
    }
    if report.deaf() > 0 {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}

/// Render the probe as prose, faults first.
fn render_doctor(report: &mailbox::doctor::FleetReport, show_all: bool) {
    use mailbox::doctor::Reachability;

    let deaf = report.deaf();
    println!(
        "probed {} session(s) in one window, {}ms budget: {} wakeable, {} deaf, {} UNMEASURED",
        report.sessions.len(),
        report.budget.as_millis(),
        report.wakeable(),
        deaf,
        report.unmeasured()
    );
    for row in &report.sessions {
        let show = show_all || row.reachability.is_fault();
        if !show {
            continue;
        }
        let detail = match &row.reachability {
            Reachability::Wakeable { took } => {
                format!("wakeable (answered in {}ms)", took.as_millis())
            }
            Reachability::Deaf => {
                "DEAF — its sentinel changed and Claude Code never ran the wake hook; \
                 mail will not reach this agent"
                    .to_string()
            }
            Reachability::Busy => {
                "UNMEASURED — mid-turn, so it could not have answered. This is NOT a \
                 clean bill of health: a deaf session that happens to be busy looks \
                 exactly like this. Re-probe it while idle."
                    .to_string()
            }
            Reachability::Gone => "gone (no live Claude Code process; not a fault)".to_string(),
            Reachability::NeverArmed => {
                "never armed (no sentinel has been written yet)".to_string()
            }
            Reachability::Undetermined { reason } => format!("undetermined ({reason})"),
        };
        println!("  {}  {}", row.session.as_str(), detail);
    }
    if report.looks_like_a_stale_install() {
        println!(
            "\nNOTHING answered. Before believing that, check that the `mailbox` binary your \
             FileChanged hook runs is current — the ack this probe reads is written by that \
             binary, so an old one looks exactly like a fleet-wide blackout. Re-run after \
             installing; no session needs restarting, since the hook invokes the binary afresh \
             every time."
        );
    } else if deaf > 0 {
        println!(
            "\n{deaf} agent(s) cannot be woken. Mail still lands in their inboxes durably, but \
             they will not act on it until they take a turn for another reason. Deafness is \
             acquired, so re-run this after any recovery to confirm."
        );
    }
}

/// The machine-readable probe result, for a supervisor agent or a cron.
fn doctor_json(report: &mailbox::doctor::FleetReport) -> String {
    use mailbox::doctor::Reachability;

    let rows: Vec<serde_json::Value> = report
        .sessions
        .iter()
        .map(|row| {
            let mut value = serde_json::json!({
                "session": row.session.as_str(),
                "state": row.reachability.label(),
                "fault": row.reachability.is_fault(),
            });
            match &row.reachability {
                Reachability::Wakeable { took } => {
                    value["answered_ms"] = serde_json::json!(took.as_millis() as u64);
                }
                Reachability::Undetermined { reason } => {
                    value["reason"] = serde_json::json!(reason);
                }
                _ => {}
            }
            value
        })
        .collect();
    serde_json::json!({
        "budget_ms": report.budget.as_millis() as u64,
        "wakeable": report.wakeable(),
        "deaf": report.deaf(),
        "unmeasured": report.unmeasured(),
        "stale_install_suspected": report.looks_like_a_stale_install(),
        "sessions": rows,
    })
    .to_string()
}


/// The `FileChanged` wake hook (ADR-0008), run synchronously (a read-only peek, no
/// runtime): decide whether THIS session has genuine unread mail and, if so, WAKE it.
///
/// Exit codes are the whole anti-loop contract:
/// - **2** with `mail on topic <X>` on stderr — there is genuinely unread mail, so
///   wake the idle session (the `asyncRewake` wake wire, payload-free: topic names
///   only, never a body).
/// - **0** — no unread mail. A `FileChanged` fires on ANY change to the watched
///   sentinel (the watcher's own bookkeeping write, a stray editor touch, a
///   `create`/`remove` at `SessionEnd`), so exiting 2 unconditionally would loop the
///   agent forever. Exiting 0 unless the read-only store confirms unread is what
///   breaks that loop — the earlier prototype looped precisely because it did not.
/// - **1** — the hook payload could not be read (a real error).
///
/// The unread check is the read-only store, NOT the sentinel's contents: the store
/// is authoritative (it also excludes the session's own authored events), so a wake
/// can never fire for mail that is not really there.
///
/// **The store re-check is load-bearing for cross-session ISOLATION, not just
/// anti-loop (ADR-0008 §Isolation).** The sentinel is a TRIGGER, never authority. When
/// a session's cwd is an ancestor of `~/.mailbox` (e.g. `claude` launched from `$HOME`,
/// which recursively watches the cwd), a bump to ANOTHER session's `.mailbox-wake`
/// fires THIS session's `FileChanged` too — but this hook then re-checks THIS session's
/// own unread from the store and finds none, so it exits 0 (no false wake). Isolation
/// therefore comes from the per-session store re-check here, NOT from the sentinel
/// path — a future change MUST NOT start trusting the sentinel's contents in place of
/// this check, or session B's mail could wake session A.
pub fn run_wake_hook() -> ExitCode {
    let config = match StorageConfig::from_env() {
        Ok(config) => config,
        Err(err) => {
            eprintln!("mailbox harness wake: {err}");
            return ExitCode::FAILURE;
        }
    };
    let session = match HookInput::from_reader(std::io::stdin().lock()) {
        Ok(input) => input.session_id,
        Err(err) => {
            eprintln!("mailbox harness wake: could not read the FileChanged hook payload: {err}");
            return ExitCode::FAILURE;
        }
    };

    // Stamp the ack FIRST, before any decision and before the store is even
    // consulted (ADR-0016). The point of this record is not what the hook decides
    // — it is that Claude Code delivered the file-change event at all, which is the
    // one hop the bridge cannot otherwise observe. Recording it late, or only on
    // the wake path, would make a healthy-but-quiet session indistinguishable from
    // an unwatched one, which is the confusion this record exists to end.
    // Best-effort: a health record must never be able to break a wake.
    match Sentinel::for_session(&session) {
        Ok(sentinel) => {
            if let Err(err) = sentinel.record_hook_ran(std::time::SystemTime::now()) {
                warn!(
                    session = %session.as_str(),
                    error = %err,
                    "could not record that the FileChanged hook ran; wake is unaffected but \
                     `mailbox doctor` will under-report this session's health"
                );
            }
        }
        Err(err) => warn!(
            session = %session.as_str(),
            error = %err,
            "could not resolve the sentinel to record that the FileChanged hook ran; \
             wake is unaffected"
        ),
    }

    // No store at all: the bridge has never run here, so there is nothing to wake
    // about. Exit 0 (no wake) — never loop an agent over a phantom sentinel.
    if !config.path().exists() {
        info!(
            session = %session.as_str(),
            "no mailbox store; FileChanged wake is a no-op (exit 0)"
        );
        return ExitCode::SUCCESS;
    }

    let waiter = Waiter::new(
        config.waiters_dir(),
        config.path().to_path_buf(),
        session.clone(),
    );
    match waiter.peek_unread() {
        Ok(topics) if !topics.is_empty() => {
            let names: Vec<&str> = topics.iter().map(Topic::as_str).collect();
            // The payload-free wake reminder — topic names only — surfaced to the
            // agent verbatim as its system reminder (docs/01-wake-and-rearm.md).
            eprintln!("mail on topic {}", names.join(", "));
            info!(
                session = %session.as_str(),
                topics = names.join(","),
                "FileChanged wake: genuine unread mail; exiting 2 to wake the session"
            );
            ExitCode::from(WakeOutcome::EXIT_CODE)
        }
        Ok(_) => {
            // A change fired but nothing is unread — the anti-loop path. Do NOT wake.
            info!(
                session = %session.as_str(),
                "FileChanged wake: nothing unread (stray sentinel change); exiting 0 (no wake)"
            );
            ExitCode::SUCCESS
        }
        Err(err) => {
            // We could not confirm unread (store unreadable, WAL absent while the
            // bridge is down). Exit 0, not 2: an unconfirmable wake must never loop
            // the agent. The next real kick re-bumps the sentinel and re-fires this.
            warn!(
                session = %session.as_str(),
                error = %err,
                "FileChanged wake: could not check unread; exiting 0 (no wake) to stay anti-loop-safe"
            );
            ExitCode::SUCCESS
        }
    }
}

/// The detached mail watcher (ADR-0008), run synchronously (a blocking FIFO loop,
/// no runtime). Spawned by the `SessionStart` hook; not invoked by hand.
///
/// It first calls `setsid` to detach into its own session and process group, so the
/// hook's exit — or a `killpg` on the hook's group — cannot take it down. Then it
/// runs [`Waiter::watch_sentinel`], which blocks on the mail FIFO forever and bumps
/// the wake sentinel on real mail, until `SIGTERM`'d at `SessionEnd`.
///
/// Exit codes: **0** for every clean end — the arm-iff-subscribed self-exit
/// ([`WatchOutcome::Unsubscribed`]) AND the single-instance lock-loser
/// ([`WakeError::AlreadyWaiting`], the expected outcome when a `SessionStart` races a
/// still-live watcher). **1** only for a genuine watcher failure.
pub fn run_watch_sentinel(args: &WatchSentinelArgs) -> ExitCode {
    // Detach into a fresh session/process group. EPERM means we are already a group
    // leader (already detached), which is fine — either way we end up detached.
    if let Err(err) = nix::unistd::setsid() {
        info!(
            error = %err,
            "watcher setsid did not detach a new session (already a leader?); continuing"
        );
    }

    let config = match StorageConfig::from_env() {
        Ok(config) => config,
        Err(err) => {
            eprintln!("mailbox harness watch: {err}");
            return ExitCode::FAILURE;
        }
    };
    let session = match args.session.resolve() {
        Ok(session) => session,
        Err(err) => {
            eprintln!("mailbox harness watch: {err:#}");
            return ExitCode::FAILURE;
        }
    };
    let sentinel = match Sentinel::for_session(&session) {
        Ok(sentinel) => sentinel,
        Err(err) => {
            error!(session = %session.as_str(), error = %err, "watcher could not resolve its sentinel path");
            return ExitCode::FAILURE;
        }
    };

    // No store yet (hooks installed but no daemon started): nothing to watch. Exit 0,
    // like the waiter — a fresh SessionStart re-spawns us once the bridge exists.
    if !config.path().exists() {
        info!(
            session = %session.as_str(),
            db = %config.path().display(),
            "no mailbox store exists; watcher exiting without arming (start `mailbox serve`)"
        );
        return ExitCode::SUCCESS;
    }

    let waiter = Waiter::new(
        config.waiters_dir(),
        config.path().to_path_buf(),
        session.clone(),
    );
    match waiter.watch_sentinel(&sentinel) {
        Ok(WatchOutcome::Unsubscribed) => ExitCode::SUCCESS,
        // The single-instance invariant working as designed: a second watcher (a
        // SessionStart racing a still-live one) loses the lock and exits cleanly. It
        // is the EXPECTED outcome, not a fault — exit 0 so nothing reads it as an error.
        Err(WakeError::AlreadyWaiting { path }) => {
            info!(
                session = %session.as_str(),
                lock = %path.display(),
                "another watcher already holds this session's lock; exiting cleanly (single-instance)"
            );
            ExitCode::SUCCESS
        }
        Err(err) => {
            error!(session = %session.as_str(), error = %err, "watcher failed");
            eprintln!("mailbox harness watch: {err}");
            ExitCode::FAILURE
        }
    }
}

/// The `Stop` hook: the turn-boundary safety net. It does two things and NEVER wakes
/// the session itself.
///
/// 1. **Watcher liveness** (ADR-0008): respawn the detached watcher if it died.
/// 2. **The level-triggered re-trigger** (ADR-0012): if the session is sitting on
///    unread mail it has not been re-triggered for, re-bump the wake sentinel — see
///    [`retrigger_wake_if_unread`] for why an edge-only wake goes deaf on a BUSY
///    session, which is the bug that motivates it.
///
/// It is the recovery mechanism for **a dead watcher**: if the watcher died (a crash, an
/// OS/OOM kill, an unrecoverable FIFO error), a session that keeps taking turns re-spawns
/// it here. It respawns only when the pidfile is missing or names a dead pid; a LIVE
/// watcher is left untouched — and even a redundant spawn is free, because the loser
/// loses the single-instance lock and exits `AlreadyWaiting` (exit 0).
///
/// It **prints nothing to stdout**: a Stop hook cannot register `watchPaths` (that is a
/// `SessionStart`-only output — emitting it from a Stop fails Claude Code's event-name
/// check), so watchPath registration lives solely in the `SessionStart` hook. That hook
/// now fires on resume too (matcher `""`, ADR-0013), so a resumed process re-registers
/// its own watchPaths rather than relying on this one — which it never could.
///
/// It **also re-registers the session's inbox** on every turn boundary (best-effort,
/// fail-open — ADR-0013), restoring the ADR-0007 invariant that the inbox is registered
/// on every `SessionStart` AND every `Stop`. ADR-0008 moved registration into
/// `session-start` alone and dropped it here; the consequence was that a session whose
/// inbox lapsed (a resume within the tombstone guard, an unsubscribe) had no per-turn
/// path to re-register. The socket call fails safe: a down/erroring bridge is logged and
/// skipped, exactly like `session-start`'s registration.
///
/// It **never exits 2** (it is not an `asyncRewake` hook), so a Stop can never itself
/// wake the session — that is the load-bearing invariant. It exits 1 on a config/stdin
/// error (it could do nothing useful) and 0 otherwise, including every no-op and every
/// respawn. The inbox re-registration is the one bridge socket call it makes; watcher
/// liveness stays local, and a down daemon cannot block it (the client fails fast).
///
/// The residual it does NOT cover (documented in ADR-0008): a session that goes idle
/// **forever** — never another Stop — whose watcher then dies stays deaf until it next
/// takes a turn or is restarted. That is the accepted limit of a zero-spurious-wake
/// design; the OS service supervises the daemon, this hook supervises the watcher.
/// Which end of a turn is being recorded.
#[derive(Clone, Copy)]
enum TurnBoundary {
    Started,
    Ended,
}

/// Stamp a turn boundary for `session` (ADR-0016).
///
/// Best-effort throughout: these stamps exist so a health check can avoid libelling a
/// busy session as unreachable. Losing one costs accuracy in `mailbox doctor`; failing
/// the hook over it would cost a turn, so it is logged and swallowed.
fn record_turn_boundary(session: &SessionId, boundary: TurnBoundary) {
    let sentinel = match Sentinel::for_session(session) {
        Ok(sentinel) => sentinel,
        Err(err) => {
            warn!(session = %session.as_str(), error = %err,
                "could not resolve the sentinel to record a turn boundary; \
                 `mailbox doctor` may report this session as deaf while it is merely busy");
            return;
        }
    };
    let now = std::time::SystemTime::now();
    let result = match boundary {
        TurnBoundary::Started => sentinel.record_turn_started(now),
        TurnBoundary::Ended => sentinel.record_turn_ended(now),
    };
    if let Err(err) = result {
        warn!(session = %session.as_str(), error = %err,
            "could not record a turn boundary; `mailbox doctor` may report this session \
             as deaf while it is merely busy");
    }
}

/// The `UserPromptSubmit` hook (ADR-0016): record that a turn has opened.
///
/// Pairs with the `Stop` hook's turn-ended stamp. It does nothing else — it prints
/// nothing, never blocks, and always exits 0, because a hook on the prompt path must
/// be incapable of getting between the user and their agent.
pub fn run_turn_start_hook() -> ExitCode {
    match HookInput::from_reader(std::io::stdin().lock()) {
        Ok(input) => record_turn_boundary(&input.session_id, TurnBoundary::Started),
        Err(err) => warn!(error = %err,
            "mailbox harness turn-start: could not read the UserPromptSubmit payload; \
             no turn boundary recorded"),
    }
    ExitCode::SUCCESS
}

pub async fn run_ensure_watcher_hook() -> ExitCode {
    let config = match StorageConfig::from_env() {
        Ok(config) => config,
        Err(err) => {
            // Logged, not just eprintln'd: a total ensure-watcher failure now ALSO means
            // the per-turn inbox re-registration (ADR-0013) never ran, so it must leave a
            // trace where an operator diagnosing an unreachable session looks (harness.log
            // when resolvable, else stderr). Same reasoning for the stdin branch below.
            error!(error = %err, "mailbox harness ensure-watcher: could not resolve storage config; \
                did not re-register the inbox or ensure the watcher");
            return ExitCode::FAILURE;
        }
    };
    let session = match HookInput::from_reader(std::io::stdin().lock()) {
        Ok(input) => input.session_id,
        Err(err) => {
            error!(error = %err, "mailbox harness ensure-watcher: could not read the Stop hook \
                payload; did not re-register the inbox or ensure the watcher");
            return ExitCode::FAILURE;
        }
    };

    // Close the turn (ADR-0016). The Stop hook already fires at every turn boundary,
    // so it is the natural place to record that this session is no longer executing —
    // which is what lets `mailbox doctor` tell a session that CANNOT be woken apart
    // from one that is merely mid-turn and will pick its mail up at this very
    // boundary. Best-effort: bookkeeping must never be able to fail a Stop hook.
    record_turn_boundary(&session, TurnBoundary::Ended);

    // Re-register the inbox on EVERY Stop (ADR-0007's invariant, restored — ADR-0013).
    // Best-effort and fail-open: a down bridge is logged and skipped, never fatal to the
    // hook (a Stop must never wake or fail). This is what heals a session whose inbox was
    // dropped and whose SessionStart re-registration was refused by the tombstone guard:
    // once the 10s guard lapses, the next Stop re-subscribes it. Idempotent — an existing
    // subscription's cursor is left untouched.
    register_inbox(&config, &session, "ensure-watcher").await;

    // A Stop hook must NOT print a `watchPaths` registration: Claude Code validates that
    // a hook's `hookSpecificOutput.hookEventName` matches the firing event, and watchPath
    // registration is a `SessionStart`-only output — emitting it here fails the Stop hook
    // ("expected 'Stop' but got 'SessionStart'"). So this hook stays silent on stdout and
    // only ensures watcher liveness. (Consequence: this hook cannot re-register the
    // watchPath mid-session — but `session-start` now re-registers it on every resume via
    // its wider matcher (ADR-0013), so a fresh process is not left relying on the previous
    // process's registration persisting.)

    // Ensure a detached watcher is alive; respawn only when it is missing or dead. A
    // live watcher is left strictly alone (the respawn would lose the single-instance
    // lock anyway, but skipping it avoids a needless per-turn process spawn).
    if mailbox::wake::waiter_alive(&config.waiters_dir(), &session) {
        info!(
            session = %session.as_str(),
            "ensure-watcher: a live watcher already holds the lock; leaving it (no-op)"
        );
    } else {
        info!(
            session = %session.as_str(),
            "ensure-watcher: no live watcher; respawning the detached watcher"
        );
        spawn_detached_watcher(&session);
    }

    // The ADR-0012 turn-boundary re-trigger: level-triggered here, edge-triggered
    // thereafter. This is what rescues mail that arrived while the session was BUSY.
    retrigger_wake_if_unread(&config, &session);

    // ALWAYS exit 0 — a Stop-liveness hook must never wake the session.
    ExitCode::SUCCESS
}

/// Run the ADR-0012 turn-boundary re-trigger and log what it decided.
///
/// The decision itself lives in [`Waiter::retrigger_if_unread`] (the wake domain owns
/// "read unread, bump the sentinel"); this is the hook-layer half — resolve the
/// config edges, then report the outcome. It is a safety net on a per-turn hook, so
/// every failure is a logged no-op: a `Stop` that failed loudly, or slowly, would
/// cost every turn on every session.
fn retrigger_wake_if_unread(config: &StorageConfig, session: &SessionId) {
    // No store: the bridge has never run here, so there is nothing to re-trigger.
    if !config.path().exists() {
        info!(session = %session.as_str(), "turn boundary: no mailbox store; nothing to re-trigger");
        return;
    }
    let sentinel = match Sentinel::for_session(session) {
        Ok(sentinel) => sentinel,
        Err(err) => {
            warn!(session = %session.as_str(), error = %err, "turn boundary: no sentinel path; skipping the re-trigger");
            return;
        }
    };

    let waiter = Waiter::new(
        config.waiters_dir(),
        config.path().to_path_buf(),
        session.clone(),
    );
    match waiter.retrigger_if_unread(&sentinel) {
        Ok(outcome) => log_retrigger(session, &outcome),
        Err(err) => {
            warn!(session = %session.as_str(), error = %err, "turn boundary: could not check unread; skipping the re-trigger")
        }
    }
}

/// Log one [`RetriggerOutcome`]. Exhaustive by construction, so a new outcome cannot
/// be added and silently go unreported — the last generation of lost-wake bugs was
/// invisible precisely because the deciding lines were not in the log
/// (ADR-0008/0009).
fn log_retrigger(session: &SessionId, outcome: &RetriggerOutcome) {
    let names = |topics: &[Topic]| {
        topics
            .iter()
            .map(Topic::as_str)
            .collect::<Vec<_>>()
            .join(",")
    };
    match outcome {
        RetriggerOutcome::CaughtUp => {
            info!(session = %session.as_str(), "turn boundary: session is caught up; nothing to re-trigger")
        }
        RetriggerOutcome::AlreadyRetriggered { last, high_water } => info!(
            session = %session.as_str(),
            // Both sides of the comparison, or the log cannot show WHY this mail was
            // judged already-nudged.
            last = ?last,
            watermark = high_water.get(),
            "turn boundary: this mail was already re-triggered; not nudging again (anti-loop)"
        ),
        RetriggerOutcome::Retriggered { topics, high_water } => info!(
            session = %session.as_str(),
            topics = names(topics),
            watermark = high_water.get(),
            "turn boundary: unread mail arrived while busy; re-bumped the wake sentinel (FileChanged will fire against the now-idle session)"
        ),
        RetriggerOutcome::RetriggeredUnrecorded {
            topics,
            high_water,
            error,
        } => warn!(
            session = %session.as_str(),
            topics = names(topics),
            watermark = high_water.get(),
            error = %error,
            "turn boundary: re-bumped the sentinel but could not record the watermark (a later turn may nudge again)"
        ),
        RetriggerOutcome::BumpFailed { error } => warn!(
            session = %session.as_str(),
            error = %error,
            "turn boundary: could not re-bump the wake sentinel; this mail waits for the next kick"
        ),
    }
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
    fn empty_session_flag_falls_back_to_env_not_a_phantom_session() {
        // The trap: `--session "$MAILBOX_SESSION_ID"` with that var unset expands to
        // `--session ""`. An empty flag must be treated as absent and fall through
        // to CLAUDE_CODE_SESSION_ID, NOT bind an anonymous empty session.
        let resolved = resolve_session(
            Some(SessionId::new("")),
            None,
            Some("from-claude-env".to_string()),
        )
        .unwrap();
        assert_eq!(resolved, SessionId::new("from-claude-env"));

        // Whitespace-only is treated the same, and a kept flag is trimmed for
        // consistency with the env sources.
        let whitespace = resolve_session(Some(SessionId::new("   ")), None, None).unwrap_err();
        assert!(format!("{whitespace}").contains("no session id"));
        let trimmed = resolve_session(Some(SessionId::new("  real  ")), None, None).unwrap();
        assert_eq!(trimmed, SessionId::new("real"));
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
