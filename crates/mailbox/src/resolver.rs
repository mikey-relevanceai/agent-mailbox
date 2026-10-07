//! Concrete [`AdapterResolver`]s that map a watch onto a real adapter program.
//!
//! The supervisor is deliberately decoupled from any concrete adapter — it takes
//! an injected [`AdapterResolver`] (see [`crate::supervisor`]). This module holds
//! the resolvers `serve` actually wires in.
//!
//! - [`StubResolver`] points a `stub` watch at the reference stub adapter and
//!   derives its config from the watch row (card 09).
//! - [`GithubPrResolver`] points a `github-pr` watch at the real PR poller and
//!   derives its config (repo/pr/interval) from the watch row (card 10).
//! - [`DefaultResolver`] is what `serve` injects: it routes each watch kind to
//!   the resolver above for it, so BOTH `stub` and `github-pr` now resolve to a
//!   real adapter. It replaces the card-08
//!   [`UnavailableResolver`](crate::supervisor::UnavailableResolver) (no kinds)
//!   and the card-09 stub-only default.
//!
//! # Binary-path resolution
//!
//! Each program is resolved at spawn time (not construction): an env override
//! (e.g. [`ENV_STUB_ADAPTER_BIN`] / [`ENV_GITHUB_PR_ADAPTER_BIN`]) wins (tests/dev
//! point it at the freshly built binary), else the adapter co-installed beside the
//! running bridge binary, else the bare binary name on `PATH`. Resolving per-spawn
//! means a test can set the override before it triggers a watch without rebuilding
//! the resolver.

use serde_json::json;
use tracing::debug;

use mailbox_protocol::AdapterId;

use crate::host::AdapterConfig;
use crate::host::subprocess::AdapterSpec;
use crate::storage::{Watch, WatchKind, WatchTarget};
use crate::supervisor::{AdapterResolver, ResolveError, ResolvedAdapter, topic_for_watch};

/// Env override for the stub adapter binary path (tests/dev). When unset, the
/// resolver falls back to [`DEFAULT_STUB_ADAPTER_BIN`] on `PATH`.
pub const ENV_STUB_ADAPTER_BIN: &str = "MAILBOX_STUB_ADAPTER_BIN";

/// Default stub adapter program name, found on `PATH` for a normal install.
const DEFAULT_STUB_ADAPTER_BIN: &str = "mailbox-stub-adapter";

/// Provenance the stub's events are stamped with (the host stamps this, not the
/// child — see [`crate::host::subprocess`]).
const STUB_ADAPTER_ID: &str = "stub-adapter";

/// Resolves the reference stub adapter for `stub` watches and reports
/// [`ResolveError::NoAdapter`] for other kinds. `github-pr` is handled by
/// [`GithubPrResolver`]; the production [`DefaultResolver`] routes each kind to
/// the right one, so this resolver's `NoAdapter` arm for github is only reached
/// if it is used standalone (as in the `watch` unit tests).
///
/// Stateless — the program path is read from the environment per resolve — so it
/// is cheap to construct and share behind the supervisor's `Arc`.
pub struct StubResolver;

impl StubResolver {
    /// The stub adapter program, and whether it came from the env override.
    ///
    /// Prefers the [`ENV_STUB_ADAPTER_BIN`] override (privileged deployments
    /// should pass an absolute path here). With it unset, it defaults to the
    /// adapter co-installed **beside the running bridge binary** — deterministic
    /// and `PATH`-independent — and only falls back to the bare binary name (on
    /// `PATH`) if the current-exe directory cannot be resolved.
    fn program() -> (String, bool) {
        if let Ok(value) = std::env::var(ENV_STUB_ADAPTER_BIN)
            && !value.is_empty()
        {
            return (value, true);
        }
        let colocated = std::env::current_exe()
            .ok()
            .and_then(|exe| exe.parent().map(|dir| dir.join(DEFAULT_STUB_ADAPTER_BIN)))
            .filter(|path| path.exists())
            .and_then(|path| path.to_str().map(str::to_string));
        (
            colocated.unwrap_or_else(|| DEFAULT_STUB_ADAPTER_BIN.to_string()),
            false,
        )
    }
}

impl AdapterResolver for StubResolver {
    fn resolve(&self, watch: &Watch) -> Result<ResolvedAdapter, ResolveError> {
        match &watch.target {
            // Other kinds have their own resolvers; DefaultResolver never routes
            // them here.
            WatchTarget::GithubPr { .. } | WatchTarget::Slack(_) => Err(ResolveError::NoAdapter {
                kind: watch.target.kind(),
            }),
            WatchTarget::Stub { label, count } => {
                // Derive the whole config (topic/interval/count) from the watch row
                // so a re-watch's updated interval/count take effect on respawn.
                let topic = topic_for_watch(watch).ok_or_else(|| {
                    ResolveError::Invalid(format!("invalid stub label {label:?}"))
                })?;
                let config = json!({
                    "topic": topic.as_str(),
                    "interval_ms": interval_ms(watch),
                    "count": count,
                });
                let (program, env_override) = StubResolver::program();
                debug!(
                    watch = watch.id.get(),
                    program = %program,
                    env_override,
                    "resolved stub adapter binary"
                );
                Ok(ResolvedAdapter {
                    spec: AdapterSpec::new(program, AdapterId(STUB_ADAPTER_ID.to_string())),
                    config: AdapterConfig::new(config),
                })
            }
        }
    }
}

/// Env override for the github-pr adapter binary path (tests/dev). When unset,
/// the resolver falls back to the co-located binary, then
/// [`DEFAULT_GITHUB_PR_ADAPTER_BIN`] on `PATH`.
pub const ENV_GITHUB_PR_ADAPTER_BIN: &str = "MAILBOX_GH_ADAPTER_BIN";

/// Default github-pr adapter program name, found on `PATH` for a normal install.
const DEFAULT_GITHUB_PR_ADAPTER_BIN: &str = "mailbox-github-pr-adapter";

/// Provenance the github-pr adapter's events are stamped with (the host stamps
/// this, not the child).
const GITHUB_PR_ADAPTER_ID: &str = "github-pr-adapter";

/// The `serve` resolver for `github-pr` watches: runs the real PR poller (card
/// 10) and derives its config (repo/pr/interval) from the watch row. Reports
/// [`ResolveError::NoAdapter`] for other kinds (routed elsewhere by
/// [`DefaultResolver`]).
///
/// Stateless — the program path is read from the environment per resolve — so it
/// is cheap to construct and share behind the supervisor's `Arc`. The baseline is
/// NOT built here: the supervisor injects the persisted baseline into the config
/// at spawn (design/01 / card 10), keeping this resolver storage-free.
pub struct GithubPrResolver;

impl GithubPrResolver {
    /// The github-pr adapter program, and whether it came from the env override.
    /// Same precedence as [`StubResolver::program`]: env override → co-located
    /// beside the bridge binary → bare name on `PATH`.
    fn program() -> (String, bool) {
        if let Ok(value) = std::env::var(ENV_GITHUB_PR_ADAPTER_BIN)
            && !value.is_empty()
        {
            return (value, true);
        }
        let colocated = std::env::current_exe()
            .ok()
            .and_then(|exe| {
                exe.parent()
                    .map(|dir| dir.join(DEFAULT_GITHUB_PR_ADAPTER_BIN))
            })
            .filter(|path| path.exists())
            .and_then(|path| path.to_str().map(str::to_string));
        (
            colocated.unwrap_or_else(|| DEFAULT_GITHUB_PR_ADAPTER_BIN.to_string()),
            false,
        )
    }
}

impl AdapterResolver for GithubPrResolver {
    fn resolve(&self, watch: &Watch) -> Result<ResolvedAdapter, ResolveError> {
        match &watch.target {
            WatchTarget::Stub { .. } | WatchTarget::Slack(_) => Err(ResolveError::NoAdapter {
                kind: watch.target.kind(),
            }),
            WatchTarget::GithubPr { repo, pr } => {
                // The stored `repo` column is `owner/repo`; split it for the
                // adapter's `gh --repo owner/repo` invocation.
                let (owner, repo_name) = repo.split_once('/').ok_or_else(|| {
                    ResolveError::Invalid(format!("watch repo {repo:?} is not owner/repo"))
                })?;
                let topic = topic_for_watch(watch).ok_or_else(|| {
                    ResolveError::Invalid(format!("invalid github repo {repo:?}"))
                })?;
                let config = json!({
                    "topic": topic.as_str(),
                    "owner": owner,
                    "repo": repo_name,
                    "number": pr,
                    "interval_ms": interval_ms(watch),
                });
                let (program, env_override) = GithubPrResolver::program();
                debug!(
                    watch = watch.id.get(),
                    program = %program,
                    env_override,
                    "resolved github-pr adapter binary"
                );
                Ok(ResolvedAdapter {
                    spec: AdapterSpec::new(program, AdapterId(GITHUB_PR_ADAPTER_ID.to_string())),
                    config: AdapterConfig::new(config),
                })
            }
        }
    }
}

/// Env override for the Slack adapter binary path (tests/dev). When unset, the
/// resolver falls back to the co-located binary, then [`DEFAULT_SLACK_ADAPTER_BIN`]
/// on `PATH`.
pub const ENV_SLACK_ADAPTER_BIN: &str = "MAILBOX_SLACK_ADAPTER_BIN";

/// Default Slack adapter program name, found on `PATH` for a normal install.
const DEFAULT_SLACK_ADAPTER_BIN: &str = "mailbox-slack-adapter";

/// Provenance the Slack adapter's events are stamped with (the host stamps this,
/// not the child).
const SLACK_ADAPTER_ID: &str = "slack-adapter";

/// The `serve` resolver for `slack-channel` and `slack-thread` watches (design/02).
/// One adapter program serves both kinds; the config says which. Like
/// [`GithubPrResolver`] it is storage-free: the supervisor injects the baseline.
///
/// The config carries no credential. The adapter reads its Slack token from the
/// macOS Keychain itself (ADR-0027), so the token never passes through the bridge.
pub struct SlackResolver;

impl SlackResolver {
    /// Same precedence as [`StubResolver::program`]: env override → co-located
    /// beside the bridge binary → bare name on `PATH`.
    fn program() -> (String, bool) {
        if let Ok(value) = std::env::var(ENV_SLACK_ADAPTER_BIN)
            && !value.is_empty()
        {
            return (value, true);
        }
        let colocated = std::env::current_exe()
            .ok()
            .and_then(|exe| exe.parent().map(|dir| dir.join(DEFAULT_SLACK_ADAPTER_BIN)))
            .filter(|path| path.exists())
            .and_then(|path| path.to_str().map(str::to_string));
        (
            colocated.unwrap_or_else(|| DEFAULT_SLACK_ADAPTER_BIN.to_string()),
            false,
        )
    }
}

impl AdapterResolver for SlackResolver {
    fn resolve(&self, watch: &Watch) -> Result<ResolvedAdapter, ResolveError> {
        let WatchTarget::Slack(slack) = &watch.target else {
            return Err(ResolveError::NoAdapter {
                kind: watch.target.kind(),
            });
        };
        let config = json!({
            "topic": slack.topic().as_str(),
            "channel": slack.channel().as_str(),
            "thread_ts": slack.thread_ts().map(|ts| ts.as_str()),
            "interval_ms": interval_ms(watch),
        });
        let (program, env_override) = SlackResolver::program();
        debug!(
            watch = watch.id.get(),
            program = %program,
            env_override,
            "resolved slack adapter binary"
        );
        Ok(ResolvedAdapter {
            spec: AdapterSpec::new(program, AdapterId(SLACK_ADAPTER_ID.to_string())),
            config: AdapterConfig::new(config),
        })
    }
}

/// The production `serve` resolver: routes each watch kind to its adapter —
/// `stub` to [`StubResolver`], `github-pr` to [`GithubPrResolver`], both Slack
/// kinds to [`SlackResolver`]. Matching on the kind (rather than one resolver
/// knowing every kind) keeps each resolver focused and makes adding a kind a
/// matter of adding an arm here.
pub struct DefaultResolver {
    stub: StubResolver,
    github: GithubPrResolver,
    slack: SlackResolver,
}

impl Default for DefaultResolver {
    fn default() -> Self {
        Self {
            stub: StubResolver,
            github: GithubPrResolver,
            slack: SlackResolver,
        }
    }
}

impl AdapterResolver for DefaultResolver {
    fn resolve(&self, watch: &Watch) -> Result<ResolvedAdapter, ResolveError> {
        match watch.target.kind() {
            WatchKind::Stub => self.stub.resolve(watch),
            WatchKind::GithubPr => self.github.resolve(watch),
            WatchKind::SlackChannel | WatchKind::SlackThread => self.slack.resolve(watch),
        }
    }
}

/// The watch's interval in whole milliseconds, saturating rather than wrapping on
/// the practically-impossible overflow of a `u128`-millis Duration into `u64`.
fn interval_ms(watch: &Watch) -> u64 {
    u64::try_from(watch.interval.as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    use crate::storage::{WatchId, WatchKind, WatchState};

    fn stub_watch(label: &str, interval: Duration, count: u64) -> Watch {
        Watch {
            id: WatchId::new(1),
            target: WatchTarget::Stub {
                label: label.to_string(),
                count,
            },
            interval,
            state: WatchState::Desired,
        }
    }

    fn github_watch() -> Watch {
        Watch {
            id: WatchId::new(2),
            target: WatchTarget::GithubPr {
                repo: "octocat/hello-world".to_string(),
                pr: 42,
            },
            interval: Duration::from_secs(60),
            state: WatchState::Desired,
        }
    }

    #[test]
    fn resolves_stub_config_from_the_watch_row() {
        let watch = stub_watch("demo", Duration::from_millis(250), 7);
        let resolved = StubResolver.resolve(&watch).unwrap();
        let config = resolved.config.value();
        assert_eq!(config["topic"], "stub.demo");
        assert_eq!(config["interval_ms"], 250);
        assert_eq!(config["count"], 7);
    }

    #[test]
    fn github_pr_resolver_builds_config_from_the_watch_row() {
        let resolved = GithubPrResolver.resolve(&github_watch()).unwrap();
        let config = resolved.config.value();
        assert_eq!(config["topic"], "github.pr.octocat/hello-world#42");
        assert_eq!(config["owner"], "octocat");
        assert_eq!(config["repo"], "hello-world");
        assert_eq!(config["number"], 42);
        assert_eq!(config["interval_ms"], 60_000);
        // The baseline is injected by the supervisor at spawn, not by the
        // resolver — so the resolved config carries no baseline key.
        assert!(config.get("baseline").is_none());
    }

    #[test]
    fn github_pr_resolver_reports_no_adapter_for_stub() {
        let watch = stub_watch("demo", Duration::from_millis(250), 0);
        let err = GithubPrResolver.resolve(&watch).unwrap_err();
        assert!(matches!(
            err,
            ResolveError::NoAdapter {
                kind: WatchKind::Stub
            }
        ));
    }

    #[test]
    fn default_resolver_routes_each_kind_to_its_adapter() {
        let resolver = DefaultResolver::default();
        // A stub watch resolves via the stub resolver...
        let stub = resolver
            .resolve(&stub_watch("demo", Duration::from_millis(250), 0))
            .unwrap();
        assert_eq!(stub.config.value()["topic"], "stub.demo");
        // ...and a github watch via the github resolver.
        let github = resolver.resolve(&github_watch()).unwrap();
        assert_eq!(
            github.config.value()["topic"],
            "github.pr.octocat/hello-world#42"
        );
    }

    #[test]
    fn program_reports_no_override_when_env_unset() {
        // The env override is exercised end-to-end by the stub integration test
        // (which sets it on the spawned `serve`); here we only pin that, with the
        // env unset, the resolver reports `env_override = false` and yields a
        // non-empty program (either the co-located binary or the default name).
        // No process-global env mutation (the workspace denies the `unsafe` that
        // `set_var` now requires).
        if std::env::var(ENV_STUB_ADAPTER_BIN).is_err() {
            let (program, env_override) = StubResolver::program();
            assert!(!env_override, "unset env must report no override");
            assert!(!program.is_empty());
        }
    }
}
