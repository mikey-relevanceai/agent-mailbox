//! Concrete [`AdapterResolver`]s that map a watch onto a real adapter program.
//!
//! The supervisor is deliberately decoupled from any concrete adapter — it takes
//! an injected [`AdapterResolver`] (see [`crate::supervisor`]). This module holds
//! the resolvers `serve` actually wires in. Today that is [`StubResolver`]: it
//! points a `stub` watch at the reference stub adapter binary and derives its
//! config from the watch row, while still reporting "no adapter" for `github-pr`
//! (the real poller lands in card 10). It replaces the card-08
//! [`UnavailableResolver`](crate::supervisor::UnavailableResolver), which knew no
//! kinds at all.
//!
//! # Binary-path resolution
//!
//! The stub program is resolved at spawn time (not construction): an env override
//! [`ENV_STUB_ADAPTER_BIN`] wins (tests/dev point it at the freshly built
//! binary), falling back to the bare binary name [`DEFAULT_STUB_ADAPTER_BIN`] so a
//! production install finds it on `PATH`. Resolving per-spawn means a test can set
//! the override before it triggers a watch without rebuilding the resolver.

use serde_json::json;
use tracing::debug;

use mailbox_protocol::AdapterId;

use crate::host::AdapterConfig;
use crate::host::subprocess::AdapterSpec;
use crate::storage::{Watch, WatchTarget};
use crate::supervisor::{AdapterResolver, ResolveError, ResolvedAdapter, topic_for_watch};

/// Env override for the stub adapter binary path (tests/dev). When unset, the
/// resolver falls back to [`DEFAULT_STUB_ADAPTER_BIN`] on `PATH`.
pub const ENV_STUB_ADAPTER_BIN: &str = "MAILBOX_STUB_ADAPTER_BIN";

/// Default stub adapter program name, found on `PATH` for a normal install.
const DEFAULT_STUB_ADAPTER_BIN: &str = "mailbox-stub-adapter";

/// Provenance the stub's events are stamped with (the host stamps this, not the
/// child — see [`crate::host::subprocess`]).
const STUB_ADAPTER_ID: &str = "stub-adapter";

/// The `serve` resolver: runs the reference stub adapter for `stub` watches and
/// reports [`ResolveError::NoAdapter`] for everything else (so a `github-pr`
/// watch still records intent and stays `Desired` until card 10).
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
            // The real `github-pr` poller ships in card 10; until then a github
            // watch records intent and stays Desired (no adapter resolved).
            WatchTarget::GithubPr { .. } => Err(ResolveError::NoAdapter {
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
    fn github_pr_still_has_no_adapter() {
        let err = StubResolver.resolve(&github_watch()).unwrap_err();
        assert!(matches!(
            err,
            ResolveError::NoAdapter {
                kind: WatchKind::GithubPr
            }
        ));
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
