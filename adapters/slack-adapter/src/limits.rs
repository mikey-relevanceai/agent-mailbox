//! Retry budgets shared by every mode, from the environment so tests can shrink
//! them.

use std::time::Duration;

/// Wait after a rate limit that came with no `Retry-After`.
const DEFAULT_RATE_LIMIT_BACKOFF_MS: u64 = 30_000;
const ENV_RATE_LIMIT_BACKOFF_MS: &str = "MAILBOX_SLACK_RATE_LIMIT_BACKOFF_MS";

/// Consecutive rate limits before exiting non-zero, so a persistent limit
/// surfaces through the supervisor instead of hiding behind a live process.
const DEFAULT_MAX_RATE_LIMIT_RETRIES: u64 = 12;
const ENV_MAX_RATE_LIMIT_RETRIES: &str = "MAILBOX_SLACK_MAX_RATE_LIMIT_RETRIES";

/// Consecutive failures before exiting non-zero: skipped polls in the poll
/// modes, failed connections in the bot mode. A chosen budget, ten minutes at
/// the default poll interval, not one derived from measured outages. Past it,
/// the supervisor's give-up is the more useful signal.
const DEFAULT_MAX_TRANSIENT_FAILURES: u64 = 10;
const ENV_MAX_TRANSIENT_FAILURES: &str = "MAILBOX_SLACK_MAX_TRANSIENT_FAILURES";

pub struct Limits {
    pub rate_limit_backoff: Duration,
    pub max_rate_limit_retries: u64,
    pub max_transient: u64,
}

impl Limits {
    pub fn from_env() -> Self {
        Self {
            rate_limit_backoff: Duration::from_millis(env_u64(
                ENV_RATE_LIMIT_BACKOFF_MS,
                DEFAULT_RATE_LIMIT_BACKOFF_MS,
            )),
            max_rate_limit_retries: env_u64(
                ENV_MAX_RATE_LIMIT_RETRIES,
                DEFAULT_MAX_RATE_LIMIT_RETRIES,
            ),
            max_transient: env_u64(ENV_MAX_TRANSIENT_FAILURES, DEFAULT_MAX_TRANSIENT_FAILURES),
        }
    }
}

pub fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}
