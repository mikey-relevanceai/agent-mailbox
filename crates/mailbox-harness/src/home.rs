//! The one home resolution both setup commands use.
//!
//! `install-skills` defaults to `~/.claude/skills` and `install-hooks` defaults to
//! `~/.claude/settings.json`. Both must resolve `~` the SAME way, or a test (or a
//! sandbox) that redirects the home for one command would silently write into the
//! user's real `~/.claude` through the other. One convention, one implementation:
//! [`ENV_HOME`] first, then `HOME` — and **`None` rather than a guess** when
//! neither is set, because a guessed path inside someone's config space is exactly
//! the mistake worth making unrepresentable.

use std::path::PathBuf;

/// Env var overriding the home the default Claude Code paths resolve under,
/// falling back to `HOME`. Mirrors the storage layer's precedence so tests and
/// sandboxes have ONE way to escape the real home — a second convention here would
/// be a second thing to remember to override.
pub const ENV_HOME: &str = "AGENT_MAILBOX_HOME";

/// Claude Code's config directory under a home.
pub const CLAUDE_DIR: &str = ".claude";

/// The home the default Claude Code paths resolve under, or `None` when neither
/// [`ENV_HOME`] nor `HOME` is set.
///
/// This is the ONLY place the global environment is read: every layout decision
/// downstream is a pure function of the returned home, so it stays testable
/// without mutating the (process-global, test-shared) environment.
pub fn harness_home() -> Option<PathBuf> {
    env_path(ENV_HOME).or_else(|| env_path("HOME"))
}

/// A non-empty, **absolute** env var read as a path.
///
/// An empty value is unset. A *relative* one is refused for the same reason this
/// module returns `None` rather than guessing: with a relative `HOME` (containers,
/// CI, `env -i`), `<home>/.claude/settings.json` resolves against the CWD — so
/// `install-hooks` run from a project root would merge into that project's
/// `.claude/settings.json`, which is Claude Code's *project* settings file, is
/// committed, and ships to teammates — all while reporting it as the user's home
/// settings. A relative home is a mistake, not an intent.
fn env_path(key: &str) -> Option<PathBuf> {
    usable_home(std::env::var_os(key))
}

/// The pure half of [`env_path`]: which raw env values are a usable home. Split out
/// so the rules are testable without mutating the (process-global, test-shared)
/// environment.
fn usable_home(value: Option<std::ffi::OsString>) -> Option<PathBuf> {
    value
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .filter(|path| path.is_absolute())
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::ffi::OsString;

    fn home_of(value: &str) -> Option<PathBuf> {
        usable_home(Some(OsString::from(value)))
    }

    #[test]
    fn an_absolute_home_is_used() {
        assert_eq!(home_of("/home/u"), Some(PathBuf::from("/home/u")));
    }

    #[test]
    fn an_unset_or_empty_home_is_no_home() {
        assert_eq!(usable_home(None), None);
        assert_eq!(home_of(""), None);
    }

    /// A RELATIVE home would resolve `<home>/.claude/settings.json` against the CWD:
    /// run from a project root, `install-hooks` would merge into that project's
    /// committed `.claude/settings.json` while calling it the user's home settings.
    #[test]
    fn a_relative_home_is_refused_rather_than_resolved_against_the_cwd() {
        assert_eq!(home_of("relative/home"), None);
        assert_eq!(home_of("."), None);
        assert_eq!(home_of(".."), None);
    }
}
