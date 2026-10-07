//! Where the Slack token comes from: the macOS Keychain, read through the
//! `security` CLI (ADR-0027).
//!
//! The token is never passed in through the bridge, in this adapter's config, or
//! in an environment variable. The daemon's environment is inherited by EVERY
//! adapter it spawns, so a token there would reach the stub and `gh` adapters
//! too. When this was built, `security` read an item that `security
//! add-generic-password` had created without prompting; if it ever prompts, the
//! item's access control list is the thing to check.

use std::fmt;

/// The Keychain service name the token is stored under. Store it with:
/// `security add-generic-password -s agent-mailbox.slack -a "$USER" -w`
pub const KEYCHAIN_SERVICE: &str = "agent-mailbox.slack";

/// Env var overriding the `security` binary, so tests can point at a fake.
pub const ENV_SECURITY_BIN: &str = "MAILBOX_SLACK_SECURITY_BIN";

const DEFAULT_SECURITY_BIN: &str = "security";

/// A Slack token. `Debug` is redacted so it cannot reach a log by accident.
#[derive(Clone)]
pub struct Token(String);

impl Token {
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Token {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Token(<redacted>)")
    }
}

#[derive(Debug, thiserror::Error)]
pub enum TokenError {
    #[error(
        "no Slack token in the Keychain under service {KEYCHAIN_SERVICE:?}; store one with: security add-generic-password -s {KEYCHAIN_SERVICE} -a \"$USER\" -w"
    )]
    Missing,
    #[error("could not run {bin:?} to read the Slack token from the Keychain: {source}")]
    Spawn {
        bin: String,
        #[source]
        source: std::io::Error,
    },
    #[error(
        "the Keychain item {KEYCHAIN_SERVICE:?} is not a Slack token (expected xoxb-… or xoxp-…)"
    )]
    Malformed,
}

/// Read the token from the Keychain.
pub async fn load() -> Result<Token, TokenError> {
    let bin = std::env::var(ENV_SECURITY_BIN)
        .ok()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| DEFAULT_SECURITY_BIN.to_string());
    let output = tokio::process::Command::new(&bin)
        .args(["find-generic-password", "-s", KEYCHAIN_SERVICE, "-w"])
        .output()
        .await
        .map_err(|source| TokenError::Spawn {
            bin: bin.clone(),
            source,
        })?;
    // `security` exits non-zero when the item does not exist. Its stderr is not
    // forwarded: it names the item, which the error above already does.
    if !output.status.success() {
        return Err(TokenError::Missing);
    }
    parse(&String::from_utf8_lossy(&output.stdout))
}

fn parse(stdout: &str) -> Result<Token, TokenError> {
    let token = stdout.trim();
    // A token is a single header-safe word; anything else would corrupt the curl
    // config it is written into, so it is refused rather than escaped.
    let well_formed = (token.starts_with("xoxb-") || token.starts_with("xoxp-"))
        && token.chars().all(|c| c.is_ascii_alphanumeric() || c == '-');
    if !well_formed {
        return Err(TokenError::Malformed);
    }
    Ok(Token(token.to_string()))
}

#[cfg(test)]
pub mod tests_support {
    use super::Token;

    pub fn token(raw: &str) -> Token {
        Token(raw.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_bot_and_user_tokens_and_trims_the_newline() {
        assert_eq!(parse("xoxb-1-2-abc\n").unwrap().expose(), "xoxb-1-2-abc");
        assert_eq!(parse("xoxp-1-2-abc").unwrap().expose(), "xoxp-1-2-abc");
    }

    #[test]
    fn refuses_anything_that_could_break_out_of_a_header() {
        for bad in ["", "hunter2", "xoxb-ok\"\nurl = \"https://evil", "xoxb-a b"] {
            assert!(matches!(parse(bad), Err(TokenError::Malformed)), "{bad:?}");
        }
    }

    #[test]
    fn debug_never_shows_the_token() {
        let token = parse("xoxb-secret").unwrap();
        assert!(!format!("{token:?}").contains("secret"));
    }
}
