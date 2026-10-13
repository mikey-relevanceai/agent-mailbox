//! Where the Slack tokens come from: the macOS Keychain, read through the
//! `security` CLI (ADR-0027).
//!
//! The token is never passed in through the bridge, in this adapter's config, or
//! in an environment variable. The daemon's environment is inherited by EVERY
//! adapter it spawns, so a token there would reach the stub and `gh` adapters
//! too. When this was built, `security` read an item that `security
//! add-generic-password` had created without prompting; if it ever prompts, the
//! item's access control list is the thing to check.

use std::fmt;

/// Env var overriding the `security` binary, so tests can point at a fake.
pub const ENV_SECURITY_BIN: &str = "MAILBOX_SLACK_SECURITY_BIN";

const DEFAULT_SECURITY_BIN: &str = "security";

/// Which of the app's two tokens to read. Each has its own Keychain item.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Credential {
    /// The bot token (`xoxb-`), for Web API calls: reading conversations and
    /// looking up names.
    Bot,
    /// The app-level token (`xapp-`, scope `connections:write`), which can do
    /// nothing but open a Socket Mode connection (design/04). It is a separate
    /// item, not a second field, because Slack issues and rotates them
    /// separately.
    App,
}

impl Credential {
    /// The Keychain service name the token is stored under.
    pub fn service(self) -> &'static str {
        match self {
            Credential::Bot => "agent-mailbox.slack",
            Credential::App => "agent-mailbox.slack-app-token",
        }
    }

    fn prefixes(self) -> &'static [&'static str] {
        match self {
            Credential::Bot => &["xoxb-", "xoxp-"],
            Credential::App => &["xapp-"],
        }
    }

    fn describe(self) -> &'static str {
        match self {
            Credential::Bot => "a Slack bot token (xoxb-… or xoxp-…)",
            Credential::App => "a Slack app-level token (xapp-…)",
        }
    }
}

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
        "no Slack token in the Keychain under service {service:?}; store one with: security add-generic-password -U -s {service} -a \"$USER\" -w"
    )]
    Missing { service: &'static str },
    #[error("could not run {bin:?} to read the Slack token from the Keychain: {source}")]
    Spawn {
        bin: String,
        #[source]
        source: std::io::Error,
    },
    #[error("the Keychain item {service:?} is not {expected}")]
    Malformed {
        service: &'static str,
        expected: &'static str,
    },
}

/// Read one of the app's tokens from the Keychain.
pub async fn load(credential: Credential) -> Result<Token, TokenError> {
    let bin = std::env::var(ENV_SECURITY_BIN)
        .ok()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| DEFAULT_SECURITY_BIN.to_string());
    let service = credential.service();
    let output = tokio::process::Command::new(&bin)
        .args(["find-generic-password", "-s", service, "-w"])
        .output()
        .await
        .map_err(|source| TokenError::Spawn {
            bin: bin.clone(),
            source,
        })?;
    // `security` exits non-zero when the item does not exist. Its stderr is not
    // forwarded: it names the item, which the error above already does.
    if !output.status.success() {
        return Err(TokenError::Missing { service });
    }
    parse(credential, &String::from_utf8_lossy(&output.stdout))
}

fn parse(credential: Credential, stdout: &str) -> Result<Token, TokenError> {
    let token = stdout.trim();
    // A token is a single header-safe word; anything else would corrupt the curl
    // config it is written into, so it is refused rather than escaped.
    let well_formed = credential
        .prefixes()
        .iter()
        .any(|prefix| token.starts_with(prefix))
        && token.chars().all(|c| c.is_ascii_alphanumeric() || c == '-');
    if !well_formed {
        return Err(TokenError::Malformed {
            service: credential.service(),
            expected: credential.describe(),
        });
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
        let bot = |raw| parse(Credential::Bot, raw).unwrap();
        assert_eq!(bot("xoxb-1-2-abc\n").expose(), "xoxb-1-2-abc");
        assert_eq!(bot("xoxp-1-2-abc").expose(), "xoxp-1-2-abc");
    }

    #[test]
    fn refuses_anything_that_could_break_out_of_a_header() {
        for bad in ["", "hunter2", "xoxb-ok\"\nurl = \"https://evil", "xoxb-a b"] {
            assert!(
                matches!(
                    parse(Credential::Bot, bad),
                    Err(TokenError::Malformed { .. })
                ),
                "{bad:?}"
            );
        }
    }

    /// Each item holds one kind of token, so a token stored under the wrong
    /// service is caught at start rather than as a confusing `invalid_auth`.
    #[test]
    fn each_credential_accepts_only_its_own_kind_of_token() {
        assert!(parse(Credential::App, "xapp-1-A0-123-abc").is_ok());
        assert!(parse(Credential::App, "xoxb-1-2-abc").is_err());
        assert!(parse(Credential::Bot, "xapp-1-A0-123-abc").is_err());
    }

    #[test]
    fn debug_never_shows_the_token() {
        let token = parse(Credential::Bot, "xoxb-secret").unwrap();
        assert!(!format!("{token:?}").contains("secret"));
    }
}
