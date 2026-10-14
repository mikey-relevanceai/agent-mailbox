//! Where the Slack tokens come from: the macOS Keychain, read through the
//! `security` CLI (ADR-0027).
//!
//! The token is never passed in through the bridge, in this adapter's config, or
//! in an environment variable. The daemon's environment is inherited by EVERY
//! adapter it spawns, so a token there would reach the stub and `gh` adapters
//! too. When this was built, `security` read an item that `security
//! add-generic-password` had created without prompting; if it ever prompts, the
//! item's access control list is the thing to check.
//!
//! The app has two tokens, each in its own Keychain item, and each a type of its
//! own ([`Bot`], [`App`]), so a call that needs one cannot be handed the other.

use std::fmt;
use std::marker::PhantomData;

/// Env var overriding the `security` binary, so tests can point at a fake.
pub const ENV_SECURITY_BIN: &str = "MAILBOX_SLACK_SECURITY_BIN";

const DEFAULT_SECURITY_BIN: &str = "security";

/// One of the app's tokens: where it is stored and what it looks like.
pub trait Credential {
    /// The Keychain service name the token is stored under.
    const SERVICE: &'static str;
    const PREFIXES: &'static [&'static str];
    const DESCRIPTION: &'static str;
}

/// The bot token (`xoxb-`), for Web API calls: reading conversations and looking
/// up names.
#[derive(Debug, Clone, Copy)]
pub struct Bot;

impl Credential for Bot {
    const SERVICE: &'static str = "agent-mailbox.slack";
    const PREFIXES: &'static [&'static str] = &["xoxb-", "xoxp-"];
    const DESCRIPTION: &'static str = "a Slack bot token (xoxb-… or xoxp-…)";
}

/// The app-level token (`xapp-`, scope `connections:write`), which can do
/// nothing but open a Socket Mode connection (design/04). Slack issues and
/// rotates it apart from the bot token.
#[derive(Debug, Clone, Copy)]
pub struct App;

impl Credential for App {
    const SERVICE: &'static str = "agent-mailbox.slack-app-token";
    const PREFIXES: &'static [&'static str] = &["xapp-"];
    const DESCRIPTION: &'static str = "a Slack app-level token (xapp-…)";
}

/// A Slack token of kind `C`. `Debug` is redacted so it cannot reach a log by
/// accident.
pub struct Token<C> {
    raw: String,
    kind: PhantomData<C>,
}

// Written out rather than derived: a derive would require `C: Clone`.
impl<C> Clone for Token<C> {
    fn clone(&self) -> Self {
        Self {
            raw: self.raw.clone(),
            kind: PhantomData,
        }
    }
}

impl<C> Token<C> {
    pub fn expose(&self) -> &str {
        &self.raw
    }
}

impl<C> fmt::Debug for Token<C> {
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
pub async fn load<C: Credential>() -> Result<Token<C>, TokenError> {
    let bin = std::env::var(ENV_SECURITY_BIN)
        .ok()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| DEFAULT_SECURITY_BIN.to_string());
    let output = tokio::process::Command::new(&bin)
        .args(["find-generic-password", "-s", C::SERVICE, "-w"])
        .output()
        .await
        .map_err(|source| TokenError::Spawn {
            bin: bin.clone(),
            source,
        })?;
    // `security` exits non-zero when the item does not exist. Its stderr is not
    // forwarded: it names the item, which the error above already does.
    if !output.status.success() {
        return Err(TokenError::Missing {
            service: C::SERVICE,
        });
    }
    parse(&String::from_utf8_lossy(&output.stdout))
}

fn parse<C: Credential>(stdout: &str) -> Result<Token<C>, TokenError> {
    let token = stdout.trim();
    // A token is a single header-safe word; anything else would corrupt the curl
    // config it is written into, so it is refused rather than escaped.
    let well_formed = C::PREFIXES.iter().any(|prefix| token.starts_with(prefix))
        && token.chars().all(|c| c.is_ascii_alphanumeric() || c == '-');
    if !well_formed {
        return Err(TokenError::Malformed {
            service: C::SERVICE,
            expected: C::DESCRIPTION,
        });
    }
    Ok(Token {
        raw: token.to_string(),
        kind: PhantomData,
    })
}

#[cfg(test)]
pub mod tests_support {
    use super::{Bot, Token};

    pub fn token(raw: &str) -> Token<Bot> {
        super::parse(raw).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_bot_and_user_tokens_and_trims_the_newline() {
        let bot = |raw| parse::<Bot>(raw).unwrap();
        assert_eq!(bot("xoxb-1-2-abc\n").expose(), "xoxb-1-2-abc");
        assert_eq!(bot("xoxp-1-2-abc").expose(), "xoxp-1-2-abc");
    }

    #[test]
    fn refuses_anything_that_could_break_out_of_a_header() {
        for bad in ["", "hunter2", "xoxb-ok\"\nurl = \"https://evil", "xoxb-a b"] {
            assert!(
                matches!(parse::<Bot>(bad), Err(TokenError::Malformed { .. })),
                "{bad:?}"
            );
        }
    }

    /// Each item holds one kind of token, so a token stored under the wrong
    /// service is caught at start rather than as a confusing `invalid_auth`.
    #[test]
    fn each_credential_accepts_only_its_own_kind_of_token() {
        assert!(parse::<App>("xapp-1-A0-123-abc").is_ok());
        assert!(parse::<App>("xoxb-1-2-abc").is_err());
        assert!(parse::<Bot>("xapp-1-A0-123-abc").is_err());
    }

    #[test]
    fn debug_never_shows_the_token() {
        let token = parse::<Bot>("xoxb-secret").unwrap();
        assert!(!format!("{token:?}").contains("secret"));
    }
}
