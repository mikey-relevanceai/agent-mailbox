//! The session identity shared by the bridge and the harness.
//!
//! `SessionId` lives here — not in the bridge's storage layer — because two
//! crates need the *same* branded type and the *same* filesystem encoding:
//!
//! - the bridge (`mailbox`) keys subscriptions, interests, and wake FIFOs by it;
//! - the harness (`mailbox-harness`) reads it from the Claude Code hook payload
//!   and writes a per-session waiter pidfile beside that FIFO.
//!
//! The pidfile and the FIFO/lock MUST map a session id to the same filename stem,
//! or a session's files scatter and the coordination in the wake loop breaks. A
//! single shared [`SessionId::encode_filename`] guarantees they agree by
//! construction (there is no second copy of the rule to drift).

use serde::{Deserialize, Serialize};

/// Identity of a Claude/Codex session that expresses interest in a topic or
/// watch. Sourced from the harness (hook `session_id`); the bridge treats it as
/// an opaque label. Branded so it cannot be swapped with a `Topic` or any other
/// string at a call site.
///
/// The inner string is private and minted only through [`SessionId::new`] — so a
/// call site cannot reach in and treat it as a bare `String`.
/// `#[serde(transparent)]` so on the wire a session id is just its bare string —
/// no envelope for a non-Rust peer to produce — while in Rust it stays branded.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SessionId(String);

impl SessionId {
    /// Wrap a session label coming from the harness. Accepts anything
    /// string-like so call sites need not pre-convert.
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// Borrow as a string slice (for binding into SQL, argv, or logs).
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Encode this session id into a filesystem-safe, collision-free file stem.
    ///
    /// The one authoritative rule shared by the wake FIFO/lock (`mailbox::wake`)
    /// and the waiter pidfile (`mailbox-harness`): a session's files must key
    /// identically or the wake loop's coordination breaks.
    ///
    /// Crucially the safe set excludes UPPERCASE ASCII letters: macOS's default
    /// APFS is case-INSENSITIVE, so `aB` and `Ab` would otherwise map to the same
    /// file. Encoding every non-lowercase byte as `%XX` (uppercase hex, whose only
    /// letters are `A`–`F` and never fold onto a lowercase safe char) keeps the
    /// map injective on BOTH case-sensitive and case-insensitive filesystems. The
    /// escape char `%` is itself encoded, so distinct inputs can never collide.
    pub fn encode_filename(&self) -> String {
        let mut out = String::with_capacity(self.0.len());
        for &byte in self.0.as_bytes() {
            // Safe set: lowercase letters, digits, and `. _ -`. Everything else —
            // including uppercase letters — is percent-encoded.
            if byte.is_ascii_lowercase()
                || byte.is_ascii_digit()
                || matches!(byte, b'.' | b'_' | b'-')
            {
                out.push(byte as char);
            } else {
                out.push('%');
                out.push_str(&format!("{byte:02X}"));
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::SessionId;

    #[test]
    fn encodes_lowercase_safe_session_ids_unchanged() {
        // A lowercase-only id with the allowed punctuation passes through verbatim.
        assert_eq!(
            SessionId::new("abc-123_def.456").encode_filename(),
            "abc-123_def.456"
        );
    }

    #[test]
    fn encodes_uppercase_letters_for_case_insensitive_filesystems() {
        // Uppercase letters MUST be encoded: on case-insensitive APFS `aB` and
        // `Ab` would otherwise share one file and alias each other.
        assert_eq!(SessionId::new("aB").encode_filename(), "a%42");
        assert_eq!(SessionId::new("Ab").encode_filename(), "%41b");
        assert_ne!(
            SessionId::new("aB").encode_filename().to_ascii_lowercase(),
            SessionId::new("Ab").encode_filename().to_ascii_lowercase()
        );
    }

    #[test]
    fn encodes_unsafe_bytes_and_stays_collision_free() {
        // A slash cannot be allowed to create a subdirectory, and the escape char
        // itself must be encoded so distinct inputs never collide.
        assert_eq!(SessionId::new("a/b").encode_filename(), "a%2Fb");
        // The literal string "a%2Fb" must NOT encode to the same thing as "a/b".
        assert_eq!(SessionId::new("a%2Fb").encode_filename(), "a%252%46b");
        assert_ne!(
            SessionId::new("a/b").encode_filename(),
            SessionId::new("a%2Fb").encode_filename()
        );
    }

    #[test]
    fn session_serialises_transparently() {
        // A branded SessionId round-trips as its bare string (no envelope).
        let json = serde_json::to_string(&SessionId::new("plain-id")).unwrap();
        assert_eq!(json, "\"plain-id\"");
        let back: SessionId = serde_json::from_str(&json).unwrap();
        assert_eq!(back, SessionId::new("plain-id"));
    }
}
