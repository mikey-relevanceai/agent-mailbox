//! Parsing the hook payload Claude Code delivers on stdin.
//!
//! Every hook receives its event as a single JSON object on stdin. We parse it
//! into a typed [`HookInput`] at the edge (parse, don't validate) so the rest of
//! the harness works with a guaranteed-present `session_id` rather than poking at
//! a `serde_json::Value`. Fields beyond the ones we use (`cwd`,
//! `transcript_path`, event-specific extras) are ignored — we deliberately do not
//! model the whole surface, only what the arm/cleanup logic needs.

use std::io::Read;

use mailbox_protocol::SessionId;
use serde::Deserialize;

/// Hard cap on the hook stdin payload we will buffer (1 MiB). A Claude Code hook
/// payload is a small JSON object; capping the read means a truncated pipe or an
/// adversarial unbounded stream is rejected cleanly instead of buffering without
/// limit (mirrors the daemon's frame cap).
const MAX_HOOK_PAYLOAD_BYTES: u64 = 1024 * 1024;

/// A hook could not be understood from its stdin payload.
#[derive(Debug, thiserror::Error)]
pub enum HookError {
    /// stdin was empty (or all whitespace) — almost always a human running a hook
    /// handler by hand from a shell, where stdin is an immediately-closed TTY.
    ///
    /// Kept apart from [`Self::Parse`] because the raw serde message for this case
    /// ("EOF while parsing a value at line 1 column 0") tells an operator nothing
    /// about what they did wrong. These commands are hook targets, not interactive
    /// commands, and the one time that matters most is manual recovery of a session
    /// whose registration lapsed — so the error names the incantation instead.
    #[error(
        "no hook payload on stdin. `mailbox harness <command>` is a Claude Code HOOK \
         handler, not an interactive command: it reads the hook's JSON (including \
         `session_id`) from stdin. To run one by hand — e.g. to re-register a session \
         whose inbox lapsed — pipe it a payload:\n    \
         echo '{{\"session_id\":\"<your-session-id>\"}}' | mailbox harness session-start\n\
         (`mailbox whoami` prints your session id.)"
    )]
    NoPayload,

    /// The stdin bytes were not valid JSON, or lacked a `session_id`.
    #[error("could not parse hook stdin JSON: {0}")]
    Parse(#[from] serde_json::Error),

    /// A `session_id` was present but empty — the harness owns the label, and an
    /// empty one names no session, so we refuse it rather than arm a phantom.
    #[error("hook payload carried an empty session_id")]
    EmptySessionId,

    /// The payload exceeded [`MAX_HOOK_PAYLOAD_BYTES`] — a hook object is tiny, so
    /// this is a malformed/adversarial stream, not a real hook.
    #[error("hook payload exceeds the {MAX_HOOK_PAYLOAD_BYTES}-byte limit")]
    TooLarge,

    /// Reading the stdin bytes failed.
    #[error("could not read hook stdin: {0}")]
    Read(#[source] std::io::Error),
}

/// The slice of a Claude Code hook payload the harness needs.
///
/// `session_id` is required (a hook with no session cannot be armed or cleaned
/// up) and parses straight into a branded [`SessionId`] (parse, don't validate),
/// so it flows end-to-end without being re-minted. `hook_event_name` is captured
/// only for logging so a line can say which event fired.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct HookInput {
    /// The session this hook fired for — the identity used everywhere downstream
    /// (settled decision: identity comes from the hook, card 11).
    pub session_id: SessionId,
    /// The event name (`SessionStart`, `Stop`, `SessionEnd`, …), if provided.
    #[serde(default)]
    pub hook_event_name: Option<String>,
}

impl HookInput {
    /// Parse a hook payload from its raw JSON, rejecting an empty session id.
    pub fn parse(json: &str) -> Result<Self, HookError> {
        let input: HookInput = serde_json::from_str(json)?;
        if input.session_id.as_str().trim().is_empty() {
            return Err(HookError::EmptySessionId);
        }
        Ok(input)
    }

    /// Parse a hook payload by reading all of `reader` (stdin in production),
    /// bounded to [`MAX_HOOK_PAYLOAD_BYTES`].
    pub fn from_reader(reader: impl Read) -> Result<Self, HookError> {
        // `take(cap + 1)` so we can tell "exactly at the cap" from "over the cap".
        let mut limited = reader.take(MAX_HOOK_PAYLOAD_BYTES + 1);
        let mut buf = String::new();
        limited.read_to_string(&mut buf).map_err(HookError::Read)?;
        if buf.len() as u64 > MAX_HOOK_PAYLOAD_BYTES {
            return Err(HookError::TooLarge);
        }
        // Empty stdin is its own error: it means "run by hand", not "malformed JSON".
        if buf.trim().is_empty() {
            return Err(HookError::NoPayload);
        }
        Self::parse(&buf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_session_id_and_event_name() {
        let input = HookInput::parse(
            r#"{"session_id":"abc123","cwd":"/x","hook_event_name":"Stop","transcript_path":"/t"}"#,
        )
        .unwrap();
        assert_eq!(input.session_id.as_str(), "abc123");
        assert_eq!(input.hook_event_name.as_deref(), Some("Stop"));
    }

    #[test]
    fn tolerates_missing_optional_event_name() {
        let input = HookInput::parse(r#"{"session_id":"s1"}"#).unwrap();
        assert_eq!(input.session_id.as_str(), "s1");
        assert_eq!(input.hook_event_name, None);
    }

    #[test]
    fn rejects_missing_session_id() {
        let err = HookInput::parse(r#"{"cwd":"/x"}"#).unwrap_err();
        assert!(matches!(err, HookError::Parse(_)));
    }

    #[test]
    fn rejects_empty_session_id() {
        let err = HookInput::parse(r#"{"session_id":"   "}"#).unwrap_err();
        assert!(matches!(err, HookError::EmptySessionId));
    }

    /// Running a hook handler bare from a shell gives it an empty stdin. That must
    /// produce the actionable "this is a hook handler, pipe it a payload" error, not
    /// serde's "EOF while parsing a value", which told a real operator nothing while
    /// they were trying to hand-recover a session whose inbox had lapsed.
    #[test]
    fn empty_stdin_is_a_named_error_not_a_raw_parse_failure() {
        for empty in ["", "   ", "\n\t "] {
            let err = HookInput::from_reader(empty.as_bytes()).unwrap_err();
            assert!(matches!(err, HookError::NoPayload), "{empty:?} -> {err:?}");
            // The message must carry the way out, not just the diagnosis.
            let text = err.to_string();
            assert!(text.contains("harness session-start"), "{text}");
            assert!(text.contains("session_id"), "{text}");
        }
    }

    #[test]
    fn reads_from_a_reader() {
        let input = HookInput::from_reader(r#"{"session_id":"reader-s"}"#.as_bytes()).unwrap();
        assert_eq!(input.session_id.as_str(), "reader-s");
    }

    #[test]
    fn rejects_an_oversized_payload() {
        // A payload past the cap is refused rather than buffered unbounded.
        let huge = format!(
            r#"{{"session_id":"s","pad":"{}"}}"#,
            "x".repeat(2 * 1024 * 1024)
        );
        let err = HookInput::from_reader(huge.as_bytes()).unwrap_err();
        assert!(matches!(err, HookError::TooLarge));
    }
}
