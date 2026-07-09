//! Local (Rust-side) error types.
//!
//! These are the errors a Rust caller handles via `Result` — distinct from the
//! on-wire [`crate::ProtocolError`] message, which is an error *payload* one peer
//! sends to another. Business errors live here in `Result`, never in panics
//! (per AGENTS.md / mikey-in-a-box type-driven design).

use crate::PROTOCOL_VERSION;

/// Why a candidate string is not a valid [`crate::Topic`] or GitHub-PR topic.
///
/// Kept as an exhaustive enum so callers can match on the specific failure and
/// so adding a new rule forces every match site to acknowledge it.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TopicError {
    #[error("topic must not be empty")]
    Empty,

    #[error("topic exceeds maximum length of {max} bytes (got {len})")]
    TooLong { len: usize, max: usize },

    // Topics travel one-per-line over NDJSON and are used as map keys / CLI
    // args, so control characters and whitespace are rejected at the edge
    // rather than defended against everywhere downstream.
    #[error("topic contains forbidden character {ch:?}")]
    ForbiddenChar { ch: char },

    #[error("not a GitHub PR topic: expected `github.pr.<owner>/<repo>#<n>`")]
    NotGithubPr,

    // The embedded `value` echoes adapter-authored topic data (owner/repo/PR
    // number) so the diagnostic points at the exact bad input. This is a
    // DELIBERATE, size-bounded echo of a topic segment — do NOT copy this
    // pattern for `Publish`/`Event` bodies, which are untrusted, potentially
    // large content that must never be echoed (ADR-0001).
    #[error("GitHub PR topic {field} segment is invalid: {value:?}")]
    InvalidSegment { field: &'static str, value: String },

    #[error("GitHub PR number is invalid: {value:?} (must be a positive integer)")]
    InvalidPrNumber { value: String },
}

/// A message declared a protocol version this build cannot safely read.
///
/// See the crate-level docs for the reject-newer compatibility rule.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error(
    "incompatible protocol version {found}: this build supports up to {}",
    PROTOCOL_VERSION
)]
pub struct IncompatibleVersion {
    /// The version stamped on the offending frame.
    pub found: u32,
}

/// Failures while framing/deframing a single NDJSON line.
#[derive(Debug, thiserror::Error)]
pub enum FramingError {
    #[error("i/o error on a protocol line: {0}")]
    Io(#[from] std::io::Error),

    // serde_json's Display is structural (line/col, "unknown variant ...",
    // "invalid type ...") — safe to surface and far more actionable than a
    // generic message. It does not echo full message bodies.
    #[error("could not deserialize a protocol line as JSON: {0}")]
    Json(#[from] serde_json::Error),

    #[error(transparent)]
    IncompatibleVersion(#[from] IncompatibleVersion),
}

/// A [`FramingError`] tagged with the 1-based physical line it occurred on.
///
/// `read_lines` counts every physical line — including skipped blank ones — so
/// the number matches what a human sees scrolling the adapter's raw output.
#[derive(Debug, thiserror::Error)]
#[error("line {line}: {source}")]
pub struct LineError {
    pub line: usize,
    pub source: FramingError,
}

/// Reject-newer compatibility check: accept our version and anything older,
/// refuse anything newer.
///
/// Rationale lives in the crate-level docs; the one-liner is that a frame from
/// the future may rely on fields or semantics we do not implement, so failing
/// loudly here beats silently mis-reading it.
pub fn check_version(found: u32) -> Result<(), IncompatibleVersion> {
    if found > PROTOCOL_VERSION {
        Err(IncompatibleVersion { found })
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_current_version() {
        assert!(check_version(PROTOCOL_VERSION).is_ok());
    }

    #[test]
    fn accepts_older_versions() {
        // Older peers are readable; this is the whole point of reject-newer.
        assert!(check_version(PROTOCOL_VERSION.saturating_sub(1)).is_ok());
        assert!(check_version(0).is_ok());
    }

    #[test]
    fn rejects_newer_version() {
        let err = check_version(PROTOCOL_VERSION + 1).unwrap_err();
        assert_eq!(err.found, PROTOCOL_VERSION + 1);
    }
}
