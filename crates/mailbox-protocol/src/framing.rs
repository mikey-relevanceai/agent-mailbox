//! NDJSON line framing for the stdin/stdout and CLI transports.
//!
//! The wire form is newline-delimited JSON: exactly one [`Message`] per line,
//! each line a self-contained JSON object. This is pure (de)serialization —
//! spawning processes and wiring up stdio is a *transport* concern that lives in
//! a later host boundary, not here (protocol vs transport, per ADR-0001).
//!
//! Each line carries a `version` field alongside the message. On decode the
//! version is checked *before* the body (reject-newer), so a frame from a newer
//! protocol fails with [`IncompatibleVersion`] rather than a confusing parse
//! error about an unknown message type.

use std::io::{BufRead, Write};

use serde::{Deserialize, Serialize};

use crate::PROTOCOL_VERSION;
use crate::error::{FramingError, LineError, check_version};
use crate::message::Message;

/// Borrowing frame used on encode, so we serialize the caller's message without
/// cloning it. The line is a single flat object like
/// `{"version":1,"type":"publish",...}`; on decode the `version` key sits
/// alongside the message tag and is ignored by [`Message`]'s deserializer once
/// [`VersionHeader`] has vetted it.
#[derive(Serialize)]
struct FrameRef<'a> {
    version: u32,
    #[serde(flatten)]
    message: &'a Message,
}

/// Just enough of a line to read its version before trusting the rest.
#[derive(Deserialize)]
struct VersionHeader {
    version: u32,
}

/// Encode a message as a single NDJSON line (no trailing newline).
///
/// Stamps the current [`PROTOCOL_VERSION`]; callers that write the line
/// themselves are responsible for the `\n` separator (or use [`write_line`]).
pub fn encode_line(message: &Message) -> Result<String, FramingError> {
    let frame = FrameRef {
        version: PROTOCOL_VERSION,
        message,
    };
    Ok(serde_json::to_string(&frame)?)
}

/// Decode a single NDJSON line into a message, enforcing reject-newer.
pub fn decode_line(line: &str) -> Result<Message, FramingError> {
    // Adapters may be authored on Windows; tolerate a trailing CR so a
    // `\r\n`-terminated line decodes identically to a `\n` one. `read_lines`
    // via `BufRead::lines` already strips CRLF, but `decode_line` is also called
    // directly on caller-split lines, so we defend at this boundary too.
    let line = line.strip_suffix('\r').unwrap_or(line);

    // Peek the version first so a future frame is reported as an incompatible
    // version, not as a malformed message.
    let header: VersionHeader = serde_json::from_str(line)?;
    check_version(header.version)?;

    // The `version` key is an extra field the internally-tagged `Message`
    // deserializer ignores, so no separate frame struct is needed.
    Ok(serde_json::from_str(line)?)
}

/// Write a message as one NDJSON line (message + `\n`) to `writer`.
pub fn write_line<W: Write>(writer: &mut W, message: &Message) -> Result<(), FramingError> {
    let line = encode_line(message)?;
    writer.write_all(line.as_bytes())?;
    writer.write_all(b"\n")?;
    Ok(())
}

/// Parse a stream of NDJSON lines into messages.
///
/// Blank lines are skipped (NDJSON tolerates them); every other line is decoded,
/// so a malformed or version-incompatible line surfaces as an `Err` item
/// without ending the stream. Each error carries its 1-based physical line
/// number — counting every line including skipped blanks — so it maps to what a
/// human sees in the raw output.
pub fn read_lines<R: BufRead>(reader: R) -> impl Iterator<Item = Result<Message, LineError>> {
    reader.lines().enumerate().filter_map(|(index, line)| {
        let line_number = index + 1;
        match line {
            Err(io) => Some(Err(LineError {
                line: line_number,
                source: FramingError::from(io),
            })),
            Ok(text) if text.trim().is_empty() => None,
            Ok(text) => Some(decode_line(&text).map_err(|source| LineError {
                line: line_number,
                source,
            })),
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::FramingError;
    use crate::ids::AdapterId;
    use crate::message::{Publish, Subscribe};
    use crate::topic::GithubPr;

    fn sample() -> Message {
        Message::Publish(Publish {
            topic: GithubPr::new("octocat", "hello-world", 42).unwrap().topic(),
            adapter: AdapterId("github-watch".to_string()),
            body: serde_json::json!({ "action": "synchronize" }),
        })
    }

    #[test]
    fn line_round_trips_and_is_single_line() {
        let line = encode_line(&sample()).unwrap();
        assert!(!line.contains('\n'), "an NDJSON frame must be one line");
        assert_eq!(decode_line(&line).unwrap(), sample());
    }

    #[test]
    fn encoded_line_carries_current_version() {
        let line = encode_line(&sample()).unwrap();
        let value: serde_json::Value = serde_json::from_str(&line).unwrap();
        assert_eq!(value["version"], serde_json::json!(PROTOCOL_VERSION));
        // Flattened, so the message tag sits alongside version, not nested.
        assert_eq!(value["type"], serde_json::json!("publish"));
    }

    #[test]
    fn decode_rejects_newer_version() {
        let future = format!(
            r#"{{"version":{},"type":"subscribe","topic":"a.b.c"}}"#,
            PROTOCOL_VERSION + 1
        );
        assert!(matches!(
            decode_line(&future),
            Err(FramingError::IncompatibleVersion(_))
        ));
    }

    #[test]
    fn decode_rejects_unknown_message_type() {
        let unknown = r#"{"version":1,"type":"not_a_real_type"}"#;
        assert!(matches!(decode_line(unknown), Err(FramingError::Json(_))));
    }

    #[test]
    fn decode_requires_version_key() {
        // Guards against a future `#[serde(default)]` silently defaulting the
        // version to 0 and accepting frames that never declared one.
        let no_version = r#"{"type":"subscribe","topic":"a.b.c"}"#;
        assert!(matches!(
            decode_line(no_version),
            Err(FramingError::Json(_))
        ));
    }

    #[test]
    fn decode_tolerates_trailing_carriage_return() {
        // A `\r\n`-authored line, split on `\n`, still carries the `\r`.
        let line = format!("{}\r", encode_line(&sample()).unwrap());
        assert_eq!(decode_line(&line).unwrap(), sample());
    }

    #[test]
    fn read_lines_parses_stream_and_skips_blanks() {
        let sub = Message::Subscribe(Subscribe {
            topic: GithubPr::new("o", "r", 1).unwrap().topic(),
        });
        let mut buf = Vec::new();
        write_line(&mut buf, &sample()).unwrap();
        buf.extend_from_slice(b"   \n"); // whitespace-only line, treated as blank
        write_line(&mut buf, &sub).unwrap();

        let parsed: Result<Vec<Message>, _> = read_lines(buf.as_slice()).collect();
        assert_eq!(parsed.unwrap(), vec![sample(), sub]);
    }

    #[test]
    fn read_lines_handles_crlf_terminated_lines() {
        let mut buf = Vec::new();
        buf.extend_from_slice(encode_line(&sample()).unwrap().as_bytes());
        buf.extend_from_slice(b"\r\n");

        let parsed: Result<Vec<Message>, _> = read_lines(buf.as_slice()).collect();
        assert_eq!(parsed.unwrap(), vec![sample()]);
    }

    #[test]
    fn read_lines_surfaces_bad_line_with_line_number() {
        let mut buf = Vec::new();
        write_line(&mut buf, &sample()).unwrap(); // line 1
        buf.extend_from_slice(b"\n"); // line 2, blank (skipped but counted)
        buf.extend_from_slice(b"not json\n"); // line 3

        let items: Vec<_> = read_lines(buf.as_slice()).collect();
        assert_eq!(items.len(), 2);
        assert!(items[0].is_ok());
        let err = items[1].as_ref().unwrap_err();
        assert_eq!(err.line, 3, "blank line 2 must still count");
        assert!(matches!(err.source, FramingError::Json(_)));
    }
}
