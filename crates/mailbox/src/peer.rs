//! Delivering a wake onto a Claude Code session's inbox socket (ADR-0020).
//!
//! # The whole mechanism
//!
//! Connect to the session's Unix socket, write one newline-terminated JSON frame,
//! close. If the session is idle, Claude Code starts a turn with it. That is the
//! entire wake path for a session that has a socket — no sentinel, no file watch,
//! no hook, no exit code.
//!
//! # The frame
//!
//! ```json
//! {"type":"user","message":{"role":"user","content":"mail on topic …"}}
//! ```
//!
//! This minimal form is the one Claude Code itself publishes in its startup debug
//! log as the way to inject a message. It is deliberately **not** the richer peer
//! envelope that `SendMessage` writes, because that envelope carries a `from-mode`
//! field asserting the sender's permission class — and we assert none.
//!
//! # Why claiming nothing is the right frame, not just the polite one
//!
//! Claude Code decides delivery from both sessions' permission classes. Asserting
//! `from-mode="bypass"` is believed (the field is self-reported and unverified), and
//! it is the only way to reach a `bypassPermissions` receiver whose operator has not
//! opted in. It is also what causes a **hold** against every ordinary
//! permission-prompting receiver. So the dishonest frame is also the less
//! deliverable one; see ADR-0020's matrix. We claim nothing, and a
//! `bypassPermissions` fleet opts in explicitly with
//! `mailbox harness install-inbound`.
//!
//! # Payload-free, still
//!
//! The socket *could* carry an event body; it must not. `content` is
//! [`crate::wake::reminder`] — topic names only, the same string the fallback path's
//! exit-2 hook writes to stderr. The body stays in the durable log until the agent's
//! `read`. This is the ADR-0001 invariant, preserved across a change of transport.
//!
//! # Why this lives in the daemon
//!
//! Claude Code annotates a message with the sender's verified pid when it can read
//! one, and on macOS it can only do that while the sending process is still running.
//! The `serve` daemon is long-lived, so its deliveries are attributable; a
//! spawn-and-exit CLI writer's are not.

use std::io::Write;
use std::os::unix::net::UnixStream;
use std::path::Path;

use serde_json::json;

/// A wake could not be delivered on the peer socket.
///
/// Every variant means the same thing to the caller — **fall back to the sentinel**
/// — but they are kept apart because they say different things to an operator
/// reading the log: a refused connect is a session that went away, while a write
/// failure is a session that is there and did not take it.
#[derive(Debug, thiserror::Error)]
pub enum PeerDeliveryError {
    /// The socket could not be connected: the session exited, or the socket was
    /// removed between reading the registry and delivering.
    #[error("could not connect to the session inbox socket at {path}: {source}")]
    Connect {
        /// The socket we tried.
        path: String,
        /// The underlying I/O failure.
        source: std::io::Error,
    },

    /// Connected, but the frame could not be written.
    #[error("could not write the wake frame to {path}: {source}")]
    Write {
        /// The socket we tried.
        path: String,
        /// The underlying I/O failure.
        source: std::io::Error,
    },
}

/// Build the wake frame for `content`.
///
/// Split out from [`deliver`] so the exact bytes on the wire are unit-testable
/// without a socket — this is a shape another process parses, so it is pinned by a
/// test rather than by inspection. `serde_json` does the escaping; a hand-rolled
/// format string here would be a quoting bug waiting for a topic with a `"` in it.
pub fn frame(content: &str) -> String {
    let value = json!({
        "type": "user",
        "message": { "role": "user", "content": content },
    });
    // Newline-terminated: the receiver reads line-delimited JSON.
    format!("{value}\n")
}

/// Deliver `content` to the session listening on `socket`.
///
/// Returns as soon as the frame is written. There is no acknowledgement to wait for
/// — Claude Code does not answer on this socket — so a success here means "the frame
/// was handed to the kernel", not "the model saw it". What happens next is the
/// receiver's inbound policy (deliver / hold / refuse), which is invisible from this
/// side by design.
pub fn deliver(socket: &Path, content: &str) -> Result<(), PeerDeliveryError> {
    let path = socket.display().to_string();

    let mut stream = UnixStream::connect(socket).map_err(|source| PeerDeliveryError::Connect {
        path: path.clone(),
        source,
    })?;

    let frame = frame(content);
    stream
        .write_all(frame.as_bytes())
        .and_then(|()| stream.flush())
        .map_err(|source| PeerDeliveryError::Write { path, source })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufRead, BufReader};
    use std::os::unix::net::UnixListener;
    use std::sync::mpsc;

    /// Accept exactly one connection and return the first line written to it.
    fn capture_one(socket: std::path::PathBuf) -> mpsc::Receiver<String> {
        let listener = UnixListener::bind(&socket).unwrap();
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            if let Ok((stream, _)) = listener.accept() {
                let mut line = String::new();
                let _ = BufReader::new(stream).read_line(&mut line);
                let _ = tx.send(line);
            }
        });
        rx
    }

    /// The exact bytes another process parses. Pinned because this is a wire format
    /// we do not own: the minimal frame Claude Code's own startup log documents.
    #[test]
    fn the_frame_is_the_minimal_documented_shape() {
        let wire = frame("mail on topic t.a");
        assert_eq!(
            wire,
            "{\"message\":{\"content\":\"mail on topic t.a\",\"role\":\"user\"},\"type\":\"user\"}\n"
        );
        assert!(
            wire.ends_with('\n'),
            "the receiver reads line-delimited JSON"
        );
    }

    /// We assert no permission class. A `from-mode` here would reach bypass
    /// receivers, and would be held by every prompting one — see ADR-0020.
    #[test]
    fn the_frame_asserts_no_permission_class_and_impersonates_no_one() {
        let wire = frame("mail on topic t.a");
        assert!(!wire.contains("from-mode"), "we claim no permission class");
        assert!(
            !wire.contains("cross-session-message"),
            "we do not forge the peer envelope"
        );
    }

    /// Topic names are attacker-adjacent input (they come from a repo slug), so the
    /// frame must be built by a JSON serialiser, not a format string.
    #[test]
    fn content_with_quotes_stays_valid_json() {
        let wire = frame(r#"mail on topic "weird" \ topic"#);
        let parsed: serde_json::Value = serde_json::from_str(wire.trim_end()).unwrap();
        assert_eq!(
            parsed["message"]["content"],
            r#"mail on topic "weird" \ topic"#
        );
    }

    #[test]
    fn delivers_the_frame_to_a_listening_socket() {
        let dir = tempfile::TempDir::new().unwrap();
        let socket = dir.path().join("inbox.sock");
        let rx = capture_one(socket.clone());

        deliver(&socket, "mail on topic t.a").unwrap();

        let line = rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("the listener should have received the frame");
        let parsed: serde_json::Value = serde_json::from_str(line.trim_end()).unwrap();
        assert_eq!(parsed["type"], "user");
        assert_eq!(parsed["message"]["content"], "mail on topic t.a");
    }

    /// A socket that is registered but gone must surface as an error the caller can
    /// fall back from — never a panic, and never a silent success that loses a wake.
    #[test]
    fn a_missing_socket_is_a_connect_error_not_a_panic() {
        let dir = tempfile::TempDir::new().unwrap();
        let err = deliver(&dir.path().join("nope.sock"), "mail on topic t.a").unwrap_err();
        assert!(matches!(err, PeerDeliveryError::Connect { .. }));
    }
}
