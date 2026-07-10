//! The socket client: how every mutating/reading CLI command reaches the daemon.
//!
//! A client connects to the user-scoped Unix socket, sends exactly one
//! [`Request`], reads exactly one [`Response`], and disconnects (ADR-0004). It
//! never opens the database — that would create the cross-process second-writer
//! hazard the single-writer rule (ADR-0003) exists to prevent.
//!
//! # Fail loud when the bridge is down
//!
//! If the socket cannot be connected — no daemon has bound it — this returns
//! [`ClientError::BridgeDown`], whose message tells the operator exactly what to
//! do (`start it with mailbox serve`). We deliberately do NOT auto-spawn a daemon
//! and do NOT fall back to opening the DB directly: a predictable "bridge down"
//! error beats a spawn race or a silent ad-hoc writer (ADR-0003 consequence:
//! "design lifecycle so bridge-down is obvious"). Auto-spawn is a later
//! enhancement, not MVP behaviour.

use std::path::{Path, PathBuf};

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

use crate::control::{ControlError, Request, Response, decode_frame, encode_frame};

/// Why a one-shot control request could not complete.
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    /// No daemon is listening on the socket. The message is intentionally
    /// actionable — it is what a user or agent sees on stderr.
    #[error("bridge not running; start it with `mailbox serve` (no daemon listening at {socket})")]
    BridgeDown { socket: PathBuf },

    /// The daemon closed the connection before sending a reply line.
    #[error("bridge closed the connection without replying (is `mailbox serve` healthy?)")]
    NoReply,

    /// An I/O error talking to the socket after connecting.
    #[error("i/o error talking to the bridge socket {socket}: {source}")]
    Io {
        socket: PathBuf,
        #[source]
        source: std::io::Error,
    },

    /// A framing/version error on the reply.
    #[error("could not decode the bridge reply: {0}")]
    Protocol(#[from] ControlError),
}

/// Connect, send one `request`, and return the daemon's one `response`.
///
/// A returned [`Response::Error`] is a *serviced* request that failed on the
/// daemon side — distinct from [`ClientError::BridgeDown`], which means no daemon
/// was reachable at all. Callers surface the two differently.
pub async fn send(socket: &Path, request: &Request) -> Result<Response, ClientError> {
    // Connect. NotFound (no socket node) and ConnectionRefused (stale socket, no
    // listener) both mean "no live bridge" — the fail-loud case.
    let stream = match UnixStream::connect(socket).await {
        Ok(stream) => stream,
        Err(err)
            if matches!(
                err.kind(),
                std::io::ErrorKind::NotFound | std::io::ErrorKind::ConnectionRefused
            ) =>
        {
            return Err(ClientError::BridgeDown {
                socket: socket.to_path_buf(),
            });
        }
        Err(source) => {
            return Err(ClientError::Io {
                socket: socket.to_path_buf(),
                source,
            });
        }
    };

    let io = |source| ClientError::Io {
        socket: socket.to_path_buf(),
        source,
    };

    // One request line out. We split so we can shut down the write half after
    // sending: that signals EOF to the daemon (it reads exactly one line) without
    // closing the read half we still need for the reply.
    let (read_half, mut write_half) = stream.into_split();
    let line = encode_frame(request)?;
    write_half.write_all(line.as_bytes()).await.map_err(io)?;
    write_half.write_all(b"\n").await.map_err(io)?;
    write_half.shutdown().await.map_err(io)?;

    // One reply line in.
    let mut reader = BufReader::new(read_half);
    let mut reply = String::new();
    let n = reader.read_line(&mut reply).await.map_err(io)?;
    if n == 0 {
        return Err(ClientError::NoReply);
    }
    Ok(decode_frame(&reply)?)
}
