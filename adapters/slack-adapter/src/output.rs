//! What the adapter writes to the bridge: NDJSON protocol lines on stdout.

use mailbox_protocol::{Message, encode_line};
use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::error::AdapterError;

/// Self-reported provenance. The host stamps its own spawn identity instead.
pub const ADAPTER_ID: &str = "slack-adapter";

/// One NDJSON line, flushed: stdout is block-buffered when piped, and the host
/// must see each line promptly and in order.
pub async fn write_message<W: AsyncWrite + Unpin>(
    out: &mut W,
    message: &Message,
) -> Result<(), AdapterError> {
    let line = encode_line(message)?;
    out.write_all(line.as_bytes())
        .await
        .map_err(AdapterError::Write)?;
    out.write_all(b"\n").await.map_err(AdapterError::Write)?;
    out.flush().await.map_err(AdapterError::Write)?;
    Ok(())
}
