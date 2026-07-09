//! Storage error type.
//!
//! Every fallible storage operation returns `Result<_, StorageError>`; a DB
//! failure is a value, never a panic (AGENTS.md / type-driven design). The
//! variants are deliberately coarse — callers mostly care about "the writer is
//! gone" vs "the data is bad" vs "SQLite said no" — but each keeps its source
//! for logs.

use std::path::PathBuf;

/// Something went wrong talking to the durable store.
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    /// The database directory could not be created or the file could not be
    /// opened. Carries the path so an operator can see *which* file failed.
    #[error("could not open storage at {path}: {source}")]
    Open {
        path: PathBuf,
        #[source]
        source: rusqlite::Error,
    },

    /// Creating the database directory failed.
    #[error("could not create storage directory {path}: {source}")]
    CreateDir {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    /// The default DB path could not be resolved because no home directory is
    /// known and no explicit override was given.
    #[error("could not determine a storage path: set AGENT_MAILBOX_DB or HOME/AGENT_MAILBOX_HOME")]
    NoStoragePath,

    /// The on-disk schema is newer than this build understands. Refusing to
    /// touch it (a forward frame may rely on tables/columns we do not have) is
    /// safer than best-effort corruption — mirrors the protocol reject-newer rule.
    #[error("database schema version {found} is newer than supported version {supported}")]
    UnsupportedSchemaVersion { found: u32, supported: u32 },

    /// A `PRAGMA integrity_check` reported the database is not consistent, or a
    /// stored row could not be reconstructed into its domain model.
    #[error("database failed integrity check: {detail}")]
    Corrupt { detail: String },

    /// A wire `Offset` (u64) exceeded `i64::MAX` and so cannot be stored. Caught
    /// on write (e.g. `advance_cursor`) rather than silently wrapping negative —
    /// a negative offset would make `WHERE offset > ?` match the entire log and
    /// replay everything to a consumer.
    #[error("offset {offset} is out of the representable range (max {})", i64::MAX)]
    OffsetOutOfRange { offset: u64 },

    /// A SQLite call failed. This is where a genuine `SQLITE_BUSY` *would*
    /// surface if the single-writer invariant were ever broken — under the
    /// current design (one connection, one writer thread) it should not.
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),

    /// A body/baseline JSON value could not be (de)serialized to/from its
    /// stored text form.
    #[error("could not (de)serialize stored JSON: {0}")]
    Json(#[from] serde_json::Error),

    /// The dedicated writer thread could not be spawned.
    #[error("could not start storage writer thread: {0}")]
    WriterSpawn(#[source] std::io::Error),

    /// The writer thread is no longer running (it panicked, or the store was
    /// shut down) so the request could not be serviced. Callers should treat
    /// this as "bridge down" and fail loudly rather than spawn a second writer.
    #[error("storage writer is not running")]
    WriterGone,
}
