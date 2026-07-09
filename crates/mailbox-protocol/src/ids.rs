//! Small newtypes over the primitives that flow through the protocol.
//!
//! These exist so an `Offset` can never be passed where an `EventId` is meant,
//! and so intent is visible at every call site (mikey-in-a-box: brand domain
//! primitives). They are `#[serde(transparent)]`, so on the wire they are just
//! the underlying scalar — no envelope, easy for non-Rust adapters to produce.

use serde::{Deserialize, Serialize};

/// Identity an adapter publishes under (e.g. `"github-watch"`).
///
/// This is provenance, not authority: the bridge treats it as a label, and
/// event bodies remain untrusted content regardless of who published them.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct AdapterId(pub String);

/// Stable, opaque identifier for a single event.
///
/// A string (not an integer) so the assigning side is free to use a ULID/UUID
/// without a protocol change; consumers must treat it as opaque.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct EventId(pub String);

/// Monotonic per-topic position of an event in the durable log.
///
/// This is the cursor coordinate: consumers remember the last `Offset` they saw
/// and ask for events strictly after it. Monotonicity/allocation is the
/// bridge's job; the protocol only carries the number.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Offset(pub u64);

/// Event creation time as Unix milliseconds (UTC).
///
/// A bare integer avoids pulling a date/time crate into this dependency-light
/// protocol layer and is unambiguous across languages; formatting is a
/// presentation concern for higher layers. The `i64` (signed) type is
/// intentional: it tolerates clock skew and supports signed duration math
/// without wrapping — not an oversight for a value that is almost always
/// positive.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Timestamp(pub i64);

/// Where a cursor-based read should start.
///
/// Modelled as a closed enum rather than an `Option<Offset>` so "from the
/// beginning" and "after offset N" are distinct, self-describing states that
/// cannot be confused (make invalid states unrepresentable).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "from", rename_all = "snake_case")]
pub enum Cursor {
    /// Read from the oldest retained event on the topic.
    Oldest,
    /// Read events strictly after this offset.
    After { offset: Offset },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_variants_round_trip() {
        for cursor in [Cursor::Oldest, Cursor::After { offset: Offset(7) }] {
            let json = serde_json::to_string(&cursor).unwrap();
            let back: Cursor = serde_json::from_str(&json).unwrap();
            assert_eq!(cursor, back);
        }
    }

    #[test]
    fn offset_orders_numerically() {
        // Cursor advancement relies on this ordering; encode the assumption.
        assert!(Offset(1) < Offset(2));
    }
}
