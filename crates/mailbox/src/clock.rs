//! Wall-clock helper: the current time in Unix milliseconds.
//!
//! One place for the "stamp now" choice the bridge makes for interest last-seen
//! (card 08) and elsewhere, so the pre-epoch guard is not re-derived per call
//! site. A clock before the epoch is impossible on a sane host; if it somehow
//! happens we return 0 rather than panic — timestamps here are provenance and
//! sweep bookkeeping, not authority.

use std::time::{SystemTime, UNIX_EPOCH};

/// Now, in Unix milliseconds UTC.
pub fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}
