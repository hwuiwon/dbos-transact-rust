//! Small shared helpers.

/// Current wall-clock time in epoch-milliseconds. All DB timestamps are stored
/// as `BIGINT` epoch-ms bound from the application (never DB `now()`), so this is
/// the single source of "now" across the engine.
pub(crate) fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

/// A fresh random UUID v4 string (workflow ids, owner xids, message ids).
pub(crate) fn new_uuid() -> String {
    uuid::Uuid::new_v4().to_string()
}
