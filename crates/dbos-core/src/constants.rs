//! Engine-wide constants.

use std::time::Duration;

/// Single-row table tracking the highest applied migration version.
pub(crate) const DBOS_MIGRATION_TABLE: &str = "dbos_migrations";

/// Default poll/retry interval for DB operations.
pub(crate) const DB_RETRY_INTERVAL: Duration = Duration::from_secs(1);

/// Default maximum workflow recovery attempts before dead-lettering.
pub const DEFAULT_MAX_RECOVERY_ATTEMPTS: i64 = 100;

/// Default step retry backoff base interval.
pub(crate) const DEFAULT_STEP_BASE_INTERVAL: Duration = Duration::from_millis(100);
/// Default step retry backoff cap.
pub(crate) const DEFAULT_STEP_MAX_INTERVAL: Duration = Duration::from_secs(5);
/// Default step retry exponential backoff factor.
pub(crate) const DEFAULT_STEP_BACKOFF_FACTOR: f64 = 2.0;

/// Name of the internal queue used for scheduled/internal workflows.
pub(crate) const DBOS_INTERNAL_QUEUE_NAME: &str = "_dbos_internal_queue";

/// Default topic used by send/recv when none is specified.
pub(crate) const DBOS_NULL_TOPIC: &str = "__null__topic__";

/// Sentinel value written to a stream to mark it closed.
pub(crate) const DBOS_STREAM_CLOSED_SENTINEL: &str = "__DBOS_STREAM_CLOSED__";

/// Default executor id when none is configured.
pub(crate) const DEFAULT_EXECUTOR_ID: &str = "local";

/// Default system-database schema (Postgres only; ignored for SQLite).
pub(crate) const DEFAULT_DATABASE_SCHEMA: &str = "dbos";
