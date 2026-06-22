//! The system database — the durability layer.
//!
//! The [`SystemDatabase`] trait is the
//! object-safe surface every durable operation goes through (held as
//! `Arc<dyn SystemDatabase>` on the context). Phase 1 implements the SQLite
//! backend ([`sqlite::SqliteDb`]); Postgres is added later behind the same trait.

pub mod postgres;
pub mod retry;
pub mod sqlite;

use std::time::Duration;

use async_trait::async_trait;

use crate::error::DbosError;

/// The single dialect-portable migration set, shared by every backend. Both the
/// SQLite and Postgres runners apply these `(version, sql)` entries in order,
/// tracking the highest applied version in the `dbos_migrations` table. The SQL
/// is written to be accepted by both engines, so there is exactly one set of
/// migration files to maintain.
pub(crate) const MIGRATIONS: &[(i64, &str)] =
    &[(1, include_str!("../../migrations/0001_initial_schema.sql"))];

/// The execution state of a workflow. Serializes to the exact DB string.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WorkflowStatusType {
    /// Running or ready to run.
    Pending,
    /// Queued and waiting for execution.
    Enqueued,
    /// Delayed; transitions to `Enqueued` after the delay expires.
    Delayed,
    /// Completed successfully.
    Success,
    /// Completed with an error.
    Error,
    /// Cancelled (manually or via timeout).
    Cancelled,
    /// Exceeded maximum retry attempts (dead-letter).
    MaxRecoveryAttemptsExceeded,
}

impl WorkflowStatusType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "PENDING",
            Self::Enqueued => "ENQUEUED",
            Self::Delayed => "DELAYED",
            Self::Success => "SUCCESS",
            Self::Error => "ERROR",
            Self::Cancelled => "CANCELLED",
            Self::MaxRecoveryAttemptsExceeded => "MAX_RECOVERY_ATTEMPTS_EXCEEDED",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "PENDING" => Self::Pending,
            "ENQUEUED" => Self::Enqueued,
            "DELAYED" => Self::Delayed,
            "SUCCESS" => Self::Success,
            "ERROR" => Self::Error,
            "CANCELLED" => Self::Cancelled,
            "MAX_RECOVERY_ATTEMPTS_EXCEEDED" => Self::MaxRecoveryAttemptsExceeded,
            _ => return None,
        })
    }

    /// Terminal states never re-execute and never increment attempts.
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Success | Self::Error | Self::Cancelled)
    }
}

impl std::fmt::Display for WorkflowStatusType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl serde::Serialize for WorkflowStatusType {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(self.as_str())
    }
}

impl<'de> serde::Deserialize<'de> for WorkflowStatusType {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        Self::parse(&s)
            .ok_or_else(|| serde::de::Error::custom(format!("invalid workflow status {s:?}")))
    }
}

/// Comprehensive information about a workflow's current state.
///
/// On the list path, `output`/`input` are the raw encoded `*string` forms (the
/// caller decodes when the target type is known) and `error` is the raw stored
/// string. Timestamps are epoch-milliseconds.
#[derive(Debug, Clone, serde::Serialize)]
pub struct WorkflowStatus {
    pub id: String,
    pub status: WorkflowStatusType,
    pub name: String,
    pub authenticated_user: String,
    pub assumed_role: String,
    pub authenticated_roles: Vec<String>,
    pub output: Option<String>,
    pub error: Option<String>,
    pub executor_id: String,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    pub application_version: String,
    pub application_id: String,
    pub attempts: i64,
    pub queue_name: String,
    pub timeout_ms: i64,
    pub deadline_ms: Option<i64>,
    pub started_at_ms: Option<i64>,
    pub deduplication_id: String,
    pub input: Option<String>,
    pub priority: i64,
    pub queue_partition_key: String,
    pub forked_from: String,
    pub was_forked_from: bool,
    pub parent_workflow_id: String,
    pub completed_at_ms: Option<i64>,
    pub class_name: String,
    pub config_name: Option<String>,
    pub serialization: String,
    pub delay_until_ms: Option<i64>,
}

/// The desired column values for an `insert_workflow_status` upsert.
#[derive(Debug, Clone, Default)]
pub struct NewWorkflowStatus {
    pub id: String,
    pub status: Option<WorkflowStatusType>,
    pub name: String,
    pub class_name: String,
    pub config_name: Option<String>,
    pub application_version: String,
    pub application_id: String,
    pub executor_id: String,
    pub created_at_ms: i64,
    pub updated_at_ms: Option<i64>,
    pub deadline_ms: Option<i64>,
    pub timeout_ms: Option<i64>,
    pub input: Option<String>,
    pub queue_name: String,
    pub deduplication_id: String,
    pub priority: i64,
    pub authenticated_user: String,
    pub assumed_role: String,
    pub authenticated_roles: Vec<String>,
    pub queue_partition_key: String,
    pub parent_workflow_id: String,
    pub delay_until_ms: Option<i64>,
    pub serialization: String,
}

/// A parent→child mapping recorded atomically with a child's status insert.
#[derive(Debug, Clone)]
pub struct ChildRecord {
    pub parent_workflow_id: String,
    pub step_id: i64,
    pub step_name: String,
    pub child_workflow_id: String,
}

/// Input to [`SystemDatabase::insert_workflow_status`].
#[derive(Debug, Clone)]
pub struct InsertWorkflowStatusInput {
    pub status: NewWorkflowStatus,
    pub max_retries: i64,
    pub owner_xid: String,
    pub increment_attempts: bool,
    /// When set, record the parent→child step in the same transaction.
    pub record_child: Option<ChildRecord>,
}

/// Result of the status upsert (`RETURNING` columns).
#[derive(Debug, Clone)]
pub struct InsertWorkflowResult {
    pub attempts: i64,
    pub status: WorkflowStatusType,
    pub name: String,
    pub queue_name: Option<String>,
    pub queue_partition_key: Option<String>,
    pub timeout_ms: Option<i64>,
    pub deadline_ms: Option<i64>,
    pub owner_xid: String,
}

/// Input to [`SystemDatabase::record_operation_result`].
#[derive(Debug, Clone, Default)]
pub struct RecordOperationResultInput {
    pub workflow_id: String,
    pub step_id: i64,
    pub step_name: String,
    pub output: Option<String>,
    pub error: Option<String>,
    pub child_workflow_id: Option<String>,
    pub started_at_ms: i64,
    pub completed_at_ms: i64,
    pub serialization: String,
}

/// Information about one executed step (from `get_workflow_steps`).
#[derive(Debug, Clone, serde::Serialize)]
pub struct StepInfo {
    pub step_id: i64,
    pub step_name: String,
    /// Raw encoded output (decode with the row's serialization when needed).
    pub output: Option<String>,
    /// Raw stored error string, if the step failed.
    pub error: Option<String>,
    pub child_workflow_id: Option<String>,
    pub started_at_ms: Option<i64>,
    pub completed_at_ms: Option<i64>,
    pub serialization: String,
}

/// Input to [`SystemDatabase::fork_workflow`].
#[derive(Debug, Clone, Default)]
pub struct ForkInput {
    pub original_workflow_id: String,
    pub forked_workflow_id: Option<String>,
    pub start_step: i64,
    pub application_version: Option<String>,
    pub queue_name: Option<String>,
}

/// A previously recorded step result (memoization hit).
#[derive(Debug, Clone)]
pub struct RecordedResult {
    pub output: Option<String>,
    pub error: Option<String>,
    pub serialization: String,
}

/// Output of [`SystemDatabase::await_workflow_result`].
#[derive(Debug, Clone)]
pub struct AwaitResult {
    pub output: Option<String>,
    pub error: Option<String>,
    pub serialization: String,
}

/// Input to [`SystemDatabase::dequeue_workflows`].
#[derive(Debug, Clone, Default)]
pub struct DequeueInput {
    pub queue_name: String,
    pub global_concurrency: Option<i64>,
    pub worker_concurrency: Option<i64>,
    /// `(limit, period_seconds)`.
    pub rate_limit: Option<(i64, f64)>,
    pub max_tasks: i64,
    pub local_running_count: i64,
    pub application_version: String,
    pub executor_id: String,
}

/// A workflow claimed off a queue, ready to dispatch.
#[derive(Debug, Clone)]
pub struct DequeuedWorkflow {
    pub id: String,
    pub name: String,
    pub input: Option<String>,
    pub serialization: String,
    pub config_name: Option<String>,
}

/// A row of the `workflow_schedules` table — a DB-backed (dynamic) cron schedule.
///
/// `context` is stored as a JSON string (never NULL); `last_fired_at` is an
/// RFC3339 string or `None`.
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct ScheduleRow {
    /// UUID primary key.
    pub schedule_id: String,
    /// Unique human/logical name for the schedule.
    pub schedule_name: String,
    /// The registered workflow name (custom name or FQN) to fire.
    pub workflow_name: String,
    /// Optional cross-language class/namespace name.
    pub workflow_class_name: Option<String>,
    /// The raw cron spec (without any `CRON_TZ=` prefix).
    pub schedule: String,
    /// `ACTIVE` or `PAUSED`.
    pub status: String,
    /// The schedule's user context, stored as a JSON string (`"null"` if none).
    pub context: String,
    /// The last tick time recorded, as an RFC3339 string (advisory; used for backfill).
    pub last_fired_at: Option<String>,
    /// Whether to backfill missed ticks since `last_fired_at` on (re)install.
    pub automatic_backfill: bool,
    /// Optional IANA timezone name for the cron spec.
    pub cron_timezone: Option<String>,
    /// Queue to enqueue ticks on (defaults to the internal queue when `None`).
    pub queue_name: Option<String>,
}

/// Filters for [`SystemDatabase::list_workflows`] (Phase 1 subset).
#[derive(Debug, Clone, Default)]
pub struct ListWorkflowsInput {
    pub workflow_ids: Vec<String>,
    pub workflow_name: Vec<String>,
    pub status: Vec<WorkflowStatusType>,
    pub executor_ids: Vec<String>,
    pub application_version: Vec<String>,
    pub queues_only: bool,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
    pub sort_desc: bool,
    pub load_input: bool,
    pub load_output: bool,
}

/// The object-safe durable-operations surface.
#[async_trait]
pub trait SystemDatabase: Send + Sync {
    /// Apply schema migrations (idempotent).
    async fn run_migrations(&self) -> Result<(), DbosError>;

    /// Upsert a workflow status row, handling attempt counting, dead-letter
    /// parking, dedup, and name/queue conflict detection.
    async fn insert_workflow_status(
        &self,
        input: InsertWorkflowStatusInput,
    ) -> Result<InsertWorkflowResult, DbosError>;

    /// Write a workflow's terminal outcome (forbids `CANCELLED → SUCCESS/ERROR`).
    async fn update_workflow_outcome(
        &self,
        workflow_id: &str,
        status: WorkflowStatusType,
        output: &Option<String>,
        err_str: &str,
    ) -> Result<(), DbosError>;

    /// Record a step's result; a duplicate `(workflow, function_id)` insert maps
    /// to [`DbosError::workflow_conflict_id`].
    async fn record_operation_result(
        &self,
        input: RecordOperationResultInput,
    ) -> Result<(), DbosError>;

    /// Return the recorded child workflow id for a parent step, if any.
    async fn check_child_workflow(
        &self,
        parent_workflow_id: &str,
        step_id: i64,
    ) -> Result<Option<String>, DbosError>;

    /// Memoization check: returns `Some` when the step is recorded, `None` when
    /// not yet run; errors if the workflow is cancelled or the function name
    /// mismatches a recorded step.
    async fn check_operation_execution(
        &self,
        workflow_id: &str,
        step_id: i64,
        step_name: &str,
    ) -> Result<Option<RecordedResult>, DbosError>;

    /// List workflows matching the filters.
    async fn list_workflows(
        &self,
        input: ListWorkflowsInput,
    ) -> Result<Vec<WorkflowStatus>, DbosError>;

    /// Fetch a single workflow's status, or `None` if it does not exist.
    async fn get_workflow_status(
        &self,
        workflow_id: &str,
    ) -> Result<Option<WorkflowStatus>, DbosError>;

    /// Poll until the workflow reaches a terminal state, returning its outcome.
    async fn await_workflow_result(
        &self,
        workflow_id: &str,
        poll_interval: Duration,
    ) -> Result<AwaitResult, DbosError>;

    /// Reset a queued workflow from `PENDING` back to `ENQUEUED` for re-dispatch.
    /// Returns whether a row was updated.
    async fn clear_queue_assignment(&self, workflow_id: &str) -> Result<bool, DbosError>;

    /// Atomically claim up to `max_tasks` eligible workflows off a queue
    /// (`ENQUEUED → PENDING`), respecting concurrency and rate limits.
    async fn dequeue_workflows(
        &self,
        input: DequeueInput,
    ) -> Result<Vec<DequeuedWorkflow>, DbosError>;

    /// Promote `DELAYED` workflows whose delay has elapsed to `ENQUEUED`.
    async fn transition_delayed_workflows(&self) -> Result<(), DbosError>;

    /// The id of the workflow currently holding the dedup slot for
    /// `(queue_name, deduplication_id)`, or `None` if the slot is free.
    async fn get_deduplicated_workflow(
        &self,
        queue_name: &str,
        deduplication_id: &str,
    ) -> Result<Option<String>, DbosError>;

    /// Cancel a workflow (unless already terminal). Returns whether the row exists.
    async fn cancel_workflow(&self, workflow_id: &str) -> Result<bool, DbosError>;

    /// Resume a cancelled/failed workflow: reset to `ENQUEUED` on the internal
    /// queue, clearing attempts/deadline/dedup/started/completed.
    async fn resume_workflow(&self, workflow_id: &str) -> Result<(), DbosError>;

    /// Delete a workflow row (cascades to its steps/notifications/events/streams).
    async fn delete_workflow(&self, workflow_id: &str) -> Result<(), DbosError>;

    /// Direct child workflow ids of a parent.
    async fn get_workflow_children(&self, workflow_id: &str) -> Result<Vec<String>, DbosError>;

    /// List a workflow's recorded steps in order.
    async fn get_workflow_steps(&self, workflow_id: &str) -> Result<Vec<StepInfo>, DbosError>;

    /// Fork a workflow from `start_step`, copying earlier steps; returns the new id.
    async fn fork_workflow(&self, input: ForkInput) -> Result<String, DbosError>;

    /// Aggregate workflow counts grouped by status.
    async fn get_workflow_status_counts(&self) -> Result<Vec<(String, i64)>, DbosError>;

    /// Garbage-collect terminal workflows older than `cutoff_epoch_ms` (and/or
    /// keeping only the newest `rows_threshold`). Returns the number removed.
    async fn garbage_collect(
        &self,
        cutoff_epoch_ms: Option<i64>,
        rows_threshold: Option<i64>,
    ) -> Result<u64, DbosError>;

    /// Test/recovery hook: force a workflow back to `PENDING` (simulate a crash).
    async fn set_workflow_status_pending(&self, workflow_id: &str) -> Result<(), DbosError>;

    // --- notifications & events ---

    /// Insert a notification for `dest`/`topic`. A missing destination workflow
    /// (foreign-key violation) maps to [`DbosError::non_existent_workflow`].
    async fn send_notification(
        &self,
        dest: &str,
        topic: &str,
        message: &Option<String>,
        serialization: &str,
    ) -> Result<(), DbosError>;

    /// Whether an unconsumed notification exists for `dest`/`topic`.
    async fn has_unconsumed_notification(&self, dest: &str, topic: &str)
    -> Result<bool, DbosError>;

    /// Atomically consume the oldest unconsumed notification, returning its
    /// (message, serialization) if any.
    async fn consume_oldest_notification(
        &self,
        dest: &str,
        topic: &str,
    ) -> Result<Option<(Option<String>, String)>, DbosError>;

    /// Upsert a workflow event (and its step-indexed history row).
    async fn set_event(
        &self,
        workflow_id: &str,
        function_id: i64,
        key: &str,
        value: &Option<String>,
        serialization: &str,
    ) -> Result<(), DbosError>;

    /// Read a workflow event's (value, serialization) if set.
    async fn get_event(
        &self,
        target_workflow_id: &str,
        key: &str,
    ) -> Result<Option<(Option<String>, String)>, DbosError>;

    /// All `(key, value, serialization)` rows of a workflow's events (admin read).
    async fn get_workflow_events(
        &self,
        workflow_id: &str,
    ) -> Result<Vec<(String, Option<String>, String)>, DbosError>;

    /// All notifications addressed to a workflow as
    /// `(topic, message, created_at_epoch_ms, serialization, consumed)` (admin read).
    async fn get_workflow_notifications(
        &self,
        workflow_id: &str,
    ) -> Result<Vec<(String, Option<String>, i64, String, bool)>, DbosError>;

    /// All stream entries for a workflow as `(key, offset, value, serialization)`,
    /// ordered by `(key, offset)` (admin read).
    async fn get_workflow_streams(
        &self,
        workflow_id: &str,
    ) -> Result<Vec<(String, i64, String, String)>, DbosError>;

    /// Step-execution counts grouped by function name (admin read):
    /// `(function_name, count)`.
    async fn get_step_aggregates(&self) -> Result<Vec<(String, i64)>, DbosError>;

    /// All registered application versions as
    /// `(version_id, version_name, version_timestamp, created_at)`, newest first.
    async fn list_application_versions(&self)
    -> Result<Vec<(String, String, i64, i64)>, DbosError>;

    /// Bump an application version's timestamp to "now", making it the latest.
    async fn set_latest_application_version(&self, version_name: &str) -> Result<(), DbosError>;

    /// Append a value to a stream (errors if the stream is already closed).
    async fn write_stream(
        &self,
        workflow_id: &str,
        key: &str,
        value: &str,
        function_id: i64,
        serialization: &str,
    ) -> Result<(), DbosError>;

    /// Read a stream from `from_offset`, returning `(value, offset, serialization)`
    /// entries and whether the stream is closed.
    async fn read_stream(
        &self,
        workflow_id: &str,
        key: &str,
        from_offset: i64,
    ) -> Result<(Vec<(String, i64, String)>, bool), DbosError>;

    /// Record an application version (idempotent on `version_name`).
    async fn create_application_version(&self, version_name: &str) -> Result<(), DbosError>;

    /// The most recently registered application version name, if any.
    async fn get_latest_application_version(&self) -> Result<Option<String>, DbosError>;

    // --- dynamic (DB-backed) schedules ---

    /// Insert a new schedule. A duplicate `schedule_name` is an error
    /// ([`DbosError::queue_deduplicated`]-style conflict mapped through the
    /// backend's unique-violation handling).
    async fn create_schedule(&self, row: ScheduleRow) -> Result<(), DbosError>;

    /// List all schedules (no filter).
    async fn list_schedules(&self) -> Result<Vec<ScheduleRow>, DbosError>;

    /// Fetch a single schedule by exact name, or `None`.
    async fn get_schedule(&self, name: &str) -> Result<Option<ScheduleRow>, DbosError>;

    /// Set a schedule's status (`ACTIVE`/`PAUSED`). This ALSO resets
    /// `last_fired_at` to NULL (pausing/resuming disables the next automatic
    /// backfill).
    async fn set_schedule_status(&self, name: &str, status: &str) -> Result<(), DbosError>;

    /// Update a schedule's `last_fired_at` (RFC3339 string). Does not touch status.
    async fn update_schedule_last_fired(
        &self,
        name: &str,
        last_fired_at: &str,
    ) -> Result<(), DbosError>;

    /// Delete a schedule by name.
    async fn delete_schedule(&self, name: &str) -> Result<(), DbosError>;

    /// Close the underlying pool(s).
    async fn close(&self);
}
