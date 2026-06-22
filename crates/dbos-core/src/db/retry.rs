//! Transient-error retry decorator for any [`SystemDatabase`].
//!
//! Applied to every system-DB call: a
//! dropped/unavailable database connection is retried with capped exponential
//! backoff, so workflows survive a flapping database (the basis of the chaos
//! tests). Non-transient errors (unique violations, dead-letter, etc.) pass
//! through immediately.

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;

use crate::db::{
    AwaitResult, DequeueInput, DequeuedWorkflow, ForkInput, InsertWorkflowResult,
    InsertWorkflowStatusInput, ListWorkflowsInput, RecordOperationResultInput, RecordedResult,
    ScheduleRow, StepInfo, SystemDatabase, WorkflowStatus, WorkflowStatusType,
};
use crate::error::DbosError;

/// Wraps a `SystemDatabase`, retrying transient connection failures.
pub struct RetryingDb {
    inner: Arc<dyn SystemDatabase>,
    /// Maximum total time to keep retrying a single operation before giving up.
    max_elapsed: Duration,
}

impl RetryingDb {
    /// Wrap an inner database with transient-error retry.
    pub fn wrap(inner: Arc<dyn SystemDatabase>) -> Arc<dyn SystemDatabase> {
        Arc::new(Self {
            inner,
            max_elapsed: Duration::from_secs(120),
        })
    }
}

/// Whether a `DbosError` was caused by a transient database failure (a dropped
/// connection, an unavailable/restarting server, TLS/protocol hiccup during
/// reconnect, etc.). Logical errors (decode/row-not-found/config and non-
/// connection SQLSTATEs such as unique violations) are NOT transient.
fn is_transient(e: &DbosError) -> bool {
    let mut src: Option<&(dyn std::error::Error + 'static)> = Some(e);
    while let Some(s) = src {
        if let Some(sqlx_err) = s.downcast_ref::<sqlx::Error>() {
            return match sqlx_err {
                // A real SQL error: transient only for connection-class SQLSTATEs
                // (08xxx connection exceptions, 57Pxx admin shutdown).
                sqlx::Error::Database(db) => db
                    .code()
                    .map(|c| c.starts_with("08") || c.starts_with("57P"))
                    .unwrap_or(false),
                // Clearly-logical errors never retry.
                sqlx::Error::RowNotFound
                | sqlx::Error::TypeNotFound { .. }
                | sqlx::Error::ColumnIndexOutOfBounds { .. }
                | sqlx::Error::ColumnNotFound(_)
                | sqlx::Error::ColumnDecode { .. }
                | sqlx::Error::Decode(_)
                | sqlx::Error::Configuration(_) => false,
                // Everything else (Io, Tls, Protocol, PoolTimedOut, WorkerCrashed,
                // and any future transport variant) is treated as transient.
                _ => true,
            };
        }
        src = s.source();
    }
    false
}

/// Run `f`, retrying while it returns a transient error (capped backoff).
async fn retry<T, F, Fut>(max_elapsed: Duration, f: F) -> Result<T, DbosError>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<T, DbosError>>,
{
    let start = Instant::now();
    let mut delay = Duration::from_millis(50);
    loop {
        match f().await {
            Ok(v) => return Ok(v),
            Err(e) if is_transient(&e) && start.elapsed() < max_elapsed => {
                tracing::warn!(error = %e, "transient database error; retrying");
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(Duration::from_secs(2));
            }
            Err(e) => return Err(e),
        }
    }
}

#[async_trait]
impl SystemDatabase for RetryingDb {
    async fn run_migrations(&self) -> Result<(), DbosError> {
        retry(self.max_elapsed, || self.inner.run_migrations()).await
    }
    async fn insert_workflow_status(
        &self,
        input: InsertWorkflowStatusInput,
    ) -> Result<InsertWorkflowResult, DbosError> {
        retry(self.max_elapsed, || {
            self.inner.insert_workflow_status(input.clone())
        })
        .await
    }
    async fn update_workflow_outcome(
        &self,
        workflow_id: &str,
        status: WorkflowStatusType,
        output: &Option<String>,
        err_str: &str,
    ) -> Result<(), DbosError> {
        retry(self.max_elapsed, || {
            self.inner
                .update_workflow_outcome(workflow_id, status, output, err_str)
        })
        .await
    }
    async fn record_operation_result(
        &self,
        input: RecordOperationResultInput,
    ) -> Result<(), DbosError> {
        retry(self.max_elapsed, || {
            self.inner.record_operation_result(input.clone())
        })
        .await
    }
    async fn check_child_workflow(
        &self,
        parent_workflow_id: &str,
        step_id: i64,
    ) -> Result<Option<String>, DbosError> {
        retry(self.max_elapsed, || {
            self.inner.check_child_workflow(parent_workflow_id, step_id)
        })
        .await
    }
    async fn check_operation_execution(
        &self,
        workflow_id: &str,
        step_id: i64,
        step_name: &str,
    ) -> Result<Option<RecordedResult>, DbosError> {
        retry(self.max_elapsed, || {
            self.inner
                .check_operation_execution(workflow_id, step_id, step_name)
        })
        .await
    }
    async fn list_workflows(
        &self,
        input: ListWorkflowsInput,
    ) -> Result<Vec<WorkflowStatus>, DbosError> {
        retry(self.max_elapsed, || {
            self.inner.list_workflows(input.clone())
        })
        .await
    }
    async fn get_workflow_status(
        &self,
        workflow_id: &str,
    ) -> Result<Option<WorkflowStatus>, DbosError> {
        retry(self.max_elapsed, || {
            self.inner.get_workflow_status(workflow_id)
        })
        .await
    }
    async fn await_workflow_result(
        &self,
        workflow_id: &str,
        poll_interval: Duration,
    ) -> Result<AwaitResult, DbosError> {
        retry(self.max_elapsed, || {
            self.inner.await_workflow_result(workflow_id, poll_interval)
        })
        .await
    }
    async fn clear_queue_assignment(&self, workflow_id: &str) -> Result<bool, DbosError> {
        retry(self.max_elapsed, || {
            self.inner.clear_queue_assignment(workflow_id)
        })
        .await
    }
    async fn dequeue_workflows(
        &self,
        input: DequeueInput,
    ) -> Result<Vec<DequeuedWorkflow>, DbosError> {
        retry(self.max_elapsed, || {
            self.inner.dequeue_workflows(input.clone())
        })
        .await
    }
    async fn transition_delayed_workflows(&self) -> Result<(), DbosError> {
        retry(self.max_elapsed, || {
            self.inner.transition_delayed_workflows()
        })
        .await
    }
    async fn get_deduplicated_workflow(
        &self,
        queue_name: &str,
        deduplication_id: &str,
    ) -> Result<Option<String>, DbosError> {
        retry(self.max_elapsed, || {
            self.inner
                .get_deduplicated_workflow(queue_name, deduplication_id)
        })
        .await
    }
    async fn cancel_workflow(&self, workflow_id: &str) -> Result<bool, DbosError> {
        retry(self.max_elapsed, || self.inner.cancel_workflow(workflow_id)).await
    }
    async fn resume_workflow(&self, workflow_id: &str) -> Result<(), DbosError> {
        retry(self.max_elapsed, || self.inner.resume_workflow(workflow_id)).await
    }
    async fn delete_workflow(&self, workflow_id: &str) -> Result<(), DbosError> {
        retry(self.max_elapsed, || self.inner.delete_workflow(workflow_id)).await
    }
    async fn get_workflow_children(&self, workflow_id: &str) -> Result<Vec<String>, DbosError> {
        retry(self.max_elapsed, || {
            self.inner.get_workflow_children(workflow_id)
        })
        .await
    }
    async fn get_workflow_steps(&self, workflow_id: &str) -> Result<Vec<StepInfo>, DbosError> {
        retry(self.max_elapsed, || {
            self.inner.get_workflow_steps(workflow_id)
        })
        .await
    }
    async fn fork_workflow(&self, input: ForkInput) -> Result<String, DbosError> {
        retry(self.max_elapsed, || self.inner.fork_workflow(input.clone())).await
    }
    async fn get_workflow_status_counts(&self) -> Result<Vec<(String, i64)>, DbosError> {
        retry(self.max_elapsed, || self.inner.get_workflow_status_counts()).await
    }
    async fn garbage_collect(
        &self,
        cutoff_epoch_ms: Option<i64>,
        rows_threshold: Option<i64>,
    ) -> Result<u64, DbosError> {
        retry(self.max_elapsed, || {
            self.inner.garbage_collect(cutoff_epoch_ms, rows_threshold)
        })
        .await
    }
    async fn set_workflow_status_pending(&self, workflow_id: &str) -> Result<(), DbosError> {
        retry(self.max_elapsed, || {
            self.inner.set_workflow_status_pending(workflow_id)
        })
        .await
    }
    async fn send_notification(
        &self,
        dest: &str,
        topic: &str,
        message: &Option<String>,
        serialization: &str,
    ) -> Result<(), DbosError> {
        retry(self.max_elapsed, || {
            self.inner
                .send_notification(dest, topic, message, serialization)
        })
        .await
    }
    async fn has_unconsumed_notification(
        &self,
        dest: &str,
        topic: &str,
    ) -> Result<bool, DbosError> {
        retry(self.max_elapsed, || {
            self.inner.has_unconsumed_notification(dest, topic)
        })
        .await
    }
    async fn consume_oldest_notification(
        &self,
        dest: &str,
        topic: &str,
    ) -> Result<Option<(Option<String>, String)>, DbosError> {
        retry(self.max_elapsed, || {
            self.inner.consume_oldest_notification(dest, topic)
        })
        .await
    }
    async fn set_event(
        &self,
        workflow_id: &str,
        function_id: i64,
        key: &str,
        value: &Option<String>,
        serialization: &str,
    ) -> Result<(), DbosError> {
        retry(self.max_elapsed, || {
            self.inner
                .set_event(workflow_id, function_id, key, value, serialization)
        })
        .await
    }
    async fn get_event(
        &self,
        target_workflow_id: &str,
        key: &str,
    ) -> Result<Option<(Option<String>, String)>, DbosError> {
        retry(self.max_elapsed, || {
            self.inner.get_event(target_workflow_id, key)
        })
        .await
    }
    async fn write_stream(
        &self,
        workflow_id: &str,
        key: &str,
        value: &str,
        function_id: i64,
        serialization: &str,
    ) -> Result<(), DbosError> {
        retry(self.max_elapsed, || {
            self.inner
                .write_stream(workflow_id, key, value, function_id, serialization)
        })
        .await
    }
    async fn read_stream(
        &self,
        workflow_id: &str,
        key: &str,
        from_offset: i64,
    ) -> Result<(Vec<(String, i64, String)>, bool), DbosError> {
        retry(self.max_elapsed, || {
            self.inner.read_stream(workflow_id, key, from_offset)
        })
        .await
    }
    async fn create_application_version(&self, version_name: &str) -> Result<(), DbosError> {
        retry(self.max_elapsed, || {
            self.inner.create_application_version(version_name)
        })
        .await
    }
    async fn get_latest_application_version(&self) -> Result<Option<String>, DbosError> {
        retry(self.max_elapsed, || {
            self.inner.get_latest_application_version()
        })
        .await
    }
    async fn get_workflow_events(
        &self,
        workflow_id: &str,
    ) -> Result<Vec<(String, Option<String>, String)>, DbosError> {
        retry(self.max_elapsed, || {
            self.inner.get_workflow_events(workflow_id)
        })
        .await
    }
    async fn get_workflow_notifications(
        &self,
        workflow_id: &str,
    ) -> Result<Vec<(String, Option<String>, i64, String, bool)>, DbosError> {
        retry(self.max_elapsed, || {
            self.inner.get_workflow_notifications(workflow_id)
        })
        .await
    }
    async fn get_workflow_streams(
        &self,
        workflow_id: &str,
    ) -> Result<Vec<(String, i64, String, String)>, DbosError> {
        retry(self.max_elapsed, || {
            self.inner.get_workflow_streams(workflow_id)
        })
        .await
    }
    async fn get_step_aggregates(&self) -> Result<Vec<(String, i64)>, DbosError> {
        retry(self.max_elapsed, || self.inner.get_step_aggregates()).await
    }
    async fn list_application_versions(
        &self,
    ) -> Result<Vec<(String, String, i64, i64)>, DbosError> {
        retry(self.max_elapsed, || self.inner.list_application_versions()).await
    }
    async fn set_latest_application_version(&self, version_name: &str) -> Result<(), DbosError> {
        retry(self.max_elapsed, || {
            self.inner.set_latest_application_version(version_name)
        })
        .await
    }
    async fn create_schedule(&self, row: ScheduleRow) -> Result<(), DbosError> {
        retry(self.max_elapsed, || self.inner.create_schedule(row.clone())).await
    }
    async fn list_schedules(&self) -> Result<Vec<ScheduleRow>, DbosError> {
        retry(self.max_elapsed, || self.inner.list_schedules()).await
    }
    async fn get_schedule(&self, name: &str) -> Result<Option<ScheduleRow>, DbosError> {
        retry(self.max_elapsed, || self.inner.get_schedule(name)).await
    }
    async fn set_schedule_status(&self, name: &str, status: &str) -> Result<(), DbosError> {
        retry(self.max_elapsed, || {
            self.inner.set_schedule_status(name, status)
        })
        .await
    }
    async fn update_schedule_last_fired(
        &self,
        name: &str,
        last_fired_at: &str,
    ) -> Result<(), DbosError> {
        retry(self.max_elapsed, || {
            self.inner.update_schedule_last_fired(name, last_fired_at)
        })
        .await
    }
    async fn delete_schedule(&self, name: &str) -> Result<(), DbosError> {
        retry(self.max_elapsed, || self.inner.delete_schedule(name)).await
    }
    async fn close(&self) {
        self.inner.close().await;
    }
}
