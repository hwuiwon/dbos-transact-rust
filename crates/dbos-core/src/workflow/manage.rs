//! Workflow management — cancel, resume, fork, delete, list, steps.
//!
//! These free functions operate from outside a workflow (used by the client and
//! admin server). Resume and fork return polling handles since they re-enqueue
//! the workflow for the runner to pick up.

use std::collections::VecDeque;
use std::sync::Arc;

use serde::de::DeserializeOwned;

use crate::context::DbosContext;
use crate::db::{ForkInput, ListWorkflowsInput, StepInfo, WorkflowStatus};
use crate::error::DbosError;
use crate::workflow::handle::{WorkflowHandle, boxed_polling};

/// Options for [`fork_workflow`].
#[derive(Debug, Clone, Default)]
pub struct ForkOptions {
    pub original_workflow_id: String,
    /// Steps `[0, start_step)` are copied into the fork.
    pub start_step: i64,
    pub application_version: Option<String>,
    pub queue: Option<String>,
    pub forked_workflow_id: Option<String>,
}

/// Cancel a workflow. Errors if the workflow does not exist.
pub async fn cancel_workflow(ctx: &Arc<DbosContext>, workflow_id: &str) -> Result<(), DbosError> {
    if !ctx.db.cancel_workflow(workflow_id).await? {
        return Err(DbosError::non_existent_workflow(workflow_id));
    }
    Ok(())
}

/// Resume a cancelled/failed workflow; returns a handle to await its result.
pub async fn resume_workflow<R: DeserializeOwned + Send + Sync + 'static>(
    ctx: &Arc<DbosContext>,
    workflow_id: &str,
) -> Result<Box<dyn WorkflowHandle<R>>, DbosError> {
    if ctx.db.get_workflow_status(workflow_id).await?.is_none() {
        return Err(DbosError::non_existent_workflow(workflow_id));
    }
    ctx.db.resume_workflow(workflow_id).await?;
    Ok(boxed_polling::<R>(ctx.clone(), workflow_id.to_string()))
}

/// Fork a workflow from a given step; returns a handle to the new workflow.
pub async fn fork_workflow<R: DeserializeOwned + Send + Sync + 'static>(
    ctx: &Arc<DbosContext>,
    opts: ForkOptions,
) -> Result<Box<dyn WorkflowHandle<R>>, DbosError> {
    let new_id = ctx
        .db
        .fork_workflow(ForkInput {
            original_workflow_id: opts.original_workflow_id,
            forked_workflow_id: opts.forked_workflow_id,
            start_step: opts.start_step,
            application_version: opts.application_version,
            queue_name: opts.queue,
        })
        .await?;
    Ok(boxed_polling::<R>(ctx.clone(), new_id))
}

/// Delete a workflow (and, optionally, all its descendants).
pub async fn delete_workflow(
    ctx: &Arc<DbosContext>,
    workflow_id: &str,
    delete_children: bool,
) -> Result<(), DbosError> {
    let mut to_delete = vec![workflow_id.to_string()];
    if delete_children {
        let mut queue: VecDeque<String> = VecDeque::from([workflow_id.to_string()]);
        while let Some(id) = queue.pop_front() {
            for child in ctx.db.get_workflow_children(&id).await? {
                to_delete.push(child.clone());
                queue.push_back(child);
            }
        }
    }
    for id in to_delete {
        ctx.db.delete_workflow(&id).await?;
    }
    Ok(())
}

/// List workflows matching the given filters.
pub async fn list_workflows(
    ctx: &Arc<DbosContext>,
    input: ListWorkflowsInput,
) -> Result<Vec<WorkflowStatus>, DbosError> {
    ctx.db.list_workflows(input).await
}

/// List a workflow's executed steps.
pub async fn get_workflow_steps(
    ctx: &Arc<DbosContext>,
    workflow_id: &str,
) -> Result<Vec<StepInfo>, DbosError> {
    ctx.db.get_workflow_steps(workflow_id).await
}

/// Get a handle to an existing workflow by id (polls the durable state).
pub fn retrieve_workflow<R: DeserializeOwned + Send + Sync + 'static>(
    ctx: &Arc<DbosContext>,
    workflow_id: &str,
) -> Box<dyn WorkflowHandle<R>> {
    boxed_polling::<R>(ctx.clone(), workflow_id.to_string())
}

/// Workflow counts grouped by status.
pub async fn get_workflow_status_counts(
    ctx: &Arc<DbosContext>,
) -> Result<Vec<(String, i64)>, DbosError> {
    ctx.db.get_workflow_status_counts().await
}

/// Garbage-collect terminal workflows older than `cutoff_epoch_ms` and/or beyond
/// the newest `rows_threshold`. Returns the number removed.
pub async fn garbage_collect_workflows(
    ctx: &Arc<DbosContext>,
    cutoff_epoch_ms: Option<i64>,
    rows_threshold: Option<i64>,
) -> Result<u64, DbosError> {
    ctx.db
        .garbage_collect(cutoff_epoch_ms, rows_threshold)
        .await
}

/// All of a workflow's recorded events as `(key, value, serialization)` rows.
pub async fn get_workflow_events(
    ctx: &Arc<DbosContext>,
    workflow_id: &str,
) -> Result<Vec<(String, Option<String>, String)>, DbosError> {
    ctx.db.get_workflow_events(workflow_id).await
}

/// All notifications addressed to a workflow as
/// `(topic, message, created_at_epoch_ms, serialization, consumed)`.
pub async fn get_workflow_notifications(
    ctx: &Arc<DbosContext>,
    workflow_id: &str,
) -> Result<Vec<(String, Option<String>, i64, String, bool)>, DbosError> {
    ctx.db.get_workflow_notifications(workflow_id).await
}

/// All stream entries for a workflow as `(key, offset, value, serialization)`,
/// ordered by `(key, offset)`.
pub async fn get_workflow_streams(
    ctx: &Arc<DbosContext>,
    workflow_id: &str,
) -> Result<Vec<(String, i64, String, String)>, DbosError> {
    ctx.db.get_workflow_streams(workflow_id).await
}

/// Step-execution counts grouped by function name: `(function_name, count)`.
pub async fn get_step_aggregates(ctx: &Arc<DbosContext>) -> Result<Vec<(String, i64)>, DbosError> {
    ctx.db.get_step_aggregates().await
}

/// All registered application versions as
/// `(version_id, version_name, version_timestamp, created_at)`, newest first.
pub async fn list_application_versions(
    ctx: &Arc<DbosContext>,
) -> Result<Vec<(String, String, i64, i64)>, DbosError> {
    ctx.db.list_application_versions().await
}

/// Bump an application version's timestamp to "now", making it the latest.
pub async fn set_latest_application_version(
    ctx: &Arc<DbosContext>,
    version_name: &str,
) -> Result<(), DbosError> {
    ctx.db.set_latest_application_version(version_name).await
}

/// Snapshot the in-memory registry of queues (their configured limits).
pub fn list_registered_queues(ctx: &Arc<DbosContext>) -> Vec<crate::queue::WorkflowQueue> {
    ctx.queues.read().unwrap().values().cloned().collect()
}
