//! Workflow recovery — re-dispatch `PENDING` workflows after a crash/restart.
//!
//! Queue-backed pending work is reset to `ENQUEUED` for the runner to
//! re-dispatch; directly-run pending work is re-executed via its erased registry
//! entry (incrementing the recovery attempt counter).

use std::sync::Arc;

use serde_json::Value;

use crate::context::DbosContext;
use crate::db::{ListWorkflowsInput, WorkflowStatusType};
use crate::error::DbosError;
use crate::serialization::PORTABLE_SERIALIZER_NAME;
use crate::workflow::handle::{WorkflowHandle, boxed_polling};
use crate::workflow::run::{ErasedHandle, run_workflow_erased};
use crate::workflow::{RunFlags, RunOptions};

/// Recover all `PENDING` workflows owned by the given executors. Returns a
/// polling handle for each recovered workflow.
pub async fn recover_pending_workflows(
    ctx: &Arc<DbosContext>,
    executor_ids: &[&str],
) -> Result<Vec<Box<dyn WorkflowHandle<Value>>>, DbosError> {
    let app_version = if ctx.application_version().is_empty() {
        vec![]
    } else {
        vec![ctx.application_version().to_string()]
    };

    let pending = ctx
        .db
        .list_workflows(ListWorkflowsInput {
            status: vec![WorkflowStatusType::Pending],
            executor_ids: executor_ids.iter().map(|s| s.to_string()).collect(),
            application_version: app_version,
            load_input: true,
            ..Default::default()
        })
        .await?;

    let mut handles: Vec<Box<dyn WorkflowHandle<Value>>> = Vec::new();

    for wf in pending {
        // Queue-backed work: reset to ENQUEUED for the runner to re-dispatch.
        if !wf.queue_name.is_empty() {
            match ctx.db.clear_queue_assignment(&wf.id).await {
                Ok(true) => handles.push(boxed_polling::<Value>(ctx.clone(), wf.id.clone())),
                Ok(false) => {}
                Err(e) => {
                    tracing::error!(workflow_id = %wf.id, error = %e, "error clearing queue assignment");
                }
            }
            continue;
        }

        // Directly-run work: look up its registry entry by recorded name.
        let registered = {
            let reg = ctx.registry.read().unwrap();
            reg.contains_key(&wf.name)
        };
        if !registered {
            tracing::error!(workflow_name = %wf.name, workflow_id = %wf.id, "workflow not found in registry");
            continue;
        }

        let opts = RunOptions {
            workflow_id: Some(wf.id.clone()),
            portable: wf.serialization == PORTABLE_SERIALIZER_NAME,
            authenticated_user: Some(wf.authenticated_user.clone()),
            assumed_role: Some(wf.assumed_role.clone()),
            authenticated_roles: wf.authenticated_roles.clone(),
            ..Default::default()
        };

        let handle = run_workflow_erased(
            ctx,
            &wf.name,
            wf.input.clone(),
            wf.serialization.clone(),
            opts,
            RunFlags { is_recovery: true, is_dequeue: false },
            None,
        )
        .await?;
        let id = match handle {
            ErasedHandle::Channel { workflow_id, .. } => workflow_id,
            ErasedHandle::Polling { workflow_id } => workflow_id,
        };
        handles.push(boxed_polling::<Value>(ctx.clone(), id));
    }

    Ok(handles)
}
