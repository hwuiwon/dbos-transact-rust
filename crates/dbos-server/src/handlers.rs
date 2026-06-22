//! Endpoint handlers for the admin HTTP server.
//!
//! Each handler is intentionally small: it parses the request, calls the
//! `dbos-core` (`dbos`) public API, and maps the result onto the admin wire
//! shapes. Validation/decode failures map to 400, not-found to 404, runtime
//! failures to 500, and successful mutations with no body to 204.

use std::sync::Arc;

use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use dbos::{
    DbosContext, DbosError, ForkOptions, ListWorkflowsInput, WorkflowStatusType,
    cancel_workflow as core_cancel, fork_workflow as core_fork, garbage_collect_workflows,
    get_workflow_steps as core_steps, list_workflows as core_list,
    recover_pending_workflows as core_recover, resume_workflow as core_resume,
};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::transform::{to_list_response, to_step_response};

/// Map a [`DbosError`] onto a 500 plain-text response carrying the error string.
fn internal_error(e: DbosError) -> Response {
    (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
}

// ---------------------------------------------------------------------------
// GET /dbos-healthz
// ---------------------------------------------------------------------------

/// Liveness probe.
pub async fn healthz() -> Response {
    (StatusCode::OK, "healthy").into_response()
}

// ---------------------------------------------------------------------------
// POST /dbos-workflow-recovery
// ---------------------------------------------------------------------------

/// Recover pending workflows for the given executor ids; returns the recovered
/// workflow ids.
pub async fn workflow_recovery(
    State(ctx): State<Arc<DbosContext>>,
    Json(executor_ids): Json<Vec<String>>,
) -> Response {
    let refs: Vec<&str> = executor_ids.iter().map(String::as_str).collect();
    match core_recover(&ctx, &refs).await {
        Ok(handles) => {
            let ids: Vec<String> = handles
                .iter()
                .map(|h| h.workflow_id().to_string())
                .collect();
            Json(ids).into_response()
        }
        Err(e) => internal_error(e),
    }
}

// ---------------------------------------------------------------------------
// GET /dbos-workflow-queues-metadata
// ---------------------------------------------------------------------------

/// Registered queues and their limits. dbos-core does not expose a public queue
/// list getter, so this returns an empty array (per the task constraints).
pub async fn workflow_queues_metadata(State(_ctx): State<Arc<DbosContext>>) -> Response {
    Json(Vec::<Value>::new()).into_response()
}

// ---------------------------------------------------------------------------
// GET /deactivate
// ---------------------------------------------------------------------------

/// Stop accepting new work (best-effort). dbos-core has no public deactivate
/// hook, so this simply acknowledges with 200.
pub async fn deactivate(State(_ctx): State<Arc<DbosContext>>) -> Response {
    (StatusCode::OK, "deactivated").into_response()
}

// ---------------------------------------------------------------------------
// POST /workflows
// ---------------------------------------------------------------------------

/// Request body for `POST /workflows`. All fields optional (snake_case); an empty
/// body means "no filters".
#[derive(Debug, Default, Deserialize)]
pub struct ListWorkflowsRequest {
    #[serde(default)]
    pub workflow_uuids: Vec<String>,
    pub status: Option<String>,
    pub application_version: Option<String>,
    pub workflow_name: Option<String>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
    pub sort_desc: Option<bool>,
    pub load_input: Option<bool>,
    pub load_output: Option<bool>,
    pub queue_name: Option<String>,
}

impl ListWorkflowsRequest {
    /// Map the request filters onto a core [`ListWorkflowsInput`].
    fn to_input(&self) -> ListWorkflowsInput {
        let status = self
            .status
            .as_deref()
            .filter(|s| !s.is_empty())
            .and_then(WorkflowStatusType::parse)
            .map(|s| vec![s])
            .unwrap_or_default();
        ListWorkflowsInput {
            workflow_ids: self.workflow_uuids.clone(),
            workflow_name: self
                .workflow_name
                .clone()
                .filter(|s| !s.is_empty())
                .map(|n| vec![n])
                .unwrap_or_default(),
            status,
            application_version: self
                .application_version
                .clone()
                .filter(|s| !s.is_empty())
                .map(|v| vec![v])
                .unwrap_or_default(),
            // dbos-core's ListWorkflowsInput has a `queues_only` flag but no
            // single queue-name filter; presence of a queue name implies the
            // queues-only view.
            queues_only: self.queue_name.as_deref().is_some_and(|q| !q.is_empty()),
            limit: self.limit,
            offset: self.offset,
            sort_desc: self.sort_desc.unwrap_or(false),
            load_input: self.load_input.unwrap_or(false),
            load_output: self.load_output.unwrap_or(false),
            ..Default::default()
        }
    }
}

/// List workflows matching the (optional) filters; returns PascalCase maps.
pub async fn list_workflows(
    State(ctx): State<Arc<DbosContext>>,
    body: Option<Json<ListWorkflowsRequest>>,
) -> Response {
    let req = body.map(|Json(b)| b).unwrap_or_default();
    match core_list(&ctx, req.to_input()).await {
        Ok(workflows) => {
            let out: Vec<Value> = workflows.iter().map(to_list_response).collect();
            Json(out).into_response()
        }
        Err(e) => internal_error(e),
    }
}

// ---------------------------------------------------------------------------
// GET /workflows/{id}
// ---------------------------------------------------------------------------

/// Single workflow status; 404 if absent.
pub async fn get_workflow(State(ctx): State<Arc<DbosContext>>, Path(id): Path<String>) -> Response {
    let input = ListWorkflowsInput {
        workflow_ids: vec![id],
        load_input: true,
        load_output: true,
        ..Default::default()
    };
    match core_list(&ctx, input).await {
        Ok(mut workflows) => match workflows.first_mut() {
            Some(ws) => Json(to_list_response(ws)).into_response(),
            None => (StatusCode::NOT_FOUND, "Workflow not found").into_response(),
        },
        Err(e) => internal_error(e),
    }
}

// ---------------------------------------------------------------------------
// GET /workflows/{id}/steps
// ---------------------------------------------------------------------------

/// The workflow's executed steps.
pub async fn get_workflow_steps(
    State(ctx): State<Arc<DbosContext>>,
    Path(id): Path<String>,
) -> Response {
    match core_steps(&ctx, &id).await {
        Ok(steps) => {
            let out: Vec<Value> = steps.iter().map(to_step_response).collect();
            Json(out).into_response()
        }
        Err(e) => internal_error(e),
    }
}

// ---------------------------------------------------------------------------
// POST /workflows/{id}/cancel
// ---------------------------------------------------------------------------

/// Cancel a workflow.
pub async fn cancel_workflow(
    State(ctx): State<Arc<DbosContext>>,
    Path(id): Path<String>,
) -> Response {
    match core_cancel(&ctx, &id).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => internal_error(e),
    }
}

// ---------------------------------------------------------------------------
// POST /workflows/{id}/resume
// ---------------------------------------------------------------------------

/// Resume a cancelled/failed workflow; returns the workflow id.
pub async fn resume_workflow(
    State(ctx): State<Arc<DbosContext>>,
    Path(id): Path<String>,
) -> Response {
    match core_resume::<Value>(&ctx, &id).await {
        Ok(handle) => Json(json!({ "workflow_id": handle.workflow_id() })).into_response(),
        Err(e) => internal_error(e),
    }
}

// ---------------------------------------------------------------------------
// POST /workflows/{id}/fork
// ---------------------------------------------------------------------------

/// Request body for `POST /workflows/{id}/fork`.
#[derive(Debug, Default, Deserialize)]
pub struct ForkRequest {
    pub start_step: Option<u64>,
    pub new_workflow_id: Option<String>,
    pub application_version: Option<String>,
}

/// Fork a workflow from a step; returns the new workflow id.
pub async fn fork_workflow(
    State(ctx): State<Arc<DbosContext>>,
    Path(id): Path<String>,
    body: Option<Json<ForkRequest>>,
) -> Response {
    let req = body.map(|Json(b)| b).unwrap_or_default();
    let opts = ForkOptions {
        original_workflow_id: id,
        start_step: req.start_step.unwrap_or(0) as i64,
        application_version: req.application_version,
        forked_workflow_id: req.new_workflow_id,
        queue: None,
    };
    match core_fork::<Value>(&ctx, opts).await {
        Ok(handle) => Json(json!({ "workflow_id": handle.workflow_id() })).into_response(),
        Err(e) => internal_error(e),
    }
}

// ---------------------------------------------------------------------------
// POST /dbos-garbage-collect
// ---------------------------------------------------------------------------

/// Request body for `POST /dbos-garbage-collect`.
#[derive(Debug, Default, Deserialize)]
pub struct GarbageCollectRequest {
    pub cutoff_epoch_timestamp_ms: Option<i64>,
    pub rows_threshold: Option<i64>,
}

/// Garbage-collect terminal workflows. This wires straight to
/// `garbage_collect_workflows` since dbos-core fully implements it.
pub async fn garbage_collect(
    State(ctx): State<Arc<DbosContext>>,
    body: Option<Json<GarbageCollectRequest>>,
) -> Response {
    let req = body.map(|Json(b)| b).unwrap_or_default();
    match garbage_collect_workflows(&ctx, req.cutoff_epoch_timestamp_ms, req.rows_threshold).await {
        Ok(_removed) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => internal_error(e),
    }
}

// ---------------------------------------------------------------------------
// POST /dbos-global-timeout
// ---------------------------------------------------------------------------

/// Request body for `POST /dbos-global-timeout`.
#[derive(Debug, Deserialize)]
pub struct GlobalTimeoutRequest {
    pub cutoff_epoch_timestamp_ms: i64,
}

/// Cancel all non-terminal workflows started before the cutoff.
pub async fn global_timeout(
    State(ctx): State<Arc<DbosContext>>,
    Json(req): Json<GlobalTimeoutRequest>,
) -> Response {
    let cutoff = req.cutoff_epoch_timestamp_ms;
    // Find non-terminal workflows created before the cutoff.
    let non_terminal = [
        WorkflowStatusType::Pending,
        WorkflowStatusType::Enqueued,
        WorkflowStatusType::Delayed,
    ];
    let input = ListWorkflowsInput {
        status: non_terminal.to_vec(),
        ..Default::default()
    };
    let workflows = match core_list(&ctx, input).await {
        Ok(w) => w,
        Err(e) => return internal_error(e),
    };
    for ws in workflows {
        if ws.created_at_ms < cutoff {
            if let Err(e) = core_cancel(&ctx, &ws.id).await {
                return internal_error(e);
            }
        }
    }
    StatusCode::NO_CONTENT.into_response()
}
