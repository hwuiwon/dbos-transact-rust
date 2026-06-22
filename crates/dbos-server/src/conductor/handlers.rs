//! Per-message conductor handlers and the dispatch switch.
//!
//! Each handler unmarshals its typed request, performs the work via the
//! `dbos-core` public API, and returns the JSON text of a typed response that
//! echoes the request's `type` and `request_id`.
//!
//! Error philosophy: once a message's base envelope parses, the
//! handler ALWAYS produces a response frame — failures are conveyed via
//! `error_message` (and `success:false` / empty / null output), never by
//! dropping the request. Only a failure to parse the typed *body* of an
//! understood message returns `Err` (the dispatcher then logs it and still
//! emits a best-effort error response).

use std::sync::Arc;

use dbos::{
    DbosContext, ForkOptions, ListWorkflowsInput, StepInfo, WorkflowQueue, WorkflowStatus,
    WorkflowStatusType, cancel_workflow, delete_workflow, fork_workflow, garbage_collect_workflows,
    get_schedule, get_step_aggregates, get_workflow_events, get_workflow_notifications,
    get_workflow_status_counts, get_workflow_steps, get_workflow_streams,
    list_application_versions, list_registered_queues, list_schedules, list_workflows,
    pause_schedule, recover_pending_workflows, resume_schedule, resume_workflow,
    set_latest_application_version,
};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

use super::protocol::{self as proto, BaseResponse, msg};

/// DBOS version reported in `executor_info` (the conductor crate version).
const DBOS_VERSION: &str = env!("CARGO_PKG_VERSION");
/// Language id reported in `executor_info`.
const LANGUAGE: &str = "rust";

/// The outcome of handling one message: the JSON text to send back, or — only
/// for an unparseable typed body — an error to log. The dispatcher converts the
/// error case into a best-effort error response anyway, so a parsed-but-failed
/// operation never silently drops.
pub(super) type HandleResult = Result<String, HandleError>;

/// A failure that occurred *before* a typed response could be built (typed-body
/// parse error). Carries enough context to still emit an error frame.
pub(super) struct HandleError {
    pub message: String,
}

impl HandleError {
    fn new(message: impl Into<String>) -> Self {
        HandleError {
            message: message.into(),
        }
    }
}

/// Serialize a response value to its JSON text, mapping a (very unlikely)
/// marshal failure into a [`HandleError`].
fn marshal<T: Serialize>(value: &T) -> HandleResult {
    serde_json::to_string(value).map_err(|e| HandleError::new(format!("failed to marshal: {e}")))
}

/// Parse the typed request body from the raw frame; on failure produce a
/// [`HandleError`] (no typed response possible).
fn parse<T: DeserializeOwned>(data: &[u8]) -> Result<T, HandleError> {
    serde_json::from_slice(data)
        .map_err(|e| HandleError::new(format!("failed to parse request: {e}")))
}

/// Render an epoch-ms value as a JSON-string when present and non-zero.
fn epoch_ms_string(ms: i64) -> Option<String> {
    if ms == 0 { None } else { Some(ms.to_string()) }
}

/// Render an optional epoch-ms value as a JSON-string when present and non-zero.
fn epoch_ms_string_opt(ms: Option<i64>) -> Option<String> {
    ms.and_then(epoch_ms_string)
}

/// Some-if-non-empty helper for the many string fields.
fn non_empty(s: &str) -> Option<String> {
    if s.is_empty() {
        None
    } else {
        Some(s.to_string())
    }
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

/// Switch on the parsed message type and run the matching handler, returning
/// the response JSON to send. The base envelope (`type`, `request_id`) has
/// already been parsed by the caller.
///
/// Unknown / unimplemented message types yield a well-formed error response
/// rather than being dropped.
pub(super) async fn dispatch(
    ctx: &Arc<DbosContext>,
    msg_type: &str,
    request_id: &str,
    data: &[u8],
) -> HandleResult {
    match msg_type {
        msg::EXECUTOR_INFO => handle_executor_info(ctx, request_id),
        msg::RECOVERY => handle_recovery(ctx, request_id, data).await,
        msg::CANCEL => handle_cancel(ctx, request_id, data).await,
        msg::RESUME => handle_resume(ctx, request_id, data).await,
        msg::LIST_WORKFLOWS => handle_list_workflows(ctx, request_id, data, false).await,
        msg::LIST_QUEUED_WORKFLOWS => handle_list_workflows(ctx, request_id, data, true).await,
        msg::LIST_STEPS => handle_list_steps(ctx, request_id, data).await,
        msg::GET_WORKFLOW => handle_get_workflow(ctx, request_id, data).await,
        msg::FORK_WORKFLOW => handle_fork_workflow(ctx, request_id, data).await,
        msg::EXIST_PENDING_WORKFLOWS => handle_exist_pending(ctx, request_id, data).await,
        msg::RETENTION => handle_retention(ctx, request_id, data).await,
        msg::GET_WORKFLOW_AGGREGATES | msg::GET_METRICS => {
            handle_workflow_aggregates(ctx, msg_type, request_id).await
        }
        msg::LIST_SCHEDULES => handle_list_schedules(ctx, request_id, data).await,
        msg::GET_SCHEDULE => handle_get_schedule(ctx, request_id, data).await,
        msg::PAUSE_SCHEDULE => handle_schedule_toggle(ctx, request_id, data, true).await,
        msg::RESUME_SCHEDULE => handle_schedule_toggle(ctx, request_id, data, false).await,
        msg::DELETE => handle_delete(ctx, request_id, data).await,
        msg::ALERT => handle_alert(request_id, data),
        msg::GET_WORKFLOW_EVENTS => handle_get_workflow_events(ctx, request_id, data).await,
        msg::GET_WORKFLOW_NOTIFICATIONS => {
            handle_get_workflow_notifications(ctx, request_id, data).await
        }
        msg::GET_WORKFLOW_STREAMS => handle_get_workflow_streams(ctx, request_id, data).await,
        msg::GET_STEP_AGGREGATES => handle_step_aggregates(ctx, request_id).await,
        msg::LIST_APPLICATION_VERSIONS => handle_list_application_versions(ctx, request_id).await,
        msg::SET_LATEST_APPLICATION_VERSION => {
            handle_set_latest_application_version(ctx, request_id, data).await
        }
        msg::LIST_QUEUES => handle_list_queues(ctx, request_id),
        msg::GET_QUEUE => handle_get_queue(ctx, request_id, data),
        // Catalogued but not implemented in this port — answer with a
        // well-formed "unsupported command" error rather than dropping. These
        // remain stubbed: `export_workflow` / `import_workflow` (need a
        // gzip full-state serializer) and `backfill_schedule` / `trigger_schedule`
        // (need new scheduler ops) — all out of scope here.
        other => Ok(unsupported(other, request_id)?),
    }
}

/// A well-formed error response for an unknown / unsupported message type.
fn unsupported(msg_type: &str, request_id: &str) -> HandleResult {
    marshal(&BaseResponse::err(
        msg_type,
        request_id,
        "unsupported command",
    ))
}

// ---------------------------------------------------------------------------
// executor_info
// ---------------------------------------------------------------------------

fn handle_executor_info(ctx: &Arc<DbosContext>, request_id: &str) -> HandleResult {
    let hostname = hostname();
    let resp = proto::ExecutorInfoResponse {
        base: BaseResponse::ok(msg::EXECUTOR_INFO, request_id),
        executor_id: ctx.executor_id().to_string(),
        application_version: ctx.application_version().to_string(),
        hostname,
        dbos_version: DBOS_VERSION.to_string(),
        language: LANGUAGE.to_string(),
    };
    marshal(&resp)
}

/// Best-effort hostname (`HOSTNAME` env var, else `None`). Avoids pulling in an
/// extra dependency just for this advisory field.
fn hostname() -> Option<String> {
    std::env::var("HOSTNAME").ok().filter(|s| !s.is_empty())
}

// ---------------------------------------------------------------------------
// recovery
// ---------------------------------------------------------------------------

async fn handle_recovery(ctx: &Arc<DbosContext>, request_id: &str, data: &[u8]) -> HandleResult {
    let req: proto::RecoveryRequest = parse(data)?;
    let refs: Vec<&str> = req.executor_ids.iter().map(String::as_str).collect();
    let (success, error) = match recover_pending_workflows(ctx, &refs).await {
        Ok(_) => (true, None),
        Err(e) => (
            false,
            Some(format!("failed to recover pending workflows: {e}")),
        ),
    };
    success_response(msg::RECOVERY, request_id, success, error)
}

// ---------------------------------------------------------------------------
// cancel / resume
// ---------------------------------------------------------------------------

async fn handle_cancel(ctx: &Arc<DbosContext>, request_id: &str, data: &[u8]) -> HandleResult {
    let req: proto::WorkflowIdsRequest = parse(data)?;
    let ids = req.resolve_ids();
    let mut error = None;
    for id in &ids {
        if let Err(e) = cancel_workflow(ctx, id).await {
            error = Some(format!("failed to cancel workflows: {e}"));
            break;
        }
    }
    success_response(msg::CANCEL, request_id, error.is_none(), error)
}

async fn handle_resume(ctx: &Arc<DbosContext>, request_id: &str, data: &[u8]) -> HandleResult {
    let req: proto::WorkflowIdsRequest = parse(data)?;
    let ids = req.resolve_ids();
    let mut error = None;
    for id in &ids {
        // The optional `queue_name` (WithResumeQueue) has no dbos-core analogue
        // yet; resume always returns to the internal queue.
        if let Err(e) = resume_workflow::<Value>(ctx, id).await {
            error = Some(format!("failed to resume workflows: {e}"));
            break;
        }
    }
    success_response(msg::RESUME, request_id, error.is_none(), error)
}

// ---------------------------------------------------------------------------
// list_workflows / list_queued_workflows
// ---------------------------------------------------------------------------

async fn handle_list_workflows(
    ctx: &Arc<DbosContext>,
    request_id: &str,
    data: &[u8],
    queued: bool,
) -> HandleResult {
    let req: proto::ListWorkflowsRequest = parse(data)?;
    let body = req.body;

    let mut status: Vec<WorkflowStatusType> = body
        .status
        .to_slice()
        .iter()
        .filter_map(|s| WorkflowStatusType::parse(s))
        .collect();
    if queued && status.is_empty() {
        // Default queued view to the non-terminal queue states.
        status = vec![
            WorkflowStatusType::Pending,
            WorkflowStatusType::Enqueued,
            WorkflowStatusType::Delayed,
        ];
    }

    let input = ListWorkflowsInput {
        workflow_ids: body.workflow_uuids.clone(),
        workflow_name: body.workflow_name.to_slice(),
        status,
        executor_ids: body.executor_id.to_slice(),
        application_version: body.application_version.to_slice(),
        // queued view forces queues-only; otherwise honor the flag.
        queues_only: queued || body.queues_only,
        limit: body.limit,
        offset: body.offset,
        sort_desc: body.sort_desc,
        load_input: body.load_input,
        // list_queued_workflows forces load_output = false.
        load_output: if queued { false } else { body.load_output },
    };

    let msg_type = if queued {
        msg::LIST_QUEUED_WORKFLOWS
    } else {
        msg::LIST_WORKFLOWS
    };

    match list_workflows(ctx, input).await {
        Ok(workflows) => {
            let output: Vec<proto::ListWorkflowsRow> =
                workflows.iter().map(format_list_workflows_row).collect();
            marshal(&proto::ListWorkflowsResponse {
                base: BaseResponse::ok(msg_type, request_id),
                output,
            })
        }
        Err(e) => {
            let verb = if queued {
                "list queued workflows"
            } else {
                "list workflows"
            };
            marshal(&proto::ListWorkflowsResponse {
                base: BaseResponse::err(msg_type, request_id, format!("failed to {verb}: {e}")),
                output: Vec::new(),
            })
        }
    }
}

/// Map a [`WorkflowStatus`] onto the PascalCase conductor row.
pub(super) fn format_list_workflows_row(wf: &WorkflowStatus) -> proto::ListWorkflowsRow {
    let authenticated_roles = if wf.authenticated_roles.is_empty() {
        None
    } else {
        serde_json::to_string(&wf.authenticated_roles).ok()
    };
    let dequeued_at = if wf.status == WorkflowStatusType::Pending {
        epoch_ms_string_opt(wf.started_at_ms)
    } else {
        None
    };
    proto::ListWorkflowsRow {
        workflow_uuid: wf.id.clone(),
        status: Some(wf.status.as_str().to_string()),
        workflow_name: non_empty(&wf.name),
        workflow_class_name: non_empty(&wf.class_name),
        workflow_config_name: wf.config_name.clone().filter(|s| !s.is_empty()),
        authenticated_user: non_empty(&wf.authenticated_user),
        assumed_role: non_empty(&wf.assumed_role),
        authenticated_roles,
        input: wf.input.clone(),
        output: wf.output.clone(),
        error: wf.error.clone(),
        created_at: epoch_ms_string(wf.created_at_ms),
        updated_at: epoch_ms_string(wf.updated_at_ms),
        queue_name: non_empty(&wf.queue_name),
        application_version: non_empty(&wf.application_version),
        executor_id: non_empty(&wf.executor_id),
        workflow_timeout_ms: if wf.timeout_ms > 0 {
            Some(wf.timeout_ms.to_string())
        } else {
            None
        },
        workflow_deadline_epoch_ms: epoch_ms_string_opt(wf.deadline_ms),
        deduplication_id: non_empty(&wf.deduplication_id),
        priority: wf.priority.to_string(),
        queue_partition_key: non_empty(&wf.queue_partition_key),
        forked_from: non_empty(&wf.forked_from),
        was_forked_from: wf.was_forked_from,
        parent_workflow_id: non_empty(&wf.parent_workflow_id),
        dequeued_at,
        delay_until_epoch_ms: epoch_ms_string_opt(wf.delay_until_ms),
        completed_at: epoch_ms_string_opt(wf.completed_at_ms),
    }
}

// ---------------------------------------------------------------------------
// list_steps
// ---------------------------------------------------------------------------

async fn handle_list_steps(ctx: &Arc<DbosContext>, request_id: &str, data: &[u8]) -> HandleResult {
    let req: proto::ListStepsRequest = parse(data)?;
    match get_workflow_steps(ctx, &req.workflow_id).await {
        Ok(steps) => {
            let output: Vec<proto::StepRow> = steps.iter().map(format_step_row).collect();
            marshal(&proto::ListStepsResponse {
                base: BaseResponse::ok(msg::LIST_STEPS, request_id),
                output: Some(output),
            })
        }
        Err(e) => marshal(&proto::ListStepsResponse {
            base: BaseResponse::err(
                msg::LIST_STEPS,
                request_id,
                format!("failed to list workflow steps: {e}"),
            ),
            output: None,
        }),
    }
}

/// Map a [`StepInfo`] onto the conductor step row. Epoch-ms are strings here.
fn format_step_row(step: &StepInfo) -> proto::StepRow {
    proto::StepRow {
        function_id: step.step_id,
        function_name: step.step_name.clone(),
        output: step.output.clone(),
        error: step.error.clone(),
        child_workflow_id: step.child_workflow_id.clone().filter(|s| !s.is_empty()),
        started_at_epoch_ms: epoch_ms_string_opt(step.started_at_ms),
        completed_at_epoch_ms: epoch_ms_string_opt(step.completed_at_ms),
    }
}

// ---------------------------------------------------------------------------
// get_workflow
// ---------------------------------------------------------------------------

async fn handle_get_workflow(
    ctx: &Arc<DbosContext>,
    request_id: &str,
    data: &[u8],
) -> HandleResult {
    let req: proto::GetWorkflowRequest = parse(data)?;
    let input = ListWorkflowsInput {
        workflow_ids: vec![req.workflow_id],
        load_input: req.load_input,
        load_output: req.load_output,
        ..Default::default()
    };
    match list_workflows(ctx, input).await {
        Ok(workflows) => {
            let output = workflows.first().map(format_list_workflows_row);
            marshal(&proto::GetWorkflowResponse {
                base: BaseResponse::ok(msg::GET_WORKFLOW, request_id),
                output,
            })
        }
        Err(e) => marshal(&proto::GetWorkflowResponse {
            base: BaseResponse::err(
                msg::GET_WORKFLOW,
                request_id,
                format!("failed to get workflow: {e}"),
            ),
            output: None,
        }),
    }
}

// ---------------------------------------------------------------------------
// fork_workflow
// ---------------------------------------------------------------------------

async fn handle_fork_workflow(
    ctx: &Arc<DbosContext>,
    request_id: &str,
    data: &[u8],
) -> HandleResult {
    let req: proto::ForkWorkflowRequest = parse(data)?;
    let body = req.body;

    // start_step bounds check ([0, MaxInt32/2]). A violation is reported in-band
    // (an error response), not by dropping.
    if body.start_step < 0 {
        return marshal(&proto::ForkWorkflowResponse {
            base: BaseResponse::err(
                msg::FORK_WORKFLOW,
                request_id,
                "invalid StartStep: cannot be negative",
            ),
            new_workflow_id: None,
        });
    }
    let max_start = (i32::MAX / 2) as i64;
    if body.start_step > max_start {
        return marshal(&proto::ForkWorkflowResponse {
            base: BaseResponse::err(
                msg::FORK_WORKFLOW,
                request_id,
                format!("invalid StartStep: cannot be greater than {max_start}"),
            ),
            new_workflow_id: None,
        });
    }

    let opts = ForkOptions {
        original_workflow_id: body.workflow_id,
        start_step: body.start_step,
        application_version: body.application_version,
        forked_workflow_id: body.new_workflow_id,
        queue: body.queue_name,
    };
    match fork_workflow::<Value>(ctx, opts).await {
        Ok(handle) => marshal(&proto::ForkWorkflowResponse {
            base: BaseResponse::ok(msg::FORK_WORKFLOW, request_id),
            new_workflow_id: Some(handle.workflow_id().to_string()),
        }),
        Err(e) => marshal(&proto::ForkWorkflowResponse {
            base: BaseResponse::err(
                msg::FORK_WORKFLOW,
                request_id,
                format!("failed to fork workflow: {e}"),
            ),
            new_workflow_id: None,
        }),
    }
}

// ---------------------------------------------------------------------------
// exist_pending_workflows
// ---------------------------------------------------------------------------

async fn handle_exist_pending(
    ctx: &Arc<DbosContext>,
    request_id: &str,
    data: &[u8],
) -> HandleResult {
    let req: proto::ExistPendingWorkflowsRequest = parse(data)?;
    let input = ListWorkflowsInput {
        status: vec![WorkflowStatusType::Pending],
        limit: Some(1),
        executor_ids: if req.executor_id.is_empty() {
            vec![]
        } else {
            vec![req.executor_id]
        },
        application_version: if req.application_version.is_empty() {
            vec![]
        } else {
            vec![req.application_version]
        },
        ..Default::default()
    };
    match list_workflows(ctx, input).await {
        Ok(workflows) => marshal(&proto::ExistPendingWorkflowsResponse {
            base: BaseResponse::ok(msg::EXIST_PENDING_WORKFLOWS, request_id),
            exist: !workflows.is_empty(),
        }),
        Err(e) => marshal(&proto::ExistPendingWorkflowsResponse {
            base: BaseResponse::err(
                msg::EXIST_PENDING_WORKFLOWS,
                request_id,
                format!("failed to check for pending workflows: {e}"),
            ),
            exist: false,
        }),
    }
}

// ---------------------------------------------------------------------------
// retention (garbage collect + timeout)
// ---------------------------------------------------------------------------

async fn handle_retention(ctx: &Arc<DbosContext>, request_id: &str, data: &[u8]) -> HandleResult {
    let req: proto::RetentionRequest = parse(data)?;
    let body = req.body;

    let mut success = true;
    let mut error = None;

    if body.gc_cutoff_epoch_ms.is_some() || body.gc_rows_threshold.is_some() {
        if let Err(e) =
            garbage_collect_workflows(ctx, body.gc_cutoff_epoch_ms, body.gc_rows_threshold).await
        {
            success = false;
            error = Some(format!("failed to garbage collect workflows: {e}"));
        }
    }

    // The timeout step uses dbos-core's cancel for non-terminal workflows older
    // than the cutoff (no public `cancelAllBefore`; emulate it).
    if success {
        if let Some(cutoff) = body.timeout_cutoff_epoch_ms {
            if let Err(e) = timeout_before(ctx, cutoff).await {
                success = false;
                error = Some(format!("failed to timeout workflows: {e}"));
            }
        }
    }

    success_response(msg::RETENTION, request_id, success, error)
}

/// Cancel all non-terminal workflows created before `cutoff` (epoch-ms).
async fn timeout_before(ctx: &Arc<DbosContext>, cutoff: i64) -> Result<(), dbos::DbosError> {
    let input = ListWorkflowsInput {
        status: vec![
            WorkflowStatusType::Pending,
            WorkflowStatusType::Enqueued,
            WorkflowStatusType::Delayed,
        ],
        ..Default::default()
    };
    for wf in list_workflows(ctx, input).await? {
        if wf.created_at_ms < cutoff {
            cancel_workflow(ctx, &wf.id).await?;
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// get_workflow_aggregates / get_metrics (status counts)
// ---------------------------------------------------------------------------

async fn handle_workflow_aggregates(
    ctx: &Arc<DbosContext>,
    msg_type: &str,
    request_id: &str,
) -> HandleResult {
    // dbos-core exposes workflow status counts; surface them as aggregate rows
    // of `{status, count}`. Richer group_by/select dimensions are not supported.
    match get_workflow_status_counts(ctx).await {
        Ok(counts) => {
            let output: Vec<Value> = counts
                .into_iter()
                .map(|(status, count)| json!({ "status": status, "count": count }))
                .collect();
            marshal(&proto::WorkflowAggregatesResponse {
                base: BaseResponse::ok(msg_type, request_id),
                output,
            })
        }
        Err(e) => marshal(&proto::WorkflowAggregatesResponse {
            base: BaseResponse::err(
                msg_type,
                request_id,
                format!("failed to get workflow aggregates: {e}"),
            ),
            output: Vec::new(),
        }),
    }
}

// ---------------------------------------------------------------------------
// list_schedules / get_schedule / pause_schedule / resume_schedule
// ---------------------------------------------------------------------------

async fn handle_list_schedules(
    ctx: &Arc<DbosContext>,
    request_id: &str,
    data: &[u8],
) -> HandleResult {
    let req: proto::ListSchedulesRequest = parse(data)?;
    // `load_context` defaults to true.
    let load_context = req.body.load_context.unwrap_or(true);
    match list_schedules(ctx).await {
        Ok(schedules) => {
            let output: Vec<proto::ScheduleRow> = schedules
                .iter()
                .map(|s| schedule_to_row(s, load_context))
                .collect();
            marshal(&proto::ListSchedulesResponse {
                base: BaseResponse::ok(msg::LIST_SCHEDULES, request_id),
                output,
            })
        }
        Err(e) => marshal(&proto::ListSchedulesResponse {
            base: BaseResponse::err(
                msg::LIST_SCHEDULES,
                request_id,
                format!("failed to list schedules: {e}"),
            ),
            output: Vec::new(),
        }),
    }
}

async fn handle_get_schedule(
    ctx: &Arc<DbosContext>,
    request_id: &str,
    data: &[u8],
) -> HandleResult {
    let req: proto::GetScheduleRequest = parse(data)?;
    let load_context = req.load_context.unwrap_or(true);
    match get_schedule(ctx, &req.schedule_name).await {
        Ok(schedule) => marshal(&proto::GetScheduleResponse {
            base: BaseResponse::ok(msg::GET_SCHEDULE, request_id),
            output: schedule.map(|s| schedule_to_row(&s, load_context)),
        }),
        Err(e) => marshal(&proto::GetScheduleResponse {
            base: BaseResponse::err(
                msg::GET_SCHEDULE,
                request_id,
                format!("failed to get schedule '{}': {e}", req.schedule_name),
            ),
            output: None,
        }),
    }
}

async fn handle_schedule_toggle(
    ctx: &Arc<DbosContext>,
    request_id: &str,
    data: &[u8],
    pause: bool,
) -> HandleResult {
    let req: proto::ScheduleNameRequest = parse(data)?;
    let (msg_type, verb) = if pause {
        (msg::PAUSE_SCHEDULE, "pause")
    } else {
        (msg::RESUME_SCHEDULE, "resume")
    };
    let result = if pause {
        pause_schedule(ctx, &req.schedule_name).await
    } else {
        resume_schedule(ctx, &req.schedule_name).await
    };
    let (success, error) = match result {
        Ok(()) => (true, None),
        Err(e) => (
            false,
            Some(format!(
                "failed to {verb} schedule '{}': {e}",
                req.schedule_name
            )),
        ),
    };
    success_response(msg_type, request_id, success, error)
}

/// Map a [`dbos::WorkflowSchedule`] onto the conductor schedule row.
fn schedule_to_row(s: &dbos::WorkflowSchedule, load_context: bool) -> proto::ScheduleRow {
    let context = if load_context && !s.context.is_null() {
        serde_json::to_string(&s.context).ok()
    } else {
        None
    };
    proto::ScheduleRow {
        schedule_id: s.schedule_id.clone(),
        schedule_name: s.schedule_name.clone(),
        workflow_name: s.workflow_name.clone(),
        workflow_class_name: s.workflow_class_name.clone().filter(|c| !c.is_empty()),
        schedule: s.schedule.clone(),
        status: s.status.clone(),
        context,
        last_fired_at: s.last_fired_at.clone(),
        automatic_backfill: s.automatic_backfill,
        cron_timezone: s.cron_timezone.clone().filter(|c| !c.is_empty()),
        queue_name: s.queue_name.clone().filter(|c| !c.is_empty()),
    }
}

// ---------------------------------------------------------------------------
// delete
// ---------------------------------------------------------------------------

async fn handle_delete(ctx: &Arc<DbosContext>, request_id: &str, data: &[u8]) -> HandleResult {
    let req: proto::WorkflowIdsRequest = parse(data)?;
    let ids = req.resolve_ids();
    let mut error = None;
    for id in &ids {
        if let Err(e) = delete_workflow(ctx, id, req.delete_children).await {
            error = Some(format!("failed to delete workflows: {e}"));
            break;
        }
    }
    success_response(msg::DELETE, request_id, error.is_none(), error)
}

// ---------------------------------------------------------------------------
// alert (log-only no-op; never panics)
// ---------------------------------------------------------------------------

fn handle_alert(request_id: &str, data: &[u8]) -> HandleResult {
    let req: proto::AlertRequest = parse(data)?;
    tracing::info!(
        name = %req.name,
        message = %req.message,
        metadata = ?req.metadata,
        "alert received (no handler registered)",
    );
    success_response(msg::ALERT, request_id, true, None)
}

// ---------------------------------------------------------------------------
// get_workflow_events / get_workflow_notifications / get_workflow_streams
// ---------------------------------------------------------------------------

async fn handle_get_workflow_events(
    ctx: &Arc<DbosContext>,
    request_id: &str,
    data: &[u8],
) -> HandleResult {
    let req: proto::WorkflowIdRequest = parse(data)?;
    match get_workflow_events(ctx, &req.workflow_id).await {
        Ok(rows) => {
            // Raw stored encodings are passed through (mirrors list_workflows /
            // list_steps, which surface the raw encoded strings).
            let events = rows
                .into_iter()
                .map(|(key, value, _ser)| proto::EventOutput {
                    key,
                    value: value.unwrap_or_default(),
                })
                .collect();
            marshal(&proto::GetWorkflowEventsResponse {
                base: BaseResponse::ok(msg::GET_WORKFLOW_EVENTS, request_id),
                events,
            })
        }
        Err(e) => marshal(&proto::GetWorkflowEventsResponse {
            base: BaseResponse::err(
                msg::GET_WORKFLOW_EVENTS,
                request_id,
                format!("failed to get workflow events: {e}"),
            ),
            events: Vec::new(),
        }),
    }
}

async fn handle_get_workflow_notifications(
    ctx: &Arc<DbosContext>,
    request_id: &str,
    data: &[u8],
) -> HandleResult {
    let req: proto::WorkflowIdRequest = parse(data)?;
    match get_workflow_notifications(ctx, &req.workflow_id).await {
        Ok(rows) => {
            let notifications = rows
                .into_iter()
                .map(|(topic, message, created_at_epoch_ms, _ser, consumed)| {
                    proto::NotificationOutput {
                        // An empty topic means it was sent without one => null.
                        topic: non_empty(&topic),
                        message: message.unwrap_or_default(),
                        created_at_epoch_ms,
                        consumed,
                    }
                })
                .collect();
            marshal(&proto::GetWorkflowNotificationsResponse {
                base: BaseResponse::ok(msg::GET_WORKFLOW_NOTIFICATIONS, request_id),
                notifications,
            })
        }
        Err(e) => marshal(&proto::GetWorkflowNotificationsResponse {
            base: BaseResponse::err(
                msg::GET_WORKFLOW_NOTIFICATIONS,
                request_id,
                format!("failed to get workflow notifications: {e}"),
            ),
            notifications: Vec::new(),
        }),
    }
}

async fn handle_get_workflow_streams(
    ctx: &Arc<DbosContext>,
    request_id: &str,
    data: &[u8],
) -> HandleResult {
    let req: proto::WorkflowIdRequest = parse(data)?;
    match get_workflow_streams(ctx, &req.workflow_id).await {
        Ok(rows) => {
            // Rows arrive ordered by (key, offset); group consecutive runs by
            // key into one entry each (single pass, relying on the ordering).
            let mut streams: Vec<proto::StreamEntryOutput> = Vec::new();
            for (key, _offset, value, _ser) in rows {
                match streams.last_mut() {
                    Some(entry) if entry.key == key => entry.values.push(value),
                    _ => streams.push(proto::StreamEntryOutput {
                        key,
                        values: vec![value],
                    }),
                }
            }
            marshal(&proto::GetWorkflowStreamsResponse {
                base: BaseResponse::ok(msg::GET_WORKFLOW_STREAMS, request_id),
                streams,
            })
        }
        Err(e) => marshal(&proto::GetWorkflowStreamsResponse {
            base: BaseResponse::err(
                msg::GET_WORKFLOW_STREAMS,
                request_id,
                format!("failed to get workflow streams: {e}"),
            ),
            streams: Vec::new(),
        }),
    }
}

// ---------------------------------------------------------------------------
// get_step_aggregates
// ---------------------------------------------------------------------------

async fn handle_step_aggregates(ctx: &Arc<DbosContext>, request_id: &str) -> HandleResult {
    match get_step_aggregates(ctx).await {
        Ok(rows) => {
            let output = rows
                .into_iter()
                .map(|(step_name, count)| proto::StepAggregateRow { step_name, count })
                .collect();
            marshal(&proto::StepAggregatesResponse {
                base: BaseResponse::ok(msg::GET_STEP_AGGREGATES, request_id),
                output,
            })
        }
        Err(e) => marshal(&proto::StepAggregatesResponse {
            base: BaseResponse::err(
                msg::GET_STEP_AGGREGATES,
                request_id,
                format!("Exception encountered when getting step aggregates: {e}"),
            ),
            output: Vec::new(),
        }),
    }
}

// ---------------------------------------------------------------------------
// list_application_versions / set_latest_application_version
// ---------------------------------------------------------------------------

async fn handle_list_application_versions(
    ctx: &Arc<DbosContext>,
    request_id: &str,
) -> HandleResult {
    match list_application_versions(ctx).await {
        Ok(rows) => {
            let output = rows
                .into_iter()
                .map(
                    |(version_id, version_name, version_timestamp, created_at)| {
                        proto::ApplicationVersionOutput {
                            version_id,
                            version_name,
                            version_timestamp,
                            created_at,
                        }
                    },
                )
                .collect();
            marshal(&proto::ListApplicationVersionsResponse {
                base: BaseResponse::ok(msg::LIST_APPLICATION_VERSIONS, request_id),
                output,
            })
        }
        Err(e) => marshal(&proto::ListApplicationVersionsResponse {
            base: BaseResponse::err(
                msg::LIST_APPLICATION_VERSIONS,
                request_id,
                format!("failed to list application versions: {e}"),
            ),
            output: Vec::new(),
        }),
    }
}

async fn handle_set_latest_application_version(
    ctx: &Arc<DbosContext>,
    request_id: &str,
    data: &[u8],
) -> HandleResult {
    let req: proto::SetLatestApplicationVersionRequest = parse(data)?;
    let (success, error) = match set_latest_application_version(ctx, &req.version_name).await {
        Ok(()) => (true, None),
        Err(e) => (
            false,
            Some(format!(
                "failed to set latest application version '{}': {e}",
                req.version_name
            )),
        ),
    };
    success_response(
        msg::SET_LATEST_APPLICATION_VERSION,
        request_id,
        success,
        error,
    )
}

// ---------------------------------------------------------------------------
// list_queues / get_queue
// ---------------------------------------------------------------------------

fn handle_list_queues(ctx: &Arc<DbosContext>, request_id: &str) -> HandleResult {
    let output = list_registered_queues(ctx)
        .iter()
        .map(queue_to_output)
        .collect();
    marshal(&proto::ListQueuesResponse {
        base: BaseResponse::ok(msg::LIST_QUEUES, request_id),
        output,
    })
}

fn handle_get_queue(ctx: &Arc<DbosContext>, request_id: &str, data: &[u8]) -> HandleResult {
    let req: proto::GetQueueRequest = parse(data)?;
    let queue = list_registered_queues(ctx)
        .iter()
        .find(|q| q.name == req.name)
        .map(queue_to_output);
    match queue {
        Some(output) => marshal(&proto::GetQueueResponse {
            base: BaseResponse::ok(msg::GET_QUEUE, request_id),
            output: Some(output),
        }),
        None => marshal(&proto::GetQueueResponse {
            base: BaseResponse::err(
                msg::GET_QUEUE,
                request_id,
                format!("failed to get queue '{}': queue not found", req.name),
            ),
            output: None,
        }),
    }
}

/// Map a registered [`WorkflowQueue`] onto the conductor queue row.
fn queue_to_output(q: &WorkflowQueue) -> proto::QueueOutput {
    let (rate_limit_max, rate_limit_period_sec) = match &q.rate_limit {
        Some(rl) => (Some(rl.limit), Some(rl.period.as_secs_f64())),
        None => (None, None),
    };
    proto::QueueOutput {
        name: q.name.clone(),
        concurrency: q.global_concurrency,
        worker_concurrency: q.worker_concurrency,
        rate_limit_max,
        rate_limit_period_sec,
        priority_enabled: q.priority_enabled,
        // dbos-core queues are not partitioned and have no separately tracked
        // base polling interval; report the parity defaults.
        partition_queue: false,
        polling_interval_sec: 1.0,
    }
}

// ---------------------------------------------------------------------------
// shared response helpers
// ---------------------------------------------------------------------------

/// Build and marshal a `{success}` response with an optional `error_message`.
fn success_response(
    msg_type: &str,
    request_id: &str,
    success: bool,
    error: Option<String>,
) -> HandleResult {
    let base = match error {
        Some(e) => BaseResponse::err(msg_type, request_id, e),
        None => BaseResponse::ok(msg_type, request_id),
    };
    marshal(&proto::SuccessResponse { base, success })
}
