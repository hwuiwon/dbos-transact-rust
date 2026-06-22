//! Dynamic (DB-backed) schedule management — the public CRUD API.
//!
//! Unlike statically registered scheduled workflows (installed at
//! registration time), dynamic schedules are rows in `workflow_schedules` that
//! the reconciler ([`crate::scheduler::engine::run_reconciler`]) picks up within
//! one poll interval and begins firing.
//!
//! A scheduled workflow here is an already-registered workflow (by name) that
//! takes a [`crate::scheduler::ScheduledWorkflowInput`] as input.

use std::sync::Arc;

use crate::context::DbosContext;
use crate::db::ScheduleRow;
use crate::error::DbosError;
use crate::scheduler::parse_cron;
use crate::util::new_uuid;

/// Status string for an active schedule.
const STATUS_ACTIVE: &str = "ACTIVE";
/// Status string for a paused schedule.
const STATUS_PAUSED: &str = "PAUSED";

/// Options for [`create_schedule`].
#[derive(Debug, Clone, Default)]
pub struct CreateScheduleOptions {
    /// Unique logical name for the schedule (required).
    pub schedule_name: String,
    /// The registered workflow name to fire on each tick (required).
    pub workflow_name: String,
    /// The cron spec (5-field or 6-field with leading seconds; required).
    pub schedule: String,
    /// Optional user context passed to each run (JSON-serialized; `null` if `None`).
    pub context: Option<serde_json::Value>,
    /// Backfill missed ticks since `last_fired_at` when the schedule is (re)installed.
    pub automatic_backfill: bool,
    /// Optional IANA timezone name (see the engine note: named tz needs `chrono-tz`).
    pub cron_timezone: Option<String>,
    /// Queue to enqueue ticks on (defaults to the internal queue).
    pub queue_name: Option<String>,
    /// Optional cross-language class/namespace name.
    pub workflow_class_name: Option<String>,
}

/// The public read model for a schedule (returned by [`get_schedule`]/[`list_schedules`]).
#[derive(Debug, Clone, serde::Serialize)]
pub struct WorkflowSchedule {
    pub schedule_id: String,
    pub schedule_name: String,
    pub workflow_name: String,
    pub workflow_class_name: Option<String>,
    pub schedule: String,
    pub status: String,
    pub context: serde_json::Value,
    pub last_fired_at: Option<String>,
    pub automatic_backfill: bool,
    pub cron_timezone: Option<String>,
    pub queue_name: Option<String>,
}

impl From<ScheduleRow> for WorkflowSchedule {
    fn from(r: ScheduleRow) -> Self {
        WorkflowSchedule {
            context: serde_json::from_str(&r.context).unwrap_or(serde_json::Value::Null),
            schedule_id: r.schedule_id,
            schedule_name: r.schedule_name,
            workflow_name: r.workflow_name,
            workflow_class_name: r.workflow_class_name,
            schedule: r.schedule,
            status: r.status,
            last_fired_at: r.last_fired_at,
            automatic_backfill: r.automatic_backfill,
            cron_timezone: r.cron_timezone,
            queue_name: r.queue_name,
        }
    }
}

/// Create a DB-backed schedule. The reconciler installs and begins firing it
/// within one poll interval. The cron spec is validated up front.
///
/// Returns the created schedule's id.
pub async fn create_schedule(
    ctx: &Arc<DbosContext>,
    opts: CreateScheduleOptions,
) -> Result<String, DbosError> {
    if opts.schedule_name.trim().is_empty() {
        return Err(DbosError::initialization("schedule_name is required"));
    }
    if opts.workflow_name.trim().is_empty() {
        return Err(DbosError::initialization("workflow_name is required"));
    }
    if opts.schedule.trim().is_empty() {
        return Err(DbosError::initialization("schedule is required"));
    }
    parse_cron(&opts.schedule).map_err(|e| {
        DbosError::initialization(format!("invalid cron schedule {:?}: {e}", opts.schedule))
    })?;

    let schedule_id = new_uuid();
    let context_json = serde_json::to_string(&opts.context.unwrap_or(serde_json::Value::Null))
        .unwrap_or_else(|_| "null".to_string());

    let row = ScheduleRow {
        schedule_id: schedule_id.clone(),
        schedule_name: opts.schedule_name,
        workflow_name: opts.workflow_name,
        workflow_class_name: opts.workflow_class_name.filter(|s| !s.is_empty()),
        schedule: opts.schedule,
        status: STATUS_ACTIVE.to_string(),
        context: context_json,
        last_fired_at: None,
        automatic_backfill: opts.automatic_backfill,
        cron_timezone: opts.cron_timezone.filter(|s| !s.is_empty()),
        queue_name: opts.queue_name.filter(|s| !s.is_empty()),
    };
    ctx.db.create_schedule(row).await?;
    Ok(schedule_id)
}

/// Pause a schedule (the reconciler removes its firing task within one poll).
/// Resets `last_fired_at` to NULL (disables the next automatic backfill).
pub async fn pause_schedule(ctx: &Arc<DbosContext>, name: &str) -> Result<(), DbosError> {
    set_status(ctx, name, STATUS_PAUSED).await
}

/// Resume a paused schedule (the reconciler reinstalls its firing task).
/// Resets `last_fired_at` to NULL via a single UPDATE side effect.
pub async fn resume_schedule(ctx: &Arc<DbosContext>, name: &str) -> Result<(), DbosError> {
    set_status(ctx, name, STATUS_ACTIVE).await
}

async fn set_status(ctx: &Arc<DbosContext>, name: &str, status: &str) -> Result<(), DbosError> {
    if name.is_empty() {
        return Err(DbosError::initialization("schedule_name is required"));
    }
    if ctx.db.get_schedule(name).await?.is_none() {
        return Err(DbosError::initialization(format!("schedule not found: {name}")));
    }
    ctx.db.set_schedule_status(name, status).await
}

/// Delete a schedule (the reconciler removes its firing task within one poll).
pub async fn delete_schedule(ctx: &Arc<DbosContext>, name: &str) -> Result<(), DbosError> {
    if name.is_empty() {
        return Err(DbosError::initialization("schedule_name is required"));
    }
    ctx.db.delete_schedule(name).await
}

/// Fetch a single schedule by exact name, or `None`.
pub async fn get_schedule(
    ctx: &Arc<DbosContext>,
    name: &str,
) -> Result<Option<WorkflowSchedule>, DbosError> {
    Ok(ctx.db.get_schedule(name).await?.map(WorkflowSchedule::from))
}

/// List all schedules.
pub async fn list_schedules(ctx: &Arc<DbosContext>) -> Result<Vec<WorkflowSchedule>, DbosError> {
    Ok(ctx.db.list_schedules().await?.into_iter().map(WorkflowSchedule::from).collect())
}
