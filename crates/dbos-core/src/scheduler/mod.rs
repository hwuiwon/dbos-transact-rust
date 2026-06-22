//! Durable scheduling — run a workflow on a cron schedule.
//!
//! The static [`register_scheduled_workflow`] path: a scheduled
//! workflow takes the scheduled time as input and is enqueued on the internal
//! queue at each interval with a deterministic id (`sched-{name}-{RFC3339}`),
//! giving exactly-once-per-interval semantics even across restarts/executors.

pub mod dynamic;
pub mod engine;

use std::future::Future;
use std::str::FromStr;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use cron::Schedule;
use serde::{Deserialize, Serialize};

use crate::context::{DbosContext, WfCtx};
use crate::error::DbosError;
use crate::workflow::{RegisterOptions, register_workflow_opts};

pub use dynamic::{
    CreateScheduleOptions, WorkflowSchedule, create_schedule, delete_schedule, get_schedule,
    list_schedules, pause_schedule, resume_schedule,
};

/// The input passed to a scheduled workflow: the time the run was scheduled for.
pub type ScheduledTime = DateTime<Utc>;

/// The input handed to a DB-backed (dynamic) scheduled workflow on each tick.
///
/// A DB-backed scheduled workflow is registered taking this struct as its input
/// (unlike a statically [`register_scheduled_workflow`]-registered workflow,
/// which takes the raw [`ScheduledTime`]).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduledWorkflowInput {
    /// The cron tick time this run was scheduled for.
    pub scheduled_time: DateTime<Utc>,
    /// The schedule's user context (JSON `null` if none was set).
    #[serde(default)]
    pub context: serde_json::Value,
}

/// Register a workflow to run on a cron `schedule`. The workflow receives the
/// scheduled time as its input.
///
/// The cron expression may be standard 5-field (`min hour dom mon dow`) or
/// 6-field with leading seconds (`sec min hour dom mon dow`).
pub fn register_scheduled_workflow<R, F, Fut>(
    ctx: &Arc<DbosContext>,
    name: &str,
    schedule: &str,
    f: F,
) -> Result<(), DbosError>
where
    R: Serialize + Send + 'static,
    F: Fn(WfCtx, ScheduledTime) -> Fut + Send + Sync + Clone + 'static,
    Fut: Future<Output = Result<R, DbosError>> + Send + 'static,
{
    parse_cron(schedule).map_err(|e| {
        DbosError::initialization(format!("invalid cron schedule {schedule:?}: {e}"))
    })?;
    register_workflow_opts(
        ctx,
        name,
        f,
        RegisterOptions {
            schedule: Some(schedule.to_string()),
            ..Default::default()
        },
    )
}

/// Parse a cron expression, normalizing a 5-field crontab to the 6-field form
/// (leading seconds) expected by the `cron` crate.
pub(crate) fn parse_cron(expr: &str) -> Result<Schedule, cron::error::Error> {
    let trimmed = expr.trim();
    let field_count = trimmed.split_whitespace().count();
    let normalized = if field_count == 5 {
        format!("0 {trimmed}")
    } else {
        trimmed.to_string()
    };
    Schedule::from_str(&normalized)
}
