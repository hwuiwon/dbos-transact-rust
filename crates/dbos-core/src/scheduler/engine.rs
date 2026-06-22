//! The per-schedule firing engine.
//!
//! Computes the next fire time from the cron schedule, sleeps until then, and
//! enqueues the workflow with a deterministic id so the same interval is never
//! run twice (idempotent insert on the deterministic id).
//!
//! Two firing paths share this module:
//! * the STATIC path ([`run_schedule`]) — one registered cron workflow installed
//!   at launch, input is the raw scheduled `DateTime<Utc>`, no DB row.
//! * the DYNAMIC path ([`run_db_schedule`] driven by [`run_reconciler`]) — rows
//!   in `workflow_schedules`, input is a [`ScheduledWorkflowInput`], with
//!   `last_fired_at` tracking and automatic backfill.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, Utc};
use tokio_util::sync::CancellationToken;

use crate::constants::DBOS_INTERNAL_QUEUE_NAME;
use crate::context::DbosContext;
use crate::db::ScheduleRow;
use crate::scheduler::{ScheduledWorkflowInput, parse_cron};
use crate::serialization::{self, resolve_encoder};
use crate::workflow::RunFlags;
use crate::workflow::RunOptions;
use crate::workflow::run::run_workflow_erased;

/// Drive a single cron schedule until `token` is cancelled.
pub(crate) async fn run_schedule(
    ctx: Arc<DbosContext>,
    token: CancellationToken,
    name: String,
    cron_expr: String,
) {
    let schedule = match parse_cron(&cron_expr) {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(workflow = %name, error = %e, "invalid cron schedule; scheduler not started");
            return;
        }
    };

    loop {
        let now = Utc::now();
        let Some(next) = schedule.after(&now).next() else {
            break;
        };
        let wait = (next - now).to_std().unwrap_or(Duration::ZERO);
        tokio::select! {
            biased;
            _ = token.cancelled() => break,
            _ = tokio::time::sleep(wait) => {}
        }
        if token.is_cancelled() {
            break;
        }
        fire(&ctx, &name, next).await;
    }
}

async fn fire(ctx: &Arc<DbosContext>, name: &str, scheduled_time: chrono::DateTime<Utc>) {
    let workflow_id = format!("sched-{name}-{}", scheduled_time.to_rfc3339());
    let encoder = resolve_encoder(false, ctx.serializer.as_ref());
    let encoded = match serialization::encode(encoder.as_ref(), &scheduled_time) {
        Ok(e) => e,
        Err(e) => {
            tracing::error!(workflow = %name, error = %e, "failed to encode scheduled time");
            return;
        }
    };
    let opts = RunOptions {
        workflow_id: Some(workflow_id),
        queue: Some(DBOS_INTERNAL_QUEUE_NAME.to_string()),
        ..Default::default()
    };
    if let Err(e) = run_workflow_erased(
        ctx,
        name,
        encoded,
        encoder.name().to_string(),
        opts,
        RunFlags::default(),
        None,
    )
    .await
    {
        tracing::error!(workflow = %name, error = %e, "failed to enqueue scheduled workflow");
    }
}

// --- dynamic (DB-backed) schedules ---

/// Tracks the firing task for each installed dynamic schedule so the reconciler
/// can cancel and replace them as the `workflow_schedules` table changes.
///
/// Keyed by `schedule_name`. The stored `schedule_id` lets the reconciler detect
/// a *replaced* schedule (same name, new id) and reinstall it.
#[derive(Default)]
pub(crate) struct Scheduler {
    installed: Mutex<HashMap<String, Installed>>,
}

struct Installed {
    schedule_id: String,
    token: CancellationToken,
}

impl Scheduler {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    fn installed_id(&self, name: &str) -> Option<String> {
        self.installed
            .lock()
            .unwrap()
            .get(name)
            .map(|i| i.schedule_id.clone())
    }

    fn is_installed(&self, name: &str) -> bool {
        self.installed.lock().unwrap().contains_key(name)
    }

    fn insert(&self, name: String, schedule_id: String, token: CancellationToken) {
        self.installed
            .lock()
            .unwrap()
            .insert(name, Installed { schedule_id, token });
    }

    fn remove(&self, name: &str) {
        if let Some(installed) = self.installed.lock().unwrap().remove(name) {
            installed.token.cancel();
        }
    }

    fn installed_names(&self) -> Vec<String> {
        self.installed.lock().unwrap().keys().cloned().collect()
    }

    /// Cancel every installed firing task (shutdown / final cleanup).
    pub(crate) fn cancel_all(&self) {
        let mut map = self.installed.lock().unwrap();
        for (_, installed) in map.drain() {
            installed.token.cancel();
        }
    }
}

/// The reconciler loop. Runs immediately on launch, then every
/// `config.scheduler_polling_interval`, reconciling the in-memory firing tasks
/// with the `workflow_schedules` table.
pub(crate) async fn run_reconciler(ctx: Arc<DbosContext>, token: CancellationToken) {
    let interval = ctx.config.scheduler_polling_interval;
    loop {
        if ctx.is_launched() {
            reconcile(&ctx, &token).await;
        }
        tokio::select! {
            biased;
            _ = token.cancelled() => {
                ctx.scheduler.cancel_all();
                break;
            }
            _ = tokio::time::sleep(interval) => {}
        }
    }
}

/// One reconcile pass: remove stale (deleted/paused/replaced) entries, then add
/// new ACTIVE entries (with optional automatic backfill on (re)install).
async fn reconcile(ctx: &Arc<DbosContext>, token: &CancellationToken) {
    let schedules = match ctx.db.list_schedules().await {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!(error = %e, "failed to list schedules for reconciler");
            return;
        }
    };
    let current: HashMap<String, ScheduleRow> = schedules
        .into_iter()
        .map(|s| (s.schedule_name.clone(), s))
        .collect();

    // Phase 1: remove installed entries that are deleted, paused, or replaced.
    for name in ctx.scheduler.installed_names() {
        let stale = match current.get(&name) {
            None => true,
            Some(s) => {
                s.status != "ACTIVE"
                    || ctx.scheduler.installed_id(&name).as_deref() != Some(&s.schedule_id)
            }
        };
        if stale {
            ctx.scheduler.remove(&name);
            tracing::debug!(schedule = %name, "removed schedule from scheduler");
        }
    }

    // Phase 2: add new ACTIVE entries.
    for (name, row) in &current {
        if row.status != "ACTIVE" {
            continue;
        }
        if ctx.scheduler.is_installed(name) {
            continue;
        }

        // Automatic backfill of missed ticks since last fire, exactly at (re)install.
        if row.automatic_backfill {
            if let Some(last) = row.last_fired_at.as_deref().and_then(parse_rfc3339) {
                let start = last + chrono::Duration::seconds(1);
                let end = Utc::now();
                if start < end {
                    tracing::info!(schedule = %name, "performing automatic backfill");
                    if let Err(e) = backfill(ctx, row, start, end).await {
                        tracing::warn!(schedule = %name, error = %e, "automatic backfill failed");
                    }
                }
            }
        }

        let child = token.child_token();
        ctx.scheduler
            .insert(name.clone(), row.schedule_id.clone(), child.clone());
        ctx.tracker
            .spawn(run_db_schedule(ctx.clone(), child, row.clone()));
        tracing::debug!(schedule = %name, "added schedule to scheduler");
    }
}

/// Drive a single DB-backed schedule until `token` is cancelled: compute the
/// next cron tick, sleep, enqueue a deterministic run, update `last_fired_at`.
pub(crate) async fn run_db_schedule(
    ctx: Arc<DbosContext>,
    token: CancellationToken,
    row: ScheduleRow,
) {
    let schedule = match parse_cron(&row.schedule) {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(schedule = %row.schedule_name, error = %e, "invalid cron schedule; not firing");
            return;
        }
    };
    if row
        .cron_timezone
        .as_deref()
        .is_some_and(|tz| !tz.is_empty())
    {
        // Named-timezone cron requires a tz database (chrono-tz), which is not a
        // dependency. Ticks are computed in UTC; document the limitation rather
        // than silently misfire.
        tracing::warn!(
            schedule = %row.schedule_name,
            timezone = %row.cron_timezone.as_deref().unwrap_or_default(),
            "cron_timezone is set but named-timezone scheduling is not supported; computing ticks in UTC"
        );
    }

    loop {
        let now = Utc::now();
        let Some(next) = schedule.after(&now).next() else {
            break;
        };
        let wait = (next - now).to_std().unwrap_or(Duration::ZERO);
        tokio::select! {
            biased;
            _ = token.cancelled() => break,
            _ = tokio::time::sleep(wait) => {}
        }
        if token.is_cancelled() {
            break;
        }
        fire_db(&ctx, &row, next).await;
    }
}

async fn fire_db(ctx: &Arc<DbosContext>, row: &ScheduleRow, scheduled_time: DateTime<Utc>) {
    let workflow_id = format!(
        "sched-{}-{}",
        row.schedule_name,
        scheduled_time.to_rfc3339()
    );
    let queue = row
        .queue_name
        .clone()
        .filter(|q| !q.is_empty())
        .unwrap_or_else(|| DBOS_INTERNAL_QUEUE_NAME.to_string());

    let encoder = resolve_encoder(false, ctx.serializer.as_ref());
    let input = ScheduledWorkflowInput {
        scheduled_time,
        context: decode_context(&row.context),
    };
    let encoded = match serialization::encode(encoder.as_ref(), &input) {
        Ok(e) => e,
        Err(e) => {
            tracing::error!(schedule = %row.schedule_name, error = %e, "failed to encode scheduled input");
            return;
        }
    };

    // Pin to the latest registered application version so a stale executor does
    // not pick up newly enqueued ticks (best effort).
    let application_version = match ctx.db.get_latest_application_version().await {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(schedule = %row.schedule_name, error = %e, "failed to read latest application version");
            None
        }
    };

    let opts = RunOptions {
        workflow_id: Some(workflow_id),
        queue: Some(queue),
        application_version,
        ..Default::default()
    };
    if let Err(e) = run_workflow_erased(
        ctx,
        &row.workflow_name,
        encoded,
        encoder.name().to_string(),
        opts,
        RunFlags::default(),
        None,
    )
    .await
    {
        tracing::error!(schedule = %row.schedule_name, error = %e, "failed to enqueue scheduled workflow");
        return;
    }

    // Best-effort last-fired update (advisory metadata only).
    let stamp = scheduled_time.to_rfc3339();
    if let Err(e) = ctx
        .db
        .update_schedule_last_fired(&row.schedule_name, &stamp)
        .await
    {
        tracing::warn!(schedule = %row.schedule_name, error = %e, "failed to update schedule last fired time");
    }
}

/// Enqueue every cron tick in `(start, end)` that has not already been enqueued,
/// using the same deterministic `sched-<name>-<RFC3339>` id (idempotent).
async fn backfill(
    ctx: &Arc<DbosContext>,
    row: &ScheduleRow,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<(), crate::error::DbosError> {
    let schedule = parse_cron(&row.schedule).map_err(|e| {
        crate::error::DbosError::initialization(format!("invalid cron schedule: {e}"))
    })?;
    let queue = row
        .queue_name
        .clone()
        .filter(|q| !q.is_empty())
        .unwrap_or_else(|| DBOS_INTERNAL_QUEUE_NAME.to_string());
    let encoder = resolve_encoder(false, ctx.serializer.as_ref());
    let application_version = ctx.db.get_latest_application_version().await.ok().flatten();

    for next in schedule.after(&start) {
        if next >= end {
            break;
        }
        let workflow_id = format!("sched-{}-{}", row.schedule_name, next.to_rfc3339());
        // Skip ticks already enqueued/run.
        if ctx.db.get_workflow_status(&workflow_id).await?.is_some() {
            continue;
        }
        let input = ScheduledWorkflowInput {
            scheduled_time: next,
            context: decode_context(&row.context),
        };
        let encoded = serialization::encode(encoder.as_ref(), &input)?;
        let opts = RunOptions {
            workflow_id: Some(workflow_id),
            queue: Some(queue.clone()),
            application_version: application_version.clone(),
            ..Default::default()
        };
        run_workflow_erased(
            ctx,
            &row.workflow_name,
            encoded,
            encoder.name().to_string(),
            opts,
            RunFlags::default(),
            None,
        )
        .await?;
    }
    Ok(())
}

/// Decode the stored JSON `context` string into a value (or JSON `null`).
fn decode_context(raw: &str) -> serde_json::Value {
    serde_json::from_str(raw).unwrap_or(serde_json::Value::Null)
}

/// Parse an RFC3339 (or RFC3339Nano) string into a UTC datetime.
fn parse_rfc3339(s: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|d| d.with_timezone(&Utc))
}
