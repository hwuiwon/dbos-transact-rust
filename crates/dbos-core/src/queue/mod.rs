//! Durable queues — registration and the dequeue runner.
//!
//! A queue caps how many workflows run concurrently
//! (globally and per-executor), optionally rate-limits dispatch, and supports
//! priority ordering. Enqueue happens via [`crate::RunOptions::queue`]; the
//! background runner ([`runner`]) claims `ENQUEUED` workflows and dispatches them.

pub mod runner;

use std::sync::Arc;
use std::time::Duration;

use crate::context::DbosContext;
use crate::error::DbosError;

/// Rate-limit configuration: at most `limit` workflow starts per `period`.
#[derive(Debug, Clone)]
pub struct RateLimiter {
    pub limit: i64,
    pub period: Duration,
}

/// A registered queue's configuration.
#[derive(Debug, Clone)]
pub struct WorkflowQueue {
    pub name: String,
    pub worker_concurrency: Option<i64>,
    pub global_concurrency: Option<i64>,
    pub priority_enabled: bool,
    pub rate_limit: Option<RateLimiter>,
    pub max_tasks_per_iteration: i64,
}

/// Options for [`register_queue`].
#[derive(Debug, Clone, Default)]
pub struct QueueOptions {
    /// Max concurrent workflows per executor.
    pub worker_concurrency: Option<i64>,
    /// Max concurrent workflows across all executors.
    pub global_concurrency: Option<i64>,
    /// Enable priority-based ordering (lower number = higher priority).
    pub priority_enabled: bool,
    /// Rate-limit dispatch.
    pub rate_limit: Option<RateLimiter>,
    /// Max workflows to dequeue per iteration (default 100).
    pub max_tasks_per_iteration: Option<i64>,
}

/// Register a queue (must be done before [`DbosContext::launch`]).
pub fn register_queue(
    ctx: &Arc<DbosContext>,
    name: &str,
    opts: QueueOptions,
) -> Result<(), DbosError> {
    if ctx.is_launched() {
        return Err(DbosError::initialization("cannot register a queue after the context has been launched"));
    }
    let queue = WorkflowQueue {
        name: name.to_string(),
        worker_concurrency: opts.worker_concurrency,
        global_concurrency: opts.global_concurrency,
        priority_enabled: opts.priority_enabled,
        rate_limit: opts.rate_limit,
        max_tasks_per_iteration: opts.max_tasks_per_iteration.unwrap_or(100),
    };
    let mut queues = ctx.queues.write().unwrap();
    if queues.contains_key(name) {
        return Err(DbosError::conflicting_registration(name));
    }
    queues.insert(name.to_string(), queue);
    Ok(())
}

/// Register the internal queue used by the scheduler/debouncer (idempotent).
pub(crate) fn register_internal_queue(ctx: &Arc<DbosContext>) {
    let mut queues = ctx.queues.write().unwrap();
    queues
        .entry(crate::constants::DBOS_INTERNAL_QUEUE_NAME.to_string())
        .or_insert_with(|| WorkflowQueue {
            name: crate::constants::DBOS_INTERNAL_QUEUE_NAME.to_string(),
            worker_concurrency: None,
            global_concurrency: None,
            priority_enabled: false,
            rate_limit: None,
            max_tasks_per_iteration: 100,
        });
}
