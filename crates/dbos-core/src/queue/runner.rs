//! The queue runner background task.
//!
//! A single
//! supervisor loop promotes delayed workflows, then for each registered queue
//! claims eligible `ENQUEUED` workflows and dispatches them via the erased
//! registry entry (with the dequeue flag set).

use std::sync::Arc;
use std::time::Duration;

use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;

use crate::context::DbosContext;
use crate::db::DequeueInput;
use crate::queue::WorkflowQueue;
use crate::serialization::PORTABLE_SERIALIZER_NAME;
use crate::workflow::RunFlags;
use crate::workflow::run::run_workflow_erased;
use crate::workflow::RunOptions;

/// Run the queue supervisor until `token` is cancelled.
pub(crate) async fn run_queue_runner(ctx: Arc<DbosContext>, token: CancellationToken) {
    // Poll cadence governs dispatch latency only (correctness comes from the
    // DB-level concurrency/rate checks in `dequeue_workflows`).
    let mut interval = tokio::time::interval(Duration::from_millis(100));
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            biased;
            _ = token.cancelled() => break,
            _ = interval.tick() => {}
        }
        if !ctx.is_launched() {
            continue;
        }
        if let Err(e) = ctx.db.transition_delayed_workflows().await {
            tracing::error!(error = %e, "failed to transition delayed workflows");
        }
        let queues: Vec<WorkflowQueue> = ctx.queues.read().unwrap().values().cloned().collect();
        for queue in queues {
            dequeue_and_dispatch(&ctx, &queue).await;
        }
    }
}

async fn dequeue_and_dispatch(ctx: &Arc<DbosContext>, queue: &WorkflowQueue) {
    let local_running = ctx
        .active_workflow_ids
        .iter()
        .filter(|e| e.value().queue_name == queue.name)
        .count() as i64;

    let input = DequeueInput {
        queue_name: queue.name.clone(),
        global_concurrency: queue.global_concurrency,
        worker_concurrency: queue.worker_concurrency,
        rate_limit: queue.rate_limit.as_ref().map(|r| (r.limit, r.period.as_secs_f64())),
        max_tasks: queue.max_tasks_per_iteration,
        local_running_count: local_running,
        application_version: ctx.application_version().to_string(),
        executor_id: ctx.executor_id().to_string(),
    };

    let dequeued = match ctx.db.dequeue_workflows(input).await {
        Ok(d) => d,
        Err(e) => {
            tracing::error!(queue = %queue.name, error = %e, "dequeue failed");
            return;
        }
    };

    for dq in dequeued {
        let opts = RunOptions {
            workflow_id: Some(dq.id.clone()),
            portable: dq.serialization == PORTABLE_SERIALIZER_NAME,
            ..Default::default()
        };
        if let Err(e) = run_workflow_erased(
            ctx,
            &dq.name,
            dq.input,
            dq.serialization,
            opts,
            RunFlags { is_dequeue: true, is_recovery: false },
            None,
        )
        .await
        {
            tracing::error!(workflow_id = %dq.id, error = %e, "failed to dispatch dequeued workflow");
        }
    }
}
