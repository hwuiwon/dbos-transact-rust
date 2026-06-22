//! Phase 5 exit tests: durable scheduling (cron).

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use common::new_ctx;
use dbos::db::ListWorkflowsInput;
use dbos::{DbosError, ScheduledTime, WfCtx};
use futures::future::BoxFuture;

#[tokio::test]
async fn scheduled_workflow_fires_each_second() {
    let ctx = new_ctx("p5-sched").await;
    let count = Arc::new(AtomicUsize::new(0));
    let c = count.clone();
    let wf = move |ctx: WfCtx, scheduled: ScheduledTime| {
        let c = c.clone();
        Box::pin(async move {
            let c2 = c.clone();
            ctx.run_step("tick", move |_s| {
                let c = c2.clone();
                async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    Ok::<i64, DbosError>(scheduled.timestamp())
                }
            })
            .await
        }) as BoxFuture<'static, Result<i64, DbosError>>
    };
    dbos::register_scheduled_workflow::<i64, _, _>(&ctx, "ticker", "* * * * * *", wf).unwrap();
    ctx.launch().await.unwrap();

    tokio::time::sleep(Duration::from_millis(2600)).await;
    let n = count.load(Ordering::SeqCst);
    assert!((2..=4).contains(&n), "expected ~2-3 fires in 2.6s, got {n}");

    // Each fire produced a distinct, deterministic `sched-ticker-<time>` id.
    let rows = ctx
        .system_database()
        .list_workflows(ListWorkflowsInput {
            workflow_name: vec!["ticker".into()],
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(rows.len() >= 2);
    for r in &rows {
        assert!(r.id.starts_with("sched-ticker-"), "unexpected id {}", r.id);
        assert_eq!(r.queue_name, "_dbos_internal_queue");
    }

    ctx.shutdown(Duration::from_secs(5)).await;
}

#[tokio::test]
async fn invalid_cron_is_rejected() {
    let ctx = new_ctx("p5-bad-cron").await;
    let wf = |ctx: WfCtx, _t: ScheduledTime| {
        Box::pin(async move {
            ctx.run_step("noop", |_s| async move { Ok::<i64, DbosError>(0) })
                .await
        }) as BoxFuture<'static, Result<i64, DbosError>>
    };
    let err =
        dbos::register_scheduled_workflow::<i64, _, _>(&ctx, "bad", "not a cron", wf).unwrap_err();
    assert!(err.is_code(dbos::DbosErrorCode::Initialization));
}

#[tokio::test]
async fn five_field_cron_is_accepted() {
    let ctx = new_ctx("p5-5field").await;
    let wf = |ctx: WfCtx, _t: ScheduledTime| {
        Box::pin(async move {
            ctx.run_step("noop", |_s| async move { Ok::<i64, DbosError>(0) })
                .await
        }) as BoxFuture<'static, Result<i64, DbosError>>
    };
    // Standard 5-field crontab (every minute) is normalized to 6-field.
    dbos::register_scheduled_workflow::<i64, _, _>(&ctx, "every_min", "* * * * *", wf).unwrap();
    ctx.launch().await.unwrap();
    ctx.shutdown(Duration::from_secs(2)).await;
}
