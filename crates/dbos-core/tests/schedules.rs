//! Integration tests for DB-backed (dynamic) schedules.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use dbos::db::ListWorkflowsInput;
use dbos::{Config, CreateScheduleOptions, DbosContext, DbosError, ScheduledWorkflowInput, WfCtx};
use futures::future::BoxFuture;

/// Build an unlaunched context with a short reconciler poll interval so dynamic
/// schedules are picked up quickly in tests.
async fn ctx_fast_reconciler(app: &str) -> Arc<DbosContext> {
    let url = match std::env::var("DBOS_TEST_DATABASE_URL") {
        Ok(base) if !base.is_empty() => {
            // Reuse the common harness's per-test Postgres provisioning.
            common::shared_db_url().await
        }
        _ => "sqlite::memory:".to_string(),
    };
    dbos::new_context(Config {
        app_name: app.into(),
        database_url: Some(url),
        scheduler_polling_interval: Some(Duration::from_millis(150)),
        ..Default::default()
    })
    .await
    .expect("new_context")
}

/// A scheduled workflow that bumps `counter` on each tick and records the
/// scheduled time.
fn counting_scheduled(
    counter: Arc<AtomicUsize>,
) -> impl Fn(WfCtx, ScheduledWorkflowInput) -> BoxFuture<'static, Result<i64, DbosError>>
+ Clone
+ Send
+ Sync
+ 'static {
    move |ctx: WfCtx, input: ScheduledWorkflowInput| {
        let counter = counter.clone();
        Box::pin(async move {
            let c = counter.clone();
            ctx.run_step("tick", move |_s| {
                let c = c.clone();
                async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    Ok::<i64, DbosError>(input.scheduled_time.timestamp())
                }
            })
            .await
        })
    }
}

#[tokio::test]
async fn dynamic_create_fires_then_delete_stops() {
    let ctx = ctx_fast_reconciler("dyn-create").await;
    let count = Arc::new(AtomicUsize::new(0));
    dbos::register_workflow::<ScheduledWorkflowInput, i64, _, _>(
        &ctx,
        "dyn_ticker",
        counting_scheduled(count.clone()),
    )
    .unwrap();
    ctx.launch().await.unwrap();

    let id = dbos::create_schedule(
        &ctx,
        CreateScheduleOptions {
            schedule_name: "every_second".into(),
            workflow_name: "dyn_ticker".into(),
            schedule: "* * * * * *".into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert!(!id.is_empty());

    // Reconciler installs it within a poll; it then fires each second.
    tokio::time::sleep(Duration::from_millis(2600)).await;
    let fired = count.load(Ordering::SeqCst);
    assert!((2..=4).contains(&fired), "expected ~2-3 fires, got {fired}");

    // Deterministic, schedule-prefixed ids on the internal queue.
    let rows = ctx
        .system_database()
        .list_workflows(ListWorkflowsInput {
            workflow_name: vec!["dyn_ticker".into()],
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(rows.len() >= 2);
    for r in &rows {
        assert!(
            r.id.starts_with("sched-every_second-"),
            "unexpected id {}",
            r.id
        );
        assert_eq!(r.queue_name, "_dbos_internal_queue");
    }

    // Delete -> reconciler removes the firing task; the count stops growing.
    dbos::delete_schedule(&ctx, "every_second").await.unwrap();
    tokio::time::sleep(Duration::from_millis(400)).await; // let the reconciler/firing task settle
    let after_delete = count.load(Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let later = count.load(Ordering::SeqCst);
    assert!(
        later <= after_delete + 1,
        "schedule kept firing after delete: {after_delete} -> {later}"
    );

    assert!(
        dbos::get_schedule(&ctx, "every_second")
            .await
            .unwrap()
            .is_none()
    );
    ctx.shutdown(Duration::from_secs(5)).await;
}

#[tokio::test]
async fn pause_stops_resume_restarts() {
    let ctx = ctx_fast_reconciler("dyn-pause").await;
    let count = Arc::new(AtomicUsize::new(0));
    dbos::register_workflow::<ScheduledWorkflowInput, i64, _, _>(
        &ctx,
        "pause_ticker",
        counting_scheduled(count.clone()),
    )
    .unwrap();
    ctx.launch().await.unwrap();

    dbos::create_schedule(
        &ctx,
        CreateScheduleOptions {
            schedule_name: "pausable".into(),
            workflow_name: "pause_ticker".into(),
            schedule: "* * * * * *".into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    tokio::time::sleep(Duration::from_millis(1600)).await;
    assert!(count.load(Ordering::SeqCst) >= 1);

    // Pause -> firing stops.
    dbos::pause_schedule(&ctx, "pausable").await.unwrap();
    tokio::time::sleep(Duration::from_millis(400)).await;
    let paused_at = count.load(Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let still = count.load(Ordering::SeqCst);
    assert!(
        still <= paused_at + 1,
        "kept firing while paused: {paused_at} -> {still}"
    );

    let sched = dbos::get_schedule(&ctx, "pausable").await.unwrap().unwrap();
    assert_eq!(sched.status, "PAUSED");

    // Resume -> firing restarts.
    dbos::resume_schedule(&ctx, "pausable").await.unwrap();
    tokio::time::sleep(Duration::from_millis(1800)).await;
    let resumed = count.load(Ordering::SeqCst);
    assert!(
        resumed > still,
        "did not resume firing: {still} -> {resumed}"
    );

    ctx.shutdown(Duration::from_secs(5)).await;
}

#[tokio::test]
async fn get_and_list_and_validation() {
    let ctx = ctx_fast_reconciler("dyn-getlist").await;
    dbos::register_workflow::<ScheduledWorkflowInput, i64, _, _>(
        &ctx,
        "gl_wf",
        counting_scheduled(Arc::new(AtomicUsize::new(0))),
    )
    .unwrap();
    ctx.launch().await.unwrap();

    // Invalid cron is rejected before any DB write.
    let err = dbos::create_schedule(
        &ctx,
        CreateScheduleOptions {
            schedule_name: "bad".into(),
            workflow_name: "gl_wf".into(),
            schedule: "not a cron".into(),
            ..Default::default()
        },
    )
    .await
    .unwrap_err();
    assert!(err.is_code(dbos::DbosErrorCode::Initialization));
    assert!(dbos::get_schedule(&ctx, "bad").await.unwrap().is_none());

    // Create two and list them.
    for name in ["s_a", "s_b"] {
        dbos::create_schedule(
            &ctx,
            CreateScheduleOptions {
                schedule_name: name.into(),
                workflow_name: "gl_wf".into(),
                schedule: "0 0 * * * *".into(), // hourly: won't fire during the test
                context: Some(serde_json::json!({"who": name})),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    }

    let all = dbos::list_schedules(&ctx).await.unwrap();
    assert_eq!(all.len(), 2);

    let a = dbos::get_schedule(&ctx, "s_a").await.unwrap().unwrap();
    assert_eq!(a.workflow_name, "gl_wf");
    assert_eq!(a.status, "ACTIVE");
    assert_eq!(a.context, serde_json::json!({"who": "s_a"}));

    // Duplicate name -> conflict.
    let dup = dbos::create_schedule(
        &ctx,
        CreateScheduleOptions {
            schedule_name: "s_a".into(),
            workflow_name: "gl_wf".into(),
            schedule: "0 0 * * * *".into(),
            ..Default::default()
        },
    )
    .await
    .unwrap_err();
    assert!(dup.is_code(dbos::DbosErrorCode::ConflictingId));

    // Pausing/deleting a missing schedule errors.
    assert!(dbos::pause_schedule(&ctx, "nope").await.is_err());

    ctx.shutdown(Duration::from_secs(5)).await;
}
