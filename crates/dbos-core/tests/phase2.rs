//! Phase 2 exit tests: child workflows, durable sleep, step retry, determinism.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use common::{new_ctx, new_ctx_with_executor};
use dbos::{DbosError, DbosErrorCode, RunOptions, StepOptions, WfCtx};
use futures::future::BoxFuture;

// --- child workflows ---

async fn child_wf(ctx: WfCtx, x: i32) -> Result<i32, DbosError> {
    ctx.run_step(
        "double",
        move |_s| async move { Ok::<i32, DbosError>(x * 2) },
    )
    .await
}

async fn parent_wf(ctx: WfCtx, x: i32) -> Result<i32, DbosError> {
    let handle = ctx
        .run_child_workflow::<i32, i32>("child_wf", x, RunOptions::default())
        .await?;
    let r = handle.get_result().await?;
    Ok(r + 1)
}

#[tokio::test]
async fn child_workflow_runs_and_links_to_parent() {
    let ctx = new_ctx("p2-child").await;
    dbos::register_workflow::<i32, i32, _, _>(&ctx, "child_wf", child_wf).unwrap();
    dbos::register_workflow::<i32, i32, _, _>(&ctx, "parent_wf", parent_wf).unwrap();
    ctx.launch().await.unwrap();

    let h = dbos::run_workflow::<i32, i32>(
        &ctx,
        "parent_wf",
        5,
        RunOptions {
            workflow_id: Some("parent-1".into()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(h.get_result().await.unwrap(), 11); // (5*2)+1

    // Deterministic child id `{parent}-{stepID}` with stepID 0.
    let child = ctx
        .system_database()
        .get_workflow_status("parent-1-0")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(child.name, "child_wf");
    assert_eq!(child.parent_workflow_id, "parent-1");

    ctx.shutdown(Duration::from_secs(5)).await;
}

#[tokio::test]
async fn child_workflow_replayed_on_recovery() {
    let runs = Arc::new(AtomicUsize::new(0));
    let ctx = new_ctx_with_executor("p2-child-rec", "local").await;

    // child counts its body executions
    let r = runs.clone();
    let child = move |ctx: WfCtx, x: i32| {
        let r = r.clone();
        Box::pin(async move {
            let r2 = r.clone();
            ctx.run_step("double", move |_s| async move {
                r2.fetch_add(1, Ordering::SeqCst);
                Ok::<i32, DbosError>(x * 2)
            })
            .await
        }) as BoxFuture<'static, Result<i32, DbosError>>
    };
    dbos::register_workflow::<i32, i32, _, _>(&ctx, "child_wf", child).unwrap();
    dbos::register_workflow::<i32, i32, _, _>(&ctx, "parent_wf", parent_wf).unwrap();
    ctx.launch().await.unwrap();

    let h = dbos::run_workflow::<i32, i32>(
        &ctx,
        "parent_wf",
        7,
        RunOptions {
            workflow_id: Some("p-rec".into()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(h.get_result().await.unwrap(), 15);
    assert_eq!(runs.load(Ordering::SeqCst), 1);

    // Crash the parent and recover: child must be replayed, not re-executed.
    ctx.system_database()
        .set_workflow_status_pending("p-rec")
        .await
        .unwrap();
    let handles = dbos::recover_pending_workflows(&ctx, &["local"])
        .await
        .unwrap();
    // Only the parent is PENDING (child already SUCCESS).
    assert_eq!(handles.len(), 1);
    assert_eq!(
        handles[0].get_result().await.unwrap(),
        serde_json::json!(15)
    );
    assert_eq!(runs.load(Ordering::SeqCst), 1);

    ctx.shutdown(Duration::from_secs(5)).await;
}

// --- durable sleep ---

// Returns the remaining duration the call actually waited (full on first run,
// ~0 on replay because the absolute wake time has already passed).
async fn sleeper(ctx: WfCtx, ms: i64) -> Result<i64, DbosError> {
    let remaining = ctx.sleep(Duration::from_millis(ms as u64)).await?;
    Ok(remaining.as_millis() as i64)
}

#[tokio::test]
async fn durable_sleep_replays_remaining_time() {
    let ctx = new_ctx_with_executor("p2-sleep", "local").await;
    dbos::register_workflow::<i64, i64, _, _>(&ctx, "sleeper", sleeper).unwrap();
    ctx.launch().await.unwrap();

    let started = Instant::now();
    let h = dbos::run_workflow::<i64, i64>(
        &ctx,
        "sleeper",
        300,
        RunOptions {
            workflow_id: Some("sleep-1".into()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let first = h.get_result().await.unwrap();
    // The first run waits ~the full duration in wall-clock time. (The *returned*
    // remaining can be less than the duration if recording the wake time was slow,
    // e.g. on a remote database, so we assert on elapsed wall-clock, not `first`.)
    assert!(
        first > 0,
        "first run should report a positive remaining, got {first}"
    );
    assert!(
        started.elapsed() >= Duration::from_millis(250),
        "first run should sleep ~full duration"
    );

    // After completion the wake time is in the past; recovery replays sleep as ~0.
    ctx.system_database()
        .set_workflow_status_pending("sleep-1")
        .await
        .unwrap();
    let handles = dbos::recover_pending_workflows(&ctx, &["local"])
        .await
        .unwrap();
    let recovered = handles[0].get_result().await.unwrap().as_i64().unwrap();
    assert!(
        recovered <= 50,
        "recovery should replay sleep as ~0, got {recovered}"
    );

    ctx.shutdown(Duration::from_secs(5)).await;
}

// --- step retry ---

fn flaky_step_wf(
    attempts: Arc<AtomicUsize>,
    max_retries: i64,
    predicate_false: bool,
) -> impl Fn(WfCtx, i32) -> BoxFuture<'static, Result<i32, DbosError>> + Clone + Send + Sync + 'static
{
    move |ctx: WfCtx, fail_until: i32| {
        let attempts = attempts.clone();
        Box::pin(async move {
            let a = attempts.clone();
            let mut opts = StepOptions {
                max_retries,
                base_interval: Duration::from_millis(1),
                max_interval: Duration::from_millis(5),
                ..Default::default()
            };
            if predicate_false {
                opts.retry_predicate = Some(Arc::new(|_e: &DbosError| false));
            }
            ctx.run_step_opts("flaky", opts, move |_s| {
                let a = a.clone();
                async move {
                    let n = a.fetch_add(1, Ordering::SeqCst) as i32;
                    if n < fail_until {
                        Err(DbosError::new(
                            DbosErrorCode::StepExecution,
                            format!("fail {n}"),
                        ))
                    } else {
                        Ok::<i32, DbosError>(n)
                    }
                }
            })
            .await
        })
    }
}

#[tokio::test]
async fn step_retries_until_success() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let ctx = new_ctx("p2-retry").await;
    dbos::register_workflow::<i32, i32, _, _>(
        &ctx,
        "flaky",
        flaky_step_wf(attempts.clone(), 5, false),
    )
    .unwrap();
    ctx.launch().await.unwrap();

    // Fails for n in {0,1,2}, succeeds at n=3 -> 4 body runs, result 3.
    let h = dbos::run_workflow::<i32, i32>(&ctx, "flaky", 3, RunOptions::default())
        .await
        .unwrap();
    assert_eq!(h.get_result().await.unwrap(), 3);
    assert_eq!(attempts.load(Ordering::SeqCst), 4);
    ctx.shutdown(Duration::from_secs(5)).await;
}

#[tokio::test]
async fn step_retries_exhausted() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let ctx = new_ctx("p2-retry-exhaust").await;
    dbos::register_workflow::<i32, i32, _, _>(
        &ctx,
        "flaky",
        flaky_step_wf(attempts.clone(), 2, false),
    )
    .unwrap();
    ctx.launch().await.unwrap();

    // Always fails (fail_until huge); max_retries=2 -> 3 runs then exceeded.
    let h = dbos::run_workflow::<i32, i32>(&ctx, "flaky", 999, RunOptions::default())
        .await
        .unwrap();
    let err = h.get_result().await.unwrap_err();
    assert!(
        err.message.contains("maximum of 2 retries"),
        "got: {}",
        err.message
    );
    assert_eq!(attempts.load(Ordering::SeqCst), 3);
    ctx.shutdown(Duration::from_secs(5)).await;
}

#[tokio::test]
async fn retry_predicate_stops_early() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let ctx = new_ctx("p2-retry-pred").await;
    dbos::register_workflow::<i32, i32, _, _>(
        &ctx,
        "flaky",
        flaky_step_wf(attempts.clone(), 999, true),
    )
    .unwrap();
    ctx.launch().await.unwrap();

    let h = dbos::run_workflow::<i32, i32>(&ctx, "flaky", 999, RunOptions::default())
        .await
        .unwrap();
    assert!(h.get_result().await.is_err());
    // Predicate returns false after the first failure -> only one run.
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    ctx.shutdown(Duration::from_secs(5)).await;
}

// --- determinism (UnexpectedStep) ---

#[tokio::test]
async fn nondeterministic_step_name_is_detected() {
    let flipped = Arc::new(AtomicBool::new(false));
    let ctx = new_ctx_with_executor("p2-determinism", "local").await;

    let f = flipped.clone();
    let wf = move |ctx: WfCtx, _: ()| {
        let f = f.clone();
        Box::pin(async move {
            let first = !f.swap(true, Ordering::SeqCst);
            let name = if first { "step_a" } else { "step_b" };
            ctx.run_step(name, |_s| async move { Ok::<i32, DbosError>(1) })
                .await
        }) as BoxFuture<'static, Result<i32, DbosError>>
    };
    dbos::register_workflow::<(), i32, _, _>(&ctx, "wf", wf).unwrap();
    ctx.launch().await.unwrap();

    let h = dbos::run_workflow::<(), i32>(
        &ctx,
        "wf",
        (),
        RunOptions {
            workflow_id: Some("det-1".into()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(h.get_result().await.unwrap(), 1);

    // Recover: the body now names the step differently at the same step id.
    ctx.system_database()
        .set_workflow_status_pending("det-1")
        .await
        .unwrap();
    let handles = dbos::recover_pending_workflows(&ctx, &["local"])
        .await
        .unwrap();
    let err = handles[0].get_result().await.unwrap_err();
    assert!(
        err.message.contains("was recorded when") || err.message.contains("deterministic"),
        "got: {}",
        err.message
    );

    ctx.shutdown(Duration::from_secs(5)).await;
}
