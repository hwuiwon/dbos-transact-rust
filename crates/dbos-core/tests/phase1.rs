//! Phase 1 exit tests: context + SQLite system DB + memoized steps + recovery.
//! All run against in-process SQLite with no external services.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use common::{counting_two_step, new_ctx, new_ctx_with_executor};
use dbos::{Config, DbosError, RunOptions, WfCtx, WorkflowStatusType};

#[tokio::test]
async fn register_run_one_step_workflow_succeeds() {
    let counter = Arc::new(AtomicUsize::new(0));
    let ctx = new_ctx("p1-basic").await;
    dbos::register_workflow::<i32, i32, _, _>(
        &ctx,
        "two_step_wf",
        counting_two_step(counter.clone()),
    )
    .unwrap();
    ctx.launch().await.unwrap();

    let handle = dbos::run_workflow::<i32, i32>(&ctx, "two_step_wf", 10, RunOptions::default())
        .await
        .unwrap();
    let result = handle.get_result().await.unwrap();
    assert_eq!(result, 12);

    let status = handle.get_status().await.unwrap();
    assert_eq!(status.status, WorkflowStatusType::Success);
    assert_eq!(status.attempts, 1);
    assert_eq!(counter.load(Ordering::SeqCst), 2);

    ctx.shutdown(Duration::from_secs(5)).await;
}

#[tokio::test]
async fn executed_only_once_with_fixed_id() {
    let counter = Arc::new(AtomicUsize::new(0));
    let ctx = new_ctx("p1-once").await;
    dbos::register_workflow::<i32, i32, _, _>(
        &ctx,
        "two_step_wf",
        counting_two_step(counter.clone()),
    )
    .unwrap();
    ctx.launch().await.unwrap();

    let opts = || RunOptions {
        workflow_id: Some("fixed-id".into()),
        ..Default::default()
    };

    let h1 = dbos::run_workflow::<i32, i32>(&ctx, "two_step_wf", 100, opts())
        .await
        .unwrap();
    assert_eq!(h1.get_result().await.unwrap(), 102);

    // A second run with the same id attaches to the terminal workflow.
    let h2 = dbos::run_workflow::<i32, i32>(&ctx, "two_step_wf", 100, opts())
        .await
        .unwrap();
    assert_eq!(h2.get_result().await.unwrap(), 102);

    let status = h2.get_status().await.unwrap();
    assert_eq!(status.attempts, 1);
    // Steps ran exactly once across both invocations.
    assert_eq!(counter.load(Ordering::SeqCst), 2);

    ctx.shutdown(Duration::from_secs(5)).await;
}

#[tokio::test]
async fn recovery_replays_steps_and_increments_attempts() {
    let counter = Arc::new(AtomicUsize::new(0));
    let ctx = new_ctx_with_executor("p1-recovery", "local").await;
    dbos::register_workflow::<i32, i32, _, _>(
        &ctx,
        "two_step_wf",
        counting_two_step(counter.clone()),
    )
    .unwrap();
    ctx.launch().await.unwrap();

    let h = dbos::run_workflow::<i32, i32>(
        &ctx,
        "two_step_wf",
        10,
        RunOptions {
            workflow_id: Some("wf-rec".into()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(h.get_result().await.unwrap(), 12);
    assert_eq!(counter.load(Ordering::SeqCst), 2);

    // Simulate a crash: flip the completed workflow back to PENDING.
    ctx.system_database()
        .set_workflow_status_pending("wf-rec")
        .await
        .unwrap();

    // Recover: the body re-runs but every step is replayed from operation_outputs.
    let handles = dbos::recover_pending_workflows(&ctx, &["local"])
        .await
        .unwrap();
    assert_eq!(handles.len(), 1);
    let recovered = handles[0].get_result().await.unwrap();
    assert_eq!(recovered, serde_json::json!(12));
    // No step body re-executed.
    assert_eq!(counter.load(Ordering::SeqCst), 2);

    let status = ctx
        .system_database()
        .get_workflow_status("wf-rec")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(status.status, WorkflowStatusType::Success);
    assert_eq!(status.attempts, 2);

    ctx.shutdown(Duration::from_secs(5)).await;
}

#[tokio::test]
async fn workflow_error_is_durable_and_replayed() {
    let ctx = new_ctx("p1-error").await;
    async fn failing_wf(ctx: WfCtx, _: ()) -> Result<i32, DbosError> {
        ctx.run_step("boom", |_s| async move {
            Err::<i32, DbosError>(DbosError::new(
                dbos::DbosErrorCode::StepExecution,
                "kaboom".to_string(),
            ))
        })
        .await
    }
    dbos::register_workflow::<(), i32, _, _>(&ctx, "failing_wf", failing_wf).unwrap();
    ctx.launch().await.unwrap();

    let h = dbos::run_workflow::<(), i32>(&ctx, "failing_wf", (), RunOptions::default())
        .await
        .unwrap();
    let err = h.get_result().await.unwrap_err();
    assert!(err.message.contains("kaboom"));

    let status = h.get_status().await.unwrap();
    assert_eq!(status.status, WorkflowStatusType::Error);

    ctx.shutdown(Duration::from_secs(5)).await;
}

#[tokio::test]
async fn config_validation_and_double_launch() {
    // Missing app name.
    let err = dbos::new_context(Config {
        app_name: "".into(),
        database_url: Some("sqlite::memory:".into()),
        ..Default::default()
    })
    .await
    .err()
    .unwrap();
    assert!(err.is_code(dbos::DbosErrorCode::Initialization));

    // Missing database.
    let err = dbos::new_context(Config {
        app_name: "x".into(),
        ..Default::default()
    })
    .await
    .err()
    .unwrap();
    assert!(err.is_code(dbos::DbosErrorCode::Initialization));

    // Double launch.
    let ctx = new_ctx("p1-doublelaunch").await;
    ctx.launch().await.unwrap();
    let err = ctx.launch().await.unwrap_err();
    assert!(err.is_code(dbos::DbosErrorCode::Initialization));
    ctx.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test]
async fn register_after_launch_is_rejected() {
    let ctx = new_ctx("p1-reg-after-launch").await;
    ctx.launch().await.unwrap();
    let counter = Arc::new(AtomicUsize::new(0));
    let err = dbos::register_workflow::<i32, i32, _, _>(&ctx, "late", counting_two_step(counter))
        .unwrap_err();
    assert!(err.is_code(dbos::DbosErrorCode::Initialization));
    ctx.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test]
async fn duplicate_registration_is_rejected() {
    let ctx = new_ctx("p1-dup-reg").await;
    let counter = Arc::new(AtomicUsize::new(0));
    dbos::register_workflow::<i32, i32, _, _>(&ctx, "dup", counting_two_step(counter.clone()))
        .unwrap();
    let err = dbos::register_workflow::<i32, i32, _, _>(&ctx, "dup", counting_two_step(counter))
        .unwrap_err();
    assert!(err.is_code(dbos::DbosErrorCode::ConflictingRegistration));
}

#[tokio::test]
async fn sqlite_file_url_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("dbos.sqlite");
    let url = format!("sqlite://{}", path.display());
    let ctx = dbos::new_context(Config {
        app_name: "p1-file".into(),
        database_url: Some(url),
        ..Default::default()
    })
    .await
    .unwrap();
    let counter = Arc::new(AtomicUsize::new(0));
    dbos::register_workflow::<i32, i32, _, _>(&ctx, "two_step_wf", counting_two_step(counter))
        .unwrap();
    ctx.launch().await.unwrap();
    let h = dbos::run_workflow::<i32, i32>(&ctx, "two_step_wf", 1, RunOptions::default())
        .await
        .unwrap();
    assert_eq!(h.get_result().await.unwrap(), 3);
    ctx.shutdown(Duration::from_secs(5)).await;
}
