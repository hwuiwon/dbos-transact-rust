//! Workflow timeout tests (Phase 3): a timed-out workflow is cancelled at its
//! next step boundary.

mod common;

use std::time::Duration;

use common::new_ctx;
use dbos::{DbosError, RunOptions, WfCtx};

async fn slow_wf(ctx: WfCtx, _: ()) -> Result<i32, DbosError> {
    ctx.run_step("s1", |_s| async move { Ok::<i32, DbosError>(1) })
        .await?;
    ctx.sleep(Duration::from_millis(500)).await?;
    // By now the timeout has fired and cancelled the workflow; this step aborts.
    ctx.run_step("s2", |_s| async move { Ok::<i32, DbosError>(2) })
        .await
}

#[tokio::test]
async fn workflow_timeout_cancels_at_next_step() {
    let ctx = new_ctx("timeout").await;
    dbos::register_workflow::<(), i32, _, _>(&ctx, "slow_wf", slow_wf).unwrap();
    ctx.launch().await.unwrap();

    let h = dbos::run_workflow::<(), i32>(
        &ctx,
        "slow_wf",
        (),
        RunOptions {
            workflow_id: Some("to-1".into()),
            timeout: Some(Duration::from_millis(150)),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let err = h.get_result().await.unwrap_err();
    assert!(
        err.is_code(dbos::DbosErrorCode::WorkflowCancelled),
        "got {err}"
    );
    let st = ctx
        .system_database()
        .get_workflow_status("to-1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(st.status, dbos::WorkflowStatusType::Cancelled);

    ctx.shutdown(Duration::from_secs(5)).await;
}

#[tokio::test]
async fn workflow_without_timeout_completes() {
    let ctx = new_ctx("no-timeout").await;
    dbos::register_workflow::<(), i32, _, _>(&ctx, "slow_wf", slow_wf).unwrap();
    ctx.launch().await.unwrap();
    // No timeout -> runs to completion.
    let h = dbos::run_workflow::<(), i32>(&ctx, "slow_wf", (), RunOptions::default())
        .await
        .unwrap();
    assert_eq!(h.get_result().await.unwrap(), 2);
    ctx.shutdown(Duration::from_secs(5)).await;
}
