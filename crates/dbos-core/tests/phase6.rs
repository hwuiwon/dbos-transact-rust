//! Phase 6 exit tests: management (cancel/resume/fork/delete/list/steps) + client.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use common::{counting_two_step, new_ctx, new_ctx_from_url, shared_db_url};
use dbos::{
    Client, ClientConfig, DbosError, EnqueueOptions, ForkOptions, ListWorkflowsInput, QueueOptions,
    RunOptions, WfCtx, WorkflowStatusType,
};

async fn cancellable(ctx: WfCtx, _: ()) -> Result<i32, DbosError> {
    ctx.run_step("s1", |_s| async move { Ok::<i32, DbosError>(1) })
        .await?;
    ctx.sleep(Duration::from_millis(500)).await?;
    // After a cancel, this step's check sees status CANCELLED and aborts.
    ctx.run_step("s2", |_s| async move { Ok::<i32, DbosError>(2) })
        .await
}

#[tokio::test]
async fn cancel_aborts_at_next_step() {
    let ctx = new_ctx("p6-cancel").await;
    dbos::register_workflow::<(), i32, _, _>(&ctx, "cancellable", cancellable).unwrap();
    ctx.launch().await.unwrap();

    let h = dbos::run_workflow::<(), i32>(
        &ctx,
        "cancellable",
        (),
        RunOptions {
            workflow_id: Some("cancel-1".into()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    // Cancel while it is sleeping between s1 and s2.
    tokio::time::sleep(Duration::from_millis(100)).await;
    dbos::cancel_workflow(&ctx, "cancel-1").await.unwrap();

    let err = h.get_result().await.unwrap_err();
    assert!(
        err.is_code(dbos::DbosErrorCode::WorkflowCancelled),
        "got {err}"
    );
    let st = ctx
        .system_database()
        .get_workflow_status("cancel-1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(st.status, WorkflowStatusType::Cancelled);

    ctx.shutdown(Duration::from_secs(5)).await;
}

#[tokio::test]
async fn resume_reenqueues_and_completes() {
    let ctx = new_ctx("p6-resume").await;
    dbos::register_workflow::<(), i32, _, _>(&ctx, "cancellable", cancellable).unwrap();
    ctx.launch().await.unwrap();

    let h = dbos::run_workflow::<(), i32>(
        &ctx,
        "cancellable",
        (),
        RunOptions {
            workflow_id: Some("resume-1".into()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    dbos::cancel_workflow(&ctx, "resume-1").await.unwrap();
    assert!(h.get_result().await.is_err());

    // Resume: re-enqueued on the internal queue, steps replayed, runs to success.
    let rh = dbos::resume_workflow::<i32>(&ctx, "resume-1")
        .await
        .unwrap();
    assert_eq!(rh.get_result().await.unwrap(), 2);

    ctx.shutdown(Duration::from_secs(5)).await;
}

#[tokio::test]
async fn fork_copies_prior_steps() {
    let counter = Arc::new(AtomicUsize::new(0));
    let ctx = new_ctx("p6-fork").await;
    dbos::register_workflow::<i32, i32, _, _>(&ctx, "two_step", counting_two_step(counter.clone()))
        .unwrap();
    ctx.launch().await.unwrap();

    let h = dbos::run_workflow::<i32, i32>(
        &ctx,
        "two_step",
        10,
        RunOptions {
            workflow_id: Some("fork-orig".into()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(h.get_result().await.unwrap(), 12);
    assert_eq!(counter.load(Ordering::SeqCst), 2);

    // Fork from step 1: step 0 is copied (not re-run), step 1 re-runs.
    let fh = dbos::fork_workflow::<i32>(
        &ctx,
        ForkOptions {
            original_workflow_id: "fork-orig".into(),
            start_step: 1,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(fh.get_result().await.unwrap(), 12);
    // Only step 1 re-ran -> counter incremented by exactly 1.
    assert_eq!(counter.load(Ordering::SeqCst), 3);

    let orig = ctx
        .system_database()
        .get_workflow_status("fork-orig")
        .await
        .unwrap()
        .unwrap();
    assert!(orig.was_forked_from);
    ctx.shutdown(Duration::from_secs(5)).await;
}

#[tokio::test]
async fn delete_removes_workflow() {
    let counter = Arc::new(AtomicUsize::new(0));
    let ctx = new_ctx("p6-delete").await;
    dbos::register_workflow::<i32, i32, _, _>(&ctx, "two_step", counting_two_step(counter))
        .unwrap();
    ctx.launch().await.unwrap();

    let h = dbos::run_workflow::<i32, i32>(
        &ctx,
        "two_step",
        1,
        RunOptions {
            workflow_id: Some("del-1".into()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    h.get_result().await.unwrap();
    assert!(
        ctx.system_database()
            .get_workflow_status("del-1")
            .await
            .unwrap()
            .is_some()
    );

    dbos::delete_workflow(&ctx, "del-1", false).await.unwrap();
    assert!(
        ctx.system_database()
            .get_workflow_status("del-1")
            .await
            .unwrap()
            .is_none()
    );

    ctx.shutdown(Duration::from_secs(5)).await;
}

#[tokio::test]
async fn list_workflows_and_steps() {
    let counter = Arc::new(AtomicUsize::new(0));
    let ctx = new_ctx("p6-list").await;
    dbos::register_workflow::<i32, i32, _, _>(&ctx, "two_step", counting_two_step(counter))
        .unwrap();
    ctx.launch().await.unwrap();

    for i in 0..3 {
        let h = dbos::run_workflow::<i32, i32>(
            &ctx,
            "two_step",
            i,
            RunOptions {
                workflow_id: Some(format!("list-{i}")),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        h.get_result().await.unwrap();
    }

    let all = dbos::list_workflows(
        &ctx,
        ListWorkflowsInput {
            workflow_name: vec!["two_step".into()],
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(all.len(), 3);
    assert!(all.iter().all(|w| w.status == WorkflowStatusType::Success));

    let steps = dbos::get_workflow_steps(&ctx, "list-0").await.unwrap();
    assert_eq!(steps.len(), 2);
    assert_eq!(steps[0].step_name, "step_one");
    assert_eq!(steps[1].step_name, "step_two");

    ctx.shutdown(Duration::from_secs(5)).await;
}

#[tokio::test]
async fn client_enqueue_to_server() {
    let url = shared_db_url().await;

    // Server: registers the workflow + runs the queue runner.
    let server = new_ctx_from_url("p6-client-server", &url).await;
    async fn doubler(ctx: WfCtx, x: i32) -> Result<i32, DbosError> {
        ctx.run_step(
            "double",
            move |_s| async move { Ok::<i32, DbosError>(x * 2) },
        )
        .await
    }
    dbos::register_queue(&server, "work", QueueOptions::default()).unwrap();
    dbos::register_workflow::<i32, i32, _, _>(&server, "doubler", doubler).unwrap();
    server.launch().await.unwrap();

    // Client: connects to the same DB, enqueues without registering anything.
    let client = Client::new(ClientConfig {
        app_name: "p6-client".into(),
        database_url: url.clone(),
        ..Default::default()
    })
    .await
    .unwrap();

    let handle = client
        .enqueue::<i32, i32>("work", "doubler", 21, EnqueueOptions::default())
        .await
        .unwrap();
    assert_eq!(handle.get_result().await.unwrap(), 42);

    client.shutdown(Duration::from_secs(2)).await;
    server.shutdown(Duration::from_secs(5)).await;
}
