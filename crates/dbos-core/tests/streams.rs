//! Stream tests (Phase 3): durable append-only streams with a close sentinel.

mod common;

use std::time::Duration;

use common::{new_ctx, new_ctx_with_executor};
use dbos::{DbosError, RunOptions, WfCtx};

async fn producer(ctx: WfCtx, n: i32) -> Result<i32, DbosError> {
    for i in 0..n {
        ctx.write_stream("nums", i).await?;
    }
    ctx.close_stream("nums").await?;
    Ok(n)
}

#[tokio::test]
async fn write_read_and_close_stream() {
    let ctx = new_ctx("streams").await;
    dbos::register_workflow::<i32, i32, _, _>(&ctx, "producer", producer).unwrap();
    ctx.launch().await.unwrap();

    let h = dbos::run_workflow::<i32, i32>(
        &ctx,
        "producer",
        5,
        RunOptions { workflow_id: Some("stream-1".into()), ..Default::default() },
    )
    .await
    .unwrap();
    assert_eq!(h.get_result().await.unwrap(), 5);

    let (values, closed): (Vec<i32>, bool) =
        dbos::read_stream(&ctx, "stream-1", "nums").await.unwrap();
    assert_eq!(values, vec![0, 1, 2, 3, 4]);
    assert!(closed);

    ctx.shutdown(Duration::from_secs(5)).await;
}

#[tokio::test]
async fn stream_writes_are_idempotent_on_recovery() {
    let ctx = new_ctx_with_executor("streams-rec", "local").await;
    dbos::register_workflow::<i32, i32, _, _>(&ctx, "producer", producer).unwrap();
    ctx.launch().await.unwrap();

    let h = dbos::run_workflow::<i32, i32>(
        &ctx,
        "producer",
        3,
        RunOptions { workflow_id: Some("stream-rec".into()), ..Default::default() },
    )
    .await
    .unwrap();
    assert_eq!(h.get_result().await.unwrap(), 3);

    // Recover: stream writes are memoized steps, so no duplicate entries appear.
    ctx.system_database().set_workflow_status_pending("stream-rec").await.unwrap();
    let handles = dbos::recover_pending_workflows(&ctx, &["local"]).await.unwrap();
    assert_eq!(handles[0].get_result().await.unwrap(), serde_json::json!(3));

    let (values, closed): (Vec<i32>, bool) =
        dbos::read_stream(&ctx, "stream-rec", "nums").await.unwrap();
    assert_eq!(values, vec![0, 1, 2], "recovery must not duplicate stream entries");
    assert!(closed);

    ctx.shutdown(Duration::from_secs(5)).await;
}

#[tokio::test]
async fn writing_to_closed_stream_fails() {
    let ctx = new_ctx("streams-closed").await;
    async fn bad(ctx: WfCtx, _: ()) -> Result<i32, DbosError> {
        ctx.write_stream("s", 1).await?;
        ctx.close_stream("s").await?;
        // Writing after close must error.
        match ctx.write_stream("s", 2).await {
            Err(_) => Ok(1),
            Ok(()) => Ok(0),
        }
    }
    dbos::register_workflow::<(), i32, _, _>(&ctx, "bad", bad).unwrap();
    ctx.launch().await.unwrap();
    let h = dbos::run_workflow::<(), i32>(&ctx, "bad", (), RunOptions::default()).await.unwrap();
    assert_eq!(h.get_result().await.unwrap(), 1);
    ctx.shutdown(Duration::from_secs(5)).await;
}
