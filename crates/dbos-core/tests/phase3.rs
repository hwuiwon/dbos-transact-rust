//! Phase 3 exit tests: durable notifications (send/recv) and events (set/get).

mod common;

use std::time::Duration;

use common::{new_ctx, new_ctx_with_executor};
use dbos::{DbosError, RunOptions, WfCtx};

async fn receiver(ctx: WfCtx, _: ()) -> Result<String, DbosError> {
    ctx.recv::<String>("topic", Duration::from_secs(5)).await
}

async fn receiver_short(ctx: WfCtx, _: ()) -> Result<String, DbosError> {
    ctx.recv::<String>("topic", Duration::from_millis(150))
        .await
}

#[tokio::test]
async fn send_from_outside_recv_in_workflow() {
    let ctx = new_ctx("p3-sendrecv").await;
    dbos::register_workflow::<(), String, _, _>(&ctx, "receiver", receiver).unwrap();
    ctx.launch().await.unwrap();

    let h = dbos::run_workflow::<(), String>(
        &ctx,
        "receiver",
        (),
        RunOptions {
            workflow_id: Some("recv-1".into()),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    // From outside the workflow, deliver a message.
    dbos::send(&ctx, "recv-1", "hello".to_string(), "topic")
        .await
        .unwrap();
    assert_eq!(h.get_result().await.unwrap(), "hello");

    ctx.shutdown(Duration::from_secs(5)).await;
}

#[tokio::test]
async fn recv_times_out() {
    let ctx = new_ctx("p3-recv-timeout").await;
    dbos::register_workflow::<(), String, _, _>(&ctx, "receiver_short", receiver_short).unwrap();
    ctx.launch().await.unwrap();

    let h = dbos::run_workflow::<(), String>(&ctx, "receiver_short", (), RunOptions::default())
        .await
        .unwrap();
    let err = h.get_result().await.unwrap_err();
    assert!(err.is_code(dbos::DbosErrorCode::Timeout), "got {err}");

    ctx.shutdown(Duration::from_secs(5)).await;
}

#[tokio::test]
async fn send_to_nonexistent_workflow_fails() {
    let ctx = new_ctx("p3-send-nonexistent").await;
    ctx.launch().await.unwrap();
    let err = dbos::send(&ctx, "does-not-exist", "x".to_string(), "t")
        .await
        .unwrap_err();
    assert!(
        err.is_code(dbos::DbosErrorCode::NonExistentWorkflow),
        "got {err}"
    );
    ctx.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test]
async fn recv_consumes_fifo_order() {
    let ctx = new_ctx("p3-fifo").await;

    async fn recv_two(ctx: WfCtx, _: ()) -> Result<Vec<String>, DbosError> {
        let a = ctx.recv::<String>("t", Duration::from_secs(5)).await?;
        let b = ctx.recv::<String>("t", Duration::from_secs(5)).await?;
        Ok(vec![a, b])
    }
    dbos::register_workflow::<(), Vec<String>, _, _>(&ctx, "recv_two", recv_two).unwrap();
    ctx.launch().await.unwrap();

    let h = dbos::run_workflow::<(), Vec<String>>(
        &ctx,
        "recv_two",
        (),
        RunOptions {
            workflow_id: Some("fifo-1".into()),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    dbos::send(&ctx, "fifo-1", "first".to_string(), "t")
        .await
        .unwrap();
    dbos::send(&ctx, "fifo-1", "second".to_string(), "t")
        .await
        .unwrap();
    assert_eq!(h.get_result().await.unwrap(), vec!["first", "second"]);

    ctx.shutdown(Duration::from_secs(5)).await;
}

#[tokio::test]
async fn recv_result_replayed_on_recovery() {
    let ctx = new_ctx_with_executor("p3-recv-recovery", "local").await;
    dbos::register_workflow::<(), String, _, _>(&ctx, "receiver", receiver).unwrap();
    ctx.launch().await.unwrap();

    let h = dbos::run_workflow::<(), String>(
        &ctx,
        "receiver",
        (),
        RunOptions {
            workflow_id: Some("recv-rec".into()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    dbos::send(&ctx, "recv-rec", "once".to_string(), "topic")
        .await
        .unwrap();
    assert_eq!(h.get_result().await.unwrap(), "once");

    // Recover: recv replays the recorded message (no second consume).
    ctx.system_database()
        .set_workflow_status_pending("recv-rec")
        .await
        .unwrap();
    let handles = dbos::recover_pending_workflows(&ctx, &["local"])
        .await
        .unwrap();
    assert_eq!(
        handles[0].get_result().await.unwrap(),
        serde_json::json!("once")
    );

    ctx.shutdown(Duration::from_secs(5)).await;
}

async fn setter(ctx: WfCtx, val: String) -> Result<(), DbosError> {
    ctx.set_event("status", val).await
}

#[tokio::test]
async fn set_event_then_get_from_outside() {
    let ctx = new_ctx("p3-setget").await;
    dbos::register_workflow::<String, (), _, _>(&ctx, "setter", setter).unwrap();
    ctx.launch().await.unwrap();

    let h = dbos::run_workflow::<String, ()>(
        &ctx,
        "setter",
        "complete".to_string(),
        RunOptions {
            workflow_id: Some("set-1".into()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    h.get_result().await.unwrap();

    let got: Option<String> = dbos::get_event(&ctx, "set-1", "status", Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(got, Some("complete".to_string()));

    ctx.shutdown(Duration::from_secs(5)).await;
}

#[tokio::test]
async fn get_event_times_out_to_none() {
    let ctx = new_ctx("p3-getevent-timeout").await;
    ctx.launch().await.unwrap();
    let got: Option<String> = dbos::get_event(&ctx, "no-such-wf", "k", Duration::from_millis(150))
        .await
        .unwrap();
    assert_eq!(got, None);
    ctx.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test]
async fn get_event_in_workflow_waits_for_setter() {
    let ctx = new_ctx("p3-getevent-wait").await;

    async fn waiter(ctx: WfCtx, target: String) -> Result<String, DbosError> {
        ctx.get_event::<Option<String>>(&target, "status", Duration::from_secs(5))
            .await
            .map(|o| o.unwrap_or_default())
    }
    dbos::register_workflow::<String, (), _, _>(&ctx, "setter", setter).unwrap();
    dbos::register_workflow::<String, String, _, _>(&ctx, "waiter", waiter).unwrap();
    ctx.launch().await.unwrap();

    // Waiter starts first and blocks on the not-yet-set event.
    let wh = dbos::run_workflow::<String, String>(
        &ctx,
        "waiter",
        "set-2".to_string(),
        RunOptions::default(),
    )
    .await
    .unwrap();

    // Setter creates the event.
    let sh = dbos::run_workflow::<String, ()>(
        &ctx,
        "setter",
        "ready".to_string(),
        RunOptions {
            workflow_id: Some("set-2".into()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    sh.get_result().await.unwrap();

    assert_eq!(wh.get_result().await.unwrap(), "ready");

    ctx.shutdown(Duration::from_secs(5)).await;
}
