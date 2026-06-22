//! Integration tests for the conductor WebSocket client.
//!
//! Each test stands up a mock conductor server (a `tokio-tungstenite` acceptor
//! on an ephemeral port), starts the real conductor pointed at it against an
//! in-memory `dbos-core` context, sends request frames, and asserts the
//! conductor replies with well-formed responses echoing the `request_id`. The
//! reconnect path is exercised by dropping the first connection and asserting a
//! second connection is established.

use std::sync::Arc;
use std::time::Duration;

use dbos::{Config, DbosContext, DbosError, RunOptions, WfCtx};
use futures_util::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;

/// A trivial one-step workflow returning its input + 1.
async fn increment(ctx: WfCtx, n: i32) -> Result<i32, DbosError> {
    ctx.run_step("inc", |_step| async move { Ok(n + 1) }).await
}

/// Build one in-memory context, register the workflow, launch, and run it once
/// so there is a known terminal workflow to list/inspect.
async fn setup() -> (Arc<DbosContext>, String) {
    let ctx = dbos::new_context(Config {
        app_name: "conductor-test".into(),
        database_url: Some("sqlite::memory:".into()),
        ..Default::default()
    })
    .await
    .expect("build context");

    dbos::register_workflow::<i32, i32, _, _>(&ctx, "increment", increment).expect("register");
    // Register a named queue so the observability `list_queues` handler has a
    // known, non-internal entry to surface.
    dbos::register_queue(&ctx, "test-queue", dbos::QueueOptions::default()).expect("register queue");
    ctx.launch().await.expect("launch");

    let opts = RunOptions {
        workflow_id: Some("known-wf-1".into()),
        ..Default::default()
    };
    let handle = dbos::run_workflow::<i32, i32>(&ctx, "increment", 41, opts)
        .await
        .expect("run");
    assert_eq!(handle.get_result().await.expect("result"), 42);

    (ctx, "known-wf-1".to_string())
}

/// Bind an ephemeral TCP listener and return it plus its `ws://host:port` base
/// URL (no path; the conductor appends `/websocket/<app>/<key>`).
async fn bind_mock() -> (TcpListener, String) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    (listener, format!("ws://{addr}"))
}

/// Accept one WebSocket connection and complete the handshake.
async fn accept_ws(listener: &TcpListener) -> WebSocketStream<TcpStream> {
    let (stream, _) = listener.accept().await.expect("accept");
    tokio_tungstenite::accept_async(stream)
        .await
        .expect("ws handshake")
}

#[tokio::test]
async fn executor_info_and_list_workflows_round_trip() {
    let (ctx, id) = setup().await;
    let (listener, base_url) = bind_mock().await;

    let conductor =
        dbos_server::start_conductor(ctx.clone(), base_url, "app".into(), "key".into()).await;

    // Accept the conductor's outbound connection.
    let mut server = accept_ws(&listener).await;

    // 1) executor_info.
    server
        .send(Message::Text(
            r#"{"type":"executor_info","request_id":"req-exec"}"#.into(),
        ))
        .await
        .expect("send executor_info");

    let resp = next_text(&mut server).await;
    let v: serde_json::Value = serde_json::from_str(&resp).expect("json");
    assert_eq!(v["type"], "executor_info");
    assert_eq!(v["request_id"], "req-exec");
    assert_eq!(v["language"], "rust");
    assert!(v["executor_id"].is_string());
    assert!(v.get("error_message").is_none());

    // 2) list_workflows — should include the known terminal workflow.
    server
        .send(Message::Text(
            r#"{"type":"list_workflows","request_id":"req-list","body":{"load_output":true}}"#
                .into(),
        ))
        .await
        .expect("send list_workflows");

    let resp = next_text(&mut server).await;
    let v: serde_json::Value = serde_json::from_str(&resp).expect("json");
    assert_eq!(v["type"], "list_workflows");
    assert_eq!(v["request_id"], "req-list");
    let output = v["output"].as_array().expect("output array");
    let found = output
        .iter()
        .find(|w| w["WorkflowUUID"] == serde_json::json!(id))
        .expect("known workflow present");
    assert_eq!(found["Status"], serde_json::json!("SUCCESS"));
    assert_eq!(found["WorkflowName"], serde_json::json!("increment"));
    // Epoch-ms as a string per the conductor wire contract.
    assert!(found["CreatedAt"].is_string());

    conductor.shutdown().await;
    ctx.shutdown(Duration::from_secs(5)).await;
}

#[tokio::test]
async fn delete_round_trip() {
    let (ctx, id) = setup().await;
    let (listener, base_url) = bind_mock().await;

    let conductor =
        dbos_server::start_conductor(ctx.clone(), base_url, "app".into(), "key".into()).await;
    let mut server = accept_ws(&listener).await;

    // Delete the known terminal workflow.
    server
        .send(Message::Text(
            format!(r#"{{"type":"delete","request_id":"req-del","workflow_id":"{id}"}}"#).into(),
        ))
        .await
        .expect("send delete");

    let resp = next_text(&mut server).await;
    let v: serde_json::Value = serde_json::from_str(&resp).expect("json");
    assert_eq!(v["type"], "delete");
    assert_eq!(v["request_id"], "req-del");
    assert_eq!(v["success"], serde_json::json!(true));
    assert!(v.get("error_message").is_none());

    // The workflow should no longer be listed.
    server
        .send(Message::Text(
            r#"{"type":"list_workflows","request_id":"req-list2","body":{}}"#.into(),
        ))
        .await
        .expect("send list_workflows");
    let resp = next_text(&mut server).await;
    let v: serde_json::Value = serde_json::from_str(&resp).expect("json");
    let output = v["output"].as_array().expect("output array");
    assert!(
        !output.iter().any(|w| w["WorkflowUUID"] == serde_json::json!(id)),
        "deleted workflow still listed",
    );

    conductor.shutdown().await;
    ctx.shutdown(Duration::from_secs(5)).await;
}

#[tokio::test]
async fn list_queues_round_trip() {
    let (ctx, _id) = setup().await;
    let (listener, base_url) = bind_mock().await;

    let conductor =
        dbos_server::start_conductor(ctx.clone(), base_url, "app".into(), "key".into()).await;
    let mut server = accept_ws(&listener).await;

    server
        .send(Message::Text(
            r#"{"type":"list_queues","request_id":"req-queues"}"#.into(),
        ))
        .await
        .expect("send list_queues");

    let resp = next_text(&mut server).await;
    let v: serde_json::Value = serde_json::from_str(&resp).expect("json");
    assert_eq!(v["type"], "list_queues");
    assert_eq!(v["request_id"], "req-queues");
    assert!(v.get("error_message").is_none());
    let output = v["output"].as_array().expect("output array");
    let found = output
        .iter()
        .find(|q| q["name"] == serde_json::json!("test-queue"))
        .expect("registered queue present");
    assert_eq!(found["priority_enabled"], serde_json::json!(false));
    assert!(found["polling_interval_sec"].is_number());

    conductor.shutdown().await;
    ctx.shutdown(Duration::from_secs(5)).await;
}

#[tokio::test]
async fn unknown_message_type_gets_error_response() {
    let (ctx, _id) = setup().await;
    let (listener, base_url) = bind_mock().await;

    let conductor =
        dbos_server::start_conductor(ctx.clone(), base_url, "app".into(), "key".into()).await;
    let mut server = accept_ws(&listener).await;

    server
        .send(Message::Text(
            r#"{"type":"definitely_not_a_command","request_id":"req-x"}"#.into(),
        ))
        .await
        .expect("send");

    let resp = next_text(&mut server).await;
    let v: serde_json::Value = serde_json::from_str(&resp).expect("json");
    assert_eq!(v["type"], "definitely_not_a_command");
    assert_eq!(v["request_id"], "req-x");
    assert_eq!(v["error_message"], serde_json::json!("unsupported command"));

    conductor.shutdown().await;
    ctx.shutdown(Duration::from_secs(5)).await;
}

#[tokio::test]
async fn reconnects_after_connection_drop() {
    let (ctx, _id) = setup().await;
    let (listener, base_url) = bind_mock().await;

    // Drive accepts on a dedicated task so we can observe successive connections.
    let (tx, mut rx) = mpsc::channel::<()>(4);
    let accept_task = tokio::spawn(async move {
        // First connection: accept, then immediately drop it.
        let mut first = accept_ws(&listener).await;
        tx.send(()).await.ok();
        // Drop the socket to force the conductor to reconnect.
        let _ = first.close(None).await;
        drop(first);

        // Second connection: the conductor should reconnect here.
        let mut second = accept_ws(&listener).await;
        tx.send(()).await.ok();
        // Verify the reconnected socket is functional with an executor_info.
        second
            .send(Message::Text(
                r#"{"type":"executor_info","request_id":"after-reconnect"}"#.into(),
            ))
            .await
            .expect("send");
        let resp = next_text(&mut second).await;
        (second, resp)
    });

    let conductor =
        dbos_server::start_conductor(ctx.clone(), base_url, "app".into(), "key".into()).await;

    // First connection observed.
    tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("first connect within timeout")
        .expect("first connect");

    // Second (reconnected) connection observed — backoff starts at 1s.
    tokio::time::timeout(Duration::from_secs(10), rx.recv())
        .await
        .expect("reconnect within timeout")
        .expect("reconnect");

    let (_second, resp) = tokio::time::timeout(Duration::from_secs(5), accept_task)
        .await
        .expect("accept task finishes")
        .expect("accept task ok");

    let v: serde_json::Value = serde_json::from_str(&resp).expect("json");
    assert_eq!(v["type"], "executor_info");
    assert_eq!(v["request_id"], "after-reconnect");

    conductor.shutdown().await;
    ctx.shutdown(Duration::from_secs(5)).await;
}

/// Read frames until a text frame arrives, skipping control frames (ping/pong).
async fn next_text(server: &mut WebSocketStream<TcpStream>) -> String {
    loop {
        match tokio::time::timeout(Duration::from_secs(5), server.next()).await {
            Ok(Some(Ok(Message::Text(t)))) => return t.to_string(),
            Ok(Some(Ok(Message::Ping(_) | Message::Pong(_)))) => continue,
            Ok(Some(Ok(other))) => panic!("unexpected frame: {other:?}"),
            Ok(Some(Err(e))) => panic!("ws error: {e}"),
            Ok(None) => panic!("stream ended before a text frame"),
            Err(_) => panic!("timed out waiting for a text frame"),
        }
    }
}
