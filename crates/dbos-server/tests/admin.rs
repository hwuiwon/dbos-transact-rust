//! Integration tests for the admin HTTP server.
//!
//! Each test builds a single in-memory SQLite DBOS context (shared with the
//! server task), registers + launches a trivial workflow, runs it, starts the
//! admin server on an ephemeral port, and drives it over HTTP with `reqwest`.

use std::sync::Arc;
use std::time::Duration;

use dbos::{Config, DbosContext, DbosError, RunOptions, WfCtx};

/// A trivial one-step workflow returning its input + 1.
async fn increment(ctx: WfCtx, n: i32) -> Result<i32, DbosError> {
    ctx.run_step("inc", |_step| async move { Ok(n + 1) }).await
}

/// Build one in-memory context, register the workflow, launch, and run it once.
/// Returns the context and the known workflow id.
async fn setup() -> (Arc<DbosContext>, String) {
    let ctx = dbos::new_context(Config {
        app_name: "admin-test".into(),
        database_url: Some("sqlite::memory:".into()),
        ..Default::default()
    })
    .await
    .expect("build context");

    dbos::register_workflow::<i32, i32, _, _>(&ctx, "increment", increment).expect("register");
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

#[tokio::test]
async fn healthz_returns_healthy() {
    let (ctx, _id) = setup().await;
    let server = dbos_server::start_admin_server(ctx.clone(), 0)
        .await
        .expect("start server");
    let base = format!("http://{}", server.local_addr());

    let resp = reqwest::get(format!("{base}/dbos-healthz"))
        .await
        .expect("request");
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.text().await.unwrap(), "healthy");

    server.shutdown().await;
    ctx.shutdown(Duration::from_secs(5)).await;
}

#[tokio::test]
async fn post_workflows_lists_known_workflow() {
    let (ctx, id) = setup().await;
    let server = dbos_server::start_admin_server(ctx.clone(), 0)
        .await
        .expect("start server");
    let base = format!("http://{}", server.local_addr());

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base}/workflows"))
        .json(&serde_json::json!({ "load_output": true }))
        .send()
        .await
        .expect("request");
    assert_eq!(resp.status(), 200);

    let body: serde_json::Value = resp.json().await.expect("json");
    let arr = body.as_array().expect("array");
    assert!(!arr.is_empty(), "expected at least one workflow");

    let found = arr
        .iter()
        .find(|w| w["WorkflowUUID"] == serde_json::json!(id))
        .expect("known workflow present");
    // PascalCase keys + the string-epoch-ms transform.
    assert_eq!(found["Status"], serde_json::json!("SUCCESS"));
    assert_eq!(found["WorkflowName"], serde_json::json!("increment"));
    assert!(
        found["CreatedAt"].is_string(),
        "CreatedAt should be a string epoch-ms"
    );

    server.shutdown().await;
    ctx.shutdown(Duration::from_secs(5)).await;
}

#[tokio::test]
async fn get_workflow_by_id_and_404() {
    let (ctx, id) = setup().await;
    let server = dbos_server::start_admin_server(ctx.clone(), 0)
        .await
        .expect("start server");
    let base = format!("http://{}", server.local_addr());

    // Known id -> 200 with the transformed body.
    let resp = reqwest::get(format!("{base}/workflows/{id}"))
        .await
        .expect("request");
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.expect("json");
    assert_eq!(body["WorkflowUUID"], serde_json::json!(id));
    assert_eq!(body["Status"], serde_json::json!("SUCCESS"));

    // Unknown id -> 404.
    let resp = reqwest::get(format!("{base}/workflows/does-not-exist"))
        .await
        .expect("request");
    assert_eq!(resp.status(), 404);

    server.shutdown().await;
    ctx.shutdown(Duration::from_secs(5)).await;
}

#[tokio::test]
async fn get_workflow_steps_returns_step() {
    let (ctx, id) = setup().await;
    let server = dbos_server::start_admin_server(ctx.clone(), 0)
        .await
        .expect("start server");
    let base = format!("http://{}", server.local_addr());

    let resp = reqwest::get(format!("{base}/workflows/{id}/steps"))
        .await
        .expect("request");
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.expect("json");
    let arr = body.as_array().expect("array");
    assert!(!arr.is_empty(), "expected at least one step");
    assert_eq!(arr[0]["function_name"], serde_json::json!("inc"));

    server.shutdown().await;
    ctx.shutdown(Duration::from_secs(5)).await;
}
