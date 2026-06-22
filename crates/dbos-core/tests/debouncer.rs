//! Integration tests for the debouncer.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use common::new_ctx;
use dbos::{DbosError, Debouncer, WfCtx};
use futures::future::BoxFuture;

/// A target workflow that records how many times it actually ran and returns its
/// input (so the test can assert the *last* input won).
fn recording_target(
    runs: Arc<AtomicUsize>,
) -> impl Fn(WfCtx, String) -> BoxFuture<'static, Result<String, DbosError>>
+ Clone
+ Send
+ Sync
+ 'static {
    move |ctx: WfCtx, input: String| {
        let runs = runs.clone();
        Box::pin(async move {
            let r = runs.clone();
            ctx.run_step("run", move |_s| {
                let r = r.clone();
                async move {
                    r.fetch_add(1, Ordering::SeqCst);
                    Ok::<String, DbosError>(input)
                }
            })
            .await
        })
    }
}

#[tokio::test]
async fn rapid_calls_coalesce_into_one_run() {
    let ctx = new_ctx("deb-coalesce").await;
    let runs = Arc::new(AtomicUsize::new(0));
    dbos::register_workflow::<String, String, _, _>(
        &ctx,
        "deb_target",
        recording_target(runs.clone()),
    )
    .unwrap();

    let deb = Debouncer::<String, String>::new(&ctx, "deb_target", Duration::ZERO).unwrap();
    ctx.launch().await.unwrap();

    // Five rapid calls with the same key. The debounce window (800ms) is well
    // above the inter-call gap (100ms) and any DB latency, so all calls coalesce
    // robustly on both SQLite and (slower) Postgres.
    let mut last_handle = None;
    for i in 0..5 {
        let h = deb
            .debounce(&ctx, "k1", Duration::from_millis(800), format!("v{i}"))
            .await
            .unwrap();
        last_handle = Some(h);
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // After the input settles, the target runs exactly once with the last input.
    let handle = last_handle.unwrap();
    let result = handle.get_result().await.unwrap();
    assert_eq!(result, "v4", "the last input should win");

    // Give any stragglers a moment, then assert exactly-once.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        runs.load(Ordering::SeqCst),
        1,
        "target should run exactly once"
    );

    ctx.shutdown(Duration::from_secs(5)).await;
}

#[tokio::test]
async fn timeout_caps_pushback() {
    let ctx = new_ctx("deb-timeout").await;
    let runs = Arc::new(AtomicUsize::new(0));
    dbos::register_workflow::<String, String, _, _>(
        &ctx,
        "deb_target_to",
        recording_target(runs.clone()),
    )
    .unwrap();

    // Each call asks to push the start back a very long 3s, but the cap is 300ms.
    let deb = Debouncer::<String, String>::new(&ctx, "deb_target_to", Duration::from_millis(300))
        .unwrap();
    ctx.launch().await.unwrap();

    // Three calls all arrive within the 300ms cap window, then stop. Without the
    // cap the target would not start for ~3s; the cap must force it to run within
    // ~300ms of the first call. They coalesce into one run with the last input.
    let started = std::time::Instant::now();
    let mut last = None;
    for i in 0..3 {
        last = Some(
            deb.debounce(&ctx, "kt", Duration::from_millis(3000), format!("v{i}"))
                .await
                .unwrap(),
        );
        tokio::time::sleep(Duration::from_millis(40)).await;
    }
    let result = last.unwrap().get_result().await.unwrap();
    let elapsed = started.elapsed();

    assert_eq!(
        runs.load(Ordering::SeqCst),
        1,
        "calls within the cap window coalesce into one run"
    );
    assert_eq!(result, "v2", "the last input should win");
    // Capped at ~300ms (+ polling), well under the uncapped ~3s push-back.
    assert!(
        elapsed < Duration::from_millis(2000),
        "timeout cap should force an early run: {elapsed:?}"
    );

    ctx.shutdown(Duration::from_secs(5)).await;
}

#[tokio::test]
async fn key_is_reusable_across_cycles() {
    // The common case: the same key debounced again after a previous cycle
    // finished must start a FRESH cycle (not return the stale prior result).
    let ctx = new_ctx("deb-reuse").await;
    let runs = Arc::new(AtomicUsize::new(0));
    dbos::register_workflow::<String, String, _, _>(
        &ctx,
        "deb_target_reuse",
        recording_target(runs.clone()),
    )
    .unwrap();
    let deb = Debouncer::<String, String>::new(&ctx, "deb_target_reuse", Duration::ZERO).unwrap();
    ctx.launch().await.unwrap();

    // Cycle 1.
    let h1 = deb
        .debounce(&ctx, "doc", Duration::from_millis(150), "first".into())
        .await
        .unwrap();
    assert_eq!(h1.get_result().await.unwrap(), "first");
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(runs.load(Ordering::SeqCst), 1);

    // Cycle 2 — same key, after cycle 1 completed.
    let h2 = deb
        .debounce(&ctx, "doc", Duration::from_millis(150), "second".into())
        .await
        .unwrap();
    assert_eq!(
        h2.get_result().await.unwrap(),
        "second",
        "reused key must run a fresh cycle"
    );
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        runs.load(Ordering::SeqCst),
        2,
        "two cycles -> two target runs"
    );

    ctx.shutdown(Duration::from_secs(5)).await;
}

#[tokio::test]
async fn distinct_keys_run_independently() {
    let ctx = new_ctx("deb-keys").await;
    let runs = Arc::new(AtomicUsize::new(0));
    dbos::register_workflow::<String, String, _, _>(
        &ctx,
        "deb_target_k",
        recording_target(runs.clone()),
    )
    .unwrap();
    let deb = Debouncer::<String, String>::new(&ctx, "deb_target_k", Duration::ZERO).unwrap();
    ctx.launch().await.unwrap();

    let h1 = deb
        .debounce(&ctx, "a", Duration::from_millis(150), "a-val".into())
        .await
        .unwrap();
    let h2 = deb
        .debounce(&ctx, "b", Duration::from_millis(150), "b-val".into())
        .await
        .unwrap();

    assert_eq!(h1.get_result().await.unwrap(), "a-val");
    assert_eq!(h2.get_result().await.unwrap(), "b-val");
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        runs.load(Ordering::SeqCst),
        2,
        "two distinct keys -> two runs"
    );

    ctx.shutdown(Duration::from_secs(5)).await;
}
