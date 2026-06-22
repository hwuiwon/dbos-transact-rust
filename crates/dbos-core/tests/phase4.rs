//! Phase 4 exit tests: durable queues (enqueue, concurrency, rate limit, priority).

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use common::new_ctx;
use dbos::{DbosError, QueueOptions, RateLimiter, RunOptions, WfCtx};
use futures::future::BoxFuture;

async fn doubler(ctx: WfCtx, x: i32) -> Result<i32, DbosError> {
    ctx.run_step(
        "double",
        move |_s| async move { Ok::<i32, DbosError>(x * 2) },
    )
    .await
}

#[tokio::test]
async fn enqueue_runs_via_queue_runner() {
    let ctx = new_ctx("p4-enqueue").await;
    dbos::register_queue(&ctx, "work", QueueOptions::default()).unwrap();
    dbos::register_workflow::<i32, i32, _, _>(&ctx, "doubler", doubler).unwrap();
    ctx.launch().await.unwrap();

    let h = dbos::run_workflow::<i32, i32>(
        &ctx,
        "doubler",
        21,
        RunOptions {
            queue: Some("work".into()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(h.get_result().await.unwrap(), 42);

    let status = h.get_status().await.unwrap();
    assert_eq!(status.queue_name, "work");
    ctx.shutdown(Duration::from_secs(5)).await;
}

#[tokio::test]
async fn enqueue_many_all_complete() {
    let ctx = new_ctx("p4-many").await;
    dbos::register_queue(&ctx, "work", QueueOptions::default()).unwrap();
    dbos::register_workflow::<i32, i32, _, _>(&ctx, "doubler", doubler).unwrap();
    ctx.launch().await.unwrap();

    let mut handles = Vec::new();
    for i in 0..10 {
        handles.push(
            dbos::run_workflow::<i32, i32>(
                &ctx,
                "doubler",
                i,
                RunOptions {
                    queue: Some("work".into()),
                    ..Default::default()
                },
            )
            .await
            .unwrap(),
        );
    }
    for (i, h) in handles.into_iter().enumerate() {
        assert_eq!(h.get_result().await.unwrap(), (i as i32) * 2);
    }
    ctx.shutdown(Duration::from_secs(5)).await;
}

#[tokio::test]
async fn global_concurrency_one_serializes_execution() {
    let ctx = new_ctx("p4-concurrency").await;
    dbos::register_queue(
        &ctx,
        "limited",
        QueueOptions {
            global_concurrency: Some(1),
            ..Default::default()
        },
    )
    .unwrap();

    // Step tracks the peak observed concurrency.
    let current = Arc::new(AtomicI64::new(0));
    let max_seen = Arc::new(AtomicI64::new(0));
    let cur = current.clone();
    let mx = max_seen.clone();
    let wf = move |ctx: WfCtx, _x: i32| {
        let cur = cur.clone();
        let mx = mx.clone();
        Box::pin(async move {
            let cur2 = cur.clone();
            let mx2 = mx.clone();
            ctx.run_step("track", move |_s| {
                let cur = cur2.clone();
                let mx = mx2.clone();
                async move {
                    let now = cur.fetch_add(1, Ordering::SeqCst) + 1;
                    mx.fetch_max(now, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(80)).await;
                    cur.fetch_sub(1, Ordering::SeqCst);
                    Ok::<i32, DbosError>(0)
                }
            })
            .await
        }) as BoxFuture<'static, Result<i32, DbosError>>
    };
    dbos::register_workflow::<i32, i32, _, _>(&ctx, "tracker", wf).unwrap();
    ctx.launch().await.unwrap();

    let mut handles = Vec::new();
    for i in 0..4 {
        handles.push(
            dbos::run_workflow::<i32, i32>(
                &ctx,
                "tracker",
                i,
                RunOptions {
                    queue: Some("limited".into()),
                    ..Default::default()
                },
            )
            .await
            .unwrap(),
        );
    }
    for h in handles {
        h.get_result().await.unwrap();
    }
    assert_eq!(
        max_seen.load(Ordering::SeqCst),
        1,
        "global concurrency 1 must serialize"
    );
    ctx.shutdown(Duration::from_secs(5)).await;
}

#[tokio::test]
async fn rate_limited_queue_eventually_completes() {
    let ctx = new_ctx("p4-rate").await;
    dbos::register_queue(
        &ctx,
        "rl",
        QueueOptions {
            rate_limit: Some(RateLimiter {
                limit: 2,
                period: Duration::from_millis(200),
            }),
            ..Default::default()
        },
    )
    .unwrap();
    dbos::register_workflow::<i32, i32, _, _>(&ctx, "doubler", doubler).unwrap();
    ctx.launch().await.unwrap();

    let mut handles = Vec::new();
    for i in 0..5 {
        handles.push(
            dbos::run_workflow::<i32, i32>(
                &ctx,
                "doubler",
                i,
                RunOptions {
                    queue: Some("rl".into()),
                    ..Default::default()
                },
            )
            .await
            .unwrap(),
        );
    }
    for (i, h) in handles.into_iter().enumerate() {
        assert_eq!(h.get_result().await.unwrap(), (i as i32) * 2);
    }
    ctx.shutdown(Duration::from_secs(10)).await;
}

#[tokio::test]
async fn delayed_workflow_runs_after_delay() {
    let ctx = new_ctx("p4-delay").await;
    dbos::register_queue(&ctx, "work", QueueOptions::default()).unwrap();
    dbos::register_workflow::<i32, i32, _, _>(&ctx, "doubler", doubler).unwrap();
    ctx.launch().await.unwrap();

    let started = std::time::Instant::now();
    let h = dbos::run_workflow::<i32, i32>(
        &ctx,
        "doubler",
        5,
        RunOptions {
            queue: Some("work".into()),
            delay: Some(Duration::from_millis(300)),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(h.get_result().await.unwrap(), 10);
    assert!(
        started.elapsed() >= Duration::from_millis(250),
        "should respect the delay"
    );
    ctx.shutdown(Duration::from_secs(5)).await;
}

#[tokio::test]
async fn delay_without_queue_is_rejected() {
    let ctx = new_ctx("p4-delay-noqueue").await;
    dbos::register_workflow::<i32, i32, _, _>(&ctx, "doubler", doubler).unwrap();
    ctx.launch().await.unwrap();
    let res = dbos::run_workflow::<i32, i32>(
        &ctx,
        "doubler",
        5,
        RunOptions {
            delay: Some(Duration::from_millis(100)),
            ..Default::default()
        },
    )
    .await;
    assert!(res.is_err());
    ctx.shutdown(Duration::from_secs(2)).await;
}
