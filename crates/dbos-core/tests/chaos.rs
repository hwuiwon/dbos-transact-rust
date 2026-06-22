//! Chaos test (Phase 8): workflows must complete correctly despite the database
//! going down and coming back. Gated on `DBOS_TEST_DATABASE_URL` (Postgres) — it
//! is skipped under the default SQLite run. Run alone:
//!
//! ```sh
//! DBOS_TEST_DATABASE_URL='postgres://dbos:dbos@localhost:5433' \
//!   cargo test -p dbos-core --test chaos -- --ignored --test-threads=1
//! ```
//!
//! Marked `#[ignore]` so it never runs in normal CI without an explicit opt-in
//! (it restarts a shared Postgres container, which would disrupt other tests).

mod common;

use std::time::Duration;

use common::new_ctx;
use dbos::{DbosError, RunOptions, WfCtx};

async fn chained(ctx: WfCtx, base: i64) -> Result<i64, DbosError> {
    let a = ctx
        .run_step("s1", move |_s| async move {
            tokio::time::sleep(Duration::from_millis(60)).await;
            Ok::<i64, DbosError>(base + 1)
        })
        .await?;
    let b = ctx
        .run_step("s2", move |_s| async move {
            tokio::time::sleep(Duration::from_millis(60)).await;
            Ok::<i64, DbosError>(a + 1)
        })
        .await?;
    let c = ctx
        .run_step("s3", move |_s| async move {
            tokio::time::sleep(Duration::from_millis(60)).await;
            Ok::<i64, DbosError>(b + 1)
        })
        .await?;
    Ok(c) // base + 3
}

fn pg_container() -> String {
    std::env::var("DBOS_TEST_PG_CONTAINER").unwrap_or_else(|_| "dbos-pg".to_string())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "chaos test: requires Postgres + docker; restarts a shared container"]
async fn chaos_workflows_survive_database_restart() {
    if std::env::var("DBOS_TEST_DATABASE_URL").is_err() {
        eprintln!("skipping chaos test: set DBOS_TEST_DATABASE_URL to a Postgres base URL");
        return;
    }

    let ctx = new_ctx("chaos").await;
    dbos::register_workflow::<i64, i64, _, _>(&ctx, "chained", chained).unwrap();
    ctx.launch().await.unwrap();

    const N: i64 = 30;
    let mut handles = Vec::new();
    for i in 0..N {
        handles.push(
            dbos::run_workflow::<i64, i64>(
                &ctx,
                "chained",
                i,
                RunOptions {
                    workflow_id: Some(format!("chaos-{i}")),
                    ..Default::default()
                },
            )
            .await
            .unwrap(),
        );
    }

    // Flap the database while the workflows are mid-flight.
    let container = pg_container();
    let chaos = tokio::task::spawn_blocking(move || {
        std::thread::sleep(Duration::from_millis(150));
        let status = std::process::Command::new("docker")
            .args(["restart", "-t", "0", &container])
            .status()
            .expect("run docker restart");
        assert!(status.success(), "docker restart failed");
    });

    // Every workflow must still produce the correct result once the DB returns.
    for (i, h) in handles.into_iter().enumerate() {
        let got = h.get_result().await.expect("workflow result after chaos");
        assert_eq!(got, i as i64 + 3, "workflow {i} produced wrong result");
    }
    chaos.await.unwrap();

    // No double execution: each workflow recorded exactly its 3 steps.
    for i in 0..N {
        let steps = dbos::get_workflow_steps(&ctx, &format!("chaos-{i}"))
            .await
            .unwrap();
        assert_eq!(steps.len(), 3, "workflow {i} should have exactly 3 steps");
    }

    ctx.shutdown(Duration::from_secs(10)).await;
}
