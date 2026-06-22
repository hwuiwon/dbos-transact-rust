//! Shared test helpers.
#![allow(dead_code)] // each test binary uses a subset of these helpers

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use dbos::{Config, DbosContext, DbosError, WfCtx};
use futures::future::BoxFuture;

static DB_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Provision a fresh, isolated database URL for one test.
///
/// * If `DBOS_TEST_DATABASE_URL` is set (a Postgres base URL with no database
///   path, e.g. `postgres://dbos:dbos@localhost:5433`), a unique database is
///   created and its URL returned — giving each test full isolation.
/// * Otherwise an in-process SQLite database is used.
async fn provision_database_url() -> String {
    match std::env::var("DBOS_TEST_DATABASE_URL") {
        Ok(base) if !base.is_empty() => {
            use sqlx::Connection;
            let base = base.trim_end_matches('/').to_string();
            let n = DB_COUNTER.fetch_add(1, Ordering::SeqCst);
            let nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let dbname = format!("dbos_test_{nanos}_{n}");
            let admin_url = format!("{base}/postgres");
            let mut conn = sqlx::PgConnection::connect(&admin_url)
                .await
                .expect("connect to maintenance db");
            sqlx::query(&format!("CREATE DATABASE \"{dbname}\""))
                .execute(&mut conn)
                .await
                .expect("create test database");
            conn.close().await.ok();
            format!("{base}/{dbname}")
        }
        _ => "sqlite::memory:".to_string(),
    }
}

/// Build an unlaunched context backed by a fresh, isolated database.
pub async fn new_ctx(app: &str) -> Arc<DbosContext> {
    let url = provision_database_url().await;
    dbos::new_context(Config {
        app_name: app.into(),
        database_url: Some(url),
        ..Default::default()
    })
    .await
    .expect("new_context")
}

/// A database URL that can be opened by more than one context/client at once
/// (a temp FILE for SQLite — `:memory:` is per-connection; a fresh DB for Postgres).
pub async fn shared_db_url() -> String {
    match std::env::var("DBOS_TEST_DATABASE_URL") {
        // Postgres: a freshly-provisioned database is already shareable by URL.
        Ok(base) if !base.is_empty() => provision_database_url().await,
        _ => {
            let nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let n = DB_COUNTER.fetch_add(1, Ordering::SeqCst);
            let path = std::env::temp_dir().join(format!("dbos_shared_{nanos}_{n}.sqlite"));
            format!("sqlite://{}", path.display())
        }
    }
}

/// Build an unlaunched context against an explicit URL.
pub async fn new_ctx_from_url(app: &str, url: &str) -> Arc<DbosContext> {
    dbos::new_context(Config {
        app_name: app.into(),
        database_url: Some(url.to_string()),
        ..Default::default()
    })
    .await
    .expect("new_context")
}

/// Build a context with an explicit executor id (for multi-executor tests).
pub async fn new_ctx_with_executor(app: &str, executor: &str) -> Arc<DbosContext> {
    let url = provision_database_url().await;
    dbos::new_context(Config {
        app_name: app.into(),
        database_url: Some(url),
        executor_id: Some(executor.into()),
        ..Default::default()
    })
    .await
    .expect("new_context")
}

/// A workflow with two memoized steps `(base+1)` then `(+1)` = `base+2`. Each
/// step body bumps `counter`, so tests can assert how many times the bodies
/// actually ran (memoized replays do not bump it).
pub fn counting_two_step(
    counter: Arc<AtomicUsize>,
) -> impl Fn(WfCtx, i32) -> BoxFuture<'static, Result<i32, DbosError>> + Clone + Send + Sync + 'static
{
    move |ctx: WfCtx, base: i32| {
        let counter = counter.clone();
        Box::pin(async move {
            let c1 = counter.clone();
            let a = ctx
                .run_step("step_one", move |_s| async move {
                    c1.fetch_add(1, Ordering::SeqCst);
                    Ok::<i32, DbosError>(base + 1)
                })
                .await?;
            let c2 = counter.clone();
            let b = ctx
                .run_step("step_two", move |_s| async move {
                    c2.fetch_add(1, Ordering::SeqCst);
                    Ok::<i32, DbosError>(a + 1)
                })
                .await?;
            Ok(b)
        })
    }
}
