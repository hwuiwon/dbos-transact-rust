//! Context lifecycle: `new_context`, `launch`, `shutdown`.
//!
//! The Phase-1 launch is minimal (migrate → register app version → one recovery
//! round → mark launched); the queue runner, scheduler, admin server, and
//! conductor are wired in here in later phases.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use dashmap::DashMap;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::config::{Config, process_config};
use crate::context::DbosContext;
use crate::db::SystemDatabase;
use crate::db::sqlite::SqliteDb;
use crate::error::DbosError;
use crate::recovery::recover_pending_workflows;

/// Build a DBOS context (does not launch). Register workflows/queues before
/// calling [`launch`].
pub async fn new_context(cfg: Config) -> Result<Arc<DbosContext>, DbosError> {
    let processed = process_config(cfg)?;

    let db: Arc<dyn SystemDatabase> = if let Some(pool) = &processed.sqlite_pool {
        SqliteDb::from_pool(pool.clone())
    } else {
        let url = processed
            .database_url
            .as_deref()
            .ok_or_else(|| DbosError::initialization("missing database url"))?;
        connect_database(url).await?
    };

    let serializer = processed.serializer.clone();
    let ctx = DbosContext {
        config: processed,
        db,
        token: CancellationToken::new(),
        tracker: TaskTracker::new(),
        registry: RwLock::new(HashMap::new()),
        queues: RwLock::new(HashMap::new()),
        active_workflow_ids: DashMap::new(),
        launched: AtomicBool::new(false),
        serializer,
        scheduler: crate::scheduler::engine::Scheduler::new(),
    };
    Ok(Arc::new(ctx))
}

async fn connect_database(url: &str) -> Result<Arc<dyn SystemDatabase>, DbosError> {
    let lower = url.to_ascii_lowercase();
    if lower.starts_with("postgres://") || lower.starts_with("postgresql://") {
        // Wrap Postgres in the transient-retry decorator so workflows survive a
        // flapping database (SQLite is local and needs no such resilience).
        let inner = crate::db::postgres::PostgresDb::connect(url).await?;
        Ok(crate::db::retry::RetryingDb::wrap(inner))
    } else {
        SqliteDb::connect(url).await
    }
}

impl DbosContext {
    /// Launch the context: run migrations, register the app version, perform one
    /// recovery round, and mark the context launched.
    pub async fn launch(self: &Arc<Self>) -> Result<(), DbosError> {
        if self.launched.load(Ordering::SeqCst) {
            return Err(DbosError::initialization("DBOS is already launched"));
        }

        self.db.run_migrations().await?;

        if let Err(e) = self
            .db
            .create_application_version(self.application_version())
            .await
        {
            tracing::warn!(error = %e, "failed to register application version");
        }

        // Ensure the internal queue exists, then start the queue runner.
        crate::queue::register_internal_queue(self);
        let runner_ctx = self.clone();
        let runner_token = self.token.child_token();
        self.tracker.spawn(crate::queue::runner::run_queue_runner(
            runner_ctx,
            runner_token,
        ));

        // Start a firing task for each scheduled (cron) workflow.
        let scheduled: Vec<(String, String)> = self
            .registry
            .read()
            .unwrap()
            .values()
            .filter_map(|e| e.cron.clone().map(|c| (e.name.clone(), c)))
            .collect();
        for (name, cron) in scheduled {
            let sched_ctx = self.clone();
            let sched_token = self.token.child_token();
            self.tracker.spawn(crate::scheduler::engine::run_schedule(
                sched_ctx,
                sched_token,
                name,
                cron,
            ));
        }

        // Start the dynamic-schedule reconciler (DB-backed schedules).
        let recon_ctx = self.clone();
        let recon_token = self.token.child_token();
        self.tracker.spawn(crate::scheduler::engine::run_reconciler(
            recon_ctx,
            recon_token,
        ));

        // One recovery round for this executor's pending workflows.
        let executor = self.executor_id().to_string();
        if let Err(e) = recover_pending_workflows(self, &[executor.as_str()]).await {
            tracing::error!(error = %e, "error during startup recovery");
        }

        self.launched.store(true, Ordering::SeqCst);
        tracing::info!(app = %self.app_name(), executor = %self.executor_id(), version = %self.application_version(), "DBOS launched");
        Ok(())
    }

    /// Gracefully shut down: cancel, drain in-flight workflows (bounded by
    /// `timeout`), then close the database.
    pub async fn shutdown(self: &Arc<Self>, timeout: Duration) {
        self.token.cancel();
        self.tracker.close();
        if tokio::time::timeout(timeout, self.tracker.wait())
            .await
            .is_err()
        {
            tracing::warn!("shutdown timed out waiting for in-flight workflows to drain");
        }
        self.db.close().await;
        self.launched.store(false, Ordering::SeqCst);
    }
}
