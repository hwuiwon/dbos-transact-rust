//! The central [`DbosContext`] — owns the DB pool, registries, cancellation
//! token, task tracker, and lifecycle state. Always held as `Arc<DbosContext>`.

pub mod lifecycle;
pub mod state;

use std::collections::HashMap;
use std::sync::RwLock;
use std::sync::atomic::{AtomicBool, Ordering};

use dashmap::DashMap;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::config::ProcessedConfig;
use crate::db::SystemDatabase;
use crate::queue::WorkflowQueue;
use crate::serialization::Serializer;
use crate::workflow::WorkflowEntry;
use std::sync::Arc;

pub use state::{StepCtx, WfCtx};

/// Per-process bookkeeping for a workflow currently executing on this context
/// (drives worker-concurrency counting and the "already running here" skip).
#[derive(Debug, Clone, Default)]
pub(crate) struct ActiveEntry {
    pub queue_name: String,
    #[allow(dead_code)] // used by partitioned-queue dequeue (later phase)
    pub queue_partition_key: String,
}

/// The DBOS runtime context.
pub struct DbosContext {
    pub(crate) config: ProcessedConfig,
    pub(crate) db: Arc<dyn SystemDatabase>,
    pub(crate) token: CancellationToken,
    pub(crate) tracker: TaskTracker,
    pub(crate) registry: RwLock<HashMap<String, WorkflowEntry>>,
    pub(crate) queues: RwLock<HashMap<String, WorkflowQueue>>,
    pub(crate) active_workflow_ids: DashMap<String, ActiveEntry>,
    pub(crate) launched: AtomicBool,
    pub(crate) serializer: Option<Arc<dyn Serializer>>,
    /// Tracks firing tasks for DB-backed (dynamic) schedules; driven by the reconciler.
    pub(crate) scheduler: crate::scheduler::engine::Scheduler,
}

impl DbosContext {
    /// The resolved application version.
    pub fn application_version(&self) -> &str {
        &self.config.application_version
    }

    /// The resolved executor id (`local` by default).
    pub fn executor_id(&self) -> &str {
        &self.config.executor_id
    }

    /// The configured application id.
    pub fn application_id(&self) -> &str {
        &self.config.application_id
    }

    /// The application name.
    pub fn app_name(&self) -> &str {
        &self.config.app_name
    }

    /// Whether [`lifecycle::launch`] has completed.
    pub fn is_launched(&self) -> bool {
        self.launched.load(Ordering::SeqCst)
    }

    /// The system database handle (for advanced/embedding use and tests).
    pub fn system_database(&self) -> &Arc<dyn SystemDatabase> {
        &self.db
    }

    /// Test/utility hook: clear the workflow registry.
    pub fn clear_registries(&self) {
        self.registry.write().unwrap().clear();
    }
}
