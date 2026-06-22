//! DBOS Transact — lightweight durable workflow orchestration on Postgres/SQLite.
//!
//! Workflows are ordinary async Rust functions whose inputs, outputs, and each
//! memoized *step* are checkpointed in a "system database" (Postgres / SQLite) so
//! that, after a crash, a workflow resumes from its last completed step with
//! exactly-once side-effect semantics.
//!
//! # Quick start
//!
//! ```no_run
//! use std::sync::Arc;
//! use std::time::Duration;
//! use dbos::{Config, WfCtx, DbosError, RunOptions};
//!
//! async fn greet(ctx: WfCtx, name: String) -> Result<String, DbosError> {
//!     ctx.run_step("hello", |_step| async move { Ok(format!("Hello, {name}!")) }).await
//! }
//!
//! # async fn run() -> Result<(), DbosError> {
//! let ctx = dbos::new_context(Config {
//!     app_name: "demo".into(),
//!     database_url: Some("sqlite::memory:".into()),
//!     ..Default::default()
//! }).await?;
//! dbos::register_workflow::<String, String, _, _>(&ctx, "greet", greet)?;
//! ctx.launch().await?;
//! let handle = dbos::run_workflow::<String, String>(&ctx, "greet", "world".into(), RunOptions::default()).await?;
//! let result = handle.get_result().await?;
//! assert_eq!(result, "Hello, world!");
//! ctx.shutdown(Duration::from_secs(5)).await;
//! # Ok(())
//! # }
//! ```

// `DbosError` is a rich struct (code + context fields); boxing every `Result`
// to shrink the Err variant would hurt ergonomics.
#![allow(clippy::result_large_err)]
// The erased-registry function signature is intentionally explicit.
#![allow(clippy::type_complexity)]

mod client;
mod config;
mod constants;
pub mod context;
pub mod db;
pub mod debouncer;
pub mod error;
pub mod queue;
pub mod recovery;
pub mod scheduler;
pub mod serialization;
mod util;
pub mod workflow;

pub use client::{Client, ClientConfig, EnqueueOptions};
pub use config::Config;
pub use constants::DEFAULT_MAX_RECOVERY_ATTEMPTS;
pub use context::lifecycle::new_context;
pub use context::{DbosContext, StepCtx, WfCtx};
pub use db::{
    ForkInput, ListWorkflowsInput, StepInfo, SystemDatabase, WorkflowStatus, WorkflowStatusType,
};
pub use debouncer::Debouncer;
pub use error::{DbosError, DbosErrorCode, DbosResult, PortableWorkflowError};
pub use queue::{QueueOptions, RateLimiter, WorkflowQueue, register_queue};
pub use recovery::recover_pending_workflows;
pub use scheduler::{
    CreateScheduleOptions, ScheduledTime, ScheduledWorkflowInput, WorkflowSchedule,
    create_schedule, delete_schedule, get_schedule, list_schedules, pause_schedule,
    register_scheduled_workflow, resume_schedule,
};
pub use serialization::{JsonSerializer, PortableSerializer, PortableWorkflowArgs, Serializer};
pub use workflow::comms::{get_event, read_stream, send};
pub use workflow::handle::WorkflowHandle;
pub use workflow::manage::{
    ForkOptions, cancel_workflow, delete_workflow, fork_workflow, garbage_collect_workflows,
    get_step_aggregates, get_workflow_events, get_workflow_notifications,
    get_workflow_status_counts, get_workflow_steps, get_workflow_streams,
    list_application_versions, list_registered_queues, list_workflows, resume_workflow,
    retrieve_workflow, set_latest_application_version,
};
pub use workflow::run::run_workflow;
pub use workflow::step::StepOptions;
pub use workflow::{
    DeduplicationPolicy, RegisterOptions, RunOptions, register_workflow, register_workflow_opts,
};

/// Common imports for applications using DBOS.
pub mod prelude {
    pub use crate::{
        Config, DbosContext, DbosError, DbosResult, RegisterOptions, RunOptions, StepCtx, WfCtx,
        WorkflowHandle, WorkflowStatus, WorkflowStatusType, new_context, recover_pending_workflows,
        register_workflow, run_workflow,
    };
}
