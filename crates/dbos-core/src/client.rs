//! External client — enqueue and manage workflows from outside the executor.
//!
//! A [`Client`] connects to the system database
//! without launching the executor (no queue runner / recovery): it enqueues work
//! for a server process to run, and queries/manages existing workflows.

use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::config::Config;
use crate::constants::DEFAULT_MAX_RECOVERY_ATTEMPTS;
use crate::context::DbosContext;
use crate::db::{
    InsertWorkflowStatusInput, ListWorkflowsInput, NewWorkflowStatus, StepInfo, WorkflowStatus,
    WorkflowStatusType,
};
use crate::error::DbosError;
use crate::serialization::{self, resolve_encoder};
use crate::util::{new_uuid, now_ms};
use crate::workflow::handle::{WorkflowHandle, boxed_polling};
use crate::workflow::manage::{self, ForkOptions};

/// Client connection configuration.
#[derive(Clone, Default)]
pub struct ClientConfig {
    pub app_name: String,
    pub database_url: String,
    pub application_version: Option<String>,
    pub executor_id: Option<String>,
}

/// Options for [`Client::enqueue`].
#[derive(Clone, Default)]
pub struct EnqueueOptions {
    pub workflow_id: Option<String>,
    pub application_version: Option<String>,
    pub deduplication_id: Option<String>,
    pub priority: u32,
    pub delay: Option<Duration>,
}

/// A client to a DBOS system database.
pub struct Client {
    ctx: Arc<DbosContext>,
}

impl Client {
    /// Connect (running migrations) without launching an executor.
    pub async fn new(cfg: ClientConfig) -> Result<Self, DbosError> {
        let ctx = crate::context::lifecycle::new_context(Config {
            app_name: cfg.app_name,
            database_url: Some(cfg.database_url),
            application_version: cfg.application_version,
            executor_id: cfg.executor_id,
            ..Default::default()
        })
        .await?;
        ctx.db.run_migrations().await?;
        Ok(Self { ctx })
    }

    /// The underlying context (for advanced use).
    pub fn context(&self) -> &Arc<DbosContext> {
        &self.ctx
    }

    /// Enqueue a workflow by name for a server to execute. Returns a handle that
    /// polls for the durable result.
    pub async fn enqueue<P, R>(
        &self,
        queue: &str,
        workflow_name: &str,
        input: P,
        opts: EnqueueOptions,
    ) -> Result<Box<dyn WorkflowHandle<R>>, DbosError>
    where
        P: Serialize,
        R: DeserializeOwned + Send + Sync + 'static,
    {
        let encoder = resolve_encoder(false, self.ctx.serializer.as_ref());
        let encoded = serialization::encode(encoder.as_ref(), &input)?;
        let id = opts.workflow_id.unwrap_or_else(new_uuid);
        let status = if opts.delay.is_some() {
            WorkflowStatusType::Delayed
        } else {
            WorkflowStatusType::Enqueued
        };
        let new = NewWorkflowStatus {
            id: id.clone(),
            status: Some(status),
            name: workflow_name.to_string(),
            application_version: opts
                .application_version
                .unwrap_or_else(|| self.ctx.application_version().to_string()),
            application_id: self.ctx.application_id().to_string(),
            executor_id: self.ctx.executor_id().to_string(),
            created_at_ms: now_ms(),
            input: encoded,
            queue_name: queue.to_string(),
            deduplication_id: opts.deduplication_id.unwrap_or_default(),
            priority: opts.priority as i64,
            delay_until_ms: opts.delay.map(|d| now_ms() + d.as_millis() as i64),
            serialization: encoder.name().to_string(),
            ..Default::default()
        };
        self.ctx
            .db
            .insert_workflow_status(InsertWorkflowStatusInput {
                status: new,
                max_retries: DEFAULT_MAX_RECOVERY_ATTEMPTS,
                owner_xid: new_uuid(),
                increment_attempts: false,
                record_child: None,
            })
            .await?;
        Ok(boxed_polling::<R>(self.ctx.clone(), id))
    }

    /// Get a handle to an existing workflow by id.
    pub fn retrieve_workflow<R: DeserializeOwned + Send + Sync + 'static>(
        &self,
        workflow_id: &str,
    ) -> Box<dyn WorkflowHandle<R>> {
        manage::retrieve_workflow::<R>(&self.ctx, workflow_id)
    }

    pub async fn list_workflows(
        &self,
        input: ListWorkflowsInput,
    ) -> Result<Vec<WorkflowStatus>, DbosError> {
        manage::list_workflows(&self.ctx, input).await
    }

    pub async fn get_workflow_steps(
        &self,
        workflow_id: &str,
    ) -> Result<Vec<StepInfo>, DbosError> {
        manage::get_workflow_steps(&self.ctx, workflow_id).await
    }

    pub async fn cancel_workflow(&self, workflow_id: &str) -> Result<(), DbosError> {
        manage::cancel_workflow(&self.ctx, workflow_id).await
    }

    pub async fn resume_workflow<R: DeserializeOwned + Send + Sync + 'static>(
        &self,
        workflow_id: &str,
    ) -> Result<Box<dyn WorkflowHandle<R>>, DbosError> {
        manage::resume_workflow::<R>(&self.ctx, workflow_id).await
    }

    pub async fn fork_workflow<R: DeserializeOwned + Send + Sync + 'static>(
        &self,
        opts: ForkOptions,
    ) -> Result<Box<dyn WorkflowHandle<R>>, DbosError> {
        manage::fork_workflow::<R>(&self.ctx, opts).await
    }

    pub async fn delete_workflow(
        &self,
        workflow_id: &str,
        delete_children: bool,
    ) -> Result<(), DbosError> {
        manage::delete_workflow(&self.ctx, workflow_id, delete_children).await
    }

    pub async fn send<P: Serialize>(
        &self,
        destination_id: &str,
        message: P,
        topic: &str,
    ) -> Result<(), DbosError> {
        crate::workflow::comms::send(&self.ctx, destination_id, message, topic).await
    }

    pub async fn get_event<R: DeserializeOwned>(
        &self,
        target_workflow_id: &str,
        key: &str,
        timeout: Duration,
    ) -> Result<Option<R>, DbosError> {
        crate::workflow::comms::get_event(&self.ctx, target_workflow_id, key, timeout).await
    }

    /// Close the underlying database connection.
    pub async fn shutdown(&self, timeout: Duration) {
        self.ctx.shutdown(timeout).await;
    }
}
