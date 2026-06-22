//! Workflow handles — wait for completion and read status/result.
//!
//! A [`ChannelHandle`] backs a fresh local run (result delivered over a
//! `oneshot`); a [`PollingHandle`] reads a durable workflow from the DB. Both
//! decode the stored encoded output into the caller's `R` using the row's
//! recorded serialization format.

use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde::de::DeserializeOwned;
use tokio::sync::{Mutex, oneshot};

use crate::constants::DB_RETRY_INTERVAL;
use crate::context::DbosContext;
use crate::db::WorkflowStatus;
use crate::error::DbosError;
use crate::serialization::{self, deserialize_workflow_error, resolve_decoder};

/// The encoded outcome delivered by a running workflow task.
pub(crate) struct EncodedOutcome {
    pub output: Option<String>,
    pub serialization: String,
}

/// A handle to a running or completed workflow returning `R`.
#[async_trait]
pub trait WorkflowHandle<R>: Send + Sync {
    /// Wait for completion and return the decoded result (or the workflow error).
    async fn get_result(&self) -> Result<R, DbosError>;
    /// Current status without waiting.
    async fn get_status(&self) -> Result<WorkflowStatus, DbosError>;
    /// The workflow id.
    fn workflow_id(&self) -> &str;
}

/// Decode a stored encoded output into `R` using its serialization tag.
pub(crate) fn decode_output<R: DeserializeOwned>(
    ctx: &DbosContext,
    output: &Option<String>,
    serialization_name: &str,
) -> Result<R, DbosError> {
    let decoder = resolve_decoder(serialization_name, ctx.serializer.as_ref())?;
    serialization::decode::<R>(decoder.as_ref(), output)
}

/// Handle for a freshly-started local run; the outcome arrives over a channel,
/// with a durable-DB fallback so repeated calls remain valid.
pub struct ChannelHandle<R> {
    pub(crate) ctx: Arc<DbosContext>,
    pub(crate) workflow_id: String,
    pub(crate) rx: Mutex<Option<oneshot::Receiver<Result<EncodedOutcome, DbosError>>>>,
    pub(crate) _marker: PhantomData<fn() -> R>,
}

#[async_trait]
impl<R: DeserializeOwned + Send + Sync> WorkflowHandle<R> for ChannelHandle<R> {
    async fn get_result(&self) -> Result<R, DbosError> {
        let rx = self.rx.lock().await.take();
        match rx {
            Some(rx) => match rx.await {
                Ok(Ok(outcome)) => {
                    decode_output::<R>(&self.ctx, &outcome.output, &outcome.serialization)
                }
                Ok(Err(e)) => Err(e),
                // Sender dropped (e.g. task aborted at shutdown): fall back to DB.
                Err(_) => poll_result::<R>(&self.ctx, &self.workflow_id).await,
            },
            // Already consumed: read the durable outcome.
            None => poll_result::<R>(&self.ctx, &self.workflow_id).await,
        }
    }

    async fn get_status(&self) -> Result<WorkflowStatus, DbosError> {
        get_status(&self.ctx, &self.workflow_id).await
    }

    fn workflow_id(&self) -> &str {
        &self.workflow_id
    }
}

/// Handle that polls the durable workflow_status row.
pub struct PollingHandle<R> {
    pub(crate) ctx: Arc<DbosContext>,
    pub(crate) workflow_id: String,
    pub(crate) _marker: PhantomData<fn() -> R>,
}

impl<R> PollingHandle<R> {
    pub(crate) fn new(ctx: Arc<DbosContext>, workflow_id: String) -> Self {
        Self { ctx, workflow_id, _marker: PhantomData }
    }
}

#[async_trait]
impl<R: DeserializeOwned + Send + Sync> WorkflowHandle<R> for PollingHandle<R> {
    async fn get_result(&self) -> Result<R, DbosError> {
        poll_result::<R>(&self.ctx, &self.workflow_id).await
    }

    async fn get_status(&self) -> Result<WorkflowStatus, DbosError> {
        get_status(&self.ctx, &self.workflow_id).await
    }

    fn workflow_id(&self) -> &str {
        &self.workflow_id
    }
}

async fn poll_result<R: DeserializeOwned>(
    ctx: &Arc<DbosContext>,
    workflow_id: &str,
) -> Result<R, DbosError> {
    let res = ctx.db.await_workflow_result(workflow_id, DB_RETRY_INTERVAL).await?;
    if let Some(err_str) = &res.error {
        if let Some(e) = deserialize_workflow_error(&Some(err_str.clone()), &res.serialization) {
            return Err(e);
        }
    }
    decode_output::<R>(ctx, &res.output, &res.serialization)
}

async fn get_status(ctx: &Arc<DbosContext>, workflow_id: &str) -> Result<WorkflowStatus, DbosError> {
    match ctx.db.get_workflow_status(workflow_id).await? {
        Some(s) => Ok(s),
        None => Err(DbosError::non_existent_workflow(workflow_id)),
    }
}

/// Build a typed [`PollingHandle`] boxed as a trait object.
pub(crate) fn boxed_polling<R: DeserializeOwned + Send + Sync + 'static>(
    ctx: Arc<DbosContext>,
    workflow_id: String,
) -> Box<dyn WorkflowHandle<R>> {
    Box::new(PollingHandle::new(ctx, workflow_id))
}

#[allow(dead_code)]
pub(crate) fn default_poll_interval() -> Duration {
    DB_RETRY_INTERVAL
}
