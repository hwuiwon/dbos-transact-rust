//! Workflow registration and the type-erased registry.
//!
//! Typed `P`/`R` are erased at registration into a uniform closure operating over
//! the encoded `Option<String>` form, decoding/encoding with serde inside.

pub mod comms;
pub mod handle;
pub mod manage;
pub mod run;
pub mod step;

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use futures::FutureExt;
use futures::future::BoxFuture;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::constants::DEFAULT_MAX_RECOVERY_ATTEMPTS;
use crate::context::{DbosContext, WfCtx};
use crate::error::DbosError;
use crate::serialization::{
    self, PORTABLE_SERIALIZER_NAME, decode_portable_args, resolve_decoder, resolve_encoder,
};

/// The type-erased workflow body: decode input, run, encode output.
///
/// Returns the encoded output (`*string`) on success or the workflow error.
pub(crate) type ErasedWorkflowFn = Arc<
    dyn Fn(WfCtx, Option<String>, String) -> BoxFuture<'static, Result<Option<String>, DbosError>>
        + Send
        + Sync,
>;

/// A registered workflow.
pub(crate) struct WorkflowEntry {
    pub erased: ErasedWorkflowFn,
    pub max_retries: i64,
    pub name: String,
    pub class_name: String,
    pub config_name: Option<String>,
    #[allow(dead_code)] // consumed by the scheduler (later phase)
    pub cron: Option<String>,
}

/// Queue deduplication policy for [`RunOptions`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum DeduplicationPolicy {
    /// Reject a duplicate enqueue (default).
    #[default]
    Reject,
    /// Return a handle to the existing in-flight workflow.
    ReturnExisting,
}

/// Registration-time options.
#[derive(Clone)]
pub struct RegisterOptions {
    /// Max recovery attempts before dead-lettering (default 100; <=0 disables the cap).
    pub max_retries: i64,
    /// Cron schedule (registers a scheduled workflow).
    pub schedule: Option<String>,
    /// Custom registration/lookup name (defaults to the provided name).
    pub custom_name: Option<String>,
}

impl Default for RegisterOptions {
    fn default() -> Self {
        Self {
            max_retries: DEFAULT_MAX_RECOVERY_ATTEMPTS,
            schedule: None,
            custom_name: None,
        }
    }
}

/// Per-invocation options for [`run::run_workflow`].
#[derive(Clone, Default)]
pub struct RunOptions {
    pub workflow_id: Option<String>,
    pub queue: Option<String>,
    pub application_version: Option<String>,
    pub deduplication_id: Option<String>,
    pub deduplication_policy: DeduplicationPolicy,
    pub priority: u32,
    pub queue_partition_key: Option<String>,
    pub delay: Option<Duration>,
    pub authenticated_user: Option<String>,
    pub assumed_role: Option<String>,
    pub authenticated_roles: Vec<String>,
    pub portable: bool,
    /// Optional workflow timeout; on expiry the workflow is cancelled at its next
    /// step boundary.
    pub timeout: Option<Duration>,
}

/// Internal dispatch flags for the erased run path (not user-visible).
#[derive(Clone, Copy, Default)]
pub(crate) struct RunFlags {
    pub is_recovery: bool,
    pub is_dequeue: bool,
}

/// Register an async function as a durable workflow under an explicit name.
pub fn register_workflow<P, R, F, Fut>(
    ctx: &Arc<DbosContext>,
    name: &str,
    f: F,
) -> Result<(), DbosError>
where
    P: DeserializeOwned + Send + 'static,
    R: Serialize + Send + 'static,
    F: Fn(WfCtx, P) -> Fut + Send + Sync + Clone + 'static,
    Fut: Future<Output = Result<R, DbosError>> + Send + 'static,
{
    register_workflow_opts(ctx, name, f, RegisterOptions::default())
}

/// Register a workflow with explicit [`RegisterOptions`].
pub fn register_workflow_opts<P, R, F, Fut>(
    ctx: &Arc<DbosContext>,
    name: &str,
    f: F,
    opts: RegisterOptions,
) -> Result<(), DbosError>
where
    P: DeserializeOwned + Send + 'static,
    R: Serialize + Send + 'static,
    F: Fn(WfCtx, P) -> Fut + Send + Sync + Clone + 'static,
    Fut: Future<Output = Result<R, DbosError>> + Send + 'static,
{
    if ctx.is_launched() {
        return Err(DbosError::initialization(
            "cannot register a workflow after the context has been launched",
        ));
    }
    let key = opts.custom_name.clone().unwrap_or_else(|| name.to_string());

    let erased: ErasedWorkflowFn = Arc::new(
        move |wfctx: WfCtx, input: Option<String>, input_ser: String| {
            let f = f.clone();
            async move {
                let workflow_id = wfctx.workflow_id().to_string();
                // Decode the input into P.
                let p: P = if input_ser == PORTABLE_SERIALIZER_NAME {
                    decode_portable_args::<P>(&input)?
                } else {
                    let decoder = resolve_decoder(&input_ser, wfctx.ctx.serializer.as_ref())?;
                    serialization::decode::<P>(decoder.as_ref(), &input)?
                };

                // Run the user body, converting panics into recorded errors.
                let fut = std::panic::AssertUnwindSafe(f(wfctx.clone(), p));
                let r: R = match fut.catch_unwind().await {
                    Ok(Ok(r)) => r,
                    Ok(Err(e)) => return Err(e),
                    Err(_) => {
                        return Err(DbosError::new(
                            crate::error::DbosErrorCode::WorkflowExecution,
                            format!("Workflow {workflow_id} execution error: workflow panicked"),
                        ));
                    }
                };

                // Encode the output.
                let encoder = resolve_encoder(wfctx.is_portable(), wfctx.ctx.serializer.as_ref());
                let out = serialization::encode::<R>(encoder.as_ref(), &r)?;
                Ok(out)
            }
            .boxed()
        },
    );

    let entry = WorkflowEntry {
        erased,
        max_retries: opts.max_retries,
        name: key.clone(),
        class_name: String::new(),
        config_name: None,
        cron: opts.schedule,
    };

    let mut reg = ctx.registry.write().unwrap();
    if reg.contains_key(&key) {
        return Err(DbosError::conflicting_registration(&key));
    }
    reg.insert(key, entry);
    Ok(())
}
