//! Workflow invocation — `run_workflow` and its erased core.
//!
//! The erased core inserts the status row (owner_xid + attempt counting), decides
//! whether to execute locally or hand back a polling handle, then runs the body
//! on the task tracker and writes the terminal outcome.

use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::AtomicI64;

use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::sync::{Mutex, oneshot};

use crate::constants::DB_RETRY_INTERVAL;
use crate::context::state::WorkflowState;
use crate::context::{ActiveEntry, DbosContext, WfCtx};
use crate::db::{ChildRecord, InsertWorkflowStatusInput, NewWorkflowStatus, WorkflowStatusType};
use crate::error::{DbosError, DbosErrorCode};
use crate::serialization::{
    self, PORTABLE_SERIALIZER_NAME, deserialize_workflow_error, encode_portable_args,
    resolve_encoder, serialize_workflow_error,
};
use crate::util::{new_uuid, now_ms};
use crate::workflow::handle::{ChannelHandle, EncodedOutcome, WorkflowHandle, boxed_polling};
use crate::workflow::{RunFlags, RunOptions};

/// Internal erased handle returned by [`run_workflow_erased`].
pub(crate) enum ErasedHandle {
    /// A locally-executing run; the outcome arrives on the channel.
    Channel {
        workflow_id: String,
        rx: oneshot::Receiver<Result<EncodedOutcome, DbosError>>,
    },
    /// A skipped run (enqueued, terminal, owned elsewhere, or already running);
    /// read the durable outcome by polling.
    Polling { workflow_id: String },
}

/// Child-workflow spawn metadata (parent id + reserved step id).
pub(crate) struct ChildSpawn {
    pub parent_workflow_id: String,
    pub step_id: i64,
}

/// Start (or attach to) a workflow and return a typed handle.
pub async fn run_workflow<P, R>(
    ctx: &Arc<DbosContext>,
    name: &str,
    input: P,
    opts: RunOptions,
) -> Result<Box<dyn WorkflowHandle<R>>, DbosError>
where
    P: Serialize + Send + 'static,
    R: DeserializeOwned + Send + Sync + 'static,
{
    // Encode the input.
    let (encoded, serialization_name) = if opts.portable {
        (encode_portable_args(&input)?, PORTABLE_SERIALIZER_NAME.to_string())
    } else {
        let encoder = resolve_encoder(false, ctx.serializer.as_ref());
        (serialization::encode(encoder.as_ref(), &input)?, encoder.name().to_string())
    };

    let handle = run_workflow_erased(
        ctx,
        name,
        encoded,
        serialization_name,
        opts,
        RunFlags::default(),
        None,
    )
    .await?;
    Ok(match handle {
        ErasedHandle::Channel { workflow_id, rx } => Box::new(ChannelHandle::<R> {
            ctx: ctx.clone(),
            workflow_id,
            rx: Mutex::new(Some(rx)),
            _marker: PhantomData,
        }),
        ErasedHandle::Polling { workflow_id } => boxed_polling::<R>(ctx.clone(), workflow_id),
    })
}

/// The erased run core (operates over the encoded `*string` input form).
pub(crate) async fn run_workflow_erased(
    ctx: &Arc<DbosContext>,
    name: &str,
    encoded_input: Option<String>,
    input_serialization: String,
    opts: RunOptions,
    flags: RunFlags,
    child: Option<ChildSpawn>,
) -> Result<ErasedHandle, DbosError> {
    // Registry lookup.
    let (erased, max_retries, resolved_name, class_name, config_name) = {
        let reg = ctx.registry.read().unwrap();
        let Some(entry) = reg.get(name) else {
            return Err(DbosError::non_existent_workflow(name));
        };
        (
            entry.erased.clone(),
            entry.max_retries,
            entry.name.clone(),
            entry.class_name.clone(),
            entry.config_name.clone(),
        )
    };

    let application_version = opts
        .application_version
        .clone()
        .unwrap_or_else(|| ctx.application_version().to_string());

    let queue_name = opts.queue.clone().unwrap_or_default();
    let partition_key = opts.queue_partition_key.clone().unwrap_or_default();
    let dedup_id = opts.deduplication_id.clone().unwrap_or_default();

    // Validation.
    if opts.delay.is_some() && queue_name.is_empty() {
        return Err(exec_err("", "delay provided but queue name is missing"));
    }
    if !partition_key.is_empty() && queue_name.is_empty() {
        return Err(exec_err("", "partition key provided but queue name is missing"));
    }
    if !partition_key.is_empty() && !dedup_id.is_empty() {
        return Err(exec_err("", "partition key and deduplication ID cannot be used together"));
    }
    if opts.deduplication_policy != crate::workflow::DeduplicationPolicy::Reject {
        if dedup_id.is_empty() {
            return Err(exec_err("", "a deduplication policy requires a deduplication ID"));
        }
        if queue_name.is_empty() {
            return Err(exec_err("", "a deduplication policy requires a queue name"));
        }
    }

    let workflow_id = opts.workflow_id.clone().unwrap_or_else(|| match &child {
        Some(c) => format!("{}-{}", c.parent_workflow_id, c.step_id),
        None => new_uuid(),
    });

    let status = if !queue_name.is_empty() {
        if opts.delay.is_some() {
            WorkflowStatusType::Delayed
        } else {
            WorkflowStatusType::Enqueued
        }
    } else {
        WorkflowStatusType::Pending
    };

    let delay_until_ms = opts.delay.map(|d| now_ms() + d.as_millis() as i64);

    // Timeout: store the duration always; for direct (non-queued) runs also
    // materialize the absolute deadline now (queued runs get it at dequeue).
    let wf_timeout_ms = opts.timeout.map(|t| t.as_millis() as i64);
    let wf_deadline_ms = if queue_name.is_empty() {
        opts.timeout.map(|t| now_ms() + t.as_millis() as i64)
    } else {
        None
    };

    let new_status = NewWorkflowStatus {
        id: workflow_id.clone(),
        status: Some(status),
        name: resolved_name.clone(),
        class_name,
        config_name,
        application_version,
        application_id: ctx.application_id().to_string(),
        executor_id: ctx.executor_id().to_string(),
        created_at_ms: now_ms(),
        updated_at_ms: None,
        deadline_ms: wf_deadline_ms,
        timeout_ms: wf_timeout_ms,
        input: encoded_input.clone(),
        queue_name: queue_name.clone(),
        deduplication_id: dedup_id.clone(),
        priority: opts.priority as i64,
        authenticated_user: opts.authenticated_user.clone().unwrap_or_default(),
        assumed_role: opts.assumed_role.clone().unwrap_or_default(),
        authenticated_roles: opts.authenticated_roles.clone(),
        queue_partition_key: partition_key,
        parent_workflow_id: child
            .as_ref()
            .map(|c| c.parent_workflow_id.clone())
            .unwrap_or_default(),
        delay_until_ms,
        serialization: input_serialization.clone(),
    };

    let record_child = child.as_ref().map(|c| ChildRecord {
        parent_workflow_id: c.parent_workflow_id.clone(),
        step_id: c.step_id,
        step_name: resolved_name.clone(),
        child_workflow_id: workflow_id.clone(),
    });

    let owner_xid = new_uuid();
    let insert_result = ctx
        .db
        .insert_workflow_status(InsertWorkflowStatusInput {
            status: new_status,
            max_retries,
            owner_xid: owner_xid.clone(),
            increment_attempts: flags.is_dequeue || flags.is_recovery,
            record_child,
        })
        .await?;

    let already_active = ctx.active_workflow_ids.contains_key(&workflow_id);
    let should_skip = !queue_name.is_empty()
        || insert_result.status == WorkflowStatusType::Success
        || insert_result.status == WorkflowStatusType::Error
        || (!flags.is_dequeue && !flags.is_recovery && insert_result.owner_xid != owner_xid)
        || already_active;

    if should_skip {
        return Ok(ErasedHandle::Polling { workflow_id });
    }

    // Build the workflow execution context.
    let state = Arc::new(WorkflowState {
        workflow_id: workflow_id.clone(),
        step_id: AtomicI64::new(-1),
        is_portable: opts.portable,
        authenticated_user: opts.authenticated_user.clone().unwrap_or_default(),
        assumed_role: opts.assumed_role.clone().unwrap_or_default(),
        authenticated_roles: opts.authenticated_roles.clone(),
    });
    let wfctx = WfCtx { ctx: ctx.clone(), state, is_within_step: false };

    ctx.active_workflow_ids.insert(
        workflow_id.clone(),
        ActiveEntry {
            queue_name: insert_result.queue_name.clone().unwrap_or_default(),
            queue_partition_key: insert_result.queue_partition_key.clone().unwrap_or_default(),
        },
    );

    // Deadline → cancel bridge: a watcher cancels the workflow in the DB when the
    // (durable) deadline passes; the executing body then aborts at its next step.
    let watcher_done = tokio_util::sync::CancellationToken::new();
    if let Some(deadline) = insert_result.deadline_ms {
        let wctx = ctx.clone();
        let wid = workflow_id.clone();
        let done = watcher_done.clone();
        let engine_token = ctx.token.clone();
        ctx.tracker.spawn(async move {
            let remaining = (deadline - now_ms()).max(0) as u64;
            tokio::select! {
                _ = tokio::time::sleep(std::time::Duration::from_millis(remaining)) => {
                    let _ = wctx.db.cancel_workflow(&wid).await;
                }
                _ = done.cancelled() => {}
                _ = engine_token.cancelled() => {}
            }
        });
    }

    let (tx, rx) = oneshot::channel();
    let task_ctx = ctx.clone();
    let wf_id = workflow_id.clone();
    let outcome_serialization = input_serialization.clone();
    let exec_done = watcher_done;

    ctx.tracker.spawn(async move {
        let result = erased(wfctx, encoded_input, input_serialization).await;
        exec_done.cancel();

        let outcome: Result<EncodedOutcome, DbosError> = match result {
            Ok(output) => {
                let _ = task_ctx
                    .db
                    .update_workflow_outcome(&wf_id, WorkflowStatusType::Success, &output, "")
                    .await;
                Ok(EncodedOutcome { output, serialization: outcome_serialization })
            }
            Err(e) if e.is_code(DbosErrorCode::ConflictingId) => {
                // Another execution owns this id; wait for its durable result.
                match task_ctx.db.await_workflow_result(&wf_id, DB_RETRY_INTERVAL).await {
                    Ok(ar) => {
                        if let Some(es) = &ar.error {
                            Err(deserialize_workflow_error(&Some(es.clone()), &ar.serialization)
                                .unwrap_or(e))
                        } else {
                            Ok(EncodedOutcome { output: ar.output, serialization: ar.serialization })
                        }
                    }
                    Err(await_err) => Err(await_err),
                }
            }
            Err(e) => {
                let serialized = serialize_workflow_error(&e, &outcome_serialization);
                let _ = task_ctx
                    .db
                    .update_workflow_outcome(&wf_id, WorkflowStatusType::Error, &None, &serialized)
                    .await;
                Err(e)
            }
        };

        task_ctx.active_workflow_ids.remove(&wf_id);
        let _ = tx.send(outcome);
    });

    Ok(ErasedHandle::Channel { workflow_id, rx })
}

fn exec_err(workflow_id: &str, msg: &str) -> DbosError {
    let mut e = DbosError::new(DbosErrorCode::WorkflowExecution, msg.to_string());
    if !workflow_id.is_empty() {
        e.workflow_id = Some(workflow_id.to_string());
    }
    e
}

impl WfCtx {
    /// Spawn a child workflow from within a workflow. The child gets a
    /// deterministic id (`{parent}-{stepID}`), inherits the parent's auth
    /// identity, and is recorded idempotently so replay returns the same child.
    pub async fn run_child_workflow<P, R>(
        &self,
        name: &str,
        input: P,
        opts: RunOptions,
    ) -> Result<Box<dyn WorkflowHandle<R>>, DbosError>
    where
        P: Serialize + Send + 'static,
        R: DeserializeOwned + Send + Sync + 'static,
    {
        if self.is_within_step {
            return Err(DbosError::step_execution(
                self.workflow_id(),
                name.to_string(),
                std::io::Error::other("cannot spawn child workflow from within a step"),
            ));
        }
        let step_id = self.next_step_id();

        // Inherit the parent's auth identity unless the caller overrode it.
        let mut opts = opts;
        if opts.authenticated_user.is_none() {
            opts.authenticated_user = Some(self.state.authenticated_user.clone());
        }
        if opts.assumed_role.is_none() {
            opts.assumed_role = Some(self.state.assumed_role.clone());
        }
        if opts.authenticated_roles.is_empty() {
            opts.authenticated_roles = self.state.authenticated_roles.clone();
        }

        let parent_id = self.workflow_id().to_string();

        // Already recorded (replay): return a polling handle to the child.
        if let Some(child_id) = self.ctx.db.check_child_workflow(&parent_id, step_id).await? {
            return Ok(boxed_polling::<R>(self.ctx.clone(), child_id));
        }

        let (encoded, serialization_name) = if opts.portable {
            (encode_portable_args(&input)?, PORTABLE_SERIALIZER_NAME.to_string())
        } else {
            let encoder = resolve_encoder(false, self.ctx.serializer.as_ref());
            (serialization::encode(encoder.as_ref(), &input)?, encoder.name().to_string())
        };

        let handle = run_workflow_erased(
            &self.ctx,
            name,
            encoded,
            serialization_name,
            opts,
            RunFlags::default(),
            Some(ChildSpawn { parent_workflow_id: parent_id, step_id }),
        )
        .await?;

        Ok(match handle {
            ErasedHandle::Channel { workflow_id, rx } => Box::new(ChannelHandle::<R> {
                ctx: self.ctx.clone(),
                workflow_id,
                rx: Mutex::new(Some(rx)),
                _marker: PhantomData,
            }),
            ErasedHandle::Polling { workflow_id } => boxed_polling::<R>(self.ctx.clone(), workflow_id),
        })
    }
}
