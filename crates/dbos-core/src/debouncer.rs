//! Debouncer — coalesce rapid calls for the same key into a single target run.
//!
//! Each [`Debouncer::debounce`] call sharing a `key` pushes the target workflow's
//! start time back by `delay`. An optional timeout caps how far the start can be
//! pushed from the FIRST invocation. After the input "settles" (no new call
//! within `delay`, or the timeout cap is reached), the target workflow is
//! enqueued exactly once with the most recent input. The returned handle always
//! targets the eventual target workflow.
//!
//! # Mechanism
//!
//! An internal debouncer workflow (one per key) collects inputs and sleeps until
//! the target start time, then enqueues the target once. New `debounce` calls
//! either *start* that internal workflow (first call) or `Send` it an updated
//! input, ACKed via a per-message [`WfCtx::set_event`] keyed by the message id.
//! The internal workflow reads the clock through durable steps (`run_step`) so it
//! is deterministic across replay/recovery; push-back is `now + delay`, capped at
//! `start + timeout`.
//!
//! # Coalescing
//!
//! "One internal workflow per key" is enforced via a queue **deduplication id**
//! (`_dbos_debounce_<key>`) on the internal queue: the first caller of a cycle
//! wins the dedup slot and enqueues a fresh internal workflow (a uuid) carrying a
//! fresh target-workflow id; concurrent callers get a `QueueDeduplicated` error,
//! discover the holder via [`SystemDatabase::get_deduplicated_workflow`], and
//! `Send` it an updated input (ACKed via a per-message [`WfCtx::set_event`]). The
//! dedup slot is released when the internal workflow reaches a terminal state
//! (its `deduplication_id` is cleared), so **reusing a key after a previous cycle
//! completes starts a fresh cycle** with new internal/target ids — exactly the
//! common case (e.g. debounce-per-document). The ACK wait is a hardcoded 2s.

use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::constants::DBOS_INTERNAL_QUEUE_NAME;
use crate::context::{DbosContext, WfCtx};
use crate::error::DbosError;
use crate::serialization::{self, resolve_encoder};
use crate::workflow::handle::{WorkflowHandle, boxed_polling};
use crate::workflow::run::run_workflow_erased;
use crate::workflow::{RegisterOptions, RunFlags, RunOptions, register_workflow_opts};

/// The topic the internal debouncer workflow receives update messages on.
const DEBOUNCER_TOPIC: &str = "_dbos_debouncer_topic";
/// How long a caller waits for the internal workflow to ACK its update message.
const ACK_TIMEOUT: Duration = Duration::from_secs(2);

/// The input the internal debouncer workflow is started with.
#[derive(Debug, Clone, Serialize, serde::Deserialize)]
struct DebouncerInput<P> {
    initial_input: P,
    target_workflow_name: String,
    target_workflow_id: String,
    delay_ms: i64,
    timeout_ms: i64,
}

/// An update message sent to a running internal debouncer workflow.
#[derive(Debug, Clone, Serialize, serde::Deserialize)]
struct DebounceMessage<P> {
    input: P,
    delay_ms: i64,
    /// Message id used for the ACK event; empty means "no ACK expected".
    id: String,
}

/// Coalesces rapid calls for the same key into a single target-workflow run.
///
/// Construct with [`Debouncer::new`] BEFORE [`crate::DbosContext::launch`] — it
/// registers the internal debouncer workflow, which (like all workflows) must be
/// registered pre-launch.
pub struct Debouncer<P, R> {
    target_workflow_name: String,
    internal_name: String,
    timeout: Duration,
    _marker: std::marker::PhantomData<fn() -> (P, R)>,
}

impl<P, R> Debouncer<P, R>
where
    P: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    R: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    /// Create a debouncer for the registered workflow `target_workflow_name`.
    ///
    /// Registers the internal debouncer workflow under a name derived from the
    /// target (`_dbos_internal_debouncer_<target>`), idempotently. Must be called
    /// before launch; `timeout` of zero means "no cap on push-back".
    pub fn new(
        ctx: &Arc<DbosContext>,
        target_workflow_name: &str,
        timeout: Duration,
    ) -> Result<Self, DbosError> {
        if ctx.is_launched() {
            return Err(DbosError::initialization(
                "cannot create a debouncer after the context has been launched",
            ));
        }
        let internal_name = format!("_dbos_internal_debouncer_{target_workflow_name}");
        register_internal_debouncer::<P>(ctx, &internal_name)?;
        Ok(Self {
            target_workflow_name: target_workflow_name.to_string(),
            internal_name,
            timeout,
            _marker: std::marker::PhantomData,
        })
    }

    /// Debounce a call: push the target start time back by `delay` and update the
    /// pending input to `input`. Returns a handle to the (eventual) target run.
    pub async fn debounce(
        &self,
        ctx: &Arc<DbosContext>,
        key: &str,
        delay: Duration,
        input: P,
    ) -> Result<Box<dyn WorkflowHandle<R>>, DbosError> {
        let dedup_id = format!("_dbos_debounce_{key}");
        let delay_ms = delay.as_millis() as i64;
        let timeout_ms = self.timeout.as_millis() as i64;

        loop {
            // Try to win the dedup slot and start a fresh internal workflow for
            // this cycle (its own uuid + a fresh target uuid).
            let internal_id = crate::util::new_uuid();
            let target_id = format!("_dbos_debounce_target_{}", crate::util::new_uuid());
            let dinput = DebouncerInput {
                initial_input: input.clone(),
                target_workflow_name: self.target_workflow_name.clone(),
                target_workflow_id: target_id.clone(),
                delay_ms,
                timeout_ms,
            };
            let encoder = resolve_encoder(false, ctx.serializer.as_ref());
            let encoded = serialization::encode(encoder.as_ref(), &dinput)?;
            let opts = RunOptions {
                workflow_id: Some(internal_id.clone()),
                queue: Some(DBOS_INTERNAL_QUEUE_NAME.to_string()),
                deduplication_id: Some(dedup_id.clone()),
                ..Default::default()
            };
            match run_workflow_erased(
                ctx,
                &self.internal_name,
                encoded,
                encoder.name().to_string(),
                opts,
                RunFlags::default(),
                None,
            )
            .await
            {
                // We won the slot: we are the first caller of this cycle.
                Ok(_) => return Ok(boxed_polling::<R>(ctx.clone(), target_id)),
                // Another caller already holds the slot: attach to it.
                Err(e) if e.is_code(crate::error::DbosErrorCode::QueueDeduplicated) => {
                    let Some(existing_internal) = ctx
                        .db
                        .get_deduplicated_workflow(DBOS_INTERNAL_QUEUE_NAME, &dedup_id)
                        .await?
                    else {
                        // Slot was cleared between our insert and the lookup; retry.
                        continue;
                    };

                    // Send the updated input and wait for the internal wf to ACK.
                    let message_id = crate::util::new_uuid();
                    let msg =
                        DebounceMessage { input: input.clone(), delay_ms, id: message_id.clone() };
                    crate::workflow::comms::send(ctx, &existing_internal, msg, DEBOUNCER_TOPIC)
                        .await?;
                    let acked: Option<bool> = crate::workflow::comms::get_event(
                        ctx,
                        &existing_internal,
                        &message_id,
                        ACK_TIMEOUT,
                    )
                    .await?;
                    if acked.is_none() {
                        // No ACK before the timeout: the internal wf already
                        // executed and exited. Loop to start a fresh cycle.
                        continue;
                    }

                    // Converge on the running internal wf's target id.
                    let existing = ctx.db.get_workflow_status(&existing_internal).await?;
                    let resolved_target_id = existing
                        .as_ref()
                        .and_then(|s| {
                            s.input
                                .as_deref()
                                .and_then(|raw| decode_target_id::<P>(ctx, raw, &s.serialization))
                        })
                        .unwrap_or(target_id);
                    return Ok(boxed_polling::<R>(ctx.clone(), resolved_target_id));
                }
                Err(e) => return Err(e),
            }
        }
    }
}

/// Decode the internal wf's stored `DebouncerInput` to recover the target id.
fn decode_target_id<P: DeserializeOwned>(
    ctx: &Arc<DbosContext>,
    raw: &str,
    serialization: &str,
) -> Option<String> {
    let decoder = serialization::resolve_decoder(serialization, ctx.serializer.as_ref()).ok()?;
    let dinput: DebouncerInput<P> =
        serialization::decode(decoder.as_ref(), &Some(raw.to_string())).ok()?;
    Some(dinput.target_workflow_id)
}

/// Register the internal debouncer workflow body (idempotent on its name).
fn register_internal_debouncer<P>(
    ctx: &Arc<DbosContext>,
    internal_name: &str,
) -> Result<(), DbosError>
where
    P: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    if ctx.registry.read().unwrap().contains_key(internal_name) {
        return Ok(());
    }
    let body = move |wfctx: WfCtx, input: DebouncerInput<P>| {
        async move { internal_debouncer_body::<P>(wfctx, input).await }
    };
    register_workflow_opts::<DebouncerInput<P>, (), _, _>(
        ctx,
        internal_name,
        body,
        RegisterOptions::default(),
    )
}

/// The internal debouncer workflow: collect updates, sleep to the target start
/// time, then enqueue the target workflow exactly once with the latest input.
async fn internal_debouncer_body<P>(
    ctx: WfCtx,
    input: DebouncerInput<P>,
) -> Result<(), DbosError>
where
    P: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    // First-creation time as a durable step (stable across replay).
    let start_ms = ctx
        .run_step("DBOS.debounce.startTime", |_s| async move { Ok::<i64, DbosError>(now_ms()) })
        .await?;
    let timeout_ms = input.timeout_ms;
    let max_start_ms = start_ms + timeout_ms;

    let mut current_input = input.initial_input.clone();
    let mut target_start_ms = start_ms + input.delay_ms;
    if timeout_ms > 0 && target_start_ms > max_start_ms {
        target_start_ms = max_start_ms;
    }

    // Collect-and-delay loop.
    loop {
        let now = ctx
            .run_step("DBOS.debounce.loopTime", |_s| async move { Ok::<i64, DbosError>(now_ms()) })
            .await?;
        let remaining = target_start_ms - now;
        if remaining <= 0 {
            break;
        }
        let recv_timeout = Duration::from_millis(remaining as u64);
        match ctx.recv::<DebounceMessage<P>>(DEBOUNCER_TOPIC, recv_timeout).await {
            Ok(msg) => {
                current_input = msg.input;
                let mut new_target = now + msg.delay_ms;
                if timeout_ms > 0 && new_target > max_start_ms {
                    new_target = max_start_ms;
                }
                target_start_ms = new_target;
                if !msg.id.is_empty() {
                    if let Err(e) = ctx.set_event(&msg.id, true).await {
                        tracing::error!(error = %e, "failed to ACK debounce message");
                    }
                }
            }
            // Timeout (or error): execute with whatever input is current.
            Err(_) => break,
        }
    }

    // Enqueue the target workflow exactly once with the latest input.
    let encoder = resolve_encoder(ctx.is_portable(), ctx.context().serializer.as_ref());
    let encoded = serialization::encode(encoder.as_ref(), &current_input)?;
    let opts = RunOptions {
        workflow_id: Some(input.target_workflow_id.clone()),
        queue: Some(DBOS_INTERNAL_QUEUE_NAME.to_string()),
        ..Default::default()
    };
    run_workflow_erased(
        ctx.context(),
        &input.target_workflow_name,
        encoded,
        encoder.name().to_string(),
        opts,
        RunFlags::default(),
        None,
    )
    .await?;
    Ok(())
}

use crate::util::now_ms;
