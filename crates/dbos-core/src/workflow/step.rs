//! Durable steps and durable sleep.
//!
//! A step's result (success or error) is memoized in `operation_outputs` keyed
//! by `(workflow_uuid, function_id)`; on replay the recorded result is returned
//! instead of re-executing.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::constants::{
    DEFAULT_STEP_BACKOFF_FACTOR, DEFAULT_STEP_BASE_INTERVAL, DEFAULT_STEP_MAX_INTERVAL,
};
use crate::context::state::{StepCtx, WfCtx};
use crate::db::RecordOperationResultInput;
use crate::error::{DbosError, DbosErrorCode};
use crate::serialization::{
    self, JsonSerializer, Serializer, deserialize_workflow_error, resolve_decoder, resolve_encoder,
    serialize_workflow_error,
};
use crate::util::now_ms;

/// Configuration for a step's retry behavior.
#[derive(Clone)]
pub struct StepOptions {
    /// Maximum retry attempts (0 = run once, no retry).
    pub max_retries: i64,
    /// Initial delay between retries.
    pub base_interval: Duration,
    /// Maximum delay between retries.
    pub max_interval: Duration,
    /// Exponential backoff multiplier.
    pub backoff_factor: f64,
    /// Optional predicate gating the *next* retry; `false` stops retrying.
    pub retry_predicate: Option<Arc<dyn Fn(&DbosError) -> bool + Send + Sync>>,
}

impl Default for StepOptions {
    fn default() -> Self {
        Self {
            max_retries: 0,
            base_interval: DEFAULT_STEP_BASE_INTERVAL,
            max_interval: DEFAULT_STEP_MAX_INTERVAL,
            backoff_factor: DEFAULT_STEP_BACKOFF_FACTOR,
            retry_predicate: None,
        }
    }
}

impl WfCtx {
    /// Run a closure as a durable, memoized step (no retry).
    pub async fn run_step<R, F, Fut>(&self, name: &str, f: F) -> Result<R, DbosError>
    where
        R: Serialize + DeserializeOwned + Send + 'static,
        F: FnOnce(StepCtx) -> Fut + Send,
        Fut: Future<Output = Result<R, DbosError>> + Send,
    {
        if self.is_within_step {
            return f(self.inline_step_ctx()).await;
        }
        let workflow_id = self.workflow_id().to_string();
        let step_id = self.next_step_id();

        if let Some(replayed) = self.check_step::<R>(step_id, name).await? {
            return replayed;
        }

        let step_ctx = self.step_ctx(step_id);
        let started = now_ms();
        let result = f(step_ctx).await;
        let completed = now_ms();
        self.record_step(&workflow_id, step_id, name, started, completed, &result).await?;
        result
    }

    /// Run a closure as a durable, memoized step with retry options. The closure
    /// must be re-callable (`Fn`) since it may run multiple times.
    pub async fn run_step_opts<R, F, Fut>(
        &self,
        name: &str,
        opts: StepOptions,
        f: F,
    ) -> Result<R, DbosError>
    where
        R: Serialize + DeserializeOwned + Send + 'static,
        F: Fn(StepCtx) -> Fut + Send + Sync,
        Fut: Future<Output = Result<R, DbosError>> + Send,
    {
        if self.is_within_step {
            return f(self.inline_step_ctx()).await;
        }
        let workflow_id = self.workflow_id().to_string();
        let step_id = self.next_step_id();

        if let Some(replayed) = self.check_step::<R>(step_id, name).await? {
            return replayed;
        }

        let step_ctx = self.step_ctx(step_id);
        let started = now_ms();
        let result = self.execute_with_retry(&workflow_id, name, &opts, &f, step_ctx).await;
        let completed = now_ms();
        self.record_step(&workflow_id, step_id, name, started, completed, &result).await?;
        result
    }

    /// Durably sleep for `duration`. The absolute wake time is checkpointed, so
    /// on replay only the remaining time is slept.
    pub async fn sleep(&self, duration: Duration) -> Result<Duration, DbosError> {
        if self.is_within_step {
            return Err(DbosError::step_execution(
                self.workflow_id(),
                "DBOS.sleep".to_string(),
                std::io::Error::other("cannot call Sleep within a step"),
            ));
        }
        let workflow_id = self.workflow_id().to_string();
        let step_id = self.next_step_id();
        let json = JsonSerializer;

        let end_ms: i64 = match self
            .ctx
            .db
            .check_operation_execution(&workflow_id, step_id, "DBOS.sleep")
            .await?
        {
            Some(rec) => serialization::decode::<i64>(&json, &rec.output)?,
            None => {
                let end = now_ms() + duration.as_millis() as i64;
                let encoded = serialization::encode::<i64>(&json, &end)?;
                let rec = RecordOperationResultInput {
                    workflow_id: workflow_id.clone(),
                    step_id,
                    step_name: "DBOS.sleep".to_string(),
                    output: encoded,
                    error: None,
                    child_workflow_id: None,
                    started_at_ms: now_ms(),
                    completed_at_ms: now_ms(),
                    serialization: JsonSerializer.name().to_string(),
                };
                match self.ctx.db.record_operation_result(rec).await {
                    Ok(()) => end,
                    Err(e) if e.is_code(DbosErrorCode::ConflictingId) => {
                        // Another process recorded it; read the recorded wake time.
                        let rec = self
                            .ctx
                            .db
                            .check_operation_execution(&workflow_id, step_id, "DBOS.sleep")
                            .await?
                            .ok_or_else(|| {
                                DbosError::step_execution(
                                    workflow_id.clone(),
                                    "DBOS.sleep".to_string(),
                                    std::io::Error::other("no recorded sleep end time"),
                                )
                            })?;
                        serialization::decode::<i64>(&json, &rec.output)?
                    }
                    Err(e) => return Err(e),
                }
            }
        };

        let remaining = (end_ms - now_ms()).max(0);
        let dur = Duration::from_millis(remaining as u64);
        tokio::select! {
            _ = tokio::time::sleep(dur) => {}
            _ = self.ctx.token.cancelled() => {}
        }
        Ok(dur)
    }

    // --- internals ---

    fn step_ctx(&self, step_id: i64) -> StepCtx {
        StepCtx {
            ctx: self.ctx.clone(),
            workflow_id: self.workflow_id().to_string(),
            step_id,
        }
    }

    fn inline_step_ctx(&self) -> StepCtx {
        StepCtx {
            ctx: self.ctx.clone(),
            workflow_id: self.workflow_id().to_string(),
            step_id: self.step_id(),
        }
    }

    /// Returns the replayed result if the step is recorded, else `None`.
    async fn check_step<R: DeserializeOwned>(
        &self,
        step_id: i64,
        name: &str,
    ) -> Result<Option<Result<R, DbosError>>, DbosError> {
        let Some(recorded) =
            self.ctx.db.check_operation_execution(self.workflow_id(), step_id, name).await?
        else {
            return Ok(None);
        };
        if let Some(err_str) = &recorded.error {
            let e = deserialize_workflow_error(&Some(err_str.clone()), &recorded.serialization)
                .unwrap_or_else(|| {
                    DbosError::step_execution(
                        self.workflow_id(),
                        name.to_string(),
                        std::io::Error::other("recorded step error"),
                    )
                });
            return Ok(Some(Err(e)));
        }
        let decoder = resolve_decoder(&recorded.serialization, self.ctx.serializer.as_ref())?;
        Ok(Some(Ok(serialization::decode::<R>(decoder.as_ref(), &recorded.output)?)))
    }

    async fn record_step<R: Serialize>(
        &self,
        workflow_id: &str,
        step_id: i64,
        name: &str,
        started: i64,
        completed: i64,
        result: &Result<R, DbosError>,
    ) -> Result<(), DbosError> {
        let encoder = resolve_encoder(self.is_portable(), self.ctx.serializer.as_ref());
        let serialization_name = encoder.name().to_string();
        let input = match result {
            Ok(value) => RecordOperationResultInput {
                workflow_id: workflow_id.to_string(),
                step_id,
                step_name: name.to_string(),
                output: serialization::encode::<R>(encoder.as_ref(), value)?,
                error: None,
                child_workflow_id: None,
                started_at_ms: started,
                completed_at_ms: completed,
                serialization: serialization_name,
            },
            Err(e) => RecordOperationResultInput {
                workflow_id: workflow_id.to_string(),
                step_id,
                step_name: name.to_string(),
                output: None,
                error: Some(serialize_workflow_error(e, &serialization_name)),
                child_workflow_id: None,
                started_at_ms: started,
                completed_at_ms: completed,
                serialization: serialization_name,
            },
        };
        if let Err(rec_err) = self.ctx.db.record_operation_result(input).await {
            if rec_err.is_code(DbosErrorCode::ConflictingId) {
                return Err(rec_err);
            }
            return Err(DbosError::step_execution_dbos(
                workflow_id.to_string(),
                name.to_string(),
                rec_err,
            ));
        }
        Ok(())
    }

    async fn execute_with_retry<R, F, Fut>(
        &self,
        workflow_id: &str,
        name: &str,
        opts: &StepOptions,
        f: &F,
        step_ctx: StepCtx,
    ) -> Result<R, DbosError>
    where
        F: Fn(StepCtx) -> Fut,
        Fut: Future<Output = Result<R, DbosError>>,
    {
        let mut joined: Vec<String> = Vec::new();
        let mut runs: i64 = 0;
        loop {
            let result = f(step_ctx.clone()).await;
            runs += 1;
            let err = match result {
                Ok(v) => return Ok(v),
                Err(e) => e,
            };
            joined.push(err.to_string());

            if runs > opts.max_retries {
                if opts.max_retries <= 0 {
                    return Err(err);
                }
                return Err(DbosError::max_step_retries_exceeded(
                    workflow_id.to_string(),
                    name.to_string(),
                    opts.max_retries,
                    std::io::Error::other(joined.join("; ")),
                ));
            }
            if let Some(pred) = &opts.retry_predicate {
                if !pred(&err) {
                    return Err(err);
                }
            }

            let delay = backoff_delay(opts, runs);
            tokio::select! {
                _ = tokio::time::sleep(delay) => {}
                _ = self.ctx.token.cancelled() => {
                    return Err(DbosError::step_execution(
                        workflow_id.to_string(),
                        name.to_string(),
                        std::io::Error::other("context cancelled during retry"),
                    ));
                }
            }
        }
    }
}

/// Exponential backoff with light jitter (no external RNG dependency).
fn backoff_delay(opts: &StepOptions, run: i64) -> Duration {
    let base = opts.base_interval.as_secs_f64();
    let factor = opts.backoff_factor.max(1.0);
    let raw = base * factor.powi((run - 1).max(0) as i32);
    let capped = raw.min(opts.max_interval.as_secs_f64());
    let jitter = 0.95 + ((now_ms() as u64 % 100) as f64) / 1000.0; // [0.95, 1.05)
    Duration::from_secs_f64((capped * jitter).max(0.0))
}
