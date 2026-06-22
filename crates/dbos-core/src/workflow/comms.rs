//! Durable notifications (send/recv) and events (set/get).
//!
//! On SQLite (no LISTEN/NOTIFY) waiting is implemented by polling the durable
//! state with a checkpointed deadline, so a crash mid-wait resumes correctly.

use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::constants::DBOS_NULL_TOPIC;
use crate::context::DbosContext;
use crate::context::state::WfCtx;
use crate::db::RecordOperationResultInput;
use crate::error::DbosError;
use crate::serialization::{self, JsonSerializer, Serializer, resolve_decoder, resolve_encoder};
use crate::util::now_ms;

/// Polling cadence while waiting for a notification/event on SQLite.
const POLL_INTERVAL: Duration = Duration::from_millis(50);

impl WfCtx {
    /// Send a message to another workflow (durable step). Sending to a
    /// nonexistent destination is a [`DbosError::non_existent_workflow`].
    pub async fn send<P: Serialize + Send + 'static>(
        &self,
        destination_id: &str,
        message: P,
        topic: &str,
    ) -> Result<(), DbosError> {
        if self.is_within_step {
            return Err(DbosError::step_execution(
                self.workflow_id(),
                "DBOS.send".to_string(),
                std::io::Error::other("cannot call Send within a step"),
            ));
        }
        let encoder = resolve_encoder(self.is_portable(), self.ctx.serializer.as_ref());
        let encoded = serialization::encode(encoder.as_ref(), &message)?;
        let serialization_name = encoder.name().to_string();
        let topic = topic.to_string();
        let dest = destination_id.to_string();
        // Memoized as a step so replay does not re-deliver.
        self.run_step("DBOS.send", move |s| async move {
            s.ctx
                .db
                .send_notification(&dest, &resolve_topic(&topic), &encoded, &serialization_name)
                .await
        })
        .await
    }

    /// Receive a message for this workflow, waiting up to `timeout`. FIFO,
    /// exactly-once. Returns a [`DbosError::timeout`] error on timeout.
    pub async fn recv<R: DeserializeOwned + Send + 'static>(
        &self,
        topic: &str,
        timeout: Duration,
    ) -> Result<R, DbosError> {
        if self.is_within_step {
            return Err(DbosError::step_execution(
                self.workflow_id(),
                "DBOS.recv".to_string(),
                std::io::Error::other("cannot call Recv within a step"),
            ));
        }
        let workflow_id = self.workflow_id().to_string();
        let step_id = self.next_step_id();
        let sleep_step_id = self.next_step_id();
        let topic = resolve_topic(topic);

        // Replay: return the recorded message (or timeout error).
        if let Some(rec) = self
            .ctx
            .db
            .check_operation_execution(&workflow_id, step_id, "DBOS.recv")
            .await?
        {
            return replay_message::<R>(&self.ctx, rec);
        }

        let deadline = self.durable_deadline_ms(sleep_step_id, timeout).await?;
        self.wait_until(deadline, |db| {
            let dest = workflow_id.clone();
            let topic = topic.clone();
            async move { db.has_unconsumed_notification(&dest, &topic).await }
        })
        .await?;

        let consumed = self
            .ctx
            .db
            .consume_oldest_notification(&workflow_id, &topic)
            .await?;
        let receiver_ser = resolve_encoder(self.is_portable(), self.ctx.serializer.as_ref())
            .name()
            .to_string();

        match consumed {
            Some((message, serialization)) => {
                self.ctx
                    .db
                    .record_operation_result(RecordOperationResultInput {
                        workflow_id: workflow_id.clone(),
                        step_id,
                        step_name: "DBOS.recv".to_string(),
                        output: message.clone(),
                        error: None,
                        child_workflow_id: None,
                        started_at_ms: now_ms(),
                        completed_at_ms: now_ms(),
                        serialization: serialization.clone(),
                    })
                    .await?;
                let decoder = resolve_decoder(&serialization, self.ctx.serializer.as_ref())?;
                serialization::decode::<R>(decoder.as_ref(), &message)
            }
            None => {
                let err = DbosError::timeout(
                    &workflow_id,
                    "DBOS.recv",
                    format!("no message received within {timeout:?}"),
                );
                let serialized =
                    crate::serialization::serialize_workflow_error(&err, &receiver_ser);
                self.ctx
                    .db
                    .record_operation_result(RecordOperationResultInput {
                        workflow_id,
                        step_id,
                        step_name: "DBOS.recv".to_string(),
                        output: None,
                        error: Some(serialized),
                        child_workflow_id: None,
                        started_at_ms: now_ms(),
                        completed_at_ms: now_ms(),
                        serialization: receiver_ser,
                    })
                    .await?;
                Err(err)
            }
        }
    }

    /// Set a key/value event on this workflow (durable step). Overwrites any
    /// previous value for the key.
    pub async fn set_event<P: Serialize + Send + 'static>(
        &self,
        key: &str,
        message: P,
    ) -> Result<(), DbosError> {
        if self.is_within_step {
            return Err(DbosError::step_execution(
                self.workflow_id(),
                "DBOS.setEvent".to_string(),
                std::io::Error::other("cannot call SetEvent within a step"),
            ));
        }
        let workflow_id = self.workflow_id().to_string();
        let step_id = self.next_step_id();

        if self
            .ctx
            .db
            .check_operation_execution(&workflow_id, step_id, "DBOS.setEvent")
            .await?
            .is_some()
        {
            return Ok(());
        }

        let encoder = resolve_encoder(self.is_portable(), self.ctx.serializer.as_ref());
        let encoded = serialization::encode(encoder.as_ref(), &message)?;
        let serialization_name = encoder.name().to_string();

        self.ctx
            .db
            .set_event(&workflow_id, step_id, key, &encoded, &serialization_name)
            .await?;
        self.ctx
            .db
            .record_operation_result(RecordOperationResultInput {
                workflow_id,
                step_id,
                step_name: "DBOS.setEvent".to_string(),
                output: None,
                error: None,
                child_workflow_id: None,
                started_at_ms: now_ms(),
                completed_at_ms: now_ms(),
                serialization: serialization_name,
            })
            .await?;
        Ok(())
    }

    /// Get an event from a target workflow, waiting up to `timeout` (durable step
    /// when called inside a workflow).
    pub async fn get_event<R: DeserializeOwned + Send + 'static>(
        &self,
        target_workflow_id: &str,
        key: &str,
        timeout: Duration,
    ) -> Result<R, DbosError> {
        if self.is_within_step {
            return Err(DbosError::step_execution(
                self.workflow_id(),
                "DBOS.getEvent".to_string(),
                std::io::Error::other("cannot call GetEvent within a step"),
            ));
        }
        let workflow_id = self.workflow_id().to_string();
        let step_id = self.next_step_id();
        let sleep_step_id = self.next_step_id();

        if let Some(rec) = self
            .ctx
            .db
            .check_operation_execution(&workflow_id, step_id, "DBOS.getEvent")
            .await?
        {
            return replay_message::<R>(&self.ctx, rec);
        }

        let deadline = self.durable_deadline_ms(sleep_step_id, timeout).await?;
        let target = target_workflow_id.to_string();
        let key_owned = key.to_string();
        self.wait_until(deadline, |db| {
            let target = target.clone();
            let key = key_owned.clone();
            async move { Ok(db.get_event(&target, &key).await?.is_some()) }
        })
        .await?;

        let event = self.ctx.db.get_event(target_workflow_id, key).await?;
        let receiver_ser = resolve_encoder(self.is_portable(), self.ctx.serializer.as_ref())
            .name()
            .to_string();
        match event {
            Some((value, serialization)) => {
                self.ctx
                    .db
                    .record_operation_result(RecordOperationResultInput {
                        workflow_id,
                        step_id,
                        step_name: "DBOS.getEvent".to_string(),
                        output: value.clone(),
                        error: None,
                        child_workflow_id: None,
                        started_at_ms: now_ms(),
                        completed_at_ms: now_ms(),
                        serialization: serialization.clone(),
                    })
                    .await?;
                let decoder = resolve_decoder(&serialization, self.ctx.serializer.as_ref())?;
                serialization::decode::<R>(decoder.as_ref(), &value)
            }
            None => {
                self.ctx
                    .db
                    .record_operation_result(RecordOperationResultInput {
                        workflow_id: workflow_id.clone(),
                        step_id,
                        step_name: "DBOS.getEvent".to_string(),
                        output: None,
                        error: None,
                        child_workflow_id: None,
                        started_at_ms: now_ms(),
                        completed_at_ms: now_ms(),
                        serialization: receiver_ser.clone(),
                    })
                    .await?;
                // No event by deadline: decode None into R (Option<T> -> None).
                let decoder = resolve_decoder(&receiver_ser, self.ctx.serializer.as_ref())?;
                serialization::decode::<R>(decoder.as_ref(), &None)
            }
        }
    }

    /// Checkpoint an absolute deadline (`now + timeout`) at `sleep_step_id` and
    /// return it; on replay the recorded deadline is returned.
    async fn durable_deadline_ms(
        &self,
        sleep_step_id: i64,
        timeout: Duration,
    ) -> Result<i64, DbosError> {
        let json = JsonSerializer;
        let workflow_id = self.workflow_id().to_string();
        if let Some(rec) = self
            .ctx
            .db
            .check_operation_execution(&workflow_id, sleep_step_id, "DBOS.sleep")
            .await?
        {
            return serialization::decode::<i64>(&json, &rec.output);
        }
        let deadline = now_ms() + timeout.as_millis() as i64;
        let encoded = serialization::encode::<i64>(&json, &deadline)?;
        let rec = RecordOperationResultInput {
            workflow_id: workflow_id.clone(),
            step_id: sleep_step_id,
            step_name: "DBOS.sleep".to_string(),
            output: encoded,
            error: None,
            child_workflow_id: None,
            started_at_ms: now_ms(),
            completed_at_ms: now_ms(),
            serialization: json.name().to_string(),
        };
        match self.ctx.db.record_operation_result(rec).await {
            Ok(()) => Ok(deadline),
            Err(e) if e.is_code(crate::error::DbosErrorCode::ConflictingId) => {
                let rec = self
                    .ctx
                    .db
                    .check_operation_execution(&workflow_id, sleep_step_id, "DBOS.sleep")
                    .await?
                    .ok_or_else(|| {
                        DbosError::step_execution(
                            workflow_id,
                            "DBOS.sleep".to_string(),
                            std::io::Error::other("no recorded deadline"),
                        )
                    })?;
                serialization::decode::<i64>(&json, &rec.output)
            }
            Err(e) => Err(e),
        }
    }

    /// Poll `ready` until it returns true or `deadline_ms` passes (cancel-aware).
    async fn wait_until<F, Fut>(&self, deadline_ms: i64, ready: F) -> Result<(), DbosError>
    where
        F: Fn(Arc<dyn crate::db::SystemDatabase>) -> Fut,
        Fut: std::future::Future<Output = Result<bool, DbosError>>,
    {
        loop {
            if ready(self.ctx.db.clone()).await? {
                return Ok(());
            }
            let remaining = deadline_ms - now_ms();
            if remaining <= 0 {
                return Ok(());
            }
            let nap = POLL_INTERVAL.min(Duration::from_millis(remaining as u64));
            tokio::select! {
                _ = tokio::time::sleep(nap) => {}
                _ = self.ctx.token.cancelled() => return Ok(()),
            }
        }
    }
}

impl WfCtx {
    /// Append a value to a durable stream (memoized step).
    pub async fn write_stream<P: Serialize + Send + 'static>(
        &self,
        key: &str,
        value: P,
    ) -> Result<(), DbosError> {
        self.write_stream_raw(key, &value, "DBOS.writeStream").await
    }

    /// Close a durable stream so readers see end-of-stream (memoized step).
    pub async fn close_stream(&self, key: &str) -> Result<(), DbosError> {
        if self.is_within_step {
            return Err(DbosError::step_execution(
                self.workflow_id(),
                "DBOS.closeStream".to_string(),
                std::io::Error::other("cannot call CloseStream within a step"),
            ));
        }
        let workflow_id = self.workflow_id().to_string();
        let step_id = self.next_step_id();
        if self
            .ctx
            .db
            .check_operation_execution(&workflow_id, step_id, "DBOS.closeStream")
            .await?
            .is_some()
        {
            return Ok(());
        }
        self.ctx
            .db
            .write_stream(
                &workflow_id,
                key,
                crate::constants::DBOS_STREAM_CLOSED_SENTINEL,
                step_id,
                JsonSerializer.name(),
            )
            .await?;
        self.record_stream_step(&workflow_id, step_id, "DBOS.closeStream")
            .await
    }

    async fn write_stream_raw<P: Serialize>(
        &self,
        key: &str,
        value: &P,
        step_name: &str,
    ) -> Result<(), DbosError> {
        if self.is_within_step {
            return Err(DbosError::step_execution(
                self.workflow_id(),
                step_name.to_string(),
                std::io::Error::other("cannot write to a stream within a step"),
            ));
        }
        let workflow_id = self.workflow_id().to_string();
        let step_id = self.next_step_id();
        if self
            .ctx
            .db
            .check_operation_execution(&workflow_id, step_id, step_name)
            .await?
            .is_some()
        {
            return Ok(());
        }
        let encoder = resolve_encoder(self.is_portable(), self.ctx.serializer.as_ref());
        let encoded = serialization::encode(encoder.as_ref(), value)?;
        let serialization_name = encoder.name().to_string();
        self.ctx
            .db
            .write_stream(
                &workflow_id,
                key,
                encoded.as_deref().unwrap_or_default(),
                step_id,
                &serialization_name,
            )
            .await?;
        self.record_stream_step(&workflow_id, step_id, step_name)
            .await
    }

    async fn record_stream_step(
        &self,
        workflow_id: &str,
        step_id: i64,
        step_name: &str,
    ) -> Result<(), DbosError> {
        self.ctx
            .db
            .record_operation_result(RecordOperationResultInput {
                workflow_id: workflow_id.to_string(),
                step_id,
                step_name: step_name.to_string(),
                output: None,
                error: None,
                child_workflow_id: None,
                started_at_ms: now_ms(),
                completed_at_ms: now_ms(),
                serialization: JsonSerializer.name().to_string(),
            })
            .await
    }
}

/// Read a durable stream's values (decoded) and whether it is closed. Readable
/// from inside or outside a workflow.
pub async fn read_stream<R: DeserializeOwned>(
    ctx: &Arc<DbosContext>,
    workflow_id: &str,
    key: &str,
) -> Result<(Vec<R>, bool), DbosError> {
    let (entries, closed) = ctx.db.read_stream(workflow_id, key, 0).await?;
    let mut values = Vec::with_capacity(entries.len());
    for (value, _offset, ser) in entries {
        let decoder = resolve_decoder(&ser, ctx.serializer.as_ref())?;
        values.push(serialization::decode::<R>(decoder.as_ref(), &Some(value))?);
    }
    Ok((values, closed))
}

fn resolve_topic(topic: &str) -> String {
    if topic.is_empty() {
        DBOS_NULL_TOPIC.to_string()
    } else {
        topic.to_string()
    }
}

fn replay_message<R: DeserializeOwned>(
    ctx: &Arc<DbosContext>,
    rec: crate::db::RecordedResult,
) -> Result<R, DbosError> {
    if let Some(err_str) = &rec.error {
        return Err(crate::serialization::deserialize_workflow_error(
            &Some(err_str.clone()),
            &rec.serialization,
        )
        .unwrap_or_else(|| DbosError::new(crate::error::DbosErrorCode::Timeout, err_str.clone())));
    }
    let decoder = resolve_decoder(&rec.serialization, ctx.serializer.as_ref())?;
    serialization::decode::<R>(decoder.as_ref(), &rec.output)
}

/// Send a message to a workflow from *outside* any workflow (no step recording).
pub async fn send<P: Serialize>(
    ctx: &Arc<DbosContext>,
    destination_id: &str,
    message: P,
    topic: &str,
) -> Result<(), DbosError> {
    let encoder = resolve_encoder(false, ctx.serializer.as_ref());
    let encoded = serialization::encode(encoder.as_ref(), &message)?;
    ctx.db
        .send_notification(
            destination_id,
            &resolve_topic(topic),
            &encoded,
            encoder.name(),
        )
        .await
}

/// Get an event from a target workflow from *outside* any workflow, waiting up
/// to `timeout` (polls durable state).
pub async fn get_event<R: DeserializeOwned>(
    ctx: &Arc<DbosContext>,
    target_workflow_id: &str,
    key: &str,
    timeout: Duration,
) -> Result<Option<R>, DbosError> {
    let deadline = now_ms() + timeout.as_millis() as i64;
    loop {
        if let Some((value, serialization)) = ctx.db.get_event(target_workflow_id, key).await? {
            let decoder = resolve_decoder(&serialization, ctx.serializer.as_ref())?;
            return Ok(Some(serialization::decode::<R>(decoder.as_ref(), &value)?));
        }
        let remaining = deadline - now_ms();
        if remaining <= 0 {
            return Ok(None);
        }
        let nap = POLL_INTERVAL.min(Duration::from_millis(remaining as u64));
        tokio::select! {
            _ = tokio::time::sleep(nap) => {}
            _ = ctx.token.cancelled() => return Ok(None),
        }
    }
}
