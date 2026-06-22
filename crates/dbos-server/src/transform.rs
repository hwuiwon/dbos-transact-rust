//! Wire-format transforms shared by the workflow endpoints.
//!
//! The workflow-list response and the step-map shape use PascalCase keys,
//! epoch-ms timestamps rendered as **strings**, and string-passthrough rules for
//! `Input`/`Output`/`Error`.

use dbos::{StepInfo, WorkflowStatus};
use serde_json::{Map, Value, json};

/// Render an epoch-ms timestamp as a JSON **string** (`"1700000000000"`), or
/// `null` when the timestamp is absent/zero.
fn format_epoch_ms(ms: Option<i64>) -> Value {
    match ms {
        Some(ms) if ms != 0 => Value::String(ms.to_string()),
        _ => Value::Null,
    }
}

/// Same as [`format_epoch_ms`] but for non-optional fields that use `0` as the
/// zero/unset sentinel (`created_at`/`updated_at`).
fn format_epoch_ms_zero(ms: i64) -> Value {
    if ms == 0 {
        Value::Null
    } else {
        Value::String(ms.to_string())
    }
}

/// String passthrough for `Input`/`Output`: a stored value is already a JSON
/// string; a non-string/non-present value becomes `""`.
fn string_passthrough(s: &Option<String>) -> Value {
    match s {
        Some(s) => Value::String(s.clone()),
        None => Value::Null,
    }
}

/// Transform a [`WorkflowStatus`] into the admin server's PascalCase response
/// map (used by `POST /workflows` and `GET /workflows/{id}`).
pub fn to_list_response(ws: &WorkflowStatus) -> Value {
    let mut map = Map::new();
    map.insert("WorkflowUUID".into(), json!(ws.id));
    map.insert("Status".into(), json!(ws.status.as_str()));
    map.insert("WorkflowName".into(), json!(ws.name));
    map.insert("AuthenticatedUser".into(), json!(ws.authenticated_user));
    map.insert("AssumedRole".into(), json!(ws.assumed_role));
    map.insert("AuthenticatedRoles".into(), json!(ws.authenticated_roles));
    map.insert("Output".into(), string_passthrough(&ws.output));
    map.insert("ExecutorID".into(), json!(ws.executor_id));
    map.insert("ApplicationVersion".into(), json!(ws.application_version));
    map.insert("ApplicationID".into(), json!(ws.application_id));
    map.insert("Attempts".into(), json!(ws.attempts));
    map.insert("QueueName".into(), json!(ws.queue_name));
    map.insert("Timeout".into(), json!(ws.timeout_ms));
    map.insert("DeduplicationID".into(), json!(ws.deduplication_id));
    map.insert("Priority".into(), json!(ws.priority));
    map.insert("QueuePartitionKey".into(), json!(ws.queue_partition_key));
    map.insert("Input".into(), string_passthrough(&ws.input));
    map.insert("CreatedAt".into(), format_epoch_ms_zero(ws.created_at_ms));
    map.insert("UpdatedAt".into(), format_epoch_ms_zero(ws.updated_at_ms));
    map.insert(
        "WorkflowDeadlineEpochMS".into(),
        format_epoch_ms(ws.deadline_ms),
    );
    map.insert("StartedAt".into(), format_epoch_ms(ws.started_at_ms));
    // `Error`: a stored error string is emitted as a JSON-quoted string; absent
    // errors render as "".
    let error = match &ws.error {
        Some(e) => Value::String(serde_json::to_string(e).unwrap_or_else(|_| format!("{e:?}"))),
        None => Value::String(String::new()),
    };
    map.insert("Error".into(), error);
    Value::Object(map)
}

/// Transform a [`StepInfo`] into the admin server's step map (used by
/// `GET /workflows/{id}/steps`). Here epoch-ms are raw int64 (not strings), and
/// `error` is omitted entirely when there is no error.
pub fn to_step_response(step: &StepInfo) -> Value {
    let mut map = Map::new();
    map.insert("function_id".into(), json!(step.step_id));
    map.insert("function_name".into(), json!(step.step_name));
    map.insert(
        "child_workflow_id".into(),
        json!(step.child_workflow_id.clone().unwrap_or_default()),
    );
    if let Some(ms) = step.started_at_ms {
        if ms != 0 {
            map.insert("started_at_epoch_ms".into(), json!(ms));
        }
    }
    if let Some(ms) = step.completed_at_ms {
        if ms != 0 {
            map.insert("completed_at_epoch_ms".into(), json!(ms));
        }
    }
    // `output`: string passthrough; non-string/absent => "".
    map.insert(
        "output".into(),
        Value::String(step.output.clone().unwrap_or_default()),
    );
    // `error`: only present when the step failed.
    if let Some(e) = &step.error {
        map.insert(
            "error".into(),
            Value::String(serde_json::to_string(e).unwrap_or_else(|_| format!("{e:?}"))),
        );
    }
    Value::Object(map)
}
