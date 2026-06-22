//! Wire framing and serde types for the conductor WebSocket protocol.
//!
//! Every message that crosses the
//! socket is a JSON text frame carrying a base envelope (`type` + `request_id`);
//! responses echo both and convey failure via an optional `error_message`. The
//! field names/casing here are load-bearing — they are consumed verbatim by the
//! DBOS Conductor cloud service (PascalCase for the workflow-list body,
//! snake_case elsewhere).

use serde::de::Deserializer;
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Message-type catalog (wire strings).
// ---------------------------------------------------------------------------

/// The complete catalog of conductor message-type wire strings.
///
/// These are the values of the base envelope's `type` field. They are kept as
/// plain `&str` consts (rather than an enum) so dispatch can `match` on the raw
/// string and still answer *unknown* types with a well-formed error response.
pub mod msg {
    pub const EXECUTOR_INFO: &str = "executor_info";
    pub const RECOVERY: &str = "recovery";
    pub const CANCEL: &str = "cancel";
    pub const RESUME: &str = "resume";
    pub const LIST_WORKFLOWS: &str = "list_workflows";
    pub const LIST_QUEUED_WORKFLOWS: &str = "list_queued_workflows";
    pub const LIST_STEPS: &str = "list_steps";
    pub const GET_WORKFLOW: &str = "get_workflow";
    pub const FORK_WORKFLOW: &str = "fork_workflow";
    pub const EXIST_PENDING_WORKFLOWS: &str = "exist_pending_workflows";
    pub const RETENTION: &str = "retention";
    pub const GET_METRICS: &str = "get_metrics";
    pub const EXPORT_WORKFLOW: &str = "export_workflow";
    pub const IMPORT_WORKFLOW: &str = "import_workflow";
    pub const DELETE: &str = "delete";
    pub const ALERT: &str = "alert";
    pub const LIST_SCHEDULES: &str = "list_schedules";
    pub const GET_SCHEDULE: &str = "get_schedule";
    pub const PAUSE_SCHEDULE: &str = "pause_schedule";
    pub const RESUME_SCHEDULE: &str = "resume_schedule";
    pub const BACKFILL_SCHEDULE: &str = "backfill_schedule";
    pub const TRIGGER_SCHEDULE: &str = "trigger_schedule";
    pub const GET_WORKFLOW_EVENTS: &str = "get_workflow_events";
    pub const GET_WORKFLOW_NOTIFICATIONS: &str = "get_workflow_notifications";
    pub const GET_WORKFLOW_STREAMS: &str = "get_workflow_streams";
    pub const GET_WORKFLOW_AGGREGATES: &str = "get_workflow_aggregates";
    pub const GET_STEP_AGGREGATES: &str = "get_step_aggregates";
    pub const LIST_APPLICATION_VERSIONS: &str = "list_application_versions";
    pub const SET_LATEST_APPLICATION_VERSION: &str = "set_latest_application_version";
    pub const LIST_QUEUES: &str = "list_queues";
    pub const GET_QUEUE: &str = "get_queue";
}

// ---------------------------------------------------------------------------
// stringOrList
// ---------------------------------------------------------------------------

/// A JSON value that accepts either a single string or an array of strings,
/// always yielding a `Vec<String>`:
/// `null` -> empty, a string -> one element, an array -> the array.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StringOrList(pub Vec<String>);

impl StringOrList {
    /// The underlying slice of strings.
    pub fn to_slice(&self) -> Vec<String> {
        self.0.clone()
    }

    /// Whether no strings were provided.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl<'de> Deserialize<'de> for StringOrList {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Raw {
            One(String),
            Many(Vec<String>),
        }
        // `Option` lets an explicit JSON `null` (and an absent field) map to the
        // empty list; otherwise a single string or an array of strings.
        match Option::<Raw>::deserialize(deserializer)? {
            None => Ok(StringOrList(Vec::new())),
            Some(Raw::One(s)) => Ok(StringOrList(vec![s])),
            Some(Raw::Many(v)) => Ok(StringOrList(v)),
        }
    }
}

// ---------------------------------------------------------------------------
// Base envelopes
// ---------------------------------------------------------------------------

/// The base of every incoming message: its `type` and `request_id`. Parsed
/// first to route dispatch; a parse failure here is the *only* case where no
/// response is sent (it is logged and skipped).
#[derive(Debug, Clone, Deserialize)]
pub struct BaseMessage {
    #[serde(rename = "type")]
    pub r#type: String,
    pub request_id: String,
}

/// The base of every outgoing response. Echoes the request's `type` and
/// `request_id`; `error_message` is present (non-null) only on failure.
#[derive(Debug, Clone, Serialize)]
pub struct BaseResponse {
    #[serde(rename = "type")]
    pub r#type: String,
    pub request_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
}

impl BaseResponse {
    /// A successful base (no error).
    pub fn ok(r#type: &str, request_id: &str) -> Self {
        BaseResponse {
            r#type: r#type.to_string(),
            request_id: request_id.to_string(),
            error_message: None,
        }
    }

    /// A failing base with the given error message.
    pub fn err(r#type: &str, request_id: &str, message: impl Into<String>) -> Self {
        BaseResponse {
            r#type: r#type.to_string(),
            request_id: request_id.to_string(),
            error_message: Some(message.into()),
        }
    }
}

// ---------------------------------------------------------------------------
// executor_info
// ---------------------------------------------------------------------------

/// `executor_info` response. `language` is `"rust"` for this port.
#[derive(Debug, Clone, Serialize)]
pub struct ExecutorInfoResponse {
    #[serde(flatten)]
    pub base: BaseResponse,
    pub executor_id: String,
    pub application_version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hostname: Option<String>,
    pub dbos_version: String,
    pub language: String,
}

// ---------------------------------------------------------------------------
// recovery
// ---------------------------------------------------------------------------

/// `recovery` request.
#[derive(Debug, Clone, Deserialize)]
pub struct RecoveryRequest {
    #[serde(default)]
    pub executor_ids: Vec<String>,
}

/// A response carrying only a `success` flag (used by recovery, cancel, resume,
/// retention, delete, import, alert, set_latest_application_version, schedule
/// pause/resume).
#[derive(Debug, Clone, Serialize)]
pub struct SuccessResponse {
    #[serde(flatten)]
    pub base: BaseResponse,
    pub success: bool,
}

// ---------------------------------------------------------------------------
// cancel / resume / delete (shared id resolution)
// ---------------------------------------------------------------------------

/// `cancel` / `resume` / `delete` request. Use `workflow_ids`; if empty and
/// `workflow_id` is set, fall back to the single id.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct WorkflowIdsRequest {
    #[serde(default)]
    pub workflow_id: String,
    #[serde(default)]
    pub workflow_ids: Vec<String>,
    /// Only used by `resume`.
    #[serde(default)]
    pub queue_name: Option<String>,
    /// Only used by `delete`.
    #[serde(default)]
    pub delete_children: bool,
}

impl WorkflowIdsRequest {
    /// Resolve the target id set: prefer the `workflow_ids` list, else the single
    /// `workflow_id`, else empty.
    pub fn resolve_ids(&self) -> Vec<String> {
        if !self.workflow_ids.is_empty() {
            self.workflow_ids.clone()
        } else if !self.workflow_id.is_empty() {
            vec![self.workflow_id.clone()]
        } else {
            Vec::new()
        }
    }
}

// ---------------------------------------------------------------------------
// list_workflows / list_queued_workflows
// ---------------------------------------------------------------------------

/// The shared request body for `list_workflows` / `list_queued_workflows`,
/// wrapped under a top-level `body` key on the wire.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct ListWorkflowsRequest {
    #[serde(default)]
    pub body: ListWorkflowsBody,
}

/// The filters carried by `list_workflows` / `list_queued_workflows`. Many use
/// `stringOrList`. Fields the Rust `dbos-core` does not yet support (time
/// windows, parent/forked filters, etc.) are accepted and ignored so the wire
/// contract stays intact.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct ListWorkflowsBody {
    #[serde(default)]
    pub workflow_uuids: Vec<String>,
    #[serde(default)]
    pub workflow_name: StringOrList,
    #[serde(default)]
    pub authenticated_user: StringOrList,
    #[serde(default)]
    pub status: StringOrList,
    #[serde(default)]
    pub application_version: StringOrList,
    #[serde(default)]
    pub queue_name: StringOrList,
    #[serde(default)]
    pub executor_id: StringOrList,
    #[serde(default)]
    pub workflow_id_prefix: StringOrList,
    #[serde(default)]
    pub limit: Option<i64>,
    #[serde(default)]
    pub offset: Option<i64>,
    #[serde(default)]
    pub sort_desc: bool,
    #[serde(default)]
    pub load_input: bool,
    #[serde(default)]
    pub load_output: bool,
    #[serde(default)]
    pub queues_only: bool,
    // Accepted-but-unsupported filters (kept for wire compatibility):
    #[serde(default)]
    pub start_time: Option<String>,
    #[serde(default)]
    pub end_time: Option<String>,
    #[serde(default)]
    pub completed_after: Option<String>,
    #[serde(default)]
    pub completed_before: Option<String>,
    #[serde(default)]
    pub dequeued_after: Option<String>,
    #[serde(default)]
    pub dequeued_before: Option<String>,
    #[serde(default)]
    pub forked_from: StringOrList,
    #[serde(default)]
    pub parent_workflow_id: StringOrList,
    #[serde(default)]
    pub was_forked_from: Option<bool>,
    #[serde(default)]
    pub has_parent: Option<bool>,
}

/// `list_workflows` / `list_queued_workflows` / `get_workflow` row. **Keys are
/// PascalCase**; all but `WorkflowUUID` are optional. Built by
/// [`crate::conductor::handlers::format_list_workflows_row`].
#[derive(Debug, Clone, Default, Serialize)]
pub struct ListWorkflowsRow {
    #[serde(rename = "WorkflowUUID")]
    pub workflow_uuid: String,
    #[serde(rename = "Status", skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    #[serde(rename = "WorkflowName", skip_serializing_if = "Option::is_none")]
    pub workflow_name: Option<String>,
    #[serde(rename = "WorkflowClassName", skip_serializing_if = "Option::is_none")]
    pub workflow_class_name: Option<String>,
    #[serde(rename = "WorkflowConfigName", skip_serializing_if = "Option::is_none")]
    pub workflow_config_name: Option<String>,
    #[serde(rename = "AuthenticatedUser", skip_serializing_if = "Option::is_none")]
    pub authenticated_user: Option<String>,
    #[serde(rename = "AssumedRole", skip_serializing_if = "Option::is_none")]
    pub assumed_role: Option<String>,
    #[serde(rename = "AuthenticatedRoles", skip_serializing_if = "Option::is_none")]
    pub authenticated_roles: Option<String>,
    #[serde(rename = "Input", skip_serializing_if = "Option::is_none")]
    pub input: Option<String>,
    #[serde(rename = "Output", skip_serializing_if = "Option::is_none")]
    pub output: Option<String>,
    #[serde(rename = "Error", skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(rename = "CreatedAt", skip_serializing_if = "Option::is_none")]
    pub created_at: Option<String>,
    #[serde(rename = "UpdatedAt", skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,
    #[serde(rename = "QueueName", skip_serializing_if = "Option::is_none")]
    pub queue_name: Option<String>,
    #[serde(rename = "ApplicationVersion", skip_serializing_if = "Option::is_none")]
    pub application_version: Option<String>,
    #[serde(rename = "ExecutorID", skip_serializing_if = "Option::is_none")]
    pub executor_id: Option<String>,
    #[serde(rename = "WorkflowTimeoutMS", skip_serializing_if = "Option::is_none")]
    pub workflow_timeout_ms: Option<String>,
    #[serde(
        rename = "WorkflowDeadlineEpochMS",
        skip_serializing_if = "Option::is_none"
    )]
    pub workflow_deadline_epoch_ms: Option<String>,
    #[serde(rename = "DeduplicationID", skip_serializing_if = "Option::is_none")]
    pub deduplication_id: Option<String>,
    #[serde(rename = "Priority")]
    pub priority: String,
    #[serde(rename = "QueuePartitionKey", skip_serializing_if = "Option::is_none")]
    pub queue_partition_key: Option<String>,
    #[serde(rename = "ForkedFrom", skip_serializing_if = "Option::is_none")]
    pub forked_from: Option<String>,
    #[serde(rename = "WasForkedFrom")]
    pub was_forked_from: bool,
    #[serde(rename = "ParentWorkflowID", skip_serializing_if = "Option::is_none")]
    pub parent_workflow_id: Option<String>,
    #[serde(rename = "DequeuedAt", skip_serializing_if = "Option::is_none")]
    pub dequeued_at: Option<String>,
    #[serde(rename = "DelayUntilEpochMS", skip_serializing_if = "Option::is_none")]
    pub delay_until_epoch_ms: Option<String>,
    #[serde(rename = "CompletedAt", skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<String>,
}

/// `list_workflows` / `list_queued_workflows` response.
#[derive(Debug, Clone, Serialize)]
pub struct ListWorkflowsResponse {
    #[serde(flatten)]
    pub base: BaseResponse,
    pub output: Vec<ListWorkflowsRow>,
}

// ---------------------------------------------------------------------------
// list_steps
// ---------------------------------------------------------------------------

/// `list_steps` request.
#[derive(Debug, Clone, Deserialize)]
pub struct ListStepsRequest {
    pub workflow_id: String,
    #[serde(default)]
    pub load_output: bool,
    #[serde(default)]
    pub limit: Option<i64>,
    #[serde(default)]
    pub offset: Option<i64>,
}

/// One row of the `list_steps` output (snake_case).
#[derive(Debug, Clone, Serialize)]
pub struct StepRow {
    pub function_id: i64,
    pub function_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub child_workflow_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at_epoch_ms: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_at_epoch_ms: Option<String>,
}

/// `list_steps` response (`output` is null on error).
#[derive(Debug, Clone, Serialize)]
pub struct ListStepsResponse {
    #[serde(flatten)]
    pub base: BaseResponse,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output: Option<Vec<StepRow>>,
}

// ---------------------------------------------------------------------------
// get_workflow
// ---------------------------------------------------------------------------

/// `get_workflow` request.
#[derive(Debug, Clone, Deserialize)]
pub struct GetWorkflowRequest {
    pub workflow_id: String,
    #[serde(default)]
    pub load_input: bool,
    #[serde(default)]
    pub load_output: bool,
}

/// `get_workflow` response (`output` null/omitted when absent or on error).
#[derive(Debug, Clone, Serialize)]
pub struct GetWorkflowResponse {
    #[serde(flatten)]
    pub base: BaseResponse,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output: Option<ListWorkflowsRow>,
}

// ---------------------------------------------------------------------------
// fork_workflow
// ---------------------------------------------------------------------------

/// `fork_workflow` request (wrapped under `body`).
#[derive(Debug, Clone, Deserialize)]
pub struct ForkWorkflowRequest {
    pub body: ForkWorkflowBody,
}

/// The `fork_workflow` body.
#[derive(Debug, Clone, Deserialize)]
pub struct ForkWorkflowBody {
    pub workflow_id: String,
    #[serde(default)]
    pub start_step: i64,
    #[serde(default)]
    pub application_version: Option<String>,
    #[serde(default)]
    pub new_workflow_id: Option<String>,
    #[serde(default)]
    pub queue_name: Option<String>,
    #[serde(default)]
    pub queue_partition_key: Option<String>,
}

/// `fork_workflow` response.
#[derive(Debug, Clone, Serialize)]
pub struct ForkWorkflowResponse {
    #[serde(flatten)]
    pub base: BaseResponse,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub new_workflow_id: Option<String>,
}

// ---------------------------------------------------------------------------
// exist_pending_workflows
// ---------------------------------------------------------------------------

/// `exist_pending_workflows` request.
#[derive(Debug, Clone, Deserialize)]
pub struct ExistPendingWorkflowsRequest {
    #[serde(default)]
    pub executor_id: String,
    #[serde(default)]
    pub application_version: String,
}

/// `exist_pending_workflows` response.
#[derive(Debug, Clone, Serialize)]
pub struct ExistPendingWorkflowsResponse {
    #[serde(flatten)]
    pub base: BaseResponse,
    pub exist: bool,
}

// ---------------------------------------------------------------------------
// retention
// ---------------------------------------------------------------------------

/// `retention` request (wrapped under `body`).
#[derive(Debug, Clone, Deserialize)]
pub struct RetentionRequest {
    pub body: RetentionBody,
}

/// The `retention` body. All fields optional.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct RetentionBody {
    #[serde(default)]
    pub gc_cutoff_epoch_ms: Option<i64>,
    #[serde(default)]
    pub gc_rows_threshold: Option<i64>,
    #[serde(default)]
    pub timeout_cutoff_epoch_ms: Option<i64>,
}

// ---------------------------------------------------------------------------
// get_workflow_aggregates / get_metrics (status counts)
// ---------------------------------------------------------------------------

/// `get_workflow_aggregates` request (wrapped under `body`). Only the
/// count-by-status path is supported by `dbos-core`; other group_by/select
/// flags are accepted and ignored.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct WorkflowAggregatesRequest {
    #[serde(default)]
    pub body: serde_json::Value,
}

/// `get_workflow_aggregates` response. `output` rows are emitted as raw JSON
/// objects so the row shape can evolve without churn here.
#[derive(Debug, Clone, Serialize)]
pub struct WorkflowAggregatesResponse {
    #[serde(flatten)]
    pub base: BaseResponse,
    pub output: Vec<serde_json::Value>,
}

// ---------------------------------------------------------------------------
// list_schedules / get_schedule
// ---------------------------------------------------------------------------

/// `list_schedules` request (wrapped under `body`).
#[derive(Debug, Clone, Default, Deserialize)]
pub struct ListSchedulesRequest {
    #[serde(default)]
    pub body: ListSchedulesBody,
}

/// The `list_schedules` body filters. `load_context` defaults to `true`.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct ListSchedulesBody {
    #[serde(default)]
    pub status: StringOrList,
    #[serde(default)]
    pub workflow_name: StringOrList,
    #[serde(default)]
    pub schedule_name_prefix: StringOrList,
    #[serde(default)]
    pub load_context: Option<bool>,
}

/// `get_schedule` request.
#[derive(Debug, Clone, Deserialize)]
pub struct GetScheduleRequest {
    pub schedule_name: String,
    #[serde(default)]
    pub load_context: Option<bool>,
}

/// A schedule row in the conductor wire shape (snake_case; nullable fields are
/// emitted as `null`, never omitted).
#[derive(Debug, Clone, Serialize)]
pub struct ScheduleRow {
    pub schedule_id: String,
    pub schedule_name: String,
    pub workflow_name: String,
    pub workflow_class_name: Option<String>,
    pub schedule: String,
    pub status: String,
    pub context: Option<String>,
    pub last_fired_at: Option<String>,
    pub automatic_backfill: bool,
    pub cron_timezone: Option<String>,
    pub queue_name: Option<String>,
}

/// `list_schedules` response.
#[derive(Debug, Clone, Serialize)]
pub struct ListSchedulesResponse {
    #[serde(flatten)]
    pub base: BaseResponse,
    pub output: Vec<ScheduleRow>,
}

/// `get_schedule` response (`output` null when absent or on error).
#[derive(Debug, Clone, Serialize)]
pub struct GetScheduleResponse {
    #[serde(flatten)]
    pub base: BaseResponse,
    pub output: Option<ScheduleRow>,
}

/// `pause_schedule` / `resume_schedule` request.
#[derive(Debug, Clone, Deserialize)]
pub struct ScheduleNameRequest {
    pub schedule_name: String,
}

// ---------------------------------------------------------------------------
// alert
// ---------------------------------------------------------------------------

/// `alert` request. All fields optional; the handler is a no-op that logs.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct AlertRequest {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub message: String,
    #[serde(default)]
    pub metadata: std::collections::HashMap<String, String>,
}

// ---------------------------------------------------------------------------
// get_workflow_events / get_workflow_notifications / get_workflow_streams
// ---------------------------------------------------------------------------

/// `get_workflow_events` / `get_workflow_notifications` / `get_workflow_streams`
/// request — all keyed by a single workflow id.
#[derive(Debug, Clone, Deserialize)]
pub struct WorkflowIdRequest {
    pub workflow_id: String,
}

/// One `get_workflow_events` row.
#[derive(Debug, Clone, Serialize)]
pub struct EventOutput {
    pub key: String,
    pub value: String,
}

/// `get_workflow_events` response.
#[derive(Debug, Clone, Serialize)]
pub struct GetWorkflowEventsResponse {
    #[serde(flatten)]
    pub base: BaseResponse,
    pub events: Vec<EventOutput>,
}

/// One `get_workflow_notifications` row. `topic` is `null` when sent without a
/// topic; `created_at_epoch_ms` is a raw number (NOT a string).
#[derive(Debug, Clone, Serialize)]
pub struct NotificationOutput {
    pub topic: Option<String>,
    pub message: String,
    pub created_at_epoch_ms: i64,
    pub consumed: bool,
}

/// `get_workflow_notifications` response.
#[derive(Debug, Clone, Serialize)]
pub struct GetWorkflowNotificationsResponse {
    #[serde(flatten)]
    pub base: BaseResponse,
    pub notifications: Vec<NotificationOutput>,
}

/// One `get_workflow_streams` row: a stream key and its ordered values.
#[derive(Debug, Clone, Serialize)]
pub struct StreamEntryOutput {
    pub key: String,
    pub values: Vec<String>,
}

/// `get_workflow_streams` response.
#[derive(Debug, Clone, Serialize)]
pub struct GetWorkflowStreamsResponse {
    #[serde(flatten)]
    pub base: BaseResponse,
    pub streams: Vec<StreamEntryOutput>,
}

// ---------------------------------------------------------------------------
// get_step_aggregates
// ---------------------------------------------------------------------------

/// One `get_step_aggregates` row.
#[derive(Debug, Clone, Serialize)]
pub struct StepAggregateRow {
    pub step_name: String,
    pub count: i64,
}

/// `get_step_aggregates` response.
#[derive(Debug, Clone, Serialize)]
pub struct StepAggregatesResponse {
    #[serde(flatten)]
    pub base: BaseResponse,
    pub output: Vec<StepAggregateRow>,
}

// ---------------------------------------------------------------------------
// list_application_versions
// ---------------------------------------------------------------------------

/// One `list_application_versions` row. Timestamps are raw int64 epoch-ms.
#[derive(Debug, Clone, Serialize)]
pub struct ApplicationVersionOutput {
    pub version_id: String,
    pub version_name: String,
    pub version_timestamp: i64,
    pub created_at: i64,
}

/// `list_application_versions` response.
#[derive(Debug, Clone, Serialize)]
pub struct ListApplicationVersionsResponse {
    #[serde(flatten)]
    pub base: BaseResponse,
    pub output: Vec<ApplicationVersionOutput>,
}

/// `set_latest_application_version` request.
#[derive(Debug, Clone, Deserialize)]
pub struct SetLatestApplicationVersionRequest {
    pub version_name: String,
}

// ---------------------------------------------------------------------------
// list_queues / get_queue
// ---------------------------------------------------------------------------

/// `get_queue` request.
#[derive(Debug, Clone, Deserialize)]
pub struct GetQueueRequest {
    pub name: String,
}

/// A queue's configuration on the wire (snake_case; nullable fields emitted as
/// `null`, not omitted).
#[derive(Debug, Clone, Serialize)]
pub struct QueueOutput {
    pub name: String,
    pub concurrency: Option<i64>,
    pub worker_concurrency: Option<i64>,
    pub rate_limit_max: Option<i64>,
    pub rate_limit_period_sec: Option<f64>,
    pub priority_enabled: bool,
    pub partition_queue: bool,
    pub polling_interval_sec: f64,
}

/// `list_queues` response.
#[derive(Debug, Clone, Serialize)]
pub struct ListQueuesResponse {
    #[serde(flatten)]
    pub base: BaseResponse,
    pub output: Vec<QueueOutput>,
}

/// `get_queue` response (`output` null when absent or on error).
#[derive(Debug, Clone, Serialize)]
pub struct GetQueueResponse {
    #[serde(flatten)]
    pub base: BaseResponse,
    pub output: Option<QueueOutput>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn string_or_list_accepts_string_array_and_null() {
        let one: StringOrList = serde_json::from_str(r#""a""#).unwrap();
        assert_eq!(one.to_slice(), vec!["a".to_string()]);

        let many: StringOrList = serde_json::from_str(r#"["a","b"]"#).unwrap();
        assert_eq!(many.to_slice(), vec!["a".to_string(), "b".to_string()]);

        let null: StringOrList = serde_json::from_str("null").unwrap();
        assert!(null.is_empty());
    }

    #[test]
    fn base_response_omits_error_when_ok() {
        let r = BaseResponse::ok(msg::EXECUTOR_INFO, "req-1");
        let v = serde_json::to_value(&r).unwrap();
        assert_eq!(v["type"], "executor_info");
        assert_eq!(v["request_id"], "req-1");
        assert!(v.get("error_message").is_none());
    }

    #[test]
    fn id_resolution_prefers_list_then_single() {
        let req = WorkflowIdsRequest {
            workflow_ids: vec!["a".into(), "b".into()],
            workflow_id: "c".into(),
            ..Default::default()
        };
        assert_eq!(req.resolve_ids(), vec!["a".to_string(), "b".to_string()]);

        let req = WorkflowIdsRequest {
            workflow_id: "c".into(),
            ..Default::default()
        };
        assert_eq!(req.resolve_ids(), vec!["c".to_string()]);

        let req = WorkflowIdsRequest::default();
        assert!(req.resolve_ids().is_empty());
    }
}
