//! The unified DBOS error type and taxonomy.
//!
//! [`DbosErrorCode`] enumerates the error categories (1..=17). [`DbosError`]
//! carries a code, a human message, and a set of optional context fields, plus
//! an optional wrapped source error.

use std::fmt;
use std::sync::Arc;

/// Programmatic error categories. Discriminants are the stable `1..` values used
/// in the `DBOS Error <n>` text and any cross-language reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i32)]
pub enum DbosErrorCode {
    /// Workflow ID conflicts or duplicate step operations.
    ConflictingId = 1,
    /// DBOS context initialization failures.
    Initialization = 2,
    /// Referenced workflow does not exist.
    NonExistentWorkflow = 3,
    /// Workflow with same ID already exists with different parameters.
    ConflictingWorkflow = 4,
    /// Workflow was cancelled during execution.
    WorkflowCancelled = 5,
    /// Step function mismatch during recovery (non-deterministic workflow).
    UnexpectedStep = 6,
    /// A workflow being awaited was cancelled.
    AwaitedWorkflowCancelled = 7,
    /// Attempting to register a workflow/queue that already exists.
    ConflictingRegistration = 8,
    /// Type mismatch in workflow input/output.
    WorkflowUnexpectedType = 9,
    /// General workflow execution error.
    WorkflowExecution = 10,
    /// General step execution error.
    StepExecution = 11,
    /// Workflow moved to dead-letter queue after max retries.
    DeadLetterQueue = 12,
    /// Step exceeded maximum retry attempts.
    MaxStepRetriesExceeded = 13,
    /// Workflow was deduplicated in the queue.
    QueueDeduplicated = 14,
    /// Patching system is not enabled in the configuration.
    PatchingNotEnabled = 15,
    /// Operation timed out (e.g. recv/get-event timeout).
    Timeout = 16,
    /// No application versions are registered in the system database.
    NoApplicationVersions = 17,
}

impl fmt::Display for DbosErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", *self as i32)
    }
}

/// The unified error type for all DBOS operations.
#[derive(Clone)]
pub struct DbosError {
    /// Human-readable message.
    pub message: String,
    /// Error category for programmatic handling.
    pub code: DbosErrorCode,

    // Optional context fields — only set when relevant.
    /// Associated workflow identifier.
    pub workflow_id: Option<String>,
    /// Target workflow identifier (communication errors).
    pub destination_id: Option<String>,
    /// Step function name (step errors).
    pub step_name: Option<String>,
    /// Queue name (queue-related errors).
    pub queue_name: Option<String>,
    /// Deduplication identifier.
    pub deduplication_id: Option<String>,
    /// Step sequence number.
    pub step_id: Option<i64>,
    /// Expected function name (determinism errors).
    pub expected_name: Option<String>,
    /// Recorded function name (determinism errors).
    pub recorded_name: Option<String>,
    /// Maximum retry limit (retry-related errors).
    pub max_retries: Option<i64>,

    /// Underlying error being wrapped. `Arc` keeps `DbosError: Clone`.
    pub(crate) source: Option<Arc<dyn std::error::Error + Send + Sync + 'static>>,
}

/// Convenience alias for fallible DBOS operations.
pub type DbosResult<T> = Result<T, DbosError>;

impl DbosError {
    /// Base constructor with only code + message; all context fields cleared.
    pub fn new(code: DbosErrorCode, message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            code,
            workflow_id: None,
            destination_id: None,
            step_name: None,
            queue_name: None,
            deduplication_id: None,
            step_id: None,
            expected_name: None,
            recorded_name: None,
            max_retries: None,
            source: None,
        }
    }

    /// Attach a wrapped source error (for `source()` / unwrapping).
    pub fn with_source(
        mut self,
        err: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        self.source = Some(Arc::new(err));
        self
    }

    /// `errors.Is(err, &DBOSError{Code})` analogue: matches when this error — or
    /// any `DbosError` in its source chain — has the given code.
    pub fn is_code(&self, code: DbosErrorCode) -> bool {
        if self.code == code {
            return true;
        }
        let mut src = std::error::Error::source(self);
        while let Some(s) = src {
            if let Some(d) = s.downcast_ref::<DbosError>() {
                if d.code == code {
                    return true;
                }
            }
            src = s.source();
        }
        false
    }

    // --- constructors ---

    pub fn initialization(message: impl AsRef<str>) -> Self {
        Self::new(
            DbosErrorCode::Initialization,
            format!("Error initializing DBOS Transact: {}", message.as_ref()),
        )
    }

    pub fn non_existent_workflow(workflow_id: impl Into<String>) -> Self {
        let id = workflow_id.into();
        let mut e = Self::new(
            DbosErrorCode::NonExistentWorkflow,
            format!("workflow {id} does not exist"),
        );
        e.destination_id = Some(id);
        e
    }

    pub fn conflicting_workflow(workflow_id: impl Into<String>, message: impl AsRef<str>) -> Self {
        let id = workflow_id.into();
        let mut msg = format!("Conflicting workflow invocation with the same ID ({id})");
        let extra = message.as_ref();
        if !extra.is_empty() {
            msg.push_str(": ");
            msg.push_str(extra);
        }
        let mut e = Self::new(DbosErrorCode::ConflictingWorkflow, msg);
        e.workflow_id = Some(id);
        e
    }

    pub fn workflow_conflict_id(workflow_id: impl Into<String>) -> Self {
        let id = workflow_id.into();
        let mut e = Self::new(
            DbosErrorCode::ConflictingId,
            format!("Conflicting workflow ID {id}"),
        );
        e.workflow_id = Some(id);
        e
    }

    pub fn conflicting_registration(name: impl AsRef<str>) -> Self {
        Self::new(
            DbosErrorCode::ConflictingRegistration,
            format!("{} is already registered", name.as_ref()),
        )
    }

    pub fn unexpected_step(
        workflow_id: impl Into<String>,
        step_id: i64,
        expected_name: impl Into<String>,
        recorded_name: impl Into<String>,
    ) -> Self {
        let id = workflow_id.into();
        let expected = expected_name.into();
        let recorded = recorded_name.into();
        let mut e = Self::new(
            DbosErrorCode::UnexpectedStep,
            format!(
                "During execution of workflow {id} step {step_id}, function {recorded} was recorded when {expected} was expected. Check that your workflow is deterministic."
            ),
        );
        e.workflow_id = Some(id);
        e.step_id = Some(step_id);
        e.expected_name = Some(expected);
        e.recorded_name = Some(recorded);
        e
    }

    pub fn awaited_workflow_cancelled(workflow_id: impl Into<String>) -> Self {
        let id = workflow_id.into();
        let mut e = Self::new(
            DbosErrorCode::AwaitedWorkflowCancelled,
            format!("Awaited workflow {id} was cancelled"),
        );
        e.workflow_id = Some(id);
        e
    }

    pub fn awaited_workflow_max_step_retries(workflow_id: impl Into<String>) -> Self {
        let id = workflow_id.into();
        let mut e = Self::new(
            DbosErrorCode::MaxStepRetriesExceeded,
            format!("Awaited workflow {id} has exceeded the maximum number of step retries"),
        );
        e.workflow_id = Some(id);
        e
    }

    pub fn workflow_cancelled(workflow_id: impl AsRef<str>) -> Self {
        Self::new(
            DbosErrorCode::WorkflowCancelled,
            format!("Workflow {} was cancelled", workflow_id.as_ref()),
        )
    }

    pub fn workflow_unexpected_result_type(
        workflow_id: impl Into<String>,
        expected_type: impl AsRef<str>,
        actual_type: impl AsRef<str>,
    ) -> Self {
        let id = workflow_id.into();
        let mut e = Self::new(
            DbosErrorCode::WorkflowUnexpectedType,
            format!(
                "Workflow {id} returned unexpected result type: expected {}, got {}",
                expected_type.as_ref(),
                actual_type.as_ref()
            ),
        );
        e.workflow_id = Some(id);
        e
    }

    pub fn workflow_unexpected_input_type(
        workflow_name: impl AsRef<str>,
        expected_type: impl AsRef<str>,
        actual_type: impl AsRef<str>,
    ) -> Self {
        Self::new(
            DbosErrorCode::WorkflowUnexpectedType,
            format!(
                "Workflow {} received unexpected input type: expected {}, got {}",
                workflow_name.as_ref(),
                expected_type.as_ref(),
                actual_type.as_ref()
            ),
        )
    }

    pub fn workflow_execution(
        workflow_id: impl Into<String>,
        err: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        let id = workflow_id.into();
        let mut e = Self::new(
            DbosErrorCode::WorkflowExecution,
            format!("Workflow {id} execution error: {err}"),
        );
        e.workflow_id = Some(id);
        e.source = Some(Arc::new(err));
        e
    }

    pub fn step_execution(
        workflow_id: impl Into<String>,
        step_name: impl Into<String>,
        err: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        let id = workflow_id.into();
        let name = step_name.into();
        let mut e = Self::new(
            DbosErrorCode::StepExecution,
            format!("Step {name} in workflow {id} execution error: {err}"),
        );
        e.workflow_id = Some(id);
        e.step_name = Some(name);
        e.source = Some(Arc::new(err));
        e
    }

    /// Variant of [`Self::step_execution`] for cases where the error is already
    /// a [`DbosError`] (avoids double-wrapping the code, keeps the source chain).
    pub fn step_execution_dbos(
        workflow_id: impl Into<String>,
        step_name: impl Into<String>,
        err: DbosError,
    ) -> Self {
        let id = workflow_id.into();
        let name = step_name.into();
        let mut e = Self::new(
            DbosErrorCode::StepExecution,
            format!("Step {name} in workflow {id} execution error: {err}"),
        );
        e.workflow_id = Some(id);
        e.step_name = Some(name);
        e.source = Some(Arc::new(err));
        e
    }

    pub fn dead_letter_queue(workflow_id: impl Into<String>, max_retries: i64) -> Self {
        let id = workflow_id.into();
        let mut e = Self::new(
            DbosErrorCode::DeadLetterQueue,
            format!(
                "Workflow {id} has been moved to the dead-letter queue after exceeding the maximum of {max_retries} retries"
            ),
        );
        e.workflow_id = Some(id);
        e.max_retries = Some(max_retries);
        e
    }

    pub fn max_step_retries_exceeded(
        workflow_id: impl Into<String>,
        step_name: impl Into<String>,
        max_retries: i64,
        err: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        let id = workflow_id.into();
        let name = step_name.into();
        let mut e = Self::new(
            DbosErrorCode::MaxStepRetriesExceeded,
            format!("Step {name} has exceeded its maximum of {max_retries} retries: {err}"),
        );
        e.workflow_id = Some(id);
        e.step_name = Some(name);
        e.max_retries = Some(max_retries);
        e.source = Some(Arc::new(err));
        e
    }

    pub fn queue_deduplicated(
        workflow_id: impl Into<String>,
        queue_name: impl Into<String>,
        deduplication_id: impl Into<String>,
    ) -> Self {
        let id = workflow_id.into();
        let queue = queue_name.into();
        let dedup = deduplication_id.into();
        let mut e = Self::new(
            DbosErrorCode::QueueDeduplicated,
            format!(
                "Workflow {id} was deduplicated due to an existing workflow in queue {queue} with deduplication ID {dedup}"
            ),
        );
        e.workflow_id = Some(id);
        e.queue_name = Some(queue);
        e.deduplication_id = Some(dedup);
        e
    }

    pub fn patching_not_enabled() -> Self {
        Self::new(
            DbosErrorCode::PatchingNotEnabled,
            "Patching system is not enabled. Set EnablePatching to true in the DBOS context configuration to use Patch and DeprecatePatch",
        )
    }

    pub fn no_application_versions() -> Self {
        Self::new(DbosErrorCode::NoApplicationVersions, "No application versions are registered")
    }

    pub fn timeout(
        workflow_id: impl Into<String>,
        step_name: impl Into<String>,
        message: impl AsRef<str>,
    ) -> Self {
        let id = workflow_id.into();
        let name = step_name.into();
        let mut msg = if name.is_empty() {
            "Operation timed out".to_string()
        } else {
            format!("Step {name} timed out")
        };
        if !id.is_empty() {
            msg.push_str(&format!(" in workflow {id}"));
        }
        let extra = message.as_ref();
        if !extra.is_empty() {
            msg.push_str(": ");
            msg.push_str(extra);
        }
        let mut e = Self::new(DbosErrorCode::Timeout, msg);
        if !id.is_empty() {
            e.workflow_id = Some(id);
        }
        if !name.is_empty() {
            e.step_name = Some(name);
        }
        e
    }
}

impl fmt::Display for DbosError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DBOS Error {}: {}", self.code, self.message)
    }
}

impl fmt::Debug for DbosError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DbosError")
            .field("code", &self.code)
            .field("message", &self.message)
            .field("workflow_id", &self.workflow_id)
            .field("step_name", &self.step_name)
            .field("source", &self.source.as_ref().map(|s| s.to_string()))
            .finish()
    }
}

impl std::error::Error for DbosError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_ref()
            .map(|e| e.as_ref() as &(dyn std::error::Error + 'static))
    }
}

/// Two `DbosError`s are equal when their codes match — the comparison semantics
/// used pervasively by the tests.
impl PartialEq for DbosError {
    fn eq(&self, other: &Self) -> bool {
        self.code == other.code
    }
}

/// Portable, cross-language error envelope (the `portable_json` error form).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PortableWorkflowError {
    pub name: String,
    pub message: String,
    /// Optional application-specific error code (number or string).
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub code: Option<serde_json::Value>,
    /// Optional structured error details.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub data: Option<serde_json::Value>,
}

impl fmt::Display for PortableWorkflowError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for PortableWorkflowError {}
