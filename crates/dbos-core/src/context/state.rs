//! Per-run workflow state and the workflow/step contexts.
//!
//! The per-run state is carried explicitly in [`WfCtx`] / [`StepCtx`].

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use tokio_util::sync::CancellationToken;

use crate::context::DbosContext;

/// Runtime state for a single workflow execution. Shared (via `Arc`) by every
/// [`WfCtx`] clone for that run; the step-ID counter must advance sequentially.
pub(crate) struct WorkflowState {
    pub workflow_id: String,
    /// Pre-increment counter starting at -1, so the first step gets id 0.
    pub step_id: AtomicI64,
    pub is_portable: bool,
    pub authenticated_user: String,
    pub assumed_role: String,
    pub authenticated_roles: Vec<String>,
}

impl WorkflowState {
    pub(crate) fn next_step_id(&self) -> i64 {
        self.step_id.fetch_add(1, Ordering::SeqCst) + 1
    }
    pub(crate) fn current_step_id(&self) -> i64 {
        self.step_id.load(Ordering::SeqCst)
    }
}

/// The context handed to a workflow body. Cheaply cloneable (two `Arc`s + a flag).
#[derive(Clone)]
pub struct WfCtx {
    pub(crate) ctx: Arc<DbosContext>,
    pub(crate) state: Arc<WorkflowState>,
    /// True when this context is executing inside a step body (nested steps run
    /// inline without a fresh checkpoint).
    pub(crate) is_within_step: bool,
}

impl WfCtx {
    /// The id of the currently executing workflow.
    pub fn workflow_id(&self) -> &str {
        &self.state.workflow_id
    }

    /// The most recently allocated step id.
    pub fn step_id(&self) -> i64 {
        self.state.current_step_id()
    }

    /// The owning DBOS context.
    pub fn context(&self) -> &Arc<DbosContext> {
        &self.ctx
    }

    /// A child of the engine cancellation token, cancelled on shutdown.
    pub fn cancellation_token(&self) -> CancellationToken {
        self.ctx.token.child_token()
    }

    pub(crate) fn next_step_id(&self) -> i64 {
        self.state.next_step_id()
    }

    pub(crate) fn is_portable(&self) -> bool {
        self.state.is_portable
    }
}

/// The context handed to a step body. A leaf context: it cannot spawn child
/// workflows (that is an error), but exposes the workflow id and a cancellation
/// token for cooperative cancellation of long-running step work.
#[derive(Clone)]
pub struct StepCtx {
    pub(crate) ctx: Arc<DbosContext>,
    pub(crate) workflow_id: String,
    pub(crate) step_id: i64,
}

impl StepCtx {
    pub fn workflow_id(&self) -> &str {
        &self.workflow_id
    }

    pub fn step_id(&self) -> i64 {
        self.step_id
    }

    pub fn cancellation_token(&self) -> CancellationToken {
        self.ctx.token.child_token()
    }
}
