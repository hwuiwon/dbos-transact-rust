//! Admin HTTP server for DBOS Transact.
//!
//! An [`axum`]-based operational HTTP server. It exposes health, recovery,
//! list/get workflows, cancel/resume/fork, garbage-collect, and global-timeout
//! endpoints, delegating all real work to the `dbos-core` (`dbos`) public API.
//!
//! The [`conductor`] module additionally provides a reconnecting WebSocket
//! client ([`conductor::start_conductor`]) for the DBOS cloud control plane.
//!
//! # Quick start
//!
//! ```no_run
//! # use std::sync::Arc;
//! # async fn run(ctx: Arc<dbos::DbosContext>) -> Result<(), dbos::DbosError> {
//! let handle = dbos_server::start_admin_server(ctx, 3001).await?;
//! // ... serve ...
//! handle.shutdown().await;
//! # Ok(())
//! # }
//! ```

pub mod conductor;
mod handlers;
mod transform;

use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;

use axum::Router;
use axum::routing::{get, post};
use dbos::{DbosContext, DbosError};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

pub use conductor::{ConductorHandle, start_conductor};
pub use transform::{to_list_response, to_step_response};

/// Handle to a running admin server: lets the caller trigger a graceful shutdown
/// and discover the bound address (useful when binding to an ephemeral port).
pub struct AdminServerHandle {
    addr: SocketAddr,
    shutdown_tx: Option<oneshot::Sender<()>>,
    task: JoinHandle<()>,
}

impl AdminServerHandle {
    /// The address the server is bound to.
    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    /// Trigger a graceful shutdown and wait for the server task to finish.
    pub async fn shutdown(mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        let _ = self.task.await;
    }
}

/// Build the admin [`Router`] backed by the given context.
///
/// Exposed so callers (and tests) can mount it into a larger service or drive it
/// via `tower::ServiceExt::oneshot`.
pub fn admin_router(ctx: Arc<DbosContext>) -> Router {
    Router::new()
        .route("/dbos-healthz", get(handlers::healthz))
        .route("/dbos-workflow-recovery", post(handlers::workflow_recovery))
        .route(
            "/dbos-workflow-queues-metadata",
            get(handlers::workflow_queues_metadata),
        )
        .route("/deactivate", get(handlers::deactivate))
        .route("/dbos-garbage-collect", post(handlers::garbage_collect))
        .route("/dbos-global-timeout", post(handlers::global_timeout))
        .route("/workflows", post(handlers::list_workflows))
        .route("/workflows/{id}", get(handlers::get_workflow))
        .route("/workflows/{id}/steps", get(handlers::get_workflow_steps))
        .route("/workflows/{id}/cancel", post(handlers::cancel_workflow))
        .route("/workflows/{id}/resume", post(handlers::resume_workflow))
        .route("/workflows/{id}/fork", post(handlers::fork_workflow))
        .with_state(ctx)
}

/// Bind to `port` (all interfaces) and serve the admin API in a background task.
///
/// Returns once the listener is bound, so bind errors surface synchronously.
/// Use [`AdminServerHandle::shutdown`] for a graceful stop.
pub async fn start_admin_server(
    ctx: Arc<DbosContext>,
    port: u16,
) -> Result<AdminServerHandle, DbosError> {
    let listener = tokio::net::TcpListener::bind(SocketAddr::from((Ipv4Addr::UNSPECIFIED, port)))
        .await
        .map_err(|e| {
            DbosError::initialization(format!("failed to bind admin server on port {port}: {e}"))
                .with_source(e)
        })?;
    let addr = listener
        .local_addr()
        .map_err(|e| DbosError::initialization(format!("failed to read bound address: {e}")))?;

    let router = admin_router(ctx);
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    let task = tokio::spawn(async move {
        let result = axum::serve(listener, router)
            .with_graceful_shutdown(async move {
                let _ = shutdown_rx.await;
            })
            .await;
        if let Err(e) = result {
            tracing::error!(error = %e, "admin server exited with error");
        }
    });

    tracing::info!(%addr, "admin server listening");
    Ok(AdminServerHandle {
        addr,
        shutdown_tx: Some(shutdown_tx),
        task,
    })
}
