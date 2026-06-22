//! Conductor — a reconnecting WebSocket client for the DBOS cloud control plane.
//!
//! The conductor dials *out* to the DBOS
//! Conductor service, receives command messages over a long-lived WebSocket,
//! executes them against the local `dbos-core` runtime, and replies in-band.
//! It is a client, not a server.
//!
//! Lifecycle: [`start_conductor`] spawns a background task and returns a
//! [`ConductorHandle`]; [`ConductorHandle::shutdown`] stops it gracefully. The
//! task runs a connect → read → dispatch loop with:
//!
//! - **Exponential reconnect backoff** 1s → 30s with [0.5, 1.5) jitter, reset
//!   to 1s on a successful connect.
//! - **A ping watchdog**: a control-frame ping every ~20s; if no traffic (pong
//!   or message) arrives within ~30s the read times out and the loop reconnects.
//!
//! The API key is embedded in the WebSocket URL *path*
//! (`<base>/websocket/<app>/<api_key>`), and the scheme is inherited from the
//! configured base URL (typically `ws`/`wss`).

mod handlers;
pub mod protocol;

use std::sync::Arc;
use std::time::Duration;

use dbos::DbosContext;
use futures_util::{SinkExt, StreamExt};
use rand::Rng;
use tokio::net::TcpStream;
use tokio::task::JoinHandle;
use tokio::time::{Instant, MissedTickBehavior};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use tokio_util::sync::CancellationToken;

use protocol::BaseMessage;

/// How often we send a WebSocket ping control frame.
const PING_INTERVAL: Duration = Duration::from_secs(20);
/// Read deadline: no traffic within this window => the connection is dead.
const PING_TIMEOUT: Duration = Duration::from_secs(30);
/// Initial reconnect backoff.
const INITIAL_RECONNECT_WAIT: Duration = Duration::from_secs(1);
/// Maximum reconnect backoff.
const MAX_RECONNECT_WAIT: Duration = Duration::from_secs(30);

/// A connected WebSocket stream over a (possibly TLS) TCP connection.
type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Handle to a running conductor: lets the caller trigger a graceful shutdown.
pub struct ConductorHandle {
    cancel: CancellationToken,
    task: JoinHandle<()>,
}

impl ConductorHandle {
    /// Signal the conductor to stop and wait for its task to finish.
    pub async fn shutdown(self) {
        self.cancel.cancel();
        let _ = self.task.await;
    }
}

/// Start the conductor against `base_url` for `app_name`/`api_key`, returning a
/// [`ConductorHandle`]. The connection runs in a background task and reconnects
/// automatically; the returned handle stops it via
/// [`ConductorHandle::shutdown`].
///
/// The WebSocket URL is `<base_url>/websocket/<app_name>/<api_key>`.
pub async fn start_conductor(
    ctx: Arc<DbosContext>,
    base_url: String,
    app_name: String,
    api_key: String,
) -> ConductorHandle {
    let cancel = CancellationToken::new();
    let url = build_ws_url(&base_url, &app_name, &api_key);

    let task = {
        let cancel = cancel.clone();
        tokio::spawn(async move {
            run_loop(ctx, url, cancel).await;
        })
    };

    tracing::info!(app = %app_name, "conductor launched");
    ConductorHandle { cancel, task }
}

/// Build the conductor WebSocket URL: `<base>/websocket/<app>/<api_key>`. The
/// scheme/host are taken from the base URL as-is. Path segments are joined
/// without introducing double slashes.
fn build_ws_url(base_url: &str, app_name: &str, api_key: &str) -> String {
    let trimmed = base_url.trim_end_matches('/');
    format!("{trimmed}/websocket/{app_name}/{api_key}")
}

/// The connect → read → dispatch loop. Owns the (re)connection lifecycle and
/// the per-connection ping watchdog. Returns only when `cancel` fires.
async fn run_loop(ctx: Arc<DbosContext>, url: String, cancel: CancellationToken) {
    let mut reconnect_wait = INITIAL_RECONNECT_WAIT;

    loop {
        if cancel.is_cancelled() {
            return;
        }

        // --- connect (with backoff on failure) ---
        let ws = tokio::select! {
            biased;
            _ = cancel.cancelled() => return,
            result = connect(&url) => match result {
                Ok(ws) => ws,
                Err(e) => {
                    tracing::warn!(error = %e, "failed to connect to conductor");
                    let wait = with_jitter(reconnect_wait);
                    tokio::select! {
                        _ = cancel.cancelled() => return,
                        _ = tokio::time::sleep(wait) => {}
                    }
                    reconnect_wait = next_backoff(reconnect_wait);
                    continue;
                }
            },
        };

        tracing::info!("connected to DBOS conductor");
        reconnect_wait = INITIAL_RECONNECT_WAIT;

        // --- serve this connection until it dies or we are cancelled ---
        serve_connection(&ctx, ws, &cancel).await;

        if cancel.is_cancelled() {
            return;
        }
        tracing::debug!("conductor connection closed; reconnecting");
    }
}

/// Dial the conductor and complete the WebSocket handshake.
async fn connect(url: &str) -> Result<WsStream, tokio_tungstenite::tungstenite::Error> {
    let (ws, _resp) = tokio_tungstenite::connect_async(url).await?;
    Ok(ws)
}

/// Read and dispatch messages on a single connection, driving the ping
/// watchdog. Returns when the connection dies (read error / timeout / close) or
/// `cancel` fires. All writes (pings, responses, close) happen on this task so
/// no write mutex is needed.
async fn serve_connection(ctx: &Arc<DbosContext>, mut ws: WsStream, cancel: &CancellationToken) {
    let mut ping_ticker = tokio::time::interval(PING_INTERVAL);
    ping_ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    // Skip the immediate first tick.
    ping_ticker.tick().await;

    // The read watchdog: reset whenever any frame (incl. pong) arrives.
    let mut deadline = Instant::now() + PING_TIMEOUT;

    loop {
        tokio::select! {
            biased;

            // Graceful shutdown: send a close frame and stop.
            _ = cancel.cancelled() => {
                let _ = ws.send(Message::Close(None)).await;
                let _ = ws.close(None).await;
                return;
            }

            // Ping watchdog: send a ping every interval.
            _ = ping_ticker.tick() => {
                if let Err(e) = ws.send(Message::Ping(Vec::new().into())).await {
                    tracing::warn!(error = %e, "ping failed, signaling reconnection");
                    return;
                }
            }

            // Read deadline: no traffic within PING_TIMEOUT => dead connection.
            _ = tokio::time::sleep_until(deadline) => {
                tracing::debug!("read deadline reached; reconnecting");
                let _ = ws.close(None).await;
                return;
            }

            // Inbound frame.
            frame = ws.next() => {
                match frame {
                    Some(Ok(msg)) => {
                        // Any frame is liveness; push the read deadline forward.
                        deadline = Instant::now() + PING_TIMEOUT;
                        match msg {
                            Message::Text(text) => {
                                if let Some(reply) = handle_message(ctx, text.as_bytes()).await {
                                    if let Err(e) = ws.send(Message::Text(reply.into())).await {
                                        tracing::warn!(error = %e, "failed to send response");
                                        return;
                                    }
                                }
                            }
                            Message::Ping(payload) => {
                                // Reply to server pings to keep the link alive.
                                let _ = ws.send(Message::Pong(payload)).await;
                            }
                            Message::Pong(_) => { /* liveness already recorded */ }
                            Message::Close(_) => {
                                tracing::debug!("conductor sent close; reconnecting");
                                return;
                            }
                            // Binary / other frames carry no protocol messages.
                            other => {
                                tracing::warn!(?other, "unexpected non-text frame; reconnecting");
                                return;
                            }
                        }
                    }
                    Some(Err(e)) => {
                        tracing::warn!(error = %e, "websocket read error; reconnecting");
                        return;
                    }
                    None => {
                        tracing::debug!("websocket stream ended; reconnecting");
                        return;
                    }
                }
            }
        }
    }
}

/// Parse the base envelope and dispatch. Returns `Some(json)` to send back, or
/// `None` only when the *base* envelope fails to parse (logged + skipped, no
/// response). A typed-body parse failure still produces a best-effort error
/// response.
async fn handle_message(ctx: &Arc<DbosContext>, data: &[u8]) -> Option<String> {
    let base: BaseMessage = match serde_json::from_slice(data) {
        Ok(b) => b,
        Err(e) => {
            tracing::error!(error = %e, "failed to parse base message");
            return None;
        }
    };

    match handlers::dispatch(ctx, &base.r#type, &base.request_id, data).await {
        Ok(reply) => Some(reply),
        Err(err) => {
            // A typed-body parse error: log it, but still answer with a
            // well-formed error response rather than dropping the request.
            tracing::error!(
                message_type = %base.r#type,
                request_id = %base.request_id,
                error = %err.message,
                "failed to handle message",
            );
            serde_json::to_string(&protocol::BaseResponse::err(
                &base.r#type,
                &base.request_id,
                err.message,
            ))
            .ok()
        }
    }
}

/// Double the backoff, clamped to [`MAX_RECONNECT_WAIT`].
fn next_backoff(current: Duration) -> Duration {
    (current * 2).min(MAX_RECONNECT_WAIT)
}

/// Apply [0.5, 1.5) multiplicative jitter to a backoff duration.
fn with_jitter(wait: Duration) -> Duration {
    let factor = 0.5 + rand::rng().random::<f64>();
    wait.mul_f64(factor)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ws_url_joins_without_double_slash() {
        assert_eq!(
            build_ws_url("wss://api.dbos.dev", "app", "key"),
            "wss://api.dbos.dev/websocket/app/key"
        );
        assert_eq!(
            build_ws_url("wss://api.dbos.dev/", "app", "key"),
            "wss://api.dbos.dev/websocket/app/key"
        );
    }

    #[test]
    fn backoff_doubles_and_clamps() {
        assert_eq!(next_backoff(Duration::from_secs(1)), Duration::from_secs(2));
        assert_eq!(
            next_backoff(Duration::from_secs(20)),
            Duration::from_secs(30)
        );
        assert_eq!(
            next_backoff(Duration::from_secs(30)),
            Duration::from_secs(30)
        );
    }

    #[test]
    fn jitter_stays_in_range() {
        let base = Duration::from_secs(10);
        for _ in 0..100 {
            let j = with_jitter(base);
            assert!(j >= base.mul_f64(0.5));
            assert!(j < base.mul_f64(1.5));
        }
    }
}
