//! HTTP server for the task API.

use axum::{
    Router,
    routing::{get, post},
};
use std::{net::SocketAddr, sync::Arc};
use tracing::info;

use crate::engine::Engine;
use crate::rest::{
    handlers::{health::health, task::enqueue_task},
    state::State,
};

/// Builds the API router, backed by `engine`.
///
/// Use this to mount the API in an existing axum application. Otherwise, use [`serve`].
pub fn router(engine: Arc<Engine>) -> Router {
    let api = Router::new();

    let api = v1_health_routes(api);
    let api = v1_task_routes(api);

    api.with_state(State { engine })
}

fn v1_health_routes(router: Router<State>) -> Router<State> {
    router.route("/v1/health", get(health))
}

fn v1_task_routes(router: Router<State>) -> Router<State> {
    router.route("/v1/task", post(enqueue_task))
}

/// Serves the API on `addr` until the future is dropped or the server fails.
///
/// The engine must be started separately with [`Engine::start`] for enqueued tasks to run.
pub async fn serve(engine: Arc<Engine>, addr: SocketAddr) -> std::io::Result<()> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let local_addr = listener.local_addr()?;
    info!(address = %local_addr, "REST API listening");

    axum::serve(listener, router(engine)).await
}
