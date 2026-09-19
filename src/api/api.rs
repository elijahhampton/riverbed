use axum::{
    Router,
    routing::{get, post},
};
use std::sync::Arc;
use tracing::info;

use crate::api::{
    handlers::{health::health, task::enqueue_task},
    state::State,
};

fn v1_router() -> Router {
    let api = Router::new();

    let api = v1_health_routes(api);
    let api = v1_task_routes(api);

    api.with_state(State::new())
}

fn v1_health_routes(router: Router<State>) -> Router<State> {
    router.route("/v1/health", get(health))
}

fn v1_task_routes(router: Router<State>) -> Router<State> {
    router.route("/v1/task", post(enqueue_task))
}

pub async fn serve() {
    let addr = "0.0.0.0:3000";
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    info!(address = addr, "REST API listening");
    let router = v1_router();

    axum::serve(listener, router).await.unwrap();
}
