use axum::{extract::State, http::StatusCode};
use std::sync::Arc;

use crate::api::state::State as AppState;

pub async fn health(State(state): State<AppState>) -> StatusCode {
    StatusCode::OK
}
