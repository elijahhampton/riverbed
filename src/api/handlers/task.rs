use crate::api::error::{ApiError, ApiResponse, AppJson};
use crate::api::state::State as AppState;
use crate::task::{Task, TaskId};
use axum::extract::{Json, State};
use axum::http::StatusCode;
use axum::response::ErrorResponse;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Serialize, Deserialize)]
pub struct EnqueueTaskPayload {
    category: String,
    payload: serde_json::Value
}

#[axum::debug_handler]
pub async fn enqueue_task(
    State(state): State<AppState>,
    Json(data): Json<EnqueueTaskPayload>,
) -> Result<AppJson<TaskId>, ApiError> {
    let engine = state.engine;

    let EnqueueTaskPayload { category, payload } = data;
    let task_id = engine
        .enqueue_task(category, payload)
        .await
        .map_err(|err| ApiError::ServiceError(err))?;

    Ok(AppJson(task_id))
}

