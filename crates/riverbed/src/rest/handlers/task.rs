use crate::rest::error::{ApiError, ApiResponse, AppJson};
use crate::rest::state::State as AppState;
use crate::task::TaskId;
use axum::extract::{Json, State};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct EnqueueTaskPayload {
    category: String,
    payload: serde_json::Value,
}

#[axum::debug_handler]
pub async fn enqueue_task(
    State(state): State<AppState>,
    Json(data): Json<EnqueueTaskPayload>,
) -> ApiResponse<AppJson<TaskId>> {
    let engine = state.engine;

    let EnqueueTaskPayload { category, payload } = data;
    let task_id = engine
        .enqueue_task(category, payload)
        .await
        .map_err(ApiError::ServiceError)?;

    Ok(AppJson(task_id))
}
