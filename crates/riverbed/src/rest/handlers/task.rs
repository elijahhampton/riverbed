use crate::broker::TaskRecord;
use crate::rest::error::{ApiError, ApiResponse, AppJson};
use crate::rest::state::State as AppState;
use crate::task::{TaskId, TaskSpec};
use axum::extract::{Json, Path, State};
use axum::http::StatusCode;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct EnqueueTaskPayload {
    category: String,
    payload: serde_json::Value,
    #[serde(default)]
    queue: Option<String>,
    #[serde(default)]
    max_attempts: Option<u32>,
    #[serde(default)]
    dedup_key: Option<String>,
}

#[axum::debug_handler]
pub async fn enqueue_task(
    State(state): State<AppState>,
    Json(data): Json<EnqueueTaskPayload>,
) -> ApiResponse<AppJson<TaskId>> {
    let EnqueueTaskPayload {
        category,
        payload,
        queue,
        max_attempts,
        dedup_key,
    } = data;

    let mut spec = TaskSpec::new(category, payload);
    if let Some(queue) = queue {
        spec = spec.queue(queue);
    }
    if let Some(max_attempts) = max_attempts {
        spec = spec.max_attempts(max_attempts);
    }
    if let Some(dedup_key) = dedup_key {
        spec = spec.dedup_key(dedup_key);
    }

    let task_id = state
        .engine
        .enqueue(spec)
        .await
        .map_err(ApiError::ServiceError)?;

    Ok(AppJson(task_id))
}

/// What a task looks like over HTTP.
#[derive(Serialize)]
pub struct TaskView {
    id: TaskId,
    category: String,
    queue: String,
    state: &'static str,
    attempts: u32,
    reclaims: u32,
    result: Option<serde_json::Value>,
    failure: Option<FailureView>,
}

#[derive(Serialize)]
pub struct FailureView {
    kind: &'static str,
    detail: String,
    attempts: u32,
}

impl From<TaskRecord> for TaskView {
    fn from(record: TaskRecord) -> Self {
        Self {
            id: record.task.id,
            category: record.task.definition,
            queue: record.task.queue,
            state: record.state.as_str(),
            attempts: record.task.attempts,
            reclaims: record.task.reclaims,
            result: record.result,
            failure: record.failure.map(|failure| FailureView {
                kind: failure.kind.as_str(),
                detail: failure.detail,
                attempts: failure.attempts,
            }),
        }
    }
}

#[axum::debug_handler]
pub async fn get_task(
    State(state): State<AppState>,
    Path(id): Path<TaskId>,
) -> Result<AppJson<TaskView>, StatusCode> {
    let store = state.store.as_ref().ok_or(StatusCode::NOT_IMPLEMENTED)?;
    match store.get(id).await {
        Ok(Some(record)) => Ok(AppJson(record.into())),
        Ok(None) => Err(StatusCode::NOT_FOUND),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}
