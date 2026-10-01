//! The task type stored by brokers.

use crate::error::LibResult;
use std::time::SystemTime;

/// The representation of a tasks' 'category'. The title or definition of a task is
/// represented by an arbitrary string whose meaning is interpreted by the implementer.
/// Ex: "process:transaction", "ingest:bucket_data", "process:user"
pub type TaskDefinition = String;

/// A unique identifier per task.
pub type TaskId = uuid::Uuid;

/// A task accepted by the task queue.
#[derive(Debug, Clone)]
pub struct Task {
    /// Unique identifier, assigned when the task is created.
    pub id: TaskId,
    /// Task type, used to route the task to its handler.
    pub definition: String,
    /// Input passed to the handler.
    pub payload: serde_json::Value,
    /// Number of earlier execution attempts. The current attempt is `attempts + 1`.
    pub attempts: u32,
    /// Earliest time the task can be claimed.
    pub available_at: SystemTime,
}

impl Task {
    /// Creates a task of type `definition` that is available immediately.
    pub fn new(definition: String, payload: serde_json::Value) -> LibResult<Self> {
        Ok(Self {
            id: TaskId::new_v4(),
            definition,
            payload,
            attempts: 0,
            available_at: SystemTime::now(),
        })
    }
}
