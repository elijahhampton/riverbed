use crate::error::LibResult;
use serde::{Serialize, de::DeserializeOwned};
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
    pub id: TaskId,
    pub definition: String,
    pub payload: serde_json::Value,
    pub attempts: u32,
    pub available_at: SystemTime,
}

impl Task {
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
