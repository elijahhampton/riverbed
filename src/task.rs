use crate::error::LibResult;
use serde::{Serialize, de::DeserializeOwned};
use std::time::SystemTime;

/// The representation of a tasks' 'category'. The title or definition of a task is
/// represented by an arbitrary string whose meaning is interpreted by the implementer.
/// Ex: "process:transaction", "ingest:bucket_data", "process:user"
pub type TaskDefinition = String;

/// A unique identifier per task.
pub type TaskId = uuid::Uuid;

pub trait TTask: Serialize + DeserializeOwned + Send + 'static {
    const NAME: &'static str;
}

/// A task accepted by the task queue.
#[derive(Debug, Clone)]
pub struct Task {
    pub id: TaskId,
    pub definition: TaskDefinition,
    pub payload: Vec<u8>,
    pub attempts: u32,
    pub available_at: SystemTime,
}

impl Task {
    pub fn new<T: TTask>(payload: &T) -> LibResult<Self> {
        Ok(Self {
            id: TaskId::new_v4(),
            definition: T::NAME.to_owned(),
            payload: serde_json::to_vec(payload)?,
            attempts: 0,
            available_at: SystemTime::now(),
        })
    }
}

