use crate::{
    error::LibResult,
    task::{Task, TaskId},
};
use async_trait::async_trait;
use std::time::SystemTime;

/// Support for  in-memory task queues.
#[cfg(feature = "in-memory")]
mod memory;
#[cfg(feature = "in-memory")]
pub use memory::MemoryBroker;

/// Storage backend for tasks.
/// A claimed task is leased to a single executor until it is acked, retried, or failed.
#[async_trait]
pub trait TaskBroker: Send + Sync {
    /// Stores a task, to become claimable at its `available_at`.
    async fn enqueue(&self, task: Task) -> LibResult<()>;

    /// Waits until a task is due and leases it to the caller.
    /// Must be cancel-safe: dropping the returned future must not lose a task.
    async fn claim(&self) -> LibResult<Task>;

    /// Marks a claimed task as completed.
    async fn ack(&self, task_id: TaskId) -> LibResult<()>;

    /// Requeues a claimed task with its attempt count incremented, to become
    /// claimable again at `available_at`.
    async fn retry(&self, task_id: TaskId, available_at: SystemTime) -> LibResult<()>;

    /// Marks a claimed task as permanently failed. It will not be claimed again.
    async fn fail(&self, task_id: TaskId) -> LibResult<()>;
}
