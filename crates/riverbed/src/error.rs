//! Error types.

use crate::{
    handler::HandlerError,
    task::{TaskDefinition, TaskId},
};
use std::result::Result;
use thiserror::Error;

/// Errors returned by riverbed.
#[derive(Error, Debug)]
pub enum CoreErr {
    /// No handler is registered for the task type.
    #[error("no handler registered for task type `{0}`")]
    HandlerNotFound(TaskDefinition),
    /// A handler is already registered for the task type.
    #[error("a handler is already registered for task type `{0}`")]
    DuplicateHandler(String),
    /// A task payload could not be serialized or deserialized.
    #[error("failed to encode or decode task payload")]
    Payload(#[from] serde_json::Error),
    /// A task handler returned an error.
    #[error("task handler failed")]
    HandlerFailed(#[source] HandlerError),
    /// The broker holds no lease for the task, for example because its outcome was already
    /// recorded or the lease expired and the task went back to the queue.
    #[error("no lease held for task `{0}`")]
    LeaseNotFound(TaskId),
    /// The lease expired and the task was granted to another executor. The caller's write was
    /// rejected rather than applied over the newer lease.
    #[error("lease on task `{0}` was reclaimed and granted to another executor")]
    LeaseFenced(TaskId),
    /// The backend holds no task with this identifier.
    #[error("no task found with id `{0}`")]
    TaskNotFound(TaskId),
    /// The task is not in a state the operation accepts.
    #[error("task `{id}` is {state}, which this operation does not accept")]
    UnexpectedState {
        /// The task the operation was attempted on.
        id: TaskId,
        /// The state it was found in.
        state: &'static str,
    },
    /// The broker's storage backend failed.
    #[error("broker operation failed")]
    Broker(#[source] Box<dyn std::error::Error + Send + Sync>),
}

/// A [`Result`] with [`CoreErr`] as its error type.
pub type LibResult<T> = Result<T, CoreErr>;
