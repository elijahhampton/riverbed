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
    /// recorded.
    #[error("no lease held for task `{0}`")]
    LeaseNotFound(TaskId),
    /// The broker's storage backend failed.
    #[error("broker operation failed")]
    Broker(#[source] Box<dyn std::error::Error + Send + Sync>),
}

/// A [`Result`] with [`CoreErr`] as its error type.
pub type LibResult<T> = Result<T, CoreErr>;
