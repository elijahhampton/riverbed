use crate::{
    handler::HandlerError,
    task::{TaskDefinition, TaskId},
};
use std::result::Result;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum CoreErr {
    #[error("no handler registered for task type `{0}`")]
    HandlerNotFound(TaskDefinition),
    #[error("a handler is already registered for task type `{0}`")]
    DuplicateHandler(&'static str),
    #[error("failed to encode or decode task payload")]
    Payload(#[from] serde_json::Error),
    #[error("task handler failed")]
    HandlerFailed(#[source] HandlerError),
    #[error("no lease held for task `{0}`")]
    LeaseNotFound(TaskId),
    #[error("broker operation failed")]
    Broker(#[source] Box<dyn std::error::Error + Send + Sync>),
}

pub type LibResult<T> = Result<T, CoreErr>;
