//! What an executor records when a task ends.

use crate::task::TaskSpec;

/// The outcome of a successful execution.
///
/// A broker stores the result and enqueues the spawned tasks in the same operation that releases
/// the lease, so a crash cannot leave a task acknowledged without its children, or children
/// without a recorded parent. How strong that guarantee is depends on the backend; the in-memory
/// broker applies all three under one lock.
#[derive(Debug, Clone, Default)]
pub struct Completion {
    /// Opaque value stored with the task, for a caller to read back later.
    pub result: Option<serde_json::Value>,
    /// Tasks enqueued as part of this completion.
    pub spawn: Vec<TaskSpec>,
}

impl Completion {
    /// A completion with no result and no spawned tasks.
    pub fn empty() -> Self {
        Self::default()
    }

    /// Sets the result stored with the task.
    pub fn result(mut self, result: serde_json::Value) -> Self {
        self.result = Some(result);
        self
    }

    /// Enqueues `spec` as part of this completion.
    pub fn spawn(mut self, spec: TaskSpec) -> Self {
        self.spawn.push(spec);
        self
    }

    /// Enqueues every spec in `specs` as part of this completion.
    pub fn spawn_all(mut self, specs: impl IntoIterator<Item = TaskSpec>) -> Self {
        self.spawn.extend(specs);
        self
    }
}

/// Lets a handler that has nothing to report return `Ok(())`.
impl From<()> for Completion {
    fn from(_: ()) -> Self {
        Self::empty()
    }
}

/// Lets a handler return a result value directly.
impl From<serde_json::Value> for Completion {
    fn from(result: serde_json::Value) -> Self {
        Self::empty().result(result)
    }
}

/// Why a task failed terminally.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureKind {
    /// The payload could not be deserialized into the handler's input type.
    PayloadDecode,
    /// The handler returned an error and no attempts were left.
    HandlerError,
    /// The handler panicked and no attempts were left.
    HandlerPanic,
    /// An execution exceeded the task's timeout and no attempts were left.
    Timeout,
    /// Recorded by a caller rather than by the engine.
    Manual,
}

impl FailureKind {
    /// Whether retrying could plausibly have produced a different outcome.
    ///
    /// A payload that does not decode will not decode on the next attempt either, so that task is
    /// failed without consuming its retry budget.
    pub fn is_retryable(self) -> bool {
        !matches!(self, Self::PayloadDecode)
    }

    /// A short name for the kind, for logs.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::PayloadDecode => "payload_decode",
            Self::HandlerError => "handler_error",
            Self::HandlerPanic => "handler_panic",
            Self::Timeout => "timeout",
            Self::Manual => "manual",
        }
    }
}

/// The record kept when a task fails terminally, so a later reader knows what was tried.
#[derive(Debug, Clone)]
pub struct FailureRecord {
    /// Why the task failed.
    pub kind: FailureKind,
    /// Human-readable detail, typically the final error.
    pub detail: String,
    /// Executions attempted before the task was given up on.
    pub attempts: u32,
}

impl FailureRecord {
    /// Creates a record of `kind` with `detail`.
    pub fn new(kind: FailureKind, detail: impl Into<String>) -> Self {
        Self {
            kind,
            detail: detail.into(),
            attempts: 0,
        }
    }

    /// Sets how many executions were attempted before the task was given up on.
    pub fn attempts(mut self, attempts: u32) -> Self {
        self.attempts = attempts;
        self
    }
}
