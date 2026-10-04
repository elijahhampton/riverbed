//! The task type stored by brokers, and the spec used to enqueue one.

use std::time::{Duration, SystemTime};

/// The representation of a tasks' 'category'. The title or definition of a task is
/// represented by an arbitrary string whose meaning is interpreted by the implementer.
/// Ex: "process:transaction", "ingest:bucket_data", "process:user"
pub type TaskDefinition = String;

/// A unique identifier per task.
pub type TaskId = uuid::Uuid;

/// The name of a queue. Tasks are claimed by queue, so a pool of workers only receives work it has
/// handlers for.
pub type QueueName = String;

/// The queue a task is enqueued onto when its spec does not name one.
pub const DEFAULT_QUEUE: &str = "default";

/// A request to enqueue a task.
///
/// Options left unset fall back to the engine's defaults when the task runs.
///
/// ```
/// use riverbed::task::TaskSpec;
/// use std::time::Duration;
///
/// let spec = TaskSpec::new("email:send".into(), serde_json::json!({ "to": "a@example.com" }))
///     .queue("mail")
///     .available_in(Duration::from_secs(30))
///     .max_attempts(3);
/// ```
#[derive(Debug, Clone)]
pub struct TaskSpec {
    /// Task type, used to route the task to its handler.
    pub definition: TaskDefinition,
    /// Input passed to the handler.
    pub payload: serde_json::Value,
    /// Queue the task is claimed from.
    pub queue: QueueName,
    /// Earliest time the task can be claimed.
    pub available_at: SystemTime,
    /// Attempt limit for this task, overriding the engine's retry policy.
    pub max_attempts: Option<u32>,
    /// Time one execution may run before it is cancelled.
    pub timeout: Option<Duration>,
    /// Key that makes enqueueing idempotent. A spec carrying a key an unfinished task already
    /// holds creates no task.
    pub dedup_key: Option<String>,
}

impl TaskSpec {
    /// Creates a spec for a task of type `definition`, available immediately on the default queue.
    pub fn new(definition: TaskDefinition, payload: serde_json::Value) -> Self {
        Self {
            definition,
            payload,
            queue: DEFAULT_QUEUE.to_owned(),
            available_at: SystemTime::now(),
            max_attempts: None,
            timeout: None,
            dedup_key: None,
        }
    }

    /// Sets the queue the task is claimed from.
    pub fn queue(mut self, queue: impl Into<QueueName>) -> Self {
        self.queue = queue.into();
        self
    }

    /// Delays the task until `at`.
    pub fn available_at(mut self, at: SystemTime) -> Self {
        self.available_at = at;
        self
    }

    /// Delays the task by `delay`, measured from now rather than from when it is claimed.
    pub fn available_in(mut self, delay: Duration) -> Self {
        self.available_at = SystemTime::now() + delay;
        self
    }

    /// Sets the attempt limit for this task, overriding the engine's retry policy.
    pub fn max_attempts(mut self, max_attempts: u32) -> Self {
        self.max_attempts = Some(max_attempts);
        self
    }

    /// Sets how long one execution may run before it is cancelled.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Sets the key that makes enqueueing this task idempotent.
    pub fn dedup_key(mut self, key: impl Into<String>) -> Self {
        self.dedup_key = Some(key.into());
        self
    }
}

/// A task accepted by the task queue.
#[derive(Debug, Clone)]
pub struct Task {
    /// Unique identifier, assigned when the task is created.
    pub id: TaskId,
    /// Task type, used to route the task to its handler.
    pub definition: TaskDefinition,
    /// Input passed to the handler.
    pub payload: serde_json::Value,
    /// Queue the task is claimed from.
    pub queue: QueueName,
    /// Number of earlier executions that ended in a handler failure. The current attempt is
    /// `attempts + 1`.
    pub attempts: u32,
    /// Number of times a lease on this task expired before its outcome was recorded. Counted
    /// separately from `attempts`: a worker that vanished says nothing about the task, so it must
    /// not consume the retry budget.
    pub reclaims: u32,
    /// Earliest time the task can be claimed.
    pub available_at: SystemTime,
    /// Attempt limit for this task, overriding the engine's retry policy.
    pub max_attempts: Option<u32>,
    /// Time one execution may run before it is cancelled.
    pub timeout: Option<Duration>,
    /// Key that makes enqueueing idempotent.
    pub dedup_key: Option<String>,
}

impl Task {
    /// Creates a task from `spec`, assigning it an identifier.
    pub fn from_spec(spec: TaskSpec) -> Self {
        Self {
            id: TaskId::new_v4(),
            definition: spec.definition,
            payload: spec.payload,
            queue: spec.queue,
            attempts: 0,
            reclaims: 0,
            available_at: spec.available_at,
            max_attempts: spec.max_attempts,
            timeout: spec.timeout,
            dedup_key: spec.dedup_key,
        }
    }

    /// Creates a task of type `definition`, available immediately on the default queue.
    pub fn new(definition: TaskDefinition, payload: serde_json::Value) -> Self {
        Self::from_spec(TaskSpec::new(definition, payload))
    }

    /// The number of the execution that runs next, starting at 1.
    pub fn attempt(&self) -> u32 {
        self.attempts + 1
    }
}

/// Where a task sits in its lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskState {
    /// Waiting to be claimed.
    Pending,
    /// Leased to an executor.
    Leased,
    /// Completed successfully.
    Completed,
    /// Terminally failed. It will not be claimed again unless it is requeued.
    Failed,
}

impl TaskState {
    /// A short name for the state, for error messages and logs.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Leased => "leased",
            Self::Completed => "completed",
            Self::Failed => "failed",
        }
    }
}
