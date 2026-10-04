//! Storage backends for tasks.

use crate::{
    completion::{Completion, FailureRecord},
    error::LibResult,
    lease::Lease,
    task::{QueueName, Task, TaskId, TaskSpec, TaskState},
};
use async_trait::async_trait;
use std::time::{Duration, SystemTime};

/// Support for  in-memory task queues.
#[cfg(feature = "in-memory")]
mod memory;
#[cfg(feature = "in-memory")]
pub use memory::MemoryBroker;

/// How long a claimed task stays leased before the broker may grant it to another executor.
pub const DEFAULT_LEASE_DURATION: Duration = Duration::from_secs(30);

/// The queues a claimer will accept work from.
///
/// Filtering happens in the broker rather than the executor because only the broker can see the
/// whole queue. An executor that claimed anything and then looked for a handler would take work it
/// cannot serve, and in a deployment running more than one pool that is a routing mistake rather
/// than a failure of the task.
#[derive(Debug, Clone, Default)]
pub struct ClaimFilter {
    queues: Vec<QueueName>,
}

impl ClaimFilter {
    /// Accepts a task from any queue.
    pub fn any() -> Self {
        Self::default()
    }

    /// Accepts a task from any of `queues`. An empty iterator accepts any queue.
    pub fn queues(queues: impl IntoIterator<Item = impl Into<QueueName>>) -> Self {
        Self {
            queues: queues.into_iter().map(Into::into).collect(),
        }
    }

    /// Whether the filter accepts every queue.
    pub fn is_any(&self) -> bool {
        self.queues.is_empty()
    }

    /// Whether the filter accepts `queue`.
    pub fn matches(&self, queue: &str) -> bool {
        self.is_any() || self.queues.iter().any(|q| q == queue)
    }

    /// The queues the filter names, empty when it accepts any.
    pub fn named(&self) -> &[QueueName] {
        &self.queues
    }
}

/// Storage backend for tasks.
///
/// A claimed task is leased to a single executor until it is acked, retried, released, or failed.
/// The lease carries a token, and every method that records an outcome takes the lease rather than
/// an id, so a broker can reject a holder whose lease has since been reclaimed.
#[async_trait]
pub trait TaskBroker: Send + Sync {
    /// Stores a task, to become claimable at its `available_at`, and returns its identifier.
    ///
    /// When the spec carries a dedup key that an unfinished task already holds, no task is stored
    /// and that task's identifier is returned instead.
    async fn enqueue(&self, spec: TaskSpec) -> LibResult<TaskId>;

    /// Waits until a task the filter accepts is due, and leases it to the caller.
    /// Must be cancel-safe: dropping the returned future must not lose a task.
    async fn claim(&self, filter: &ClaimFilter) -> LibResult<Lease>;

    /// Extends the lease and returns its new expiry.
    ///
    /// Callers heartbeat for as long as a handler runs. The returned expiry can be ignored; the
    /// broker is authoritative.
    ///
    /// # Errors
    ///
    /// Returns [`CoreErr::LeaseFenced`](crate::error::CoreErr::LeaseFenced) when the lease was
    /// reclaimed and granted to another executor, and
    /// [`CoreErr::LeaseNotFound`](crate::error::CoreErr::LeaseNotFound) when the task is no longer
    /// leased at all.
    async fn heartbeat(&self, lease: &Lease) -> LibResult<SystemTime>;

    /// Marks a leased task as completed, storing the completion's result and enqueueing the tasks
    /// it spawns.
    ///
    /// The result, the spawned tasks, and the release of the lease are one operation: a backend
    /// must not apply some of them and not the rest.
    async fn ack(&self, lease: &Lease, completion: Completion) -> LibResult<()>;

    /// Requeues a leased task with its attempt count incremented, to become claimable again at
    /// `available_at`.
    async fn retry(&self, lease: &Lease, available_at: SystemTime) -> LibResult<()>;

    /// Returns a leased task to the queue without recording an outcome, leaving its attempt count
    /// unchanged. Used when an executor stops before its handler reached a verdict.
    async fn release(&self, lease: &Lease) -> LibResult<()>;

    /// Marks a leased task as permanently failed, retaining `failure` for later inspection. It
    /// will not be claimed again unless it is requeued.
    async fn fail(&self, lease: &Lease, failure: FailureRecord) -> LibResult<()>;
}

/// What a task looks like to a reader.
#[derive(Debug, Clone)]
pub struct TaskRecord {
    /// The task itself.
    pub task: Task,
    /// Where the task sits in its lifecycle.
    pub state: TaskState,
    /// The result stored by a successful completion.
    pub result: Option<serde_json::Value>,
    /// Why the task failed, when it did.
    pub failure: Option<FailureRecord>,
    /// When the task was first enqueued.
    pub enqueued_at: SystemTime,
    /// When the task's state last changed.
    pub updated_at: SystemTime,
}

/// Which tasks a listing returns.
#[derive(Debug, Clone, Default)]
pub struct ListFilter {
    /// Only tasks in this state.
    pub state: Option<TaskState>,
    /// Only tasks on this queue.
    pub queue: Option<QueueName>,
    /// Maximum number of records to return. Zero means the backend's own default.
    pub limit: usize,
}

impl ListFilter {
    /// Only tasks in `state`.
    pub fn state(mut self, state: TaskState) -> Self {
        self.state = Some(state);
        self
    }

    /// Only tasks on `queue`.
    pub fn queue(mut self, queue: impl Into<QueueName>) -> Self {
        self.queue = Some(queue.into());
        self
    }

    /// At most `limit` records.
    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = limit;
        self
    }
}

/// Read access to the tasks a broker holds.
///
/// Separate from [`TaskBroker`] because a backend can be perfectly good at being written to without
/// being queryable, and because inspection is not on the execution path.
#[async_trait]
pub trait TaskStore: Send + Sync {
    /// Returns the task with `id`, or nothing if the backend no longer holds it.
    async fn get(&self, id: TaskId) -> LibResult<Option<TaskRecord>>;

    /// Returns the tasks the filter accepts, most recently updated first.
    async fn list(&self, filter: &ListFilter) -> LibResult<Vec<TaskRecord>>;

    /// Returns a failed task to the queue for another attempt, resetting its counts.
    ///
    /// # Errors
    ///
    /// Returns [`CoreErr::TaskNotFound`](crate::error::CoreErr::TaskNotFound) when no task with
    /// `id` is held, and [`CoreErr::UnexpectedState`](crate::error::CoreErr::UnexpectedState) when
    /// it is held but has not failed.
    async fn requeue(&self, id: TaskId) -> LibResult<()>;
}

/// Lets one broker back an engine while the caller keeps a handle on it.
#[async_trait]
impl<B: TaskBroker + ?Sized> TaskBroker for std::sync::Arc<B> {
    async fn enqueue(&self, spec: TaskSpec) -> LibResult<TaskId> {
        (**self).enqueue(spec).await
    }

    async fn claim(&self, filter: &ClaimFilter) -> LibResult<Lease> {
        (**self).claim(filter).await
    }

    async fn heartbeat(&self, lease: &Lease) -> LibResult<SystemTime> {
        (**self).heartbeat(lease).await
    }

    async fn ack(&self, lease: &Lease, completion: Completion) -> LibResult<()> {
        (**self).ack(lease, completion).await
    }

    async fn retry(&self, lease: &Lease, available_at: SystemTime) -> LibResult<()> {
        (**self).retry(lease, available_at).await
    }

    async fn release(&self, lease: &Lease) -> LibResult<()> {
        (**self).release(lease).await
    }

    async fn fail(&self, lease: &Lease, failure: FailureRecord) -> LibResult<()> {
        (**self).fail(lease, failure).await
    }
}

#[async_trait]
impl<S: TaskStore + ?Sized> TaskStore for std::sync::Arc<S> {
    async fn get(&self, id: TaskId) -> LibResult<Option<TaskRecord>> {
        (**self).get(id).await
    }

    async fn list(&self, filter: &ListFilter) -> LibResult<Vec<TaskRecord>> {
        (**self).list(filter).await
    }

    async fn requeue(&self, id: TaskId) -> LibResult<()> {
        (**self).requeue(id).await
    }
}
