//! Engine configuration, task registration, and enqueueing.

use crate::broker::TaskBroker;
use crate::completion::Completion;
use crate::error::{CoreErr, LibResult};
use crate::execution::{ExecutionContext, Executor};
use crate::handler::{HandlerError, THandler, TypedHandler};
use crate::retry::RetryPolicy;
use crate::task::{DEFAULT_QUEUE, QueueName, TaskId, TaskSpec};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, warn};

/// Configures and builds an [`Engine`].
pub struct EngineBuilder {
    handlers: HashMap<String, Box<dyn THandler>>,
    /// Queue each definition is claimed from, and the concurrency of each queue.
    definition_queues: HashMap<String, QueueName>,
    queue_concurrency: HashMap<QueueName, usize>,
    broker: Option<Arc<dyn TaskBroker>>,
    retry_policy: RetryPolicy,
    concurrency: usize,
    task_timeout: Option<Duration>,
}

impl Default for EngineBuilder {
    fn default() -> Self {
        Self {
            handlers: HashMap::new(),
            definition_queues: HashMap::new(),
            queue_concurrency: HashMap::new(),
            broker: None,
            retry_policy: RetryPolicy::default(),
            concurrency: std::thread::available_parallelism().map_or(1, NonZeroUsize::get),
            task_timeout: None,
        }
    }
}

impl EngineBuilder {
    /// Creates a builder with the default configuration.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the task storage backend. Defaults to an in-memory broker when the
    /// `in-memory` feature is enabled.
    pub fn broker<B: TaskBroker + 'static>(mut self, broker: B) -> Self {
        self.broker = Some(Arc::new(broker));
        self
    }

    /// Sets the default number of tasks executed concurrently per queue. Defaults to the number of
    /// available CPUs.
    pub fn concurrency(mut self, concurrency: usize) -> Self {
        assert!(concurrency > 0, "concurrency must be at least 1");
        self.concurrency = concurrency;
        self
    }

    /// Sets how many tasks from `queue` run at once, overriding [`EngineBuilder::concurrency`].
    pub fn queue_concurrency(mut self, queue: impl Into<QueueName>, concurrency: usize) -> Self {
        assert!(concurrency > 0, "concurrency must be at least 1");
        self.queue_concurrency.insert(queue.into(), concurrency);
        self
    }

    /// Sets the policy for retrying failed tasks. Defaults to [`RetryPolicy::default`].
    pub fn retry_policy(mut self, retry_policy: RetryPolicy) -> Self {
        self.retry_policy = retry_policy;
        self
    }

    /// Sets how long one execution may run before it is cancelled, for tasks that do not set their
    /// own. Defaults to no limit.
    pub fn task_timeout(mut self, task_timeout: Duration) -> Self {
        self.task_timeout = Some(task_timeout);
        self
    }

    /// Registers `handler` to execute tasks of type `definition`, on the default queue.
    ///
    /// Each task's JSON payload is deserialized into `T` before the handler is called. A payload
    /// that cannot be deserialized fails the task permanently, without retries.
    ///
    /// The handler may return `Ok(())`, or a [`Completion`] carrying a result to store and tasks
    /// to enqueue alongside the acknowledgement.
    ///
    /// # Errors
    ///
    /// Returns [`CoreErr::DuplicateHandler`] if a handler is already registered for `definition`.
    pub fn register_task<T, R, F, Fut>(self, definition: String, handler: F) -> LibResult<Self>
    where
        T: Serialize + DeserializeOwned + 'static,
        R: Into<Completion> + Send + 'static,
        F: Fn(T, ExecutionContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<R, HandlerError>> + Send + 'static,
    {
        self.register_task_on(DEFAULT_QUEUE, definition, handler)
    }

    /// Registers `handler` to execute tasks of type `definition`, claimed from `queue`.
    ///
    /// Each queue is served by its own pool with its own concurrency limit, so a slow queue cannot
    /// starve a fast one.
    ///
    /// # Errors
    ///
    /// Returns [`CoreErr::DuplicateHandler`] if a handler is already registered for `definition`.
    pub fn register_task_on<T, R, F, Fut>(
        mut self,
        queue: impl Into<QueueName>,
        definition: String,
        handler: F,
    ) -> LibResult<Self>
    where
        T: Serialize + DeserializeOwned + 'static,
        R: Into<Completion> + Send + 'static,
        F: Fn(T, ExecutionContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<R, HandlerError>> + Send + 'static,
    {
        if self.handlers.contains_key(&definition) {
            return Err(CoreErr::DuplicateHandler(definition.clone()));
        }

        let queue = queue.into();
        self.handlers.insert(
            definition.clone(),
            Box::new(TypedHandler::<T, R, F>::new(handler)),
        );
        self.definition_queues
            .insert(definition.clone(), queue.clone());
        debug!(task_type = definition, queue, "registered task handler");
        Ok(self)
    }

    /// Panics if no broker was set and the `in-memory` feature is disabled.
    pub fn build(self) -> Engine {
        // Only queues with handlers get a pool: claiming from a queue nothing serves would take
        // work this process cannot run.
        let mut queues: HashMap<QueueName, usize> = HashMap::new();
        for queue in self.definition_queues.values() {
            let concurrency = self
                .queue_concurrency
                .get(queue)
                .copied()
                .unwrap_or(self.concurrency);
            queues.insert(queue.clone(), concurrency);
        }
        if queues.is_empty() {
            queues.insert(DEFAULT_QUEUE.to_owned(), self.concurrency);
        }

        debug!(
            handlers = self.handlers.len(),
            queues = queues.len(),
            "building engine"
        );

        let broker = self.broker.unwrap_or_else(default_broker);
        let shutdown = CancellationToken::new();
        let executor = Executor::new(
            Arc::clone(&broker),
            self.handlers,
            queues,
            self.retry_policy,
            self.task_timeout,
            shutdown.clone(),
        );

        Engine {
            broker,
            executor: Arc::new(executor),
            shutdown,
            run_handle: Mutex::new(None),
        }
    }
}

#[cfg(feature = "in-memory")]
fn default_broker() -> Arc<dyn TaskBroker> {
    tracing::info!(
        "no broker configured, using the in-memory broker; queued tasks will not survive a restart"
    );
    Arc::new(crate::broker::MemoryBroker::new())
}

#[cfg(not(feature = "in-memory"))]
fn default_broker() -> Arc<dyn TaskBroker> {
    panic!("no broker configured: call `EngineBuilder::broker` or enable the `in-memory` feature")
}

/// A task queue that enqueues tasks into its broker and, once started, executes them with the
/// registered handlers.
///
/// Create one with [`Engine::builder`].
pub struct Engine {
    broker: Arc<dyn TaskBroker>,
    /// Shared with the claim loops, so [`Engine::shutdown`] can wait for in-flight work after they
    /// have stopped.
    executor: Arc<Executor>,
    shutdown: CancellationToken,
    /// The running claim loops, taken by the first [`Engine::shutdown`].
    run_handle: Mutex<Option<JoinHandle<()>>>,
}

impl Engine {
    /// Returns a builder for configuring an engine.
    pub fn builder() -> EngineBuilder {
        EngineBuilder::default()
    }

    /// Starts executing tasks in the background on the current Tokio runtime.
    ///
    /// Tasks can be enqueued before the engine starts; they wait in the broker until it does.
    /// Calls after the first log a warning and have no other effect.
    ///
    /// # Panics
    ///
    /// Panics if called outside of a Tokio runtime.
    pub fn start(&self) {
        let mut run_handle = self.run_handle.lock().expect("engine lock poisoned");
        if run_handle.is_some() {
            warn!("engine is already running; ignoring repeated call to start()");
            return;
        }

        let executor = Arc::clone(&self.executor);
        *run_handle = Some(tokio::spawn(executor.run()));
    }

    /// Stops claiming tasks and waits up to `timeout` for in-flight handlers to finish.
    ///
    /// Returns whether everything finished in time. A handler still running at the deadline is
    /// cancelled and its lease returned to the broker, so its task is claimable again immediately
    /// and whatever outcome it eventually reports is rejected.
    ///
    /// Calling this on an engine that was never started, or calling it twice, is safe.
    pub async fn shutdown(&self, timeout: Duration) -> bool {
        self.shutdown.cancel();

        let run_handle = self.run_handle.lock().expect("engine lock poisoned").take();
        if let Some(run_handle) = run_handle
            && let Err(err) = run_handle.await
        {
            error!(
                error = &err as &dyn std::error::Error,
                "the claim loops did not stop cleanly"
            );
        }

        let drained = self.executor.drain(timeout).await;
        debug!(drained, "engine shut down");
        drained
    }

    /// Enqueues a task of type `definition` on the default queue and returns its ID.
    ///
    /// The task is available for execution immediately. When it runs, `payload` is deserialized
    /// into the input type of the handler registered for `definition`.
    ///
    /// # Errors
    ///
    /// Returns an error if the broker fails to store the task.
    pub async fn enqueue_task(
        &self,
        definition: String,
        payload: serde_json::Value,
    ) -> LibResult<TaskId> {
        self.enqueue(TaskSpec::new(definition, payload)).await
    }

    /// Enqueues `spec` and returns the task's ID.
    ///
    /// When the spec carries a dedup key an unfinished task already holds, no task is created and
    /// that task's ID is returned instead.
    ///
    /// # Errors
    ///
    /// Returns an error if the broker fails to store the task.
    pub async fn enqueue(&self, spec: TaskSpec) -> LibResult<TaskId> {
        let task_type = spec.definition.clone();
        let queue = spec.queue.clone();

        let task_id = self.broker.enqueue(spec).await?;
        debug!(%task_id, task_type, queue, "task enqueued");

        Ok(task_id)
    }

    /// Enqueues a task of type `definition` to become claimable after `delay`.
    ///
    /// # Errors
    ///
    /// Returns an error if the broker fails to store the task.
    pub async fn enqueue_in(
        &self,
        definition: String,
        payload: serde_json::Value,
        delay: Duration,
    ) -> LibResult<TaskId> {
        self.enqueue(TaskSpec::new(definition, payload).available_in(delay))
            .await
    }

    /// Enqueues a task of type `definition` to become claimable at `at`.
    ///
    /// # Errors
    ///
    /// Returns an error if the broker fails to store the task.
    pub async fn enqueue_at(
        &self,
        definition: String,
        payload: serde_json::Value,
        at: std::time::SystemTime,
    ) -> LibResult<TaskId> {
        self.enqueue(TaskSpec::new(definition, payload).available_at(at))
            .await
    }
}
