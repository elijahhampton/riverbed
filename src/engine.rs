//! Engine configuration, task registration, and enqueueing.

use crate::broker::TaskBroker;
use crate::error::{CoreErr, LibResult};
use crate::execution::{ExecutionContext, Executor};
use crate::handler::{HandlerError, THandler, TypedHandler};
use crate::retry::RetryPolicy;
use crate::task::{Task, TaskId};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use tracing::{debug, warn};

/// Configures and builds an [`Engine`].
pub struct EngineBuilder {
    handlers: HashMap<String, Box<dyn THandler>>,
    broker: Option<Arc<dyn TaskBroker>>,
    retry_policy: RetryPolicy,
    concurrency: usize,
}

impl Default for EngineBuilder {
    fn default() -> Self {
        Self {
            handlers: HashMap::new(),
            broker: None,
            retry_policy: RetryPolicy::default(),
            concurrency: std::thread::available_parallelism().map_or(1, NonZeroUsize::get),
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

    /// Sets the maximum number of tasks executed concurrently. Defaults to the
    /// number of available CPUs.
    pub fn concurrency(mut self, concurrency: usize) -> Self {
        assert!(concurrency > 0, "concurrency must be at least 1");
        self.concurrency = concurrency;
        self
    }

    /// Sets the policy for retrying failed tasks. Defaults to [`RetryPolicy::default`].
    pub fn retry_policy(mut self, retry_policy: RetryPolicy) -> Self {
        self.retry_policy = retry_policy;
        self
    }

    /// Registers `handler` to execute tasks of type `definition`.
    ///
    /// Each task's JSON payload is deserialized into `T` before the handler is called. A payload
    /// that cannot be deserialized fails the task permanently, without retries.
    ///
    /// # Errors
    ///
    /// Returns [`CoreErr::DuplicateHandler`] if a handler is already registered for `definition`.
    pub fn register_task<T, F, Fut>(mut self, definition: String, handler: F) -> LibResult<Self>
    where
        T: Serialize + DeserializeOwned + 'static,
        F: Fn(T, ExecutionContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), HandlerError>> + Send + 'static,
    {
        if self.handlers.contains_key(&definition) {
            return Err(CoreErr::DuplicateHandler(definition.clone()));
        }
        self.handlers.insert(
            definition.clone(),
            Box::new(TypedHandler::<T, F>::new(handler)),
        );
        debug!(task_type = definition, "registered task handler");
        Ok(self)
    }

    /// Panics if no broker was set and the `in-memory` feature is disabled.
    pub fn build(self) -> Engine {
        debug!(
            handlers = self.handlers.len(),
            concurrency = self.concurrency,
            "building engine"
        );
        let broker = self.broker.unwrap_or_else(default_broker);
        let executor = Executor::new(
            Arc::clone(&broker),
            self.handlers,
            self.retry_policy,
            self.concurrency,
        );

        Engine {
            broker,
            executor: Mutex::new(Some(executor)),
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
    /// The executor is moved into its own task by once start() is called and becomes independent
    /// of the Engine. The value here becomes Option::None once the engine is running.
    executor: Mutex<Option<Executor>>,
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
        let Some(executor) = self.executor.lock().unwrap().take() else {
            warn!("engine is already running; ignoring repeated call to start()");
            return;
        };

        tokio::spawn(Arc::new(executor).run());
    }

    /// Enqueues a task of type `definition` and returns its ID.
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
        let task = Task::new(definition, payload)?;
        let task_id = task.id;
        let task_type = task.definition.clone();

        self.broker.enqueue(task).await?;
        debug!(%task_id, task_type, "task enqueued");

        Ok(task_id)
    }
}
