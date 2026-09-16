use crate::broker::TaskBroker;
use crate::error::{CoreErr, LibResult};
use crate::execution::{ExecutionContext, Executor};
use crate::handler::{HandlerError, THandler, TypedHandler};
use crate::retry::RetryPolicy;
use crate::task::{TConfig, TTask, Task, TaskId};
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use tracing::{debug, info, warn};

pub struct EngineBuilder {
    handlers: HashMap<&'static str, Box<dyn THandler>>,
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

    pub fn retry_policy(mut self, retry_policy: RetryPolicy) -> Self {
        self.retry_policy = retry_policy;
        self
    }

    pub fn register_task<T, F, Fut>(mut self, handler: F) -> LibResult<Self>
    where
        T: TTask,
        F: Fn(T, ExecutionContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), HandlerError>> + Send + 'static,
    {
        if self.handlers.contains_key(T::NAME) {
            return Err(CoreErr::DuplicateHandler(T::NAME));
        }
        self.handlers
            .insert(T::NAME, Box::new(TypedHandler::<T, F>::new(handler)));
        debug!(task_type = T::NAME, "registered task handler");
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
            executor: Some(executor),
        }
    }
}

#[cfg(feature = "in-memory")]
fn default_broker() -> Arc<dyn TaskBroker> {
    Arc::new(crate::broker::MemoryBroker::new())
}

#[cfg(not(feature = "in-memory"))]
fn default_broker() -> Arc<dyn TaskBroker> {
    panic!("no broker configured: call `EngineBuilder::broker` or enable the `in-memory` feature")
}

pub struct Engine {
    broker: Arc<dyn TaskBroker>,
    /// The executor is moved into its own task by once start() is called and becomes independent
    /// of the Engine. The value here becomes Option::None once the engine is running.
    executor: Option<Executor>,
}

impl Engine {
    pub fn builder() -> EngineBuilder {
        EngineBuilder::default()
    }

    pub fn start(&mut self) {
        let Some(executor) = self.executor.take() else {
            warn!("engine already started");
            return;
        };

        info!("starting engine");
        tokio::spawn(Arc::new(executor).run());
    }

    pub async fn enqueue_task<T: TTask>(&self, payload: T, config: Box<dyn TConfig>) -> LibResult<TaskId> {
        let task = Task::new(&payload, config)?;
        let task_id = task.id;

        self.broker.enqueue(task).await?;
        debug!(%task_id, task_type = T::NAME, "task enqueued");

        Ok(task_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize)]
    struct AddNum {
        num: u32,
    }

    impl TTask for AddNum {
        const NAME: &'static str = "add_num";
    }

    fn init_tracing() {
        let filter = tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("riverbed=debug"));

        let _ = tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_test_writer()
            .try_init();
    }

    #[tokio::test]
    async fn registers_task() {
        init_tracing();

        let _engine = Engine::builder()
            .register_task(|task: AddNum, ctx: ExecutionContext| async move {
                println!("task {} attempt {}: num={}", ctx.task_id, ctx.attempt, task.num);
                Ok::<(), HandlerError>(())
            })
            .unwrap()
            .build();
    }
}
