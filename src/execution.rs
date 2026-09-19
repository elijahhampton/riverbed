use std::{
    collections::HashMap,
    error::Error,
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};

use tokio::sync::Semaphore;
use tracing::{Instrument, debug, error, info, instrument, warn};

use crate::{
    broker::TaskBroker,
    error::LibResult,
    handler::THandler,
    retry::RetryPolicy,
    task::{Task, TaskId},
};

/// Pause after a failed claim so an unavailable broker to 
/// prevent polling in a hot loop.
const CLAIM_ERROR_BACKOFF: Duration = Duration::from_secs(1);

#[derive(Debug, Clone)]
pub struct ExecutionContext {
    pub task_id: TaskId,
    pub attempt: u32,
}

enum Failure {
    /// A permanent failure retrying cannot recover. 
    /// Ex. the payload does not decode.
    Permanent,
    /// A transient failure a retry may recover on later attempts.
    Transient,
}

pub(crate) struct Executor {
    /// The underlying broker the executor leases task from. The only
    /// guarantee is that the executor will attempt to execute the task
    /// at-least once in the case of a non-memory based broker.
    broker: Arc<dyn TaskBroker>,
    /// A mapping of task definitions to execution handlers.
    handlers: HashMap<String, Box<dyn THandler>>,
    retry_policy: RetryPolicy,
    concurrency: usize,
}

impl Executor {
    pub(crate) fn new(
        broker: Arc<dyn TaskBroker>,
        handlers: HashMap<String, Box<dyn THandler>>,
        retry_policy: RetryPolicy,
        concurrency: usize,
    ) -> Self {
        Self {
            broker,
            handlers,
            retry_policy,
            concurrency,
        }
    }

    /// Claims and executes tasks forever with at most `concurrency` being executed.
    pub(crate) async fn run(self: Arc<Self>) {
        if self.handlers.is_empty() {
            warn!("engine started with no task handlers registered; every task it picks up will fail");
        }
        info!(
            concurrency = self.concurrency,
            task_types = ?self.handlers.keys().collect::<Vec<_>>(),
            "engine started"
        );
        let permits = Arc::new(Semaphore::new(self.concurrency));

        loop {
            // Reserve capacity before claiming so a task never waits for a worker
            let permit = Arc::clone(&permits)
                .acquire_owned()
                .await
                .expect("semaphore is never closed");

            let task = match self.broker.claim().await {
                Ok(task) => task,
                Err(err) => {
                    error!(
                        error = &err as &dyn Error,
                        retry_in = ?CLAIM_ERROR_BACKOFF,
                        "could not fetch the next task from the broker"
                    );
                    tokio::time::sleep(CLAIM_ERROR_BACKOFF).await;
                    continue;
                }
            };

            let executor = Arc::clone(&self);
            tokio::spawn(async move {
                executor.process(task).await;
                // Naming the permit moves it into this task, holding the slot until completion.
                drop(permit);
            });
        }
    }

    #[instrument(
        name = "task",
        skip_all,
        fields(task_id = %task.id, task_type = %task.definition, attempt = task.attempts + 1)
    )]
    async fn process(&self, task: Task) {
        debug!("task started");
        let started_at = Instant::now();

        let is_execution_recorded = match self.execute(&task).await {
            Ok(()) => {
                debug!(elapsed = ?started_at.elapsed(), "task completed");
                self.broker.ack(task.id).await
            }
            Err(Failure::Permanent) => self.broker.fail(task.id).await,
            Err(Failure::Transient) => self.retry_or_fail(&task).await,
        };

        if let Err(err) = is_execution_recorded {
            error!(
                error = &err as &dyn Error,
                "could not update the task's status in the broker"
            );
        }
    }

    async fn execute(&self, task: &Task) -> Result<(), Failure> {
        let Some(handler) = self.handlers.get(task.definition.as_str()) else {
            // Failure to get a reference to a handler is a transient error and can be retried again.
            // Ex. In a mixed-version deployment another worker may have this handler.
            warn!("no handler registered for this task type");
            return Err(Failure::Transient);
        };

        let ctx = ExecutionContext {
            task_id: task.id,
            attempt: task.attempts + 1,
        };
        let fut = handler.call(ctx, task.payload.clone()).map_err(|err| {
            error!(
                error = &err as &dyn Error,
                "task payload does not match the handler's expected type; failing without retry"
            );
            Failure::Permanent
        })?;

        // A separate task turns a handler panic into a `JoinError` instead of unwinding past the ack.
        match tokio::spawn(fut.in_current_span()).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(err)) => {
                warn!(error = &*err as &dyn Error, "task handler returned an error");
                Err(Failure::Transient)
            }
            Err(err) => {
                error!(error = &err as &dyn Error, "task handler panicked");
                Err(Failure::Transient)
            }
        }
    }

    async fn retry_or_fail(&self, task: &Task) -> LibResult<()> {
        let attempt = task.attempts + 1;
        if attempt >= self.retry_policy.max_attempts {
            error!(
                max_attempts = self.retry_policy.max_attempts,
                "task failed permanently; retries exhausted"
            );
            return self.broker.fail(task.id).await;
        }

        let delay = self.retry_policy.backoff(attempt);
        info!(retry_in = ?delay, "task will be retried");
        self.broker.retry(task.id, SystemTime::now() + delay).await
    }
}
