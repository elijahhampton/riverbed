//! Task execution.

use std::{
    collections::HashMap,
    error::Error,
    sync::{Arc, Mutex, MutexGuard},
    time::{Duration, Instant, SystemTime},
};

use tokio::{sync::Semaphore, task::JoinError};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, debug, error, info, instrument, warn};

use crate::{
    broker::{ClaimFilter, TaskBroker},
    completion::{Completion, FailureKind, FailureRecord},
    error::{CoreErr, LibResult},
    handler::{HandlerError, HandlerFut, THandler},
    lease::Lease,
    retry::RetryPolicy,
    task::{QueueName, Task, TaskId},
};

/// Pause after a failed claim so an unavailable broker to
/// prevent polling in a hot loop.
const CLAIM_ERROR_BACKOFF: Duration = Duration::from_secs(1);

/// Floor on the heartbeat interval, so a very short lease cannot spin the broker.
const MIN_HEARTBEAT_INTERVAL: Duration = Duration::from_millis(50);

/// Information about the current execution of a task, passed to its handler.
#[derive(Debug, Clone)]
pub struct ExecutionContext {
    /// ID of the task being executed.
    pub task_id: TaskId,
    /// Attempt number of this execution, starting at 1.
    pub attempt: u32,
    /// Fires when the engine is shutting down or this execution has run past its timeout.
    ///
    /// Observing it is cooperative: a handler that ignores it keeps running until the engine's
    /// drain deadline, and is then dropped wherever it happens to be. A handler that observes it
    /// and returns early has its task released, with no attempt consumed.
    pub cancel: CancellationToken,
    /// When this execution runs out of time, if it has a timeout.
    pub deadline: Option<Instant>,
}

impl ExecutionContext {
    /// Whether this execution has been asked to stop.
    pub fn is_cancelled(&self) -> bool {
        self.cancel.is_cancelled()
    }

    /// Resolves when this execution is asked to stop.
    pub async fn cancelled(&self) {
        self.cancel.cancelled().await;
    }
}

/// How one execution ended.
enum Outcome {
    /// The handler reported success.
    Completed(Completion),
    /// The handler stopped without reaching a verdict. Released, not counted as an attempt.
    Cancelled,
    /// The execution ran past its deadline. Counted as an attempt.
    TimedOut,
    /// Retrying cannot recover. Ex. the payload does not decode.
    Permanent(FailureKind, String),
    /// A retry may recover on later attempts.
    Transient(FailureKind, String),
}

/// A pool of workers serving one queue.
struct Pool {
    queue: QueueName,
    permits: Arc<Semaphore>,
    concurrency: usize,
}

/// A running attempt, so a drain can stop it and hand its lease back.
struct InFlight {
    lease: Arc<Lease>,
    cancel: CancellationToken,
}

pub(crate) struct Executor {
    /// The underlying broker the executor leases task from. The only
    /// guarantee is that the executor will attempt to execute the task
    /// at-least once in the case of a non-memory based broker.
    broker: Arc<dyn TaskBroker>,
    /// A mapping of task definitions to execution handlers.
    handlers: HashMap<String, Box<dyn THandler>>,
    /// One pool per queue that has handlers, each claiming independently.
    pools: Vec<Pool>,
    retry_policy: RetryPolicy,
    /// Default time one execution may run, when the task does not set its own.
    task_timeout: Option<Duration>,
    /// Cancelled on shutdown. Each attempt's token is a child of this one, so cancelling here
    /// reaches every running handler.
    shutdown: CancellationToken,
    in_flight: Mutex<HashMap<TaskId, InFlight>>,
}

impl Executor {
    pub(crate) fn new(
        broker: Arc<dyn TaskBroker>,
        handlers: HashMap<String, Box<dyn THandler>>,
        queues: HashMap<QueueName, usize>,
        retry_policy: RetryPolicy,
        task_timeout: Option<Duration>,
        shutdown: CancellationToken,
    ) -> Self {
        let pools = queues
            .into_iter()
            .map(|(queue, concurrency)| Pool {
                queue,
                permits: Arc::new(Semaphore::new(concurrency)),
                concurrency,
            })
            .collect();

        Self {
            broker,
            handlers,
            pools,
            retry_policy,
            task_timeout,
            shutdown,
            in_flight: Mutex::new(HashMap::new()),
        }
    }

    fn in_flight(&self) -> MutexGuard<'_, HashMap<TaskId, InFlight>> {
        self.in_flight.lock().expect("in-flight lock poisoned")
    }

    /// Claims and executes tasks until the shutdown token is cancelled.
    ///
    /// Returns once every pool stops claiming. In-flight handlers keep running; waiting for them
    /// is [`Executor::drain`].
    pub(crate) async fn run(self: Arc<Self>) {
        if self.handlers.is_empty() {
            warn!(
                "engine started with no task handlers registered; every task it picks up will fail"
            );
        }
        info!(
            queues = ?self.pools.iter().map(|p| (&p.queue, p.concurrency)).collect::<Vec<_>>(),
            task_types = ?self.handlers.keys().collect::<Vec<_>>(),
            "engine started"
        );

        // One loop per queue. A single loop with a merged filter could not reserve the right
        // pool's permit, because which pool serves a task is only known after it is claimed.
        let loops: Vec<_> = (0..self.pools.len())
            .map(|index| {
                let executor = Arc::clone(&self);
                tokio::spawn(async move { executor.claim_loop(index).await })
            })
            .collect();

        for claim_loop in loops {
            if let Err(err) = claim_loop.await {
                error!(
                    error = &err as &dyn Error,
                    "a claim loop stopped unexpectedly"
                );
            }
        }
        info!("engine stopped claiming tasks");
    }

    async fn claim_loop(self: Arc<Self>, index: usize) {
        let pool = &self.pools[index];
        let filter = ClaimFilter::queues([pool.queue.clone()]);

        loop {
            // Reserve capacity before claiming so a task never waits for a worker
            let permit = tokio::select! {
                biased;
                _ = self.shutdown.cancelled() => break,
                permit = Arc::clone(&pool.permits).acquire_owned() => {
                    permit.expect("semaphore is never closed")
                }
            };

            // Claiming parks until a task is due, so the signal has to interrupt the claim itself.
            // Checking it between iterations would wait for a task that may never arrive.
            let claimed = tokio::select! {
                biased;
                _ = self.shutdown.cancelled() => {
                    // Released before breaking, or the drain waits on a permit held by the loop
                    // that is trying to finish.
                    drop(permit);
                    break;
                }
                claimed = self.broker.claim(&filter) => claimed,
            };

            let lease = match claimed {
                Ok(lease) => lease,
                Err(err) => {
                    error!(
                        error = &err as &dyn Error,
                        queue = %pool.queue,
                        retry_in = ?CLAIM_ERROR_BACKOFF,
                        "could not fetch the next task from the broker"
                    );
                    drop(permit);
                    tokio::time::sleep(CLAIM_ERROR_BACKOFF).await;
                    continue;
                }
            };

            let executor = Arc::clone(&self);
            tokio::spawn(async move {
                executor.process(lease).await;
                // Naming the permit moves it into this task, holding the slot until completion.
                drop(permit);
            });
        }
    }

    /// Waits for in-flight handlers to finish, for at most `timeout`.
    ///
    /// Returns whether everything finished in time. Anything still running at the deadline is
    /// cancelled and its lease released, so its task becomes claimable immediately instead of
    /// waiting out its expiry.
    pub(crate) async fn drain(&self, timeout: Duration) -> bool {
        let wait = async {
            for pool in &self.pools {
                let all = u32::try_from(pool.concurrency).expect("concurrency fits in a u32");
                // Released again rather than forgotten: a drain that kept them would make every
                // later drain wait for permits that can no longer exist.
                drop(
                    pool.permits
                        .acquire_many(all)
                        .await
                        .expect("semaphore is never closed"),
                );
            }
        };

        match tokio::time::timeout(timeout, wait).await {
            Ok(()) => {
                debug!("all in-flight tasks finished");
                true
            }
            Err(_) => {
                self.release_in_flight().await;
                false
            }
        }
    }

    async fn release_in_flight(&self) {
        let held: Vec<(Arc<Lease>, CancellationToken)> = self
            .in_flight()
            .values()
            .map(|in_flight| (Arc::clone(&in_flight.lease), in_flight.cancel.clone()))
            .collect();
        warn!(
            held = held.len(),
            "drain timed out; cancelling handlers and returning their leases"
        );

        for (lease, cancel) in held {
            cancel.cancel();
            // Releasing ends the lease the handler still holds, so whatever outcome it eventually
            // reports is rejected rather than applied over another worker's.
            if let Err(err) = self.broker.release(&lease).await {
                debug!(
                    task_id = %lease.task_id(),
                    error = &err as &dyn Error,
                    "could not release a lease while draining"
                );
            }
        }
    }

    #[instrument(
        name = "task",
        skip_all,
        fields(
            task_id = %lease.task.id,
            task_type = %lease.task.definition,
            queue = %lease.task.queue,
            attempt = lease.task.attempt(),
            reclaims = lease.task.reclaims,
        )
    )]
    async fn process(&self, lease: Lease) {
        // A child of the shutdown token, so shutting the engine down reaches this handler while
        // its own timeout can fire without touching anything else.
        let cancel = self.shutdown.child_token();
        let lease = Arc::new(lease);

        self.in_flight().insert(
            lease.task_id(),
            InFlight {
                lease: Arc::clone(&lease),
                cancel: cancel.clone(),
            },
        );
        self.attempt(&lease, cancel).await;
        self.in_flight().remove(&lease.task_id());
    }

    async fn attempt(&self, lease: &Lease, cancel: CancellationToken) {
        debug!("task started");
        let started_at = Instant::now();

        let timeout = lease.task.timeout.or(self.task_timeout);
        // Measured from here rather than from the claim, so a task that waited behind a slow queue
        // still gets its whole execution window.
        let deadline = timeout.map(|timeout| started_at + timeout);

        let handler = match self.prepare(&lease.task, &cancel, deadline) {
            Ok(handler) => handler,
            Err(outcome) => return self.record(lease, outcome, started_at).await,
        };

        // A separate task turns a handler panic into a `JoinError` instead of unwinding past the ack.
        let mut handle = tokio::spawn(handler.in_current_span());
        // Taken before the select so cancelling the handler does not need to borrow `handle`,
        // which the select holds for the duration of the race.
        let abort = handle.abort_handle();

        let joined = tokio::select! {
            // A handler that finished has an outcome worth recording even if something else
            // happened in the same instant, so its branch is polled first.
            biased;
            joined = &mut handle => joined,
            () = expire(deadline) => {
                cancel.cancel();
                // Cooperative: wait for the handler to notice, up to the drain deadline. Aborting
                // here would leave whatever it had already done half applied.
                match (&mut handle).await {
                    Ok(_) | Err(_) => return self.record(lease, Outcome::TimedOut, started_at).await,
                }
            }
            err = self.keep_alive(lease) => {
                // The task is already running elsewhere. Every outcome this executor could record
                // would be rejected, and letting the handler run on would duplicate its effects.
                abort.abort();
                error!(
                    error = &err as &dyn Error,
                    elapsed = ?started_at.elapsed(),
                    "lost the lease while the handler was running; abandoning this attempt"
                );
                return;
            }
        };

        let outcome = Self::classify(joined, &cancel);
        self.record(lease, outcome, started_at).await;
    }

    /// Resolves the handler for `task` and decodes its payload.
    ///
    /// Runs before anything is spawned, so a task that can never execute does not start a
    /// heartbeat.
    fn prepare(
        &self,
        task: &Task,
        cancel: &CancellationToken,
        deadline: Option<Instant>,
    ) -> Result<HandlerFut, Outcome> {
        let Some(handler) = self.handlers.get(task.definition.as_str()) else {
            // Failure to get a reference to a handler is a transient error and can be retried again.
            // Ex. In a mixed-version deployment another worker may have this handler.
            warn!("no handler registered for this task type");
            return Err(Outcome::Transient(
                FailureKind::HandlerError,
                format!("no handler registered for `{}`", task.definition),
            ));
        };

        let ctx = ExecutionContext {
            task_id: task.id,
            attempt: task.attempt(),
            cancel: cancel.clone(),
            deadline,
        };
        handler.call(ctx, task.payload.clone()).map_err(|err| {
            error!(
                error = &err as &dyn Error,
                "task payload does not match the handler's expected type; failing without retry"
            );
            Outcome::Permanent(FailureKind::PayloadDecode, err.to_string())
        })
    }

    fn classify(
        joined: Result<Result<Completion, HandlerError>, JoinError>,
        cancel: &CancellationToken,
    ) -> Outcome {
        match joined {
            // A handler that returned success claims the work is done, so it is acknowledged even
            // if cancellation fired in the same instant. Re-running completed work is worse.
            Ok(Ok(completion)) => Outcome::Completed(completion),
            Ok(Err(err)) if cancel.is_cancelled() => {
                debug!(
                    error = &*err as &dyn Error,
                    "task handler stopped after cancellation"
                );
                Outcome::Cancelled
            }
            Ok(Err(err)) => {
                warn!(
                    error = &*err as &dyn Error,
                    "task handler returned an error"
                );
                Outcome::Transient(FailureKind::HandlerError, err.to_string())
            }
            Err(err) if err.is_cancelled() && cancel.is_cancelled() => Outcome::Cancelled,
            Err(err) => {
                error!(error = &err as &dyn Error, "task handler panicked");
                Outcome::Transient(FailureKind::HandlerPanic, err.to_string())
            }
        }
    }

    /// Extends the lease for as long as the handler runs. Only returns when the lease is lost.
    async fn keep_alive(&self, lease: &Lease) -> CoreErr {
        let mut expires_at = lease.expires_at;
        loop {
            tokio::time::sleep(heartbeat_interval(expires_at)).await;
            match self.broker.heartbeat(lease).await {
                Ok(extended) => expires_at = extended,
                Err(err) => return err,
            }
        }
    }

    async fn record(&self, lease: &Lease, outcome: Outcome, started_at: Instant) {
        let is_execution_recorded = match outcome {
            Outcome::Completed(completion) => {
                debug!(elapsed = ?started_at.elapsed(), "task completed");
                self.broker.ack(lease, completion).await
            }
            Outcome::Cancelled => {
                info!(elapsed = ?started_at.elapsed(), "task cancelled; returning it to the queue");
                self.broker.release(lease).await
            }
            Outcome::TimedOut => {
                warn!(elapsed = ?started_at.elapsed(), "task exceeded its timeout");
                self.retry_or_fail(lease, FailureKind::Timeout, "execution timed out")
                    .await
            }
            Outcome::Permanent(kind, detail) => {
                self.broker
                    .fail(
                        lease,
                        FailureRecord::new(kind, detail).attempts(lease.task.attempt()),
                    )
                    .await
            }
            Outcome::Transient(kind, detail) => self.retry_or_fail(lease, kind, detail).await,
        };

        if let Err(err) = is_execution_recorded {
            error!(
                error = &err as &dyn Error,
                "could not update the task's status in the broker"
            );
        }
    }

    async fn retry_or_fail(
        &self,
        lease: &Lease,
        kind: FailureKind,
        detail: impl Into<String>,
    ) -> LibResult<()> {
        let attempt = lease.task.attempt();
        let max_attempts = lease
            .task
            .max_attempts
            .unwrap_or(self.retry_policy.max_attempts);

        if attempt >= max_attempts {
            error!(
                max_attempts,
                failure = kind.as_str(),
                "task failed permanently; retries exhausted"
            );
            return self
                .broker
                .fail(lease, FailureRecord::new(kind, detail).attempts(attempt))
                .await;
        }

        let delay = self.retry_policy.backoff(attempt);
        info!(retry_in = ?delay, "task will be retried");
        self.broker.retry(lease, SystemTime::now() + delay).await
    }
}

/// A third of the remaining lease, so two consecutive missed beats are survivable.
fn heartbeat_interval(expires_at: SystemTime) -> Duration {
    let remaining = expires_at
        .duration_since(SystemTime::now())
        .unwrap_or_default();
    (remaining / 3).max(MIN_HEARTBEAT_INTERVAL)
}

/// Resolves at `deadline`, or never when there is none.
async fn expire(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline.into()).await,
        None => std::future::pending().await,
    }
}
