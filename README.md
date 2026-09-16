# riverbed

An embeddable, distributed task queue for Rust, inspired by [Asynq](https://github.com/hibiken/asynq).

Riverbed runs inside your application. It can keep tasks in memory with no external services, or use a database for durable storage shared across many workers. The long-term goal is to be the fastest and most robust distributed task queue available in Rust. Every performance and reliability claim will be backed by published, reproducible benchmarks and fault-injection tests.

## Status

Early development. The API is unstable and not yet ready for production use. Riverbed is not yet published on crates.io.

## Features

- **Typed tasks**: handlers receive strongly typed, deserialized payloads.
- **Pluggable brokers**: storage sits behind the `TaskBroker` trait, and an in-memory broker ships by default.
- **Delayed execution**: tasks become claimable at a scheduled time.
- **Retries**: failed tasks are retried with exponential backoff, up to a configurable limit.
- **Bounded concurrency**: a configurable limit on how many tasks run at once.
- **Panic isolation**: a panicking handler fails its task, and the worker keeps running.
- **Structured logging**: [`tracing`](https://docs.rs/tracing) spans carry `task_id`, `task_type`, and `attempt` into your handlers' logs.

## Quick start

Requires Rust 1.85+ (edition 2024) and a [Tokio](https://tokio.rs) runtime.

```rust
use riverbed::{engine::Engine, execution::ExecutionContext, handler::HandlerError, task::TTask};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Serialize, Deserialize)]
struct SendEmail {
    to: String,
}

impl TTask for SendEmail {
    const NAME: &'static str = "send_email";
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut engine = Engine::builder()
        .concurrency(8)
        .register_task(|task: SendEmail, ctx: ExecutionContext| async move {
            println!("attempt {}: emailing {}", ctx.attempt, task.to);
            Ok::<(), HandlerError>(())
        })?
        .build();

    engine.start();
    engine
        .enqueue_task(SendEmail { to: "user@example.com".into() })
        .await?;

    // Keep the process alive long enough for the task to run.
    tokio::time::sleep(Duration::from_secs(1)).await;
    Ok(())
}
```

`TTask::NAME` identifies the task type in storage. Keep it stable across releases, because tasks enqueued by one version of your application may be processed by the next.

## Configuration

```rust
use riverbed::retry::RetryPolicy;
use std::time::Duration;

let engine = Engine::builder()
    .concurrency(16)
    .retry_policy(RetryPolicy {
        max_attempts: 10,
        base_delay: Duration::from_millis(500),
        max_delay: Duration::from_secs(60),
    })
    // .broker(MyPostgresBroker::new(pool))
    .build();
```

| Option | Default | Description |
|---|---|---|
| `concurrency` | Number of CPUs | Maximum number of tasks executing at once |
| `retry_policy` | 5 attempts, 1s base delay, 5m cap | Retry limit and exponential backoff |
| `broker` | `MemoryBroker` | Storage backend |

## Delivery semantics

Riverbed delivers **at least once**. A task may run more than once, for example after a retry. Write handlers to be idempotent.

A task's outcome depends on how its handler ends:

| Handler outcome | Result |
|---|---|
| Returns `Ok(())` | Task is acknowledged and removed |
| Returns `Err(_)` or panics | Task is retried with backoff until `max_attempts` is reached, then permanently failed |
| Payload fails to deserialize | Task is permanently failed without retrying, because retrying cannot fix it |
| No handler registered for the task type | Task is retried, so that a worker running a newer version can pick it up during a rolling deploy |

The in-memory broker does not persist tasks. Pending tasks are lost when the process exits.

## Custom brokers

Implement `TaskBroker` to store tasks anywhere:

```rust
#[async_trait]
pub trait TaskBroker: Send + Sync {
    async fn enqueue(&self, task: Task) -> LibResult<()>;
    async fn claim(&self) -> LibResult<Task>;
    async fn ack(&self, task_id: TaskId) -> LibResult<()>;
    async fn retry(&self, task_id: TaskId, available_at: SystemTime) -> LibResult<()>;
    async fn fail(&self, task_id: TaskId) -> LibResult<()>;
}
```

`claim` waits until a task is due and leases it to the caller. It must be cancel-safe: dropping the future must never lose a task.

## Observability

Riverbed emits logs through `tracing` and never installs a subscriber itself. Install one in your application to see the output:

```rust
tracing_subscriber::fmt()
    .with_env_filter("riverbed=info")
    .init();
```

Each task runs inside a `task` span, so logs from your handlers automatically include `task_id`, `task_type`, and `attempt`.

## Feature flags

| Flag | Default | Description |
|---|---|---|
| `in-memory` | Yes | Enables `MemoryBroker` and uses it when no broker is configured |

## How it works

```
enqueue_task ──► TaskBroker ──claim──► Executor ──spawn──► handler
                     ▲                                        │
                     └─────────── ack / retry / fail ◄────────┘
```

The executor reserves a concurrency slot *before* claiming, so a leased task never waits for a free worker. Each task runs on its own Tokio task. The outcome is reported back to the broker, which owns all task state. That separation is what lets the same executor run on top of memory, PostgreSQL, or SQLite.

## Roadmap

### Core

- [x] Typed task registration
- [x] In-memory broker with delayed tasks
- [x] Bounded concurrency
- [x] Retries with exponential backoff
- [x] Panic isolation
- [x] `tracing` instrumentation
- [ ] Graceful shutdown that drains in-flight tasks
- [ ] `enqueue_in` / `enqueue_at` for scheduling tasks from the public API
- [ ] Per-task options: max attempts, timeout, queue
- [ ] Backoff jitter to avoid synchronized retry storms

### Durable, distributed backends

- [ ] PostgreSQL broker using `FOR UPDATE SKIP LOCKED` claims and `LISTEN/NOTIFY` wakeups
- [ ] SQLite broker for single-node durable deployments
- [ ] Lease expiry with heartbeats, so tasks held by crashed workers are recovered
- [ ] Dead-letter queue with inspection and requeue

### Robustness

- [ ] Fault-injection test suite covering worker crashes mid-task, broker outages, and network partitions
- [ ] Deterministic simulation testing of the executor and brokers
- [ ] Concurrency model checking with [`loom`](https://github.com/tokio-rs/loom)
- [ ] Unique tasks and deduplication via idempotency keys
- [ ] Task timeouts and cancellation
- [ ] Per-task-type rate limiting

### Performance

- [ ] Benchmark suite with published, reproducible results against other Rust and non-Rust task queues
- [ ] Batch claiming to amortize broker round trips
- [ ] Pluggable payload encoding (e.g. MessagePack, postcard) alongside JSON
- [ ] Sharded in-memory broker to reduce lock contention at high throughput
- [ ] Allocation profiling of the hot path

### Features

- [ ] Priority and weighted queues
- [ ] Periodic (cron) tasks
- [ ] Task result storage

### Operations

- [ ] Metrics via OpenTelemetry: queue depth, latency, throughput, failure rate
- [ ] CLI and web dashboard for inspecting and managing queues
