# riverbed

[![CI](https://github.com/elijahhampton/riverbed/actions/workflows/ci.yml/badge.svg)](https://github.com/elijahhampton/riverbed/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)
[![MSRV](https://img.shields.io/badge/rustc-1.88+-lightgray.svg)](Cargo.toml)

An embeddable and high performance distributed task queue written in Rust.

Riverbed runs inside your application. It can keep tasks in memory with no external services, or use a database for durable storage shared across many workers. The long-term goal is to be the fastest and most robust distributed task queue available in Rust. Every performance and reliability claim will be backed by published, reproducible benchmarks and fault-injection tests.

## Status

Early development. The API is unstable and not yet ready for production use. Riverbed is not yet published on crates.io.

## Features

- **Typed tasks**: handlers receive strongly typed, deserialized payloads.
- **Queues**: each queue is served by its own pool with its own concurrency, so a slow queue cannot starve a fast one.
- **Results and follow-up work**: a handler can return a result to store and tasks to enqueue, committed with the acknowledgement.
- **Cancellation and timeouts**: handlers receive a cancellation signal, fired on shutdown and when a task runs past its timeout.
- **Deduplication**: a task carrying a dedup key an unfinished task already holds is not created twice.
- **Inspection**: look up a task's state, result, or failure reason, and requeue a failed one.
- **Pluggable brokers**: storage sits behind the `TaskBroker` trait, and an in-memory broker ships by default.
- **Delayed execution**: tasks become claimable at a scheduled time.
- **Retries**: failed tasks are retried with exponential backoff, up to a configurable limit.
- **Bounded concurrency**: a configurable limit on how many tasks run at once.
- **Panic isolation**: a panicking handler fails its task, and the worker keeps running.
- **Leases with fencing**: a task whose worker disappears is recovered, and the vanished worker cannot overwrite the outcome of the one that replaced it.
- **Graceful shutdown**: `Engine::shutdown` stops claiming and drains in-flight tasks, returning any lease still held.
- **Structured logging**: [`tracing`](https://docs.rs/tracing) spans carry `task_id`, `task_type`, and `attempt` into your handlers' logs.
- **HTTP and gRPC APIs**: optional servers that let other services enqueue tasks.

## Quick start

Requires Rust 1.88+ and a [Tokio](https://tokio.rs) runtime. Until the first release, depend on the Git repository:

```toml
[dependencies]
riverbed = { git = "https://github.com/elijahhampton/riverbed" }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
tokio = { version = "1", features = ["full"] }
```

```rust
use riverbed::{engine::Engine, execution::ExecutionContext, handler::HandlerError};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Serialize, Deserialize)]
struct SendEmail {
    to: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let engine = Engine::builder()
        .concurrency(8)
        .register_task("email:send".into(), |task: SendEmail, ctx: ExecutionContext| async move {
            println!("attempt {}: emailing {}", ctx.attempt, task.to);
            Ok::<(), HandlerError>(())
        })?
        .build();

    engine.start();

    let payload = serde_json::to_value(SendEmail { to: "user@example.com".into() })?;
    engine.enqueue_task("email:send".into(), payload).await?;

    // Keep the process alive long enough for the task to run.
    tokio::time::sleep(Duration::from_secs(1)).await;
    Ok(())
}
```

The task type (`"email:send"` above) routes each task to its handler and is stored with the task. Keep it stable across releases, because tasks enqueued by one version of your application may be processed by the next.

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
        jitter: true,
    })
    // .broker(MyPostgresBroker::new(pool))
    .build();
```

| Option | Default | Description |
|---|---|---|
| `concurrency` | Number of CPUs | Maximum number of tasks executing at once |
| `retry_policy` | 5 attempts, 1s base delay, 5m cap | Retry limit and exponential backoff |
| `broker` | `MemoryBroker` | Storage backend |
| `queue_concurrency` | `concurrency` | Per-queue override of the concurrency limit |
| `task_timeout` | None | Default time one execution may run before it is cancelled |

## Delivery semantics

Riverbed delivers **at least once**. A task may run more than once, for example after a retry. Write handlers to be idempotent.

A task's outcome depends on how its handler ends:

| Handler outcome | Result |
|---|---|
| Returns `Ok(())` or a `Completion` | Task is acknowledged; any result is stored and any spawned tasks enqueued, in one operation |
| Returns `Err(_)` or panics | Task is retried with backoff until `max_attempts` is reached, then permanently failed |
| Its worker disappears | The lease expires and the task is claimed again. This counts a reclaim, not an attempt, so the retry budget is untouched |
| Payload fails to deserialize | Task is permanently failed without retrying, because retrying cannot fix it |
| No handler registered for the task type | Task is retried, so that a worker running a newer version can pick it up during a rolling deploy |
| Observes cancellation and returns `Err(_)` | Task is released back to the queue with no attempt consumed |
| Runs past its timeout | Task is cancelled and the attempt is consumed, then retried or failed with a `Timeout` reason |

The in-memory broker does not persist tasks. Pending tasks are lost when the process exits.

## Custom brokers

Implement `TaskBroker` to store tasks anywhere:

```rust
#[async_trait]
pub trait TaskBroker: Send + Sync {
    async fn enqueue(&self, spec: TaskSpec) -> LibResult<TaskId>;
    async fn claim(&self, filter: &ClaimFilter) -> LibResult<Lease>;
    async fn heartbeat(&self, lease: &Lease) -> LibResult<SystemTime>;
    async fn ack(&self, lease: &Lease, completion: Completion) -> LibResult<()>;
    async fn retry(&self, lease: &Lease, available_at: SystemTime) -> LibResult<()>;
    async fn release(&self, lease: &Lease) -> LibResult<()>;
    async fn fail(&self, lease: &Lease, failure: FailureRecord) -> LibResult<()>;
}
```

Inspection sits behind a second trait, `TaskStore` (`get`, `list`, `requeue`), because a backend can
be written to without being queryable and reads never touch the execution path.

`claim` waits until a task is due and leases it to the caller. It must be cancel-safe: dropping the future must never lose a task.

A lease expires, so a task whose worker disappears is handed to another one. Because two workers can
then be running the same task, every method that records an outcome takes the `Lease` rather than an
id: it carries a token, and a broker rejects one that is no longer current. An executor calls
`heartbeat` while its handler runs to hold the lease open.

Run the shared conformance suites against your backend to check it behaves like the others:

```rust
# // Enable the `testing` feature.
riverbed::testing::Conformance::new(|| MyBroker::new()).run().await;
riverbed::testing::StoreConformance::new(|| MyBroker::new()).run().await;
```

## Observability

Riverbed emits logs through `tracing` and never installs a subscriber itself. Install one in your application to see the output:

```rust
tracing_subscriber::fmt()
    .with_env_filter("riverbed=info")
    .init();
```

Each task runs inside a `task` span, so logs from your handlers automatically include `task_id`, `task_type`, and `attempt`.

## HTTP and gRPC APIs

Other services can enqueue tasks over HTTP or gRPC. Enable the `rest` feature, the `grpc` feature, or both, and serve the API from your engine:

```rust
let engine = Arc::new(engine);
engine.start();

riverbed::rest::server::serve(engine, "0.0.0.0:3000".parse()?).await?;
```

| API | Feature | Endpoint | Server |
|---|---|---|---|
| HTTP/JSON | `rest` | `POST /v1/task` with `{"category": "email:send", "payload": {...}}` | `rest::server::serve`, or `rest::server::router` to mount in an existing axum app |
| gRPC | `grpc` | `riverbed.v1.TaskService/EnqueueTask`, defined in [`proto/service.proto`](proto/service.proto) | `grpc::server::serve`, or `grpc::server::GrpcTaskService` to mount in an existing tonic server |

With both features enabled, pass clones of the same `Arc<Engine>` to each server. Building the `grpc` feature requires [`protoc`](https://protobuf.dev/installation/).

## Feature flags

| Flag | Default | Description |
|---|---|---|
| `in-memory` | Yes | Enables `MemoryBroker` and uses it when no broker is configured |
| `rest` | No | HTTP/JSON API for enqueuing tasks, built on [axum](https://docs.rs/axum) |
| `grpc` | No | gRPC API for enqueuing tasks, built on [tonic](https://docs.rs/tonic). Requires `protoc` at build time |

## Examples

| Example | Command |
|---|---|
| [`basic`](examples/basic.rs): handlers, retries, and logging | `cargo run --example basic` |
| [`rest_server`](examples/rest_server.rs): enqueue tasks over HTTP | `cargo run --example rest_server --features rest` |
| [`grpc_server`](examples/grpc_server.rs): enqueue tasks over gRPC | `cargo run --example grpc_server --features grpc` |

## Benchmarks

`make bench` runs the [Criterion](https://github.com/criterion-rs/criterion.rs) benchmarks in [`benches/`](benches/), which cover the in-memory broker, end-to-end engine throughput, and payload size. See [CONTRIBUTING.md](CONTRIBUTING.md#benchmarks) for how to compare a change against a baseline.

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
- [x] Graceful shutdown that drains in-flight tasks
- [x] `enqueue_in` / `enqueue_at` for scheduling tasks from the public API
- [x] Per-task options: max attempts, timeout, queue
- [x] Backoff jitter to avoid synchronized retry storms

### Durable, distributed backends

- [ ] PostgreSQL broker using `FOR UPDATE SKIP LOCKED` claims and `LISTEN/NOTIFY` wakeups
- [ ] SQLite broker for single-node durable deployments
- [x] Lease expiry with heartbeats, so tasks held by crashed workers are recovered
- [x] Dead-letter queue with inspection and requeue

### Robustness

- [ ] Fault-injection test suite covering worker crashes mid-task, broker outages, and network partitions
- [ ] Deterministic simulation testing of the executor and brokers
- [ ] Concurrency model checking with [`loom`](https://github.com/tokio-rs/loom)
- [x] Unique tasks and deduplication via idempotency keys
- [x] Task timeouts and cancellation
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
- [x] Task result storage

### Operations

- [ ] Metrics via OpenTelemetry: queue depth, latency, throughput, failure rate
- [ ] CLI and web dashboard for inspecting and managing queues

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for the development setup and pull request guidelines. Report security vulnerabilities privately, as described in [SECURITY.md](SECURITY.md).

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or <https://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or <https://opensource.org/licenses/MIT>)

at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.
