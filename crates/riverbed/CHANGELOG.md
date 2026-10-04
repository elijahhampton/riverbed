# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Leases: `TaskBroker::claim` returns a `Lease` carrying a token and an expiry, and `heartbeat`
  holds it open while a handler runs. A lease that expires is reclaimed and the task requeued,
  counted as a `reclaim` rather than an `attempt` so a vanished worker does not spend the retry
  budget. An outcome recorded against a reclaimed lease is rejected with `CoreErr::LeaseFenced`.
- `TaskBroker::release`, which requeues a task without recording an outcome or consuming an attempt.
- Queues: a task carries a queue name, `TaskBroker::claim` takes a `ClaimFilter`, and the engine
  runs one claim loop and one concurrency limit per queue. Register with `register_task_on` and
  configure with `EngineBuilder::queue_concurrency`.
- `Completion`: a handler may return a result to store and tasks to enqueue, which the broker
  applies in the same operation that releases the lease. Handlers returning `Ok(())` are unchanged.
- Cancellation: `ExecutionContext` carries a `CancellationToken` and an optional deadline, fired on
  shutdown and when an execution runs past its timeout. A handler that observes it and stops has
  its task released with no attempt consumed.
- Per-task options via `TaskSpec`: queue, delay, attempt limit, timeout, and a dedup key that makes
  enqueueing idempotent. `Engine::enqueue`, `enqueue_in` and `enqueue_at` accept them.
- `FailureRecord` retained on terminal failure, with `TaskStore` (`get`, `list`, `requeue`) for
  inspection, exposed over HTTP as `GET /v1/task/{id}` and over gRPC as `GetTask`.
- Backoff jitter, so retries of a batch do not synchronise.
- `Engine::shutdown`, which stops claiming, drains in-flight handlers up to a timeout, and releases
  any lease still held at the deadline.
- `riverbed::testing::Conformance`, a shared suite that any `TaskBroker` implementation can be run
  against, behind the `testing` feature.
- `Engine` with bounded concurrency, retries with exponential backoff, and panic isolation.
- `TaskBroker` trait for custom storage backends.
- `MemoryBroker`, enabled by the default `in-memory` feature.
- HTTP/JSON API for enqueuing tasks, behind the `rest` feature.
- gRPC API for enqueuing tasks, behind the `grpc` feature.

[Unreleased]: https://github.com/elijahhampton/riverbed/commits/main
