// A simple, reliable and efficient distributed task queue in Rust, inspired by Go-Asynq.

//! An embeddable task queue for async Rust.
//!
//! Riverbed runs inside your application on a [Tokio](https://tokio.rs) runtime. Each task has a
//! type name and a JSON payload. The [`Engine`](engine::Engine) claims tasks from a
//! [`TaskBroker`](broker::TaskBroker) and runs the handler registered for each task's type,
//! retrying failed tasks with exponential backoff.
//!
//! # Example
//!
//! ```no_run
//! use riverbed::{engine::Engine, execution::ExecutionContext, handler::HandlerError};
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Serialize, Deserialize)]
//! struct SendEmail {
//!     to: String,
//! }
//!
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let engine = Engine::builder()
//!     .register_task("email:send".into(), |email: SendEmail, ctx: ExecutionContext| async move {
//!         println!("attempt {}: sending email to {}", ctx.attempt, email.to);
//!         Ok::<(), HandlerError>(())
//!     })?
//!     .build();
//! engine.start();
//!
//! let payload = serde_json::to_value(SendEmail { to: "user@example.com".into() })?;
//! engine.enqueue_task("email:send".into(), payload).await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Delivery
//!
//! Tasks are delivered at least once. A task can run more than once, for example when it is
//! retried after its handler fails partway through, so handlers should be idempotent.
//!
//! # Feature flags
//!
//! - `in-memory` (default): enables [`MemoryBroker`](broker::MemoryBroker), which the engine uses
//!   when no broker is configured. Tasks do not survive a restart.
//! - `rest`: an HTTP/JSON API for enqueuing tasks. See [`rest`].
//! - `grpc`: a gRPC API for enqueuing tasks. See [`grpc`]. Building it requires `protoc`.
//!
//! The `rest` and `grpc` features can be enabled together and served from the same engine.

#![cfg_attr(docsrs, feature(doc_cfg))]

#[cfg(feature = "rest")]
pub mod rest;

#[cfg(feature = "grpc")]
pub mod grpc;

pub mod broker;
pub mod engine;
pub mod error;
pub mod execution;
pub mod handler;
pub mod retry;
pub mod task;
