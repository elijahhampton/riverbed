// A simple, reliable and efficient distributed task queue in Rust, inspired by Go-Asynq.

#[cfg(all(feature = "rest-api", feature = "grpc"))]
compile_error!(
    "features `rest-api` and `grpc` are mutually exclusive; to use gRPC, set \
     `default-features = false, features = [\"in-memory\", \"grpc\"]`"
);

#[cfg(feature = "rest-api")]
pub mod api;

#[cfg(feature = "grpc")]
pub mod grpc;

pub mod broker;
pub mod engine;
pub mod error;
pub mod execution;
pub mod handler;
pub mod retry;
pub mod task;

