//! gRPC API for enqueuing tasks, built on [tonic](https://docs.rs/tonic).
//!
//! The service is defined as `riverbed.v1.TaskService` in `proto/service.proto`.

pub mod server;

/// Message and service types generated from `proto/service.proto`.
#[allow(missing_docs)]
pub mod proto {
    tonic::include_proto!("riverbed.v1");
}
