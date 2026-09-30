//! Serves the gRPC API and executes the tasks it receives. Building it requires `protoc`.
//!
//! ```sh
//! cargo run --example grpc_server --features grpc
//! ```
//!
//! Then enqueue a task from another terminal with [grpcurl](https://github.com/fullstorydev/grpcurl).
//! The `payload` field holds JSON bytes, which grpcurl expects base64-encoded. The payload below
//! is `{"report_id":42}`.
//!
//! ```sh
//! grpcurl -plaintext -import-path proto -proto service.proto \
//!     -d '{"category": "report:generate", "payload": "eyJyZXBvcnRfaWQiOjQyfQ=="}' \
//!     127.0.0.1:50051 riverbed.v1.TaskService/EnqueueTask
//! ```

use std::{net::SocketAddr, sync::Arc};

use riverbed::{engine::Engine, execution::ExecutionContext, handler::HandlerError};
use serde::{Deserialize, Serialize};
use tracing_subscriber::EnvFilter;

#[derive(Serialize, Deserialize)]
struct GenerateReport {
    report_id: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let engine = Engine::builder()
        .register_task(
            "report:generate".into(),
            |report: GenerateReport, _ctx: ExecutionContext| async move {
                tracing::info!(report_id = report.report_id, "report generated");
                Ok::<(), HandlerError>(())
            },
        )?
        .build();
    let engine = Arc::new(engine);
    engine.start();

    let addr = SocketAddr::from(([127, 0, 0, 1], 50051));
    riverbed::grpc::server::serve(engine, addr).await?;

    Ok(())
}
