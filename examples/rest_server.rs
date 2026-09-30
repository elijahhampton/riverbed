//! Serves the HTTP API and executes the tasks it receives.
//!
//! ```sh
//! cargo run --example rest_server --features rest
//! ```
//!
//! Then enqueue a task from another terminal:
//!
//! ```sh
//! curl -X POST http://127.0.0.1:3000/v1/task \
//!     -H 'Content-Type: application/json' \
//!     -d '{"category": "report:generate", "payload": {"report_id": 42}}'
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

    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    riverbed::rest::server::serve(engine, addr).await?;

    Ok(())
}
