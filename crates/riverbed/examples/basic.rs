//! Processes tasks with the in-memory broker. One task fails on its first attempt and succeeds
//! when retried.
//!
//! ```sh
//! cargo run --example basic
//! ```

use std::time::Duration;

use riverbed::{
    engine::Engine, execution::ExecutionContext, handler::HandlerError, retry::RetryPolicy,
};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tracing_subscriber::EnvFilter;

const SEND_EMAIL: &str = "email:send";

#[derive(Serialize, Deserialize)]
struct SendEmail {
    to: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info,riverbed=debug")),
        )
        .init();

    // Handlers report completion here so the example can exit once every task has run.
    let (done_tx, mut done_rx) = mpsc::unbounded_channel();

    let engine = Engine::builder()
        .concurrency(4)
        .retry_policy(RetryPolicy {
            max_attempts: 3,
            base_delay: Duration::from_millis(500),
            max_delay: Duration::from_secs(5),
        })
        .register_task(
            SEND_EMAIL.into(),
            move |email: SendEmail, ctx: ExecutionContext| {
                let done_tx = done_tx.clone();
                async move {
                    if email.to == "flaky@example.com" && ctx.attempt == 1 {
                        return Err(HandlerError::from("mail server unavailable"));
                    }
                    tracing::info!(to = %email.to, "email sent");
                    let _ = done_tx.send(());
                    Ok(())
                }
            },
        )?
        .build();

    engine.start();

    let recipients = ["alice@example.com", "bob@example.com", "flaky@example.com"];
    for to in recipients {
        let payload = serde_json::to_value(SendEmail { to: to.into() })?;
        engine.enqueue_task(SEND_EMAIL.into(), payload).await?;
    }

    for _ in recipients {
        done_rx.recv().await;
    }

    Ok(())
}
