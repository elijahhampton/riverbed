//! Cost of carrying payloads of different sizes through the engine: serializing each payload on
//! enqueue, holding it in the broker, and deserializing it for the handler.

mod common;

use std::sync::Arc;
use std::time::Instant;

use common::{Completions, runtime};
use criterion::{BenchmarkId, Criterion, Throughput};
use riverbed::{engine::Engine, execution::ExecutionContext, handler::HandlerError};
use serde::{Deserialize, Serialize};

const TASK_TYPE: &str = "bench:report";

/// Label, approximate encoded payload size in bytes, and tasks enqueued per measured iteration.
/// Larger payloads use smaller batches to bound memory use.
const CASES: [(&str, usize, u64); 3] = [
    ("100B", 100, 1_000),
    ("10KB", 10_000, 100),
    ("1MB", 1_000_000, 5),
];

#[derive(Serialize, Deserialize)]
struct Report {
    rows: Vec<Row>,
}

#[derive(Serialize, Deserialize)]
struct Row {
    id: u64,
    name: String,
}

impl Report {
    /// Builds a report whose JSON encoding is roughly `bytes` long.
    fn with_size(bytes: usize) -> Self {
        // A row such as `{"id":42,"name":"row-42"},` encodes to about 30 bytes.
        let rows = (0..(bytes / 30).max(1) as u64)
            .map(|id| Row {
                id,
                name: format!("row-{id}"),
            })
            .collect();
        Self { rows }
    }
}

fn payload_size(c: &mut Criterion) {
    let rt = runtime();
    let completions = Arc::new(Completions::default());
    let engine = report_engine(Arc::clone(&completions));
    rt.block_on(async { engine.start() });

    let mut group = c.benchmark_group("payload");
    group.sample_size(20);

    for (label, bytes, batch) in CASES {
        let report = Report::with_size(bytes);
        let encoded_len = serde_json::to_vec(&report)
            .expect("serialization failed")
            .len() as u64;
        group.throughput(Throughput::Bytes(encoded_len * batch));

        group.bench_function(BenchmarkId::from_parameter(label), |b| {
            let engine = &engine;
            let completions = &*completions;
            let report = &report;
            b.to_async(&rt).iter_custom(move |iters| async move {
                let start = Instant::now();
                for _ in 0..iters {
                    let target = completions.start_batch(batch);
                    for _ in 0..batch {
                        let payload = serde_json::to_value(report).expect("serialization failed");
                        engine
                            .enqueue_task(TASK_TYPE.into(), payload)
                            .await
                            .expect("enqueue failed");
                    }
                    completions.wait(target).await;
                }
                start.elapsed()
            });
        });
    }

    group.finish();
}

fn report_engine(completions: Arc<Completions>) -> Engine {
    Engine::builder()
        .register_task(
            TASK_TYPE.into(),
            move |_report: Report, _: ExecutionContext| {
                let completions = Arc::clone(&completions);
                async move {
                    completions.record();
                    Ok::<(), HandlerError>(())
                }
            },
        )
        .expect("handler registration failed")
        .build()
}

// Equivalent to `criterion_group!` and `criterion_main!`, without the undocumented public
// function those macros generate.
fn main() {
    let mut criterion = Criterion::default().configure_from_args();
    payload_size(&mut criterion);
    criterion.final_summary();
}
