//! End-to-end throughput of the engine with the in-memory broker: tasks are enqueued, executed by
//! a no-op handler, and acknowledged, at several concurrency limits.

mod common;

use std::sync::Arc;
use std::time::Instant;

use common::{Completions, runtime};
use criterion::{BenchmarkId, Criterion, Throughput};
use riverbed::{engine::Engine, execution::ExecutionContext, handler::HandlerError};

const TASK_TYPE: &str = "bench:noop";

/// Tasks enqueued per measured iteration.
const BATCH: u64 = 1_000;

fn throughput(c: &mut Criterion) {
    let rt = runtime();
    let mut group = c.benchmark_group("engine/throughput");
    group.throughput(Throughput::Elements(BATCH));
    group.sample_size(20);

    for concurrency in [1, 4, 16, 64] {
        // One engine per concurrency level, reused across that level's iterations.
        let completions = Arc::new(Completions::default());
        let engine = noop_engine(concurrency, Arc::clone(&completions));
        rt.block_on(async { engine.start() });

        group.bench_function(BenchmarkId::new("concurrency", concurrency), |b| {
            let engine = &engine;
            let completions = &*completions;
            b.to_async(&rt).iter_custom(move |iters| async move {
                let start = Instant::now();
                for _ in 0..iters {
                    let target = completions.start_batch(BATCH);
                    for _ in 0..BATCH {
                        engine
                            .enqueue_task(TASK_TYPE.into(), serde_json::Value::Null)
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

fn noop_engine(concurrency: usize, completions: Arc<Completions>) -> Engine {
    Engine::builder()
        .concurrency(concurrency)
        .register_task(TASK_TYPE.into(), move |_: (), _: ExecutionContext| {
            let completions = Arc::clone(&completions);
            async move {
                completions.record();
                Ok::<(), HandlerError>(())
            }
        })
        .expect("handler registration failed")
        .build()
}

// Equivalent to `criterion_group!` and `criterion_main!`, without the undocumented public
// function those macros generate.
fn main() {
    let mut criterion = Criterion::default().configure_from_args();
    throughput(&mut criterion);
    criterion.final_summary();
}
