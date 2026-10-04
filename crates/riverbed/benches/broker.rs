//! Throughput of the in-memory broker on its own, without the engine.

use std::sync::Arc;
use std::time::{Duration, Instant};

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput};
use riverbed::broker::{ClaimFilter, MemoryBroker, TaskBroker};
use riverbed::completion::Completion;
use riverbed::task::TaskSpec;
use tokio::runtime::Runtime;

const TASK_TYPE: &str = "bench:noop";

/// Tasks moved through the broker per measured iteration of the producer benchmark.
const PRODUCER_BATCH: usize = 10_000;

fn new_task() -> TaskSpec {
    TaskSpec::new(TASK_TYPE.to_owned(), serde_json::Value::Null)
}

/// One enqueue, claim, and ack at several queue depths. The depth stays constant because each
/// cycle adds one task and removes the oldest.
fn cycle(c: &mut Criterion) {
    let rt = Runtime::new().expect("failed to build the Tokio runtime");
    let mut group = c.benchmark_group("broker/cycle");
    group.throughput(Throughput::Elements(1));

    for depth in [0, 1_000, 100_000] {
        let broker = MemoryBroker::new();
        rt.block_on(async {
            for _ in 0..depth {
                broker.enqueue(new_task()).await.expect("enqueue failed");
            }
        });

        group.bench_function(BenchmarkId::new("depth", depth), |b| {
            let broker = &broker;
            b.to_async(&rt).iter_batched(
                new_task,
                move |task| async move {
                    let filter = ClaimFilter::any();
                    broker.enqueue(task).await.expect("enqueue failed");
                    let lease = broker.claim(&filter).await.expect("claim failed");
                    broker
                        .ack(&lease, Completion::empty())
                        .await
                        .expect("ack failed");
                },
                BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

/// Several producers enqueue concurrently while a single consumer claims and acks, which is how
/// an engine serving an API uses its broker.
fn producers(c: &mut Criterion) {
    let rt = Runtime::new().expect("failed to build the Tokio runtime");
    let mut group = c.benchmark_group("broker/producers");
    group.throughput(Throughput::Elements(PRODUCER_BATCH as u64));
    group.sample_size(20);

    for producer_count in [1, 4, 16] {
        let broker = Arc::new(MemoryBroker::new());

        group.bench_function(BenchmarkId::from_parameter(producer_count), |b| {
            let broker = &broker;
            b.to_async(&rt).iter_custom(move |iters| async move {
                let mut elapsed = Duration::ZERO;
                for _ in 0..iters {
                    // Tasks are built before the clock starts so only broker operations are timed.
                    let batches: Vec<Vec<TaskSpec>> = (0..producer_count)
                        .map(|_| {
                            (0..PRODUCER_BATCH / producer_count)
                                .map(|_| new_task())
                                .collect()
                        })
                        .collect();

                    let start = Instant::now();
                    let handles: Vec<_> = batches
                        .into_iter()
                        .map(|batch| {
                            let broker = Arc::clone(broker);
                            tokio::spawn(async move {
                                for task in batch {
                                    broker.enqueue(task).await.expect("enqueue failed");
                                }
                            })
                        })
                        .collect();

                    let filter = ClaimFilter::any();
                    for _ in 0..PRODUCER_BATCH {
                        let lease = broker.claim(&filter).await.expect("claim failed");
                        broker
                            .ack(&lease, Completion::empty())
                            .await
                            .expect("ack failed");
                    }
                    for handle in handles {
                        handle.await.expect("producer panicked");
                    }
                    elapsed += start.elapsed();
                }
                elapsed
            });
        });
    }

    group.finish();
}

// Equivalent to `criterion_group!` and `criterion_main!`, without the undocumented public
// function those macros generate.
fn main() {
    let mut criterion = Criterion::default().configure_from_args();
    cycle(&mut criterion);
    producers(&mut criterion);
    criterion.final_summary();
}
