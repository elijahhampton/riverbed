use std::sync::atomic::{AtomicU64, Ordering};

use tokio::runtime::Runtime;
use tokio::sync::Notify;

/// Builds the multi-threaded runtime that benchmarks run the engine on.
pub fn runtime() -> Runtime {
    Runtime::new().expect("failed to build the Tokio runtime")
}

/// Counts handler completions so a benchmark can wait for a batch of tasks to finish.
///
/// The engine cannot report when it is idle, so handlers call `record` and the benchmark calls
/// `wait`. Only the final task of a batch signals the waiter, which keeps the per-task overhead
/// to one atomic increment. Batches must not overlap.
#[derive(Default)]
pub struct Completions {
    count: AtomicU64,
    target: AtomicU64,
    reached: Notify,
}

impl Completions {
    /// Records one completed task.
    pub fn record(&self) {
        let count = self.count.fetch_add(1, Ordering::AcqRel) + 1;
        if count == self.target.load(Ordering::Acquire) {
            self.reached.notify_one();
        }
    }

    /// Starts a batch of `tasks` tasks and returns the count to pass to `wait`. Call this before
    /// enqueuing the batch.
    pub fn start_batch(&self, tasks: u64) -> u64 {
        let target = self.count.load(Ordering::Acquire) + tasks;
        self.target.store(target, Ordering::Release);
        target
    }

    /// Waits until the total number of completions reaches `target`.
    pub async fn wait(&self, target: u64) {
        // `Notify` stores a permit when nobody is waiting, so a completion that lands between
        // the load and the await still wakes this loop.
        while self.count.load(Ordering::Acquire) < target {
            self.reached.notified().await;
        }
    }
}
