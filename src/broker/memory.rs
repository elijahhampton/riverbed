use super::TaskBroker;
use crate::{
    error::{CoreErr, LibResult},
    task::{Task, TaskId},
};
use async_trait::async_trait;
use std::{
    collections::{BTreeMap, HashMap},
    sync::{Mutex, MutexGuard},
    time::SystemTime,
};
use tokio::sync::Notify;

/// The in-memory broker does not persist task being processed during a 
/// restart and permanently failed tasks are discarded.
#[derive(Default)]
pub struct MemoryBroker {
    state: Mutex<State>,
    /// State signalling when `pending` changes. Claimers waiting to claim a 
    /// due task can recheck when the pending state updates.
    task_ready: Notify,
}

/// Memory broker state holding any current pending task, in flight task and the next
/// task sequence number.
///
/// Pending task are keyed by due time and then by insertion order. Two task enqueued 
/// within the same clock tick would collide when claimed and the second would silently 
/// evict the first. This is an edge case but the insert order protects against it for 
/// distributed systems which claim task arbitrarily. Equally due tasks are claimed 
/// first in first out.
///
/// Alternatives to the insertion key:
/// - BTreeMap<SystemTime, Vec<Task>> Groups naturally but every operation has to handle 
/// the empty-bucket case and you still need the vector for order.
///
/// - (SystemTime, TaskId) Unique so nothing gets lost, but UUIDs are random so tasks 
/// with the same due time run in arbitrary order. Although, UUIDv7 would sort by time 
/// and could serve as both an ID and a tie-breaker.
///
/// - BinaryHeap<Reverse<(SystemTime, u64, Task)>> Performing peek() becomes O(1) 
/// instead of O(log n), with better constrants, but it would still need the 
/// counter, because heap order among equal keys is unspecified. This method loses 
/// range queries and removal by key.
///
/// Alternatives to the mapping data structure:
/// The BTreeMap costs slightly more per claim() but keeps two doors open:
/// 1. Batch claiming becomes range(..=now), taking everything due in one lock 
/// acquisiton rather than one round per task. A heap can do this by popping 
/// repeatedly, but only from the front, which might constrain flexibility in the implementation.
///
/// 2. Cancellation and deduplication need removing or finding a specific 
/// pending task. A BinaryHeap can't remove from the middle so you'd need lazy deletion, 
/// keeping a reference set and discarding stale entries on pop.
#[derive(Default)]
struct State {
    pending: BTreeMap<(SystemTime, u64), Task>,
    leased: HashMap<TaskId, Task>,
    next_seq: u64,
}

impl State {
    fn push(&mut self, task: Task) {
        let seq = self.next_seq;
        self.next_seq += 1;
        self.pending.insert((task.available_at, seq), task);
    }

    fn release_lease(&mut self, task_id: TaskId) -> LibResult<Task> {
        self.leased
            .remove(&task_id)
            .ok_or(CoreErr::LeaseNotFound(task_id))
    }
}

impl MemoryBroker {
    pub fn new() -> Self {
        Self::default()
    }

    fn state(&self) -> MutexGuard<'_, State> {
        self.state.lock().expect("memory broker lock poisoned")
    }

    /// Leases the earliest due task or returns when the earliest 
    /// pending task becomes due
    fn try_claim(&self) -> Result<Task, Option<SystemTime>> {
        let mut guard = self.state();
        let state = &mut *guard;

        let Some(entry) = state.pending.first_entry() else {
            return Err(None);
        };

        let available_at = entry.key().0;
        if available_at > SystemTime::now() {
            return Err(Some(available_at));
        }

        let task = entry.remove();
        state.leased.insert(task.id, task.clone());
        Ok(task)
    }
}

#[async_trait]
impl TaskBroker for MemoryBroker {
    async fn enqueue(&self, task: Task) -> LibResult<()> {
        self.state().push(task);
        self.task_ready.notify_one();
        Ok(())
    }

    async fn claim(&self) -> LibResult<Task> {
        loop {
            match self.try_claim() {
                Ok(task) => return Ok(task),
                Err(Some(available_at)) => {
                    let delay = available_at.duration_since(SystemTime::now()).unwrap_or_default();
                    tokio::select! {
                        _ = self.task_ready.notified() => {}
                        _ = tokio::time::sleep(delay) => {}
                    }
                }
                Err(None) => self.task_ready.notified().await,
            }
        }
    }

    async fn ack(&self, task_id: TaskId) -> LibResult<()> {
        self.state().release_lease(task_id)?;
        Ok(())
    }

    async fn retry(&self, task_id: TaskId, available_at: SystemTime) -> LibResult<()> {
        let mut state = self.state();
        let mut task = state.release_lease(task_id)?;
        task.attempts += 1;
        task.available_at = available_at;
        state.push(task);
        self.task_ready.notify_one();
        Ok(())
    }

    async fn fail(&self, task_id: TaskId) -> LibResult<()> {
        self.state().release_lease(task_id)?;
        Ok(())
    }
}
