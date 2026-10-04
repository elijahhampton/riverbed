use super::{ClaimFilter, DEFAULT_LEASE_DURATION, ListFilter, TaskBroker, TaskRecord, TaskStore};
use crate::{
    completion::{Completion, FailureRecord},
    error::{CoreErr, LibResult},
    lease::{Lease, LeaseToken},
    task::{QueueName, Task, TaskId, TaskSpec, TaskState},
};
use async_trait::async_trait;
use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    sync::{Mutex, MutexGuard},
    time::{Duration, SystemTime},
};
use tokio::sync::Notify;
use tracing::debug;

/// Completed and failed tasks kept for inspection before the oldest are dropped.
pub const DEFAULT_RETENTION: usize = 1024;

/// The in-memory broker does not persist tasks across a restart. Completed and failed tasks are
/// retained for inspection up to a bounded count, then discarded oldest first.
pub struct MemoryBroker {
    state: Mutex<State>,
    /// Signals that the set of claimable tasks may have grown.
    ///
    /// Every waiter is woken rather than one, because claimers filter by queue: waking a single
    /// arbitrary waiter can wake one whose filter rejects the new task while a waiter that would
    /// have taken it stays parked. Claimers register interest before re-checking state, so a
    /// notification landing in between is not lost.
    task_ready: Notify,
    lease_duration: Duration,
    retention: usize,
}

impl Default for MemoryBroker {
    fn default() -> Self {
        Self {
            state: Mutex::default(),
            task_ready: Notify::new(),
            lease_duration: DEFAULT_LEASE_DURATION,
            retention: DEFAULT_RETENTION,
        }
    }
}

/// One task and everything the broker knows about it.
struct Entry {
    task: Task,
    state: TaskState,
    /// The most recently issued lease token. An outcome carrying an older one is rejected.
    token: LeaseToken,
    expires_at: Option<SystemTime>,
    result: Option<serde_json::Value>,
    failure: Option<FailureRecord>,
    enqueued_at: SystemTime,
    updated_at: SystemTime,
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
///   the empty-bucket case and you still need the vector for order.
///
/// - (SystemTime, TaskId) Unique so nothing gets lost, but UUIDs are random so tasks
///   with the same due time run in arbitrary order. Although, UUIDv7 would sort by time
///   and could serve as both an ID and a tie-breaker.
///
/// - BinaryHeap<Reverse<(SystemTime, u64, Task)>> Performing peek() becomes O(1)
///   instead of O(log n), with better constrants, but it would still need the
///   counter, because heap order among equal keys is unspecified. This method loses
///   range queries and removal by key.
///
/// Alternatives to the mapping data structure:
/// The BTreeMap costs slightly more per claim() but keeps two doors open:
/// 1. Batch claiming becomes range(..=now), taking everything due in one lock
///    acquisiton rather than one round per task. A heap can do this by popping
///    repeatedly, but only from the front, which might constrain flexibility in the implementation.
///
/// 2. Cancellation and deduplication need removing or finding a specific
///    pending task. A BinaryHeap can't remove from the middle so you'd need lazy deletion,
///    keeping a reference set and discarding stale entries on pop.
///
/// There is one index per queue. A claimer filters by queue, so the earliest due task overall may
/// be one it cannot take, and a single index would have to be scanned past it; one index per queue
/// makes the earliest acceptable task the first entry of each candidate index.
#[derive(Default)]
struct State {
    pending: HashMap<QueueName, BTreeMap<(SystemTime, u64), TaskId>>,
    tasks: HashMap<TaskId, Entry>,
    /// Dedup key to the unfinished task holding it.
    dedup: HashMap<String, TaskId>,
    /// Terminal tasks in the order they finished, so the oldest can be dropped first.
    terminal: VecDeque<TaskId>,
    next_seq: u64,
    next_token: u64,
}

impl State {
    /// Puts `task` back in its queue's index and updates its entry to match.
    fn push_pending(&mut self, task: Task) {
        let seq = self.next_seq;
        self.next_seq += 1;
        self.pending
            .entry(task.queue.clone())
            .or_default()
            .insert((task.available_at, seq), task.id);

        let entry = self
            .tasks
            .get_mut(&task.id)
            .expect("a pending task always has an entry");
        entry.task = task;
        entry.state = TaskState::Pending;
        entry.expires_at = None;
        entry.updated_at = SystemTime::now();
    }

    fn issue_token(&mut self) -> LeaseToken {
        self.next_token += 1;
        LeaseToken::from_raw(self.next_token)
    }

    /// Validates that `lease` is the current lease on its task.
    ///
    /// Checks state before the token: a reclaimed task is no longer leased while its token is
    /// still the one most recently issued, so a token-only comparison would accept an outcome for
    /// a task that has already gone back to the queue.
    fn current(&mut self, lease: &Lease) -> LibResult<&mut Entry> {
        let id = lease.task_id();
        let entry = self.tasks.get_mut(&id).ok_or(CoreErr::LeaseNotFound(id))?;
        if entry.state != TaskState::Leased {
            return Err(CoreErr::LeaseNotFound(id));
        }
        if entry.token != lease.token {
            return Err(CoreErr::LeaseFenced(id));
        }
        Ok(entry)
    }

    /// Records a task as terminal, freeing its dedup key and evicting the oldest if needed.
    fn finish(&mut self, id: TaskId, retention: usize) {
        if let Some(key) = self
            .tasks
            .get(&id)
            .and_then(|entry| entry.task.dedup_key.clone())
        {
            self.dedup.remove(&key);
        }

        self.terminal.push_back(id);
        while self.terminal.len() > retention {
            if let Some(evicted) = self.terminal.pop_front() {
                self.tasks.remove(&evicted);
            }
        }
    }

    /// Returns leases that outlived their expiry to the queue.
    ///
    /// A worker that vanished says nothing about the task, so this counts a reclaim rather than an
    /// attempt and leaves the retry budget alone.
    fn sweep_expired(&mut self, now: SystemTime) {
        let expired: Vec<TaskId> = self
            .tasks
            .iter()
            .filter(|(_, entry)| {
                entry.state == TaskState::Leased && entry.expires_at.is_some_and(|at| at <= now)
            })
            .map(|(id, _)| *id)
            .collect();

        for id in expired {
            let mut task = self
                .tasks
                .get(&id)
                .expect("id came from this map")
                .task
                .clone();
            task.reclaims += 1;
            task.available_at = now;
            debug!(
                task_id = %id,
                reclaims = task.reclaims,
                "lease expired; returning the task to the queue"
            );
            self.push_pending(task);
        }
    }

    /// The earliest moment the claimable set can change without an enqueue.
    fn next_lease_expiry(&self) -> Option<SystemTime> {
        self.tasks
            .values()
            .filter(|entry| entry.state == TaskState::Leased)
            .filter_map(|entry| entry.expires_at)
            .min()
    }

    /// The queues the filter accepts that currently hold anything.
    fn candidate_queues(&self, filter: &ClaimFilter) -> Vec<QueueName> {
        if filter.is_any() {
            self.pending.keys().cloned().collect()
        } else {
            filter
                .named()
                .iter()
                .filter(|queue| self.pending.contains_key(*queue))
                .cloned()
                .collect()
        }
    }

    /// Leases the earliest acceptable due task, or reports when to look again.
    fn take_due(
        &mut self,
        filter: &ClaimFilter,
        now: SystemTime,
    ) -> Result<Task, Option<SystemTime>> {
        let mut earliest: Option<((SystemTime, u64), QueueName)> = None;
        for queue in self.candidate_queues(filter) {
            let Some(key) = self.pending[&queue].keys().next().copied() else {
                continue;
            };
            if earliest.as_ref().is_none_or(|(best, _)| key < *best) {
                earliest = Some((key, queue));
            }
        }

        let Some((key, queue)) = earliest else {
            return Err(self.next_lease_expiry());
        };
        if key.0 > now {
            return Err(Some(match self.next_lease_expiry() {
                Some(expiry) => key.0.min(expiry),
                None => key.0,
            }));
        }

        let index = self.pending.get_mut(&queue).expect("queue index vanished");
        let id = index.remove(&key).expect("indexed task vanished");
        if index.is_empty() {
            self.pending.remove(&queue);
        }

        let token = self.issue_token();
        let entry = self
            .tasks
            .get_mut(&id)
            .expect("a claimed task always has an entry");
        entry.state = TaskState::Leased;
        entry.token = token;
        entry.updated_at = now;
        Ok(entry.task.clone())
    }

    /// Stores `spec` as a new task, or returns the id of the task already holding its dedup key.
    fn insert(&mut self, spec: TaskSpec) -> TaskId {
        if let Some(key) = spec.dedup_key.as_ref()
            && let Some(existing) = self.dedup.get(key).copied()
        {
            return existing;
        }

        let task = Task::from_spec(spec);
        let id = task.id;
        let now = SystemTime::now();
        if let Some(key) = task.dedup_key.clone() {
            self.dedup.insert(key, id);
        }
        self.tasks.insert(
            id,
            Entry {
                task: task.clone(),
                state: TaskState::Pending,
                token: LeaseToken::ZERO,
                expires_at: None,
                result: None,
                failure: None,
                enqueued_at: now,
                updated_at: now,
            },
        );
        self.push_pending(task);
        id
    }
}

impl MemoryBroker {
    /// Creates an empty broker.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets how long a claimed task stays leased before it may be granted to another executor.
    pub fn with_lease_duration(mut self, lease_duration: Duration) -> Self {
        self.lease_duration = lease_duration;
        self
    }

    /// Sets how many terminal tasks are retained for inspection.
    pub fn with_retention(mut self, retention: usize) -> Self {
        self.retention = retention;
        self
    }

    fn state(&self) -> MutexGuard<'_, State> {
        self.state.lock().expect("memory broker lock poisoned")
    }

    /// Leases the earliest acceptable due task, or returns when the claimable set can next change.
    fn try_claim(&self, filter: &ClaimFilter) -> Result<Lease, Option<SystemTime>> {
        let now = SystemTime::now();
        let mut guard = self.state();
        let state = &mut *guard;

        // Before looking at the indexes, so a lease that expired while this claimer waited is
        // visible to the call that can act on it.
        state.sweep_expired(now);

        let task = state.take_due(filter, now)?;
        let expires_at = now + self.lease_duration;
        let entry = state
            .tasks
            .get_mut(&task.id)
            .expect("a just-claimed task always has an entry");
        entry.expires_at = Some(expires_at);

        Ok(Lease {
            token: entry.token,
            task,
            expires_at,
        })
    }

    fn record(entry: &Entry) -> TaskRecord {
        TaskRecord {
            task: entry.task.clone(),
            state: entry.state,
            result: entry.result.clone(),
            failure: entry.failure.clone(),
            enqueued_at: entry.enqueued_at,
            updated_at: entry.updated_at,
        }
    }
}

#[async_trait]
impl TaskBroker for MemoryBroker {
    async fn enqueue(&self, spec: TaskSpec) -> LibResult<TaskId> {
        let id = self.state().insert(spec);
        self.task_ready.notify_waiters();
        Ok(id)
    }

    async fn claim(&self, filter: &ClaimFilter) -> LibResult<Lease> {
        loop {
            // Interest is registered before the state is checked, because `notify_waiters` wakes
            // only waiters already registered and stores no permit for a latecomer.
            let notified = self.task_ready.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            match self.try_claim(filter) {
                Ok(lease) => return Ok(lease),
                Err(Some(wake_at)) => {
                    let delay = wake_at
                        .duration_since(SystemTime::now())
                        .unwrap_or_default();
                    tokio::select! {
                        _ = notified => {}
                        _ = tokio::time::sleep(delay) => {}
                    }
                }
                Err(None) => notified.await,
            }
        }
    }

    async fn heartbeat(&self, lease: &Lease) -> LibResult<SystemTime> {
        let expires_at = SystemTime::now() + self.lease_duration;
        let mut state = self.state();
        let entry = state.current(lease)?;
        entry.expires_at = Some(expires_at);
        entry.updated_at = SystemTime::now();
        Ok(expires_at)
    }

    async fn ack(&self, lease: &Lease, completion: Completion) -> LibResult<()> {
        let mut state = self.state();
        let entry = state.current(lease)?;
        entry.state = TaskState::Completed;
        entry.expires_at = None;
        entry.result = completion.result;
        entry.updated_at = SystemTime::now();

        // The spawned tasks are inserted under the same lock as the acknowledgement, so a reader
        // never sees the parent completed without them, or them without the parent completed.
        let spawned = completion.spawn.len();
        for spec in completion.spawn {
            state.insert(spec);
        }
        state.finish(lease.task_id(), self.retention);
        drop(state);

        if spawned > 0 {
            self.task_ready.notify_waiters();
        }
        Ok(())
    }

    async fn retry(&self, lease: &Lease, available_at: SystemTime) -> LibResult<()> {
        let mut state = self.state();
        state.current(lease)?;
        let mut task = lease.task.clone();
        task.attempts += 1;
        task.available_at = available_at;
        state.push_pending(task);
        drop(state);

        self.task_ready.notify_waiters();
        Ok(())
    }

    async fn release(&self, lease: &Lease) -> LibResult<()> {
        let mut state = self.state();
        state.current(lease)?;
        let mut task = lease.task.clone();
        task.available_at = SystemTime::now();
        state.push_pending(task);
        drop(state);

        self.task_ready.notify_waiters();
        Ok(())
    }

    async fn fail(&self, lease: &Lease, failure: FailureRecord) -> LibResult<()> {
        let mut state = self.state();
        let entry = state.current(lease)?;
        entry.state = TaskState::Failed;
        entry.expires_at = None;
        entry.failure = Some(failure);
        entry.updated_at = SystemTime::now();
        state.finish(lease.task_id(), self.retention);
        Ok(())
    }
}

#[async_trait]
impl TaskStore for MemoryBroker {
    async fn get(&self, id: TaskId) -> LibResult<Option<TaskRecord>> {
        Ok(self.state().tasks.get(&id).map(Self::record))
    }

    async fn list(&self, filter: &ListFilter) -> LibResult<Vec<TaskRecord>> {
        let state = self.state();
        let mut records: Vec<TaskRecord> = state
            .tasks
            .values()
            .filter(|entry| filter.state.is_none_or(|state| state == entry.state))
            .filter(|entry| {
                filter
                    .queue
                    .as_ref()
                    .is_none_or(|queue| *queue == entry.task.queue)
            })
            .map(Self::record)
            .collect();

        records.sort_by(|a, b| b.updated_at.cmp(&a.updated_at));
        if filter.limit > 0 {
            records.truncate(filter.limit);
        }
        Ok(records)
    }

    async fn requeue(&self, id: TaskId) -> LibResult<()> {
        let mut state = self.state();
        let entry = state.tasks.get(&id).ok_or(CoreErr::TaskNotFound(id))?;
        if entry.state != TaskState::Failed {
            return Err(CoreErr::UnexpectedState {
                id,
                state: entry.state.as_str(),
            });
        }

        let mut task = entry.task.clone();
        task.attempts = 0;
        task.reclaims = 0;
        task.available_at = SystemTime::now();

        state
            .tasks
            .get_mut(&id)
            .expect("checked just above")
            .failure = None;
        if let Some(key) = task.dedup_key.clone() {
            state.dedup.insert(key, id);
        }
        state.terminal.retain(|terminal| *terminal != id);
        state.push_pending(task);
        drop(state);

        self.task_ready.notify_waiters();
        Ok(())
    }
}
