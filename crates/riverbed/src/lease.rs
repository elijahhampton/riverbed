//! Leases, which grant an executor the exclusive right to record a task's outcome.

use crate::task::{Task, TaskId};
use std::time::SystemTime;

/// Proof that the holder's lease on a task is the current one.
///
/// Leases expire, so a second executor can claim a task while the first is still running. Both will
/// try to record an outcome. The broker keeps the token it issued most recently and rejects any
/// mutation carrying an older one, so the stale executor's write fails instead of overwriting the
/// fresh one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct LeaseToken(u64);

impl LeaseToken {
    /// The token a task carries before any lease has been taken on it.
    pub const ZERO: Self = Self(0);

    /// Creates a token from its raw value. Brokers issue tokens; callers should not.
    pub fn from_raw(value: u64) -> Self {
        Self(value)
    }

    /// The token's raw value, for brokers that persist it.
    pub fn into_raw(self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for LeaseToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A claimed task, held until its outcome is recorded or the lease expires.
#[derive(Debug, Clone)]
pub struct Lease {
    /// The claimed task.
    pub task: Task,
    /// Proof that this lease is the current one for the task.
    pub token: LeaseToken,
    /// When the broker may hand this task to another executor. Only updated by
    /// [`TaskBroker::heartbeat`](crate::broker::TaskBroker::heartbeat)'s return value, so a
    /// long-held lease's copy can lag behind the broker's.
    pub expires_at: SystemTime,
}

impl Lease {
    /// The identifier of the leased task.
    pub fn task_id(&self) -> TaskId {
        self.task.id
    }

    /// How long until the lease expires, or zero if it already has.
    pub fn remaining(&self) -> std::time::Duration {
        self.expires_at
            .duration_since(SystemTime::now())
            .unwrap_or_default()
    }
}
