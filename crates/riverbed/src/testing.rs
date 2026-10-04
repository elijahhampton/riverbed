//! Conformance suites for [`TaskBroker`] and [`TaskStore`] implementations.
//!
//! Every backend must behave the same way, so the cases live here rather than beside any one
//! implementation: a crate providing a broker imports these suites and runs them against its own.
//!
//! ```no_run
//! # use riverbed::{broker::TaskBroker, testing::Conformance};
//! # async fn example<B: TaskBroker + 'static>(new_broker: impl Fn() -> B) {
//! Conformance::new(new_broker).run().await;
//! # }
//! ```
//!
//! Each case asserts something the trait documentation promises, and nothing it does not.
//! Ordering *across* queues, for instance, is unspecified, so asserting it here would reject a
//! valid backend.

use crate::{
    broker::{ClaimFilter, ListFilter, TaskBroker, TaskStore},
    completion::{Completion, FailureKind, FailureRecord},
    error::CoreErr,
    lease::Lease,
    task::{DEFAULT_QUEUE, TaskId, TaskSpec, TaskState},
};
use std::{
    collections::HashSet,
    sync::Arc,
    time::{Duration, SystemTime},
};

/// How long a case waits before deciding a task is not coming.
const SETTLE: Duration = Duration::from_millis(50);

/// How long a case waits for something it expects to arrive.
const PATIENCE: Duration = Duration::from_secs(5);

const DEFINITION: &str = "conformance:task";

/// Runs the broker conformance cases against a backend.
///
/// `new_broker` must return an empty, independent broker each time it is called: cases do not
/// share state. The lease duration must match what those brokers are configured with, because the
/// expiry cases wait it out.
pub struct Conformance<F> {
    new_broker: F,
    lease_duration: Duration,
}

impl<B, F> Conformance<F>
where
    B: TaskBroker + 'static,
    F: Fn() -> B,
{
    /// Creates a suite over brokers produced by `new_broker`, leasing for 200ms.
    pub fn new(new_broker: F) -> Self {
        Self {
            new_broker,
            lease_duration: Duration::from_millis(200),
        }
    }

    /// Declares how long the brokers under test hold a lease.
    pub fn lease_duration(mut self, lease_duration: Duration) -> Self {
        self.lease_duration = lease_duration;
        self
    }

    /// Runs every case, panicking on the first failure.
    pub async fn run(&self) {
        self.equally_due_tasks_are_claimed_in_order().await;
        self.a_delayed_task_is_not_claimed_early().await;
        self.an_outcome_cannot_be_recorded_twice().await;
        self.retry_requeues_with_an_attempt_consumed().await;
        self.fail_is_terminal().await;
        self.release_requeues_without_consuming_an_attempt().await;
        self.a_dropped_claim_loses_no_task().await;
        self.concurrent_claimers_receive_disjoint_tasks().await;
        self.an_expired_lease_returns_the_task().await;
        self.a_reclaimed_holder_cannot_record_an_outcome().await;
        self.a_heartbeat_keeps_a_lease_past_its_expiry().await;
        self.a_heartbeat_fails_once_the_lease_is_gone().await;
        self.a_claimer_only_receives_tasks_from_its_queues().await;
        self.a_notification_reaches_a_claimer_that_can_serve_it()
            .await;
        self.a_completion_enqueues_the_tasks_it_spawns().await;
        self.a_dedup_key_makes_enqueueing_idempotent().await;
    }

    fn broker(&self) -> B {
        (self.new_broker)()
    }

    /// Waits out a lease, with a margin so a case is not decided by scheduler jitter.
    async fn outlive_lease(&self) {
        tokio::time::sleep(self.lease_duration + self.lease_duration / 2).await;
    }

    async fn equally_due_tasks_are_claimed_in_order(&self) {
        let broker = self.broker();
        let mut enqueued = Vec::new();
        for _ in 0..8 {
            enqueued.push(enqueue(&broker, spec()).await);
        }

        let mut claimed = Vec::new();
        for _ in 0..enqueued.len() {
            claimed.push(claim(&broker).await.task.id);
        }
        assert_eq!(
            claimed, enqueued,
            "tasks that are equally due must be claimed first in, first out"
        );
    }

    async fn a_delayed_task_is_not_claimed_early(&self) {
        let broker = self.broker();
        let id = enqueue(&broker, spec().available_in(SETTLE * 4)).await;

        assert!(
            claim_times_out(&broker, SETTLE).await,
            "a task must not be claimable before its available_at"
        );
        assert_eq!(claim(&broker).await.task.id, id);
    }

    async fn an_outcome_cannot_be_recorded_twice(&self) {
        let broker = self.broker();
        enqueue(&broker, spec()).await;

        let lease = claim(&broker).await;
        broker
            .ack(&lease, Completion::empty())
            .await
            .expect("the first ack must succeed");

        assert!(
            matches!(
                broker.ack(&lease, Completion::empty()).await,
                Err(CoreErr::LeaseNotFound(_))
            ),
            "a second ack on a released lease must be rejected"
        );
    }

    async fn retry_requeues_with_an_attempt_consumed(&self) {
        let broker = self.broker();
        let id = enqueue(&broker, spec()).await;

        let lease = claim(&broker).await;
        assert_eq!(lease.task.attempts, 0);
        broker
            .retry(&lease, SystemTime::now())
            .await
            .expect("retry failed");

        let requeued = claim(&broker).await;
        assert_eq!(requeued.task.id, id);
        assert_eq!(requeued.task.attempts, 1, "retry must consume an attempt");
    }

    async fn fail_is_terminal(&self) {
        let broker = self.broker();
        enqueue(&broker, spec()).await;

        let lease = claim(&broker).await;
        broker.fail(&lease, failure()).await.expect("fail failed");

        assert!(
            claim_times_out(&broker, SETTLE).await,
            "a failed task must not be claimed again"
        );
    }

    async fn release_requeues_without_consuming_an_attempt(&self) {
        let broker = self.broker();
        let id = enqueue(&broker, spec()).await;

        let lease = claim(&broker).await;
        broker.release(&lease).await.expect("release failed");

        let requeued = claim(&broker).await;
        assert_eq!(requeued.task.id, id);
        assert_eq!(
            requeued.task.attempts, 0,
            "release must not consume an attempt"
        );
    }

    /// `claim` is documented as cancel-safe, so a claimer that parks, is notified, and is then
    /// dropped must hand the notification on rather than swallowing it.
    async fn a_dropped_claim_loses_no_task(&self) {
        let broker = self.broker();
        let filter = ClaimFilter::any();

        let mut parked = broker.claim(&filter);
        assert!(
            tokio::time::timeout(SETTLE, &mut parked).await.is_err(),
            "nothing is enqueued yet, so this claim must still be waiting"
        );

        let id = enqueue(&broker, spec()).await;
        drop(parked);

        assert_eq!(
            claim(&broker).await.task.id,
            id,
            "the task must survive a claimer dropped after being notified"
        );
    }

    async fn concurrent_claimers_receive_disjoint_tasks(&self) {
        let broker = Arc::new(self.broker());
        let count = 16;
        for _ in 0..count {
            enqueue(&*broker, spec()).await;
        }

        let claimers: Vec<_> = (0..count)
            .map(|_| {
                let broker = Arc::clone(&broker);
                tokio::spawn(async move { claim(&*broker).await.task.id })
            })
            .collect();

        let mut seen = HashSet::new();
        for claimer in claimers {
            let id = claimer.await.expect("claimer panicked");
            assert!(seen.insert(id), "task {id} was leased to two claimers");
        }
    }

    async fn an_expired_lease_returns_the_task(&self) {
        let broker = self.broker();
        let id = enqueue(&broker, spec()).await;

        let first = claim(&broker).await;
        self.outlive_lease().await;
        let second = claim(&broker).await;

        assert_eq!(second.task.id, id);
        assert_eq!(second.task.reclaims, 1, "an expiry must count as a reclaim");
        assert_eq!(
            second.task.attempts, 0,
            "an expiry must not consume an attempt"
        );
        assert!(
            second.token > first.token,
            "a re-granted lease must carry a newer token"
        );
    }

    async fn a_reclaimed_holder_cannot_record_an_outcome(&self) {
        let broker = self.broker();
        enqueue(&broker, spec()).await;

        let first = claim(&broker).await;
        self.outlive_lease().await;
        let second = claim(&broker).await;

        assert!(
            matches!(
                broker.ack(&first, Completion::empty()).await,
                Err(CoreErr::LeaseFenced(_))
            ),
            "a holder whose lease was reclaimed must not be able to record an outcome"
        );
        broker
            .ack(&second, Completion::empty())
            .await
            .expect("the current holder's ack must succeed");
    }

    async fn a_heartbeat_keeps_a_lease_past_its_expiry(&self) {
        let broker = self.broker();
        enqueue(&broker, spec()).await;

        let lease = claim(&broker).await;
        for _ in 0..4 {
            tokio::time::sleep(self.lease_duration / 2).await;
            broker.heartbeat(&lease).await.expect("heartbeat failed");
        }

        assert!(
            claim_times_out(&broker, SETTLE).await,
            "a heartbeaten task must not be claimable by anyone else"
        );
        broker
            .ack(&lease, Completion::empty())
            .await
            .expect("ack after heartbeats failed");
    }

    async fn a_heartbeat_fails_once_the_lease_is_gone(&self) {
        let broker = self.broker();
        enqueue(&broker, spec()).await;

        let first = claim(&broker).await;
        self.outlive_lease().await;
        let _second = claim(&broker).await;

        assert!(
            matches!(broker.heartbeat(&first).await, Err(CoreErr::LeaseFenced(_))),
            "a heartbeat must fail once the lease has been granted to someone else"
        );
    }

    async fn a_claimer_only_receives_tasks_from_its_queues(&self) {
        let broker = self.broker();
        let wanted = enqueue(&broker, spec().queue("alpha")).await;
        enqueue(&broker, spec().queue("beta")).await;

        let alpha = ClaimFilter::queues(["alpha"]);
        let lease = tokio::time::timeout(PATIENCE, broker.claim(&alpha))
            .await
            .expect("timed out claiming from alpha")
            .expect("claim failed");
        assert_eq!(lease.task.id, wanted);
        assert_eq!(lease.task.queue, "alpha");

        assert!(
            tokio::time::timeout(SETTLE, broker.claim(&alpha))
                .await
                .is_err(),
            "a claimer must not receive a task from a queue outside its filter"
        );
    }

    /// With filters in play, waking one arbitrary waiter can wake one that cannot serve the new
    /// task while the one that could stays parked.
    async fn a_notification_reaches_a_claimer_that_can_serve_it(&self) {
        for _ in 0..8 {
            let broker = Arc::new(self.broker());

            let idle = {
                let broker = Arc::clone(&broker);
                tokio::spawn(async move {
                    let filter = ClaimFilter::queues(["idle"]);
                    broker.claim(&filter).await
                })
            };
            let busy = {
                let broker = Arc::clone(&broker);
                tokio::spawn(async move {
                    let filter = ClaimFilter::queues(["busy"]);
                    broker.claim(&filter).await
                })
            };
            // Both claimers park before anything is enqueued.
            tokio::time::sleep(SETTLE).await;

            let id = enqueue(&*broker, spec().queue("busy")).await;
            let lease = tokio::time::timeout(PATIENCE, busy)
                .await
                .expect("the claimer serving this queue was never woken")
                .expect("claimer panicked")
                .expect("claim failed");
            assert_eq!(lease.task.id, id);

            idle.abort();
        }
    }

    async fn a_completion_enqueues_the_tasks_it_spawns(&self) {
        let broker = self.broker();
        enqueue(&broker, spec()).await;

        let lease = claim(&broker).await;
        let child = spec().queue(DEFAULT_QUEUE);
        broker
            .ack(&lease, Completion::empty().spawn(child))
            .await
            .expect("ack with a spawned task failed");

        let spawned = claim(&broker).await;
        assert_ne!(
            spawned.task.id, lease.task.id,
            "the spawned task must be a new task, claimable after the parent's ack"
        );
    }

    async fn a_dedup_key_makes_enqueueing_idempotent(&self) {
        let broker = self.broker();
        let first = enqueue(&broker, spec().dedup_key("only-once")).await;
        let second = enqueue(&broker, spec().dedup_key("only-once")).await;

        assert_eq!(
            first, second,
            "a second enqueue with a live dedup key must return the existing task"
        );

        claim(&broker).await;
        assert!(
            claim_times_out(&broker, SETTLE).await,
            "only one task should have been created"
        );
    }
}

/// Runs the read-side conformance cases against a backend that is both a broker and a store.
pub struct StoreConformance<F> {
    new_broker: F,
}

impl<B, F> StoreConformance<F>
where
    B: TaskBroker + TaskStore + 'static,
    F: Fn() -> B,
{
    /// Creates a suite over backends produced by `new_broker`.
    pub fn new(new_broker: F) -> Self {
        Self { new_broker }
    }

    /// Runs every case, panicking on the first failure.
    pub async fn run(&self) {
        self.a_tasks_lifecycle_is_visible().await;
        self.a_failure_is_retained_with_its_reason().await;
        self.a_failed_task_can_be_requeued().await;
        self.requeue_rejects_a_task_that_has_not_failed().await;
        self.list_filters_by_state_and_queue().await;
        self.get_returns_nothing_for_an_unknown_id().await;
    }

    fn broker(&self) -> B {
        (self.new_broker)()
    }

    async fn a_tasks_lifecycle_is_visible(&self) {
        let broker = self.broker();
        let id = enqueue(&broker, spec()).await;

        let pending = get(&broker, id).await;
        assert_eq!(pending.state, TaskState::Pending);
        assert!(pending.result.is_none());

        let lease = claim(&broker).await;
        assert_eq!(get(&broker, id).await.state, TaskState::Leased);

        let result = serde_json::json!({ "rows": 3 });
        broker
            .ack(&lease, Completion::empty().result(result.clone()))
            .await
            .expect("ack failed");

        let completed = get(&broker, id).await;
        assert_eq!(completed.state, TaskState::Completed);
        assert_eq!(
            completed.result.as_ref(),
            Some(&result),
            "a completion's result must be readable afterwards"
        );
    }

    async fn a_failure_is_retained_with_its_reason(&self) {
        let broker = self.broker();
        let id = enqueue(&broker, spec()).await;

        let lease = claim(&broker).await;
        broker
            .fail(
                &lease,
                FailureRecord::new(FailureKind::Timeout, "too slow").attempts(3),
            )
            .await
            .expect("fail failed");

        let record = get(&broker, id).await;
        assert_eq!(record.state, TaskState::Failed);
        let failure = record
            .failure
            .expect("a failed task must retain its reason");
        assert_eq!(failure.kind, FailureKind::Timeout);
        assert_eq!(failure.detail, "too slow");
        assert_eq!(failure.attempts, 3);
    }

    async fn a_failed_task_can_be_requeued(&self) {
        let broker = self.broker();
        let id = enqueue(&broker, spec()).await;

        let lease = claim(&broker).await;
        broker
            .retry(&lease, SystemTime::now())
            .await
            .expect("retry failed");
        let lease = claim(&broker).await;
        assert_eq!(lease.task.attempts, 1);
        broker.fail(&lease, failure()).await.expect("fail failed");

        broker.requeue(id).await.expect("requeue failed");

        let requeued = claim(&broker).await;
        assert_eq!(requeued.task.id, id);
        assert_eq!(requeued.task.attempts, 0, "requeue must reset the counts");
        assert!(
            get(&broker, id).await.failure.is_none(),
            "requeue must clear the failure"
        );
    }

    async fn requeue_rejects_a_task_that_has_not_failed(&self) {
        let broker = self.broker();
        let id = enqueue(&broker, spec()).await;

        assert!(matches!(
            broker.requeue(id).await,
            Err(CoreErr::UnexpectedState { .. })
        ));
        assert!(matches!(
            broker.requeue(TaskId::new_v4()).await,
            Err(CoreErr::TaskNotFound(_))
        ));
    }

    async fn list_filters_by_state_and_queue(&self) {
        let broker = self.broker();
        enqueue(&broker, spec().queue("alpha")).await;
        enqueue(&broker, spec().queue("alpha")).await;
        enqueue(&broker, spec().queue("beta")).await;

        let alpha = broker
            .list(&ListFilter::default().queue("alpha"))
            .await
            .expect("list failed");
        assert_eq!(alpha.len(), 2);
        assert!(alpha.iter().all(|record| record.task.queue == "alpha"));

        let pending = broker
            .list(&ListFilter::default().state(TaskState::Pending))
            .await
            .expect("list failed");
        assert_eq!(pending.len(), 3);

        let capped = broker
            .list(&ListFilter::default().limit(2))
            .await
            .expect("list failed");
        assert_eq!(capped.len(), 2, "a listing must respect its limit");
    }

    async fn get_returns_nothing_for_an_unknown_id(&self) {
        let broker = self.broker();
        assert!(
            broker
                .get(TaskId::new_v4())
                .await
                .expect("get failed")
                .is_none(),
            "an unknown id must return nothing rather than an error"
        );
    }
}

fn spec() -> TaskSpec {
    TaskSpec::new(DEFINITION.into(), serde_json::Value::Null)
}

fn failure() -> FailureRecord {
    FailureRecord::new(FailureKind::Manual, "conformance")
}

async fn enqueue(broker: &impl TaskBroker, spec: TaskSpec) -> TaskId {
    broker.enqueue(spec).await.expect("enqueue failed")
}

async fn claim(broker: &impl TaskBroker) -> Lease {
    let filter = ClaimFilter::any();
    tokio::time::timeout(PATIENCE, broker.claim(&filter))
        .await
        .expect("timed out waiting for a task that should have been claimable")
        .expect("claim failed")
}

async fn claim_times_out(broker: &impl TaskBroker, within: Duration) -> bool {
    let filter = ClaimFilter::any();
    tokio::time::timeout(within, broker.claim(&filter))
        .await
        .is_err()
}

async fn get(broker: &impl TaskStore, id: TaskId) -> crate::broker::TaskRecord {
    broker
        .get(id)
        .await
        .expect("get failed")
        .expect("the backend should still hold this task")
}
