//! Cancellation and per-task options: a handler that observes cancellation is released rather
//! than failed, a task that runs past its timeout consumes an attempt, and a task's own options
//! beat the engine's defaults.

use riverbed::{
    broker::{ListFilter, MemoryBroker, TaskStore},
    completion::FailureKind,
    engine::Engine,
    execution::ExecutionContext,
    handler::HandlerError,
    retry::RetryPolicy,
    task::{TaskSpec, TaskState},
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::Semaphore;

const TASK: &str = "cancel:task";

fn broker() -> Arc<MemoryBroker> {
    Arc::new(MemoryBroker::new().with_lease_duration(Duration::from_secs(30)))
}

#[tokio::test(flavor = "multi_thread")]
async fn a_handler_that_observes_cancellation_is_released_not_failed() {
    let broker = broker();
    let started = Arc::new(Semaphore::new(0));

    let engine = Engine::builder()
        .broker(Arc::clone(&broker))
        .register_task(TASK.into(), {
            let started = Arc::clone(&started);
            move |_: (), ctx: ExecutionContext| {
                let started = Arc::clone(&started);
                async move {
                    started.add_permits(1);
                    ctx.cancelled().await;
                    // Returning an error after cancellation is how a handler reports that it
                    // stopped without finishing.
                    Err::<(), HandlerError>(HandlerError::from("cancelled"))
                }
            }
        })
        .expect("handler registration failed")
        .build();

    let id = engine
        .enqueue(TaskSpec::new(TASK.into(), serde_json::Value::Null))
        .await
        .expect("enqueue failed");
    engine.start();
    let _ = started.acquire().await.expect("semaphore is never closed");

    // Shutting down cancels every running handler.
    engine.shutdown(Duration::from_secs(5)).await;

    let record = broker
        .get(id)
        .await
        .expect("get failed")
        .expect("the task should still be held");
    assert_eq!(
        record.state,
        TaskState::Pending,
        "a cancelled handler's task goes back to the queue"
    );
    assert_eq!(
        record.task.attempts, 0,
        "cancellation must not consume an attempt"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_task_that_runs_past_its_timeout_fails_with_that_reason() {
    let broker = broker();
    let runs = Arc::new(AtomicUsize::new(0));

    let engine = Engine::builder()
        .broker(Arc::clone(&broker))
        .retry_policy(RetryPolicy {
            max_attempts: 2,
            base_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(5),
            jitter: true,
        })
        .register_task(TASK.into(), {
            let runs = Arc::clone(&runs);
            move |_: (), ctx: ExecutionContext| {
                let runs = Arc::clone(&runs);
                async move {
                    runs.fetch_add(1, Ordering::SeqCst);
                    ctx.cancelled().await;
                    Err::<(), HandlerError>(HandlerError::from("timed out"))
                }
            }
        })
        .expect("handler registration failed")
        .build();

    let id = engine
        .enqueue(
            TaskSpec::new(TASK.into(), serde_json::Value::Null).timeout(Duration::from_millis(100)),
        )
        .await
        .expect("enqueue failed");
    engine.start();

    let failure = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let record = broker.get(id).await.expect("get failed");
            if let Some(record) = record
                && record.state == TaskState::Failed
            {
                return record.failure.expect("a failed task retains its reason");
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the task never failed");

    engine.shutdown(Duration::from_secs(5)).await;

    assert_eq!(failure.kind, FailureKind::Timeout);
    assert_eq!(
        runs.load(Ordering::SeqCst),
        2,
        "a timeout consumes an attempt, so it retries until the limit"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_tasks_own_attempt_limit_beats_the_engine_default() {
    let broker = broker();
    let runs = Arc::new(AtomicUsize::new(0));

    let engine = Engine::builder()
        .broker(Arc::clone(&broker))
        .retry_policy(RetryPolicy {
            max_attempts: 10,
            base_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(5),
            jitter: true,
        })
        .register_task(TASK.into(), {
            let runs = Arc::clone(&runs);
            move |_: (), _: ExecutionContext| {
                let runs = Arc::clone(&runs);
                async move {
                    runs.fetch_add(1, Ordering::SeqCst);
                    Err::<(), HandlerError>(HandlerError::from("always fails"))
                }
            }
        })
        .expect("handler registration failed")
        .build();

    let id = engine
        .enqueue(TaskSpec::new(TASK.into(), serde_json::Value::Null).max_attempts(2))
        .await
        .expect("enqueue failed");
    engine.start();

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(record) = broker.get(id).await.expect("get failed")
                && record.state == TaskState::Failed
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the task never failed");

    engine.shutdown(Duration::from_secs(5)).await;
    assert_eq!(
        runs.load(Ordering::SeqCst),
        2,
        "the task's own limit of 2 must beat the engine's 10"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn queues_are_served_by_independent_pools() {
    let broker = broker();
    let slow_started = Arc::new(Semaphore::new(0));
    let fast_ran = Arc::new(AtomicUsize::new(0));

    let engine = Engine::builder()
        .broker(Arc::clone(&broker))
        .queue_concurrency("slow", 1)
        .queue_concurrency("fast", 1)
        .register_task_on("slow", "slow:task".into(), {
            let slow_started = Arc::clone(&slow_started);
            move |_: (), ctx: ExecutionContext| {
                let slow_started = Arc::clone(&slow_started);
                async move {
                    slow_started.add_permits(1);
                    ctx.cancelled().await;
                    Err::<(), HandlerError>(HandlerError::from("cancelled"))
                }
            }
        })
        .expect("handler registration failed")
        .register_task_on("fast", "fast:task".into(), {
            let fast_ran = Arc::clone(&fast_ran);
            move |_: (), _: ExecutionContext| {
                let fast_ran = Arc::clone(&fast_ran);
                async move {
                    fast_ran.fetch_add(1, Ordering::SeqCst);
                    Ok::<(), HandlerError>(())
                }
            }
        })
        .expect("handler registration failed")
        .build();

    // The slow queue's only worker is occupied before the fast task is enqueued.
    engine
        .enqueue(TaskSpec::new("slow:task".into(), serde_json::Value::Null).queue("slow"))
        .await
        .expect("enqueue failed");
    engine.start();
    let _ = slow_started
        .acquire()
        .await
        .expect("semaphore is never closed");

    engine
        .enqueue(TaskSpec::new("fast:task".into(), serde_json::Value::Null).queue("fast"))
        .await
        .expect("enqueue failed");

    tokio::time::timeout(Duration::from_secs(5), async {
        while fast_ran.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("a blocked queue must not starve another");

    engine.shutdown(Duration::from_secs(5)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_completion_stores_a_result_and_spawns_follow_up_work() {
    let broker = broker();

    let engine = Engine::builder()
        .broker(Arc::clone(&broker))
        .register_task("parent".into(), |_: (), _: ExecutionContext| async {
            Ok::<_, HandlerError>(
                riverbed::completion::Completion::empty()
                    .result(serde_json::json!({ "ok": true }))
                    .spawn(TaskSpec::new("child".into(), serde_json::Value::Null)),
            )
        })
        .expect("handler registration failed")
        .register_task("child".into(), |_: (), _: ExecutionContext| async {
            Ok::<(), HandlerError>(())
        })
        .expect("handler registration failed")
        .build();

    let parent = engine
        .enqueue(TaskSpec::new("parent".into(), serde_json::Value::Null))
        .await
        .expect("enqueue failed");
    engine.start();

    let result = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(record) = broker.get(parent).await.expect("get failed")
                && record.state == TaskState::Completed
            {
                return record.result;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the parent never completed");

    assert_eq!(result, Some(serde_json::json!({ "ok": true })));

    // The child was enqueued by the parent's acknowledgement and ran on its own.
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let children = broker
                .list(&ListFilter::default())
                .await
                .expect("list failed");
            if children
                .iter()
                .any(|r| r.task.definition == "child" && r.state == TaskState::Completed)
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the spawned child never ran");

    engine.shutdown(Duration::from_secs(5)).await;
}
