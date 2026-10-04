//! Graceful shutdown: the engine stops claiming, drains what is running, and hands back any lease
//! still held when the deadline passes.

use riverbed::{
    broker::{ClaimFilter, MemoryBroker, TaskBroker},
    engine::Engine,
    execution::ExecutionContext,
    handler::HandlerError,
    task::TaskSpec,
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::Semaphore;

const TASK: &str = "shutdown:task";
const LEASE: Duration = Duration::from_secs(30);

fn broker() -> Arc<MemoryBroker> {
    Arc::new(MemoryBroker::new().with_lease_duration(LEASE))
}

async fn enqueue(broker: &Arc<MemoryBroker>, count: usize) {
    for _ in 0..count {
        let spec = TaskSpec::new(TASK.into(), serde_json::Value::Null);
        broker.enqueue(spec).await.expect("enqueue failed");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn shutdown_waits_for_in_flight_handlers() {
    let broker = broker();
    let finished = Arc::new(AtomicUsize::new(0));
    // A semaphore rather than a `Notify`, which keeps only one permit and would lose the rest.
    let started = Arc::new(Semaphore::new(0));

    let engine = Engine::builder()
        .broker(Arc::clone(&broker))
        .concurrency(4)
        .register_task(TASK.into(), {
            let finished = Arc::clone(&finished);
            let started = Arc::clone(&started);
            move |_: (), _: ExecutionContext| {
                let finished = Arc::clone(&finished);
                let started = Arc::clone(&started);
                async move {
                    started.add_permits(1);
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    finished.fetch_add(1, Ordering::SeqCst);
                    Ok::<(), HandlerError>(())
                }
            }
        })
        .expect("handler registration failed")
        .build();

    enqueue(&broker, 4).await;
    engine.start();
    let _ = started
        .acquire_many(4)
        .await
        .expect("semaphore is never closed");

    assert!(
        engine.shutdown(Duration::from_secs(5)).await,
        "the drain should finish well inside its timeout"
    );
    assert_eq!(
        finished.load(Ordering::SeqCst),
        4,
        "shutdown must wait for every in-flight handler"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_handler_still_running_at_the_deadline_has_its_lease_released() {
    let broker = broker();
    let started = Arc::new(Semaphore::new(0));

    let engine = Engine::builder()
        .broker(Arc::clone(&broker))
        .concurrency(1)
        .register_task(TASK.into(), {
            let started = Arc::clone(&started);
            move |_: (), _: ExecutionContext| {
                let started = Arc::clone(&started);
                async move {
                    started.add_permits(1);
                    // Never returns, so the drain can only end at its deadline.
                    std::future::pending::<()>().await;
                    Ok::<(), HandlerError>(())
                }
            }
        })
        .expect("handler registration failed")
        .build();

    enqueue(&broker, 1).await;
    engine.start();
    let _ = started.acquire().await.expect("semaphore is never closed");

    assert!(
        !engine.shutdown(Duration::from_millis(200)).await,
        "a handler that never returns must make the drain time out"
    );

    // Released rather than left to expire, so the task is claimable long before its lease would
    // have run out, and with its attempt count untouched.
    let reclaimed = tokio::time::timeout(Duration::from_secs(1), broker.claim(&ClaimFilter::any()))
        .await
        .expect("the lease should have been released at the deadline")
        .expect("claim failed");
    assert_eq!(
        reclaimed.task.attempts, 0,
        "a drain must not consume an attempt"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn no_task_is_lost_across_a_shutdown() {
    let broker = broker();
    let ran = Arc::new(AtomicUsize::new(0));

    let engine = Engine::builder()
        .broker(Arc::clone(&broker))
        .concurrency(2)
        .register_task(TASK.into(), {
            let ran = Arc::clone(&ran);
            move |_: (), _: ExecutionContext| {
                let ran = Arc::clone(&ran);
                async move {
                    ran.fetch_add(1, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    Ok::<(), HandlerError>(())
                }
            }
        })
        .expect("handler registration failed")
        .build();

    enqueue(&broker, 20).await;
    engine.start();
    tokio::time::sleep(Duration::from_millis(60)).await;
    engine.shutdown(Duration::from_secs(5)).await;

    let completed = ran.load(Ordering::SeqCst);
    let mut left = 0;
    while tokio::time::timeout(Duration::from_millis(50), broker.claim(&ClaimFilter::any()))
        .await
        .is_ok()
    {
        left += 1;
    }

    assert_eq!(
        completed + left,
        20,
        "every task should be either done or still claimable, never lost"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn shutdown_is_safe_to_repeat_and_without_a_start() {
    let engine = Engine::builder()
        .broker(broker())
        .register_task(TASK.into(), |_: (), _: ExecutionContext| async {
            Ok::<(), HandlerError>(())
        })
        .expect("handler registration failed")
        .build();

    assert!(engine.shutdown(Duration::from_millis(50)).await);
    assert!(engine.shutdown(Duration::from_millis(50)).await);
}
