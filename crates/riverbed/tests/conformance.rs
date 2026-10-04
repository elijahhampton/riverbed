//! Runs the broker conformance suite against the in-memory broker.

use riverbed::{
    broker::MemoryBroker,
    testing::{Conformance, StoreConformance},
};
use std::time::Duration;

/// Short enough that the lease-expiry cases stay quick, long enough that a loaded CI runner does
/// not expire a lease the suite expects to still be held.
const LEASE: Duration = Duration::from_millis(200);

#[tokio::test(flavor = "multi_thread")]
async fn memory_broker_conforms() {
    Conformance::new(|| MemoryBroker::new().with_lease_duration(LEASE))
        .lease_duration(LEASE)
        .run()
        .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn memory_broker_store_conforms() {
    StoreConformance::new(|| MemoryBroker::new().with_lease_duration(LEASE))
        .run()
        .await;
}
