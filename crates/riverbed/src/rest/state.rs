use crate::broker::TaskStore;
use crate::engine::Engine;
use std::sync::Arc;

#[derive(Clone)]
pub struct State {
    pub engine: Arc<Engine>,
    /// Backs the read endpoints. Absent when the caller only wants to enqueue.
    pub store: Option<Arc<dyn TaskStore>>,
}
