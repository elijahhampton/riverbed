use crate::engine::Engine;
use std::sync::Arc;

#[derive(Clone)]
pub struct State {
    pub engine: Arc<Engine>,
}
