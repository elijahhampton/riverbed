use std::sync::Arc;
use tokio::sync::Mutex;
use crate::engine::{Engine, EngineBuilder};

#[derive(Clone)]
pub struct State {
    pub engine: Arc<Engine>
}

impl State {
    pub fn new() -> Self {
        let engine_builder = EngineBuilder::default();
        let engine = Arc::new(engine_builder.build());

        let engine_clone = engine.clone();
        tokio::spawn(async move { engine_clone.start(); });

        Self {
            engine
        }
    }
}