use serde::{Serialize, de::DeserializeOwned};

use crate::{error::LibResult, execution::ExecutionContext};
use std::{error::Error, marker::PhantomData, pin::Pin};

/// A future returned from an execution handler
pub(crate) type HandlerFut = Pin<Box<dyn Future<Output = Result<(), HandlerError>> + Send>>;

pub(crate) trait THandler: Send + Sync {
    fn call(&self, ctx: ExecutionContext, payload: serde_json::Value) -> LibResult<HandlerFut>;
}

pub(crate) struct TypedHandler<T, F> {
    handler: F,
    _payload: PhantomData<fn(T)>,
}

impl<T, F> TypedHandler<T, F> {
    pub(crate) fn new(handler: F) -> Self {
        Self {
            handler,
            _payload: PhantomData,
        }
    }
}

impl<T, F, Fut> THandler for TypedHandler<T, F>
where
    T: Serialize + DeserializeOwned,
    F: Fn(T, ExecutionContext) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), HandlerError>> + Send + 'static,
{
    fn call(&self, ctx: ExecutionContext, payload: serde_json::Value) -> LibResult<HandlerFut> {
        let payload = serde_json::from_value::<T>(payload)?;
        Ok(Box::pin((self.handler)(payload, ctx)))
    }
}

pub type HandlerError = Box<dyn Error + Send + Sync>;
