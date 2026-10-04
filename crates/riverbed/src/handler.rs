//! Task handler types.

use serde::{Serialize, de::DeserializeOwned};

use crate::{completion::Completion, error::LibResult, execution::ExecutionContext};
use std::{error::Error, marker::PhantomData, pin::Pin};

/// A future returned from an execution handler
pub(crate) type HandlerFut = Pin<Box<dyn Future<Output = Result<Completion, HandlerError>> + Send>>;

pub(crate) trait THandler: Send + Sync {
    fn call(&self, ctx: ExecutionContext, payload: serde_json::Value) -> LibResult<HandlerFut>;
}

pub(crate) struct TypedHandler<T, R, F> {
    handler: F,
    _payload: PhantomData<fn(T) -> R>,
}

impl<T, R, F> TypedHandler<T, R, F> {
    pub(crate) fn new(handler: F) -> Self {
        Self {
            handler,
            _payload: PhantomData,
        }
    }
}

impl<T, R, F, Fut> THandler for TypedHandler<T, R, F>
where
    T: Serialize + DeserializeOwned,
    R: Into<Completion> + Send + 'static,
    F: Fn(T, ExecutionContext) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<R, HandlerError>> + Send + 'static,
{
    fn call(&self, ctx: ExecutionContext, payload: serde_json::Value) -> LibResult<HandlerFut> {
        let payload = serde_json::from_value::<T>(payload)?;
        let fut = (self.handler)(payload, ctx);
        Ok(Box::pin(async move { fut.await.map(Into::into) }))
    }
}

/// Error returned by a task handler.
///
/// Handler errors are treated as transient: the task is retried until its attempt limit, from the
/// task itself or the engine's [`RetryPolicy`](crate::retry::RetryPolicy), allows no more.
pub type HandlerError = Box<dyn Error + Send + Sync>;
