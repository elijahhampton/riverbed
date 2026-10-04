//! gRPC server for the task service.

use std::error::Error;
use std::net::SocketAddr;
use std::sync::Arc;

use tonic::{Request, Response, Status};
use tracing::{debug, error, info};

use crate::broker::TaskStore;
use crate::engine::Engine;
use crate::error::CoreErr;
use crate::grpc::proto::task_service_server::{TaskService, TaskServiceServer};
use crate::grpc::proto::{
    EnqueueTaskRequest, EnqueueTaskResponse, GetTaskRequest, GetTaskResponse, TaskFailure,
};
use crate::task::{TaskId, TaskSpec};

/// Implementation of `riverbed.v1.TaskService` that enqueues tasks into an [`Engine`].
///
/// Use this to mount the service in an existing tonic server. Otherwise, use [`serve`].
pub struct GrpcTaskService {
    engine: Arc<Engine>,
    store: Option<Arc<dyn TaskStore>>,
}

impl GrpcTaskService {
    /// Creates a service that enqueues tasks into `engine`.
    ///
    /// Without a store, `GetTask` reports `Unimplemented`: a backend can be written to without
    /// being queryable.
    pub fn new(engine: Arc<Engine>) -> Self {
        Self {
            engine,
            store: None,
        }
    }

    /// Creates a service whose `GetTask` reads from `store`.
    pub fn with_store(engine: Arc<Engine>, store: Arc<dyn TaskStore>) -> Self {
        Self {
            engine,
            store: Some(store),
        }
    }

    /// Wraps the service so it can be added to a [`tonic::transport::Server`].
    pub fn into_server(self) -> TaskServiceServer<Self> {
        TaskServiceServer::new(self)
    }
}

#[tonic::async_trait]
impl TaskService for GrpcTaskService {
    async fn enqueue_task(
        &self,
        request: Request<EnqueueTaskRequest>,
    ) -> Result<Response<EnqueueTaskResponse>, Status> {
        let EnqueueTaskRequest {
            category,
            payload,
            queue,
            max_attempts,
            dedup_key,
        } = request.into_inner();
        let payload = serde_json::from_slice(&payload).map_err(|err| {
            debug!(
                error = &err as &dyn Error,
                "rejected task: payload is not valid JSON"
            );
            Status::invalid_argument(format!("payload is not valid JSON: {err}"))
        })?;

        let mut spec = TaskSpec::new(category, payload);
        if !queue.is_empty() {
            spec = spec.queue(queue);
        }
        if max_attempts > 0 {
            spec = spec.max_attempts(max_attempts);
        }
        if !dedup_key.is_empty() {
            spec = spec.dedup_key(dedup_key);
        }

        let task_id = self.engine.enqueue(spec).await.map_err(|err| {
            error!(error = &err as &dyn Error, "failed to enqueue task");
            Status::from(err)
        })?;

        Ok(Response::new(EnqueueTaskResponse {
            task_id: task_id.to_string(),
        }))
    }

    async fn get_task(
        &self,
        request: Request<GetTaskRequest>,
    ) -> Result<Response<GetTaskResponse>, Status> {
        let Some(store) = self.store.as_ref() else {
            return Err(Status::unimplemented("this service has no task store"));
        };

        let task_id: TaskId = request
            .into_inner()
            .task_id
            .parse()
            .map_err(|_| Status::invalid_argument("task_id is not a UUID"))?;

        let record = store
            .get(task_id)
            .await
            .map_err(|err| {
                error!(error = &err as &dyn Error, "failed to read task");
                Status::from(err)
            })?
            .ok_or_else(|| Status::not_found("no task with that id"))?;

        Ok(Response::new(GetTaskResponse {
            task_id: record.task.id.to_string(),
            category: record.task.definition,
            queue: record.task.queue,
            state: record.state.as_str().to_owned(),
            attempts: record.task.attempts,
            reclaims: record.task.reclaims,
            result: record
                .result
                .map(|result| serde_json::to_vec(&result).unwrap_or_default())
                .unwrap_or_default(),
            failure: record.failure.map(|failure| TaskFailure {
                kind: failure.kind.as_str().to_owned(),
                detail: failure.detail,
                attempts: failure.attempts,
            }),
        }))
    }
}

impl From<CoreErr> for Status {
    fn from(err: CoreErr) -> Self {
        match err {
            CoreErr::Payload(err) => Status::invalid_argument(err.to_string()),
            CoreErr::TaskNotFound(_) => Status::not_found(err.to_string()),
            CoreErr::UnexpectedState { .. } | CoreErr::LeaseFenced(_) => {
                Status::failed_precondition(err.to_string())
            }
            err => Status::internal(err.to_string()),
        }
    }
}

/// Serves the task service on `addr` until the future is dropped or the server fails.
pub async fn serve(engine: Arc<Engine>, addr: SocketAddr) -> Result<(), tonic::transport::Error> {
    info!(address = %addr, "starting gRPC server");
    tonic::transport::Server::builder()
        .add_service(GrpcTaskService::new(engine).into_server())
        .serve(addr)
        .await
}
