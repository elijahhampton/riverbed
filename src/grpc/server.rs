use std::error::Error;
use std::net::SocketAddr;
use std::sync::Arc;

use tonic::{Request, Response, Status};
use tracing::{debug, error, info};

use crate::engine::Engine;
use crate::error::CoreErr;
use crate::grpc::proto::task_service_server::{TaskService, TaskServiceServer};
use crate::grpc::proto::{EnqueueTaskRequest, EnqueueTaskResponse};

pub struct GrpcTaskService {
    engine: Arc<Engine>,
}

impl GrpcTaskService {
    pub fn new(engine: Arc<Engine>) -> Self {
        Self { engine }
    }

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
        let EnqueueTaskRequest { category, payload } = request.into_inner();
        let payload = serde_json::from_slice(&payload).map_err(|err| {
            debug!(error = &err as &dyn Error, "rejected task: payload is not valid JSON");
            Status::invalid_argument(format!("payload is not valid JSON: {err}"))
        })?;

        let task_id = self
            .engine
            .enqueue_task(category, payload)
            .await
            .map_err(|err| {
                error!(error = &err as &dyn Error, "failed to enqueue task");
                Status::from(err)
            })?;

        Ok(Response::new(EnqueueTaskResponse {
            task_id: task_id.to_string(),
        }))
    }
}

impl From<CoreErr> for Status {
    fn from(err: CoreErr) -> Self {
        match err {
            CoreErr::Payload(err) => Status::invalid_argument(err.to_string()),
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
