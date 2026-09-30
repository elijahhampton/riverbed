use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::Serialize;
use tracing::error;

use crate::error::CoreErr;

// Create our own JSON extractor by wrapping `axum::Json`. This makes it easy to override the
// rejection and provide our own which formats errors to match our application.
//
// `axum::Json` responds with plain text if the input is invalid.
pub struct AppJson<T>(pub T);

impl<T> IntoResponse for AppJson<T>
where
    axum::Json<T>: IntoResponse,
{
    fn into_response(self) -> Response {
        axum::Json(self.0).into_response()
    }
}

#[derive(Debug)]
pub enum ApiError {
    ServiceError(CoreErr),
}

#[derive(Serialize)]
struct ErrorResponse {
    status_code: u16,
    message: String,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        let ApiError::ServiceError(core_err) = &self;
        error!(
            error = core_err as &dyn std::error::Error,
            "API request failed"
        );

        let (message, status_code) = match self {
            ApiError::ServiceError(core_err) => match core_err {
                CoreErr::HandlerNotFound(e) => (e, StatusCode::INTERNAL_SERVER_ERROR),
                CoreErr::DuplicateHandler(e) => (e, StatusCode::INTERNAL_SERVER_ERROR),
                CoreErr::Payload(error) => (error.to_string(), StatusCode::BAD_REQUEST),
                CoreErr::HandlerFailed(error) => {
                    (error.to_string(), StatusCode::INTERNAL_SERVER_ERROR)
                }
                CoreErr::LeaseNotFound(_) => (
                    "task not found".to_owned(),
                    StatusCode::INTERNAL_SERVER_ERROR,
                ),
                CoreErr::Broker(error) => (error.to_string(), StatusCode::INTERNAL_SERVER_ERROR),
            },
        };

        let body = AppJson(ErrorResponse {
            status_code: status_code.as_u16(),
            message,
        });
        (status_code, body).into_response()
    }
}

pub type ApiResponse<T> = Result<T, ApiError>;
