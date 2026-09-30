//! HTTP/JSON API for enqueuing tasks, built on [axum](https://docs.rs/axum).
//!
//! | Method | Path | Request body | Response |
//! |---|---|---|---|
//! | `GET` | `/v1/health` | Empty | `200 OK` with an empty body |
//! | `POST` | `/v1/task` | `{"category": "<task type>", "payload": <any JSON>}` | The task ID as a JSON string |
//!
//! When the engine rejects a request, the response carries the matching HTTP status and a body of
//! the form `{"status_code": 500, "message": "..."}`.

mod error;
mod handlers;
pub mod server;
mod state;
