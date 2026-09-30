# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `Engine` with bounded concurrency, retries with exponential backoff, and panic isolation.
- `TaskBroker` trait for custom storage backends.
- `MemoryBroker`, enabled by the default `in-memory` feature.
- HTTP/JSON API for enqueuing tasks, behind the `rest` feature.
- gRPC API for enqueuing tasks, behind the `grpc` feature.

[Unreleased]: https://github.com/elijahhampton/riverbed/commits/main
