# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.0] - 2026-05-03

### Added
- LabelBasedScheduler for lightweight, TaskIQ-native scheduling
- PipelineTrackingManager for execution tracking
- Redis storage backend for tracking
- PostgreSQL storage backend for tracking
- SQLite storage backend for tracking
- Memory storage backend for tracking
- WebSocket transport middleware for real-time events
- HTTP stream transport middleware (stub)
- Redis pub/sub transport middleware (stub)
- DAG visualization (JSON, DOT, NetworkX, ASCII)
- REST API for pipeline management and visualization
- Map-reduce with intelligent chunking
- Adaptive chunking strategies
- Progress callbacks
- Parameter sweep functionality
- Structured logging with pipeline context
- Hook system for event handling
- Granular retry system with exponential backoff
- Error handling modes (fail_fast, continue_on_error, skip_failed)
- Dead letter queue support
- Pipeline middleware system
- DataCache with automatic dependency injection
- Execution engine with parallel processing

### Changed
- Improved import organization
- Fixed type annotations throughout the codebase
- Enhanced error messages
- Updated documentation with comprehensive examples

### Fixed
- 63 ruff linting errors
- Type checking errors in scheduler module
- Test failures in tracking factory
- Missing exports for PipelineTrackingManager and LabelBasedScheduler

### Deprecated
- Nothing

### Removed
- Nothing

## [0.2.0] - 2026-04-15

### Added
- Initial release
- Pipeline execution engine
- DAG construction and validation
- Basic scheduling with APScheduler
- WebSocket event system
- Basic tracking system
- Map-reduce operations
- DataCache implementation

### Changed
- Migrated from taskiq-pipelines
- Improved API design
- Better error handling

### Fixed
- Various bugs from initial implementation

## [0.1.0] - 2026-03-01

### Added
- Initial prototype
- Basic pipeline functionality
- Simple scheduling
- Basic tracking

[0.3.0]: https://github.com/username/taskiq-flow/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/username/taskiq-flow/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/username/taskiq-flow/releases/tag/v0.1.0