# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.1]

### Added

- Comprehensive French module docstrings across all taskiq_flow modules
- Enhanced class and method docstrings for IDE autocomplete
- Improved documentation for Pipeline, DataflowPipeline, and Steps
- Enhanced decorator documentation (@pipeline_task, @pipeline_task_multi_output)
- Completed dataflow component documentation (DAG, DAGNode, DataNode, Registry)
- Enhanced tracking system documentation (PipelineTrackingManager, models, storage)
- Improved ExecutionEngine documentation
- Added detailed examples and Args/Returns sections throughout

### Changed

- Standardized all docstrings to French format with comprehensive details
- Improved type hint consistency across public APIs

## [0.3.2] - 2026-05-04

### Added

- Comprehensive French documentation for DataflowRegistry with detailed examples
- New example: `registry_discovery_example.py` demonstrating manual registry usage
- Enhanced docstrings for DAG, DAGNode, DataNode, and DataCache classes
- Added usage examples for all dataflow components
- Improved module-level documentation consistency

### Changed

- Standardized French docstrings across all dataflow modules
- Minor documentation formatting improvements

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

[0.3.2]: https://github.com/dorel14/taskiq-flow/compare/v0.3.1...v0.3.2
[0.3.1]: https://github.com/dorel14/taskiq-flow/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/dorel14/taskiq-flow/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/dorel14/taskiq-flow/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/dorel14/taskiq-flow/releases/tag/v0.1.0

---

> 🌐 **International Documentation**: This project also provides documentation in [Français](CHANGELOG.fr.md).
