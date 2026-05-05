# Taskiq-Flow Documentation

> **Version**: 0.3.2 | Last updated: 2026-05-05

Welcome to the official documentation for **Taskiq-Flow**, a powerful Python library for orchestrating asynchronous task workflows with pipelines, dataflow DAGs, real-time tracking, and distributed scheduling.

## 🌐 Language Selection

This documentation is available in two languages:

- **[🇬🇧 English Documentation](/docs/en/)** — Complete technical documentation in English (source language)
- **[🇫🇷 Documentation Française](/docs/fr/)** — Traduction française complète

Both versions are kept synchronized, with code examples remaining in English for consistency.

## 📚 Documentation Structure

The documentation is organized into the following sections:

### Getting Started
- **[Quick Start](/docs/en/quickstart.md)** — 5-minute tutorial to run your first pipeline
- **Installation** — Setup instructions and configuration
- **Core Concepts** — Understanding pipeline types and patterns

### User Guides
- **[Pipelines](/docs/en/guides/pipelines.md)** — Sequential and dataflow pipeline patterns
- **[Tasks](/docs/en/guides/tasks.md)** — Task definition, decorators, and metadata
- **[Execution](/docs/en/guides/execution.md)** — Execution models and error handling
- **[Tracking & Monitoring](/docs/en/guides/tracking.md)** — Real-time progress and status
- **[WebSocket](/docs/en/guides/websocket.md)** — Live event streaming
- **[Scheduling](/docs/en/guides/scheduling.md)** — Cron-based pipeline scheduling
- **[Retry](/docs/en/guides/retry.md)** — Error recovery and retry strategies
- **[Performance](/docs/en/guides/performance.md)** — Optimization and scaling
- **[API (REST)](/docs/en/guides/api.md)** — FastAPI integration and endpoints

### API Reference
- **[Core API](/docs/en/api/core.md)** — Pipeline, DataflowPipeline, middleware
- **[Decorators](/docs/en/api/decorators.md)** — @pipeline_task and utilities
- **[Execution](/docs/en/api/execution.md)** — ExecutionEngine, DAG, DAGBuilder
- **[Tracking](/docs/en/api/tracking.md)** — TrackingManager and storage backends
- **[WebSocket](/docs/en/api/websocket.md)** — HookManager and event system

### Examples
- **[Example Gallery](/docs/en/examples/)** — Walkthroughs of all example scripts
  - Basic pipeline
  - Tracking demo
  - Scheduled pipeline
  - Dataflow audio pipeline
  - Registry discovery
  - WebSocket demo
  - REST API

## 🚀 Quick Overview

Taskiq-Flow combines **taskiq-pipelines' orchestration** with **pipefunc's dataflow model**:

- **Sequential Pipelines** — Linear workflows with `.call_next()`, `.map()`, `.filter()`, `.group()`
- **Dataflow Pipelines** — Automatic DAG construction from task dependencies using `@pipeline_task`
- **Real-time Tracking** — Monitor execution with PipelineTrackingManager
- **WebSocket Streaming** — Live events for dashboard integration
- **Scheduling** — Cron-based pipeline execution with APScheduler
- **REST API** — FastAPI endpoints for remote management
- **Parallel Execution** — Automatic concurrency for independent tasks
- **Map-Reduce** — Built-in batch processing helpers

## 📖 Read the Guides

**[→ Getting Started (English)](/docs/en/quickstart.md)**

**[→ Commencer (Français)](/docs/fr/quickstart.md)**

## 🔗 Quick Links

- **Project Repository**: [GitHub - taskiq-flow](https://github.com/your-repo/taskiq-flow)
- **PyPI Package**: [taskiq-flow](https://pypi.org/project/taskiq-flow/)
- **Taskiq Documentation**: [taskiq.readthedocs.io](https://taskiq.readthedocs.io/)
- **Issue Tracker**: [GitHub Issues](https://github.com/your-repo/taskiq-flow/issues)

## 🤝 Contributing

Contributions are welcome! Please read our [contributing guide](../CONTRIBUTING.md) for details on how to submit pull requests, add features, or report bugs.

## 📄 License

This project is licensed under the MIT License — see the [LICENSE](../LICENSE) file for details.

---

*Maintained by SoniqueBay Team · Documentation last built: 2026-05-05*
