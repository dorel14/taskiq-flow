---
title: Taskiq-Flow Documentation
nav_order: 1
permalink: /
---
# Taskiq-Flow Documentation

> **Version**: 0.3.2 | Last updated: 2026-05-05

Welcome to the official documentation for **Taskiq-Flow**, a powerful Python library for orchestrating asynchronous task workflows with pipelines, dataflow DAGs, real-time tracking, and distributed scheduling.

## 🌐 Language Selection

This documentation is available in two languages:

- **[🇬🇧 English Documentation]({{ '/en/' | relative_url }})** — Complete technical documentation in English (source language)
- **[🇫🇷 Documentation Française]({{ '/fr/' | relative_url }})** — Traduction française complète

Both versions are kept synchronized, with code examples remaining in English for consistency.

## 📚 Documentation Structure

The documentation is organized into the following sections:

### Getting Started
- **[Installation]({{ '/en/guides/installation/' | relative_url }})** — Setup instructions and configuration
- **[Quick Start]({{ '/en/quickstart/' | relative_url }})** — 5-minute tutorial to run your first pipeline
- **[Core Concepts]({{ '/en/guides/core-concepts/' | relative_url }})** — Understanding pipeline types and patterns

### User Guides
- **[Pipelines]({{ '/en/guides/pipelines/' | relative_url }})** — Sequential and dataflow pipeline patterns
- **[Tasks]({{ '/en/guides/tasks/' | relative_url }})** — Task definition, decorators, and metadata
- **[Execution]({{ '/en/guides/execution/' | relative_url }})** — Execution models and error handling
- **[Tracking & Monitoring]({{ '/en/guides/tracking/' | relative_url }})** — Real-time progress and status
- **[WebSocket]({{ '/en/guides/websocket/' | relative_url }})** — Live event streaming
- **[Scheduling]({{ '/en/guides/scheduling/' | relative_url }})** — Cron-based pipeline scheduling
- **[Retry]({{ '/en/guides/retry/' | relative_url }})** — Error recovery and retry strategies
- **[Performance]({{ '/en/guides/performance/' | relative_url }})** — Optimization and scaling
- **[API (REST)]({{ '/en/guides/api/' | relative_url }})** — FastAPI integration and endpoints

### API Reference
- **[Core API]({{ '/en/api/core/' | relative_url }})** — Pipeline, DataflowPipeline, middleware
- **[Decorators]({{ '/en/api/decorators/' | relative_url }})** — @pipeline_task and utilities
- **[Execution]({{ '/en/api/execution/' | relative_url }})** — ExecutionEngine, DAG, DAGBuilder
- **[Tracking]({{ '/en/api/tracking/' | relative_url }})** — TrackingManager and storage backends
- **[WebSocket]({{ '/en/api/websocket/' | relative_url }})** — HookManager and event system

### Examples
- **[Example Gallery]({{ '/en/examples/' | relative_url }})** — Walkthroughs of all example scripts
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

**[→ Getting Started (English)]({{ '/en/quickstart/' | relative_url }})**

**[→ Commencer (Français)]({{ '/fr/quickstart/' | relative_url }})**

## 🔗 Quick Links

- **Project Repository**: [GitHub - taskiq-flow](https://github.com/dorel14/taskiq-flow)
- **PyPI Package**: [taskiq-flow](https://pypi.org/project/taskiq-flow/)
- **Taskiq Documentation**: [taskiq-python.github.io](https://taskiq-python.github.io/)
- **Issue Tracker**: [GitHub Issues](https://github.com/dorel14/taskiq-flow/issues)

## 🤝 Contributing

Contributions are welcome! Please read our [contributing guide](https://github.com/dorel14/taskiq-flow/blob/master/CONTRIBUTING.md) for details on how to submit pull requests, add features, or report bugs.

## 📄 License

This project is licensed under the MIT License — see the [LICENSE](https://github.com/dorel14/taskiq-flow/blob/main/LICENSE) file for details.

---

*Maintained by SoniqueBay Team · Documentation last built: 2026-05-05*
