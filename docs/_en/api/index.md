---
title: API Reference
nav_order: 35
permalink: /en/api/
color_scheme: dark
---
# API Reference

Complete module and class documentation for Taskiq-Flow.

## Available API Docs

| Module | Description |
|--------|-------------|
| **[Core Components]({{ '/en/api/core/' | relative_url }})** | Pipeline, DataflowPipeline, middleware, exceptions |
| **[Decorators]({{ '/en/api/decorators/' | relative_url }})** | `@pipeline_task` and utilities |
| **[Execution]({{ '/en/api/execution/' | relative_url }})** | ExecutionEngine, DAG, DAGBuilder |
| **[Storage]({{ '/en/api/storage/' | relative_url }})** new in v1.2.0 | Pluggable storage adapters (InMemory, Redis, SQLite), factory, StorageMiddleware |
| **[Cache]({{ '/en/api/cache/' | relative_url }})** new in v1.2.0 | Dogpile-based caching (InMemory, Redis adapters), CacheMiddleware |
| **[Tracking]({{ '/en/api/tracking/' | relative_url }})** | TrackingManager and storage backends |
| **[Optimization]({{ '/en/api/optimization/' | relative_url }})** | ResourceAwareExecutor |
| **[WebSocket]({{ '/en/api/websocket/' | relative_url }})** | HookManager and event system |

---

*Not sure where to start? See the [Quick Start]({{ '/en/quickstart/' | relative_url }}) or [User Guides]({{ '/en/guides/' | relative_url }}).*
