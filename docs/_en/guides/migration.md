# Migration Guide

> **Version**: {VERSION} | **From**: v0.4.5 | **To**: v1.0.2
> **Related**: [Changelog]({{ '/CHANGELOG/' | relative_url }}), [API Reference]({{ '/api/core/' | relative_url }})

---

## Overview

This guide helps you migrate from Taskiq-Flow v0.4.5 to v1.0.2. The migration involves changes to imports, API signatures, configuration, and new features.

**Breaking changes** are marked with ⚠️. Non-breaking additions are marked with ✨.

---

## 1. Package & Import Changes

### Renamed Modules

| Old Import (v0.4.5) | New Import (v1.0.2) |
|----------------------|----------------------|
| `taskiq_flow.pipeline` | `taskiq_flow.pipeliner` (original `Pipeline` class) |
| `taskiq_flow.pipeline` | `taskiq_flow.pipeline` (new: `DataflowPipeline`) |
| `taskiq_flow.decorators.pipeline_task` | `taskiq_flow.decorators.pipeline_task` (unchanged) |

### New Top-Level Exports

```python
# v1.0.2 - New imports available from taskiq_flow
from taskiq_flow import (
    DAG,
    DAGBuilder,
    DAGNode,
    DAGVisualizer,
    DataNode,
    DataflowPipeline,
    DataflowRegistry,
    ExecutionEngine,
    HookManager,
    MapReduce,
    Pipeline,  # Re-exported for backward compatibility
    PipelineMiddleware,
    PipelineScheduler,
    PipelineTrackingManager,
    TrackingStorageFactory,
    create_visualization_api,
    visualize_pipeline,
)
```

---

## 2. Configuration Changes

### ⚠️ Security Now Enabled by Default

In v0.4.5, security was optional and required explicit setup. In v1.0.2, `TaskiqFlowConfig` enables security by default:

```python
# v0.4.5 - No default security
config = {}  # No config needed

# v1.0.2 - Security enabled by default
from taskiq_flow.config import TaskiqFlowConfig

config = TaskiqFlowConfig(
    security_enabled=True,  # Default: True
    auth_provider="api_key",  # or "jwt"
    api_keys={
        "my-key": {
            "role": "admin",
            "pipeline_whitelist": ["*"],
            "permissions": ["read", "execute", "admin"],
        }
    },
)

# Optionally disable security for development
config = TaskiqFlowConfig(security_enabled=False)
```

### ✨ New Configuration Options

```python
config = TaskiqFlowConfig(
    # Security
    security_enabled=True,
    auth_provider="api_key",  # "api_key" | "jwt"
    jwt_secret="your-secret",  # pragma: allowlist secret - Required for JWT auth 
    api_keys={...},
    require_https=True,

    # Authorization
    pipeline_acls={
        "my_pipeline": {
            "read": ["admin", "viewer"],
            "execute": ["admin", "worker"],
            "admin": ["admin"],
        }
    },

    # Rate Limiting
    rate_limit_enabled=True,
    rate_limit_default="100/minute",

    # Metrics
    metrics_enabled=True,
    metrics_path="/metrics",

    # WebSocket
    websocket_require_auth=True,
    websocket_max_connections=1000,
)
```

---

## 3. Pipeline Creation

### ⚠️ `Pipeline` Class Moved

The original `Pipeline` class has been moved to `pipeliner`. The top-level `Pipeline` is now a re-export for backward compatibility.

```python
# v0.4.5
from taskiq_flow.pipeline import Pipeline

# v1.0.2 (still works via re-export)
from taskiq_flow import Pipeline
# Or explicitly:
from taskiq_flow.pipeliner import Pipeline
```

### ✨ New `DataflowPipeline`

The new `DataflowPipeline` is the recommended way to create pipelines with DAG support:

```python
# v1.0.2
from taskiq_flow import DataflowPipeline, Pipeline, pipeline_task

# Define tasks
@broker.task
@pipeline_task(output="processed_data")
async def process_data(data):
    return await transform(data)

@broker.task
@pipeline_task(output="result")
async def aggregate(data):
    return await compute(data)

# Create as Pipeline (backward compatible)
pipe = Pipeline(broker, process_data).call_next(aggregate)

# Or as DataflowPipeline (new)
from taskiq import InMemoryBroker
broker: InMemoryBroker = ...
pipeline = DataflowPipeline(broker)
pipeline.add_task(process_data)
pipeline.add_task(aggregate)
```

---

## 4. Execution Engine

### ⚠️ `ExecutionEngine` Replaces Direct Pipeline Execution

For DAG-based execution, use `ExecutionEngine`:

```python
# v0.4.5 - Sequential pipeline execution
pipe = Pipeline(broker, task_a).call_next(task_b)
result = await pipe.kiq(input_data)

# v1.0.2 - DAG-based execution with ExecutionEngine
from taskiq_flow import ExecutionEngine, DataflowPipeline

pipeline = DataflowPipeline(broker)
# ... build DAG ...

engine = ExecutionEngine(
    broker=broker,
    dag=pipeline._dag,
    max_parallel=10,
)

outputs = await engine.execute(
    inputs={"data": input_data},
    pipeline_id="my_pipeline",
)
```

### ✨ New Execution Options

```python
engine = ExecutionEngine(
    broker=broker,
    dag=dag,
    fail_fast=True,             # Stop on first error (default)
    continue_on_error=False,    # Continue despite errors
    skip_failed=False,          # Skip failed tasks
    error_mode=None,            # Override fail_fast/continue_on_error
    max_parallel=10,            # Max concurrent tasks
    resource_aware=False,       # Enable resource-aware scheduling
    adaptive_parallelism=False, # Dynamic parallelism per level
)
```

---

## 5. Middleware Changes

### ✨ `PipelineMiddleware` Now Supports Metrics

The `PipelineMiddleware` constructor accepts an optional `metrics_collector`:

```python
from taskiq_flow import PipelineMiddleware
from taskiq_flow.metrics.collector import MetricsCollector

metrics = MetricsCollector()
middleware = PipelineMiddleware(
    metrics_collector=metrics,
)

broker = InMemoryBroker().with_middlewares(middleware)
```

### ⚠️ `TransportMiddleware` Construction Changed

```python
# v0.4.5 - Implicit WebSocket
transport = TransportMiddleware()

# v1.0.2 - Explicit transport type
from taskiq_flow.middleware import TransportMiddleware

# WebSocket (default)
transport = TransportMiddleware(transport_type="websocket")

# HTTP Streaming (SSE) - NEW
transport = TransportMiddleware(transport_type="http_stream")

# Redis Pub/Sub
transport = TransportMiddleware(
    transport_type="redis_pubsub",
    redis_client=my_redis_client,
)
```

---

## 6. Metrics System

### ✨ New Prometheus Metrics

Metrics are now collected via `MetricsCollector`:

```python
from taskiq_flow import MetricsCollector

collector = MetricsCollector()  # Singleton

# Available metrics:
# - taskiq_flow_pipeline_executions_total
# - taskiq_flow_pipeline_duration_seconds
# - taskiq_flow_pipeline_steps_total
# - taskiq_flow_active_pipelines
# - taskiq_flow_task_executions_total
# - taskiq_flow_task_duration_seconds
# - taskiq_flow_task_retry_attempts_total
# - taskiq_flow_websocket_messages_total
# - taskiq_flow_sse_events_sent_total (new)
# - taskiq_flow_worker_cpu_usage_percent
# - taskiq_flow_worker_memory_usage_bytes
```

### Prometheus Endpoint

```python
from taskiq_flow.metrics.exporters.prometheus import get_metrics_endpoint

app.get("/metrics")(get_metrics_endpoint())
```

---

## 7. WebSocket Changes

### ✨ New HTTP Streaming (SSE) Transport

```python
from taskiq_flow.transport.http_stream import HTTPStreamTransport, get_http_stream_transport

transport = get_http_stream_transport()

# Get FastAPI endpoint
sse_endpoint = transport.get_sse_endpoint(pipeline_id="my_pipeline")
app.get("/events")(sse_endpoint)

# Or broadcast manually
await transport.broadcast(event)
```

### ⚠️ WebSocket Authentication Now Properly Enforced

v0.4.5 had a bypass in the WebSocket handler. v1.0.2 properly validates tokens:

```python
# Client must send auth message first
# WebSocket connection:
ws = new WebSocket("ws://localhost:8000/ws/my_pipeline")

# First message MUST be auth
ws.send(JSON.stringify({
    "action": "auth",
    "token": "your-jwt-or-api-key"
}))

# Then subscribe
ws.send(JSON.stringify({
    "action": "subscribe",
    "channel": "pipeline.my_pipeline"
}))
```

### ✨ Authorization Checks on Subscribe

```python
# Server-side ACL enforcement
if not self.authorization.can_read(pipeline_id, self.user):
    # Reject subscription
```

---

## 8. Tracking System

### ✨ Tracking Manager Improvements

```python
from taskiq_flow import PipelineTrackingManager, TrackingStorageFactory

# With Redis backend
storage = TrackingStorageFactory.create_redis_storage(redis_url="redis://localhost:6379")
tracking = PipelineTrackingManager(storage=storage)

# Or with auto-detection
tracking = PipelineTrackingManager().with_auto_storage(broker)

# Query status
status = await tracking.get_status(pipeline_id)
print(status.state)  # PipelineState.RUNNING, SUCCESS, FAILED, etc.
```

---

## 9. Security Setup

### ✨ New Security Module

```python
from taskiq_flow.security import SecurityMiddleware
from taskiq_flow.security.auth import create_auth_provider
from taskiq_flow.security.authorization import PipelineAuthorization
from taskiq_flow.security.rate_limiting import RateLimiter

# Auth Provider
auth_provider = create_auth_provider(config)

# Authorization
authorization = PipelineAuthorization(pipeline_acls=config.pipeline_acls)

# Rate Limiting
rate_limiter = RateLimiter(default_limits={
    "list_pipelines": "60/minute",
    "get_dag": "120/minute",
    "execute_pipeline": "10/minute",
})
```

---

## 10. Step-by-Step Migration Checklist

1. **Update imports** - Replace old module paths with new ones
2. **Add configuration** - Create `TaskiqFlowConfig` instance
3. **Set up security** - Configure auth provider if needed
4. **Update middleware** - Add metrics collector to `PipelineMiddleware`
5. **Choose transport** - Decide between WebSocket, SSE, or Redis Pub/Sub
6. **Test locally** - Run with `security_enabled=False` first
7. **Enable security** - Set up API keys or JWT tokens
8. **Add monitoring** - Configure Prometheus metrics endpoint
9. **Run tests** - Verify all existing tests pass
10. **Deploy** - Set environment variables for production

---

## 11. Common Issues

### Error: "No API key provided"
```
# Fix: Add API key to headers or configure auth_provider
X-API-Key: your-api-key
```

### Error: "Authorization required" on WebSocket
```python
# Fix: Send auth message before subscribing
ws.send(JSON.stringify({"action": "auth", "token": "your-token"}))
```

### Error: "Unsupported transport type"
```python
# Fix: Use valid transport type
TransportMiddleware(transport_type="websocket")  # "websocket", "http_stream", or "redis_pubsub"
```

### Metrics not showing up
```python
# Fix: Add metrics_collector to PipelineMiddleware
metrics = MetricsCollector()
middleware = PipelineMiddleware(metrics_collector=metrics)
```

---

## 12. Version Compatibility

| Feature | v0.4.5 | v1.0.0 | v1.0.2 |
|---------|--------|--------|--------|
| Basic pipeline execution | ✓ | ✓ | ✓ |
| DAG-based dataflow | ✗ | ✓ | ✓ |
| Security module | ✗ | ✓ | ✓ |
| Metrics system | ✗ | ✓ | ✓ |
| WebSocket transport | ✗ | ✓ | ✓ |
| HTTP SSE transport | ✗ | ✗ | ✓ |
| Authorization ACLs | ✗ | ✓ | ✓ |
| Rate limiting | ✗ | ✓ | ✓ |
| Resource-aware executor | ✗ | ✗ | ✓ |
| Execution engine | ✗ | ✓ | ✓ |
| Auto-configuration | ✗ | ✗ | ✓ |

---

*Migration guide created for v1.0.2 release*
