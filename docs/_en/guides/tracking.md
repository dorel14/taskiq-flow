---
title: Pipeline Tracking & Monitoring Guide
nav_order: 23
---
# Pipeline Tracking & Monitoring Guide

**Real-time and historical execution monitoring with PipelineTrackingManager**

> **Version**: {VERSION} | **Related**: [Execution Guide]({{ '/en/guides/execution/' | relative_url }}), [WebSocket Guide]({{ '/en/guides/websocket/' | relative_url }})

---

## Overview

Taskiq-Flow provides comprehensive tracking capabilities to monitor pipeline executions in real-time and historically. This guide covers:

- `PipelineTrackingManager` — Central tracking coordinator
- Storage backends (Memory, Redis)
- Status queries and history
- Pipeline metrics collection
- Hooking into step-level events

---

## 1. Quick Start

```python
from taskiq_flow import Pipeline, PipelineTrackingManager

# Initialize tracking with automatic storage selection
tracking = PipelineTrackingManager().with_auto_storage(broker)

# Attach tracking to your pipeline
pipeline = Pipeline(broker).with_tracking(tracking)

# Execute
task = await pipeline.kiq(data)
result = await task.wait_result()

# Query status
status = await tracking.get_status(pipeline.pipeline_id)
print(f"Status: {status.status}")        # COMPLETED
print(f"Steps: {len(status.steps)}")     # Number of steps executed
print(f"Duration: {status.duration_ms}ms")
```

That's the basic pattern. Let's dive deeper.

---

## 2. PipelineTrackingManager

The central component for recording and retrieving pipeline execution data.

### 2.1. Initialization

```python
from taskiq_flow import PipelineTrackingManager, InMemoryPipelineStorage, RedisPipelineStorage

# Option 1: Auto-select based on broker (recommended)
tracking = PipelineTrackingManager().with_auto_storage(broker)
# Uses Redis if broker supports it, otherwise falls back to Memory

# Option 2: Explicit memory storage (development only)
tracking = PipelineTrackingManager().with_storage(InMemoryPipelineStorage())

# Option 3: Explicit Redis storage (production)
tracking = PipelineTrackingManager().with_storage(
    RedisPipelineStorage(redis_client)
)

# Option 4: Custom storage backend
tracking = PipelineTrackingManager().with_storage(MyCustomStorage())
```

### 2.2. Storage Lifetime

- **InMemoryPipelineStorage**: Lives only in current process; cleared on restart
- **RedisPipelineStorage**: Persistent across processes; survives restarts

Choose based on deployment:
- Local development → Memory
- Single-worker production → Memory (if no restart needed)
- Multi-worker / distributed → Redis (or other shared storage)

---

## 3. Pipeline Status Model

Every tracked pipeline produces a `PipelineStatus` object:

```python
from taskiq_flow.tracking.models import PipelineStatus

status: PipelineStatus
```

**Fields**:

| Field | Type | Description |
|-------|------|-------------|
| `pipeline_id` | `str` | Unique pipeline instance ID |
| `status` | `str` | `PENDING`, `RUNNING`, `COMPLETED`, `FAILED`, `CANCELLED` |
| `pipeline_type` | `str` | `"sequential"` or `"dataflow"` |
| `started_at` | `datetime` | Execution start timestamp |
| `completed_at` | `datetime` | Execution end timestamp (if finished) |
| `duration_ms` | `float` | Total execution time in milliseconds |
| `steps` | `list[StepStatus]` | Per-step breakdown |
| `result` | `Any` | Final return value (if completed) |
| `error` | `str` | Error message (if failed) |

**StepStatus** fields:

| Field | Type | Description |
|-------|------|-------------|
| `step_name` | `str` | Name of the task |
| `status` | `str` | `PENDING`, `RUNNING`, `COMPLETED`, `FAILED` |
| `started_at` | `datetime` | Step start time |
| `completed_at` | `datetime` | Step end time |
| `duration_ms` | `float` | Step execution time |
| `result` | `Any` | Step return value |
| `error` | `str` | Error message if failed |

---

## 4. Querying Status

### 4.1. Get Status of a Pipeline

```python
status = await tracking.get_status(pipeline_id)

if status.status == "COMPLETED":
    print(f"Pipeline finished in {status.duration_ms}ms")
    print(f"Result: {status.result}")
elif status.status == "FAILED":
    print(f"Failed: {status.error}")
```

### 4.2. List All Pipelines

```python
all_statuses = await tracking.list_pipelines()
for status in all_statuses:
    print(f"{status.pipeline_id}: {status.status}")
```

### 4.3. Filter by Status

```python
running = await tracking.list_pipelines(status_filter="RUNNING")
failed = await tracking.list_pipelines(status_filter="FAILED")
completed = await tracking.list_pipelines(status_filter="COMPLETED")
```

### 4.4. Get Pipeline History

```python
# Get last 10 pipelines
history = await tracking.get_history(limit=10)

# Filter by date range
from datetime import datetime, timedelta
week_ago = datetime.now() - timedelta(days=7)
recent = await tracking.get_history(since=week_ago)
```

### 4.5. Delete Old Records

```python
# Delete records older than 30 days
deleted = await tracking.cleanup_older_than(days=30)
print(f"Deleted {deleted} old pipeline records")

# Delete specific pipeline
await tracking.delete_pipeline(pipeline_id)
```

---

## 5. Storage Backends

### 5.1. InMemoryPipelineStorage

```python
from taskiq_flow.tracking import InMemoryPipelineStorage

storage = InMemoryPipelineStorage()
tracking = PipelineTrackingManager().with_storage(storage)

# Data lives only in current Python process
# On restart, all history is lost
# Suitable for: development, testing, single-run scripts
```

**Pros**:
- Zero configuration
- Fast (no network I/O)
- Simple

**Cons**:
- Not shareable between workers
- Lost on restart
- Limited history size

### 5.2. RedisPipelineStorage

```python
from taskiq_flow.tracking import RedisPipelineStorage
import redis.asyncio as redis

redis_client = redis.Redis(host="localhost", port=6379, decode_responses=True)
storage = RedisPipelineStorage(redis_client)
tracking = PipelineTrackingManager().with_storage(storage)
```

**Configuration**:

```python
# With custom key prefix and TTL
storage = RedisPipelineStorage(
    redis_client,
    key_prefix="taskiq_flow:tracking:",
    ttl_seconds=604800  # 7 days retention
)
```

**Pros**:
- Shared across multiple workers
- Persists across restarts
- Scalable
- Can be clustered for HA

**Cons**:
- Requires Redis server
- Network latency
- TTL management needed (avoid unbounded growth)

### 5.3. Custom Storage

Implement `TrackingStorage` protocol:

```python
from taskiq_flow.tracking.storage import TrackingStorage
from taskiq_flow.tracking.models import PipelineStatus

class PostgresStorage(TrackingStorage):
    async def save_status(self, status: PipelineStatus):
        # Insert/update in PostgreSQL
        pass

    async def get_status(self, pipeline_id: str) -> PipelineStatus | None:
        # Fetch from DB
        pass

    async def list_pipelines(self, status_filter: str | None = None):
        # Query with optional filter
        pass

    async def delete_pipeline(self, pipeline_id: str):
        # Remove record
        pass

tracking = PipelineTrackingManager().with_storage(PostgresStorage())
```

---

## 6. Real-Time Tracking with WebSocket

For live dashboard updates, combine `PipelineTrackingManager` with `HookManager`:

```python
from taskiq_flow.hooks import HookManager, TrackingEventBroadcaster

hook_manager = HookManager()
broadcaster = TrackingEventBroadcaster(tracking, hook_manager)
tracking.add_listener(broadcaster.on_status_update)

pipeline = Pipeline(broker).with_hooks(hook_manager).with_tracking(tracking)
```

Now pipeline events are broadcast via WebSocket as they happen.

See [WebSocket Guide]({{ '/en/guides/websocket/' | relative_url }}) for complete setup.

---

## 7. Metrics Collection

Track pipeline performance over time:

```python
# Collect statistics
stats = await tracking.get_metrics(days=7)

print(f"Total executions: {stats.total_pipelines}")
print(f"Success rate: {stats.success_rate:.1%}")
print(f"Avg duration: {stats.avg_duration_ms:.0f}ms")
print(f"Failure reasons: {stats.failure_reasons}")
```

**Common metrics**:

- Throughput (pipelines/minute)
- Success/failure ratio
- Average step duration
- Longest-running steps
- Busy hours

Integrate with monitoring systems (Prometheus, Grafana):

```python
from prometheus_client import Counter, Histogram

PIPELINE_COUNT = Counter('pipelines_total', 'Total pipelines', ['status'])
PIPELINE_DURATION = Histogram('pipeline_duration_seconds', 'Pipeline runtime')

class PrometheusExporter:
    async def on_pipeline_complete(self, status: PipelineStatus):
        PIPELINE_COUNT.labels(status=status.status).inc()
        PIPELINE_DURATION.observe(status.duration_ms / 1000)
```

---

## 8. Event Listeners

Attach callbacks to tracking events:

```python
class MyListener:
    async def on_pipeline_start(self, pipeline_id: str):
        print(f"Pipeline {pipeline_id} started")
        send_slack_notification(f"Pipeline {pipeline_id} started")

    async def on_step_complete(self, pipeline_id: str, step_name: str, result: Any):
        log_step_metric(step_name, result)

    async def on_pipeline_complete(self, pipeline_id: str, status: PipelineStatus):
        if status.status == "FAILED":
            alert_on_failure(pipeline_id)

listener = MyListener()
tracking.add_listener(listener)
```

**Listener methods** (all optional):

- `on_pipeline_start(pipeline_id: str)`
- `on_step_start(pipeline_id: str, step_name: str)`
- `on_step_complete(pipeline_id: str, step_name: str, result: Any)`
- `on_pipeline_complete(pipeline_id: str, status: PipelineStatus)`
- `on_pipeline_error(pipeline_id: str, error: str)`

---

## 9. Visualizing Tracking Data

### 9.1. Console Output

```python
status = await tracking.get_status(pipeline_id)
print(f"\n{'='*60}")
print(f"Pipeline: {status.pipeline_id}")
print(f"Status: {status.status}")
print(f"Duration: {status.duration_ms:.0f}ms")
print(f"Steps:")
for step in status.steps:
    bar = "█" * int(step.duration_ms / 10)
    print(f"  {step.step_name:<30} {bar} {step.duration_ms:.0f}ms")
```

### 9.2. JSON Export

```python
import json
status_dict = status.model_dump(mode="json", exclude={"result"})  # exclude large results
print(json.dumps(status_dict, indent=2, default=str))
```

### 9.3. Integration with Dashboards

Use the REST API endpoints (see [API Guide]({{ '/en/guides/api/' | relative_url }})) to build custom dashboards:

```javascript
// Frontend fetch
fetch('/api/pipelines/{pipeline_id}/status')
  .then(res => res.json())
  .then(status => {
    // Render timeline chart of step durations
    // Show success/failure badges
  });
```

---

## 10. Production Best Practices

### 10.1. Use Redis for Production

Always use `RedisPipelineStorage` in production:

```python
# config.py
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

# app.py
from redis.asyncio import Redis
redis_client = Redis.from_url(REDIS_URL)
tracking = PipelineTrackingManager().with_storage(
    RedisPipelineStorage(redis_client, ttl_seconds=2592000)  # 30 days
)
```

### 10.2. Set Up Retention Policies

```python
# Periodic cleanup job (daily)
async def cleanup_old_trackings():
    deleted = await tracking.cleanup_older_than(days=7)
    print(f"Cleaned up {deleted} old pipeline records")

# Use APScheduler to run daily
from taskiq_flow import PipelineScheduler
scheduler = PipelineScheduler(broker)
scheduler.schedule_at(cleanup_old_trackings, run_at="0 3 * * *")  # 3 AM daily
```

### 10.3. Monitor Tracker Health

```python
# Health check for monitoring systems
async def tracking_health():
    try:
        test_pipeline = Pipeline(broker).with_tracking(tracking)
        await test_pipeline.kiq("health_check")
        return {"status": "healthy"}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}
```

### 10.4. Limit History Size

```python
# Keep only last N pipelines per pipeline_id pattern
import fnmatch

patterns = ["batch_job_*", "etl_*"]
for pattern in patterns:
    old = await tracking.list_pipelines()
    matching = [p for p in old if fnmatch.fnmatch(p.pipeline_id, pattern)]
    if len(matching) > 100:
        for old_pipeline in matching[-100:]:
            await tracking.delete_pipeline(old_pipeline.pipeline_id)
```

---

## 11. Troubleshooting

### "No storage configured" Error

**Symptom**: `RuntimeError: No tracking storage configured`

**Fix**: Add storage before using tracking：

```python
tracking = PipelineTrackingManager().with_auto_storage(broker)
# or
tracking = PipelineTrackingManager().with_storage(InMemoryPipelineStorage())
```

### Tracking Data Missing

**Symptom**: `get_status()` returns `None` even though pipeline ran

**Causes & fixes**:

1. **Tracking not attached**:
   ```python
   pipeline = Pipeline(broker).with_tracking(tracking)  # Must call with_tracking()
   ```

2. **Using different brokers** — Ensure same `broker` instance across task and pipeline.

3. **Storage lifetime** — InMemory storage lost on restart; switch to Redis.

4. **Pipeline ID mismatch** — Confirm `pipeline.pipeline_id` matches what you query.

### Performance Degradation with Redis

**Symptom**: Tracking slows down pipeline execution

**Fixes**:
- Use Redis connection pooling
- Batch status updates (bundle multiple steps)
- Async batch writes (default behavior)
- Increase Redis `maxmemory` and use appropriate eviction policy

---

## 12. Summary

| Feature | In-Memory | Redis |
|---------|-----------|-------|
| **Multi-process** | ❌ No | ✅ Yes |
| **Persistent** | ❌ No | ✅ Yes |
| **Shared state** | ❌ No | ✅ Yes |
| **Speed** | ⚡ Fastest | ⚡ Fast (network) |
| **Config required** | None | Redis server |

**Basic recipe**:
```python
tracking = PipelineTrackingManager().with_auto_storage(broker)
pipeline = Pipeline(broker).with_tracking(tracking)
```

**Production recipe**:
```python
tracking = PipelineTrackingManager().with_storage(
    RedisPipelineStorage(redis_client, ttl_seconds=604800)
)
pipeline = Pipeline(broker).with_tracking(tracking)
```

---

## Next Steps

- **[WebSocket Streaming]({{ '/en/guides/websocket/' | relative_url }})** — Real-time event delivery for dashboards
- **[Dataflow Guide]({{ '/en/guides/dataflow/' | relative_url }})** — Full DAG pipelines with automatic parallelism
- **[Scheduling]({{ '/en/guides/scheduling/' | relative_url }})** — Automated recurring pipeline execution
- **[Performance Tuning]({{ '/en/guides/performance/' | relative_url }})** — Optimize tracking overhead

---

*Track everything. Visualize with [WebSocket]({{ '/en/guides/websocket/' | relative_url }}).*
