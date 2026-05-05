---
title: API Reference: Tracking
nav_order: 33
---
# API Reference: Tracking

**PipelineTrackingManager, storage backends, and status models**

> **Version**: 0.3.2 | **Module**: `taskiq_flow.tracking`, `taskiq_flow.tracking.models`

---

## PipelineTrackingManager

Central coordinator for recording and retrieving pipeline execution data.

```python
from taskiq_flow import PipelineTrackingManager

tracking = PipelineTrackingManager()
tracking = tracking.with_auto_storage(broker)
# or
tracking = tracking.with_storage(InMemoryPipelineStorage())
```

**Configuration**:

```python
tracking = PipelineTrackingManager(
    storage=None,         # Optional pre-configured storage
    max_history=1000,     # Max pipeline records to keep (memory store only)
    auto_cleanup=True     # Auto-purge old records
)
```

**Storage selection** (via `with_auto_storage`):

| Broker | Auto-selected storage |
|--------|----------------------|
| `InMemoryBroker` | `InMemoryPipelineStorage` |
| `RedisBroker` | `RedisPipelineStorage` |
| Other | Falls back to memory |

---

## Methods

### Attaching to Pipelines

```python
pipeline = Pipeline(broker).with_tracking(tracking)
# or
pipeline.with_tracking(tracking)  # in-place modification
```

The tracking manager must be attached **before** calling `pipeline.kiq()`.

### Querying Status

```python
# Get status of a specific pipeline execution
status = await tracking.get_status(pipeline_id: str) -> PipelineStatus | None

# List all tracked pipelines
all_statuses = await tracking.list_pipelines(
    status_filter: str | None = None,  # Filter by status
    limit: int = 100
) -> list[PipelineStatus]

# Get historical executions
history = await tracking.get_history(
    since: datetime | None = None,
    until: datetime | None = None,
    limit: int = 100
) -> list[PipelineStatus]
```

### Maintenance

```python
# Delete specific pipeline record
await tracking.delete_pipeline(pipeline_id: str)

# Delete records older than N days
deleted_count = await tracking.cleanup_older_than(days: int = 30) -> int

# Get aggregate metrics
metrics = await tracking.get_metrics(
    days: int = 7
) -> TrackingMetrics
```

### Event Listeners

```python
class MyListener:
    async def on_pipeline_start(self, pipeline_id: str):
        print(f"Pipeline {pipeline_id} started")

    async def on_pipeline_complete(self, pipeline_id: str, status: PipelineStatus):
        send_alert_if_failed(status)

listener = MyListener()
tracking.add_listener(listener)
```

**Listener hooks** (all optional):

- `on_pipeline_start(pipeline_id)`
- `on_step_start(pipeline_id, step_name)`
- `on_step_complete(pipeline_id, step_name, result)`
- `on_pipeline_complete(pipeline_id, status)`
- `on_pipeline_error(pipeline_id, error)`

---

## Storage Backends

### InMemoryPipelineStorage

```python
from taskiq_flow.tracking import InMemoryPipelineStorage

storage = InMemoryPipelineStorage(max_records=1000)
tracking = PipelineTrackingManager().with_storage(storage)
```

**Features**:
- Zero configuration
- Fast (no I/O)
- **Not shared between workers**
- Lost on process restart
- Good for: development, testing, single-process

**Parameters**:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `max_records` | `int` | 1000 | Maximum pipeline records to retain (LRU eviction) |

---

### RedisPipelineStorage

```python
from taskiq_flow.tracking import RedisPipelineStorage
import redis.asyncio as redis

redis_client = redis.Redis(host="localhost", port=6379, decode_responses=True)
storage = RedisPipelineStorage(
    redis_client,
    key_prefix="taskiq_flow:tracking:",
    ttl_seconds=604800  # 7 days
)
tracking = PipelineTrackingManager().with_storage(storage)
```

**Features**:
- Shared across multiple workers
- Persistent across restarts
- Scalable (Redis cluster)
- TTL-based expiration
- Good for: production, distributed deployments

**Parameters**:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `redis_client` | `Redis` | **required** | Connected Redis client |
| `key_prefix` | `str` | `"taskiq_flow:tracking:"` | Prefix for all keys |
| `ttl_seconds` | `int` | 604800 (7d) | Auto-expire after N seconds |
| `serializer` | `Callable` | `json.dumps` | Custom serialization function |

---

## Data Models

### PipelineStatus

Complete status of a pipeline execution.

```python
from taskiq_flow.tracking.models import PipelineStatus

status: PipelineStatus
```

**Attributes**:

| Attribute | Type | Description |
|-----------|------|-------------|
| `pipeline_id` | `str` | Unique identifier |
| `status` | `str` | `PENDING`, `RUNNING`, `COMPLETED`, `FAILED`, `CANCELLED` |
| `pipeline_type` | `str` | `"sequential"` or `"dataflow"` |
| `started_at` | `datetime` | Execution start timestamp |
| `completed_at` | `datetime | None` | End time if finished |
| `duration_ms` | `float` | Total duration in milliseconds |
| `steps` | `list[StepStatus]` | Per-step status objects |
| `result` | `Any` | Final return value (if completed) |
| `error` | `str \| None` | Error message if failed |

**Methods**:
- `model_dump()` — Return as dictionary (Pydantic model)
- `is_finished()` — True if terminal state (COMPLETED/FAILED/CANCELLED)

---

### StepStatus

Status of a single pipeline step.

```python
from taskiq_flow.tracking.models import StepStatus

step: StepStatus
```

**Attributes**:

| Attribute | Type | Description |
|-----------|------|-------------|
| `step_name` | `str` | Task name |
| `status` | `str` | `PENDING`, `RUNNING`, `COMPLETED`, `FAILED` |
| `started_at` | `datetime` | Step start time |
| `completed_at` | `datetime | None` | Step end time |
| `duration_ms` | `float` | Execution duration |
| `result` | `Any` | Return value |
| `error` | `str \| None` | Error message |
| `retry_count` | `int` | Number of retry attempts |

---

### TrackingMetrics

Aggregated statistics (returned by `get_metrics()`).

```python
from taskiq_flow.tracking.models import TrackingMetrics

metrics: TrackingMetrics
```

**Attributes**:

| Attribute | Type | Description |
|-----------|------|-------------|
| `total_pipelines` | `int` | Total executions tracked |
| `completed` | `int` | Successful completions |
| `failed` | `int` | Failed executions |
| `success_rate` | `float` | Ratio completed / total |
| `avg_duration_ms` | `float` | Average pipeline duration |
| `p95_duration_ms` | `float` | 95th percentile duration |
| `failure_reasons` | `dict[str, int]` | Error type → count |
| `most_frequent_step` | `str | None` | Step that fails most often |

---

## Custom Storage Implementation

Implement `TrackingStorage` protocol for custom backends:

```python
from taskiq_flow.tracking.storage import TrackingStorage
from taskiq_flow.tracking.models import PipelineStatus

class PostgresStorage(TrackingStorage):
    async def save_status(self, status: PipelineStatus):
        """Save or update pipeline status."""
        ...

    async def get_status(self, pipeline_id: str) -> PipelineStatus | None:
        """Fetch pipeline status by ID."""
        ...

    async def list_pipelines(self, status_filter: str | None = None,
                             limit: int = 100) -> list[PipelineStatus]:
        """List pipelines, optionally filtered by status."""
        ...

    async def delete_pipeline(self, pipeline_id: str):
        """Remove pipeline record."""
        ...

    async def cleanup_older_than(self, days: int) -> int:
        """Delete records older than N days. Returns count deleted."""
        ...

tracking = PipelineTrackingManager().with_storage(PostgresStorage())
```

All storage methods must be async.

---

## Best Practices

1. **Production**: Always use Redis storage (shared, persistent)
2. **TTL**: Set appropriate TTL (7–30 days) to bound storage growth
3. **Listeners**: Add alerting listeners for failures
4. **Cleanup**: Schedule periodic cleanup (daily cron job)
5. **Indexing**: For custom DB stores, index on `pipeline_id`, `started_at` for query performance

---

## Troubleshooting

| Issue | Likely Cause | Fix |
|-------|--------------|-----|
| `get_status()` returns `None` | Tracking not attached, or wrong `pipeline_id` | Ensure `pipeline.with_tracking(tracking)` called before `kiq()` |
| Storage errors | Redis connection failed | Check Redis is running, connection string valid |
| Memory growth (memory store) | Not purging old records | Set `max_records` or use Redis with TTL |
| Listeners not firing | Not added before pipeline start | Call `tracking.add_listener()` before `pipeline.kiq()` |

---

*Combine with [WebSocket]({{ '/en/api/websocket.md' | relative_url }}) for real-time streaming. See [Tracking Guide]({{ '/en/guides/tracking.md' | relative_url }}) for usage patterns.*
