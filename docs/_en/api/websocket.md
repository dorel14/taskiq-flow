---
permalink: /en/api/websocket/
title: API Reference: WebSocket Integration
nav_order: 34
color_scheme: dark
---
# API Reference: WebSocket Integration

**HookManager, WebSocket bridge, and real-time event transport**

> **Version**: 0.4.5 | **Module**: `taskiq_flow.hooks`, `taskiq_flow.transport.websocket`

---

## HookManager

Central event bus that collects pipeline execution events and dispatches them to subscribers.

```python
from taskiq_flow.hooks import HookManager

hook_manager = HookManager()
pipeline = Pipeline(broker).with_hooks(hook_manager)
```

**Events emitted**:

- `PipelineStartEvent` — Pipeline execution started
- `StepStartEvent` — A step started
- `StepCompleteEvent` — A step completed
- `PipelineCompleteEvent` — Pipeline finished successfully
- `StepErrorEvent` — A step failed
- `PipelineErrorEvent` — Pipeline failed

### Adding Custom Hooks

```python
class MyHook:
    async def on_pipeline_start(self, event):
        print(f"Pipeline {event.pipeline_id} started")

    async def on_step_complete(self, event):
        print(f"Step {event.step_name} finished in {event.duration_ms}ms")

hook = MyHook()
hook_manager.add_hook(hook)
```

**Hook methods** (all optional):

| Method | Event |
|--------|-------|
| `on_pipeline_start(event: PipelineStartEvent)` | Pipeline started |
| `on_step_start(event: StepStartEvent)` | Step starting |
| `on_step_complete(event: StepCompleteEvent)` | Step finished |
| `on_pipeline_complete(event: PipelineCompleteEvent)` | Pipeline completed |
| `on_step_error(event: StepErrorEvent)` | Step failed |
| `on_pipeline_error(event: PipelineErrorEvent)` | Pipeline error |

---

## Event Types

All events are Pydantic models with a `type` discriminator.

### PipelineStartEvent

```python
from taskiq_flow.hooks import PipelineStartEvent

event = PipelineStartEvent(
    pipeline_id="my_pipeline",
    pipeline_type="sequential",
    timestamp=datetime.now(),
    input=initial_data
)
```

**Fields**:

| Field | Type | Description |
|-------|------|-------------|
| `type` | `Literal["PipelineStartEvent"]` | Event type discriminator |
| `pipeline_id` | `str` | Pipeline instance ID |
| `pipeline_type` | `str` | `"sequential"` or `"dataflow"` |
| `timestamp` | `datetime` | Event time |
| `input` | `Any` | Initial input data |

---

### StepStartEvent

```python
from taskiq_flow.hooks import StepStartEvent

event = StepStartEvent(
    pipeline_id="my_pipeline",
    step_name="process_data",
    step_index=2,
    task_id="task_abc123",
    timestamp=datetime.now()
)
```

**Fields**:

| Field | Type | Description |
|-------|------|-------------|
| `type` | `Literal["StepStartEvent"]` | Event type |
| `pipeline_id` | `str` | Origin pipeline |
| `step_name` | `str` | Task name |
| `step_index` | `int` | Position in pipeline (0-indexed) |
| `task_id` | `str` | Underlying taskiq task ID |
| `timestamp` | `datetime` | Event time |

---

### StepCompleteEvent

```python
from taskiq_flow.hooks import StepCompleteEvent

event = StepCompleteEvent(
    pipeline_id="my_pipeline",
    step_name="process_data",
    step_index=2,
    result={"processed": 42},
    duration_ms=150.5,
    timestamp=datetime.now()
)
```

**Fields**:

| Field | Type | Description |
|-------|------|-------------|
| `type` | `Literal["StepCompleteEvent"]` | Event type |
| `pipeline_id` | `str` | Origin pipeline |
| `step_name` | `str` | Completed task |
| `step_index` | `int` | Step position |
| `result` | `Any` | Task return value |
| `duration_ms` | `float` | Execution time in milliseconds |
| `timestamp` | `datetime` | Event time |

---

### PipelineCompleteEvent

```python
from taskiq_flow.hooks import PipelineCompleteEvent

event = PipelineCompleteEvent(
    pipeline_id="my_pipeline",
    pipeline_type="dataflow",
    status="COMPLETED",
    duration_ms=1250.3,
    result={"final": "output"},
    timestamp=datetime.now()
)
```

**Fields**:

| Field | Type | Description |
|-------|------|-------------|
| `type` | `Literal["PipelineCompleteEvent"]` | Event type |
| `pipeline_id` | `str` | Pipeline ID |
| `pipeline_type` | `str` | Pipeline type |
| `status` | `str` | `"COMPLETED"`, `"FAILED"`, `"CANCELLED"` |
| `duration_ms` | `float` | Total execution time |
| `result` | `Any` | Final pipeline result |
| `timestamp` | `datetime` | Event time |

---

### Error Events

```python
from taskiq_flow.hooks import StepErrorEvent, PipelineErrorEvent

step_error = StepErrorEvent(
    pipeline_id="my_pipeline",
    step_name="failing_task",
    error="ValueError: invalid input",
    timestamp=datetime.now()
)

pipeline_error = PipelineErrorEvent(
    pipeline_id="my_pipeline",
    error="Pipeline aborted after 3 failures",
    timestamp=datetime.now()
)
```

---

## WebSocket Transport

### setup_websocket_bridge

Connects `HookManager` to the WebSocket transport layer:

```python
from taskiq_flow.hooks import HookManager, setup_websocket_bridge

hook_manager = HookManager()
setup_websocket_bridge(hook_manager)
# Now all hooks are forwarded to WebSocket server
```

This installs a bridge that forwards events from the `HookManager` to any connected WebSocket servers.

### get_websocket_server

Factory function to obtain or create a WebSocket server:

```python
from taskiq_flow.integration.websocket import get_websocket_server

server = get_websocket_server(
    host="0.0.0.0",
    port=8765,
    transport=None  # Uses default WebSocketTransport
)
await server.start_server()
```

**Parameters**:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `host` | `str` | `"0.0.0.0"` | Bind address |
| `port` | `int` | `8765` | Bind port |
| `transport` | `WebSocketTransport | None` | Auto-created if None |

The server is a singleton per configuration; subsequent calls to `get_websocket_server()` with same host/port return the same instance.

---

## Event Filtering

Reduce traffic by filtering events:

```python
from taskiq_flow.hooks import EventFilter

# Only specific pipelines
filter = EventFilter(pipeline_ids=["pipeline_1", "pipeline_2"])

# Only step events
filter = EventFilter(event_types=["StepStartEvent", "StepCompleteEvent"])

# Both
filter = EventFilter(
    pipeline_ids=["*"],  # all pipelines (or specific)
    event_types=["StepCompleteEvent", "PipelineCompleteEvent"]
)

hook_manager.add_filter(filter)
```

### EventFilter Logic

```
Event → Check pipeline_id match? → Check event_type match? → Emit?
```

Both filters are **ORed** internally: an event passes if it matches *both* the pipeline_ids AND event_types filters. Use `"*"` to match all.

---

## WebSocket Protocol

### Connection

Client connects via standard WebSocket:

```
ws://localhost:8765
```

For secure connections (WSS), terminate SSL at reverse proxy (nginx, Traefik).

### Subscription

After connecting, client sends a subscription message:

```json
{
  "type": "subscribe",
  "pipeline_id": "my_pipeline"
}
```

Wildcard subscription (receive all events):

```json
{
  "type": "subscribe",
  "pipeline_id": "*"
}
```

Unsubscribe:

```json
{
  "type": "unsubscribe",
  "pipeline_id": "my_pipeline"
}
```

### Message Format (Server → Client)

All messages are JSON with a `type` field:

```json
{
  "type": "StepCompleteEvent",
  "pipeline_id": "pipeline_123",
  "step_name": "process_data",
  "duration_ms": 150.2,
  "timestamp": "2026-05-05T16:30:00Z"
}
```

Full field reference in the WebSocket Guide.

---

## Custom Transport

For advanced use cases, implement your own transport:

```python
from taskiq_flow.transport import WebSocketTransport

class MyTransport(WebSocketTransport):
    async def broadcast(self, event: BaseEvent):
        # Custom routing logic (e.g., to Redis Pub/Sub, Kafka, etc.)
        await self.redis.publish("pipeline_events", event.json())

transport = MyTransport()
server = get_websocket_server(transport=transport)
```

---

## Multi-Worker Coordination

For multiple Python workers sharing event state:

```python
from taskiq_flow.transport import RedisPubSubTransport

transport = RedisPubSubTransport(redis_client)
server = get_websocket_server(transport=transport)
# All workers connected to same Redis channel share events
```

All workers subscribe to the same Redis pub/sub channel; events from any worker are broadcast to all WebSocket clients connected to any worker.

---

## Production Considerations

### Connection Limits

```python
import asyncio

# Limit concurrent WebSocket connections
MAX_CONNECTIONS = 1000
semaphore = asyncio.Semaphore(MAX_CONNECTIONS)

# In your connection handler:
async def handle_connection(websocket):
    if not semaphore.acquire(blocking=False):
        await websocket.close(code=1013, reason="Too many connections")
        return
    try:
        await websocket_service.handle(websocket)
    finally:
        semaphore.release()
```

### Graceful Shutdown

```python
async def shutdown():
    await server.close()  # Stop accepting new connections
    await server.wait_closed()  # Wait for existing connections to close
```

### Monitoring

Expose metrics:

```python
@app.get("/ws/metrics")
async def ws_metrics():
    return {
        "connections": server.connection_count(),
        "messages_sent": server.messages_sent,
        "messages_per_second": server.rate()
    }
```

---

## Troubleshooting

| Issue | Diagnosis | Fix |
|-------|-----------|-----|
| Clients not receiving events | `setup_websocket_bridge()` not called | Call before pipeline starts |
| Connection refused | Server not started | Call `await server.start_server()` |
| Events delayed | Event filter blocking | Check filter configuration |
| High CPU | Too many connections | Enforce connection limits |

---

## Summary

| Component | Role |
|-----------|------|
| `HookManager` | Collect events from pipelines |
| `BaseEvent` subclasses | Structured event data |
| `EventFilter` | Selectively broadcast events |
| `WebSocketTransport` | Low-level transport (default or custom) |
| `WebSocketServer` | Manage client connections |
| `get_websocket_server()` | Factory/singleton accessor |

**Minimum setup**:

```python
hooks = HookManager()
setup_websocket_bridge(hooks)
pipeline = Pipeline(broker).with_hooks(hooks)
server = get_websocket_server()
await server.start_server()
```

---

*For client implementation details, see [WebSocket Guide]({{ '/en/guides/websocket/' | relative_url }}). For event filtering strategies, see that guide's Section 7.*
