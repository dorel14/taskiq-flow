---
permalink: /en/api/websocket/
title: 'API Reference: WebSocket Integration'
nav_order: 34
color_scheme: dark
---
# API Reference: WebSocket Integration

**HookManager, WebSocket bridge, and real-time event transport via FastAPI**

> **Version**: {VERSION} | **Module**: `taskiq_flow.hooks`, `taskiq_flow.integration.websocket`

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

## WebSocket Integration (FastAPI)

Taskiq-Flow uses a **FastAPI-only** WebSocket integration. WebSocket events are served via a FastAPI `WebSocket` route at `/ws/{pipeline_id}` in your application.

### setup_websocket_bridge

Connects the `HookManager` event system to the WebSocket transport layer:

```python
from taskiq_flow.hooks import HookManager, setup_websocket_bridge

hook_manager = HookManager()
setup_websocket_bridge(hook_manager)
# Now all pipeline events are forwarded to the WebSocket transport
```

Once the bridge is active, mount the FastAPI WebSocket endpoint in your application:

```python
from fastapi import FastAPI, WebSocket
from taskiq_flow.integration.websocket.fastapi_ws import fastapi_websocket_endpoint

app = FastAPI()

@app.websocket("/ws/{pipeline_id}")
async def ws_endpoint(websocket: WebSocket, pipeline_id: str):
    await fastapi_websocket_endpoint(websocket, pipeline_id)
```

Clients connect to `ws://<host>:<port>/ws/<pipeline_id>` and receive real-time pipeline events. The `pipeline_id` in the URL path is used to subscribe the client to that specific pipeline's event channel.

### get_fastapi_ws_manager

Get the singleton FastAPIWebSocketManager instance:

```python
from taskiq_flow.integration.websocket.fastapi_ws import get_fastapi_ws_manager

manager = get_fastapi_ws_manager()
print(f"Active channels: {manager.get_channel_ids()}")
print(f"Client count: {manager.get_client_count('pipeline_1')}")
```

### fastapi_websocket_endpoint

The FastAPI WebSocket route handler. Use it as a route callback:

```python
from taskiq_flow.integration.websocket.fastapi_ws import fastapi_websocket_endpoint

@app.websocket("/ws/{pipeline_id}")
async def ws_endpoint(websocket: WebSocket, pipeline_id: str):
    await fastapi_websocket_endpoint(websocket, pipeline_id)
```

The endpoint:
- Accepts the WebSocket connection
- Subscribes the client to the `pipeline_id` channel
- Tracks the connection metric
- Disconnects cleanly on client disconnect

---

### get_websocket_server ⚠️ Deprecated

:::warning Deprecated since v1.1
The function `get_websocket_server()` and class `PipelineWebSocketServer` were removed in v1.1. The standalone picows WebSocket server is no longer supported. Use the FastAPI WebSocket integration instead.
:::

```python
from taskiq_flow.integration.websocket import get_websocket_server

server = get_websocket_server(
    host="0.0.0.0",
    port=8765,
    transport=None  # Uses default WebSocketTransport
)
await server.start_server()
```

:::danger Raises RuntimeError
Calling `get_websocket_server()` always raises `RuntimeError`. Use the FastAPI integration instead:

```python
from fastapi import FastAPI, WebSocket
from taskiq_flow.integration.websocket.fastapi_ws import fastapi_websocket_endpoint

app = FastAPI()

@app.websocket("/ws/{pipeline_id}")
async def ws_endpoint(websocket: WebSocket, pipeline_id: str):
    await fastapi_websocket_endpoint(websocket, pipeline_id)
```
:::

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

Both filters are **ANDed** internally: an event passes if it matches *both* the pipeline_ids AND event_types filters. Use `"*"` to match all.

---

## WebSocket Protocol

### Connection

Clients connect via standard WebSocket to the FastAPI route:

```
ws://localhost:8000/ws/my_pipeline
```

For secure connections (WSS), terminate TLS at a reverse proxy (nginx, Traefik).

### Subscription

After connecting, the client is already subscribed to the pipeline identified by the URL path. Additional subscribe/unsubscribe messages can be sent:

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

Full field reference in the [WebSocket Guide]({{ '/en/guides/websocket/' | relative_url }}).

---

## FastAPI Route Reference

### Mounting the WebSocket Endpoint

```python
from fastapi import FastAPI, WebSocket
from taskiq_flow.integration.websocket.fastapi_ws import fastapi_websocket_endpoint

app = FastAPI()

@app.websocket("/ws/{pipeline_id}")
async def ws_endpoint(websocket: WebSocket, pipeline_id: str):
    await fastapi_websocket_endpoint(websocket, pipeline_id)
```

### Querying Connected Clients

```python
from taskiq_flow.integration.websocket.fastapi_ws import get_fastapi_ws_manager

manager = get_fastapi_ws_manager()
client_count = manager.get_client_count("pipeline_1")
```

Full REST endpoints for querying clients and pipelines are available in the API routes module:

```python
from taskiq_flow.api.routes.websocket import router as ws_router
app.include_router(ws_router, prefix="/api")
```

---

## Multi-Worker Coordination

For multiple workers sharing event state via Redis:

```python
from taskiq_flow.transport import RedisPubSubTransport
from taskiq_flow.hooks.bridge import setup_websocket_bridge
import redis

redis_client = redis.Redis(host="localhost", port=6379)
transport = RedisPubSubTransport(redis_client)
setup_websocket_bridge(hook_manager, use_fastapi=True)
```

All workers subscribe to the same Redis pub/sub channel; events from any worker are broadcast to all WebSocket clients connected to any worker. WebSocket connections are always served through the FastAPI application.

> **Prerequisite**: Install the `[brokers]` extra: `pip install "taskiq-flow[brokers]"` for Redis support.

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
    manager = get_fastapi_ws_manager()
    await manager.close_all()
```

### Monitoring

Expose metrics via the built-in metrics endpoint:

```python
@app.get("/ws/metrics")
async def ws_metrics():
    manager = get_fastapi_ws_manager()
    return {
        "channels": manager.get_channel_ids(),
    }
```

### SSL/TLS

Terminate TLS at a reverse proxy (nginx, Traefik) — never inline in the application:

```nginx
server {
    listen 443 ssl;
    location /ws {
        proxy_pass http://localhost:8000;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
    }
}
```

> **Note**: The legacy `PipelineWebSocketServer` with inline SSL (`ssl_cert`/`ssl_key` parameters) was removed in v1.1. Use a reverse proxy for TLS termination.

---

## Troubleshooting

| Issue | Diagnosis | Fix |
|-------|-----------|-----|
| Clients not receiving events | `setup_websocket_bridge()` not called | Call before pipeline starts |
| Connection refused | FastAPI app not running | Start with `uvicorn app:app` |
| Events delayed | Event filter blocking | Check filter configuration |
| High CPU | Too many connections | Enforce connection limits |

---

## Summary

| Component | Role |
|-----------|------|
| `HookManager` | Collect events from pipelines |
| `BaseEvent` subclasses | Structured event data |
| `EventFilter` | Selectively broadcast events |
| `setup_websocket_bridge()` | Connects HookManager to WebSocket transport |
| `FastAPIWebSocketManager` | WebSocket client management (singleton) |
| `fastapi_websocket_endpoint()` | FastAPI WebSocket route handler |
| `FastAPI route /ws/{pipeline_id}` | Client-facing WebSocket endpoint |

**Minimum FastAPI setup**:

```python
from fastapi import FastAPI, WebSocket
from taskiq_flow.integration.websocket.fastapi_ws import fastapi_websocket_endpoint
from taskiq_flow.hooks import HookManager, setup_websocket_bridge

hook_manager = HookManager()
setup_websocket_bridge(hook_manager)
pipeline = Pipeline(broker).with_hooks(hook_manager)

app = FastAPI()

@app.websocket("/ws/{pipeline_id}")
async def ws_endpoint(websocket: WebSocket, pipeline_id: str):
    await fastapi_websocket_endpoint(websocket, pipeline_id)
```

---

*For client implementation details, see the [WebSocket Guide]({{ '/en/guides/websocket/' | relative_url }}). For event filtering strategies, see that guide's Section 7.*
