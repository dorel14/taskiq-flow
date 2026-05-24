---
permalink: /en/examples/websocket-demo/
title: 'Example: websocket_demo.py'
nav_order: 46
color_scheme: dark
---
# Example: websocket_demo.py

**Real-time event streaming via WebSockets**

> **Version**: {VERSION} | **File**: `examples/websocket_demo.py`

---

## Overview

This example demonstrates how to set up WebSocket real-time pipeline event streaming using **FastAPI-only WebSocket integration**.

This example covers:

- Creating a `HookManager` and connecting it to the WebSocket transport via `setup_websocket_bridge()`
- Running a pipeline with WebSocket events active
- Connecting a JavaScript client to the FastAPI WebSocket route
- Observing live step completion events

**Note**: WebSocket events are served through your FastAPI application at `/ws/{pipeline_id}`. The standalone `get_websocket_server()` and `PipelineWebSocketServer` were removed in v1.1. Use the FastAPI WebSocket route instead.

---

## Prerequisites

```bash
pip install "taskiq-flow[brokers]" fastapi uvicorn
```

---

## What This Example Shows

- Setting up `HookManager` with `setup_websocket_bridge()`
- Attaching hooks to a pipeline
- Executing the pipeline and broadcasting events to connected WebSocket clients
- Connecting a WebSocket client via FastAPI at `/ws/{pipeline_id}`

---

## Part 1: The Pipeline Script (Python)

This script sets up the pipeline, hooks, and FastAPI application. Start this first.

```python
import asyncio
from taskiq import InMemoryBroker
from taskiq_flow import Pipeline
from taskiq_flow.hooks import HookManager, setup_websocket_bridge
from taskiq_flow.middleware import PipelineMiddleware
from fastapi import FastAPI, WebSocket
from taskiq_flow.integration.websocket.fastapi_ws import (
    fastapi_websocket_endpoint,
    get_fastapi_ws_manager,
)

# Create broker
broker = InMemoryBroker(await_inplace=True).with_middlewares(PipelineMiddleware())

# Define simple tasks
@broker.task
def add_one(x: int) -> int:
    return x + 1

@broker.task
def multiply_by_two(x: int) -> int:
    return x * 2

# -- WebSocket setup --------------------------------------------------------

# 1. Create hook manager
hook_manager = HookManager()

# 2. Set up the WebSocket bridge (connects HookManager → FastAPI WebSocket transport)
setup_websocket_bridge(hook_manager)

# 3. Mount the FastAPI WebSocket route
app = FastAPI()

@app.websocket("/ws/{pipeline_id}")
async def ws_endpoint(websocket: WebSocket, pipeline_id: str):
    """
    WebSocket endpoint for real-time pipeline events.
    Clients connect to: ws://localhost:8000/ws/{pipeline_id}
    """
    await fastapi_websocket_endpoint(websocket, pipeline_id)

# -- Pipeline ---------------------------------------------------------------

async def main():
    # 4. Create pipeline and attach hooks
    pipeline = Pipeline(broker)
    pipeline.pipeline_id = "websocket_demo"
    pipeline.call_next(add_one, param_name="x")
    pipeline.call_next(multiply_by_two, param_name="x")
    pipeline.with_hooks(hook_manager)

    # 5. Execute the pipeline (events are broadcast to any connected WS clients)
    result = await pipeline.kiq(5)  # Start with 5 → 6 → 12
    print(f"Pipeline result: {result}")

    # Keep FastAPI app running briefly (in production, use uvicorn separately)
    await asyncio.sleep(5)
    print("Demo complete. Shutting down.")

asyncio.run(main())
```

Start the script with:

```bash
uvicorn examples.websocket_demo:app --host 0.0.0.0 --port 8000
```

Or during development:

```bash
uvicorn examples.websocket_demo:app --reload
```

---

## Part 2: The WebSocket Client (JavaScript)

Open a browser console or a Node.js script and connect to your FastAPI app:

```javascript
// Connect to the FastAPI WebSocket route
const ws = new WebSocket('ws://localhost:8000/ws/websocket_demo');

// Subscribe to the demo pipeline (already done by connecting to /ws/websocket_demo)
ws.onopen = () => {
    console.log('Connected to WebSocket server');
    ws.send(JSON.stringify({
        type: 'subscribe',
        pipeline_id: 'websocket_demo'
    }));
};

// Handle incoming events
ws.onmessage = (event) => {
    const data = JSON.parse(event.data);
    console.log('Event:', data.type, data);

    switch (data.type) {
        case 'StepCompleteEvent':
            console.log(`Step ${data.step_name} finished:`, data.result);
            break;
        case 'PipelineCompleteEvent':
            console.log('Pipeline finished with status:', data.status);
            console.log('Final result:', data.result);
            break;
    }
};
```

---

## Event Sequence

When the pipeline runs, the following events are broadcast:

1. **PipelineStartEvent**
   ```json
   {"type": "PipelineStartEvent", "pipeline_id": "websocket_demo", "timestamp": "..."}
   ```

2. **StepStartEvent** (for add_one)
   ```json
   {"type": "StepStartEvent", "pipeline_id": "websocket_demo", "step_name": "add_one", ...}
   ```

3. **StepCompleteEvent** (for add_one)
   ```json
   {"type": "StepCompleteEvent", "pipeline_id": "websocket_demo", "step_name": "add_one", "result": 6, "duration_ms": 1.2, ...}
   ```

4. **StepStartEvent** (for multiply_by_two)

5. **StepCompleteEvent** (for multiply_by_two)

6. **PipelineCompleteEvent**
   ```json
   {"type": "PipelineCompleteEvent", "pipeline_id": "websocket_demo", "status": "COMPLETED", "result": 12, ...}
   ```

---

## Key Setup Steps

### 1. Create HookManager
```python
hook_manager = HookManager()
```

### 2. Install WebSocket Bridge
```python
setup_websocket_bridge(hook_manager)
```
This connects the HookManager's event system to the WebSocket transport layer.

### 3. Mount FastAPI WebSocket Route
```python
@app.websocket("/ws/{pipeline_id}")
async def ws_endpoint(websocket: WebSocket, pipeline_id: str):
    await fastapi_websocket_endpoint(websocket, pipeline_id)
```
The WebSocket endpoint is served by your FastAPI application.

### 4. Attach Hooks to Pipeline
```python
pipeline = Pipeline(broker).with_hooks(hook_manager)
```
Without this, the pipeline won't emit events to the WebSocket.

### 5. Set pipeline_id
```python
pipeline.pipeline_id = "my_pipeline"
```
Required for clients to subscribe to specific pipelines.

---

## Customization

### Multiple Pipelines
```python
pipeline1 = Pipeline(broker).with_hooks(hook_manager)
pipeline1.pipeline_id = "pipeline_1"

pipeline2 = Pipeline(broker).with_hooks(hook_manager)
pipeline2.pipeline_id = "pipeline_2"
```

Clients can subscribe to specific pipeline IDs via `/ws/{pipeline_id}`.

### Event Filtering
```python
from taskiq_flow.hooks import EventFilter

# Only send step completion events
filter = EventFilter(
    pipeline_ids=["*"],
    event_types=["StepCompleteEvent", "PipelineCompleteEvent"]
)
hook_manager.add_filter(filter)
```

---

## Troubleshooting

### No Events Received
- Ensure `setup_websocket_bridge(hook_manager)` called **before** `pipeline.kiq()`
- Ensure `pipeline.with_hooks(hook_manager)` called
- Ensure `pipeline.pipeline_id` is set
- Ensure the FastAPI WebSocket route is mounted at `/ws/{pipeline_id}`

### Connection Refused
- Ensure the FastAPI app is running: `uvicorn examples.websocket_demo:app`
- Check that the host/port match the client connection string

### Events Out of Order
WebSocket delivers messages in order; if you see out-of-order, check for network issues or custom middleware emitting events incorrectly.

---

## Learning Path

After this example:

1. **[WebSocket Guide]({{ '/en/guides/websocket/' | relative_url }})** — Complete WebSocket setup, filtering, production deployment
2. **[Tracking Guide]({{ '/en/guides/tracking/' | relative_url }})** — Historical data storage alongside real-time events
3. **[API Guide]({{ '/en/guides/api/' | relative_url }})** — Expose via REST for non-WebSocket clients

---

*This example shows real-time streaming basics with the FastAPI-only WebSocket integration. For production, add authentication, connection pooling, and horizontal scaling with Redis Pub/Sub transport.*
