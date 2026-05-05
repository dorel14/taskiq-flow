# Example: websocket_demo.py

**Real-time pipeline event streaming with WebSocket**

> **Version**: 0.3.2 | **File**: `examples/websocket_demo.py`

---

## Overview

This example demonstrates how to set up a WebSocket server that streams real-time pipeline execution events. It covers:

- Creating a `HookManager` and connecting it to WebSocket transport
- Starting a WebSocket server on a specific host/port
- Subscribing to pipeline events from a client
- Observing live step completion events

**Note**: This is a minimal demo. For production use, add authentication, error handling, and proper connection management.

---

## What This Example Shows

- Setting up `HookManager` with `setup_websocket_bridge()`
- Attaching hooks to a pipeline
- Starting the WebSocket server
- How clients can connect and subscribe
- The event messages that are broadcast

---

## Code Walkthrough

```python
import asyncio
from taskiq import InMemoryBroker
from taskiq_flow import Pipeline
from taskiq_flow.hooks import HookManager, setup_websocket_bridge
from taskiq_flow.integration.websocket import get_websocket_server
from taskiq_flow.middleware import PipelineMiddleware

# Create broker
broker = InMemoryBroker(await_inplace=True).with_middlewares(PipelineMiddleware())

# Define simple tasks
@broker.task
def add_one(x: int) -> int:
    return x + 1

@broker.task
def multiply_by_two(x: int) -> int:
    return x * 2

async def main():
    # 1. Set up hook manager and WebSocket bridge
    hook_manager = HookManager()
    setup_websocket_bridge(hook_manager)

    # 2. Create pipeline and attach hooks
    pipeline = Pipeline(broker)
    pipeline.pipeline_id = "websocket_demo"
    pipeline.call_next(add_one, param_name="x")
    pipeline.call_next(multiply_by_two, param_name="x")
    pipeline.with_hooks(hook_manager)

    # 3. Start WebSocket server in background
    websocket_server = get_websocket_server()
    _ = asyncio.create_task(
        websocket_server.start_server("127.0.0.1", 8765),
    )

    print("WebSocket server started on ws://127.0.0.1:8765")
    msg = '{"pipeline_id": "websocket_demo"}'
    print(f"Connect a WebSocket client and subscribe with: {msg}")
    print("Then run the pipeline to see real-time events...")

    # Wait for server to start
    await asyncio.sleep(1)

    # 4. Execute the pipeline
    result = await pipeline.kiq(5)  # Start with 5 → 6 → 12
    print(f"Pipeline result: {result}")

    # Keep server running briefly
    await asyncio.sleep(5)
    print("Demo complete. Server will shut down.")

asyncio.run(main())
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

## Client Implementation (JavaScript)

Open a browser console or Node.js script:

```javascript
// Connect to WebSocket server
const ws = new WebSocket('ws://127.0.0.1:8765');

// Subscribe to the demo pipeline
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

### 3. Attach Hooks to Pipeline
```python
pipeline = Pipeline(broker).with_hooks(hook_manager)
```
Without this, the pipeline won't emit events to the WebSocket.

### 4. Set pipeline_id
```python
pipeline.pipeline_id = "my_pipeline"
```
Required for clients to subscribe to specific pipelines.

### 5. Start Server
```python
server = get_websocket_server(host="127.0.0.1", port=8765)
await server.start_server()
```

---

## Customization

### Change Port
```python
server = get_websocket_server(port=9000)
```

### Multiple Pipelines
```python
pipeline1 = Pipeline(broker).with_hooks(hook_manager)
pipeline1.pipeline_id = "pipeline_1"

pipeline2 = Pipeline(broker).with_hooks(hook_manager)
pipeline2.pipeline_id = "pipeline_2"
```

Clients can subscribe to specific pipeline IDs.

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

### Connection Refused
- Ensure `await server.start_server()` called before connecting
- Check that the host/port match client connection string

### Events Out of Order
WebSocket delivers messages in order; if you see out-of-order, check for network issues or custom middleware emitting events incorrectly.

---

## Learning Path

After this example:

1. **[WebSocket Guide](/docs/en/guides/websocket.md)** — Complete WebSocket setup, filtering, production deployment
2. **[Tracking Guide](/docs/en/guides/tracking.md)** — Historical data storage alongside real-time events
3. **[API Guide](/docs/en/guides/api.md)** — Expose via REST for non-WebSocket clients

---

*This example shows real-time streaming basics. For production, add authentication, connection pooling, and horizontal scaling with Redis Pub/Sub transport.*
