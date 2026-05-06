---
title: WebSocket Guide
nav_order: 24
---
# WebSocket Guide

**Real-time event streaming for live dashboards and monitoring**

> **Version**: {VERSION} | **Related**: [Tracking Guide]({{ '/en/guides/tracking/' | relative_url }}), [API Guide]({{ '/en/guides/api/' | relative_url }})

---

## Overview

Taskiq-Flow's WebSocket integration provides live streaming of pipeline execution events — perfect for building real-time dashboards, progress displays, and monitoring tools.

This guide covers:

- Setting up a WebSocket server
- Subscribing clients to pipeline events
- Event types and payloads
- Transport layer configuration
- Production deployment considerations

---

## 1. Architecture

```
[Pipeline] → [HookManager] → [WebSocketBridge] → [WebSocket Server] → [Clients]
```

**Components**:

1. **Pipeline** — Emits events via hooks at each lifecycle stage
2. **HookManager** — Collects events from pipelines
3. **WebSocketBridge** — Connects HookManager to WebSocket transport
4. **WebSocketServer** — Manages client connections and broadcasts
5. **Client** — Web browser, monitoring app, dashboard

---

## 2. Quick Start

### 2.1. Server-Side Setup

```python
import asyncio
from taskiq import InMemoryBroker
from taskiq_flow import Pipeline
from taskiq_flow.hooks import HookManager, setup_websocket_bridge
from taskiq_flow.integration.websocket import get_websocket_server

# 1. Create broker and hook manager
broker = InMemoryBroker()
hook_manager = HookManager()

# 2. Set up the WebSocket bridge
setup_websocket_bridge(hook_manager)  # connects HookManager → WebSocket transport

# 3. Create a pipeline with hooks attached
pipeline = Pipeline(broker)
pipeline.pipeline_id = "demo_workflow"
pipeline.with_hooks(hook_manager)

# Add tasks to pipeline...

# 4. Start WebSocket server
async def main():
    server = get_websocket_server(host="0.0.0.0", port=8765)
    await server.start_server()

    # 5. Execute the pipeline
    result = await pipeline.kiq(data)

    # 6. Keep server alive (or integrate into your app's event loop)
    await asyncio.Event().wait()

asyncio.run(main())
```

### 2.2. Client Connection (JavaScript)

```javascript
// Connect to WebSocket server
const ws = new WebSocket('ws://localhost:8765');

// Subscribe to a specific pipeline
ws.onopen = () => {
    ws.send(JSON.stringify({
        type: 'subscribe',
        pipeline_id: 'demo_workflow'
    }));
};

// Receive events
ws.onmessage = (event) => {
    const eventData = JSON.parse(event.data);
    console.log('Pipeline event:', eventData);

    switch (eventData.type) {
        case 'PipelineStartEvent':
            showPipelineStarted();
            break;
        case 'StepStartEvent':
            showStepProgress(eventData.step_name);
            break;
        case 'StepCompleteEvent':
            updateProgress(eventData.step_name, eventData.duration_ms);
            break;
        case 'PipelineCompleteEvent':
            showResults(eventData.result);
            break;
        case 'PipelineErrorEvent':
            showError(eventData.error);
            break;
    }
};
```

---

## 3. Event Types

All events are JSON-serializable with a `type` field indicating the event kind.

### 3.1. PipelineStartEvent

```json
{
  "type": "PipelineStartEvent",
  "pipeline_id": "demo_workflow",
  "pipeline_type": "sequential",
  "timestamp": "2026-04-29T18:50:19+02:00",
  "input": {...}
}
```

Emitted when a pipeline begins execution.

### 3.2. StepStartEvent

```json
{
  "type": "StepStartEvent",
  "pipeline_id": "demo_workflow",
  "step_name": "process_data",
  "step_index": 2,
  "task_id": "abc123",
  "timestamp": "2026-04-29T18:50:19.5+02:00"
}
```

Emitted before each step starts.

### 3.3. StepCompleteEvent

```json
{
  "type": "StepCompleteEvent",
  "pipeline_id": "demo_workflow",
  "step_name": "process_data",
  "step_index": 2,
  "result": {"processed": 42},
  "duration_ms": 150.5,
  "timestamp": "2026-04-29T18:50:19.7+02:00"
}
```

Emitted after each step completes successfully.

### 3.4. PipelineCompleteEvent

```json
{
  "type": "PipelineCompleteEvent",
  "pipeline_id": "demo_workflow",
  "pipeline_type": "sequential",
  "status": "COMPLETED",
  "duration_ms": 1250.3,
  "result": {"final": "output"},
  "timestamp": "2026-04-29T18:50:20.5+02:00"
}
```

Emitted when the entire pipeline finishes successfully.

### 3.5. StepErrorEvent

```json
{
  "type": "StepErrorEvent",
  "pipeline_id": "demo_workflow",
  "step_name": "failing_task",
  "error": "ValueError: invalid input",
  "timestamp": "2026-04-29T18:50:19.9+02:00"
}
```

Emitted when a step fails.

### 3.6. PipelineErrorEvent

```json
{
  "type": "PipelineErrorEvent",
  "pipeline_id": "demo_workflow",
  "error": "Pipeline failed at step 'validate'",
  "timestamp": "2026-04-29T18:50:20.2+02:00"
}
```

Emitted when the pipeline aborts due to an unrecoverable error.

---

## 4. Client-Side Implementation

### 4.1. Basic JavaScript Client

```javascript
class PipelineMonitor {
    constructor(url, pipelineId) {
        this.url = url;
        this.pipelineId = pipelineId;
        this.ws = null;
        this.events = [];
        this.callbacks = {};
    }

    connect() {
        this.ws = new WebSocket(this.url);

        this.ws.onopen = () => {
            console.log('Connected to WebSocket server');
            this.subscribe(this.pipelineId);
        };

        this.ws.onmessage = (event) => {
            const data = JSON.parse(event.data);
            this.handleEvent(data);
        };

        this.ws.onerror = (err) => {
            console.error('WebSocket error:', err);
        };

        this.ws.onclose = () => {
            console.log('WebSocket connection closed');
            this.reconnect();
        };
    }

    subscribe(pipelineId) {
        this.ws.send(JSON.stringify({
            type: 'subscribe',
            pipeline_id: pipelineId
        }));
    }

    handleEvent(event) {
        this.events.push(event);
        const eventType = event.type;

        if (this.callbacks[eventType]) {
            this.callbacks[eventType](event);
        }

        // Generic event handler
        if (this.callbacks['*']) {
            this.callbacks['*'](event);
        }
    }

    on(eventType, callback) {
        this.callbacks[eventType] = callback;
    }

    reconnect() {
        setTimeout(() => this.connect(), 3000);
    }
}

// Usage
monitor = new PipelineMonitor('ws://localhost:8765', 'pipeline_123');
monitor.on('StepCompleteEvent', (event) => {
    console.log(`Step ${event.step_name} completed in ${event.duration_ms}ms`);
});
monitor.on('PipelineCompleteEvent', (event) => {
    console.log('Pipeline finished with status:', event.status);
});
monitor.connect();
```

### 4.2. Python Client (for scripts)

```python
import asyncio
import websockets
import json

async def monitor_pipeline(uri, pipeline_id):
    async with websockets.connect(uri) as websocket:
        # Subscribe
        await websocket.send(json.dumps({
            "type": "subscribe",
            "pipeline_id": pipeline_id
        }))

        # Receive events
        async for message in websocket:
            event = json.loads(message)
            print(f"[{event['type']}] {event}")

            if event['type'] == 'PipelineCompleteEvent':
                print(f"Pipeline finished: {event['status']}")

asyncio.run(monitor_pipeline('ws://localhost:8765', 'pipeline_123'))
```

---

## 5. Subscription Management

### 5.1. Subscribing to a Pipeline

Clients send a subscription message:

```json
{
  "type": "subscribe",
  "pipeline_id": "my_pipeline_001"
}
```

After subscribing, all events for that pipeline are forwarded.

### 5.2. Unsubscribing

```json
{
  "type": "unsubscribe",
  "pipeline_id": "my_pipeline_001"
}
```

### 5.3. Subscribing to All Pipelines (Wildcard)

```json
{
  "type": "subscribe",
  "pipeline_id": "*"
}
```

**Caution**: Broadcasting all events can generate significant traffic in high-throughput systems.

### 5.4. Multiple Subscriptions

A client can subscribe to multiple pipelines:

```javascript
monitor.subscribe('pipeline_1');
monitor.subscribe('pipeline_2');
// Receive events for both, distinguished by pipeline_id field
```

---

## 6. Server Configuration

### 6.1. Custom Host and Port

```python
# Use specific interface and port
server = get_websocket_server(host='127.0.0.1', port=8765)
await server.start_server()

# Or bind to all interfaces (expose to network)
server = get_websocket_server(host='0.0.0.0', port=8765)
```

### 6.2. CORS and Security Headers

If behind a reverse proxy (nginx, Traefik), configure CORS headers:

```nginx
# nginx.conf
location /ws {
    proxy_pass http://localhost:8765;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "upgrade";
    add_header Access-Control-Allow-Origin "*";
    add_header Access-Control-Allow-Credentials true;
}
```

### 6.3. SSL/TLS Termination

Terminate SSL at reverse proxy:

```nginx
# HTTPS → WSS forwarding
location /ws {
    proxy_pass http://localhost:8765;
    # WSS (secure WebSocket) handled by nginx SSL config
}
```

Client connects with:

```javascript
const ws = new WebSocket('wss://yourdomain.com/ws');
```

### 6.4. Multiple Workers

For multiple Python worker processes, each needs its own WebSocket server on a different port (or use a message broker like Redis Pub/Sub to coordinate):

```python
# Worker 1
server1 = get_websocket_server(port=8765)

# Worker 2
server2 = get_websocket_server(port=8766)

# Load balancer distributes WebSocket connections
```

For true multi-worker event broadcasting, use the Redis-based transport:

```python
from taskiq_flow.transport import RedisPubSubTransport

transport = RedisPubSubTransport(redis_client)
server = get_websocket_server(transport=transport)
# Now all workers share event state via Redis
```

---

## 7. Filtering Events

Reduce bandwidth by filtering on the server side:

```python
from taskiq_flow.hooks import EventFilter

# Only send events for specific pipelines
filter = EventFilter(pipeline_ids=['pipeline_1', 'pipeline_2'])
hook_manager.add_filter(filter)

# Only send step events (not pipeline-level)
filter = EventFilter(event_types=['StepStartEvent', 'StepCompleteEvent'])
hook_manager.add_filter(filter)
```

Client-side filtering also possible:

```javascript
monitor.on('StepCompleteEvent', (event) => {
    if (event.step_name === 'important_step') {
        highlightStep(event.step_name);
    }
});
```

---

## 8. Message Format Reference

### Subscription Request

| Field | Type | Description |
|-------|------|-------------|
| `type` | `"subscribe"` | Message type |
| `pipeline_id` | `str` or `"*"` | Pipeline to subscribe to |

### Unsubscription Request

| Field | Type | Description |
|-------|------|-------------|
| `type` | `"unsubscribe"` | Message type |
| `pipeline_id` | `str` | Pipeline to unsubscribe from |

### Event Message (server → client)

| Field | Type | Description |
|-------|------|-------------|
| `type` | `str` | Event type (see Section 3) |
| `pipeline_id` | `str` | Origin pipeline ID |
| `timestamp` | `ISO 8601 str` | Event time |

Additional fields per event type (see above).

---

## 9. Production Deployment

### 9.1. Docker Deployment

```dockerfile
# Dockerfile
FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

CMD ["python", "-m", "my_websocket_app"]
```

```yaml
# docker-compose.yml
version: '3.8'
services:
  redis:
    image: redis:7-alpine
  app:
    build: .
    ports:
      - "8765:8765"
    environment:
      - REDIS_URL=redis://redis:6379
    depends_on:
      - redis
```

### 9.2. Systemd Service

```ini
# /etc/systemd/system/taskiq-flow-ws.service
[Unit]
Description=Taskiq-Flow WebSocket Server
After=network.target

[Service]
Type=simple
User=appuser
WorkingDirectory=/opt/taskiq-flow
ExecStart=/usr/bin/python3 -m my_websocket_app
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

### 9.3. Monitoring

Health check endpoint:

```python
from aiohttp import web

async def health(request):
    return web.json_response({"status": "healthy"})

app = web.Application()
app.router.add_get('/health', health)
```

Or use the built-in API health endpoint (/health) from [API Guide]({{ '/en/guides/api/' | relative_url }}).

### 9.4. Scalability

For high-volume deployments:

- **Horizontal scaling**: Deploy multiple WebSocket server instances with sticky sessions or Redis Pub/Sub transport
- **Load balancing**: Use nginx or HAProxy with WebSocket support
- **Connection limits**: Configure max connections per worker (OS limits)
- **Message compression**: Enable permessage-deflate for large payloads

---

## 10. Security Considerations

### 10.1. Authentication

Require authentication tokens on connection:

```python
# Server-side validation
async def authenticate(websocket, token):
    if not validate_token(token):
        await websocket.close(code=4001, reason="Unauthorized")
        return False
    return True

# Client sends token upon connection
ws = new WebSocket(`ws://localhost:8765?token=${authToken}`);
```

### 10.2. Authorization

Filter events by user permissions:

```python
class AuthFilter(EventFilter):
    def __init__(self, user_id, allowed_pipelines):
        self.user_id = user_id
        self.allowed = allowed_pipelines

    def should_emit(self, event):
        return event.pipeline_id in self.allowed
```

### 10.3. Rate Limiting

Prevent abuse:

```python
from collections import defaultdict
import time

class RateLimiter:
    def __init__(self, max_events_per_second=100):
        self.limits = defaultdict(list)

    def allow(self, client_id):
        now = time.time()
        self.limits[client_id] = [
            t for t in self.limits[client_id] if now - t < 1
        ]
        if len(self.limits[client_id]) < 100:
            self.limits[client_id].append(now)
            return True
        return False
```

---

## 11. Troubleshooting

### Connection Refused

**Symptom**: Client can't connect, "Connection refused" error.

**Fixes**:
- Verify server is running: `netstat -lnp | grep 8765`
- Check firewall rules allow port 8765
- Ensure host binding matches (0.0.0.0 for external access)

### No Events Received After Connection

**Symptom**: Connection succeeds, but no events arrive.

**Fixes**:
- Ensure pipeline has `pipeline_id` set
- Confirm `pipeline.with_hooks(hook_manager)` called
- Verify `setup_websocket_bridge(hook_manager)` called before pipeline starts
- Check subscription message format (see Section 5)

### High Memory Usage

**Symptom**: Server memory grows over time.

**Fixes**:
- Limit number of tracked pipelines
- Implement automatic cleanup of disconnected clients
- Use Redis transport to offload state from process memory
- Set max connections limit

### Events Out of Order

**Symptom**: Client receives StepComplete before StepStart.

**Fixes**:
- Use sequential delivery guarantees (default for WebSocket)
- Ensure all hooks are correctly attached
- Check for custom middleware that may emit events asynchronously

---

## 12. Summary

| Component | Responsibility |
|-----------|----------------|
| `Pipeline` | Generates execution events |
| `HookManager` | Collects events from pipelines |
| `WebSocketBridge` | Routes events to WebSocket transport |
| `WebSocketServer` | Manages client connections, broadcasts |
| `Client` | Subscribes, receives, displays events |

**Basic setup (5 lines)**:

```python
hooks = HookManager()
setup_websocket_bridge(hooks)
pipeline = Pipeline(broker).with_hooks(hooks)
server = get_websocket_server()
await server.start_server()
```

---

## Next Steps

- **[Tracking Guide]({{ '/en/guides/tracking/' | relative_url }})** — Backend storage and historical queries
- **[API Guide]({{ '/en/guides/api/' | relative_url }})** — REST endpoints for dashboard backends
- **[Examples: WebSocket Demo]({{ '/en/examples/websocket-demo/' | relative_url }})** — Complete working code

---

*Stream live pipeline events. Combine with [Tracking Storage]({{ '/en/guides/tracking/' | relative_url }}) for persistent history.*
