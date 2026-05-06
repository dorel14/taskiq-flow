---
permalink: /en/examples/tracking-demo/
title: Example: tracking_demo.py
nav_order: 45
color_scheme: dark
---
# Example: tracking_demo.py

**Pipeline execution tracking with PipelineTrackingManager**

> **Version**: 0.4.5 | **File**: `examples/tracking_demo.py`

---

## Overview

This example demonstrates how to monitor pipeline execution in real-time using the `PipelineTrackingManager`. It covers:

- Setting up tracking with automatic storage selection
- Attaching tracking to a pipeline
- Running a pipeline and checking its status
- Accessing step-by-step execution history

---

## What This Example Shows

- Creating a `PipelineTrackingManager` with auto-storage
- Using `.with_tracking()` on a pipeline
- Waiting for pipeline completion
- Querying pipeline status from the tracking manager
- Logging step progress

---

## Code Walkthrough

```python
import asyncio
import logging

from taskiq import InMemoryBroker
from taskiq_flow import Pipeline, PipelineMiddleware
from taskiq_flow.tracking import PipelineTrackingManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create broker
broker = InMemoryBroker(await_inplace=True)

# Define a task with a delay to show tracking in action
@broker.task
async def slow_task(x: int) -> int:
    """Slow task that doubles the input."""
    await asyncio.sleep(1)
    print(f"Slow task called with {x}")
    return x * 2

async def main():
    # 1. Setup tracking with auto-storage selection
    tracking_manager = PipelineTrackingManager().with_auto_storage(broker)

    # 2. Create middleware with tracking manager
    middleware = PipelineMiddleware(tracking_manager=tracking_manager)
    broker_with_middleware = broker.with_middlewares(middleware)

    # 3. Create pipeline with tracking enabled
    pipeline = (
        Pipeline(broker_with_middleware)
        .with_tracking(manager=tracking_manager)
        .call_next(slow_task)
        .call_next(slow_task)
    )

    # 4. Execute the pipeline
    result = await pipeline.kiq(10)
    await result.wait_result()

    # 5. Query the tracking status
    pipeline_id = pipeline.pipeline_id
    if pipeline_id is None:
        raise RuntimeError("Pipeline has no ID")

    status = await tracking_manager.get_status(pipeline_id)
    if status is None:
        raise RuntimeError("Failed to get pipeline status")

    logger.info(f"Pipeline status: {status.status}")
    logger.info(f"Steps completed: {len(status.steps)}")

asyncio.run(main())
```

---

## Key Points

### Tracking Setup

```python
tracking_manager = PipelineTrackingManager().with_auto_storage(broker)
```

- `with_auto_storage()` automatically selects the appropriate storage backend based on the broker
- For `InMemoryBroker`, uses `InMemoryPipelineStorage`
- For Redis brokers, uses `RedisPipelineStorage`

### Attaching Tracking to Pipeline

```python
pipeline = Pipeline(broker).with_tracking(manager=tracking_manager)
```

The tracking manager must be attached **before** calling `pipeline.kiq()`.

### Inspecting Status

After execution, the `PipelineStatus` object contains:

- `status` — Overall status (`COMPLETED`, `FAILED`, etc.)
- `steps` — List of `StepStatus` objects, one per step
- `started_at` / `completed_at` — Timestamps
- `duration_ms` — Total execution time
- `result` — Final return value (if completed)

Each `StepStatus` includes:

- `step_name` — Task name
- `status` — Step's status
- `duration_ms` — How long the step took
- `result` — Step's return value

---

## Expected Output

```
INFO:__main__:Pipeline status: COMPLETED
INFO:__main__:Steps completed: 2
```

You'll also see log messages from the slow_task calls with 1-second delays.

---

## Variations

### Access Step Details

```python
for step in status.steps:
    logger.info(f"Step '{step.step_name}' took {step.duration_ms:.0f}ms")
    if step.result:
        logger.info(f"  Result: {step.result}")
```

### Track Multiple Pipelines

```python
# Launch several pipelines concurrently
tasks = [pipeline.kiq(i) for i in range(5)]
await asyncio.gather(*[t.wait_result() for t in tasks])

# List all tracked pipelines
all_statuses = await tracking_manager.list_pipelines()
for s in all_statuses:
    print(f"{s.pipeline_id}: {s.status}")
```

---

## Learning Path

After this example:

1. **[Tracking Guide]({{ '/en/guides/tracking/' | relative_url }})** — Full tracking features (storage backends, metrics)
2. **[WebSocket Guide]({{ '/en/guides/websocket/' | relative_url }})** — Real-time streaming of tracking events
3. **[API Guide]({{ '/en/guides/api/' | relative_url }})** — Expose tracking data via REST API

---

*This example shows the basics. For production, use Redis storage and add listeners for alerts.*
