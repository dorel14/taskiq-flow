# Quick Start Guide

**Getting up and running with Taskiq-Flow in 5 minutes**

> **Version**: 0.3.2 | **Prerequisites**: Python 3.9+, asyncio basics

---

## Overview

This guide will help you create your first pipelines with Taskiq-Flow. By the end, you'll understand:

- How to set up a broker and add the PipelineMiddleware
- Defining tasks with `@broker.task`
- Building sequential pipelines with `.call_next()`, `.map()`, `.filter()`
- Running pipelines and retrieving results
- Basic dataflow pipelines with `@pipeline_task`

---

## Prerequisites

```bash
pip install taskiq taskiq-flow
```

For this guide, we'll use the in-memory broker which requires no external services.

---

## 1. Basic Sequential Pipeline

### 1.1. Setup

Create a new Python file `quickstart_basic.py`:

```python
import asyncio
from taskiq import InMemoryBroker
from taskiq_flow import Pipeline, PipelineMiddleware

# Initialize broker and add required middleware
broker = InMemoryBroker()
broker.add_middlewares(PipelineMiddleware())
```

### 1.2. Define Tasks

All functions in a pipeline must be taskiq tasks (decorated with `@broker.task`):

```python
@broker.task
def add_one(value: int) -> int:
    """Add 1 to the input value."""
    return value + 1

@broker.task
def repeat(value: int, times: int) -> list[int]:
    """Repeat a value multiple times."""
    return [value] * times

@broker.task
def is_positive(value: int) -> bool:
    """Check if value is non-negative."""
    return value >= 0
```

### 1.3. Build & Run the Pipeline

```python
async def main():
    # Build the pipeline by chaining operations
    pipeline = (
        Pipeline(broker)
        .call_next(add_one)           # Step 1: 1 → 2
        .call_next(repeat, times=4)   # Step 2: 2 → [2, 2, 2, 2]
        .map(add_one)                  # Step 3: apply to each element → [3, 3, 3, 3]
        .filter(is_positive)           # Step 4: keep elements where result is True
    )

    # Kick off the pipeline with initial input
    task = await pipeline.kiq(1)

    # Wait for completion and retrieve the result
    result = await task.wait_result()
    print("Result:", result.return_value)  # Output: [3, 3, 3, 3]

asyncio.run(main())
```

**Expected output**:
```
Result: [3, 3, 3, 3]
```

### 1.4. How It Works

| Step | Operation | Input | Output |
|------|-----------|-------|--------|
| 1 | `.call_next(add_one)` | `1` | `2` |
| 2 | `.call_next(repeat, times=4)` | `2` | `[2, 2, 2, 2]` |
| 3 | `.map(add_one)` | `[2, 2, 2, 2]` | `[3, 3, 3, 3]` (parallel) |
| 4 | `.filter(is_positive)` | `[3, 3, 3, 3]` | `[3, 3, 3, 3]` (unchanged) |

**Key points**:

- The `PipelineMiddleware` handles task routing; it **must** be added to your broker.
- Each step receives the previous step's output as input.
- `.map()` and `.filter()` operate on iterable results and run elements in parallel.
- `pipeline.kiq(initial_input)` starts the pipeline and returns a `Task` object.
- `task.wait_result()` blocks until the pipeline finishes.

---

## 2. Dataflow Pipeline (Automatic DAG)

For more complex workflows, use `DataflowPipeline` which automatically builds a dependency graph.

### 2.1. Define Tasks with `@pipeline_task`

Mark task outputs using the `@pipeline_task` decorator:

```python
from taskiq_flow import DataflowPipeline, pipeline_task

@broker.task
@pipeline_task(output="features")
def extract_audio(track_paths: list[str]) -> dict:
    """Extract audio features from tracks."""
    print(f"Extracting features from {len(track_paths)} tracks...")
    return {"duration": 180.0, "tempo": 120.0, "energy": 0.8}

@broker.task
@pipeline_task(output="tags")
def generate_tags(features: dict) -> list[str]:
    """Generate tags based on audio features."""
    print(f"Generating tags from features: {features}")
    return ["electronic", "dance", "upbeat"]

@broker.task
@pipeline_task(output="embedding")
def compute_embedding(features: dict) -> list[float]:
    """Compute vector embedding from features."""
    print(f"Computing embedding from {features}")
    return [0.1, 0.2, 0.3, 0.4, 0.5]
```

**How dependency resolution works**:
- `extract_audio` declares `output="features"`
- `generate_tags` has parameter `features: dict` → automatically depends on `extract_audio`
- `compute_embedding` also depends on `extract_audio` (same `features` param)
- Taskiq-Flow constructs a DAG and runs independent tasks in parallel

### 2.2. Build & Execute

```python
async def main():
    # Auto-build the DAG from task list
    pipeline = DataflowPipeline.from_tasks(
        broker,
        [extract_audio, generate_tags, compute_embedding]
    )

    # Optional: visualize the DAG
    pipeline.print_dag()

    # Execute with input data (only external inputs needed)
    results = await pipeline.kiq_dataflow(track_paths=["song1.mp3", "song2.mp3"])
    print("Results:", results)
    # Output: {
    #   "features": {"duration": 180.0, ...},
    #   "tags": ["electronic", "dance", "upbeat"],
    #   "embedding": [0.1, 0.2, 0.3, 0.4, 0.5]
    # }

asyncio.run(main())
```

**Sample DAG output** (printed to console):
```
DAG Execution Order:
  Level 0 (parallel): extract_audio
  Level 1 (parallel): generate_tags, compute_embedding
  Final outputs: features, tags, embedding
```

### 2.3. Visualizing the Pipeline

```python
# ASCII DAG in console
pipeline.print_dag()

# JSON representation for web UIs
viz_json = pipeline.visualize()
print(viz_json)

# DOT format for Graphviz
dot = pipeline.visualize_dot()
with open("pipeline.dot", "w") as f:
    f.write(dot)
# Render: dot -Tpng pipeline.dot -o pipeline.png
```

---

## 3. Common Patterns

### 3.1. Map-Reduce Pattern

Process items in parallel, then aggregate:

```python
from taskiq_flow import MapReduce

# Map phase: process each track independently
mapped = await MapReduce.map(
    broker,
    process_track,          # task function
    track_list,             # iterable of items
    output="processed",     # name of intermediate output
    max_parallel=10         # limit concurrency
)

# Reduce phase: aggregate all results
reduced = await MapReduce.reduce(
    broker,
    aggregate_results,      # aggregation function
    mapped,                 # MapReduceResult object
    input_name="processed", # consume the mapped output
    output="final_stats"
)

print("Final:", reduced.return_value)
```

See `examples/dataflow_audio_pipeline.py` for a complete audio-processing pipeline.

### 3.2. Group Parallel Execution

Run multiple independent tasks simultaneously:

```python
pipeline = Pipeline(broker)

pipeline.group(
    [task_a, task_b, task_c],
    param_names=["input_a", "input_b", "input_c"]
)
# Returns: [result_a, result_b, result_c]
```

### 3.3. Pipeline with Tracking

Monitor pipeline status in real-time:

```python
from taskiq_flow import PipelineTrackingManager

tracking = PipelineTrackingManager().with_auto_storage(broker)
pipeline = Pipeline(broker).with_tracking(tracking)

task = await pipeline.kiq(data)

# Check status later
status = await tracking.get_status(pipeline.pipeline_id)
print(f"Status: {status.status}, Steps completed: {len(status.steps)}")
```

---

## 4. Running Example Scripts

The `examples/` directory contains complete runnable demonstrations:

```bash
# Basic sequential pipeline
python examples/quickstart.py

# Tracking and monitoring
python examples/tracking_demo.py

# Scheduled pipelines (cron)
python examples/scheduled_pipeline.py

# Full dataflow DAG with map-reduce
python examples/dataflow_audio_pipeline.py

# Manual DAG construction with DataflowRegistry
python examples/registry_discovery_example.py

# WebSocket event streaming
python examples/websocket_demo.py

# REST API with FastAPI
python examples/api_example.py
```

---

## 5. Next Steps

With the basics under your belt, explore the deeper guides:

| Topic | Guide |
|-------|-------|
| Sequential & Dataflow Pipelines | [Pipelines Guide](/docs/en/guides/pipelines.md) |
| Task definitions & decorators | [Tasks Guide](/docs/en/guides/tasks.md) |
| Execution modes & error handling | [Execution Guide](/docs/en/guides/execution.md) |
| Real-time monitoring | [Tracking Guide](/docs/en/guides/tracking.md) |
| Live dashboards | [WebSocket Guide](/docs/en/guides/websocket.md) |
| Cron scheduling | [Scheduling Guide](/docs/en/guides/scheduling.md) |
| Error recovery | [Retry Guide](/docs/en/guides/retry.md) |
| Performance tuning | [Performance Guide](/docs/en/guides/performance.md) |
| REST API integration | [API Guide](/docs/en/guides/api.md) |
| Full API reference | [API Reference](/docs/en/api/) |

---

## Troubleshooting

### "PipelineMiddleware not found" Error

**Symptom**: Tasks fail with middleware errors.

**Fix**: Ensure `PipelineMiddleware()` is added to your broker before creating pipelines:

```python
broker.add_middlewares(PipelineMiddleware())  # Must be called
```

### "Task not found" or "Result is None"

**Symptom**: `wait_result()` returns `None`.

**Cause**: InMemoryBroker only works within the same process. For multi-worker distributed setups, use Redis or another persistent broker.

**Fix**: Switch to `RedisStreamBroker` with a shared result backend:

```python
from taskiq_flow.broker import RedisStreamBroker
broker = RedisStreamBroker(redis_url="redis://localhost:6379")
```

### WebSocket Connection Refused

**Symptom**: Client cannot connect to WebSocket server.

**Fix**: Ensure the WebSocket server is running and the port is accessible:

```python
server = get_websocket_server(host="0.0.0.0", port=8765)
await server.start_server()
```

---

## 📚 Further Reading

- **[Full API Reference](/docs/en/api/)** — Complete class and method documentation
- **[Example Gallery](/docs/en/examples/)** — Detailed walkthroughs of each example script
- **[Project README](../README.md)** — Project overview, installation, and philosophy

---

*Ready to dive deeper? Continue with the [Pipelines Guide](/docs/en/guides/pipelines.md).*
