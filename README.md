# Taskiq-Flow

Taskiq-Flow lets you chain intensive functions together and fire them off without waiting for each to complete. Think of it as orchestration for async task workflows—you define the steps, and the library handles the execution order, data passing, and parallelism automatically.

*Version: 0.2.0*

## Inspiration & Design Philosophy

Taskiq-Flow is built on the shoulders of two excellent projects:

### From taskiq-pipelines
- **Sequential pipeline chaining** — The original `Pipeline` class with `.call_next()`, `.map()`, `.filter()`, and `.group()` operations.
- **Middleware-based orchestration** — The `PipelineMiddleware` that intercepts task completion and triggers the next step.
- **Tracking & monitoring** — Pipeline execution tracking, status management, and storage backends.
- **Scheduling** — Cron-based pipeline scheduling via `PipelineScheduler`.
- **WebSocket hooks** — Real-time event streaming through a hook system.

### From pipefunc
- **Declarative dataflow** — Automatic DAG construction from task dependencies using `@pipeline_task(output=...)` annotations.
- **Implicit dependency resolution** — Tasks declare what they produce; downstream tasks automatically receive needed inputs by parameter name matching.
- **Parallel execution** — Independent tasks run concurrently; the library handles data passing and synchronization.
- **Map-reduce helpers** — First-class support for parallel processing and aggregation patterns.

### The Result
Taskiq-Flow combines taskiq-pipelines' battle-tested orchestration with pipefunc's elegant dataflow programming model, giving you:
- **Sequential pipelines** for straightforward linear workflows.
- **Dataflow pipelines** for complex, branched, or parallel workflows where tasks naturally depend on each other.
- **Unified tracking, scheduling, and monitoring** across both styles.
- **Full async support** with taskiq's distributed broker backends (Redis, Kafka, RabbitMQ, etc.).

## Installation

Install from PyPI:

```bash
pip install taskiq-flow
```

For optional features:

```bash
# All features (tracking, scheduling, visualization)
pip install taskiq-flow[all]

# Just Redis support for tracking/storage
pip install taskiq-flow[redis]

# With scheduling capabilities
pip install taskiq-flow[scheduler]
```

### Basic Setup

Add the `PipelineMiddleware` to your broker. This middleware is the engine that decides what to execute next after each step finishes.

```python
from taskiq_flow.middleware import PipelineMiddleware

broker = ...  # Your broker (RedisStreamBroker, InMemoryBroker, etc.)
broker.add_middlewares(PipelineMiddleware())
```

**Important**: Your broker needs a shared result backend (Redis, database, etc.) so workers can read results. The `InMemoryBroker` works for local development only.


### Quick Example

Here's a simple pipeline that processes a value through several steps:

```python
import asyncio
from taskiq import InMemoryBroker
from taskiq_flow import Pipeline, PipelineMiddleware

broker = InMemoryBroker()
broker.add_middlewares(PipelineMiddleware())

@broker.task
def add_one(value: int) -> int:
    return value + 1

@broker.task
def repeat(value: int, times: int) -> list[int]:
    return [value] * times

@broker.task
def is_positive(value: int) -> bool:
    return value >= 0

async def main():
    pipeline = (
        Pipeline(broker)
        .call_next(add_one)           # 1 → 2
        .call_next(repeat, times=4)   # 2 → [2, 2, 2, 2]
        .map(add_one)                  # [2, 2, 2, 2] → [3, 3, 3, 3]
        .filter(is_positive)           # [3, 3, 3, 3] (unchanged)
    )

    task = await pipeline.kiq(1)
    result = await task.wait_result()
    print("Result:", result.return_value)  # [3, 3, 3, 3]

if __name__ == "__main__":
    asyncio.run(main())
```

Two things to keep in mind:

1. **All functions in the pipeline must be tasks** (decorated with `@broker.task`). Regular functions need to be wrapped.
2. The `PipelineMiddleware` must be added to your broker before creating pipelines.


## Core Concepts

### Pipeline Types

Taskiq-pipelines offers two main approaches:

#### 1. Classic Sequential Pipeline
The original `Pipeline` class where you manually chain steps in order:

```python
pipeline = Pipeline(broker).call_next(task1).call_next(task2).map(task3)
```

Use this when you have a fixed, linear flow with occasional branching (map/filter).

#### 2. Dataflow Pipeline (Recommended for complex workflows)

The `DataflowPipeline` builds a directed acyclic graph (DAG) automatically from task dependencies. Declare what data each task produces and consumes, and the library figures out the rest.

```python
from taskiq_flow import DataflowPipeline, pipeline_task

@broker.task
@pipeline_task(output="features")
def extract_features(data): ...

@broker.task
@pipeline_task(output="stats")
def compute_stats(features):  # automatically receives 'features'
    ...

pipeline = DataflowPipeline.from_tasks(
    broker,
    [extract_features, compute_stats]
)
results = await pipeline.kiq_dataflow(data=my_data)
```

This is powerful for audio/video processing, ETL jobs, or any workflow where tasks naturally depend on each other's outputs.

## Available Steps & Operations

The library provides several step types and operations you can use in pipelines.

### Sequential Steps (`.call_next` / `.call_after`)

Calls a task with the previous step's result as input:

```python
pipeline.call_next(process_data).call_next(save_result)
```

- `call_next(task, ...)` – passes the previous result as the first argument (or via `param_name=`)
- `call_after(task, ...)` – runs a task without passing the previous result (fire-and-forget style)

### Map & Filter (`.map` / `.filter`)

Operations on iterable results:

```python
pipeline.map(process_item)     # Apply to each element in parallel
pipeline.filter(validate)       # Keep elements where task returns True
```

Both require the previous result to be iterable.

### Group Step (`.group`)

Execute multiple independent tasks in parallel and collect all results:

```python
pipeline.group(
    [task_a, task_b, task_c],
    param_names=["x", "y", "z"]
)
```

All tasks start together; results come back as a list in order.

## Map-Reduce Pattern

For batch processing, use the built-in map-reduce helpers:

```python
from taskiq_flow import DataflowPipeline

pipeline = DataflowPipeline(broker)

# Process multiple items in parallel
pipeline.map(
    process_single_track,
    track_list,
    output="track_features",
    max_parallel=10
)

# Aggregate results
pipeline.reduce(
    aggregate_features,
    input_name="track_features",
    output="stats"
)

results = await pipeline.kiq_map_reduce()
```

Or use the standalone `MapReduce` utility:

```python
from taskiq_flow import MapReduce

mapped = await MapReduce.map(
    broker, process_item, items, output="processed"
)
reduced = await MapReduce.reduce(
    broker, aggregate, mapped, output="final"
)
```

*See `examples/dataflow_audio_pipeline.py` (Example 3) for a full walk-through.*

## Pipeline Scheduling

Schedule pipelines to run periodically or at specific times with cron-like schedules:

```python
from taskiq_flow import Pipeline, PipelineScheduler

scheduler = PipelineScheduler(broker)

pipeline = Pipeline(broker).call_next(my_task)

# Run every minute
job_id = await scheduler.schedule(
    pipeline,
    cron="* * * * *",
    args=("some", "data")
)

# Start the scheduler (runs in background)
await scheduler.start()

# ... keep your app running ...

await scheduler.shutdown()
```

Other scheduling options:

- `scheduler.schedule_interval(pipeline, minutes=5)` — every 5 minutes
- `scheduler.schedule_at(pipeline, run_at=datetime(...))` — one-off run

*Example: `examples/scheduled_pipeline.py`*

## Pipeline Visualization

Inspect your dataflow pipeline as DAG:

```python
# ASCII art in console
pipeline.print_dag()

# JSON for web UI / APIs
viz_json = pipeline.visualize()

# DOT format for Graphviz
dot = pipeline.visualize_dot()
# Save and render: dot -Tpng pipeline.dot -o pipeline.png
```

Useful for debugging complex workflows and sharing pipeline structure with team members.

*Example: `examples/dataflow_audio_pipeline.py` (Example 4)*

## Pipeline Tracking & Monitoring

Track pipeline executions in real-time with the `PipelineTrackingManager`:

```python
from taskiq_flow import Pipeline, PipelineTrackingManager

tracking = PipelineTrackingManager().with_auto_storage(broker)

pipeline = Pipeline(broker).with_tracking(tracking)
task = await pipeline.kiq(some_data)

# Check status
status = await tracking.get_status(pipeline.pipeline_id)
print(f"Status: {status.status}, Steps: {len(status.steps)}")
```

Storage backends:
- `InMemoryPipelineStorage` – transient, for development
- `RedisPipelineStorage` – persistent, multi-worker

You can also hook into step-level events via the tracking manager's callbacks.

## Dataflow Pipelines

The `DataflowPipeline` builds a directed acyclic graph (DAG) automatically from task dependencies. Instead of manually chaining, you declare what data each task produces and consumes using the `@pipeline_task` decorator—the library figures out the rest.

### The `@pipeline_task` Decorator

Mark tasks with their inputs/outputs:

```python
from taskiq_flow import pipeline_task

@broker.task
@pipeline_task(output="audio_features")
async def extract_audio(track_paths: list[str]) -> dict:
    # produces audio_features
    return {"duration": 180.0, "tempo": 120.0}

@broker.task
@pipeline_task(output="tags")
async def generate_tags(audio_features: dict) -> list[str]:
    # automatically receives audio_features as input
    return ["electronic", "dance"]
```

The pipeline automatically detects that `generate_tags` depends on `extract_audio` because the function signature includes `audio_features`.

### Building & Running a Dataflow Pipeline

```python
from taskiq_flow import DataflowPipeline

pipeline = DataflowPipeline.from_tasks(
    broker,
    [extract_audio, generate_tags, compute_embedding]
)

# See the execution order
pipeline.print_dag()

# Execute with automatic parallelism
results = await pipeline.kiq_dataflow(track_paths=["song.mp3"])
# results = {"audio_features": ..., "tags": ..., "embedding": ...}
```

Tasks with no dependencies run first. Independent tasks (like `generate_tags` and `compute_mir_features`) run in parallel automatically.

*Full example: `examples/dataflow_audio_pipeline.py`*

## WebSocket Tracking

Taskiq-Flow supports real-time tracking of pipeline execution via WebSockets. You can receive live updates about pipeline and step status as they happen.

### Setting up WebSocket Tracking

```python
import asyncio
from taskiq import InMemoryBroker
from taskiq_flow import Pipeline
from taskiq_flow.hooks import HookManager, setup_websocket_bridge
from taskiq_flow.integration.websocket import get_websocket_server

# Create broker and hook manager
broker = InMemoryBroker()
hook_manager = HookManager()

# Set up WebSocket bridge
setup_websocket_bridge(hook_manager)

# Create pipeline and attach hook manager
pipeline = Pipeline(broker)
pipeline.pipeline_id = "demo_pipeline"  # set a known ID for WebSocket clients
pipeline.with_hooks(hook_manager)

# Add your pipeline steps...

# Start WebSocket server (configure host/port as needed)
async def main():
    # Configure server host and port
    server = get_websocket_server(host="127.0.0.1", port=8765)
    # Or override at startup: server = get_websocket_server()
    # asyncio_server = await server.start_server("127.0.0.1", 8765)

    asyncio_server = await server.start_server()

    # Run your pipeline
    result = await pipeline.kiq(some_data)

    # Keep server running
    await asyncio_server.serve_forever()

asyncio.run(main())
```

### Connecting WebSocket Clients

WebSocket clients can connect to `ws://127.0.0.1:8765` and subscribe to specific pipelines by sending a JSON message:

```json
{"pipeline_id": "demo_pipeline"}
```

Once subscribed, clients will receive real-time events like:

```json
{
  "type": "PipelineStartEvent",
  "pipeline_id": "demo_pipeline",
  "timestamp": "2026-04-29T18:50:19+02:00"
}
```

Available event types:
- `PipelineStartEvent` - Pipeline execution started
- `StepStartEvent` - A pipeline step started
- `StepCompleteEvent` - A pipeline step completed
- `PipelineCompleteEvent` - Pipeline execution completed
- `StepErrorEvent` - A pipeline step failed
- `PipelineErrorEvent` - Pipeline execution failed

See `examples/websocket_demo.py` for a complete working example.

## Example Scripts

The `examples/` directory includes:

- `quickstart.py` – basic pipeline with sequential steps, map, and filter
- `tracking_demo.py` – monitoring pipeline status with the tracking manager
- `scheduled_pipeline.py` – cron-based recurring pipeline execution
- `dataflow_audio_pipeline.py` – full-featured dataflow DAG, parallelism, map-reduce, and visualization
- `websocket_demo.py` – WebSocket server streaming live pipeline events

Run any example directly: `python examples/quickstart.py`
