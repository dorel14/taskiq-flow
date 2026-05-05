---
title: Pipeline Execution Guide
nav_order: 22
---
# Pipeline Execution Guide

**Understanding execution models, modes, and result handling**

> **Version**: 0.3.2 | **Applies to**: SequentialPipeline, DataflowPipeline, MapReduce

---

## Overview

This guide covers how Taskiq-Flow executes pipelines, manages concurrency, handles errors, and returns results.

---

## 1. Execution Models

### 1.1. Sequential Execution (Classic Pipeline)

The classic `Pipeline` executes steps one after another in a linear chain:

```python
pipeline = Pipeline(broker).call_next(task1).call_next(task2).call_next(task3)
# Execution order: task1 → task2 → task3 (synchronously)
```

**Characteristics**:
- Each step waits for the previous to complete
- Results pass directly from one step to the next
- Predictable, deterministic execution order
- Suitable for linear workflows

### 1.2. Parallel Execution (Dataflow & Map)

`DataflowPipeline` automatically parallelizes independent tasks:

```python
@broker.task
@pipeline_task(output="features")
def extract(tracks): ...

@broker_task
@pipeline_task(output="tags")
def tag(features): ...  # Runs after extract

@broker.task
@pipeline_task(output="embedding")
def embed(features): ...  # Also runs after extract, in parallel with tag

pipeline = DataflowPipeline.from_tasks(broker, [extract, tag, embed])
# DAG: extract → (tag & embed in parallel)
```

**Characteristics**:
- Tasks with no unmet dependencies run concurrently
- DAG determines execution order
- Maximum throughput for independent operations
- Controlled by `max_parallel` parameter on `.map()` and `.reduce()`

### 1.3. Map-Reduce Parallelism

The `MapReduce` utility explicitly processes items in parallel:

```python
from taskiq_flow import MapReduce

# Process 100 items with max 10 concurrent workers
result = await MapReduce.map(
    broker,
    process_item,
    items=items_list,
    output="processed",
    max_parallel=10  # controls concurrency level
)
```

**Parallelism control**:
- `max_parallel=None` → unlimited concurrency (use with caution)
- `max_parallel=1` → sequential execution
- Recommended: `max_parallel = number_of_cpu_cores * 2` for CPU-bound tasks

---

## 2. Starting a Pipeline

There are several ways to kick off pipeline execution:

### 2.1. `pipeline.kiq(...)` — Fire and Forget

Returns a `Task` immediately; you must manually wait for results:

```python
task = await pipeline.kiq(initial_input)
# Do other things...
result = await task.wait_result()  # blocks until complete
```

Use when:
- You need to track the task ID for later status checks
- You want to start multiple pipelines concurrently
- You're building a task queue system

### 2.2. `pipeline.kiq_dataflow(...)` — Dataflow Convenience

Same as `kiq()` but specifically for DataflowPipeline, with clearer semantics:

```python
results = await pipeline.kiq_dataflow(track_paths=["a.mp3", "b.mp3"])
# Returns: dict mapping output names → values
```

### 2.3. `pipeline.kiq_map_reduce(...)` — Map-Reduce Shortcut

Combined map and reduce in one call:

```python
final = await pipeline.kiq_map_reduce(
    items=items,
    map_output="processed",
    reduce_output="final"
)
```

---

## 3. Waiting for Results

### 3.1. Blocking Wait

```python
task = await pipeline.kiq(data)
result = await task.wait_result()  # blocks
print(result.return_value)
```

**Options**:
- `wait_result(timeout=30)` — timeout in seconds (raises `asyncio.TimeoutError`)
- `wait_result(raise_on_error=True)` — re-raise exceptions from tasks

### 3.2. Polling for Status

```python
task = await pipeline.kiq(data)

# Check periodically without blocking
while not task.is_finished:
    await asyncio.sleep(0.5)
    status = await task.get_status()
    print(f"Status: {status}")
```

Useful for progress bars or interactive applications.

### 3.3. Fetch by Task ID (Distributed)

If you have only the task ID (from another process):

```python
from taskiq import Task
task = Task(task_id="abc123", broker=broker)
result = await task.wait_result()
```

---

## 4. Error Handling

### 4.1. Task-Level Errors

When a single task fails, the pipeline either:

- **Stops immediately** (default) — remaining tasks are cancelled
- **Continues** if configured with error handling policies

```python
pipeline = Pipeline(broker)

# Configure to continue despite errors
pipeline.on_error("continue")  # options: "stop", "continue", "retry"

# Or use a retry policy (see Retry Guide)
pipeline.with_retry(
    max_attempts=3,
    delay=5,
    backoff=2
)
```

### 4.2. Pipeline-Level Errors

The entire pipeline may fail if:

- A critical task (no consumers) fails
- A task times out
- The broker becomes unavailable

Handle pipeline errors with try/except:

```python
try:
    result = await pipeline.kiq(data)
    output = await result.wait_result()
except TaskiqError as exc:
    print(f"Pipeline failed: {exc}")
    # Access partial results if any
    if result.is_failed:
        print(f"Failed at step: {result.failed_step}")
```

### 4.3. Partial Results on Failure

Even if a pipeline fails, you may have partial results from completed steps:

```python
result = await pipeline.kiq(data)
try:
    output = await result.wait_result()
except PipelineError:
    # Some steps succeeded before failure
    partial = result.partial_results  # dict of completed outputs
    print(f"Partial: {partial}")
```

---

## 5. Timeouts

Set timeouts at the pipeline level:

```python
pipeline = Pipeline(broker)

# Global timeout for entire pipeline (seconds)
pipeline.with_timeout(60)

# Or per-task timeout via taskiq task decorator
@broker.task(timeout=30)
def slow_task(): ...
```

**Timeout behavior**:
- Exceeding the timeout cancels the running task
- `asyncio.TimeoutError` is raised
- Pipeline state is set to `ERROR`

---

## 6. Execution Context

Each task receives an optional `context` parameter containing metadata:

```python
from taskiq_flow import PipelineContext

@broker.task
async def my_task(data: str, context: PipelineContext):
    print(f"Pipeline ID: {context.pipeline_id}")
    print(f"Step number: {context.step_index}")
    print(f"Task ID: {context.task_id}")
    return data.upper()
```

**Context fields**:

| Field | Type | Description |
|-------|------|-------------|
| `pipeline_id` | `str` | Unique identifier of the pipeline instance |
| `step_index` | `int` | Index of this step in the pipeline sequence |
| `task_id` | `str` | ID of the underlying taskiq task |
| `execution_mode` | `str` | `"sequential"`, `"parallel"`, or `"map_reduce"` |
| `started_at` | `datetime` | Timestamp when pipeline started |
| `broker` | `BaseBroker` | Reference to the task broker |

Enable context passing when building the pipeline:

```python
pipeline = Pipeline(broker).with_context(enable=True)
```

---

## 7. Custom Execution Engines (Advanced)

For low-level control, use `ExecutionEngine` directly:

```python
from taskiq_flow import ExecutionEngine, DAGBuilder
from taskiq_flow.dataflow import DataflowRegistry

# Build registry manually
registry = DataflowRegistry()
registry.register_task(load, output="raw", inputs=[])
registry.register_task(process, output="clean", inputs=["raw"])
registry.register_task(save, output="saved", inputs=["clean"])

# Build DAG
dag = registry.build_dag()

# Create execution engine
engine = ExecutionEngine(broker, dag)

# Execute with custom inputs
results = await engine.execute(inputs={"source_file": "data.csv"})
print(results)  # {"raw": ..., "clean": ..., "saved": ...}
```

**When to use ExecutionEngine**:
- Building dynamic pipelines at runtime
- Custom scheduling/logic outside Pipeline abstraction
- Inspecting DAG structure before execution
- Integrating with external workflow managers

---

## 8. Result Shapes

Different pipeline types return different result structures:

### 8.1. Sequential Pipeline Results

```python
task = await pipeline.kiq(input)
result = await task.wait_result()

# result.return_value is the final output after all steps
# Example: [3, 3, 3, 3] from our quickstart pipeline
```

### 8.2. Dataflow Pipeline Results

```python
result = await pipeline.kiq_dataflow(input_data)

# Returns a dict mapping each output name to its value
{
  "features": {...},
  "tags": [...],
  "embedding": [...]
}
```

### 8.3. MapReduce Results

```python
mapped = await MapReduce.map(...)
print(mapped.return_value)      # List of mapped results

reduced = await MapReduce.reduce(...)
print(reduced.return_value)     # Final aggregated result
```

---

## 9. Inspecting Pipeline State

Query pipeline status during or after execution:

```python
from taskiq_flow import PipelineTrackingManager

tracking = PipelineTrackingManager().with_auto_storage(broker)
pipeline = Pipeline(broker).with_tracking(tracking)

task = await pipeline.kiq(data)

# Get detailed status
status = await tracking.get_status(pipeline.pipeline_id)
print(f"Status: {status.status}")        # PENDING, RUNNING, COMPLETED, FAILED
print(f"Steps: {len(status.steps)}")     # Number of completed steps
print(f"Started: {status.started_at}")
print(f"Completed: {status.completed_at}")

# Get step-by-step history
for step in status.steps:
    print(f"  {step.name}: {step.status} ({step.duration_ms}ms)")
```

**Status values**:

| Status | Meaning |
|--------|---------|
| `PENDING` | Pipeline queued, not started |
| `RUNNING` | Currently executing |
| `COMPLETED` | Finished successfully |
| `FAILED` | Terminated with error |
| `CANCELLED` | Manually cancelled |

See [Tracking Guide]({{ '/en/guides/tracking/' | relative_url }}) for advanced monitoring.

---

## 10. Debugging Execution

### 10.1. Enable Logging

```python
import logging
logging.basicConfig(level=logging.DEBUG)

# Or configure specific loggers
logger = logging.getLogger("taskiq_flow")
logger.setLevel(logging.DEBUG)
```

### 10.2. Print DAG Before Execution

```python
pipeline.print_dag()
# Shows execution levels and dependencies
```

### 10.3. Inspect Task Arguments

```python
@broker.task
async def debug_task(data, context: PipelineContext):
    print(f"Received: {data}")
    print(f"Context: pipeline={context.pipeline_id}, step={context.step_index}")
    return data
```

### 10.4. Middleware for Tracing

```python
from taskiq_flow.middleware import PipelineMiddleware

class DebugMiddleware(PipelineMiddleware):
    async def on_step_complete(self, ctx, result):
        print(f"Step {ctx.task_id} completed with: {result}")
        await super().on_step_complete(ctx, result)

broker.add_middlewares(DebugMiddleware())
```

---

## 11. Performance Considerations

### 11.1. Concurrency Limits

```python
# Limit total parallel tasks globally
from taskiq_flow.optimization.parallel import set_max_parallel_tasks
set_max_parallel_tasks(20)  # never more than 20 tasks simultaneously
```

### 11.2. Selective Parallelism

Not all tasks benefit from parallel execution:

```python
# CPU-bound tasks: benefit from parallelism up to core count
# I/O-bound tasks: can handle higher parallelism
# Small/fast tasks: overhead may outweigh benefits

# Tip: Profile with varying max_parallel values
pipeline.map(process_item, items, max_parallel=8)
```

### 11.3. Memory Footprint

Parallel execution loads more data into memory:

```python
# Process large datasets in chunks
chunks = split_into_chunks(large_list, chunk_size=100)
for chunk in chunks:
    results = await pipeline.kiq_dataflow(chunk)
    # process results before next chunk
```

See [Performance Guide]({{ '/en/guides/performance/' | relative_url }}) for detailed optimization strategies.

---

## 12. Common Pitfalls

| Issue | Cause | Solution |
|-------|-------|----------|
| Tasks run sequentially | `max_parallel=1` or sequential pipeline type | Use DataflowPipeline or increase parallelism |
| `wait_result()` hangs forever | Broker not shared, results lost | Use persistent broker (Redis) with result backend |
| Tasks receive wrong inputs | Incorrect parameter naming | Ensure `@pipeline_task(output=...)` matches downstream param names |
| Out-of-order results | Dataflow tasks finishing at different times | Results dict preserves output names, not execution order |
| Memory explosion | Unlimited parallelism | Set `max_parallel` or process in batches |

---

## 13. Summary

| Feature | Sequential Pipeline | DataflowPipeline | MapReduce |
|---------|--------------------|------------------|-----------|
| **Execution** | Linear chain | Automatic DAG | Parallel map + reduce |
| **Parallelism** | None (unless `.group()` used) | Automatic (independent tasks) | Explicit per-map call |
| **Control** | Manual chaining | Declarative dependencies | Batch-oriented |
| **Best for** | Simple linear workflows | Complex branched workflows | Bulk data transformation |

---

## Next Steps

- **[Pipelines Guide]({{ '/en/guides/pipelines/' | relative_url }})** — Choosing between pipeline types and patterns
- **[Tracking Guide]({{ '/en/guides/tracking/' | relative_url }})** — Monitoring pipeline status and history
- **[Performance Guide]({{ '/en/guides/performance/' | relative_url }})** — Tuning for speed and resource usage

---

*Understanding execution is key to building reliable pipelines. Next, learn about [Pipeline Types]({{ '/en/guides/pipelines/' | relative_url }}).*
