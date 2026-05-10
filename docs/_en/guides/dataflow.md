---
title: Dataflow Guide
nav_order: 29
---

# Dataflow Guide

**Build complex, parallel pipelines using data-driven DAG orchestration**

> **Version**: {VERSION} | **Related**: [Pipelines Guide]({{ '/en/guides/pipelines/' | relative_url }}), [Execution Guide]({{ '/en/guides/execution/' | relative_url }}), [Core Concepts]({{ '/en/guides/core-concepts/' | relative_url }})

---

## Overview

Taskiq-Flow's **dataflow** system is the most powerful way to orchestrate complex workflows. Unlike sequential pipelines where you manually chain steps, dataflow pipelines automatically construct a Directed Acyclic Graph (DAG) from your task declarations, enabling:

- **Automatic dependency resolution** — tasks declare what they produce and consume
- **Automatic parallelism** — independent tasks run concurrently without manual configuration
- **Data-driven execution** — the flow of data determines the order of execution
- **Dynamic pipeline construction** — add tasks flexibly at runtime

This guide covers the complete dataflow system: the DAG, the registry, decorators, the execution engine, and advanced patterns.

---

## 1. Core Concepts

### 1.1. The Dataflow Paradigm

In a dataflow pipeline, tasks are connected by **data dependencies** rather than explicit ordering:

```
Sequential:              Dataflow:
task1 → task2 → task3    task1 ──→ task2
                              └──→ task3  (parallel!)
```

Each task declares:
- **`output`**: What data it produces (e.g., `"features"`)
- **`inputs`**: What data it consumes (e.g., `["features", "config"]`)

The library automatically resolves dependencies and builds the execution graph.

### 1.2. Key Components

| Component | Purpose | Module |
|-----------|---------|--------|
| `@pipeline_task` | Decorator to declare task I/O | `taskiq_flow.decorators` |
| `DataNode` | Represents a data artifact in the graph | `taskiq_flow.dataflow.node` |
| `DAG` / `DAGNode` | Graph structure for dependency tracking | `taskiq_flow.dataflow.dag` |
| `DataflowRegistry` | Central registry for task metadata and DAG building | `taskiq_flow.dataflow.registry` |
| `DataCache` | Stores intermediate results during execution | `taskiq_flow.dataflow.cache` |
| `DataflowPipeline` | High-level pipeline using dataflow orchestration | `taskiq_flow.pipeline` |
| `ExecutionEngine` | Low-level DAG executor with parallelism | `taskiq_flow.execution_engine` |

---

## 2. Declaring Dataflow Tasks

### 2.1. The `@pipeline_task` Decorator

Mark a function as a pipeline task with explicit output:

```python
from taskiq import InMemoryBroker
from taskiq_flow import pipeline_task, DataflowPipeline

broker = InMemoryBroker()

@broker.task
@pipeline_task(output="features")
async def extract_features(paths: list[str]) -> dict:
    """Extract audio features from file paths."""
    return {"tempo": 120.0, "energy": 0.8}
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `output` | `str` | **required** | Name of the data this task produces |
| `inputs` | `list[str]` | `None` (auto-inferred) | Names of data consumed |
| `retries` | `int` | `0` | Number of retry attempts on failure |
| `retry_delay` | `float` | `1.0` | Initial delay between retries (seconds) |
| `retry_backoff` | `float` | `2.0` | Multiplier for delay between retries |
| `retry_jitter` | `bool` | `True` | Add randomness to retry delays |
| `max_retry_time` | `int` | `300` | Maximum total time for retries (seconds) |
| `resources` | `dict` | `{}` | Estimated resource usage (memory, cpu) |

### 2.2. Automatic Input Inference

If you don't specify `inputs`, they're inferred from the function signature:

```python
@broker.task
@pipeline_task(output="stats")
def compute_stats(features: dict, config: dict) -> dict:
    # Automatically inferred: inputs=["features", "config"]
    return {"count": len(features)}
```

Parameters named `self`, `cls`, or those with default values are excluded from inference.

### 2.3. Multiple Outputs

Use `@pipeline_task_multi_output` when a task produces multiple data artifacts:

```python
from taskiq_flow.decorators import pipeline_task_multi_output

@broker.task
@pipeline_task_multi_output(
    outputs={"features": dict, "metadata": dict},
    retries=2
)
async def process_audio(path: str) -> dict:
    features = extract(path)
    meta = get_metadata(path)
    return {
        "features": features,   # → output "features"
        "metadata": meta,       # → output "metadata"
    }
```

The **first key** is the primary output; all keys are registered as named outputs.

---

## 3. Building Dataflow Pipelines

### 3.1. Using `DataflowPipeline.from_tasks()`

The recommended approach for most use cases:

```python
pipeline = DataflowPipeline.from_tasks(
    broker,
    [extract_features, compute_stats, generate_report]
)
```

The DAG is built automatically:
- `extract_features` produces `"features"` — no dependencies → Level 0
- `compute_stats` consumes `"features"` → depends on `extract_features` → Level 1
- `generate_report` consumes `"stats"` → depends on `compute_stats` → Level 2

### 3.2. Adding Tasks Dynamically

```python
pipeline = DataflowPipeline(broker)
pipeline.add_dataflow_task(extract_features)
pipeline.add_dataflow_task(compute_stats)
pipeline.add_dataflow_task(generate_report)

# DAG is rebuilt lazily on first execution
results = await pipeline.kiq_dataflow(paths=["song.mp3"])
```

### 3.3. Fan-Out / Fan-In (Multiple Dependencies)

Tasks can consume multiple outputs, and multiple tasks can share a dependency:

```python
@broker.task
@pipeline_task(output="audio")
def load_audio(path: str) -> dict: ...

@broker.task
@pipeline_task(output="transcription")
def transcribe(audio: dict) -> str: ...

@broker.task
@pipeline_task(output="tags")
def generate_tags(audio: dict) -> list[str]: ...  # parallel with transcribe

@broker.task
@pipeline_task(output="report")
def create_report(
    transcription: str,
    tags: list[str]
) -> dict: ...  # fan-in: waits for both

pipeline = DataflowPipeline.from_tasks(
    broker,
    [load_audio, transcribe, generate_tags, create_report]
)
# DAG:
#   load_audio → (transcribe ∥ generate_tags) → create_report
```

### 3.4. External Inputs

Pass data at execution time that isn't produced by any task:

```python
results = await pipeline.kiq_dataflow(
    user_id="user_123",      # external input
    config={"mode": "fast"}  # external input
)
```

External inputs are automatically identified — they're parameters with no corresponding producer task.

---

## 4. DAG Construction & Inspection

### 4.1. The `DataflowRegistry`

For advanced use cases, build DAGs manually:

```python
from taskiq_flow import DataflowRegistry

registry = DataflowRegistry()

# Register tasks with explicit I/O
registry.register_task(
    task=load_data,
    output="raw_data",
    inputs=["source_url"]  # external input
)
registry.register_task(
    task=clean_data,
    output="clean_data",
    inputs=["raw_data"]
)
registry.register_task(
    task=save_data,
    output="saved",
    inputs=["clean_data"]
)

# Inspect the graph
print("Tasks:", [t.task_name for t in registry.get_tasks()])
print("Outputs:", registry.get_outputs())
print("External inputs:", registry.get_external_inputs())

# Build DAG
dag = registry.build_dag()
dag.print()
```

### 4.2. DAG Inspection Methods

```python
from taskiq_flow.dataflow import DAG, DAGNode

# Get topological order
ordered = dag.topological_sort()
for node in ordered:
    print(f"Level {node.level}: {node.task_name}")

# Get parallel execution groups
dag.compute_levels()
for i, level in enumerate(dag.levels):
    names = [n.task_name for n in level]
    print(f"Level {i} (parallel): {names}")

# Find ready tasks given completed set
ready = dag.get_ready_tasks(completed={node_a})

# Visualize DAG (requires networkx)
from taskiq_flow.visualization import DAGVisualizer
viz = DAGVisualizer(dag)
print(viz.to_json())
print(viz.to_graphviz())
print(viz.visualize_ascii())
```

### 4.3. Detecting Critical Path & Parallel Groups

```python
from taskiq_flow.visualization import DAGVisualizer

viz = DAGVisualizer(dag)

# Critical path = longest execution chain
critical = viz.detect_critical_path()
print(f"Critical path: {' → '.join(critical)}")

# Groups of tasks that can run in parallel
groups = viz.find_parallelizable_groups()
for i, group in enumerate(groups):
    print(f"Parallel group {i}: {group}")
```

---

## 5. Execution

### 5.1. Running Dataflow Pipelines

```python
# Execute and get all outputs
results = await pipeline.kiq_dataflow(track_paths=["song1.mp3", "song2.mp3"])
print(results)
# {"audio_features": {...}, "mir_features": {...}, "tags": [...], "embedding": [...]}
```

### 5.2. The Execution Engine

`DataflowPipeline` uses `ExecutionEngine` internally for DAG-based execution:

```python
from taskiq_flow import ExecutionEngine

# Custom execution with fine-grained control
engine = ExecutionEngine(
    broker=broker,
    dag=dag,
    max_parallel=10,
    error_mode=ErrorHandlingMode.CONTINUE_ON_ERROR,
    resource_aware=True,
)

outputs = await engine.execute(
    inputs={"source_file": "data.csv"},
    pipeline_id="my_pipeline"
)
```

**Execution features:**
- Topological ordering — tasks execute after their dependencies
- Parallel execution — independent tasks run concurrently
- Retry per task — configured via `@pipeline_task(retries=N)`
- Error modes — `FAIL_FAST`, `CONTINUE_ON_ERROR`, `SKIP_FAILED`
- Resource-aware parallelism — adjusts concurrency based on CPU/memory

### 5.3. Map-Reduce Operations

For batch processing within pipelines:

```python
from taskiq_flow import MapReduce

# Parallel map
mapped = await MapReduce.map(
    broker,
    process_item,
    items=list(range(100)),
    output="processed",
    max_parallel=10,
)

# Reduce results
result = await MapReduce.reduce(
    broker,
    aggregate_results,
    mapped.results,
    output="final",
    initial=0,
)

# Combined map-reduce
final = await MapReduce.map_reduce(
    broker,
    map_task=process_item,
    reduce_task=aggregate_results,
    items=list(range(1000)),
    max_parallel=20,
    reduce_chunk_size=100,
)
```

**Map features:**
- Automatic parallelism with `asyncio`
- Intelligent chunking for large datasets
- Progress callbacks
- Error collection with success rate reporting

### 5.4. Pipeline-Level Map/Reduce

`DataflowPipeline` integrates map-reduce as pipeline operations:

```python
pipeline = DataflowPipeline.from_tasks(
    broker, [extract_features]
)

# Add map operation (process many items in parallel)
pipeline.map(
    process_track,
    track_list,
    output="track_features",
    max_parallel=10,
)

# Add reduce operation (aggregate results)
pipeline.reduce(
    aggregate_features,
    input_name="track_features",
    output="playlist_stats",
)

# Execute
results = await pipeline.kiq_map_reduce(track_list=tracks)
```

---

## 6. Combining Sequential and Dataflow Pipelines

### 6.1. Hybrid Pattern

Use sequential pipelines for linear flows and dataflow for complex sub-workflows:

```python
# Sequential outer shell
main_pipeline = Pipeline(broker)

@broker.task
async def run_dataflow_subset(data: list) -> dict:
    # Inner dataflow pipeline
    sub_pipeline = DataflowPipeline.from_tasks(
        broker,
        [task_a, task_b, task_c]
    )
    return await sub_pipeline.kiq_dataflow(data=data)

main_pipeline.call_next(run_dataflow_subset).call_next(finalize)
```

### 6.2. Pipeline Scheduling

Schedule dataflow pipelines with cron or intervals:

```python
from taskiq_flow import PipelineScheduler

scheduler = PipelineScheduler(broker)

# Cron-based scheduling
await scheduler.schedule(
    pipeline,
    cron="0 2 * * *",  # Daily at 2 AM
    kwargs={"paths": ["daily_files/*.mp3"]}
)

# Interval-based
await scheduler.schedule(
    pipeline,
    interval_seconds=3600,  # Every hour
    label="hourly_analysis"
)
```

---

## 7. Caching & Intermediate Results

### 7.1. `DataCache`

The `DataCache` stores intermediate results during pipeline execution:

```python
from taskiq_flow.dataflow.cache import DataCache

cache = DataCache()

# Store results
cache.set("features", {"tempo": 120.0})
cache.set("tags", ["electronic", "dance"])

# Retrieve
features = cache.get("features")

# Check existence
if cache.has("embedding"):
    embedding = cache.get("embedding")

# Inject dependencies for tasks
inputs = cache.inject(["features", "tags"])
# → {"features": {...}, "tags": [...]}

# Clear between runs
cache.clear()
```

---

## 8. Error Handling in Dataflow

### 8.1. Error Modes

```python
from taskiq_flow.errors import ErrorHandlingMode

# Fail on first error (default)
engine = ExecutionEngine(broker, dag, error_mode=ErrorHandlingMode.FAIL_FAST)

# Continue despite errors
engine = ExecutionEngine(broker, dag, error_mode=ErrorHandlingMode.CONTINUE_ON_ERROR)

# Skip failed tasks
engine = ExecutionEngine(broker, dag, error_mode=ErrorHandlingMode.SKIP_FAILED)
```

### 8.2. Retry Configuration

Configure retries at the task level:

```python
@broker.task
@pipeline_task(
    output="reliable_feature",
    retries=3,
    retry_delay=2.0,
    retry_backoff=2.0,
)
def fetch_with_retry(url: str) -> dict:
    # Will be retried up to 3 times with exponential backoff
    ...
```

---

## 9. Resource Management

### 9.1. Resource-Aware Parallelism

Control parallelism based on estimated task resource usage:

```python
from taskiq_flow.optimization.parallel import ResourceAwareExecutor

executor = ResourceAwareExecutor(
    max_cpu_percent=80.0,
    max_memory_percent=80.0,
    min_parallel=1,
    max_parallel=10,
)

engine = ExecutionEngine(
    broker,
    dag,
    resource_aware=True,
    resource_profiles={
        "heavy_task": {"estimated_memory_mb": 500, "estimated_cpu_cores": 2.0},
        "light_task": {"estimated_memory_mb": 50, "estimated_cpu_cores": 0.5},
    },
)
```

---

## 10. Visualization

### 10.1. Built-in Visualization

```python
# ASCII in console
pipeline.print_dag()

# JSON for web UIs
viz = pipeline.visualize()  # → {"nodes": [...], "edges": [...], "levels": [...]}

# DOT for Graphviz
dot = pipeline.visualize_dot()
# Render: dot -Tpng pipeline.dot -o pipeline.png
```

### 10.2. Advanced Visualization (requires networkx)

```python
from taskiq_flow.visualization import DAGVisualizer

viz = DAGVisualizer(dag)

# Export formats
viz.to_json()           # JSON for frontend
viz.to_cytoscape_json() # Cytoscape.js format
viz.to_graphviz()       # DOT format
viz.visualize_ascii()   # ASCII art

# Analysis
viz.detect_critical_path()      # Longest execution path
viz.find_parallelizable_groups()  # Tasks that can run in parallel
```

### 10.3. Mermaid Diagrams

```python
from taskiq_flow.visualization import MermaidGenerator

mermaid = MermaidGenerator(dag)
print(mermaid.to_mermaid())           # Basic diagram
print(mermaid.to_mermaid_with_styling())  # Styled with colors
```

---

## 11. Full Example: Audio Processing Pipeline

```python
import asyncio
from taskiq import InMemoryBroker
from taskiq_flow import DataflowPipeline, pipeline_task

broker = InMemoryBroker()

@broker.task
@pipeline_task(output="audio_features")
async def extract_audio(paths: list[str]) -> dict:
    return {"duration": 180.0, "tempo": 120.0}

@broker.task
@pipeline_task(output="mir_features")
async def compute_mir(audio_features: dict) -> dict:
    return {"key": "C major", "loudness": -12.5}

@broker.task
@pipeline_task(output="tags")
async def generate_tags(mir_features: dict) -> list[str]:
    return ["electronic", "dance"]

@broker.task
@pipeline_task(output="embedding")
async def create_embedding(
    mir_features: dict,
    tags: list[str]
) -> list[float]:
    return [0.1, 0.2, 0.3, 0.4, 0.5]

async def main():
    pipeline = DataflowPipeline.from_tasks(
        broker,
        [extract_audio, compute_mir, generate_tags, create_embedding]
    )

    # Inspect before execution
    pipeline.print_dag()

    # Execute
    results = await pipeline.kiq_dataflow(
        paths=["track1.mp3", "track2.mp3"]
    )

    print(results)
    # {
    #   "audio_features": {"duration": 180.0, "tempo": 120.0},
    #   "mir_features": {"key": "C major", "loudness": -12.5},
    #   "tags": ["electronic", "dance"],
    #   "embedding": [0.1, 0.2, 0.3, 0.4, 0.5]
    # }

asyncio.run(main())
```

---

## 12. Common Pitfalls

| Symptom | Cause | Fix |
|---------|-------|-----|
| All tasks run sequentially | Using `Pipeline` instead of `DataflowPipeline` | Switch to `DataflowPipeline` |
| Missing output errors | `@pipeline_task(output=...)` doesn't match downstream param name | Align output name with parameter name |
| "No DAG built" error | Called `kiq_dataflow()` without adding tasks | Add tasks via `from_tasks()` or `add_dataflow_task()` |
| Tasks run twice | Mixed `.call_next()` and `@pipeline_task` dependencies | Use one approach consistently |
| Deadlock detected | Circular dependency in data flow | Redesign with forward data flow only |
| Memory explosion | Too many parallel tasks | Set `max_parallel` or use resource-aware mode |

---

## 13. Performance Tips

1. **Limit parallelism** — Use `max_parallel` to control concurrent task count
2. **Use map-reduce for batches** — `MapReduce.map()` with chunking for large datasets
3. **Profile resource usage** — Set `resource_profiles` for adaptive parallelism
4. **Avoid large intermediate results** — Stream data when possible
5. **Reuse DAG** — Build DAG once, execute multiple times with different inputs

---

## 14. API Reference Summary

### `DataflowPipeline`

| Method | Description |
|--------|-------------|
| `from_tasks(broker, tasks)` | Create pipeline from task list |
| `add_dataflow_task(task)` | Add task dynamically |
| `kiq_dataflow(**inputs)` | Execute pipeline with dataflow orchestration |
| `kiq_map_reduce(**inputs)` | Execute map-reduce pipeline |
| `kiq_map_reduce_advanced(...)` | Execute with full map-reduce options |
| `kiq_map_sweep(task, param_values, ...)` | Multi-dimensional parameter sweep |
| `visualize()` | Get DAG as JSON dict |
| `visualize_dot()` | Get DAG as DOT string |
| `print_dag()` | Print ASCII representation |
| `schedule_with_cron(scheduler, label, cron, **inputs)` | Schedule with cron expression |
| `schedule_with_labels(scheduler, label, ...)` | Schedule with label-based scheduler |
| `map(task, items, output, ...)` | Add map operation |
| `reduce(task, input_name, output, ...)` | Add reduce operation |

### `DataflowRegistry`

| Method | Description |
|--------|-------------|
| `register_task(task, output, inputs, **meta)` | Register task with I/O metadata |
| `build_dag()` | Construct DAG from registered tasks |
| `get_producer(data_name)` | Find producer task for data |
| `get_consumers(data_name)` | Find consumer tasks for data |
| `get_external_inputs()` | List external input names |
| `get_outputs()` | List all output names |
| `get_tasks()` | List all registered tasks |

### `ExecutionEngine`

| Method | Description |
|--------|-------------|
| `execute(inputs, pipeline_id)` | Execute DAG with inputs |
| `get_execution_report()` | Get execution statistics |

### `MapReduce`

| Method | Description |
|--------|-------------|
| `map(broker, task, items, output, ...)` | Parallel map operation |
| `reduce(broker, task, inputs, output, ...)` | Reduction operation |
| `map_reduce(broker, map_task, reduce_task, items, ...)` | Combined map+reduce |
| `map_sweep(broker, task, param_values, output, ...)` | Multi-dimensional sweep |


*Master dataflow to build complex, parallel workflows. For sequential patterns, see [Pipelines Guide]({{ '/en/guides/pipelines/' | relative_url }}).*