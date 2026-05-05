---
title: Pipelines Guide
nav_order: 20
---
# Pipelines Guide

**Sequential and Dataflow pipeline patterns, configurations, and best practices**

> **Version**: 0.3.2 | **Related**: [Execution Guide]({{ '/en/guides/execution.md' | relative_url }}), [Tasks Guide]({{ '/en/guides/tasks.md' | relative_url }})

---

## Overview

Taskiq-Flow provides two main pipeline types for orchestrating task workflows:

1. **SequentialPipeline** — Manual step chaining for linear workflows
2. **DataflowPipeline** — Automatic DAG construction from task dependencies

This guide explores both types, their use cases, and how to choose between them.

---

## 1. Sequential Pipeline

The classic pipeline model where you explicitly chain steps in order.

### 1.1. Basic Structure

```python
from taskiq_flow import Pipeline

pipeline = (
    Pipeline(broker)
    .call_next(task1)
    .call_next(task2)
    .call_next(task3)
)
```

**Execution**: `task1 → task2 → task3` (synchronously)

### 1.2. Available Operations

#### `.call_next(task, *args, **kwargs)`

Execute a task, passing the previous result as the first argument:

```python
pipeline.call_next(process_data).call_next(save_result)
# process_data receives output of previous step
# save_result receives output of process_data
```

**Parameter binding**:
- By position: result becomes first argument
- By name: `pipeline.call_next(task, param_name=previous_result)`

Example:
```python
@broker.task
def multiply(value: int, factor: int) -> int:
    return value * factor

pipeline.call_next(add_one).call_next(multiply, factor=3)
# add_one output → multiply(value=...) , factor=3
```

#### `.call_after(task, *args, **kwargs)`

Execute a task **without** consuming the previous result (fire-and-forget within pipeline):

```python
pipeline.call_next(process).call_after(log_completion)
# log_completion runs after process but doesn't receive process's output
```

Useful for side effects (logging, notifications) that shouldn't transform the data flow.

#### `.map(task, max_parallel=None)`

Apply a task to each element of an iterable result in parallel:

```python
# Previous step returned: [1, 2, 3, 4]
pipeline.map(process_item)
# Runs process_item(1), process_item(2), ... concurrently
# Collects results: [processed1, processed2, ...]
```

**Options**:
- `max_parallel=10` — limit concurrent executions
- `output_name="results"` — custom output key (default: task output name)

#### `.filter(task)`

Keep elements where the task returns truthy:

```python
# Previous step returned: [1, 2, 3, 4]
pipeline.filter(is_even)
# Keeps elements where is_even(element) returns True
# Result: [2, 4]
```

#### `.group(tasks, param_names=None)`

Execute multiple independent tasks in parallel, starting from the same input:

```python
pipeline.group(
    [task_a, task_b, task_c],
    param_names=["x", "y", "z"]  # bind input to these parameters
)
# All three tasks receive the same previous result
# Returns: [result_a, result_b, result_c]
```

---

## 2. Dataflow Pipeline

Automatic DAG construction using `@pipeline_task(output=...)` annotations.

### 2.1. Declaring Task Outputs

```python
from taskiq_flow import pipeline_task, DataflowPipeline

@broker.task
@pipeline_task(output="features")
def extract_features(data: list[str]) -> dict:
    return {"count": len(data)}

@broker.task
@pipeline_task(output="stats")
def compute_stats(features: dict) -> dict:
    return {"entries": features["count"] * 2}

@broker.task
@pipeline_task(output="report")
def generate_report(stats: dict) -> str:
    return f"Stats: {stats}"
```

**Key**: The `output` parameter declares what this task produces. Downstream tasks declare matching parameter names to consume those outputs.

### 2.2. Building the Pipeline

```python
pipeline = DataflowPipeline.from_tasks(
    broker,
    [extract_features, compute_stats, generate_report]
)
```

**Automatic dependency resolution**:

1. `extract_features` produces `features` — no dependencies
2. `compute_stats` needs `features` — depends on `extract_features`
3. `generate_report` needs `stats` — depends on `compute_stats`

**Resulting DAG**:
```
extract_features → compute_stats → generate_report
```

### 2.3. Multiple Consumers

Multiple tasks can consume the same output; they'll all wait for the producer:

```python
@broker.task
@pipeline_task(output="features")
def extract(data): ...

@broker.task
@pipeline_task(output="tags")
def tag(features: dict): ...   # consumer 1 of features

@broker.task
@pipeline_task(output="embedding")
def embed(features: dict): ... # consumer 2 of features

# Both tag and embed run in parallel after extract completes
```

### 2.4. Input Parameters

Dataflow pipelines accept external inputs via `kiq_dataflow(**kwargs)`:

```python
results = await pipeline.kiq_dataflow(data=["file1.mp3", "file2.mp3"])
# The `data` parameter is matched to any task needing it
# Must match a parameter name of a task with no producer (external input)
```

---

## 3. Pipeline Configuration

### 3.1. Adding Tracking

```python
from taskiq_flow import PipelineTrackingManager

tracking = PipelineTrackingManager().with_auto_storage(broker)
pipeline = Pipeline(broker).with_tracking(tracking)
```

See [Tracking Guide]({{ '/en/guides/tracking.md' | relative_url }}) for details.

### 3.2. Setting a Custom Pipeline ID

```python
pipeline.pipeline_id = "my_custom_workflow_001"
# If not set, a UUID is generated automatically
```

Important for tracking and WebSocket subscriptions.

### 3.3. Attaching Hooks (WebSocket)

```python
from taskiq_flow.hooks import HookManager

hooks = HookManager()
pipeline = Pipeline(broker).with_hooks(hooks)
```

See [WebSocket Guide]({{ '/en/guides/websocket.md' | relative_url }}).

### 3.4. Retry & Error Policies

```python
pipeline.with_retry(
    max_attempts=3,
    delay=1.0,
    backoff=2.0
)
pipeline.on_error("continue")  # or "stop"
```

See [Retry Guide]({{ '/en/guides/retry.md' | relative_url }}).

### 3.5. Timeouts

```python
pipeline.with_timeout(seconds=60)
```

---

## 4. Pipeline Lifecycle

### 4.1. Creation → Execution → Completion

```
1.  pipeline = Pipeline(broker)           # Create pipeline object
2.  pipeline.call_next(...)               # Chain steps
3.  task = await pipeline.kiq(input)      # Launch
4.  result = await task.wait_result()     # Wait & retrieve
```

### 4.2. Reuseability

Pipeline objects are **single-use**. For repeated execution, create a new pipeline or use the `PipelineScheduler`:

```python
# Correct: Create fresh pipeline each time
async def run_workflow(data):
    pipeline = Pipeline(broker).call_next(step1).call_next(step2)
    return await pipeline.kiq(data)

# For recurring schedules, use PipelineScheduler
from taskiq_flow import PipelineScheduler
scheduler = PipelineScheduler(broker)
await scheduler.schedule(pipeline, cron="* * * * *")
```

---

## 5. Visualizing Pipelines

### 5.1. ASCII DAG (Console)

```python
pipeline.print_dag()
```

Example output:
```
DAG Execution Order:
  Level 0: task_a
  Level 1: task_b, task_c
  Level 2: task_d
```

### 5.2. JSON for Web UIs

```python
viz = pipeline.visualize()  # returns dict
print(viz)
```

Structure:
```json
{
  "nodes": [
    {"id": "task_a", "outputs": ["x", "y"]},
    {"id": "task_b", "inputs": ["x"]}
  ],
  "edges": [{"from": "task_a", "to": "task_b"}]
}
```

### 5.3. DOT Format (Graphviz)

```python
dot = pipeline.visualize_dot()
with open("pipeline.dot", "w") as f:
    f.write(dot)
# Render: dot -Tpng pipeline.dot -o pipeline.png
```

Resulting diagram shows nodes, edges, and execution order.

---

## 6. Pipeline Inspection (DataflowRegistry)

For advanced use cases, manually construct and inspect the dataflow graph:

```python
from taskiq_flow import DataflowRegistry

registry = DataflowRegistry()

# Register tasks with explicit I/O
registry.register_task(
    task=load_data,
    output="raw",
    inputs=["source"]  # external input
)
registry.register_task(
    task=clean,
    output="clean",
    inputs=["raw"]
)
registry.register_task(
    task=save,
    output="saved",
    inputs=["clean"]
)

# Inspect structure
print("Tasks:", [t.task_name for t in registry.get_tasks()])
print("Outputs:", registry.get_outputs())           # ["raw", "clean", "saved"]
print("External inputs:", registry.get_external_inputs())  # ["source"]

# Find dependencies
producer = registry.get_producer("clean")   # returns TaskNode for 'clean'
consumers = registry.get_consumers("raw")   # list of tasks needing 'raw'

# Build DAG
dag = registry.build_dag()
dag.print()
order = dag.topological_sort()  # list of tasks in execution order
levels = dag.levels              # list of lists (parallel groups)
```

See `examples/registry_discovery_example.py` for complete usage.

---

## 7. Choosing Between Pipeline Types

| Criteria | SequentialPipeline | DataflowPipeline |
|----------|-------------------|------------------|
| **Workflow shape** | Linear, with occasional branching | Complex DAG with many branches |
| **Task dependencies** | Implicit (chaining order) | Explicit (`@pipeline_task`) |
| **Parallel needs** | Manual (`.group()`) | Automatic (independent tasks) |
| **Flexibility** | Full control over order | Declarative; library optimizes |
| **Dynamic workflows** | Hard (fixed at build time) | Easy (can add tasks flexibly) |
| **Best for** | ETL linear steps, simple batch | Audio/video processing, ML pipelines |

**Rule of thumb**:
- **SequentialPipeline** for simple, fixed-order workflows
- **DataflowPipeline** for complex, branched, or reusable workflows

---

## 8. Best Practices

### 8.1. Task Naming & Outputs

Use clear, unique output names:

```python
@pipeline_task(output="user_features")  # clear
@pipeline_task(output="features_2")     # ambiguous (if multiple features exist)
```

### 8.2. Avoid Circular Dependencies

DataflowPipeline detects cycles and raises `CycleError` during `build_dag()`. Design with forward data flow only.

### 8.3. Minimize Shared State

Each task should be pure (output depends only on inputs) for parallel safety.

### 8.4. Version Pipeline IDs

Include version in pipeline IDs for tracking:

```python
pipeline.pipeline_id = f"audio_analysis_v1_{int(time.time())}"
```

### 8.5. Use `.call_after()` for Side Effects

Don't corrupt the data flow with logging/metrics:

```python
pipeline.call_next(process).call_after(log_result)  # correct
pipeline.call_next(process_and_log)                    # anti-pattern
```

### 8.6. Limit Parallelism for Resource-Heavy Tasks

```python
# CPU-intensive transcoding
pipeline.map(transcode, files, max_parallel=2)
```

### 8.7. Validate DAG Before Execution

```python
pipeline.print_dag()  # Always inspect complex pipelines
input("Press Enter to execute...")
```

---

## 9. Common Pitfalls

| Symptom | Likely cause | Fix |
|---------|-------------|-----|
| Task runs twice | `.call_next()` and dependent task both declared | Remove redundant call; Dataflow manages dependencies |
| Missing output key | `@pipeline_task(output=...)` doesn't match downstream param | Align output name with parameter name |
| All tasks sequential | Using Pipeline instead of DataflowPipeline | Switch to DataflowPipeline for automatic parallelism |
| Results None | Forgetting `broker.add_middlewares(PipelineMiddleware())` | Add middleware before creating pipelines |
| Stale pipeline reused | Attempting to call `kiq()` twice on same pipeline object | Create fresh pipeline per execution |

---

## 10. Advanced Patterns

### 10.1. Hybrid Sequential + Dataflow

Combine both types for maximum control:

```python
# Sequential outer shell
sequential = Pipeline(broker)

# Inside a step, spawn a dataflow sub-pipeline
@broker.task
async def process_batch(data: list) -> dict:
    sub_pipeline = DataflowPipeline.from_tasks(
        broker,
        [subtask1, subtask2, subtask3]
    )
    return await sub_pipeline.kiq_dataflow(data=data)

sequential.call_next(process_batch).call_next(finalize)
```

### 10.2. Dynamic Pipeline Construction

Build pipelines at runtime based on configuration:

```python
def build_pipeline(config: dict) -> Pipeline:
    steps = []
    if config.get("preprocess"):
        steps.append(preprocess_task)
    if config.get("analyze"):
        steps.append(analyze_task)
    # ...
    pipeline = Pipeline(broker)
    for step in steps:
        pipeline.call_next(step)
    return pipeline
```

### 10.3. Conditional Branching

Use `.filter()` and condition steps:

```python
high_value = pipeline.filter(is_high_value)
high_value.call_next(premium_processing)
low_value = pipeline.filter(is_low_value)
low_value.call_next(standard_processing)

# Merge back
merged = high_value.group([premium_processing, standard_processing])
```

See [steps/condition.py](https://github.com/SoniqueBay/taskiq-flow/blob/main/taskiq_flow/steps/condition.py) for `IfStep`.

---

## 11. Summary Checklist

Before running a pipeline, verify:

- [ ] Pipeline type chosen appropriately (Sequential vs Dataflow)
- [ ] All functions decorated with `@broker.task`
- [ ] Dataflow: all relevant tasks decorated with `@pipeline_task(output=…)`
- [ ] Output names match downstream parameter names exactly
- [ ] `PipelineMiddleware` added to broker
- [ ] `pipeline_id` set if tracking/WebSocket needed
- [ ] DAG inspected with `print_dag()` for complex workflows
- [ ] Parallelism limits (`max_parallel`) set appropriately
- [ ] Timeouts configured for long-running tasks
- [ ] Example run completed successfully before production use

---

## Further Reading

- **[Execution Guide]({{ '/en/guides/execution.md' | relative_url }})** — How pipelines run, error handling, timeouts
- **[Tasks Guide]({{ '/en/guides/tasks.md' | relative_url }})** — Writing task functions and decorators
- **[Examples]({{ '/en/examples/' | relative_url }})** — End-to-end pipeline demonstrations

---

*Master pipelines to orchestrate any workflow. Next, learn about [Task Definition]({{ '/en/guides/tasks.md' | relative_url }}).*
