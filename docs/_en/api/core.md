---
permalink: /en/api/core/
title: API Reference: Core Components
nav_order: 30
---
# API Reference: Core Components

**Pipeline, DataflowPipeline, PipelineMiddleware, PipelineContext, and core exceptions**

> **Version**: 0.4.0 | **Module**: `taskiq_flow.core`, `taskiq_flow.pipeline`, `taskiq_flow.middleware`

---

## Core Classes

### Pipeline (SequentialPipeline)

The classic sequential pipeline for linear task orchestration.

```python
from taskiq_flow import Pipeline

pipeline = Pipeline(broker)
```

**Constructor**:
```python
Pipeline(
    broker: BaseBroker,
    max_parallel: int = None,  # Global parallelism limit
    timeout: float = None,     # Overall timeout in seconds
    pipeline_id: str = None    # Auto-generated if not provided
)
```

**Methods**:

| Method | Signature | Description |
|--------|-----------|-------------|
| `call_next` | `call_next(task, *args, **kwargs) -> Pipeline` | Chain a task; passes previous result as first arg |
| `call_after` | `call_after(task, *args, **kwargs) -> Pipeline` | Run task without consuming previous result |
| `map` | `map(task, max_parallel=None, output_name=None) -> Pipeline` | Apply task to each element of iterable result |
| `filter` | `filter(task) -> Pipeline` | Keep elements where task returns truthy |
| `group` | `group(tasks, param_names=None) -> Pipeline` | Run multiple tasks in parallel from same input |
| `kiq` | `kiq(*args, **kwargs) -> Task` | Start pipeline execution |
| `with_tracking` | `with_tracking(tracking_manager) -> Pipeline` | Attach tracking manager |
| `with_hooks` | `with_hooks(hook_manager) -> Pipeline` | Attach hook manager for events |
| `with_retry` | `with_retry(...) -> Pipeline` | Configure retry policy |
| `with_timeout` | `with_timeout(seconds) -> Pipeline` | Set timeout |
| `with_context` | `with_context(enable=True) -> Pipeline` | Enable passing PipelineContext to tasks |

**Example**:
```python
pipeline = (
    Pipeline(broker)
    .call_next(task1)
    .call_next(task2, factor=2)
    .map(task3, max_parallel=10)
    .filter(validate)
    .with_tracking(tracking)
)
result = await pipeline.kiq(initial_input)
```

---

### DataflowPipeline

Automatic DAG construction from task dependencies using `@pipeline_task` decorators.

```python
from taskiq_flow import DataflowPipeline

pipeline = DataflowPipeline.from_tasks(
    broker,
    [task_a, task_b, task_c]
)
```

**Constructor**:
```python
DataflowPipeline(
    broker: BaseBroker,
    tasks: list[Callable] = None,
    max_parallel: int = None,
    timeout: float = None,
    pipeline_id: str = None
)
```

**Class Methods**:

| Method | Description |
|--------|-------------|
| `from_tasks(broker, tasks, **kwargs)` | Build pipeline from list of task functions with `@pipeline_task` decorators |

**Instance Methods** (most shared with `Pipeline`):

| Method | Description |
|--------|-------------|
| `print_dag()` | Print ASCII DAG to console |
| `visualize()` | Return JSON representation of DAG |
| `visualize_dot()` | Return Graphviz DOT string |
| `kiq_dataflow(**kwargs)` | Execute pipeline with named inputs |

**Example**:
```python
@broker.task
@pipeline_task(output="features")
def extract(data): ...

@broker.task
@pipeline_task(output="tags")
def tag(features): ...

pipeline = DataflowPipeline.from_tasks(broker, [extract, tag])
pipeline.print_dag()
# Output:
# Level 0: extract
# Level 1: tag

results = await pipeline.kiq_dataflow(data=input_data)
# results = {"features": ..., "tags": ...}
```

---

### PipelineMiddleware

The middleware that orchestrates pipeline step execution.

```python
from taskiq_flow import PipelineMiddleware

broker.add_middlewares(PipelineMiddleware())
```

**Responsibilities**:

- Intercepts task completion
- Determines next step to execute
- Manages pipeline state transitions
- Passes results between steps
- Emits hook events

**Note**: This middleware **must** be added to the broker for any pipeline to work.

---

### PipelineContext

Metadata passed to tasks when `with_context(enable=True)` is set.

```python
from taskiq_flow import PipelineContext

@broker.task
async def my_task(data: str, context: PipelineContext):
    print(f"Pipeline: {context.pipeline_id}")
    print(f"Step: {context.step_index}")
    print(f"Task ID: {context.task_id}")
```

**Fields**:

| Field | Type | Description |
|-------|------|-------------|
| `pipeline_id` | `str` | Unique pipeline instance ID |
| `step_index` | `int` | Current step number (0-indexed) |
| `task_id` | `str` | Underlying taskiq task ID |
| `execution_mode` | `str` | `"sequential"`, `"parallel"`, `"map_reduce"` |
| `started_at` | `datetime` | Pipeline start timestamp |
| `broker` | `BaseBroker` | Reference to broker instance |

---

## Core Exceptions

All exceptions inherit from `TaskiqFlowError` base class.

```python
from taskiq_flow import TaskiqFlowError
```

| Exception | Meaning | Typical Cause |
|-----------|---------|--------------|
| `PipelineError` | Generic pipeline failure | Step failed |
| `CycleError` | Circular dependency detected | DAG has cycle |
| `TaskNotFoundError` | Task not in registry | Missing task in DataflowPipeline |
| `InvalidOutputError` | Output key conflict | Two tasks declare same output |
| `ConfigurationError` | Invalid pipeline config | Missing middleware, bad parameters |
| `TrackingError` | Tracking operation failed | Storage unavailable |

**Example handling**:
```python
try:
    result = await pipeline.kiq(data)
except CycleError as e:
    print(f"DAG cycle detected: {e}")
except PipelineError as e:
    print(f"Pipeline failed: {e}")
```

---

## Utilities

### DataflowRegistry

For manual DAG construction and inspection.

```python
from taskiq_flow import DataflowRegistry

registry = DataflowRegistry()
registry.register_task(task, output="out", inputs=["in"])
dag = registry.build_dag()
```

See detailed documentation in `docs/en/api/dataflow.md`.

---

### ExecutionEngine

Low-level DAG executor for advanced use cases.

```python
from taskiq_flow import ExecutionEngine

engine = ExecutionEngine(broker, dag)
results = await engine.execute(inputs={"x": 1, "y": 2})
```

See execution API docs.

---

### PipelineScheduler

Cron-based pipeline scheduling.

```python
from taskiq_flow import PipelineScheduler

scheduler = PipelineScheduler(broker)
await scheduler.schedule(pipeline, cron="* * * * *")
await scheduler.start()
```

See scheduling guide.

---

## Version Compatibility

This documentation covers **Taskiq-Flow v0.3.0+**.

API stability:
- `Pipeline` and `DataflowPipeline`: Stable (v0.3+)
- `pipeline_task` decorator: Stable (v0.3+)
- `PipelineMiddleware`: Stable (v0.3+)
- `PipelineScheduler`: Stable (v0.3+)
- `PipelineTrackingManager`: Stable (v0.3+)

Breaking changes will be noted in [CHANGELOG.md](https://github.com/dorel14/taskiq-flow/blob/main/CHANGELOG.md).

---

*For detailed examples, see the [Examples]({{ '/en/examples/' | relative_url }}) section. For method-level documentation, refer to inline Python docstrings (`help(Pipeline)`).*
