---
permalink: /en/api/decorators/
title: API Reference: Decorators
nav_order: 31
color_scheme: dark
---
# API Reference: Decorators

**Task decorators, pipeline_task, and utility decorators**

> **Version**: 0.4.5 | **Module**: `taskiq_flow.decorators`

---

## Overview

The `@pipeline_task` decorator annotates taskiq tasks with output declarations, enabling automatic dependency resolution in DataflowPipeline.

---

## @pipeline_task

Marks a task with what it produces for downstream consumers.

```python
from taskiq_flow import pipeline_task

@broker.task
@pipeline_task(output="features")
def extract(data: list[str]) -> dict:
    return compute_features(data)
```

**Parameters**:

| Parameter | Type | Description |
|-----------|------|-------------|
| `output` | `str` | Single output key name |
| `outputs` | `list[str]` | Multiple output keys (for tuple returns) |
| `inputs` | `list[str]` | Explicit input dependencies (optional, auto-detected) |
| `description` | `str` | Human-readable description (for documentation) |

**Usage patterns**:

### Single output (most common)

```python
@broker.task
@pipeline_task(output="processed_data")
def process(raw_data: str) -> dict:
    return {"result": raw_data.upper()}
```

### Multiple outputs

```python
@broker.task
@pipeline_task(outputs=["features", "metadata"])
def split_output(audio: np.ndarray) -> tuple[dict, dict]:
    features = extract_features(audio)
    metadata = extract_meta(audio)
    return features, metadata  # unpacked to both outputs
```

Downstream tasks can consume either output:

```python
@broker.task
@pipeline_task(output="tags")
def tag(features: dict): ...  # consumes 'features' output

@broker.task
@pipeline_task(output="info")
def describe(metadata: dict): ...  # consumes 'metadata' output
```

---

## @pipeline_task_multi_output

Alias for `@pipeline_task(outputs=[...])`. Provides clarity for multi-output tasks:

```python
from taskiq_flow import pipeline_task_multi_output

@broker.task
@pipeline_task_multi_output(outputs=["x", "y"])
def split(value: int) -> tuple[int, int]:
    return value // 2, value % 2
```

---

## Utility Functions

### get_task_outputs(task: Callable) -> list[str]

Get declared output keys for a task:

```python
from taskiq_flow import get_task_outputs

outputs = get_task_outputs(extract_task)
print(outputs)  # ['features']
```

### get_task_inputs(task: Callable) -> list[str]

Get declared input dependencies:

```python
from taskiq_flow import get_task_inputs

inputs = get_task_inputs(tag_task)
print(inputs)  # ['features']
```

### is_pipeline_task(task: Callable) -> bool

Check if a function has been decorated with `@pipeline_task`:

```python
from taskiq_flow import is_pipeline_task

if is_pipeline_task(my_func):
    print("This is a pipeline task with output declarations")
```

### resolve_task_dependencies(tasks: list[Callable]) -> dict

Build a dependency map:

```python
from taskiq_flow import resolve_task_dependencies

deps = resolve_task_dependencies([task_a, task_b, task_c])
# Returns: {task_a: [], task_b: ['features'], task_c: ['tags']}
```

---

## Decorator Order

The decorator order matters: `@broker.task` must be outermost (applied last), `@pipeline_task` inner (applied first):

```python
# CORRECT
@broker.task
@pipeline_task(output="result")
def my_task(): ...

# INCORRECT (will fail)
@pipeline_task(output="result")
@broker.task
def my_task(): ...
```

Why: `@broker.task` wraps the function; `@pipeline_task` attaches metadata to the original function. Python applies decorators bottom-to-top.

---

## Type Hints & Static Analysis

Type hints help IDEs and static checkers understand dataflow:

```python
from typing import TypedDict

class AudioFeatures(TypedDict):
    duration: float
    tempo: float

@broker.task
@pipeline_task(output="features")
def extract(path: str) -> AudioFeatures:
    return {"duration": 180.0, "tempo": 120.0}

@broker.task
@pipeline_task(output="tags")
def tag(features: AudioFeatures) -> list[str]:  # type-safe
    return ["fast", "electronic"]
```

Using `TypedDict` or Pydantic models provides better IDE autocomplete and mypy checking.

---

## Versioning & Metadata

Attach version and other metadata:

```python
@broker.task(
    name="extract_features_v2",
    labels={"version": "2.0.0", "experimental": False}
)
@pipeline_task(
    output="features",
    description="Extract audio features (v2 with improvedtempo estimation)"
)
def extract(path: str) -> dict:
    ...
```

---

## Common Pitfalls

| Pitfall | Consequence | Fix |
|----------|-------------|-----|
| Missing `@broker.task` | Task not registered with broker | Add decorator |
| `output` not set | No downstream consumers can depend on it | Always declare `output` for dataflow tasks |
| Output name mismatch | Downstream task doesn't receive input | Ensure downstream parameter name matches upstream `output` |
| Using `@pipeline_task` on SequentialPipeline tasks | No effect but unnecessary | Only needed for DataflowPipeline |

---

## Example: Complete Dataflow Pipeline

```python
from taskiq import InMemoryBroker
from taskiq_flow import DataflowPipeline, pipeline_task

broker = InMemoryBroker()

@broker.task
@pipeline_task(output="raw")
def load(source: str) -> dict:
    return {"data": read_file(source)}

@broker.task
@pipeline_task(output="clean")
def clean(raw: dict) -> dict:
    return {"data": preprocess(raw["data"])}

@broker.task
@pipeline_task(output="stats")
def analyze(clean: dict) -> dict:
    return compute_stats(clean["data"])

# Build
pipeline = DataflowPipeline.from_tasks(broker, [load, clean, analyze])

# Execute
results = await pipeline.kiq_dataflow(source="data.csv")
# results = {"raw": {...}, "clean": {...}, "stats": {...}}
```

---

*For the full task API, see [Tasks Guide]({{ '/en/guides/tasks/' | relative_url }}). For writing custom decorators, extend `BaseTaskDecorator` from `taskiq_flow.decorators`.*
