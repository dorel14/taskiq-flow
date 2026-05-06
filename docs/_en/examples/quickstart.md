---
permalink: /en/examples/quickstart/
title: Exemple: quickstart.py
nav_order: 41
---
# Exemple: quickstart.py

**Basic sequential pipeline with map, filter, and group operations**

> **Version**: 0.4.0 | **File**: `examples/quickstart.py`

---

## Overview

This example demonstrates the fundamentals of Taskiq-Flow using a classic sequential pipeline. It covers:

- Task definition with `@broker.task`
- Pipeline construction with `.call_next()`, `.map()`, `.filter()`
- Running the pipeline and retrieving results
- Understanding data flow through steps

---

## Code Walkthrough

```python
import asyncio
from taskiq import InMemoryBroker
from taskiq_flow import Pipeline, PipelineMiddleware

# 1. Initialize broker and add middleware
broker = InMemoryBroker()
broker.add_middlewares(PipelineMiddleware())

# 2. Define tasks
@broker.task
def add_one(value: int) -> int:
    return value + 1

@broker.task
def repeat(value: int, times: int) -> list[int]:
    return [value] * times

@broker.task
def is_positive(value: int) -> bool:
    return value >= 0

# 3. Build pipeline
async def main():
    pipeline = (
        Pipeline(broker)
        .call_next(add_one)           # Step 1: 1 → 2
        .call_next(repeat, times=4)   # Step 2: 2 → [2,2,2,2]
        .map(add_one)                  # Step 3: [2,2,2,2] → [3,3,3,3]
        .filter(is_positive)           # Step 4: keep positives (all kept)
    )

    # 4. Execute
    task = await pipeline.kiq(1)
    result = await task.wait_result()
    print("Result:", result.return_value)  # [3, 3, 3, 3]

asyncio.run(main())
```

---

## Step-by-Step Explanation

### Step 1: `call_next(add_one)`

- **Input**: `1`
- **Operation**: `add_one(1) = 2`
- **Output**: `2`

### Step 2: `call_next(repeat, times=4)`

- **Input**: `2`
- **Operation**: `repeat(2, times=4) = [2, 2, 2, 2]`
- **Output**: `[2, 2, 2, 2]`

### Step 3: `map(add_one)`

- **Input**: `[2, 2, 2, 2]` (iterable)
- **Operation**: Apply `add_one` to each element **in parallel**
  - `add_one(2) = 3`
  - `add_one(2) = 3`
  - `add_one(2) = 3`
  - `add_one(2) = 3`
- **Output**: `[3, 3, 3, 3]`

### Step 4: `filter(is_positive)`

- **Input**: `[3, 3, 3, 3]` (iterable)
- **Operation**: Keep elements where `is_positive(element) == True`
  - All 4 elements are positive → all kept
- **Output**: `[3, 3, 3, 3]`

---

## Key Concepts Demonstrated

1. **Task definition** — Every pipeline step must be a task (`@broker.task`)
2. **Middleware requirement** — `PipelineMiddleware` **must** be added to broker
3. **Data flow** — Each step receives previous output (except `call_after`)
4. **Parallel execution** — `.map()` runs elements concurrently
5. **Chaining** — Methods return pipeline for fluent interface

---

## Running the Example

```bash
python examples/quickstart.py
```

Expected output:
```
Result: [3, 3, 3, 3]
```

---

## Variations to Try

### Use `filter` to remove negatives

```python
@broker.task
def subtract_three(value: int) -> int:
    return value - 5  # results in [-2, -2, -2, -2]

pipeline = (
    Pipeline(broker)
    .call_next(add_one)
    .call_next(repeat, times=4)
    .map(subtract_three)  # [2,2,2,2] → [-2,-2,-2,-2]
    .filter(is_positive)   # [] — all filtered out
)
```

### Use `group` for parallel independent tasks

```python
@broker.task
def task_a(x: int) -> int: return x * 2
@broker.task
def task_b(x: int) -> int: return x + 10
@broker.task
def task_c(x: int) -> int: return x ** 2

pipeline = Pipeline(broker).call_next(add_one)  # 1 → 2
pipeline.group([task_a, task_b, task_c], param_names=["x"])
# All three receive 2 and run in parallel
# Result: [4, 12, 4]
```

---

## Learning Path

After this example:

1. **[Dataflow Pipelines]({{ '/en/guides/pipelines.md#2-dataflow-pipeline' | relative_url }})** — Automatic DAG construction
2. **[Task Definition]({{ '/en/guides/tasks/' | relative_url }})** — Advanced task features
3. **[Tracking]({{ '/en/guides/tracking/' | relative_url }})** — Monitor pipeline execution
4. **[MapReduce]({{ '/en/guides/execution.md#3-map-reduce-pattern' | relative_url }})** — Batch processing pattern

---

*This example is the "Hello World" of Taskiq-Flow. Master it before moving to more complex patterns.*
