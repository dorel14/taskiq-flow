---
title: Optimization API
nav_order: 35
permalink: /en/api/optimization/
color_scheme: dark
---
# Optimization API

**Resource-aware parallelism and execution optimization**

> **Version**: 0.4.5 | **Module**: `taskiq_flow.optimization`, `taskiq_flow.optimization.parallel`

---

## Overview

The `taskiq_flow.optimization` module provides tools for optimizing pipeline execution based on system resources. It helps prevent overloading the host by dynamically adjusting parallelism.

Key components:

- **`ResourceAwareExecutor`** — Computes optimal parallelism based on CPU/memory constraints
- **`TaskResourceProfile`** — Annotates tasks with resource requirements
- **`get_default_executor()`** — Returns a singleton executor with system defaults

---

## ResourceAwareExecutor

```python
from taskiq_flow.optimization import ResourceAwareExecutor

executor = ResourceAwareExecutor(
    max_cpu_percent=80.0,      # Max CPU usage allowed (percentage)
    max_memory_percent=80.0,   # Max memory usage allowed (percentage)
    min_parallel=1,            # Minimum parallelism floor
    max_parallel=100,          # Maximum parallelism ceiling
)
```

### Methods

#### `get_optimal_parallelism(task_memory_estimate: int, task_cpu_estimate: float) -> int`

Compute the maximum number of concurrent tasks that fit within resource limits.

**Parameters:**
- `task_memory_estimate` — Expected memory per task (MB)
- `task_cpu_estimate` — Expected CPU cores per task (0.5 = half a core)

**Returns:** Optimal number of parallel instances

**Example:**

```python
optimal = executor.get_optimal_parallelism(
    task_memory_estimate=100,   # 100 MB per task
    task_cpu_estimate=0.5,      # 0.5 cores per task
)
print(f"Run up to {optimal} tasks in parallel")
```

The executor queries `psutil` for current system usage and computes capacity remaining.

---

## TaskResourceProfile

```python
from taskiq_flow.optimization import TaskResourceProfile

profile = TaskResourceProfile(
    estimated_memory_mb=256,     # Memory needed per task
    estimated_cpu_cores=1.0,     # CPU cores needed
    io_bound=False,              # True = I/O wait, False = CPU intensive
)
```

Use with `@pipeline_task`:

```python
@broker.task
@pipeline_task(
    output="result",
    resources=TaskResourceProfile(
        estimated_memory_mb=512,
        estimated_cpu_cores=2.0,
        io_bound=False,
    ),
)
async def heavy_computation(data: dict) -> dict:
    ...
```

### Fields

| Field | Type | Description |
|-------|------|-------------|
| `estimated_memory_mb` | int | Expected RAM usage per task instance |
| `estimated_cpu_cores` | float | CPU cores required (0.25, 0.5, 1.0, etc.) |
| `io_bound` | bool | `True` if task spends time waiting (network/disk), `False` if CPU-bound |

---

## get_default_executor

```python
from taskiq_flow.optimization import get_default_executor

executor = get_default_executor()
# Returns a singleton ResourceAwareExecutor with default settings
```

Convenient for quick access without manual configuration.

---

## Integration with DataflowPipeline

Pass `max_parallel` computed by the executor to your pipeline:

```python
from taskiq_flow import DataflowPipeline

executor = ResourceAwareExecutor()
optimal_parallel = executor.get_optimal_parallelism(
    task_memory_estimate=50,
    task_cpu_estimate=0.2,
)

pipeline = DataflowPipeline(broker, max_parallel=optimal_parallel)
pipeline.map(light_task, items, output="results")
results = await pipeline.kiq_dataflow()
```

For mixed workloads, compute a safe `max_parallel` that accommodates the most resource-intensive task type.

---

## Best Practices

1. **Profile tasks in production** — Measure actual memory/CPU under load
2. **Set conservative defaults** — Start with `max_parallel=5` and increase gradually
3. **Monitor system metrics** — Watch `psutil.cpu_percent()` and `memory.percent` while running
4. **Differentiate task types** — I/O-bound tasks can have higher `max_parallel` than CPU-bound
5. **Use bounds** — `ResourceAwareExecutor` respects `min_parallel` and `max_parallel` limits

---

## Related

- **[Performance Guide]({{ '/en/guides/performance/' | relative_url }})** — In-depth discussion of resource-aware parallelism
- **[Tracking Guide]({{ '/en/guides/tracking/' | relative_url }})** — Monitor resource usage over time
- **[Example: Resource-Aware Demo]({{ '/en/examples/resource-aware-demo/' | relative_url }})** — Complete working demo

---

*The optimization module ensures pipelines scale without overwhelming the host. Always test resource profiles with realistic data volumes.*
