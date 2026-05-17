---
permalink: /en/examples/resource-aware-demo/
title: Example: resource_aware_demo.py
nav_order: 49
color_scheme: dark
---
# Example: resource_aware_demo.py

**Dynamic parallelism based on CPU/memory**

> **Version**: {VERSION} | **File**: `examples/resource_aware_demo.py`

---

## Overview

This example demonstrates the `ResourceAwareExecutor` and `TaskResourceProfile` features introduced in v0.4.5. It shows how to:

- Annotate tasks with resource requirements (CPU, memory, I/O vs CPU-bound)
- Compute optimal parallelism based on current system resources
- Adjust `max_parallel` dynamically to avoid overloading the host
- Apply different parallelism strategies for I/O-bound vs CPU-bound tasks

---

## What This Example Shows

- Defining `TaskResourceProfile` for tasks
- Creating a `ResourceAwareExecutor` with system limits
- Querying `get_optimal_parallelism()` for task types
- Using resource profiles in DataflowPipeline
- Manual parallelism tuning guidelines

---

## Code Walkthrough

### 1. Resource-Aware Executor Setup

```python
from taskiq_flow.optimization import ResourceAwareExecutor, TaskResourceProfile

executor = ResourceAwareExecutor(
    max_cpu_percent=80.0,   # Don't exceed 80% CPU usage
    max_memory_percent=80.0,  # Don't exceed 80% RAM
    min_parallel=1,
    max_parallel=20,
)

# Query optimal parallelism for a given task resource estimate
optimal_light = executor.get_optimal_parallelism(
    task_memory_estimate=50,   # 50 MB per task
    task_cpu_estimate=0.2,     # 0.2 cores per task
)
print(f"Optimal for light tasks: {optimal_light}")

optimal_heavy = executor.get_optimal_parallelism(
    task_memory_estimate=200,  # 200 MB per task
    task_cpu_estimate=1.0,     # 1.0 core per task
)
print(f"Optimal for heavy tasks: {optimal_heavy}")
```

The executor queries current system load (via `psutil`) and computes how many tasks of the given profile can run in parallel without exceeding the configured limits.

---

### 2. Annotating Tasks with Resource Profiles

```python
@broker.task
@pipeline_task(
    output="light_result",
    resources=TaskResourceProfile(
        estimated_memory_mb=50,
        estimated_cpu_cores=0.2,
        io_bound=True,
    ),
)
async def light_task(item: int) -> dict:
    await asyncio.sleep(0.1)  # Simulate I/O
    return {"item": item, "result": item * 2}

@broker.task
@pipeline_task(
    output="heavy_result",
    resources=TaskResourceProfile(
        estimated_memory_mb=200,
        estimated_cpu_cores=1.0,
        io_bound=False,
    ),
)
async def heavy_task(item: int) -> dict:
    total = 0
    for _ in range(100000):
        total += item * 2
    return {"item": item, "result": total}
```

**ResourceProfile fields:**

- `estimated_memory_mb`: Expected memory usage per task instance
- `estimated_cpu_cores`: CPU cores required (0.5 = half a core)
- `io_bound`: True for I/O-heavy tasks (network, disk), False for CPU-heavy

---

### 3. Using Resource Profiles in Pipelines

The `DataflowPipeline`'s `max_parallel` parameter acts as an upper bound. The `ResourceAwareExecutor` can be used to compute a dynamic `max_parallel` before launching:

```python
# Compute optimal parallelism for current system state
current_parallel = executor.get_optimal_parallelism(
    task_memory_estimate=50,
    task_cpu_estimate=0.2,
)

pipeline = DataflowPipeline(broker, max_parallel=current_parallel)
pipeline.map(light_task, items=list(range(20)), output="light_results")
results = await pipeline.kiq_dataflow()
```

For mixed workloads, sum resource usage across parallel tasks.

---

### 4. Manual Parallelism Tuning Guidelines

```python
import psutil

cpu_count = psutil.cpu_count() or 4
memory_gb = psutil.virtual_memory().total / (1024 ** 3)

# I/O-bound tasks: can oversubscribe CPU (they spend time waiting)
io_parallel = min(50, cpu_count * 5)

# CPU-bound tasks: limit to available cores ± a small buffer
cpu_parallel = min(cpu_count + 2, 20)

print(f"Recommended max_parallel for I/O-bound: {io_parallel}")
print(f"Recommended max_parallel for CPU-bound: {cpu_parallel}")
```

Start conservative, benchmark, and adjust.

---

## Expected Output

```
=== Resource-Aware Parallelism Demo ===

Current system state:
  CPU Usage: ? (will query at runtime)
  Memory: ? (will query at runtime)

--- Light tasks (I/O bound) ---
  Optimal parallelism for light tasks: 25

--- Heavy tasks (CPU bound) ---
  Optimal parallelism for heavy tasks: 4

Note: Actual values depend on current system load.


=== Pipeline with Resource-Aware Execution ===

Pipeline structure:
  [items:20] --light_task--> [light_results]
  [items:10] --heavy_task--> [heavy_results]
  [light_results, heavy_results] --combine--> [final]

Executing pipeline...
 Pipeline completed: {'light_results': [...], 'heavy_results': [...], 'final': {...}}

TaskResourceProfile allows you to annotate tasks with resource requirements.
ResourceAwareExecutor uses these profiles to compute optimal parallelism.


=== Manual Parallelism Tuning ===

System: 8 CPU cores, 15.6 GB RAM
Recommended max_parallel for I/O-bound tasks: 40
Recommended max_parallel for CPU-bound tasks: 10
Start with conservative values and benchmark:
  pipeline.map(light_task, items, max_parallel=10)
  pipeline.map(heavy_task, items, max_parallel=cpu_count)


=== Resource-Aware Demo Complete ===

Key takeaways:
1. Use TaskResourceProfile to annotate task resource needs
2. ResourceAwareExecutor computes optimal parallelism at runtime
3. Adjust max_parallel based on task type (I/O vs CPU)
4. Monitor system resources and tune accordingly
```

---

## Key Points

### Why Resource-Aware Parallelism?

Without resource awareness, setting `max_parallel` too high can:
- Exhaust memory → OOM kills
- Saturate CPU → tasks thrash, overall slowdown
- Starve other services on the same host

`ResourceAwareExecutor` prevents this by querying current system usage and computing safe parallelism levels.

### Best Practices

1. **Profile your tasks**: Measure actual memory/CPU usage in production
2. **Set conservative defaults**: Start with `max_parallel=5` and increase
3. **Monitor**: Watch system metrics while pipelines run
4. **Tune per task type**: I/O-bound tasks can be more parallel than CPU-bound
5. **Use `min_parallel` and `max_parallel` bounds**: `ResourceAwareExecutor` respects these

### Integration with Monitoring

Combine with Prometheus metrics:

```python
from taskiq_flow.metrics import MetricsMiddleware
broker.add_middlewares(MetricsMiddleware())
```

Track:
- `taskiq_flow_worker_cpu_usage_percent`
- `taskiq_flow_worker_memory_usage_bytes`
- `taskiq_flow_pipeline_executions_total`

---

## Learning Path

After this example:

1. **[Performance Guide]({{ '/en/guides/performance/' | relative_url }})** — Resource-aware parallelism deep dive
2. **[Optimization Module API]({{ '/en/api/optimization/' | relative_url }})** — Full `ResourceAwareExecutor` reference
3. **[Tracking Guide]({{ '/en/guides/tracking/' | relative_url }})** — Monitor resource usage over time

---

*This example keeps your pipelines from overwhelming the host. Always test resource profiles with realistic data volumes.*
