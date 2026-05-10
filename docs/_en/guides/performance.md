---
title: Performance Optimization Guide
nav_order: 27
color_scheme: dark
---
# Performance Optimization Guide

**Resource-aware parallelism, memory optimization, and scaling strategies**

> **Version**: {VERSION} | **Related**: [Execution Guide]({{ '/en/guides/execution/' | relative_url }}), [Tracking Guide]({{ '/en/guides/tracking/' | relative_url }})

---

## Overview

Taskiq-Flow is designed for high-performance asynchronous execution. This guide covers optimization techniques to maximize throughput, minimize latency, and efficiently use system resources.

Topics covered:

- Parallelism tuning (`max_parallel`)
- CPU and RAM profiling
- Task resource profiles
- Memory management strategies
- Bottleneck identification
- Scaling from single worker to distributed

---

## 1. Understanding the Performance Landscape

Performance optimization involves tradeoffs between:

| Dimension | What it affects | Typical trade-off |
|-----------|-----------------|-------------------|
| **Concurrency** | Throughput (# tasks/second) | Memory usage, context switching |
| **Parallelism** | CPU utilization | Overhead of coordination |
| **Latency** | Task completion time | Resource consumption |
| **Memory** | Dataset size capacity | GC pauses, cache efficiency |
| **I/O** | External service calls | Network bandwidth, connection limits |

**Key insight**: Taskiq-Flow's parallelism is bounded by `max_parallel` settings across pipeline steps, and by available system resources (CPU cores, RAM).

---

## 2. Parallelism Tuning

### 2.1. The `max_parallel` Parameter

Control concurrent task execution at the step level:

```python
# Sequential Pipeline
pipeline.map(process_item, items, max_parallel=10)  # Max 10 concurrent

# Dataflow Pipeline: configure at pipeline level
pipeline = DataflowPipeline(broker, max_parallel=20)

# MapReduce
mapped = await MapReduce.map(
    broker,
    process_item,
    items,
    max_parallel=15
)
```

**Default behavior**: Without `max_parallel`, Taskiq-Flow attempts to run all independent tasks concurrently (essentially unlimited). This is fine for small numbers (<100) but dangerous for large datasets.

### 2.2. Determining Optimal `max_parallel`

#### For I/O-Bound Tasks (network calls, disk I/O)

```python
# High I/O wait, low CPU: can handle many concurrent tasks
pipeline.map(fetch_url, url_list, max_parallel=50)
# Rule of thumb: 2–5× number of CPU cores
```

**Rationale**: While one task waits for network, another uses CPU. High concurrency saturates I/O pipelines.

#### For CPU-Bound Tasks (computations, transcoding)

```python
# CPU-intensive: limit to core count (or slightly higher)
import os
cpu_cores = os.cpu_count() or 4
pipeline.map(transcode, files, max_parallel=cpu_cores + 2)
# Rule of thumb: CPU cores ± 2
```

**Rationale**: Python's GIL limits true parallelism; `asyncio` still benefits from multiple cores when tasks release GIL (NumPy, C extensions). Over-subscription leads to context switching overhead.

#### For Mixed Workloads

Profile and adjust:

```python
# Start conservative
for parallel in [5, 10, 20, 50]:
    start = time.time()
    await pipeline.kiq_dataflow(data)
    duration = time.time() - start
    print(f"Parallelism {parallel}: {duration:.2f}s")
```

Find the **knee of the curve** — point where increasing parallelism yields diminishing returns.

### 2.3. Global Parallelism Limit

Set a global cap across all pipelines:

```python
from taskiq_flow.optimization.parallel import set_max_parallel_tasks

set_max_parallel_tasks(100)  # Never exceed 100 concurrent tasks globally
```

Useful in multi-tenant systems to prevent one pipeline from starving others.

---

## 3. Resource-Aware Scheduling

Taskiq-Flow can schedule tasks based on CPU/RAM requirements (requires resource-aware worker pool — advanced).

### 3.1. Annotating Tasks with Resource Needs

```python
from taskiq_flow import CPUProfile, RAMProfile

@broker.task
@CPUProfile(cpu_units=2)  # Needs 2 CPU cores
@RAMProfile(ram_mb=4096)   # Needs 4GB RAM
def heavy_computation(data):
    # Will only run on workers with sufficient resources
    pass
```

### 3.2. Resource-Aware Worker Pool

```python
from taskiq_flow import ResourceAwareWorkerPool

pool = ResourceAwareWorkerPool(
    workers=[
        {"cpu_cores": 8, "ram_gb": 32, "labels": {"gpu": True}},
        {"cpu_cores": 4, "ram_gb": 16, "labels": {"gpu": False}},
    ]
)

# Tasks are automatically routed to compatible workers
```

**Note**: This feature requires custom worker implementation; standard brokers ignore resource profiles.

---

## 4. Memory Optimization

### 4.1. Avoid Large In-Memory Data Transfers

Pass references instead of full data:

```python
# ❌ Bad: copies entire dataset per task call
pipeline.map(process, large_dataset)  # Each task gets full dataset copy

# ✅ Better: pass identifiers, fetch inside task
@broker.task
def process(item_id: str):
    item = database.get(item_id)  # Fetch on-demand
    return process_item(item)

pipeline.map(process, item_ids)  # Only IDs passed
```

### 4.2. Stream Large Datasets

Use chunking:

```python
def chunked(iterable, chunk_size=100):
    for i in range(0, len(iterable), chunk_size):
        yield iterable[i:i + chunk_size]

for chunk in chunked(large_list, 100):
    results = await pipeline.kiq_dataflow(chunk)
    # Process results before next chunk to free memory
```

### 4.3. Clear Results After Use

Pipeline results stay in tracking storage. Clean up after you're done:

```python
# After processing, delete pipeline record
await tracking.delete_pipeline(pipeline.pipeline_id)
```

Or set TTL on storage:

```python
RedisPipelineStorage(redis, ttl_seconds=86400)  # Auto-delete after 1 day
```

---

## 5. Profiling & Bottleneck Detection

### 5.1. Built-in Timing

Each step records duration automatically (with tracking enabled):

```python
status = await tracking.get_status(pipeline_id)
for step in status.steps:
    print(f"{step.name}: {step.duration_ms}ms")
```

Identify slowest steps → optimization targets.

### 5.2. Memory Profiling

Use Python's `tracemalloc`:

```python
import tracemalloc

tracemalloc.start()

# Run pipeline
await pipeline.kiq(data)

# Check memory usage
current, peak = tracemalloc.get_traced_memory()
print(f"Current: {current/1024/1024:.1f} MB")
print(f"Peak: {peak/1024/1024:.1f} MB")
tracemalloc.stop()
```

### 5.3. CPU Profiling

```python
import cProfile
import pstats

profiler = cProfile.Profile()
profiler.enable()

await pipeline.kiq(data)

profiler.disable()
stats = pstats.Stats(profiler)
stats.sort_stats('cumulative')
stats.print_stats(20)  # Top 20 functions
```

### 5.4. Async-Specific Profiling

`uvloop` for faster event loop:

```python
import uvloop
uvloop.install()  # Replaces default asyncio event loop
```

Benchmark improvement: `uvloop` can provide 2×–3× speedup for I/O-bound workloads.

---

## 6. Database/External Service Optimization

### 6.1. Connection Pooling

For databases (PostgreSQL, Redis), reuse connections:

```python
from asyncpg import create_pool

pool = await create_pool(database="...", min_size=5, max_size=20)

@broker.task
async def db_task(query: str):
    async with pool.acquire() as conn:
        return await conn.fetch(query)
```

### 6.2. Batch Operations

Instead of many small calls, batch:

```python
# ❌ N separate calls
for item in items:
    await db.insert(item)

# ✅ Single batch insert
await db.bulk_insert(items)
```

### 6.3. Cache Results

```python
from functools import lru_cache

@broker.task
@lru_cache(maxsize=1000)
def expensive_computation(key: str):
    return compute(key)
```

Or use Redis cache:

```python
import redis
cache = redis.Redis(...)

@broker.task
async def cached_task(key: str):
    cached = await cache.get(key)
    if cached:
        return json.loads(cached)
    result = await compute(key)
    await cache.setex(key, 3600, json.dumps(result))
    return result
```

---

## 7. Distributed Scaling

### 7.1. Multiple Workers

Scale horizontally by running multiple worker processes:

```bash
# Terminal 1
taskiq worker --broker redis://localhost:6379

# Terminal 2
taskiq worker --broker redis://localhost:6379

# Terminal 3
taskiq worker --broker redis://localhost:6379
```

All workers share the same broker (Redis) and process tasks concurrently.

**Throughput ≈ (# workers) × (tasks/worker/second)**.

### 7.2. Worker Pool Management

Use a process manager (systemd, supervisord, Docker Compose):

```yaml
# docker-compose.yml
services:
  worker-1:
    image: taskiq-flow-worker
    command: taskiq worker --broker ${REDIS_URL}
  worker-2:
    image: taskiq-flow-worker
    command: taskiq worker --broker ${REDIS_URL}
  worker-3:
    image: taskiq-flow-worker
    command: taskiq worker --broker ${REDIS_URL}
```

### 7.3. Queue Prioritization

Route critical pipelines to dedicated queues:

```python
@broker.task(queue="high_priority")
def critical_task(): ...

# Workers can be configured to process specific queues first
```

### 7.4. Geo-Distribution

For low-latency global deployments, deploy workers in multiple regions with a global broker (Kafka) or regional Redis clusters with replication.

---

## 8. Benchmarking

Measure before and after optimization:

```python
import time

async def benchmark(pipeline, iterations=10):
    durations = []
    for _ in range(iterations):
        start = time.perf_counter()
        result = await pipeline.kiq(data)
        await result.wait_result()
        duration = time.perf_counter() - start
        durations.append(duration)

    avg = sum(durations) / len(durations)
    p95 = sorted(durations)[int(0.95 * len(durations))]
    print(f"Average: {avg:.3f}s, P95: {p95:.3f}s")
    return durations
```

**Key metrics**:

- **Throughput**: tasks/second
- **P50/P95/P99 latency**: median, 95th, 99th percentile
- **Memory peak**: maximum RSS/resident set size
- **CPU utilization**: % of cores used

---

## 9. Production Checklist

- [ ] Set `max_parallel` appropriately for task type (CPU vs I/O)
- [ ] Use connection pooling for external services
- [ ] Enable Redis storage for tracking (avoid memory leaks)
- [ ] Set TTL on tracking/result storage
- [ ] Configure timeouts on all tasks
- [ ] Add retry policies with backoff and jitter
- [ ] Monitor memory usage and set alerts
- [ ] Profile slow tasks with cProfile/tracemalloc
- [ ] Scale workers horizontally based on queue depth
- [ ] Use queue priorities for critical pipelines
- [ ] Implement DLQ and review failed tasks regularly
- [ ] Test failure scenarios (network partitions, service outages)

---

## 10. Troubleshooting Performance

### Pipeline Running Slowly

**Diagnostic steps**:

1. Check step durations in tracking:
   ```python
   status = await tracking.get_status(pipeline_id)
   slowest = max(status.steps, key=lambda s: s.duration_ms)
   print(f"Slowest step: {slowest.name} at {slowest.duration_ms}ms")
   ```

2. Profile with cProfile to see where time is spent
3. Verify `max_parallel` not too low
4. Check for blocking I/O (use async libraries)

### High Memory Usage

**Causes & fixes**:

| Cause | Fix |
|-------|-----|
| Large dataset in single step | Chunk data, process in batches |
| Results accumulating in tracking storage | Set TTL, delete after use |
| Memory leak in task code | Profile with `tracemalloc`, fix leaks |
| Too many parallel tasks | Reduce `max_parallel` |

### Worker Starvation

**Symptom**: Tasks queued but not executing.

**Fixes**:
- Increase number of worker processes
- Ensure broker (Redis) has enough connections
- Check for long-running tasks blocking queue
- Consider task priorities or separate queues

---

## 11. Advanced: Custom Executors

For specialized workloads, implement custom executors:

```python
from taskiq_flow import ExecutionEngine
from taskiq_flow.dataflow import DAG

class GPUOptimizedEngine(ExecutionEngine):
    async def schedule_task(self, task_node, inputs):
        # Custom scheduling logic: route GPU tasks to GPU workers
        if task_node.labels.get("requires_gpu"):
            return await self.gpu_worker_pool.submit(task_node, inputs)
        return await super().schedule_task(task_node, inputs)

engine = GPUOptimizedEngine(broker, dag)
results = await engine.execute(inputs)
```

---

## 12. Summary

Performance optimization is iterative:

1. **Measure** — establish baseline with benchmarks
2. **Identify** — find bottlenecks with profiling
3. **Tune** — adjust `max_parallel`, resource profiles, batching
4. **Scale** — add workers, optimize external services
5. **Monitor** — track metrics in production
6. **Repeat** — optimization never ends

---

## Next Steps

- **[Tracking Guide]({{ '/en/guides/tracking/' | relative_url }})** — Monitor pipeline metrics
- **[Dataflow Guide]({{ '/en/guides/dataflow/' | relative_url }})** — Complete guide to DAG pipelines and dataflow architecture
- **[API Guide]({{ '/en/guides/api/' | relative_url }})** — Build custom dashboards for performance
- **[Example: Dataflow Audio Pipeline]({{ '/en/examples/dataflow-audio-pipeline/' | relative_url }})** — See optimization in action

---

*Go fast, but measure first.*
