# Tasks Guide

**Defining tasks, decorators, metadata, and resource management**

> **Version**: 0.3.2 | **Related**: [Pipelines Guide](/docs/en/guides/pipelines.md), [Execution Guide](/docs/en/guides/execution.md)

---

## Overview

Tasks are the fundamental building blocks of Taskiq-Flow pipelines. This guide covers:

- Task definition with `@broker.task`
- The `@pipeline_task` decorator for dataflow pipelines
- Task metadata and annotations
- Resource profiles and constraints
- Retry configuration
- Input/output specification

---

## 1. What Is a Task?

A **Task** is an asynchronous function that can be executed by a Taskiq broker, optionally with retry logic, timeouts, and metadata for pipeline orchestration.

### Minimal Task Definition

```python
from taskiq import InMemoryBroker

broker = InMemoryBroker()

@broker.task
async def my_task(value: int) -> int:
    return value * 2
```

**Requirements**:
- Must be an `async def` function (or regular `def` for sync tasks)
- Must be decorated with `@broker.task` (or `@broker.task(...)` with options)
- Can accept any serializable parameters
- Must return a JSON-serializable value

---

## 2. Task Decorators

### 2.1. `@broker.task` — Basic Task

```python
@broker.task
def add(a: int, b: int) -> int:
    return a + b
```

**Options**:

```python
@broker.task(
    timeout=30,           # Seconds before task times out
    retry_policy=None,    # Custom RetryPolicy (see Retry Guide)
    max_retries=3,        # Override global default
    queue="default",      # Route to specific queue
    labels={"type": "cpu"} # Custom metadata labels
)
async def slow_task():
    await asyncio.sleep(10)
    return "done"
```

### 2.2. `@pipeline_task` — Dataflow Annotation

For `DataflowPipeline`, use `@pipeline_task(output=...)` to declare what the task produces:

```python
from taskiq_flow import pipeline_task

@broker.task
@pipeline_task(output="features")
def extract(data: list[str]) -> dict:
    return {"features": compute_features(data)}

# Downstream task automatically receives 'features' parameter:
@broker.task
@pipeline_task(output="tags")
def tag(features: dict) -> list[str]:
    # 'features' is automatically passed from extract_task
    return generate_tags(features)
```

**Parameters**:

| Parameter | Type | Description |
|-----------|------|-------------|
| `output` | `str` | Output key name (must match downstream parameter names) |
| `outputs` | `list[str]` | Multiple outputs (for tuple-returning tasks) |
| `inputs` | `list[str]` | Explicit input dependencies (overrides automatic) |
| `description` | `str` | Human-readable task description |

**Multiple outputs**:
```python
@broker.task
@pipeline_task(outputs=["features", "metadata"])
def split_output(data: str) -> tuple[dict, dict]:
    features = extract_features(data)
    metadata = extract_metadata(data)
    return features, metadata  # tuple unpacked to both outputs
```

### 2.3. `@pipeline_task_multi_output` — Alternative

Same as `@pipeline_task(outputs=[...])`; provided for clarity:

```python
from taskiq_flow import pipeline_task_multi_output

@broker.task
@pipeline_task_multi_output(outputs=["x", "y"])
def split(value: int) -> tuple[int, int]:
    return value // 2, value % 2
```

---

## 3. Task Metadata

Enhance tasks with metadata for documentation, monitoring, and auto-discovery.

### 3.1. Standard Attributes

```python
@broker.task(
    name="process_audio_track",  # Override auto-generated name
    labels={
        "category": "audio_processing",
        "priority": "high"
    }
)
async def process_track(track_id: str) -> dict:
    return {"track": track_id, "status": "processed"}
```

### 3.2. Custom Task Info

```python
from taskiq_flow import TaskInfo

task_info = TaskInfo(
    name="extract_spectrogram",
    description="Extract mel-spectrogram from audio waveform",
    parameters={
        "sample_rate": {"type": "int", "default": 22050},
        "n_mels": {"type": "int", "default": 128}
    },
    outputs=["spectrogram", "sample_rate"]
)

@broker.task
@pipeline_task(output="spectrogram", description=task_info.description)
def extract_spectrogram(audio: np.ndarray, sample_rate: int = 22050, n_mels: int = 128):
    # implementation...
    return spectrogram
```

---

## 4. Resource Profiles

Control CPU and memory allocation per task for resource-aware scheduling.

### 4.1. CPU Profile

```python
from taskiq_flow import CPUProfile

@broker.task
@CPUProfile(cpu_units=2)  # Requires 2 CPU cores
def heavy_computation(data):
    # This task will be scheduled on workers with at least 2 cores
    pass
```

**`cpu_units` values**:

| Value | Meaning |
|-------|---------|
| `0.5` | Half a core (background task) |
| `1` | One full core (default) |
| `2` | Two cores (CPU-intensive) |

### 4.2. RAM Profile

```python
from taskiq_flow import RAMProfile

@broker.task
@RAMProfile(ram_mb=2048)  # Requires 2GB RAM
def memory_intensive(data):
    # Will only run on workers with at least 2GB available RAM
    pass
```

**Resource-aware scheduling** (requires compatible worker pool):

```python
from taskiq_flow import ResourceAwareWorkerPool

pool = ResourceAwareWorkerPool(
    workers=[
        {"cpu_cores": 4, "ram_gb": 8},
        {"cpu_cores": 2, "ram_gb": 4},
    ]
)
# Tasks are routed to workers with sufficient resources
```

### 4.3. Combined Profiles

```python
from taskiq_flow import CPUProfile, RAMProfile

@broker.task
@CPUProfile(cpu_units=4)
@RAMProfile(ram_mb=4096)
def gpu_style_task(data):
    # High-resource task
    pass
```

---

## 5. Input/Output Specification

### 5.1. Type Hints for Documentation

```python
@broker.task
async def process(
    text: str,                   # Required input
    max_length: int = 100,       # Optional with default
    *,
    strict: bool = False         # Keyword-only argument
) -> dict:
    return {"processed": text[:max_length]}
```

### 5.2. Pydantic Models (Recommended for Complex Data)

```python
from pydantic import BaseModel

class AudioFeatures(BaseModel):
    duration: float
    tempo: float
    key: str

@broker.task
async def extract_features(audio_path: str) -> AudioFeatures:
    # Pydantic validates and serializes automatically
    return AudioFeatures(duration=180.0, tempo=120.0, key="C")
```

### 5.3. Output Multiple Values

Tasks can return any JSON-serializable type:

```python
@broker.task
def split(data: str) -> tuple[str, str]:
    return data[:10], data[10:]  # Returns two values

# With @pipeline_task(outputs=["first", "second"])
@pipeline_task(outputs=["head", "tail"])
def split(data):
    return data[:10], data[10:]
# Produces two outputs: "head" and "tail"
```

---

## 6. Retry Configuration

### 6.1. Decorator-Level Retry

```python
@broker.task(
    retry_policy={
        "max_retries": 3,
        "delay": 5.0,
        "backoff": 2.0  # Exponential backoff multiplier
    }
)
async def flaky_task():
    # Will retry up to 3 times with delays: 5s, 10s, 20s
    possibly_fails()
```

### 6.2. Pipeline-Level Retry

Apply retry to all tasks in a pipeline：

```python
pipeline = Pipeline(broker)
pipeline.with_retry(
    max_attempts=3,
    delay=1.0,
    backoff=1.5
)
```

### 6.3. Conditional Retry

Only retry on specific exceptions：

```python
from taskiq.exceptions import RetryException

@broker.task
async def task_with_conditional_retry():
    try:
        call_external_api()
    except NetworkError:
        raise RetryException("Network error, retry allowed")
    except ValidationError:
        raise  # No retry, fail immediately
```

Detailed retry strategies covered in [Retry Guide](/docs/en/guides/retry.md).

---

## 7. Task Discovery & Registry

### 7.1. Automatic Discovery

`DataflowPipeline.from_tasks()` automatically detects dependencies via type hints and `@pipeline_task` decorators.

### 7.2. Manual Registration

For dynamic pipelines, use `DataflowRegistry`:

```python
from taskiq_flow import DataflowRegistry

registry = DataflowRegistry()

# Register with explicit I/O mapping
registry.register_task(
    task=process_data,
    output="processed",
    inputs=["raw"]  # depends on task that outputs "raw"
)

# Discover from module
import my_tasks
for task in my_tasks.ALL_TASKS:
    registry.register_task_from_object(task)
```

See `examples/registry_discovery_example.py`.

---

## 8. Writing Testable Tasks

Tasks should be pure functions for easy testing:

```python
@broker.task
def process(data: dict) -> dict:
    # Pure function: output depends only on input
    return {"result": data["value"] * 2}

# Unit test
def test_process():
    assert process({"value": 5}) == {"result": 10}
```

**Testing with broker**:

```python
import pytest
from taskiq import InMemoryBroker

@pytest.fixture
def test_broker():
    return InMemoryBroker(await_inplace=True)

async def test_task_execution(test_broker):
    @test_broker.task
    async def my_task(x: int) -> int:
        return x + 1

    result = await my_task.kiq(5)
    value = await result.wait_result()
    assert value.return_value == 6
```

---

## 9. Common Patterns

### 9.1. Idempotency

Design tasks to be safely re-runnable:

```python
@broker.task
@pipeline_task(output="user_processed")
def process_user(user_id: str) -> dict:
    # Check if already processed
    if cache.get(f"processed:{user_id}"):
        return {"status": "already_done"}
    # Perform processing
    result = heavy_compute(user_id)
    cache.set(f"processed:{user_id}", result, ttl=3600)
    return result
```

### 9.2. Composability

Break complex logic into small, reusable tasks:

```python
@broker.task
def validate(data): ...

@broker.task
def transform(data): ...

@broker.task
def enrich(data): ...

# Compose in multiple pipelines
pipeline1 = Pipeline(broker).call_next(validate).call_next(transform)
pipeline2 = Pipeline(broker).call_next(validate).call_next(enrich)
```

### 9.3. Progress Reporting

For long-running tasks, report progress via callbacks or logging：

```python
@broker.task
async def long_task(items: list, progress_callback=None):
    for i, item in enumerate(items):
        result = process(item)
        if progress_callback:
            await progress_callback(i / len(items))
    return "done"
```

---

## 10. Anti-Patterns to Avoid

| Anti-pattern | Why it's bad | Better approach |
|--------------|--------------|-----------------|
| Side effects in tasks | Makes testing/hard to reason about | Keep tasks pure; use `.call_after()` for side effects |
| Large return values | High memory, slow serialization | Store large results externally (DB, S3); return reference |
| Shared mutable state | Race conditions in parallel | Each task independent; pass data via return values |
| Blocking I/O without async | Blocks event loop | Use async libraries (aiohttp, asyncpg, etc.) |
| Tasks doing too much | Hard to reuse, test, debug | Break into smaller, focused tasks |

---

## 11. Summary

Taskiq-Flow tasks are:

- **Flexible** — Regular Python functions with `@broker.task`
- **Observable** — Metadata, labels, and tracking
- **Resilient** — Retry policies, timeouts, error handling
- **Composable** — Small functions combine into complex workflows
- **Resource-aware** — CPU/RAM profiles for optimized scheduling

---

## Next Steps

- **[Pipeline Types](/docs/en/guides/pipelines.md)** — Building workflows with tasks
- **[Execution Guide](/docs/en/guides/execution.md)** — Running pipelines and handling results
- **[Retry Guide](/docs/en/guides/retry.md)** — Robust error recovery strategies

---

*Tasks are your workflow atoms. Learn to compose them in [Pipelines](/docs/en/guides/pipelines.md).*
