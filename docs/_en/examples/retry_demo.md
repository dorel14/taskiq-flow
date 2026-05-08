---
permalink: /en/examples/retry-demo/
title: Example: retry_demo.py
nav_order: 48
color_scheme: dark
---
# Example: retry_demo.py

**Retry middleware and error handling modes**

> **Version**: {VERSION} | **File**: `examples/retry_demo.py`

---

## Overview

This example demonstrates Taskiq-Flow v0.4.5's robust retry and error handling mechanisms. It covers:

- `PipelineRetryMiddleware` with exponential backoff and jitter
- `ErrorHandlingMode` strategies (FAIL_FAST, CONTINUE_ON_ERROR, SKIP_FAILED, DEAD_LETTER)
- `PipelineErrorAggregator` for collecting and analyzing failures
- Configuring retry policies per pipeline

---

## What This Example Shows

- Adding retry middleware to a broker
- Automatic retry with backoff for transient failures
- Switching between error handling modes
- Aggregating errors for post-mortem analysis
- Distinguishing retryable vs non-retryable failures

---

## Code Walkthrough

### 1. Retry Middleware

```python
from taskiq_flow.middlewares.retry import PipelineRetryMiddleware

retry_mw = PipelineRetryMiddleware(
    max_retries=3,
    delay=0.5,
    backoff=2.0,
    jitter=True,
)
broker.add_middlewares(retry_mw)
```

**Parameters:**
- `max_retries`: Maximum retry attempts (3 → total of 4 tries)
- `delay`: Initial delay before first retry (0.5s)
- `backoff`: Multiplier for delay on each retry (2.0 → 0.5s, 1s, 2s)
- `jitter`: Add random variation to avoid thundering herd

---

### 2. Flaky Task Demo

```python
import random

@broker.task
async def flaky_task(attempt: int = 0) -> str:
    """Fails randomly, then eventually succeeds."""
    attempt += 1
    if random.random() < 0.7 and attempt < 3:
        raise RuntimeError(f"Task failed on attempt {attempt}")
    return f"Success on attempt {attempt}"
```

```python
async def demo_retry_middleware():
    pipeline = Pipeline(broker).call_next(flaky_task)
    task = await pipeline.kiq(0)
    result = await task.wait_result(timeout=10)
    print(f"Pipeline succeeded! Result: {result.return_value}")
    print(f"Retry count: {retry_mw.retry_counts}")
```

Output:

```
Pipeline succeeded! Result: Success on attempt 2
Retry count: {'flaky_task': 1}
```

The middleware automatically retries the task once before success.

---

### 3. Error Handling Modes

```python
from taskiq_flow.errors import ErrorHandlingMode
from taskiq_flow.execution_engine import ExecutionEngine
from taskiq_flow.dataflow.registry import DataflowRegistry

registry = DataflowRegistry()
registry.register_task(flaky_task, output="flaky_output", inputs=[])
registry.register_task(process_result, output="final", inputs=["flaky_output"])
dag = registry.build_dag()
```

#### FAIL_FAST (default)

```python
engine = ExecutionEngine(broker, dag, error_mode=ErrorHandlingMode.FAIL_FAST)
# Stops immediately on first error; pipeline fails
```

#### CONTINUE_ON_ERROR

```python
engine = ExecutionEngine(broker, dag, error_mode=ErrorHandlingMode.CONTINUE_ON_ERROR)
# Marks failed task as FAILED but continues with downstream tasks that don't depend on it
```

#### SKIP_FAILED

```python
engine = ExecutionEngine(broker, dag, error_mode=ErrorHandlingMode.SKIP_FAILED)
# Failed tasks are skipped; downstream tasks receive default values (None) for failed inputs
```

#### DEAD_LETTER

```python
engine = ExecutionEngine(broker, dag, error_mode=ErrorHandlingMode.DEAD_LETTER)
# Failed tasks are queued for later retry via a dead-letter queue
```

---

### 4. Error Aggregation

```python
from taskiq_flow.errors import PipelineErrorAggregator

aggregator = PipelineErrorAggregator()

# During/after execution, errors are collected:
aggregator.add_error(task=failed_task, error=exc, context={...})

# Later, analyze:
print(f"Total errors: {len(aggregator.errors)}")
print(f"Failed tasks: {aggregator.failed_tasks}")
print(f"Skipped tasks: {aggregator.skipped_tasks}")

for err in aggregator.errors:
    print(f"  {err.task_name}: {type(err.error).__name__}: {err.error}")
```

Useful for generating error reports and alerting.

---

## Expected Output

Running `python examples/retry_demo.py`:

```
=== Demo 1: Retry Middleware ===

Executing flaky task with retry middleware...
(Task may fail 1-2 times before succeeding)

✅ Pipeline succeeded! Result: Success on attempt 2

Retry count stored in middleware: {'flaky_task': 1}


=== Demo 2: Error Handling Modes ===

--- Mode: FAIL_FAST ---
  Execution raised: RuntimeError: Task failed on attempt 3

--- Mode: CONTINUE_ON_ERROR ---
  Execution completed. Results: ['flaky_output']

--- Mode: SKIP_FAILED ---
  Execution completed. Results: ['flaky_output']

Note: ErrorHandlingMode.DEAD_LETTER would queue failures for later retry.


=== Demo 3: Error Aggregation ===

Total errors collected: 3
Failed tasks: ['task_a', 'task_b', 'task_c']

Error details:
  - task_a: RuntimeError: timeout
  - task_b: ValueError: invalid data
  - task_c: ConnectionError: network down

You can use PipelineErrorAggregator to analyze failures and affected branches.


=== All Retry & Error Handling Demos Complete ===
```

---

## Key Points

### When to Use Which Error Mode

| Mode | Best for | Behavior |
|------|----------|----------|
| `FAIL_FAST` | Critical pipelines where any failure invalidates the whole run | Immediate halt |
| `CONTINUE_ON_ERROR` | Best-effort analysis where partial results are valuable | Continue; mark failures |
| `SKIP_FAILED` | Data processing where missing inputs can be tolerated | Provide None defaults |
| `DEAD_LETTER` | Systems requiring manual intervention or re-play | Queue for later retry |

### Retry Strategies

- **Transient failures** (network timeouts, temporary resource exhaustion) → Use `PipelineRetryMiddleware`
- **Permanent failures** (invalid data, code bugs) → Use `FAIL_FAST` or `SKIP_FAILED` depending on tolerance
- **Mixed workloads** → Combine retry middleware (for transient) with error modes (for permanent)

### Monitoring Retries

Track retry counts in metrics or logs:

```python
for task_name, count in retry_mw.retry_counts.items():
    logger.info(f"Task {task_name} retried {count} times")
```

Integrate with Prometheus:

```python
from taskiq_flow.metrics import MetricsMiddleware
broker.add_middlewares(MetricsMiddleware())
```

---

## Learning Path

After this example:

1. **[Retry Guide]({{ '/en/guides/retry/' | relative_url }})** — Complete retry & error handling documentation
2. **[Execution Guide]({{ '/en/guides/execution/' | relative_url }})** — Execution engine internals
3. **[Monitoring Guide]({{ '/en/guides/tracking/' | relative_url }})** — Track failed tasks and retries in production

---

*This example shows all retry patterns. In production, tune retry parameters (max_retries, backoff) based on task characteristics and SLA requirements.*
