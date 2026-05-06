---
title: Retry & Error Handling Guide
nav_order: 26
---
# Retry & Error Handling Guide

**Resilient pipeline execution with retry policies, backoff, and dead-letter queues**

> **Version**: 0.4.0 | **Related**: [Execution Guide]({{ '/en/guides/execution/' | relative_url }}), [Scheduling Guide]({{ '/en/guides/scheduling/' | relative_url }})

---

## Overview

Failures are inevitable in distributed systems. Taskiq-Flow provides comprehensive retry and error handling mechanisms to ensure pipeline robustness.

This guide covers:

- Retry policies at task and pipeline levels
- Exponential backoff strategies
- Dead-letter queues (DLQ) for unrecoverable failures
- Conditional retry logic
- Timeout configuration
- Monitoring retry metrics

---

## 1. Understanding Retries

A **retry** is automatically re-executing a failed task with the same inputs. Retry policies define **when** and **how** to retry.

### When to Retry

✅ **Good candidates for retry**:

- Network timeouts (external API unavailable)
- Database connection errors (transient)
- Rate limit hits (retry-after header)
- Temporary resource exhaustion

❌ **Do NOT retry**:

- Validation errors (bad input won't fix itself)
- Programming errors (bug in code)
- Missing data (won't reappear)
- Permanent failures (404 Not Found, 401 Unauthorized)

---

## 2. Retry at Task Level

Configure retry directly on the task decorator:

```python
@broker.task(
    max_retries=3,  # Maximum retry attempts (default: 0 = no retry)
    retry_delay=5.0,  # Seconds between retries
    retry_backoff=2.0,  # Multiply delay by this after each attempt
    retry_timeout=60  # Overall timeout including retries
)
async def flaky_api_call():
    response = await call_external_api()
    return response.json()
```

**Retry sequence**:

| Attempt | Delay | Cumulative |
|---------|-------|------------|
| 1 (initial) | 0s | 0s |
| 2 (retry 1) | 5s | 5s |
| 3 (retry 2) | 10s (5 × 2) | 15s |
| 4 (retry 3) | 20s (10 × 2) | 35s |
| Final failure | — | 35s |

---

## 3. Retry at Pipeline Level

Apply consistent retry policy to all tasks in a pipeline:

```python
pipeline = Pipeline(broker)
pipeline.with_retry(
    max_attempts=3,
    delay=2.0,         # Initial delay
    backoff=1.5,       # Backoff multiplier
    on_retry=None      # Optional callback
)
```

All tasks in this pipeline inherit this policy unless they have their own.

**Inheritance precedence**: Task-level overrides pipeline-level.

---

## 4. Custom Retry Policies

For fine control, implement `RetryPolicy`:

```python
from taskiq_flow import RetryPolicy

class MyRetryPolicy(RetryPolicy):
    def should_retry(self, attempt: int, exception: Exception) -> bool:
        # Retry only on network errors, max 5 attempts
        if attempt >= 5:
            return False
        return isinstance(exception, NetworkError)

    def get_delay(self, attempt: int) -> float:
        # Custom backoff: 2^attempt + random jitter
        import random
        base = 2 ** attempt
        jitter = random.uniform(-0.1, 0.1) * base
        return max(0.5, base + jitter)

pipeline.with_retry(policy=MyRetryPolicy())
```

### 4.1. Conditional Retry (Only on Specific Exceptions)

```python
@broker.task
async def task_with_selective_retry():
    try:
        result = await call_api()
        return result
    except NetworkTimeout:
        # This exception type should be retried
        raise RetryException("Timeout, will retry")
    except InvalidResponse:
        # This error is permanent; don't retry
        raise  # Will fail immediately
```

**Built-in exception-based retry**:

```python
from taskiq.exceptions import RetryException

@broker.task(retry_on=[NetworkError, TimeoutError])
async def task():
    # Automatically retries on these exception types
    pass
```

---

## 5. Exponential Backoff with Jitter

Avoid thundering herd problem (all retries happen at same time):

```python
import random

def exponential_backoff_with_jitter(
    attempt: int,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    backoff_factor: float = 2.0,
    jitter: bool = True
) -> float:
    """Calculate retry delay."""
    delay = min(max_delay, base_delay * (backoff_factor ** attempt))
    if jitter:
        # Add ±10% random jitter
        delay *= random.uniform(0.9, 1.1)
    return delay

# Usage in policy
class JitteredRetryPolicy(RetryPolicy):
    def get_delay(self, attempt: int) -> float:
        return exponential_backoff_with_jitter(attempt, base_delay=2.0)
```

**Why jitter?** Prevents synchronized retry storms that overwhelm services.

---

## 6. Dead Letter Queues (DLQ)

When all retries are exhausted, failed tasks need somewhere to go.

### 6.1. Configuring DLQ

```python
from taskiq_flow.middlewares.retry import RetryMiddleware

broker.add_middlewares(
    RetryMiddleware(
        max_retries=3,
        dlq_queue="failed_tasks"  # Tasks go here after exhausting retries
    )
)
```

**Behavior**:

1. Task fails → retry 1 (after delay)
2. Fails again → retry 2 (after longer delay)
3. Fails again → retry 3
4. Fails all retries → move to `failed_tasks` queue

### 6.2. DLQ Inspection & Reprocessing

```python
from taskiq_flow.middlewares.retry import DLQManager

dlq = DLQManager(broker)

# List failed tasks
failed_tasks = await dlq.list_failed()
for task_info in failed_tasks:
    print(f"Task {task_info.task_id} failed: {task_info.error}")

# Replay a failed task (re-queue for execution)
await dlq.retry_task(task_id)

# Discard a failed task permanently
await dlq.delete_task(task_id)

# Bulk delete older than N days
await dlq.cleanup_older_than(days=7)
```

### 6.3. DLQ Alerting

Set up alerts when tasks land in DLQ:

```python
class DLQAlertListener:
    async def on_task_to_dlq(self, task_id: str, error: str):
        send_slack_alert(f"Task {task_id} failed after retries: {error}")
        create_incident_ticket(task_id, error)

dlq_manager = DLQManager(broker).with_listener(DLQAlertListener())
```

---

## 7. Timeouts

Prevent tasks from running indefinitely.

### 7.1. Task-Level Timeout

```python
@broker.task(timeout=30)  # seconds
async def potentially_slow_task():
    await long_running_operation()
```

If the task exceeds 30 seconds, `asyncio.TimeoutError` is raised and retry policy applies.

### 7.2. Pipeline-Level Timeout

```python
pipeline = Pipeline(broker)
pipeline.with_timeout(seconds=300)  # 5 minutes for entire pipeline
```

Cancels all running steps when timeout expires.

### 7.3. Step-Level Timeout (Advanced)

```python
from taskiq_flow.steps import TimeoutStep

pipeline = Pipeline(broker)
pipeline.call_next(TimeoutStep(my_task, timeout=10.0))
```

---

## 8. Error Propagation

### 8.1. Fail Fast (Default)

Pipeline stops at first failure:

```python
pipeline = Pipeline(broker)
# By default: on_error="stop"

pipeline.call_next(task1)  # Fails → pipeline stops, task2 never runs
pipeline.call_next(task2)
```

### 8.2. Continue on Error

Continue executing remaining steps despite failures:

```python
pipeline = Pipeline(broker)
pipeline.on_error("continue")

pipeline.call_next(task1)  # Fails, but task2 still runs
pipeline.call_next(task2)
```

**Result**: Task2 receives `None` or partial result; check `result.is_failed`.

### 8.3. Compensation (Saga Pattern)

Execute a cleanup task if a step fails:

```python
pipeline = Pipeline(broker)

pipeline.call_next(allocate_resource)
    .on_failure(compensate_allocation)  # Run compensation if previous step failed
pipeline.call_next(process)
```

---

## 9. Monitoring Retries

Track retry metrics:

```python
from taskiq_flow import PipelineTrackingManager

tracking = PipelineTrackingManager().with_auto_storage(broker)

# Retry metrics exposed in PipelineStatus:
status = await tracking.get_status(pipeline_id)
print(f"Steps: {len(status.steps)}")
for step in status.steps:
    if step.retry_count > 0:
        print(f"  {step.name}: retried {step.retry_count} times")
        print(f"    Errors: {step.errors}")
```

**Metrics to monitor**:

- **Retry rate** (%) of tasks needing retry
- **Average retry count** per task
- **Top failing tasks** (most retries)
- **DLQ size** (tasks giving up)
- **Time spent in retries** vs actual work

### Integration with Prometheus

```python
from prometheus_client import Counter, Summary

RETRY_COUNT = Counter('task_retries_total', 'Total retry attempts', ['task_name'])
TASK_FAILURES = Counter('task_failures_total', 'Tasks that failed after retries', ['task_name'])
TASK_DURATION = Summary('task_duration_seconds', 'Task execution time', ['task_name'])

class MetricsMiddleware(PipelineMiddleware):
    async def on_step_complete(self, ctx, result):
        step_name = ctx.task_name
        RETRY_COUNT.labels(step_name).inc(ctx.retry_count)
        TASK_DURATION.labels(step_name).observe(ctx.duration_ms / 1000)
```

---

## 10. Best Practices

### 10.1. Set Reasonable Retry Limits

```python
# Don't retry indefinitely
@broker.task(max_retries=3)  # Good: bounded
@broker.task(max_retries=None)  # Bad: infinite retries
```

### 10.2. Use Exponential Backoff

Implemented via `retry_backoff`:

```python
@broker.task(max_retries=5, retry_delay=2.0, retry_backoff=2.0)
# Delays: 2s, 4s, 8s, 16s, 32s
```

### 10.3. Add Jitter

Randomize delays to avoid thundering herd:

```python
retry_backoff=2.0, retry_jitter=True  # Add ±10% jitter
```

### 10.4. Set Deadlines

```python
# Overall timeout including retries
@broker.task(retry_timeout=300)  # Give up after 5 minutes total
```

### 10.5. Log Every Retry

```python
import logging
logger = logging.getLogger(__name__)

@broker.task(
    max_retries=3,
    on_retry=lambda attempt, exc: logger.warning(f"Retry {attempt} for task: {exc}")
)
```

### 10.6. Separate Transient vs Permanent Errors

```python
@broker.task
async def smart_task():
    try:
        return await call_api()
    except (Timeout, ConnectionError) as e:
        raise RetryException("Transient error") from e  # Will retry
    except NotFoundError:
        raise  # No retry, fail permanently
```

### 10.7. DLQ for Investigation

Never discard failed tasks without review:

```python
dlq = DLQManager(broker)
# Periodically review DLQ
failed = await dlq.list_failed(limit=100)
for task in failed:
    logger.error(f"DLQ task {task.task_id}: {task.error}")
    # Consider manual replay or data correction
```

---

## 11. Common Pitfalls

| Pitfall | Consequence | Solution |
|---------|-------------|----------|
| Infinite retries (`max_retries=None`) | System stuck in retry loop | Set explicit max |
| No backoff (delay=0) | Service overwhelmed | Use exponential backoff |
| Retrying validation errors | Wasted resources | Distinguish error types |
| No DLQ | Lost failed tasks | Configure DLQ |
| Timeout shorter than retry delay | Premature timeout | Ensure timeout > sum of retry delays |
| Multiple retries on non-idempotent tasks | Duplicate side-effects | Make tasks idempotent or limit retries |

---

## 12. Summary

| Feature | Task-level | Pipeline-level |
|---------|-----------|----------------|
| **Retry limit** | `@broker.task(max_retries=N)` | `pipeline.with_retry(max_attempts=N)` |
| **Delay** | `retry_delay` | `delay` |
| **Backoff** | `retry_backoff` | `backoff` |
| **Timeout** | `timeout` per task | `with_timeout(seconds)` overall |
| **DLQ** | Via `RetryMiddleware` | Inherited from tasks |

**Complete resilient pipeline**:

```python
tracking = PipelineTrackingManager().with_auto_storage(broker)

pipeline = Pipeline(broker).with_tracking(tracking)
pipeline.with_retry(max_attempts=3, delay=2.0, backoff=2.0)
pipeline.with_timeout(seconds=300)
pipeline.on_error("continue")  # Or use compensation steps

# Add retry middleware with DLQ
from taskiq_flow.middlewares.retry import RetryMiddleware
broker.add_middlewares(RetryMiddleware(max_retries=3, dlq_queue="failed_tasks"))
```

---

## Next Steps

- **[Performance Guide]({{ '/en/guides/performance/' | relative_url }})** — Optimize task execution and resource usage
- **[Scheduling Guide]({{ '/en/guides/scheduling/' | relative_url }})** — Automated pipeline retries at scheduled intervals
- **[Tracking Guide]({{ '/en/guides/tracking/' | relative_url }})** — Monitor retry metrics in production

---

*Failures happen. Retry smart. Track everything.*
