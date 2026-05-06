---
permalink: /en/examples/scheduled-pipeline/
title: Example: scheduled_pipeline.py
nav_order: 44
color_scheme: dark
---
# Example: scheduled_pipeline.py

**Scheduling pipelines with cron and interval triggers**

> **Version**: 0.4.5 | **File**: `examples/scheduled_pipeline.py`

---

## Overview

This example demonstrates how to schedule pipelines to run periodically using `LabelBasedScheduler`. It covers:

- Cron-based scheduling (with second precision)
- Interval-based scheduling
- Listing and inspecting scheduled jobs

**Note**: This example uses `LabelBasedScheduler`, which is TaskIQ's label-based scheduling mechanism. For production cron scheduling, consider `PipelineScheduler` with APScheduler integration.

---

## What This Example Shows

- Creating a simple pipeline
- Using `LabelBasedScheduler` to schedule pipeline runs
- Cron expressions with second-level precision
- Interval-based scheduling
- Listing active schedules

---

## Code Walkthrough

```python
import asyncio
from taskiq import InMemoryBroker
from taskiq_flow import Pipeline, PipelineMiddleware
from taskiq_flow.scheduling import LabelBasedScheduler

# Create broker
broker = InMemoryBroker(await_inplace=True).with_middlewares(PipelineMiddleware())

# Define a simple task
@broker.task
async def log_message(msg: str) -> str:
    """Log a message."""
    return f"Processed: {msg}"

async def main():
    # Create pipeline
    pipeline = Pipeline(broker).call_next(log_message)

    # Create scheduler
    scheduler = LabelBasedScheduler(broker)

    # Schedule with cron expression (every 5 seconds)
    schedule_id = await scheduler.schedule_with_cron(
        pipeline=pipeline,
        label="every-5-seconds",
        cron="*/5 * * * * *",  # 6-field cron for second precision
        args=("Hello from scheduled pipeline!",),
    )
    print(f"Scheduled with cron: {schedule_id}")

    # Schedule with interval (every 3 seconds)
    interval_id = await scheduler.schedule_with_interval(
        pipeline=pipeline,
        label="every-3-seconds",
        interval_seconds=3,
        args=("Interval scheduled run!",),
    )
    print(f"Scheduled with interval: {interval_id}")

    # Wait for some executions to complete
    print("Waiting for pipeline executions (12 seconds)...")
    await asyncio.sleep(12)

    # List scheduled jobs
    schedules = scheduler.list_schedules()
    print(f"Active schedules: {len(schedules)}")
    for sched in schedules:
        print(f"  - {sched['label']}: cron={sched.get('cron')}, enabled={sched['enabled']}")

asyncio.run(main())
```

---

## Scheduling Methods

### Cron Scheduling

```python
schedule_id = await scheduler.schedule_with_cron(
    pipeline=pipeline,
    label="my-schedule",
    cron="*/5 * * * * *",  # Every 5 seconds (6-field cron)
    args=("message",),
)
```

**6-field cron format**: `second minute hour day month day-of-week`

Examples:
- `*/5 * * * * *` — Every 5 seconds
- `0 * * * * *` — Every minute at second 0
- `0 0 * * * *` — Every hour at minute 0, second 0

### Interval Scheduling

```python
interval_id = await scheduler.schedule_with_interval(
    pipeline=pipeline,
    label="interval-3s",
    interval_seconds=3,
    args=("message",),
)
```

Runs every N seconds, regardless of system time.

---

## Expected Output

```
Scheduled with cron: schedule_123456
Scheduled with interval: interval_789012
Waiting for pipeline executions (12 seconds)...
INFO:root:Processed: Hello from scheduled pipeline!
INFO:root:Processed: Interval scheduled run!
INFO:root:Processed: Hello from scheduled pipeline!
INFO:root:Processed: Interval scheduled run!
...
Active schedules: 2
  - every-5-seconds: cron=*/5 * * * * *, enabled=True
  - every-3-seconds: cron=None, enabled=True
```

You should see the log message printed multiple times as schedules trigger.

---

## Key Points

### Label-Based Scheduling

- Each schedule requires a unique `label` (used for identification)
- Labels can be used to enable/disable schedules dynamically
- The scheduler manages schedule persistence based on your broker

### InMemoryBroker Limitation

With `InMemoryBroker`, schedules only work while the process is running; they are lost on restart. For persistent scheduling, use Redis-based brokers with proper schedule stores.

### Multiple Schedules

You can schedule the same pipeline multiple times with different labels, cron expressions, or arguments.

---

## Variations

### Custom Scheduling with PipelineScheduler

For more advanced scheduling (timezones, misfire handling), use `PipelineScheduler`:

```python
from taskiq_flow import PipelineScheduler

scheduler = PipelineScheduler(broker)
job_id = await scheduler.schedule(
    pipeline,
    cron="0 9 * * *",  # Daily at 9 AM
    args=("daily",)
)
await scheduler.start()
```

See the [Scheduling Guide]({{ '/en/guides/scheduling/' | relative_url }}) for full details on `PipelineScheduler`.

---

## Learning Path

After this example:

1. **[Scheduling Guide]({{ '/en/guides/scheduling/' | relative_url }})** — Comprehensive cron and interval scheduling
2. **[PipelineScheduler]({{ '/en/api/core.md#pipelinescheduler' | relative_url }})** — API reference
3. **[Retry Guide]({{ '/en/guides/retry/' | relative_url }})** — Handling failures in scheduled pipelines

---

*This example shows label-based scheduling basics. For production use, explore PipelineScheduler with external job stores (PostgreSQL/Redis).*
