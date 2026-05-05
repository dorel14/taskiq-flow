---
title: Pipeline Scheduling Guide
nav_order: 25
---
# Pipeline Scheduling Guide

**Cron-based, interval, and one-off pipeline scheduling with PipelineScheduler**

> **Version**: 0.4.0 | **Related**: [Execution Guide]({{ '/en/guides/execution/' | relative_url }}), [Tracking Guide]({{ '/en/guides/tracking/' | relative_url }})

---

## Overview

Taskiq-Flow includes a powerful scheduling system for running pipelines at specific times or intervals, built on top of APScheduler.

This guide covers:

- `PipelineScheduler` — Main scheduling interface
- Cron expressions and patterns
- Interval-based scheduling
- One-off executions
- Timezone handling
- Job persistence and management
- Missed execution handling

---

## 1. Quick Start

```python
from taskiq_flow import Pipeline, PipelineScheduler

# Create your pipeline
pipeline = Pipeline(broker).call_next(my_task).call_next(another_task)

# Create scheduler
scheduler = PipelineScheduler(broker)

# Schedule to run every minute
job_id = await scheduler.schedule(
    pipeline,
    cron="* * * * *",  # Every minute
    args=("some", "data")  # Arguments passed to pipeline.kiq()
)

# Start the scheduler (runs in background)
await scheduler.start()

# ... keep your application running ...
# scheduler runs in background tasks

# Shutdown gracefully
await scheduler.shutdown()
```

That's the basics. Let's explore the features in detail.

---

## 2. PipelineScheduler

The main class for scheduling pipeline executions.

### 2.1. Initialization

```python
from taskiq_flow import PipelineScheduler

scheduler = PipelineScheduler(
    broker,
    store="memory",  # "memory" or "sqlite"
    store_path="./scheduler_jobs.db"  # for sqlite store
)
```

**Storage options**:

| Store | Persistence | Multi-worker | Use case |
|-------|-------------|--------------|----------|
| `"memory"` | ❌ No | ❌ No | Development, single-process |
| `"sqlite"` | ✅ Yes | ⚠️ Limited* | Single-worker production, simple persistence |

*SQLite store works with single scheduler instance; multiple workers need external DB.

**Recommendation**:
- Development/mocks → `store="memory"`
- Single-worker production → `store="sqlite"`
- Distributed → Use external job store (PostgreSQL, Redis) — future enhancement

### 2.2. Starting & Stopping

```python
# Start scheduler (begins monitoring schedules)
await scheduler.start()

# Run in background while app is alive
# Typically integrated into FastAPI/Quart lifespan events

# Graceful shutdown
await scheduler.shutdown()
# Waits for running jobs to finish, cancels pending
```

**Automatic start with context manager**:

```python
async with PipelineScheduler(broker) as scheduler:
    await scheduler.schedule(pipeline, cron="*/5 * * * *")
    # Scheduler automatically starts on __aenter__
    # ... run your app ...
# Automatically shuts down on __aexit__
```

---

## 3. Scheduling Methods

### 3.1. Cron Scheduling

```python
job_id = await scheduler.schedule(
    pipeline,
    cron="0 * * * *",  # Every hour at minute 0
    args=("input_data",),
    kwargs={"key": "value"},
    pipeline_id="hourly_job_001"
)
```

**Cron expression format**: `minute hour day month day-of-week`

| Field | Allowed values | Special characters |
|-------|----------------|-------------------|
| Minute | 0-59 | `* , - /` |
| Hour | 0-23 | `* , - /` |
| Day | 1-31 | `* , - / ?` |
| Month | 1-12 | `* , - /` |
| Day of week | 0-6 (Sun-Sat) | `* , - / ?` |

**Examples**:

```python
"*/5 * * * *"          # Every 5 minutes
"0 9 * * *"            # Daily at 9:00 AM
"0 0 * * 0"            # Weekly on Sunday at midnight
"0 0 1 * *"            # Monthly on the 1st at midnight
"0 0 1 1 *"            # Yearly on January 1st at midnight
```

### 3.2. Interval Scheduling

```python
# Run every N seconds/minutes/hours/days/weeks
job_id = await scheduler.schedule_interval(
    pipeline,
    seconds=30,       # Every 30 seconds
    # minutes=5,     # Every 5 minutes
    # hours=1,       # Every hour
    args=(data,)
)
```

**Note**: Interval scheduling uses APScheduler's `IntervalTrigger`. Cron is generally preferred for production (more flexible, handles DST).

### 3.3. One-Off Execution (Run At)

Schedule a single future execution:

```python
from datetime import datetime, timedelta

job_id = await scheduler.schedule_at(
    pipeline,
    run_at=datetime.now() + timedelta(hours=2),  # In 2 hours
    args=(payload,)
)
```

Or schedule for a specific calendar time:

```python
run_time = datetime(2026, 12, 31, 23, 59, 59)
await scheduler.schedule_at(pipeline, run_at=run_time)
```

---

## 4. Job Configuration

### 4.1. Job ID

Each scheduled job gets a unique identifier:

```python
job_id = await scheduler.schedule(pipeline, cron="* * * * *")
print(job_id)  # e.g., "job_20260505_abcdef123456"
```

Customize the ID:

```python
job_id = await scheduler.schedule(
    pipeline,
    cron="0 9 * * *",
    job_id="daily_etl_9am"  # human-readable ID
)
```

Useful for later management (update, cancel, list).

### 4.2. Arguments & Keyword Arguments

Pass arguments to the pipeline's `kiq()` method:

```python
await scheduler.schedule(
    pipeline,
    cron="* * * * *",
    args=("positional_arg",),     # tuple
    kwargs={"option": True},      # dict
    pipeline_id="my_pipeline"     # explicit pipeline ID
)
```

The scheduler calls: `await pipeline.kiq(*args, **kwargs)` on each trigger.

### 4.3. Pipeline ID

Each scheduled execution can override the pipeline's default ID:

```python
pipeline = Pipeline(broker)  # generates random ID by default

# Schedule with explicit ID (ensures uniqueness for tracking)
await scheduler.schedule(
    pipeline,
    cron="*/5 * * * *",
    pipeline_id="my_pipeline_v1"
)
```

**Best practice**: Include timestamp or version in ID for tracking:

```python
job_id = f"batch_process_v2_{int(time.time())}"
```

---

## 5. Job Management

### 5.1. List Scheduled Jobs

```python
jobs = await scheduler.list_jobs()
for job in jobs:
    print(f"ID: {job.id}")
    print(f"  Trigger: {job.trigger}")
    print(f"  Next run: {job.next_run_time}")
    print(f"  Pipeline: {job.pipeline_id}")
```

### 5.2. Get Job Details

```python
job = await scheduler.get_job(job_id)
if job:
    print(f"Job {job.id} is scheduled for {job.next_run_time}")
```

### 5.3. Modify a Job

```python
# Reschedule an existing job
await scheduler.reschedule_job(
    job_id,
    cron="0 */2 * * *"  # Change to every 2 hours
)

# Update job arguments
await scheduler.modify_job(
    job_id,
    args=("new_arg",),
    kwargs={"updated": True}
)
```

### 5.4. Remove (Cancel) a Job

```python
await scheduler.remove_job(job_id)
# Future executions are cancelled; running job continues
```

### 5.5. Pause & Resume

```python
# Temporarily pause a job
await scheduler.pause_job(job_id)

# Resume later
await scheduler.resume_job(job_id)
```

---

## 6. Tracking Scheduled Executions

Each scheduled pipeline execution is automatically tracked if the pipeline has tracking enabled:

```python
tracking = PipelineTrackingManager().with_auto_storage(broker)
pipeline = Pipeline(broker).with_tracking(tracking)

scheduler = PipelineScheduler(broker)
await scheduler.schedule(pipeline, cron="*/5 * * * *")

# Later, query execution history
history = await tracking.get_history()
for run in history:
    print(f"Run {run.pipeline_id}: {run.status} at {run.started_at}")
```

**Distinguishing scheduled runs**: Use descriptive `pipeline_id` patterns:

```python
await scheduler.schedule(
    pipeline,
    cron="0 2 * * *",  # Daily 2AM
    pipeline_id=f"daily_etl_{datetime.now().strftime('%Y%m%d')}"
)
# Each day gets a unique pipeline ID for tracking
```

---

## 7. Missed Execution Handling

When a scheduled job's trigger time is missed (e.g., scheduler downtime, long-running job), APScheduler provides controls:

### 7.1. Coalesce

Combine multiple missed runs into a single execution:

```python
from apscheduler.triggers.cron import CronTrigger

trigger = CronTrigger(
    hour=9,
    minute=0,
    coalesce=True  # If scheduler was down at 9:00, run once at 9:05 instead of 5 times
)

job = await scheduler.schedule(pipeline, trigger=trigger)
```

### 7.2. Max Instances

Prevent overlapping runs of the same job:

```python
# Job won't start a new execution if previous instance is still running
trigger = CronTrigger(minute="*/5", max_instances=1)

job = await scheduler.schedule(pipeline, trigger=trigger)
# If a 9:00 run is still executing at 9:05, the 9:05 run is skipped
```

### 7.3. Misfire Grace Time

Allow a window after scheduled time during which execution is still valid:

```python
from apscheduler.triggers.cron import CronTrigger

# If scheduler restarts within 10 minutes of scheduled time, still run
trigger = CronTrigger(
    minute="*/5",
    misfire_grace_time=600  # 10 minutes in seconds
)

job = await scheduler.schedule(pipeline, trigger=trigger)
```

---

## 8. Timezone Handling

By default, APScheduler uses the local system timezone. For production, set explicit timezone:

```python
from apscheduler.triggers.cron import CronTrigger
import pytz

# Schedule for 9:00 AM in New York timezone
trigger = CronTrigger(
    hour=9,
    minute=0,
    timezone=pytz.timezone("America/New_York")
)

job = await scheduler.schedule(pipeline, trigger=trigger)
```

Or set globally on scheduler:

```python
scheduler = PipelineScheduler(
    broker,
    timezone="UTC"  # or "America/Los_Angeles", "Europe/Paris", ...
)
```

**Daylight Saving Time (DST)**: Cron triggers with explicit timezone handle DST transitions automatically. Jobs scheduled at "9:00" will still run at 9:00 local time when clocks shift.

---

## 9. Custom Triggers

Beyond cron and intervals, use any APScheduler trigger:

```python
from apscheduler.triggers.date import DateTrigger
from datetime import datetime, timedelta

# Run once at specific datetime
trigger = DateTrigger(run_date=datetime(2026, 12, 31, 23, 59, 59))
job = await scheduler.schedule(pipeline, trigger=trigger)

# Run after a delay (from now)
trigger = DateTrigger(run_date=datetime.now() + timedelta(minutes=10))
job = await scheduler.schedule(pipeline, trigger=trigger)
```

See APScheduler documentation for advanced triggers (calendar-based, etc.).

---

## 10. Error Handling

### 10.1. Catch Job Execution Errors

Wrap pipeline execution with error handling:

```python
@broker.task
async def my_pipeline_task(data):
    try:
        result = await process(data)
        return result
    except Exception as exc:
        # Log error, but let scheduler continue
        logger.error(f"Pipeline failed: {exc}")
        raise  # Scheduler records failure, continues with next schedule
```

### 10.2. Scheduler-Level Error Callbacks

```python
scheduler = PipelineScheduler(broker)

@scheduler.on_error
async def handle_scheduler_error(job_id, exception):
    logger.error(f"Job {job_id} failed with: {exception}")
    send_alert_email(job_id, exception)

await scheduler.start()
```

### 10.3. Dead Letter Queue (DLQ)

For jobs that repeatedly fail, route to DLQ:

```python
from taskiq_flow.middlewares.retry import RetryMiddleware

# Configure retry with backoff
broker.add_middlewares(
    RetryMiddleware(
        max_retries=3,
        delay=10,
        backoff=2
    )
)

# After max retries, task goes to DLQ (if broker supports it)
# RedisStreamBroker: dead_letter_stream
# KafkaBroker: dead_letter_topic
```

---

## 11. Monitoring Scheduled Jobs

### 11.1. Health Check

```python
async def scheduler_health():
    stats = scheduler.get_stats()
    return {
        "scheduled_jobs": len(scheduler.get_jobs()),
        "running_jobs": stats.active_jobs,
        "next_run": min(job.next_run_time for job in scheduler.get_jobs())
    }
```

### 11.2. Logging

Configure structured logging:

```python
import logging
logger = logging.getLogger("taskiq_flow.scheduler")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Scheduler logs:
# 2026-05-05 10:00:00 - taskiq_flow.scheduler - INFO - Running job daily_etl_9am
# 2026-05-05 10:00:05 - taskiq_flow.scheduler - INFO - Job daily_etl_9am completed successfully
```

### 11.3. Metrics

Integrate with Prometheus:

```python
from prometheus_client import Counter, Gauge

SCHEDULED_JOBS = Gauge('scheduled_jobs_total', 'Total scheduled jobs')
JOB_RUNS = Counter('scheduler_job_runs_total', 'Job executions', ['job_id'])
JOB_FAILURES = Counter('scheduler_job_failures_total', 'Job failures', ['job_id'])

class MetricsScheduler(PipelineScheduler):
    async def _run_job(self, job_id, pipeline):
        JOB_RUNS.labels(job_id=job_id).inc()
        try:
            await super()._run_job(job_id, pipeline)
        except Exception:
            JOB_FAILURES.labels(job_id=job_id).inc()
            raise
```

---

## 12. Production Considerations

### 12.1. High Availability

For production HA deployments, run multiple scheduler instances with a shared job store (PostgreSQL recommended):

```python
# Scheduler 1
scheduler1 = PipelineScheduler(
    broker,
    store="postgresql",
    db_url="postgresql://user:pass@host/db"
)

# Scheduler 2 (identical config) — only one will acquire jobs
scheduler2 = PipelineScheduler(...)
# APScheduler's job stores use row-level locking; one scheduler per job
```

**Note**: Currently only memory and sqlite stores are implemented; PostgreSQL/Redis support planned.

### 12.2. Long-Running Jobs

If a pipeline execution might exceed its schedule interval:

```python
# Ensure no overlap
trigger = CronTrigger(minute="*/5", max_instances=1, coalesce=True)
job = await scheduler.schedule(pipeline, trigger=trigger)

# Pipeline itself has timeout
pipeline.with_timeout(seconds=300)  # 5 minutes max
```

### 12.3. Start-up Behavior

On scheduler restart, missed jobs are handled according to `misfire_grace_time`:

```python
# Scheduler restarts at 9:05 AM, job was scheduled for 9:00
# With misfire_grace_time=600 (10 min): job runs at 9:05
# With misfire_grace_time=0: job is skipped
trigger = CronTrigger(hour=9, misfire_grace_time=600)
```

### 12.4. Job Store Size

The job store accumulates job history. Periodically clean up:

```python
# Remove jobs older than 30 days
old_jobs = await scheduler.list_jobs()
for job in old_jobs:
    if job.next_run_time < datetime.now() - timedelta(days=30):
        await scheduler.remove_job(job.id)
```

---

## 13. Common Patterns

### 13.1. Daily ETL Pipeline

```python
@scheduler.schedule(
    pipeline=etl_pipeline,
    cron="0 2 * * *",  # 2:00 AM daily
    pipeline_id="daily_etl"
)
async def run_daily_etl():
    pass
```

### 13.2. Periodic Health Check

```python
health_pipeline = Pipeline(broker).call_next(health_check_task)

await scheduler.schedule_interval(
    health_pipeline,
    minutes=5,
    pipeline_id="health_check_5m"
)
```

### 13.3. Dynamic Scheduling

Create and cancel jobs at runtime:

```python
# Schedule on-demand
job_id = await scheduler.schedule(
    pipeline,
    run_at=datetime.now() + timedelta(minutes=10)
)

# Cancel if no longer needed
await scheduler.remove_job(job_id)
```

### 13.4. Chained Pipelines

Pipeline A triggers Pipeline B via scheduling:

```python
@broker.task
async def pipeline_a_finished(result):
    # Schedule pipeline B to run after A completes
    job_id = await scheduler.schedule_at(
        pipeline_b,
        run_at=datetime.now() + timedelta(minutes=5)
    )
    return job_id
```

---

## 14. Troubleshooting

### Jobs Not Running

**Symptom**: Scheduled jobs never execute.

**Fixes**:
- Ensure `await scheduler.start()` is called
- Check cron expression validity: `CronTrigger.from_crontab("* * * * *")`
- Verify timezone matches expected time (check server TZ)
- Confirm job was successfully scheduled (non-None job_id)
- Check scheduler logs for errors

### Duplicate Job Execution

**Symptom**: Same job runs multiple times concurrently.

**Fixes**:
- Set `max_instances=1` in trigger
- Use `coalesce=True` to combine missed runs
- Ensure only one scheduler instance is running (HA needs shared store)

### Job Store Persistence Not Working

**Symptom**: Jobs disappear after restart despite sqlite store.

**Fixes**:
- Use `store="sqlite"` and specify `store_path`
- Ensure file path is writable and persistent between restarts
- Don't mix memory and sqlite stores in same app

### Timezone Issues

**Symptom**: Job runs at wrong time (off by hours).

**Fixes**:
- Set explicit timezone on scheduler: `PipelineScheduler(broker, timezone="UTC")`
- Or on trigger: `CronTrigger(hour=9, timezone=pytz.timezone("America/New_York"))`
- Verify server's system timezone matches expectations

---

## 15. Summary

PipelineScheduler provides robust, production-ready scheduling:

| Feature | API |
|---------|-----|
| **Cron** | `scheduler.schedule(pipeline, cron="* * * * *")` |
| **Interval** | `scheduler.schedule_interval(pipeline, minutes=5)` |
| **One-off** | `scheduler.schedule_at(pipeline, run_at=datetime)` |
| **Management** | `list_jobs()`, `remove_job()`, `pause_job()` |
| **Persistence** | SQLite (single-worker) |
| **Tracking** | Automatic with `PipelineTrackingManager` |
| **Concurrency** | `max_instances`, `coalesce` controls |

**Typical production setup**:

```python
tracking = PipelineTrackingManager().with_storage(RedisPipelineStorage(redis))
pipeline = Pipeline(broker).with_tracking(tracking)

scheduler = PipelineScheduler(broker, store="sqlite", store_path="./jobs.db")
await scheduler.start()

# Schedule your jobs...
```

---

## Next Steps

- **[Retry Guide]({{ '/en/guides/retry/' | relative_url }})** — Error recovery and retry policies
- **[Performance Guide]({{ '/en/guides/performance/' | relative_url }})** — Optimize scheduled pipeline performance
- **[Tracking Guide]({{ '/en/guides/tracking/' | relative_url }})** — Monitor scheduled job history

---

*Schedule pipelines like cron jobs. Track them like never before.*
