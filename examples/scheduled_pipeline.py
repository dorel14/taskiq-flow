"""Scheduled pipeline example.

This example demonstrates how to schedule a pipeline to run periodically
using the LabelBasedScheduler with TaskIQ's label-based scheduling mechanism.

For this to work, you need a broker with a LabelScheduleSource.
See TaskIQ documentation for setting up label-based scheduling.
"""

import asyncio
from datetime import datetime, timedelta

from taskiq import InMemoryBroker

from taskiq_flow import Pipeline
from taskiq_flow.middleware import PipelineMiddleware
from taskiq_flow.scheduling import LabelBasedScheduler

# Create broker (using InMemoryBroker for simplicity)
broker = InMemoryBroker(await_inplace=True).with_middlewares(PipelineMiddleware())


@broker.task
async def log_message(msg: str) -> str:
    """Log a message."""
    return f"Processed: {msg}"


async def main() -> None:
    """Run the scheduled pipeline example."""
    # Create pipeline
    pipeline = Pipeline(broker).call_next(log_message)  # type: ignore

    # Create scheduler
    scheduler = LabelBasedScheduler(broker)

    # Register pipeline and schedule with cron expression
    # Runs every 5 seconds
    schedule_id = await scheduler.schedule_with_cron(
        pipeline=pipeline,
        label="every-5-seconds",
        cron="*/5 * * * * *",  # Every 5 seconds (6-field cron for second precision)
        args=("Hello from scheduled pipeline!",),
    )
    print(f"Scheduled with cron: {schedule_id}")  # noqa: T201

    # Schedule with interval (using LabelBasedScheduler's interval_seconds)
    interval_id = await scheduler.schedule_with_interval(
        pipeline=pipeline,
        label="every-3-seconds",
        interval_seconds=3,
        args=("Interval scheduled run!",),
    )
    print(f"Scheduled with interval: {interval_id}")  # noqa: T201

    # Wait for some executions to complete
    print("Waiting for pipeline executions (12 seconds)...")  # noqa: T201
    await asyncio.sleep(12)

    # List scheduled jobs
    schedules = scheduler.list_schedules()
    print(f"Active schedules: {len(schedules)}")  # noqa: T201
    for sched in schedules:
        print(
            f"  - {sched['label']}: cron={sched.get('cron')}, enabled={sched['enabled']}"
        )  # noqa: T201


if __name__ == "__main__":
    asyncio.run(main())
