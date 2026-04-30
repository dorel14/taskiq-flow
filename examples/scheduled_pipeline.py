"""Scheduled pipeline example."""

import asyncio

from taskiq import AsyncBroker, InMemoryBroker
from taskiq_redis import RedisStreamBroker

from taskiq_flow import Pipeline
from taskiq_flow.scheduling import PipelineScheduler

# Create broker
try:
    broker: AsyncBroker = RedisStreamBroker("redis://localhost:6379")
except Exception as e:
    print(f"Error creating RedisStreamBroker: {e}")  # noqa: T201
    broker = InMemoryBroker()


async def log_message(msg: str) -> str:
    """Log a message."""
    return f"Processed: {msg}"


async def main() -> None:
    """Run the scheduled pipeline example."""
    # Create scheduler
    scheduler = PipelineScheduler(broker)

    # Create pipeline
    pipeline = Pipeline(broker).call_next(log_message)  # type: ignore

    # Schedule to run every minute
    job_id = await scheduler.schedule(pipeline, cron="* * * * *", args=("Hello World",))

    _ = job_id

    # Start scheduler
    await scheduler.start()

    # Keep running
    await asyncio.sleep(300)  # Run for 5 minutes

    await scheduler.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
