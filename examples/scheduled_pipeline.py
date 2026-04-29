"""Scheduled pipeline example."""

from taskiq import TaskiqTask
from taskiq_redis import RedisBroker

from taskiq_pipelines import Pipeline
from taskiq_pipelines.scheduling import PipelineScheduler


@taskiq.task
async def log_message(msg: str) -> str:
    print(f"Scheduled task: {msg}")
    return f"Processed: {msg}"


async def main():
    broker = RedisBroker("redis://localhost:6379")

    # Create scheduler
    scheduler = PipelineScheduler(broker)

    # Create pipeline
    pipeline = Pipeline(broker).call_next(log_message)

    # Schedule to run every minute
    job_id = await scheduler.schedule(pipeline, cron="* * * * *", args=("Hello World",))

    print(f"Scheduled job: {job_id}")

    # Start scheduler
    await scheduler.start()

    # Keep running
    import asyncio
    await asyncio.sleep(300)  # Run for 5 minutes

    await scheduler.shutdown()


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())