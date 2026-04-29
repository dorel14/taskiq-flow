"""Tracking demo example."""

from taskiq_redis import RedisBroker

from taskiq_pipelines import Pipeline
from taskiq_pipelines.tracking import PipelineTrackingManager


@taskiq.task
async def slow_task(x: int) -> int:
    import asyncio
    await asyncio.sleep(1)
    return x * 2


async def main():
    broker = RedisBroker("redis://localhost:6379")

    # Setup tracking
    tracking_manager = PipelineTrackingManager().with_auto_storage(broker)

    # Create pipeline with tracking
    pipeline = (
        Pipeline(broker)
        .with_tracking(manager=tracking_manager)
        .call_next(slow_task)
        .call_next(slow_task)
    )

    # Execute
    result = await pipeline.kiq(10)

    # Check status
    status = await tracking_manager.get_status(pipeline.pipeline_id)
    print(f"Pipeline status: {status.status}")
    print(f"Steps: {len(status.steps)}")

    await result.wait_result()
    print("Done!")


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
