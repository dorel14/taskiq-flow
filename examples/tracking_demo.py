"""Tracking demo example."""

import asyncio

from taskiq import AsyncBroker, InMemoryBroker
from taskiq_redis import RedisStreamBroker

from taskiq_flow import Pipeline
from taskiq_flow.tracking import PipelineTrackingManager

# Create broker
try:
    broker: AsyncBroker = RedisStreamBroker("redis://localhost:6379")
except Exception as e:
    print(f"Error creating RedisStreamBroker: {e}")  # noqa: T201
    broker = InMemoryBroker()


async def slow_task(x: int) -> int:
    """Slow task that doubles the input."""
    await asyncio.sleep(1)
    return x * 2


async def main() -> None:
    """Run the tracking demo example."""
    # Setup tracking
    tracking_manager = PipelineTrackingManager().with_auto_storage(broker)

    # Create pipeline with tracking
    pipeline = (
        Pipeline(broker)
        .with_tracking(manager=tracking_manager)
        .call_next(slow_task)  # type: ignore
        .call_next(slow_task)  # type: ignore
    )

    # Execute
    result = await pipeline.kiq(10)

    # Check status
    status = await tracking_manager.get_status(pipeline.pipeline_id)
    if status is None:
        raise RuntimeError("Failed to get pipeline status")
    status_value = status.status
    steps_count = len(status.steps)

    await result.wait_result()
    _ = (status_value, steps_count)
