"""Tracking demo example."""

import asyncio
import logging

from taskiq import InMemoryBroker

from taskiq_flow import Pipeline
from taskiq_flow.middleware import PipelineMiddleware
from taskiq_flow.tracking import PipelineTrackingManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create broker (using InMemoryBroker for simplicity)
broker = InMemoryBroker(await_inplace=True)


@broker.task
async def slow_task(x: int) -> int:
    """Slow task that doubles the input."""
    await asyncio.sleep(1)
    print(f"Slow task called with {x}")  # noqa: T201
    return x * 2


async def main() -> None:
    """Run the tracking demo example."""
    # Setup tracking
    tracking_manager = PipelineTrackingManager().with_auto_storage(broker)

    # Create middleware with tracking manager
    middleware = PipelineMiddleware(tracking_manager=tracking_manager)
    broker_with_middleware = broker.with_middlewares(middleware)

    # Create pipeline with tracking
    pipeline = (
        Pipeline(broker_with_middleware)
        .with_tracking(manager=tracking_manager)
        .call_next(slow_task)  # type: ignore
        .call_next(slow_task)  # type: ignore
    )

    # Execute
    result = await pipeline.kiq(10)

    # Wait for completion
    await result.wait_result()

    # Check status
    pipeline_id = pipeline.pipeline_id
    if pipeline_id is None:
        raise RuntimeError("Pipeline has no ID")
    status = await tracking_manager.get_status(pipeline_id)
    if status is None:
        raise RuntimeError("Failed to get pipeline status")
    status_value = status.status
    steps_count = len(status.steps)

    logger.info(f"Pipeline status: {status_value}")
    logger.info(f"Steps completed: {steps_count}")


if __name__ == "__main__":
    asyncio.run(main())
