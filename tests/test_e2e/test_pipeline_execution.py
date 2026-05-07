# mypy: disable-error-code=no-untyped-def
"""End-to-end tests for complete pipeline execution."""

import asyncio
from typing import Any

import pytest
from taskiq import InMemoryBroker

from taskiq_flow import (
    Pipeline,
    PipelineMiddleware,
    PipelineTrackingManager,
    pipeline_task,
)
from taskiq_flow.tracking.memory_storage import InMemoryPipelineStorage


@pytest.fixture
async def e2e_setup() -> dict[str, Any]:
    """Set up end-to-end test environment."""
    broker = InMemoryBroker()
    broker.add_middlewares(PipelineMiddleware())
    storage = InMemoryPipelineStorage()
    tracking = PipelineTrackingManager().with_storage(storage)

    return {
        "broker": broker,
        "tracking": tracking,
        "storage": storage,
    }


@pytest.mark.anyio
async def test_e2e_basic_pipeline(e2e_setup):
    """Test basic pipeline execution in e2e scenario."""
    setup = e2e_setup
    broker = setup["broker"]
    tracking = setup["tracking"]

    @broker.task
    @pipeline_task(output="doubled")
    async def double(x: int) -> int:
        return x * 2

    @broker.task
    @pipeline_task(output="squared", inputs=["doubled"])
    async def square(doubled: int) -> int:
        return doubled**2

    # Build pipeline using the base Pipeline class with tracking
    pipeline: Pipeline[Any, Any] = (
        Pipeline(broker)
        .with_tracking(manager=tracking)
        .call_next(double)
        .call_next(square)
    )

    # Execute the pipeline
    task = await pipeline.kiq(x=1)
    result = await task.wait_result()

    assert result.error is None
    assert result.return_value == 4  # (1 * 2)^2 = 4


@pytest.mark.anyio
async def test_e2e_error_handling(e2e_setup):
    """Test error handling in e2e scenario."""
    setup = e2e_setup
    broker = setup["broker"]
    tracking = setup["tracking"]

    @broker.task
    @pipeline_task(output="result")
    async def failing_task(x: int) -> int:
        raise ValueError("Task failed")

    # Build pipeline with failing task
    pipeline: Pipeline[Any, Any] = Pipeline(broker).with_tracking(
                                    manager=tracking).call_next(
                                        failing_task)

    # Execute and expect error
    task = await pipeline.kiq(x=1)
    result = await task.wait_result()

    assert result.error is not None


@pytest.mark.anyio
async def test_e2e_cleanup(e2e_setup):
    """Test cleanup in e2e scenario."""
    setup = e2e_setup
    tracking = setup["tracking"]
    storage = setup["storage"]

    # Create and complete a pipeline
    pipeline_id = "cleanup_test"
    await storage.create_pipeline(pipeline_id, 1)
    await storage.start_pipeline(pipeline_id)
    await storage.complete_pipeline(pipeline_id, "done")

    # Wait a bit to ensure the pipeline is older than TTL
    await asyncio.sleep(0.1)

    # Cleanup old data with very short TTL
    cleaned = await tracking.cleanup(
        ttl_seconds=0,
    )  # TTL of 0 means cleanup everything finished
    assert cleaned >= 1

    # Pipeline should be gone
    status = await tracking.get_status(pipeline_id)
    assert status is None
