# mypy: disable-error-code=no-untyped-def
"""End-to-end tests for complete pipeline execution."""

import asyncio
from typing import Any

import pytest
from taskiq import InMemoryBroker

from taskiq_flow import (
    DataflowPipeline,
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
@pytest.mark.skip(reason="Test needs to be fixed")
async def test_e2e_basic_pipeline(e2e_setup):
    """Test basic pipeline execution in e2e scenario."""
    setup = e2e_setup
    broker = setup["broker"]
    tracking = setup["tracking"]

    @broker.task
    @pipeline_task(output="double")
    async def double(x: int) -> int:
        return x * 2

    @broker.task
    @pipeline_task(output="square")
    async def square(x: int) -> int:
        return x * x

    pipeline = DataflowPipeline(broker)
    pipeline.map(double, [1, 2, 3], "doubled")
    pipeline.map(square, [], "squared")

    task = await pipeline.kiq()
    result = await task.wait_result()

    assert result.success
    assert result.return_value["doubled"] == [2, 4, 6]

    status = await tracking.get_status(pipeline.pipeline_id)
    assert status is not None
    assert status.status.name == "COMPLETED"


@pytest.mark.anyio
@pytest.mark.skip(reason="Test needs to be fixed")
async def test_e2e_error_handling(e2e_setup):
    """Test error handling in e2e scenario."""
    setup = e2e_setup
    broker = setup["broker"]
    tracking = setup["tracking"]

    @broker.task
    @pipeline_task(output="result")
    async def failing_task(x: int) -> int:
        raise ValueError("Task failed")

    pipeline = DataflowPipeline(broker)
    pipeline.map(failing_task, [1], "failed")

    task = await pipeline.kiq()
    result = await task.wait_result()

    assert not result.success

    status = await tracking.get_status(pipeline.pipeline_id)
    assert status is not None
    assert status.steps[0].status.name == "FAILED"
    assert status.steps[0].error == "Task failed"


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
