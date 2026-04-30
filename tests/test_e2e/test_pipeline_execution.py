# mypy: disable-error-code=no-untyped-def
"""End-to-end tests for complete pipeline execution."""

import pytest
from taskiq import InMemoryBroker

from taskiq_flow.hooks.events import (
    PipelineCompleteEvent,
    PipelineErrorEvent,
    PipelineStartEvent,
    StepCompleteEvent,
    StepErrorEvent,
    StepStartEvent,
)
from taskiq_flow.hooks.manager import HookManager
from taskiq_flow.tracking.manager import PipelineTrackingManager
from taskiq_flow.tracking.memory_storage import InMemoryPipelineStorage


@pytest.fixture
async def e2e_setup():
    """Set up end-to-end test components."""
    broker = InMemoryBroker()
    storage = InMemoryPipelineStorage()
    tracking = PipelineTrackingManager(storage)
    hooks = HookManager()

    # Set up hooks to update tracking
    async def track_pipeline_start(event):
        await tracking.initiate(event.pipeline_id, 2)
        await tracking.mark_pipeline_started(event.pipeline_id)

    async def track_step_start(event):
        await tracking.mark_step_started(
            event.pipeline_id,
            event.step_index,
            event.task_id,
            event.task_name,
        )

    async def track_step_complete(event):
        await tracking.mark_step_completed(event.pipeline_id, event.step_index)

    async def track_pipeline_complete(event):
        await tracking.mark_pipeline_completed(event.pipeline_id, event.result)

    async def track_step_error(event):
        await tracking.mark_step_failed(
            event.pipeline_id,
            event.step_index,
            event.error,
        )

    async def track_pipeline_error(event) -> None:
        await tracking.mark_pipeline_failed(event.pipeline_id, event.error)

    hooks.register("PipelineStartEvent", track_pipeline_start)
    hooks.register("StepStartEvent", track_step_start)
    hooks.register("StepCompleteEvent", track_step_complete)
    hooks.register("PipelineCompleteEvent", track_pipeline_complete)
    hooks.register("StepErrorEvent", track_step_error)
    hooks.register("PipelineErrorEvent", track_pipeline_error)

    return {
        "broker": broker,
        "tracking": tracking,
        "hooks": hooks,
        "storage": storage,
    }


@pytest.mark.anyio
async def test_e2e_pipeline_execution_simulation(e2e_setup):
    """Simulate end-to-end pipeline execution with tracking and hooks."""
    setup = e2e_setup
    tracking = setup["tracking"]
    hooks = setup["hooks"]

    pipeline_id = "e2e_pipeline"

    await hooks.dispatch(PipelineStartEvent(pipeline_id=pipeline_id))

    # Check pipeline initiated and started
    status = await tracking.get_status(pipeline_id)
    assert status is not None
    assert status.status.name == "RUNNING"
    assert status.total_steps == 2

    # Simulate step 1 start and complete
    await hooks.dispatch(
        StepStartEvent(
            pipeline_id=pipeline_id,
            step_index=0,
            task_id="task_a",
            task_name="Task A",
        ),
    )
    await hooks.dispatch(
        StepCompleteEvent(
            pipeline_id=pipeline_id,
            step_index=0,
            task_id="task_a",
            task_name="Task A",
            result="result_a",
        ),
    )

    # Check step 1 completed
    status = await tracking.get_status(pipeline_id)
    assert status.steps[0].status.name == "COMPLETED"
    assert status.steps[0].task_name == "Task A"

    # Simulate step 2 start and complete
    await hooks.dispatch(
        StepStartEvent(
            pipeline_id=pipeline_id,
            step_index=1,
            task_id="task_b",
            task_name="Task B",
        ),
    )
    await hooks.dispatch(
        StepCompleteEvent(
            pipeline_id=pipeline_id,
            step_index=1,
            task_id="task_b",
            task_name="Task B",
            result="result_b",
        ),
    )

    # Check step 2 completed
    status = await tracking.get_status(pipeline_id)
    assert status.steps[1].status.name == "COMPLETED"
    assert status.steps[1].task_name == "Task B"

    # Simulate pipeline complete
    final_result = {"step1": "result_a", "step2": "result_b"}
    await hooks.dispatch(
        PipelineCompleteEvent(pipeline_id=pipeline_id, result=final_result),
    )

    # Check pipeline completed
    status = await tracking.get_status(pipeline_id)
    assert status.status.name == "COMPLETED"
    assert status.result == final_result

    # Test listing pipelines
    pipelines = await tracking.list_recent()
    assert len(pipelines) >= 1
    assert any(p.pipeline_id == pipeline_id for p in pipelines)


@pytest.mark.anyio
async def test_e2e_pipeline_error_simulation(e2e_setup):
    """Simulate end-to-end pipeline execution with error."""
    setup = e2e_setup
    tracking = setup["tracking"]
    hooks = setup["hooks"]

    pipeline_id = "e2e_error_pipeline"

    await hooks.dispatch(PipelineStartEvent(pipeline_id=pipeline_id))

    # Start a step
    await hooks.dispatch(
        StepStartEvent(
            pipeline_id=pipeline_id,
            step_index=0,
            task_id="task_fail",
            task_name="Failing Task",
        ),
    )

    # Fail the step
    await hooks.dispatch(
        StepErrorEvent(
            pipeline_id=pipeline_id,
            step_index=0,
            task_id="task_fail",
            task_name="Failing Task",
            error="Task failed",
        ),
    )

    # Fail the pipeline
    await hooks.dispatch(
        PipelineErrorEvent(pipeline_id=pipeline_id, error="Pipeline failed"),
    )

    # Check pipeline failed
    status = await tracking.get_status(pipeline_id)
    assert status.status.name == "FAILED"
    assert status.error == "Pipeline failed"
    assert status.steps[0].status.name == "FAILED"
    assert status.steps[0].error == "Task failed"


@pytest.mark.anyio
async def test_e2e_cleanup(e2e_setup):
    """Test cleanup in e2e scenario."""
    import asyncio

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
