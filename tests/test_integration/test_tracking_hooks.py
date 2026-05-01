# mypy: disable-error-code=no-untyped-def
"""Integration tests for pipeline tracking and hooks."""

import asyncio

import pytest
from taskiq import InMemoryBroker

from taskiq_flow.hooks.events import (
    PipelineCompleteEvent,
    PipelineEvent,
    PipelineStartEvent,
    StepCompleteEvent,
    StepStartEvent,
)
from taskiq_flow.hooks.manager import HookManager
from taskiq_flow.tracking.manager import PipelineTrackingManager
from taskiq_flow.tracking.memory_storage import InMemoryPipelineStorage


@pytest.fixture
async def setup_integration():
    """Set up integration components."""
    broker = InMemoryBroker()
    storage = InMemoryPipelineStorage()
    tracking_manager = PipelineTrackingManager(storage)
    hook_manager = HookManager()

    return {
        "broker": broker,
        "storage": storage,
        "tracking": tracking_manager,
        "hooks": hook_manager,
    }


@pytest.mark.anyio
async def test_hook_tracking_integration(setup_integration):
    """Test that hooks can update tracking."""
    components = setup_integration
    tracking = components["tracking"]
    hooks = components["hooks"]

    # Register hooks that update tracking
    async def on_pipeline_start(event: PipelineStartEvent):
        await tracking.initiate(event.pipeline_id, 2)
        await tracking.mark_pipeline_started(event.pipeline_id)

    async def on_step_start(event: StepStartEvent):
        await tracking.mark_step_started(
            event.pipeline_id,
            event.step_index,
            event.task_id,
            event.task_name,
        )

    async def on_step_complete(event: StepCompleteEvent):
        await tracking.mark_step_completed(event.pipeline_id, event.step_index)

    async def on_pipeline_complete(event: PipelineCompleteEvent):
        await tracking.mark_pipeline_completed(event.pipeline_id, event.result)

    # Register callbacks
    hooks.register("PipelineStartEvent", on_pipeline_start)
    hooks.register("StepStartEvent", on_step_start)
    hooks.register("StepCompleteEvent", on_step_complete)
    hooks.register("PipelineCompleteEvent", on_pipeline_complete)

    # Simulate events
    pipeline_id = "test_pipeline"

    # Start pipeline
    await hooks.dispatch(PipelineStartEvent(pipeline_id=pipeline_id))
    status = await tracking.get_status(pipeline_id)
    assert status is not None
    assert status.status.name == "RUNNING"

    # Start step
    await hooks.dispatch(
        StepStartEvent(
            pipeline_id=pipeline_id,
            step_index=0,
            task_id="task1",
            task_name="Test Task",
        ),
    )
    status = await tracking.get_status(pipeline_id)
    assert status.steps[0].status.name == "RUNNING"

    # Complete step
    await hooks.dispatch(
        StepCompleteEvent(
            pipeline_id=pipeline_id,
            step_index=0,
            task_id="task1",
            task_name="Test Task",
            result="step_result",
        ),
    )
    status = await tracking.get_status(pipeline_id)
    assert status.steps[0].status.name == "COMPLETED"

    # Complete pipeline
    await hooks.dispatch(
        PipelineCompleteEvent(pipeline_id=pipeline_id, result="final_result"),
    )
    status = await tracking.get_status(pipeline_id)
    assert status.status.name == "COMPLETED"
    assert status.result == "final_result"


@pytest.mark.anyio
async def test_hook_error_handling(setup_integration):
    """Test that hook errors don't break the system."""
    components = setup_integration
    hooks = components["hooks"]

    # Register a failing callback
    async def failing_callback(event: PipelineEvent):
        raise ValueError("Test error")

    # Register a working callback
    events_received = []

    async def working_callback(event: PipelineEvent):
        events_received.append(event.__class__.__name__)

    hooks.register("PipelineStartEvent", failing_callback)
    hooks.register("PipelineStartEvent", working_callback)

    # Dispatch event
    await hooks.dispatch(PipelineStartEvent(pipeline_id="test"))

    # Working callback should still execute
    assert "PipelineStartEvent" in events_received


@pytest.mark.anyio
async def test_hook_concurrent_dispatch(setup_integration):
    """Test concurrent hook dispatching."""
    components = setup_integration
    hooks = components["hooks"]

    results = []

    async def callback1(event: PipelineEvent):
        await asyncio.sleep(0.01)  # Simulate work
        results.append(f"cb1_{event.pipeline_id}")

    async def callback2(event: PipelineEvent):
        await asyncio.sleep(0.01)
        results.append(f"cb2_{event.pipeline_id}")

    hooks.register("PipelineStartEvent", callback1)
    hooks.register("PipelineStartEvent", callback2)

    # Dispatch multiple events
    await hooks.dispatch(PipelineStartEvent(pipeline_id="pipe1"))
    await hooks.dispatch(PipelineStartEvent(pipeline_id="pipe2"))

    # Both callbacks should execute for both events
    assert len(results) == 4
    assert "cb1_pipe1" in results
    assert "cb2_pipe1" in results
    assert "cb1_pipe2" in results
    assert "cb2_pipe2" in results
