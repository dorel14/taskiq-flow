# mypy: disable-error-code=no-untyped-def
"""Tests for pipeline storage implementations."""

from datetime import datetime, timedelta

import pytest

from taskiq_pipelines.tracking.memory_storage import InMemoryPipelineStorage
from taskiq_pipelines.tracking.models import PipelineStatus, StepStatus


@pytest.fixture
async def storage():
    """Create an in-memory storage instance."""
    return InMemoryPipelineStorage()


@pytest.mark.anyio
async def test_create_pipeline(storage) -> None:
    """Test creating a pipeline."""
    await storage.create_pipeline("test_pipe", 3)

    status = await storage.get_pipeline_status("test_pipe")
    assert status is not None
    assert status.pipeline_id == "test_pipe"
    assert status.status == PipelineStatus.PENDING
    assert status.total_steps == 3
    assert status.current_step == 0
    assert len(status.steps) == 3

    # Check initial steps
    for i, step in enumerate(status.steps):
        assert step.step_index == i
        assert step.task_name == ""
        assert step.task_id == ""
        assert step.status == StepStatus.PENDING


@pytest.mark.anyio
async def test_start_pipeline(storage) -> None:
    """Test starting a pipeline."""
    await storage.create_pipeline("test_pipe", 1)
    await storage.start_pipeline("test_pipe")

    status = await storage.get_pipeline_status("test_pipe")
    assert status.status == PipelineStatus.RUNNING
    assert status.started_at is not None


@pytest.mark.anyio
async def test_complete_pipeline(storage) -> None:
    """Test completing a pipeline."""
    await storage.create_pipeline("test_pipe", 1)
    await storage.start_pipeline("test_pipe")
    await storage.complete_pipeline("test_pipe", "result_value")

    status = await storage.get_pipeline_status("test_pipe")
    assert status.status == PipelineStatus.COMPLETED
    assert status.result == "result_value"
    assert status.finished_at is not None


@pytest.mark.anyio
async def test_fail_pipeline(storage) -> None:
    """Test failing a pipeline."""
    await storage.create_pipeline("test_pipe", 1)
    await storage.start_pipeline("test_pipe")
    await storage.fail_pipeline("test_pipe", "error_msg")

    status = await storage.get_pipeline_status("test_pipe")
    assert status.status == PipelineStatus.FAILED
    assert status.error == "error_msg"
    assert status.finished_at is not None


@pytest.mark.anyio
async def test_start_step(storage) -> None:
    """Test starting a step."""
    await storage.create_pipeline("test_pipe", 2)
    await storage.start_step("test_pipe", 0, "task1", "Test Task")

    status = await storage.get_pipeline_status("test_pipe")
    step = status.steps[0]
    assert step.status == StepStatus.RUNNING
    assert step.task_id == "task1"
    assert step.task_name == "Test Task"
    assert step.started_at is not None


@pytest.mark.anyio
async def test_complete_step(storage) -> None:
    """Test completing a step."""
    await storage.create_pipeline("test_pipe", 1)
    await storage.start_step("test_pipe", 0, "task1", "Test Task")
    await storage.complete_step("test_pipe", 0)

    status = await storage.get_pipeline_status("test_pipe")
    step = status.steps[0]
    assert step.status == StepStatus.COMPLETED
    assert step.finished_at is not None


@pytest.mark.anyio
async def test_fail_step(storage) -> None:
    """Test failing a step."""
    await storage.create_pipeline("test_pipe", 1)
    await storage.start_step("test_pipe", 0, "task1", "Failing Task")
    await storage.fail_step("test_pipe", 0, "step_error")

    status = await storage.get_pipeline_status("test_pipe")
    step = status.steps[0]
    assert step.status == StepStatus.FAILED
    assert step.error == "step_error"
    assert step.finished_at is not None


@pytest.mark.anyio
async def test_list_pipelines(storage) -> None:
    """Test listing pipelines."""
    # Create multiple pipelines
    await storage.create_pipeline("pipe1", 1)
    await storage.create_pipeline("pipe2", 1)
    await storage.create_pipeline("pipe3", 1)

    pipelines = await storage.list_pipelines()
    assert len(pipelines) == 3
    pipeline_ids = {p.pipeline_id for p in pipelines}
    assert pipeline_ids == {"pipe1", "pipe2", "pipe3"}

    # Test limit
    limited = await storage.list_pipelines(2)
    assert len(limited) == 2


@pytest.mark.anyio
async def test_list_pipelines_empty(storage) -> None:
    """Test listing pipelines when none exist."""
    pipelines = await storage.list_pipelines()
    assert pipelines == []


@pytest.mark.anyio
async def test_get_nonexistent_pipeline(storage) -> None:
    """Test getting status of nonexistent pipeline."""
    status = await storage.get_pipeline_status("nonexistent")
    assert status is None


@pytest.mark.anyio
async def test_cleanup_old(storage) -> None:
    """Test cleanup of old pipelines."""
    # Create a pipeline
    await storage.create_pipeline("test_pipe", 1)
    await storage.start_pipeline("test_pipe")
    await storage.complete_pipeline("test_pipe", "done")

    # Manually set finished_at to old time
    status = await storage.get_pipeline_status("test_pipe")
    old_time = datetime.utcnow() - timedelta(hours=2)
    status.finished_at = old_time

    # Cleanup with 1 second TTL (finished_at is 2 hours old, so it should be removed)
    cleaned = await storage.cleanup_old(ttl_seconds=1)
    assert cleaned == 1

    # Pipeline should be removed
    status = await storage.get_pipeline_status("test_pipe")
    assert status is None
