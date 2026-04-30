# mypy: disable-error-code=no-untyped-def
"""Tests for pipeline tracking manager."""

import pytest
from taskiq import InMemoryBroker

from taskiq_pipelines.tracking.manager import PipelineTrackingManager
from taskiq_pipelines.tracking.memory_storage import InMemoryPipelineStorage
from taskiq_pipelines.tracking.models import PipelineStatus


@pytest.fixture
async def storage():
    """Create an in-memory storage instance."""
    return InMemoryPipelineStorage()


@pytest.fixture
async def manager(storage):
    """Create a manager with storage."""
    return PipelineTrackingManager(storage)


@pytest.mark.anyio
async def test_manager_init_without_storage():
    """Test manager initialization without storage."""
    manager = PipelineTrackingManager()
    assert manager.storage is None


@pytest.mark.anyio
async def test_manager_with_storage():
    """Test manager with storage set."""
    storage = InMemoryPipelineStorage()
    manager = PipelineTrackingManager(storage)
    assert manager.storage == storage


@pytest.mark.anyio
async def test_manager_with_storage_method():
    """Test setting storage via with_storage method."""
    manager = PipelineTrackingManager()
    storage = InMemoryPipelineStorage()
    result = manager.with_storage(storage)
    assert manager.storage == storage
    assert result is manager  # Returns self for chaining


@pytest.mark.anyio
async def test_manager_with_auto_storage():
    """Test setting storage via with_auto_storage method."""
    manager = PipelineTrackingManager()
    broker = InMemoryBroker()
    result = manager.with_auto_storage(broker)
    assert manager.storage is not None
    assert isinstance(manager.storage, InMemoryPipelineStorage)
    assert result is manager  # Returns self for chaining


@pytest.mark.anyio
async def test_initiate(manager):
    """Test initiating a pipeline."""
    await manager.initiate("test_pipe", 3)
    status = await manager.get_status("test_pipe")
    assert status is not None
    assert status.pipeline_id == "test_pipe"
    assert status.total_steps == 3
    assert status.status == PipelineStatus.PENDING


@pytest.mark.anyio
async def test_initiate_no_storage():
    """Test initiating a pipeline without storage."""
    manager = PipelineTrackingManager()
    # Should not raise error
    await manager.initiate("test_pipe", 1)
    status = await manager.get_status("test_pipe")
    assert status is None


@pytest.mark.anyio
async def test_mark_pipeline_started(manager):
    """Test marking pipeline as started."""
    await manager.initiate("test_pipe", 1)
    await manager.mark_pipeline_started("test_pipe")
    status = await manager.get_status("test_pipe")
    assert status.status == PipelineStatus.RUNNING
    assert status.started_at is not None


@pytest.mark.anyio
async def test_mark_pipeline_completed(manager):
    """Test marking pipeline as completed."""
    await manager.initiate("test_pipe", 1)
    await manager.mark_pipeline_started("test_pipe")
    await manager.mark_pipeline_completed("test_pipe", "result_value")
    status = await manager.get_status("test_pipe")
    assert status.status == PipelineStatus.COMPLETED
    assert status.result == "result_value"
    assert status.finished_at is not None


@pytest.mark.anyio
async def test_mark_pipeline_failed(manager):
    """Test marking pipeline as failed."""
    await manager.initiate("test_pipe", 1)
    await manager.mark_pipeline_started("test_pipe")
    await manager.mark_pipeline_failed("test_pipe", "error_msg")
    status = await manager.get_status("test_pipe")
    assert status.status == PipelineStatus.FAILED
    assert status.error == "error_msg"
    assert status.finished_at is not None


@pytest.mark.anyio
async def test_mark_step_started(manager):
    """Test marking step as started."""
    await manager.initiate("test_pipe", 2)
    await manager.mark_step_started("test_pipe", 0, "task1", "Test Task")
    status = await manager.get_status("test_pipe")
    step = status.steps[0]
    assert step.status.name == "RUNNING"
    assert step.task_id == "task1"
    assert step.task_name == "Test Task"


@pytest.mark.anyio
async def test_mark_step_completed(manager):
    """Test marking step as completed."""
    await manager.initiate("test_pipe", 1)
    await manager.mark_step_started("test_pipe", 0, "task1", "Test Task")
    await manager.mark_step_completed("test_pipe", 0)
    status = await manager.get_status("test_pipe")
    step = status.steps[0]
    assert step.status.name == "COMPLETED"
    assert step.finished_at is not None


@pytest.mark.anyio
async def test_mark_step_failed(manager):
    """Test marking step as failed."""
    await manager.initiate("test_pipe", 1)
    await manager.mark_step_started("test_pipe", 0, "task1", "Failing Task")
    await manager.mark_step_failed("test_pipe", 0, "step_error")
    status = await manager.get_status("test_pipe")
    step = status.steps[0]
    assert step.status.name == "FAILED"
    assert step.error == "step_error"
    assert step.finished_at is not None


@pytest.mark.anyio
async def test_get_status(manager):
    """Test getting pipeline status."""
    await manager.initiate("test_pipe", 1)
    status = await manager.get_status("test_pipe")
    assert status is not None
    assert status.pipeline_id == "test_pipe"


@pytest.mark.anyio
async def test_get_status_nonexistent(manager):
    """Test getting status of nonexistent pipeline."""
    status = await manager.get_status("nonexistent")
    assert status is None


@pytest.mark.anyio
async def test_list_recent(manager):
    """Test listing recent pipelines."""
    await manager.initiate("pipe1", 1)
    await manager.initiate("pipe2", 1)
    await manager.initiate("pipe3", 1)

    pipelines = await manager.list_recent()
    assert len(pipelines) == 3
    pipeline_ids = {p.pipeline_id for p in pipelines}
    assert pipeline_ids == {"pipe1", "pipe2", "pipe3"}

    # Test limit
    limited = await manager.list_recent(2)
    assert len(limited) == 2


@pytest.mark.anyio
async def test_list_recent_no_storage():
    """Test listing recent pipelines without storage."""
    manager = PipelineTrackingManager()
    pipelines = await manager.list_recent()
    assert pipelines == []


@pytest.mark.anyio
async def test_cleanup(manager):
    """Test cleanup of old data."""
    await manager.initiate("test_pipe", 1)
    await manager.mark_pipeline_started("test_pipe")
    await manager.mark_pipeline_completed("test_pipe", "done")

    # Cleanup with high TTL, should not remove
    cleaned = await manager.cleanup(ttl_seconds=3600)
    assert cleaned == 0

    # With low TTL, pipeline is recent so not removed
    cleaned = await manager.cleanup(ttl_seconds=1)
    assert cleaned == 0

    # Pipeline should still exist
    status = await manager.get_status("test_pipe")
    assert status is not None


@pytest.mark.anyio
async def test_cleanup_no_storage():
    """Test cleanup without storage."""
    manager = PipelineTrackingManager()
    cleaned = await manager.cleanup()
    assert cleaned == 0
