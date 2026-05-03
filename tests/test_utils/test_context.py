# mypy: disable-error-code=no-untyped-def
"""Tests for pipeline context utilities."""

import pytest
from taskiq import InMemoryBroker, TaskiqResult

from taskiq_flow.utils.context import PipelineContext


@pytest.fixture
async def broker():
    """Create a test broker."""
    return InMemoryBroker()


@pytest.fixture
async def context(broker):
    """Create a pipeline context."""
    return PipelineContext(broker)


@pytest.mark.anyio
async def test_pipeline_context_creation(broker):
    """Test PipelineContext creation."""
    context = PipelineContext(broker)
    assert context.broker == broker


@pytest.mark.anyio
async def test_get_result_success(context, broker):
    """Test getting result successfully."""
    # Set up a result
    task_id = "test_task_123"
    result_value = "test_result"

    result_obj = TaskiqResult(
        is_err=False,
        return_value=result_value,
        error=None,
        execution_time=0,
        log="",
    )
    await broker.result_backend.set_result(task_id, result_obj)

    retrieved = await context.get_result(task_id)
    assert retrieved == result_value


@pytest.mark.anyio
async def test_get_result_failure(context, broker):
    """Test getting result when task failed."""
    # Set up a failed result
    task_id = "failed_task"
    error_msg = "Task failed"
    failed_result = TaskiqResult(
        is_err=True,
        return_value=None,
        error=Exception(error_msg),
        execution_time=0,
        log="",
    )
    await broker.result_backend.set_result(task_id, failed_result)

    with pytest.raises(RuntimeError, match=f"Task {task_id} failed: {error_msg}"):
        await context.get_result(task_id)


@pytest.mark.anyio
async def test_get_result_not_ready(context):
    """Test getting result when not ready."""
    task_id = "nonexistent_task"

    with pytest.raises(RuntimeError):
        await context.get_result(task_id)
