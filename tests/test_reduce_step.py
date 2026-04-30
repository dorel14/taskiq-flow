"""Tests for reduce step functionality."""

from collections.abc import Callable
from unittest.mock import AsyncMock, MagicMock

import pytest
from taskiq import TaskiqResult

from taskiq_flow.steps.reduce import ReduceStep


def test_reduce_step_creation() -> None:
    """Test reduce step creation and configuration."""
    # Create reduce step with sum function
    step = ReduceStep(task=None, initial=0, reduce_func="sum")  # type: ignore[arg-type]

    assert step.initial == 0
    assert step.reduce_func == "sum"
    assert step.task is None

    # Test with different reduce functions
    step_max = ReduceStep(task=None, initial=None, reduce_func="max")  # type: ignore[arg-type]
    assert step_max.reduce_func == "max"

    step_concat = ReduceStep(task=None, initial="", reduce_func="concat")  # type: ignore[arg-type]
    assert step_concat.reduce_func == "concat"


def test_reduce_step_from_task() -> None:
    """Test creating reduce step from task."""
    mock_task = MagicMock()
    mock_task.kicker.return_value = MagicMock()

    step = ReduceStep.from_task(task=mock_task, initial=0, reduce_func="sum")

    assert step.initial == 0
    assert step.reduce_func == "sum"
    assert step.task == mock_task


def test_reduce_step_invalid_input() -> None:
    """Test reduce step with invalid input."""
    step = ReduceStep(task=None, initial=0, reduce_func="sum")  # type: ignore[arg-type]

    result = TaskiqResult(
        is_err=False,
        return_value="not_iterable",  # Not iterable
        error=None,
        execution_time=0.0,
        log="Original",
    )

    # Should raise AbortPipeline for non-iterable input
    with pytest.raises(Exception):  # AbortPipeline exception
        import asyncio

        asyncio.run(
            step.act(AsyncMock(), 1, "parent_task", "task_id", "pipe_data", result),
        )
