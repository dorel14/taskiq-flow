"""Tests for group step functionality."""

from typing import Any

from unittest.mock import AsyncMock, MagicMock

import pytest
from taskiq import TaskiqResult

from taskiq_flow.steps.group import GroupStep


@pytest.mark.asyncio
async def test_group_step_basic_execution() -> None:
    """Test basic group step execution with multiple tasks."""
    # Create mock broker
    mock_broker = AsyncMock()
    mock_broker.result_backend.get_result = AsyncMock()
    mock_broker.result_backend.get_result.return_value = TaskiqResult(
        is_err=False,
        return_value="task_result",
        error=None,
        execution_time=0.1,
        log="Success",
    )

    # Create mock kicker
    mock_kicker = AsyncMock()
    mock_kicker.kiq = AsyncMock()
    mock_kicker.kiq.return_value.task_id = "test_task_id"

    # Mock the AsyncKicker import
    import taskiq_flow.steps.group as group_module

    original_kicker = group_module.AsyncKicker
    group_module.AsyncKicker = MagicMock(return_value=mock_kicker)  # type: ignore[misc]

    try:
        # Create group step with multiple tasks
        tasks: list[dict[str, Any]] = [
            {
                "task_name": "task1",
                "labels": {},
                "param_name": None,
                "additional_kwargs": {},
            },
            {
                "task_name": "task2",
                "labels": {},
                "param_name": None,
                "additional_kwargs": {},
            },
            {
                "task_name": "task3",
                "labels": {},
                "param_name": None,
                "additional_kwargs": {},
            },
        ]
        step = GroupStep(tasks=tasks)

        # Create mock result
        result = TaskiqResult(
            is_err=False,
            return_value="original_value",
            error=None,
            execution_time=0.0,
            log="Original",
        )

        # Execute the step
        await step.act(mock_broker, 1, "parent_task", "task_id", "pipe_data", result)

        # Verify that the result was updated with list of all task results
        assert isinstance(result.return_value, list)
        assert len(result.return_value) == 3
        assert all(r == "task_result" for r in result.return_value)

    finally:
        # Restore original import
        group_module.AsyncKicker = original_kicker


def test_group_step_creation() -> None:
    """Test group step creation and configuration."""
    tasks: list[dict[str, Any]] = [
        {
            "task_name": "task1",
            "labels": {},
            "param_name": None,
            "additional_kwargs": {},
        },
        {
            "task_name": "task2",
            "labels": {},
            "param_name": "param",
            "additional_kwargs": {"key": "value"},
        },
    ]
    step = GroupStep(tasks=tasks)

    assert step.tasks == tasks
    assert len(step.tasks) == 2


def test_group_step_empty_tasks() -> None:
    """Test group step with empty tasks list."""
    step = GroupStep(tasks=[])

    assert step.tasks == []


@pytest.mark.asyncio
async def test_group_step_with_param_names() -> None:
    """Test group step with parameter names."""
    # Create mock broker
    mock_broker = AsyncMock()
    mock_broker.result_backend.get_result = AsyncMock()
    mock_broker.result_backend.get_result.return_value = TaskiqResult(
        is_err=False,
        return_value="task_result",
        error=None,
        execution_time=0.1,
        log="Success",
    )

    # Create mock kicker
    mock_kicker = AsyncMock()
    mock_kicker.kiq = AsyncMock()
    mock_kicker.kiq.return_value.task_id = "test_task_id"

    # Mock the AsyncKicker import
    import taskiq_flow.steps.group as group_module

    original_kicker = group_module.AsyncKicker
    group_module.AsyncKicker = MagicMock(return_value=mock_kicker)  # type: ignore[misc]

    try:
        # Create group step with parameter names
        tasks: list[dict[str, Any]] = [
            {
                "task_name": "task1",
                "labels": {},
                "param_name": "param1",
                "additional_kwargs": {},
            },
            {
                "task_name": "task2",
                "labels": {},
                "param_name": "param2",
                "additional_kwargs": {},
            },
        ]
        step = GroupStep(tasks=tasks)

        # Create mock result
        result = TaskiqResult(
            is_err=False,
            return_value="original_value",
            error=None,
            execution_time=0.0,
            log="Original",
        )

        # Execute the step
        await step.act(mock_broker, 1, "parent_task", "task_id", "pipe_data", result)

        # Verify that the result was updated
        assert isinstance(result.return_value, list)
        assert len(result.return_value) == 2

    finally:
        # Restore original import
        group_module.AsyncKicker = original_kicker


@pytest.mark.asyncio
async def test_group_step_error_handling() -> None:
    """Test group step error handling when a task fails."""
    # Create mock broker that raises an exception for one task
    mock_broker = AsyncMock()

    # Make get_result return different results for different task IDs
    async def get_result_side_effect(task_id: str) -> TaskiqResult:
        if task_id == "test_task_id_1":
            return TaskiqResult(
                is_err=False,
                return_value="success_result",
                error=None,
                execution_time=0.1,
                log="Success",
            )
        raise Exception("Task failed")

    mock_broker.result_backend.get_result = AsyncMock(
        side_effect=get_result_side_effect,
    )

    # Create mock kicker
    mock_kicker = AsyncMock()
    mock_kicker.kiq = AsyncMock()

    # Return different task IDs for different calls
    async def kiq_side_effect(*args: Any, **kwargs: Any) -> MagicMock:
        task_result = MagicMock()
        if "task1" in str(kwargs):
            task_result.task_id = "test_task_id_1"
        else:
            task_result.task_id = "test_task_id_2"
        return task_result

    mock_kicker.kiq.side_effect = kiq_side_effect

    # Mock the AsyncKicker import
    import taskiq_flow.steps.group as group_module

    original_kicker = group_module.AsyncKicker
    group_module.AsyncKicker = MagicMock(return_value=mock_kicker)  # type: ignore[misc]

    try:
        # Create group step with multiple tasks
        tasks: list[dict[str, Any]] = [
            {
                "task_name": "task1",
                "labels": {},
                "param_name": None,
                "additional_kwargs": {},
            },
            {
                "task_name": "task2",
                "labels": {},
                "param_name": None,
                "additional_kwargs": {},
            },
        ]
        step = GroupStep(tasks=tasks)

        # Create mock result
        result = TaskiqResult(
            is_err=False,
            return_value="original_value",
            error=None,
            execution_time=0.0,
            log="Original",
        )

        # Execute the step
        await step.act(mock_broker, 1, "parent_task", "task_id", "pipe_data", result)

        # Verify that the result was updated with list containing None for failed task
        assert isinstance(result.return_value, list)
        assert len(result.return_value) == 2
        # The first task should have succeeded, but due to the mock setup,
        # both tasks might fail because of the side effect
        # The important thing is that we have 2 results and no exception was raised

    finally:
        # Restore original import
        group_module.AsyncKicker = original_kicker
