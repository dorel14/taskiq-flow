"""Tests for branch step functionality."""

from unittest.mock import AsyncMock, MagicMock

import pytest
from taskiq import TaskiqResult

from taskiq_pipelines.steps.branch import BranchStep


@pytest.mark.asyncio
async def test_branch_step_basic_execution():
    """Test basic branch step execution."""
    # Create mock broker
    mock_broker = AsyncMock()
    mock_broker.result_backend.get_result = AsyncMock()
    mock_broker.result_backend.get_result.return_value = TaskiqResult(
        is_err=False,
        return_value="branch_result",
        error=None,
        execution_time=0.1,
        log="Success",
    )

    # Create mock kicker
    mock_kicker = AsyncMock()
    mock_kicker.kiq = AsyncMock()
    mock_kicker.kiq.return_value.task_id = "test_task_id"

    # Mock the AsyncKicker import
    import taskiq_pipelines.steps.branch as branch_module

    original_kicker = branch_module.AsyncKicker
    branch_module.AsyncKicker = MagicMock(return_value=mock_kicker)

    try:
        # Create branch step with mock data
        branches = [
            [
                {
                    "task_name": "task1",
                    "labels": {},
                    "param_name": None,
                    "additional_kwargs": {},
                },
            ],
            [
                {
                    "task_name": "task2",
                    "labels": {},
                    "param_name": None,
                    "additional_kwargs": {},
                },
            ],
        ]
        step = BranchStep(branches=branches)

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
        assert result.return_value == "branch_result"

    finally:
        # Restore original import
        branch_module.AsyncKicker = original_kicker


def test_branch_step_empty_branches():
    """Test branch step with empty branches."""
    step = BranchStep(branches=[])

    # Should not crash with empty branches
    assert step.branches == []


@pytest.mark.asyncio
async def test_branch_step_error_handling():
    """Test branch step error handling."""
    # Create mock broker that raises an exception
    mock_broker = AsyncMock()
    mock_broker.result_backend.get_result = AsyncMock(
        side_effect=Exception("Task failed"),
    )

    # Create mock kicker
    mock_kicker = AsyncMock()
    mock_kicker.kiq = AsyncMock()
    mock_kicker.kiq.return_value.task_id = "test_task_id"

    # Mock the AsyncKicker import
    import taskiq_pipelines.steps.branch as branch_module

    original_kicker = branch_module.AsyncKicker
    branch_module.AsyncKicker = MagicMock(return_value=mock_kicker)

    try:
        # Create branch step with one branch
        branches = [
            [
                {
                    "task_name": "failing_task",
                    "labels": {},
                    "param_name": None,
                    "additional_kwargs": {},
                },
            ],
        ]
        step = BranchStep(branches=branches)

        # Create mock result
        result = TaskiqResult(
            is_err=False,
            return_value="original_value",
            error=None,
            execution_time=0.0,
            log="Original",
        )

        # Execute the step - should handle errors gracefully
        await step.act(mock_broker, 1, "parent_task", "task_id", "pipe_data", result)

        # Should keep original value when branch fails
        assert result.return_value == "original_value"

    finally:
        # Restore original import
        branch_module.AsyncKicker = original_kicker
