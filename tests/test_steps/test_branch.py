# mypy: disable-error-code=no-untyped-def
"""Tests for branch step."""

from taskiq_pipelines.steps.branch import BranchStep


def test_branch_step_creation():
    """Test BranchStep creation."""
    branches = [[{"task": "task1"}], [{"task": "task2"}]]
    step = BranchStep(branches=branches)
    assert step.branches == branches


# Note: The act method is a placeholder implementation
# Full testing would require mocking broker and task execution
