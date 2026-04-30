# mypy: disable-error-code=no-untyped-def
"""Tests for reduce step."""

from taskiq_pipelines.steps.reduce import ReduceStep
from taskiq_pipelines.steps.sequential import SequentialStep


def test_reduce_step_creation():
    """Test ReduceStep creation."""
    task = SequentialStep(
        task_name="test",
        labels={},
        param_name=None,
        additional_kwargs={},
    )
    step = ReduceStep(task=task, initial=0)
    assert step.task == task
    assert step.initial == 0


def test_reduce_step_creation_no_initial():
    """Test ReduceStep creation without initial value."""
    task = SequentialStep(
        task_name="test",
        labels={},
        param_name=None,
        additional_kwargs={},
    )
    step = ReduceStep(task=task)
    assert step.task == task
    assert step.initial is None


# Note: The act method has placeholder implementation
# Full testing would require mocking broker and result handling
