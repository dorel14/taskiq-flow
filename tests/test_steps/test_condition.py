"""Tests for step implementations."""

from collections.abc import Callable

import pytest
from taskiq import InMemoryBroker

from taskiq_flow.steps.condition import ConditionStep
from taskiq_flow.steps.sequential import SequentialStep


@pytest.fixture
def broker() -> InMemoryBroker:
    """Create a test broker."""
    return InMemoryBroker()


@pytest.fixture
def mock_task(broker: InMemoryBroker) -> Callable[[int], int]:
    """Create a mock task."""

    @broker.task
    def test_task(x: int) -> int:
        return x

    return test_task


def test_condition_step_creation() -> None:
    """Test ConditionStep creation."""

    def condition_func(x: int) -> bool:
        return x > 0

    step = ConditionStep(
        condition=condition_func,
        task=SequentialStep(
            task_name="test",
            labels={},
            param_name=None,
            additional_kwargs={},
        ),
    )
    assert step.condition == condition_func
    assert step.task is not None
    assert step.else_task is None


def test_condition_step_with_else() -> None:
    """Test ConditionStep with else task."""
    step = ConditionStep(
        condition="value > 5",
        task=SequentialStep(
            task_name="test",
            labels={},
            param_name=None,
            additional_kwargs={},
        ),
        else_task=SequentialStep(
            task_name="else_test",
            labels={},
            param_name=None,
            additional_kwargs={},
        ),
    )
    assert step.condition == "value > 5"
    assert step.task is not None
    assert step.else_task is not None


# Note: Testing the act method directly is complex due to dependencies
# These tests focus on the condition evaluation logic instead


def test_condition_step_eval_condition() -> None:
    """Test _eval_condition method."""
    step = ConditionStep(
        condition="value > 5",
        task=SequentialStep(
            task_name="test",
            labels={},
            param_name=None,
            additional_kwargs={},
        ),
    )

    assert step._eval_condition("value > 5", 10) is True
    assert step._eval_condition("value > 5", 3) is False
    assert step._eval_condition("len(value) == 2", [1, 2]) is True


def test_condition_step_eval_condition_error() -> None:
    """Test _eval_condition with invalid expression."""
    step = ConditionStep(
        condition="invalid syntax +++",
        task=SequentialStep(
            task_name="test",
            labels={},
            param_name=None,
            additional_kwargs={},
        ),
    )

    # Should return False on error
    assert step._eval_condition("invalid syntax +++", 10) is False
