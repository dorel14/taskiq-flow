# mypy: disable-error-code=no-untyped-def
"""Tests for filter step."""

import pytest
from taskiq import InMemoryBroker

from taskiq_flow.steps.filter import FilterStep


@pytest.fixture
def broker():
    """Create a test broker."""
    return InMemoryBroker()


@pytest.fixture
def mock_task(broker):
    """Create a mock task."""

    @broker.task
    def test_task(x):
        return x > 5

    return test_task


def test_filter_step_creation():
    """Test FilterStep creation."""
    step = FilterStep(
        task_name="test_task",
        labels={"key": "value"},
        param_name="item",
        additional_kwargs={"extra": "value"},
        skip_errors=True,
        check_interval=0.1,
        retries=3,
        timeout=10,
        retry_delay=1,
    )
    assert step.task_name == "test_task"
    assert step.labels == {"key": "value"}
    assert step.param_name == "item"
    assert step.additional_kwargs == {"extra": "value"}
    assert step.skip_errors is True
    assert step.check_interval == 0.1
    assert step.retries == 3
    assert step.timeout == 10
    assert step.retry_delay == 1


def test_filter_step_from_task(mock_task):
    """Test FilterStep.from_task creation."""
    step = FilterStep.from_task(
        mock_task,
        param_name="data",
        skip_errors=False,
        check_interval=0.2,
        retries=2,
        timeout=5,
        retry_delay=1,
        extra_param="value",
    )
    assert step.task_name == "test_filter:test_task"
    assert step.param_name == "data"
    assert step.skip_errors is False
    assert step.check_interval == 0.2
    assert step.retries == 2
    assert step.timeout == 5
    assert step.retry_delay == 1
    assert step.additional_kwargs == {"extra_param": "value"}
    # Check that retry/timeout labels are added
    assert "_step_retries" in step.labels
    assert "_step_timeout" in step.labels
    assert "_step_retry_delay" in step.labels


def test_filter_step_from_kicker(broker):
    """Test FilterStep.from_task with AsyncKicker."""
    from taskiq.kicker import AsyncKicker

    kicker: AsyncKicker[[None], None] = AsyncKicker(
        task_name="test", broker=broker, labels={"label": "val"},
    )
    step = FilterStep.from_task(
        kicker,
        param_name=None,
        skip_errors=True,
        check_interval=0.1,
    )
    assert step.task_name == "test"
    assert step.labels == {"label": "val"}


# Note: Testing the act method requires complex mocking of broker and async task execution
# These tests focus on step creation and configuration
