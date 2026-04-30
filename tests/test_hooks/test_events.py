"""Tests for hook events."""

from datetime import datetime

from taskiq_pipelines.hooks.events import (
    PipelineCompleteEvent,
    PipelineErrorEvent,
    PipelineEvent,
    PipelineStartEvent,
    StepCompleteEvent,
    StepErrorEvent,
    StepStartEvent,
)


def test_pipeline_event_base():
    """Test base PipelineEvent."""
    event = PipelineEvent(pipeline_id="test_pipe")
    assert event.pipeline_id == "test_pipe"
    assert isinstance(event.timestamp, datetime)


def test_pipeline_start_event():
    """Test PipelineStartEvent."""
    event = PipelineStartEvent(pipeline_id="test_pipe")
    assert event.pipeline_id == "test_pipe"
    assert hasattr(event, "timestamp")


def test_step_start_event():
    """Test StepStartEvent."""
    event = StepStartEvent(
        pipeline_id="test_pipe",
        step_index=0,
        task_name="test_task",
        task_id="task_123",
    )
    assert event.pipeline_id == "test_pipe"
    assert event.step_index == 0
    assert event.task_name == "test_task"
    assert event.task_id == "task_123"


def test_step_complete_event():
    """Test StepCompleteEvent."""
    result = {"data": "value"}
    event = StepCompleteEvent(
        pipeline_id="test_pipe",
        step_index=1,
        task_name="process_task",
        task_id="task_456",
        result=result,
    )
    assert event.pipeline_id == "test_pipe"
    assert event.step_index == 1
    assert event.task_name == "process_task"
    assert event.task_id == "task_456"
    assert event.result == result


def test_pipeline_complete_event():
    """Test PipelineCompleteEvent."""
    result = "final_result"
    event = PipelineCompleteEvent(pipeline_id="test_pipe", result=result)
    assert event.pipeline_id == "test_pipe"
    assert event.result == result


def test_step_error_event():
    """Test StepErrorEvent."""
    event = StepErrorEvent(
        pipeline_id="test_pipe",
        step_index=2,
        task_name="failing_task",
        task_id="task_789",
        error="Task failed",
    )
    assert event.pipeline_id == "test_pipe"
    assert event.step_index == 2
    assert event.task_name == "failing_task"
    assert event.task_id == "task_789"
    assert event.error == "Task failed"


def test_pipeline_error_event():
    """Test PipelineErrorEvent."""
    event = PipelineErrorEvent(pipeline_id="test_pipe", error="Pipeline failed")
    assert event.pipeline_id == "test_pipe"
    assert event.error == "Pipeline failed"


def test_events_have_timestamps():
    """Test that all events have timestamps."""
    events = [
        PipelineStartEvent(pipeline_id="test"),
        StepStartEvent(
            pipeline_id="test",
            step_index=0,
            task_name="task",
            task_id="id",
        ),
        StepCompleteEvent(
            pipeline_id="test",
            step_index=0,
            task_name="task",
            task_id="id",
            result=None,
        ),
        PipelineCompleteEvent(pipeline_id="test", result=None),
        StepErrorEvent(
            pipeline_id="test",
            step_index=0,
            task_name="task",
            task_id="id",
            error="err",
        ),
        PipelineErrorEvent(pipeline_id="test", error="err"),
    ]

    for event in events:
        assert hasattr(event, "timestamp")
        assert isinstance(event.timestamp, datetime)


def test_event_model_dump():
    """Test that events can be serialized."""
    event = PipelineStartEvent(pipeline_id="test_pipe")
    data = event.model_dump()
    assert data["pipeline_id"] == "test_pipe"
    assert "timestamp" in data


def test_step_event_model_dump():
    """Test step event serialization."""
    event = StepStartEvent(
        pipeline_id="test_pipe",
        step_index=1,
        task_name="my_task",
        task_id="task_123",
    )
    data = event.model_dump()
    assert data["pipeline_id"] == "test_pipe"
    assert data["step_index"] == 1
    assert data["task_name"] == "my_task"
    assert data["task_id"] == "task_123"
