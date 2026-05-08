# mypy: disable-error-code=no-untyped-def
"""Tests for pipeline tracking models."""

from datetime import datetime, timezone

from taskiq_flow.tracking.models import (
    PipelineStatus,
    PipelineStatusInfo,
    StepStatus,
    StepStatusInfo,
)


def test_pipeline_status_enum():
    """Test PipelineStatus enum values."""
    assert PipelineStatus.PENDING.value == "pending"
    assert PipelineStatus.RUNNING.value == "running"
    assert PipelineStatus.COMPLETED.value == "completed"
    assert PipelineStatus.FAILED.value == "failed"


def test_step_status_enum():
    """Test StepStatus enum values."""
    assert StepStatus.PENDING.value == "pending"
    assert StepStatus.RUNNING.value == "running"
    assert StepStatus.COMPLETED.value == "completed"
    assert StepStatus.FAILED.value == "failed"


def test_step_status_info_creation():
    """Test StepStatusInfo creation."""
    step = StepStatusInfo(
        step_index=0,
        task_name="test_task",
        task_id="task_123",
        status=StepStatus.RUNNING,
        started_at=datetime.now(timezone.utc),
        retries=2,
        error="some error",
    )

    assert step.step_index == 0
    assert step.task_name == "test_task"
    assert step.task_id == "task_123"
    assert step.status == StepStatus.RUNNING
    assert step.retries == 2
    assert step.error == "some error"


def test_pipeline_status_info_creation():
    """Test PipelineStatusInfo creation."""
    now = datetime.now(timezone.utc)
    steps = [
        StepStatusInfo(
            step_index=0,
            task_name="task1",
            task_id="id1",
            status=StepStatus.COMPLETED,
        ),
        StepStatusInfo(
            step_index=1,
            task_name="task2",
            task_id="id2",
            status=StepStatus.RUNNING,
        ),
    ]

    pipeline = PipelineStatusInfo(
        pipeline_id="pipe_123",
        status=PipelineStatus.RUNNING,
        total_steps=2,
        current_step=1,
        created_at=now,
        started_at=now,
        finished_at=None,
        result=None,
        error=None,
        steps=steps,
    )

    assert pipeline.pipeline_id == "pipe_123"
    assert pipeline.status == PipelineStatus.RUNNING
    assert pipeline.total_steps == 2
    assert pipeline.current_step == 1
    assert pipeline.created_at == now
    assert pipeline.started_at == now
    assert pipeline.finished_at is None
    assert pipeline.result is None
    assert pipeline.error is None
    assert len(pipeline.steps) == 2


def test_pipeline_status_info_default_values():
    """Test PipelineStatusInfo default values."""
    pipeline = PipelineStatusInfo(
        pipeline_id="test",
        status=PipelineStatus.PENDING,
        total_steps=1,
    )

    assert pipeline.current_step == 0
    assert pipeline.created_at is not None
    assert pipeline.started_at is None
    assert pipeline.finished_at is None
    assert pipeline.result is None
    assert pipeline.error is None
    assert pipeline.steps == []


def test_step_status_info_default_values():
    """Test StepStatusInfo default values."""
    step = StepStatusInfo(
        step_index=0,
        task_name="test",
        task_id="test_id",
        status=StepStatus.PENDING,
    )

    assert step.started_at is None
    assert step.finished_at is None
    assert step.retries == 0
    assert step.error is None
