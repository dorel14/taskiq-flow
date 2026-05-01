"""Pipeline event models."""

from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel


class PipelineEvent(BaseModel):
    """Base event for pipeline lifecycle."""

    pipeline_id: str
    timestamp: datetime = datetime.now(timezone.utc)


class PipelineStartEvent(PipelineEvent):
    """Event fired when pipeline starts."""

    # Could include lightweight pipeline reference if needed


class StepStartEvent(PipelineEvent):
    """Event fired when a step starts."""

    step_index: int
    task_name: str
    task_id: str


class StepCompleteEvent(PipelineEvent):
    """Event fired when a step completes."""

    step_index: int
    task_name: str
    task_id: str
    result: Any


class PipelineCompleteEvent(PipelineEvent):
    """Event fired when pipeline completes."""

    result: Any


class StepErrorEvent(PipelineEvent):
    """Event fired when a step fails."""

    step_index: int
    task_name: str
    task_id: str
    error: str


class PipelineErrorEvent(PipelineEvent):
    """Event fired when pipeline fails."""

    error: str
