"""Models for pipeline tracking."""

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class PipelineStatus(str, Enum):
    """Pipeline execution status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class StepStatus(str, Enum):
    """Step execution status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class StepStatusInfo(BaseModel):
    """Status information for a pipeline step."""
    step_index: int
    task_name: str
    task_id: str
    status: StepStatus
    started_at: datetime | None = None
    finished_at: datetime | None = None
    retries: int = 0
    error: str | None = None


class PipelineStatusInfo(BaseModel):
    """Status information for a pipeline."""
    pipeline_id: str
    status: PipelineStatus
    total_steps: int
    current_step: int = 0
    created_at: datetime = Field(default_factory=datetime.utcnow)
    started_at: datetime | None = None
    finished_at: datetime | None = None
    result: Any = None
    error: str | None = None
    steps: list[StepStatusInfo] = Field(default_factory=list)