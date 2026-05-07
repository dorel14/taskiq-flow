"""Modèles d'événements de pipeline.

Ce module définit tous les événements émis pendant l'exécution
d'un pipeline: PipelineStartEvent, StepStartEvent, StepCompleteEvent,
PipelineCompleteEvent, ainsi que les événements d'erreur.
Ces événements sont utilisés par le HookManager pour notifier
les callbacks enregistrés.

Auteur: SoniqueBay Team
Version: 0.3.2
"""

from datetime import datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel


class EventType(str, Enum):
    """Pipeline event types."""

    PIPELINE_START = "PipelineStartEvent"
    PIPELINE_COMPLETE = "PipelineCompleteEvent"
    PIPELINE_ERROR = "PipelineErrorEvent"
    STEP_START = "StepStartEvent"
    STEP_COMPLETE = "StepCompleteEvent"
    STEP_ERROR = "StepErrorEvent"
    STEP_RETRY = "StepRetryEvent"
    STEP_SKIP = "StepSkipEvent"
    PIPELINE_SKIP = "PipelineSkipEvent"
    RETRY_SCHEDULED = "RetryScheduledEvent"
    RETRY_EXECUTED = "RetryExecutedEvent"
    METRIC_RECORD = "MetricRecordEvent"
    DAG_UPDATED = "DAGUpdatedEvent"
    CRITICAL_PATH_CHANGED = "CriticalPathChangedEvent"


class PipelineEvent(BaseModel):
    """Base event for pipeline lifecycle."""

    pipeline_id: str
    timestamp: datetime = datetime.now(timezone.utc)
    event_type: EventType = EventType.PIPELINE_START


class PipelineStartEvent(PipelineEvent):
    """Event fired when pipeline starts."""

    event_type: EventType = EventType.PIPELINE_START
    task_count: int = 0


class StepStartEvent(PipelineEvent):
    """Event fired when a step starts."""

    event_type: EventType = EventType.STEP_START
    step_index: int
    task_name: str
    task_id: str
    attempt: int = 1


class StepCompleteEvent(PipelineEvent):
    """Event fired when a step completes."""

    event_type: EventType = EventType.STEP_COMPLETE
    step_index: int
    task_name: str
    task_id: str
    result: Any
    duration: float = 0.0
    attempt: int = 1


class PipelineCompleteEvent(PipelineEvent):
    """Event fired when pipeline completes."""

    event_type: EventType = EventType.PIPELINE_COMPLETE
    result: Any
    duration: float = 0.0
    success_rate: float = 1.0


class StepErrorEvent(PipelineEvent):
    """Event fired when a step fails."""

    event_type: EventType = EventType.STEP_ERROR
    step_index: int
    task_name: str
    task_id: str
    error: str
    attempt: int = 1
    max_attempts: int = 1


class StepRetryEvent(PipelineEvent):
    """Event fired when a step is retried."""

    event_type: EventType = EventType.STEP_RETRY
    step_index: int
    task_name: str
    task_id: str
    error: str
    attempt: int
    max_attempts: int
    wait_time: float = 0.0


class StepSkipEvent(PipelineEvent):
    """Event fired when a step is skipped."""

    event_type: EventType = EventType.STEP_SKIP
    step_index: int
    task_name: str
    task_id: str
    reason: str


class PipelineErrorEvent(PipelineEvent):
    """Event fired when pipeline fails."""

    event_type: EventType = EventType.PIPELINE_ERROR
    error: str
    failed_steps: list[str] = []


class PipelineSkipEvent(PipelineEvent):
    """Event fired when pipeline is skipped."""

    event_type: EventType = EventType.PIPELINE_SKIP
    reason: str
    skipped_steps: list[str] = []


class MetricRecordEvent(PipelineEvent):
    """Event fired when a metric is recorded."""

    event_type: EventType = EventType.METRIC_RECORD
    metric_name: str
    metric_value: float
    tags: dict[str, Any] = {}


class DAGUpdatedEvent(PipelineEvent):
    """Event fired when pipeline DAG is updated/built."""

    event_type: EventType = EventType.DAG_UPDATED
    node_count: int = 0
    edge_count: int = 0


class CriticalPathChangedEvent(PipelineEvent):
    """Event fired when critical path is recalculated."""

    event_type: EventType = EventType.CRITICAL_PATH_CHANGED
    critical_path: list[str] = []
