"""In-memory implementation of pipeline storage."""

import asyncio
from datetime import datetime, timedelta
from typing import Any

from .models import PipelineStatus, PipelineStatusInfo, StepStatus, StepStatusInfo
from .storage import PipelineStorage


class InMemoryPipelineStorage(PipelineStorage):
    """In-memory pipeline storage for development/testing."""

    def __init__(self) -> None:
        self._pipelines: dict[str, PipelineStatusInfo] = {}
        self._lock = asyncio.Lock()
        self._cleanup_task: asyncio.Task[None] | None = None

    async def create_pipeline(self, pipeline_id: str, total_steps: int) -> None:
        """Create a new pipeline with initial status."""
        async with self._lock:
            self._pipelines[pipeline_id] = PipelineStatusInfo(
                pipeline_id=pipeline_id,
                status=PipelineStatus.PENDING,
                total_steps=total_steps,
                steps=[StepStatusInfo(
                    step_index=i,
                    task_name="",
                    task_id="",
                    status=StepStatus.PENDING,
                ) for i in range(total_steps)],
            )

    async def start_pipeline(self, pipeline_id: str) -> None:
        """Mark pipeline as started."""
        async with self._lock:
            if pipeline_id in self._pipelines:
                self._pipelines[pipeline_id].status = PipelineStatus.RUNNING
                self._pipelines[pipeline_id].started_at = datetime.utcnow()

    async def complete_pipeline(self, pipeline_id: str, result: Any) -> None:
        """Mark pipeline as completed with result."""
        async with self._lock:
            if pipeline_id in self._pipelines:
                self._pipelines[pipeline_id].status = PipelineStatus.COMPLETED
                self._pipelines[pipeline_id].finished_at = datetime.utcnow()
                self._pipelines[pipeline_id].result = result

    async def fail_pipeline(self, pipeline_id: str, error: str) -> None:
        """Mark pipeline as failed with error."""
        async with self._lock:
            if pipeline_id in self._pipelines:
                self._pipelines[pipeline_id].status = PipelineStatus.FAILED
                self._pipelines[pipeline_id].finished_at = datetime.utcnow()
                self._pipelines[pipeline_id].error = error

    async def start_step(
        self,
        pipeline_id: str,
        step_index: int,
        task_id: str,
        task_name: str,
    ) -> None:
        """Mark a step as started."""
        async with self._lock:
            if pipeline_id in self._pipelines and step_index < len(self._pipelines[pipeline_id].steps):
                step = self._pipelines[pipeline_id].steps[step_index]
                step.status = StepStatus.RUNNING
                step.started_at = datetime.utcnow()
                step.task_id = task_id
                step.task_name = task_name

    async def complete_step(self, pipeline_id: str, step_index: int) -> None:
        """Mark a step as completed."""
        async with self._lock:
            if pipeline_id in self._pipelines and step_index < len(self._pipelines[pipeline_id].steps):
                step = self._pipelines[pipeline_id].steps[step_index]
                step.status = StepStatus.COMPLETED
                step.finished_at = datetime.utcnow()

    async def fail_step(self, pipeline_id: str, step_index: int, error: str) -> None:
        """Mark a step as failed."""
        async with self._lock:
            if pipeline_id in self._pipelines and step_index < len(self._pipelines[pipeline_id].steps):
                step = self._pipelines[pipeline_id].steps[step_index]
                step.status = StepStatus.FAILED
                step.finished_at = datetime.utcnow()
                step.error = error

    async def get_pipeline_status(self, pipeline_id: str) -> PipelineStatusInfo | None:
        """Get status of a pipeline."""
        async with self._lock:
            return self._pipelines.get(pipeline_id)

    async def list_pipelines(self, limit: int = 10) -> list[PipelineStatusInfo]:
        """List recent pipelines."""
        async with self._lock:
            # Sort by created_at descending
            sorted_pipelines = sorted(
                self._pipelines.values(),
                key=lambda p: p.created_at,
                reverse=True,
            )
            return sorted_pipelines[:limit]

    async def cleanup_old(self, ttl_seconds: int = 3600) -> int:
        """Clean up old pipeline data."""
        async with self._lock:
            cutoff = datetime.utcnow() - timedelta(seconds=ttl_seconds)
            to_remove = [
                pid for pid, pipeline in self._pipelines.items()
                if pipeline.finished_at and pipeline.finished_at < cutoff
            ]
            for pid in to_remove:
                del self._pipelines[pid]
            return len(to_remove)
