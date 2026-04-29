"""Storage interface for pipeline tracking."""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any

from .models import PipelineStatusInfo


class PipelineStorage(ABC):
    """Abstract base class for pipeline tracking storage."""

    @abstractmethod
    async def create_pipeline(self, pipeline_id: str, total_steps: int) -> None:
        """Create a new pipeline with initial status."""
        ...

    @abstractmethod
    async def start_pipeline(self, pipeline_id: str) -> None:
        """Mark pipeline as started."""
        ...

    @abstractmethod
    async def complete_pipeline(self, pipeline_id: str, result: Any) -> None:
        """Mark pipeline as completed with result."""
        ...

    @abstractmethod
    async def fail_pipeline(self, pipeline_id: str, error: str) -> None:
        """Mark pipeline as failed with error."""
        ...

    @abstractmethod
    async def start_step(
        self,
        pipeline_id: str,
        step_index: int,
        task_id: str,
        task_name: str,
    ) -> None:
        """Mark a step as started."""
        ...

    @abstractmethod
    async def complete_step(self, pipeline_id: str, step_index: int) -> None:
        """Mark a step as completed."""
        ...

    @abstractmethod
    async def fail_step(self, pipeline_id: str, step_index: int, error: str) -> None:
        """Mark a step as failed."""
        ...

    @abstractmethod
    async def get_pipeline_status(self, pipeline_id: str) -> PipelineStatusInfo | None:
        """Get status of a pipeline."""
        ...

    @abstractmethod
    async def list_pipelines(self, limit: int = 10) -> list[PipelineStatusInfo]:
        """List recent pipelines."""
        ...

    @abstractmethod
    async def cleanup_old(self, ttl_seconds: int = 3600) -> int:
        """Clean up old pipeline data. Returns number of cleaned items."""
        ...