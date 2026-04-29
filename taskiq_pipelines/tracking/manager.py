"""Pipeline tracking manager."""

from typing import Any

from taskiq import AsyncBroker

from .factory import TrackingStorageFactory
from .models import PipelineStatusInfo
from .storage import PipelineStorage


class PipelineTrackingManager:
    """Manager for pipeline tracking operations."""

    def __init__(self, storage: PipelineStorage | None = None) -> None:
        self.storage = storage

    def with_storage(self, storage: PipelineStorage) -> "PipelineTrackingManager":
        """Set the storage backend."""
        self.storage = storage
        return self

    def with_auto_storage(
        self,
        broker: AsyncBroker,
        redis_url: str | None = None,
        ttl_seconds: int = 3600,
    ) -> "PipelineTrackingManager":
        """Auto-detect and set storage based on broker."""
        self.storage = TrackingStorageFactory.create(broker, redis_url, ttl_seconds)
        return self

    async def initiate(self, pipeline_id: str, total_steps: int) -> None:
        """Initiate tracking for a new pipeline."""
        if self.storage:
            await self.storage.create_pipeline(pipeline_id, total_steps)

    async def mark_pipeline_started(self, pipeline_id: str) -> None:
        """Mark pipeline as started."""
        if self.storage:
            await self.storage.start_pipeline(pipeline_id)

    async def mark_pipeline_completed(self, pipeline_id: str, result: Any) -> None:
        """Mark pipeline as completed."""
        if self.storage:
            await self.storage.complete_pipeline(pipeline_id, result)

    async def mark_pipeline_failed(self, pipeline_id: str, error: str) -> None:
        """Mark pipeline as failed."""
        if self.storage:
            await self.storage.fail_pipeline(pipeline_id, error)

    async def mark_step_started(
        self,
        pipeline_id: str,
        step_index: int,
        task_id: str,
        task_name: str,
    ) -> None:
        """Mark step as started."""
        if self.storage:
            await self.storage.start_step(pipeline_id, step_index, task_id, task_name)

    async def mark_step_completed(self, pipeline_id: str, step_index: int) -> None:
        """Mark step as completed."""
        if self.storage:
            await self.storage.complete_step(pipeline_id, step_index)

    async def mark_step_failed(self, pipeline_id: str, step_index: int, error: str) -> None:
        """Mark step as failed."""
        if self.storage:
            await self.storage.fail_step(pipeline_id, step_index, error)

    async def get_status(self, pipeline_id: str) -> PipelineStatusInfo | None:
        """Get pipeline status."""
        if self.storage:
            return await self.storage.get_pipeline_status(pipeline_id)
        return None

    async def list_recent(self, limit: int = 10) -> list[PipelineStatusInfo]:
        """List recent pipelines."""
        if self.storage:
            return await self.storage.list_pipelines(limit)
        return []

    async def cleanup(self, ttl_seconds: int = 3600) -> int:
        """Clean up old data."""
        if self.storage:
            return await self.storage.cleanup_old(ttl_seconds)
        return 0
