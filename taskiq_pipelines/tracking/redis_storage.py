"""Redis implementation of pipeline storage."""

import json
from datetime import datetime, timedelta
from typing import Any

try:
    import redis.asyncio as redis
except ImportError:
    redis = None

from .models import PipelineStatus, PipelineStatusInfo, StepStatus, StepStatusInfo
from .storage import PipelineStorage


class RedisPipelineStorage(PipelineStorage):
    """Redis-based pipeline storage."""

    def __init__(self, redis_url: str = "redis://localhost:6379", ttl_seconds: int = 3600):
        if redis is None:
            raise ImportError("redis package is required for RedisPipelineStorage")
        self.redis = redis.from_url(redis_url)
        self.ttl_seconds = ttl_seconds

    async def create_pipeline(self, pipeline_id: str, total_steps: int) -> None:
        """Create a new pipeline with initial status."""
        pipeline_key = f"pipe:{pipeline_id}"
        steps_key = f"pipe:{pipeline_id}:steps"

        pipeline_data = {
            "pipeline_id": pipeline_id,
            "status": PipelineStatus.PENDING,
            "total_steps": total_steps,
            "current_step": 0,
            "created_at": datetime.utcnow().isoformat(),
            "started_at": None,
            "finished_at": None,
            "result": None,
            "error": None,
        }

        async with self.redis.pipeline() as pipe:
            pipe.hset(pipeline_key, mapping=pipeline_data)
            pipe.expire(pipeline_key, self.ttl_seconds)
            pipe.delete(steps_key)  # Ensure clean start
            await pipe.execute()

    async def start_pipeline(self, pipeline_id: str) -> None:
        """Mark pipeline as started."""
        pipeline_key = f"pipe:{pipeline_id}"
        started_at = datetime.utcnow().isoformat()

        await self.redis.hset(pipeline_key, mapping={
            "status": PipelineStatus.RUNNING,
            "started_at": started_at,
        })

    async def complete_pipeline(self, pipeline_id: str, result: Any) -> None:
        """Mark pipeline as completed with result."""
        pipeline_key = f"pipe:{pipeline_id}"
        finished_at = datetime.utcnow().isoformat()

        await self.redis.hset(pipeline_key, mapping={
            "status": PipelineStatus.COMPLETED,
            "finished_at": finished_at,
            "result": json.dumps(result),
        })

    async def fail_pipeline(self, pipeline_id: str, error: str) -> None:
        """Mark pipeline as failed with error."""
        pipeline_key = f"pipe:{pipeline_id}"
        finished_at = datetime.utcnow().isoformat()

        await self.redis.hset(pipeline_key, mapping={
            "status": PipelineStatus.FAILED,
            "finished_at": finished_at,
            "error": error,
        })

    async def start_step(
        self,
        pipeline_id: str,
        step_index: int,
        task_id: str,
        task_name: str,
    ) -> None:
        """Mark a step as started."""
        steps_key = f"pipe:{pipeline_id}:steps"
        step_data = {
            "step_index": step_index,
            "task_name": task_name,
            "task_id": task_id,
            "status": StepStatus.RUNNING,
            "started_at": datetime.utcnow().isoformat(),
            "finished_at": None,
            "retries": 0,
            "error": None,
        }

        await self.redis.lset(steps_key, step_index, json.dumps(step_data))

    async def complete_step(self, pipeline_id: str, step_index: int) -> None:
        """Mark a step as completed."""
        steps_key = f"pipe:{pipeline_id}:steps"
        step_json = await self.redis.lindex(steps_key, step_index)
        if step_json:
            step_data = json.loads(step_json)
            step_data["status"] = StepStatus.COMPLETED
            step_data["finished_at"] = datetime.utcnow().isoformat()
            await self.redis.lset(steps_key, step_index, json.dumps(step_data))

    async def fail_step(self, pipeline_id: str, step_index: int, error: str) -> None:
        """Mark a step as failed."""
        steps_key = f"pipe:{pipeline_id}:steps"
        step_json = await self.redis.lindex(steps_key, step_index)
        if step_json:
            step_data = json.loads(step_json)
            step_data["status"] = StepStatus.FAILED
            step_data["finished_at"] = datetime.utcnow().isoformat()
            step_data["error"] = error
            await self.redis.lset(steps_key, step_index, json.dumps(step_data))

    async def get_pipeline_status(self, pipeline_id: str) -> PipelineStatusInfo | None:
        """Get status of a pipeline."""
        pipeline_key = f"pipe:{pipeline_id}"
        steps_key = f"pipe:{pipeline_id}:steps"

        pipeline_data = await self.redis.hgetall(pipeline_key)
        if not pipeline_data:
            return None

        steps_json = await self.redis.lrange(steps_key, 0, -1)
        steps = [StepStatusInfo(**json.loads(s)) for s in steps_json if s]

        # Parse datetime fields
        created_at = datetime.fromisoformat(pipeline_data["created_at"]) if pipeline_data.get("created_at") else None
        started_at = datetime.fromisoformat(pipeline_data["started_at"]) if pipeline_data.get("started_at") else None
        finished_at = datetime.fromisoformat(pipeline_data["finished_at"]) if pipeline_data.get("finished_at") else None

        result = json.loads(pipeline_data["result"]) if pipeline_data.get("result") else None

        return PipelineStatusInfo(
            pipeline_id=pipeline_data["pipeline_id"],
            status=PipelineStatus(pipeline_data["status"]),
            total_steps=int(pipeline_data["total_steps"]),
            current_step=int(pipeline_data["current_step"]),
            created_at=created_at,
            started_at=started_at,
            finished_at=finished_at,
            result=result,
            error=pipeline_data.get("error"),
            steps=steps,
        )

    async def list_pipelines(self, limit: int = 10) -> list[PipelineStatusInfo]:
        """List recent pipelines. Note: This is a basic implementation."""
        # Redis doesn't have easy way to list all keys, this is simplified
        # In production, might need a separate index or use SCAN
        keys = await self.redis.keys("pipe:*:steps")
        pipeline_ids = [k.decode().replace("pipe:", "").replace(":steps", "") for k in keys]

        # Sort by creation time or something, but for now just take recent
        pipelines = []
        for pid in pipeline_ids[:limit]:
            status = await self.get_pipeline_status(pid)
            if status:
                pipelines.append(status)

        return pipelines

    async def cleanup_old(self, ttl_seconds: int = 3600) -> int:
        """Clean up old pipeline data. Redis handles TTL automatically."""
        # Since we set TTL on keys, Redis cleans up automatically
        # This method can be a no-op or count expired keys if needed
        return 0