"""Redis implementation of pipeline storage."""

import asyncio
import json
import logging
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any

try:
    import redis.asyncio as redis
except ImportError:
    redis = None  # type: ignore

from .models import PipelineStatus, PipelineStatusInfo, StepStatus, StepStatusInfo
from .storage import PipelineStorage

logger = logging.getLogger(__name__)


class RedisPipelineStorage(PipelineStorage):
    """Redis-based pipeline storage with retry logic."""

    async def _retry_operation(
        self,
        operation: Callable[..., Any],
        max_retries: int = 3,
        base_delay: float = 0.1,
    ) -> Any:
        """Execute operation with exponential backoff retry."""
        for attempt in range(max_retries):
            try:
                return await operation()
            except Exception as e:
                if attempt == max_retries - 1:
                    raise e

                delay = base_delay * (2**attempt)
                logger.warning(
                    f"Redis operation failed (attempt {attempt + 1}/"
                    f"{max_retries}): {e}. Retrying in {delay}s...",
                )
                await asyncio.sleep(delay)
        return None

    def __init__(
        self,
        redis_url: str = "redis://localhost:6379",
        ttl_seconds: int = 3600,
    ) -> None:
        if redis is None:
            raise ImportError("redis package is required for RedisPipelineStorage")
        self.redis = redis.from_url(redis_url)
        self.ttl_seconds = ttl_seconds
        self._index_key = "pipeline_index"  # Sorted set for pipeline listing

    async def create_pipeline(self, pipeline_id: str, total_steps: int) -> None:
        """Create a new pipeline with initial status."""
        try:
            pipeline_key = f"pipe:{pipeline_id}"
            steps_key = f"pipe:{pipeline_id}:steps"

            pipeline_data = {
                "pipeline_id": pipeline_id,
                "status": PipelineStatus.PENDING.value,
                "total_steps": total_steps,
                "current_step": 0,
                "created_at": datetime.now(timezone.utc).isoformat(),
            }

            # Initialize steps list with default values
            initial_steps = []
            for i in range(total_steps):
                step_data = {
                    "step_index": i,
                    "task_name": "",
                    "task_id": "",
                    "status": StepStatus.PENDING.value,
                    "retries": 0,
                }
                initial_steps.append(json.dumps(step_data))

            # Add to pipeline index with timestamp as score
            created_timestamp = datetime.now(timezone.utc).timestamp()

            async with self.redis.pipeline() as pipe:
                pipe.hset(pipeline_key, mapping=pipeline_data)
                pipe.expire(pipeline_key, self.ttl_seconds)
                pipe.delete(steps_key)  # Ensure clean start
                if initial_steps:
                    pipe.rpush(steps_key, *initial_steps)
                # Add to index (score = timestamp, member = pipeline_id)
                pipe.zadd(self._index_key, {pipeline_id: created_timestamp})
                await pipe.execute()

            logger.debug(f"Created pipeline {pipeline_id} with {total_steps} steps")
        except Exception as e:
            logger.error(f"Failed to create pipeline {pipeline_id}: {e}")
            raise

    async def start_pipeline(self, pipeline_id: str) -> None:
        """Mark pipeline as started."""
        try:
            pipeline_key = f"pipe:{pipeline_id}"
            started_at = datetime.now(timezone.utc).isoformat()

            await self.redis.hset(  # type: ignore[call-overload]
                pipeline_key,
                mapping={
                    "status": PipelineStatus.RUNNING.value,
                    "started_at": started_at,
                },
            )
            logger.debug(f"Started pipeline {pipeline_id}")
        except Exception as e:
            logger.error(f"Failed to start pipeline {pipeline_id}: {e}")
            raise

    async def complete_pipeline(self, pipeline_id: str, result: Any) -> None:
        """Mark pipeline as completed with result."""
        try:
            pipeline_key = f"pipe:{pipeline_id}"
            finished_at = datetime.now(timezone.utc).isoformat()

            await self.redis.hset(  # type: ignore[call-overload]
                pipeline_key,
                mapping={
                    "status": PipelineStatus.COMPLETED.value,
                    "finished_at": finished_at,
                    "result": json.dumps(result),
                },
            )
            logger.debug(f"Completed pipeline {pipeline_id}")
        except Exception as e:
            logger.error(f"Failed to complete pipeline {pipeline_id}: {e}")
            raise

    async def fail_pipeline(self, pipeline_id: str, error: str) -> None:
        """Mark pipeline as failed with error."""
        try:
            pipeline_key = f"pipe:{pipeline_id}"
            finished_at = datetime.now(timezone.utc).isoformat()

            await self.redis.hset(  # type: ignore[call-overload]
                pipeline_key,
                mapping={
                    "status": PipelineStatus.FAILED.value,
                    "finished_at": finished_at,
                    "error": error,
                },
            )
            logger.error(f"Failed pipeline {pipeline_id}: {error}")
        except Exception as e:
            logger.error(f"Failed to mark pipeline {pipeline_id} as failed: {e}")
            raise

    async def start_step(
        self,
        pipeline_id: str,
        step_index: int,
        task_id: str,
        task_name: str,
    ) -> None:
        """Mark a step as started."""
        try:
            steps_key = f"pipe:{pipeline_id}:steps"
            step_data = {
                "step_index": step_index,
                "task_name": task_name,
                "task_id": task_id,
                "status": StepStatus.RUNNING.value,
                "started_at": datetime.now(timezone.utc).isoformat(),
                "finished_at": None,
                "retries": 0,
                "error": None,
            }

            await self.redis.lset(  # type: ignore[call-overload]
                steps_key,
                step_index,
                json.dumps(step_data),
            )
            logger.debug(f"Started step {step_index} for pipeline {pipeline_id}")
        except Exception as e:
            logger.error(
                f"Failed to start step {step_index} for pipeline {pipeline_id}: {e}",
            )
            raise

    async def complete_step(self, pipeline_id: str, step_index: int) -> None:
        """Mark a step as completed."""
        steps_key = f"pipe:{pipeline_id}:steps"
        step_json = await self.redis.lindex(  # type: ignore[call-overload]
            steps_key,
            step_index,
        )
        if step_json:
            step_data = json.loads(step_json)
            step_data["status"] = StepStatus.COMPLETED.value
            step_data["finished_at"] = datetime.now(timezone.utc).isoformat()
            await self.redis.lset(  # type: ignore[call-overload]
                steps_key,
                step_index,
                json.dumps(step_data),
            )

    async def fail_step(self, pipeline_id: str, step_index: int, error: str) -> None:
        """Mark a step as failed."""
        steps_key = f"pipe:{pipeline_id}:steps"
        step_json = await self.redis.lindex(  # type: ignore[call-overload]
            steps_key,
            step_index,
        )
        if step_json:
            step_data = json.loads(step_json)
            step_data["status"] = StepStatus.FAILED.value
            step_data["finished_at"] = datetime.now(timezone.utc).isoformat()
            step_data["error"] = error
            await self.redis.lset(  # type: ignore[call-overload]
                steps_key,
                step_index,
                json.dumps(step_data),
            )

    async def get_pipeline_status(self, pipeline_id: str) -> PipelineStatusInfo | None:
        """Get status of a pipeline."""

        async def _get_status() -> Any:
            return await self._get_pipeline_status_impl(pipeline_id)

        try:
            return await self._retry_operation(_get_status)
        except Exception as e:
            logger.error(
                f"Failed to get pipeline status for {pipeline_id} after retries: {e}",
            )
            return None

    async def _get_pipeline_status_impl(
        self,
        pipeline_id: str,
    ) -> PipelineStatusInfo | None:
        """Internal implementation of get_pipeline_status."""
        pipeline_key = f"pipe:{pipeline_id}"
        steps_key = f"pipe:{pipeline_id}:steps"

        pipeline_data = await self.redis.hgetall(  # type: ignore[call-overload]
            pipeline_key,
        )
        if not pipeline_data:
            logger.debug(f"Pipeline {pipeline_id} not found")
            return None

        # Decode bytes to strings
        pipeline_data = {k.decode(): v.decode() for k, v in pipeline_data.items()}

        steps_json = await self.redis.lrange(  # type: ignore[call-overload]
            steps_key,
            0,
            -1,
        )
        steps: list[StepStatusInfo] = []
        for s in steps_json:
            if s:
                try:
                    steps.append(StepStatusInfo(**json.loads(s)))
                except (json.JSONDecodeError, ValueError) as e:
                    logger.warning(
                        f"Failed to parse step data for pipeline {pipeline_id}: {e}",
                    )
                    continue

        # Parse datetime fields with error handling
        try:
            created_at = (
                datetime.fromisoformat(pipeline_data["created_at"])
                if pipeline_data.get("created_at")
                else datetime.now(timezone.utc)
            )
        except ValueError:
            created_at = datetime.now(timezone.utc)

        try:
            started_at = (
                datetime.fromisoformat(pipeline_data["started_at"])
                if pipeline_data.get("started_at")
                else None
            )
        except ValueError:
            started_at = None

        try:
            finished_at = (
                datetime.fromisoformat(pipeline_data["finished_at"])
                if pipeline_data.get("finished_at")
                else None
            )
        except ValueError:
            finished_at = None

        # Parse result with error handling
        result: Any = None
        if pipeline_data.get("result"):
            try:
                result = json.loads(pipeline_data["result"])
            except (json.JSONDecodeError, ValueError):
                logger.warning(f"Failed to parse result for pipeline {pipeline_id}")
                result = pipeline_data["result"]  # Keep as string

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
        """List recent pipelines using Redis sorted set index."""
        try:
            # Get pipeline IDs from the index (most recent first)
            pipeline_ids = await self.redis.zrevrange(self._index_key, 0, limit - 1)

            if not pipeline_ids:
                return []

            pipelines = []
            for pipeline_id_bytes in pipeline_ids:
                pipeline_id = pipeline_id_bytes.decode()

                # Get full pipeline status
                status = await self.get_pipeline_status(pipeline_id)
                if status:
                    pipelines.append(status)

            return pipelines
        except Exception as e:
            logger.error(f"Failed to list pipelines: {e}")
            # Fallback: return empty list on error
            return []

    async def cleanup_old(self, ttl_seconds: int = 3600) -> int:
        """Clean up old pipeline data and update index."""
        try:
            # Calculate cutoff timestamp
            cutoff_timestamp = datetime.now(timezone.utc).timestamp() - ttl_seconds

            # Remove old entries from index
            removed_count = await self.redis.zremrangebyscore(
                self._index_key,
                0,
                cutoff_timestamp,
            )

            logger.debug(f"Cleaned up {removed_count} old pipeline entries from index")
            return removed_count
        except Exception as e:
            logger.error(f"Failed to cleanup old pipeline data: {e}")
            return 0
