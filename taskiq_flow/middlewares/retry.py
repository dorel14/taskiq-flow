"""Retry middleware for pipeline execution.

This module provides retry functionality leveraging TaskIQ's RetryMiddleware.

Author: SoniqueBay Team
Version: 0.4.5
"""

import asyncio
import logging
import secrets
from typing import Any

from taskiq_flow.metrics.collector import MetricsCollector

logger = logging.getLogger(__name__)


class PipelineRetryMiddleware:
    """
    Middleware for handling retries in pipeline execution.

    Integrates with TaskIQ's RetryMiddleware for broker-level retries.
    """

    def __init__(
        self,
        max_retries: int = 3,
        delay: float = 1.0,
        backoff: float = 2.0,
        jitter: bool = True,
        max_retry_time: int = 300,
    ) -> None:
        self.max_retries = max_retries
        self.delay = delay
        self.backoff = backoff
        self.jitter = jitter
        self.max_retry_time = max_retry_time
        self.retry_counts: dict[str, int] = {}

    async def on_task_failure(
        self,
        task_name: str,
        error: Exception,
        attempt: int,
    ) -> bool:
        """
        Handle task failure and determine if retry should occur.

        Returns:
            True if task should be retried, False otherwise
        """
        if attempt > self.max_retries:
            logger.error(
                "Retry exhausted for task",
                extra={
                    "task": task_name,
                    "attempts": attempt,
                    "error": str(error),
                },
            )
            return False

        # Calculate delay with exponential backoff and optional jitter
        wait_time = self.delay * (self.backoff ** (attempt - 1))
        if self.jitter:
            wait_time *= 1 + secrets.randbelow(100) / 1000.0

        logger.info(
            "Retrying task",
            extra={
                "task": task_name,
                "attempt": attempt,
                "wait_time": wait_time,
            },
        )

        # Record retry metric
        try:
            MetricsCollector().task_retried(task_name, type(error).__name__)
        except Exception:
            logger.debug("Failed to record retry metric (non-critical)")

        await asyncio.sleep(wait_time)
        return True

    def get_retry_state(self, task_name: str) -> dict[str, Any]:
        """Get retry state for a task."""
        return {
            "task_id": task_name,
            "attempt": self.retry_counts.get(task_name, 0),
            "max_attempts": self.max_retries + 1,
            "last_error": None,
            "next_retry_at": None,
            "total_retry_time": 0.0,
        }

    def reset_retry_count(self, task_name: str) -> None:
        """Reset retry count for a task."""
        self.retry_counts.pop(task_name, None)


async def calculate_exponential_backoff_delay(
    attempt: int,
    base_delay: float,
    backoff: float,
    jitter: bool,
) -> float:
    """Calculate delay for retry with exponential backoff."""
    delay = base_delay * (backoff ** (attempt - 1))
    if jitter:
        delay *= 1 + secrets.randbelow(100) / 1000.0
    return delay
