"""Error handling for pipeline execution.

This module provides configurable failure behavior modes and error aggregation.

Author: SoniqueBay Team
Version: 1.0.2
"""

import logging
import time
from enum import Enum
from typing import Any

from taskiq_flow.dataflow.dag import DAGNode

logger = logging.getLogger(__name__)


class ErrorHandlingMode(Enum):
    """Error handling modes for pipeline execution."""

    FAIL_FAST = "fail_fast"
    CONTINUE_ON_ERROR = "continue_on_error"
    SKIP_FAILED = "skip_failed"
    DEAD_LETTER = "dead_letter"


class PipelineError:
    """Represents an error that occurred during task execution."""

    def __init__(
        self,
        task_name: str,
        error: Exception,
        timestamp: float,
        context: dict[str, Any],
    ) -> None:
        self.task_name = task_name
        self.error = error
        self.timestamp = timestamp
        self.context = context

    def __repr__(self) -> str:
        return f"PipelineError(task={self.task_name}, error={self.error})"


class PipelineErrorAggregator:
    """Aggregate errors during pipeline execution."""

    def __init__(self) -> None:
        self.errors: list[PipelineError] = []
        self.failed_tasks: set[str] = set()
        self.skipped_tasks: set[str] = set()

    def add_error(
        self,
        task: DAGNode,
        error: Exception,
        context: dict[str, Any],
    ) -> None:
        """Add an error to the aggregator."""
        self.errors.append(
            PipelineError(
                task_name=task.task_name,
                error=error,
                timestamp=time.time(),
                context=context,
            ),
        )
        self.failed_tasks.add(task.task_name)
        logger.error(
            "Task failed",
            extra={
                "task": task.task_name,
                "error": str(error),
            },
        )

    def mark_skipped(self, task_name: str) -> None:
        """Mark a task as skipped."""
        self.skipped_tasks.add(task_name)

    def clear(self) -> None:
        """Clear all errors."""
        self.errors.clear()
        self.failed_tasks.clear()
        self.skipped_tasks.clear()

    @property
    def has_errors(self) -> bool:
        """Check if there are any errors."""
        return len(self.errors) > 0

    def get_affected_branches(self) -> list[set[str]]:
        """Determine which branches are affected by failures."""
        # Simplified - returns failed task names
        return [self.failed_tasks]
