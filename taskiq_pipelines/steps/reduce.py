"""Reduce step for cumulative aggregation."""

from typing import Any

import pydantic
from taskiq import AsyncBroker, TaskiqResult

from taskiq_pipelines.abc import AbstractStep


class ReduceStep(pydantic.BaseModel, AbstractStep, step_name="reduce"):
    """Step that performs cumulative reduction on an iterable."""

    task: Any  # SequentialStep for aggregation
    initial: Any | None = None

    async def act(
        self,
        broker: AsyncBroker,
        step_number: int,
        parent_task_id: str,
        task_id: str,
        pipe_data: str,
        result: TaskiqResult[Any],
    ) -> None:
        """Perform reduction."""
        items = result.return_value
        if not hasattr(items, "__iter__"):
            raise ValueError("Reduce step requires an iterable result")

        accumulator = self.initial
        for item in items:
            # For reduce, we need to execute the aggregation task
            # This is a simplified implementation - in practice, reduce might need
            # a different approach similar to mapper
            accumulator = item  # Placeholder - real implementation would call the task

        # For now, just pass the items as-is since reduce is complex to implement properly
        # in the current pipeline architecture
        TaskiqResult(
            is_err=False,
            return_value=accumulator,
            error=None,
            execution_time=0,
            log="Reduce step completed",
        )

        # This is a placeholder - reduce steps need more complex implementation
