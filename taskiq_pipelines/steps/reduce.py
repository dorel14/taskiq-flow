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
        pipe_data: bytes,
        result: TaskiqResult[Any],
    ) -> None:
        """Perform reduction."""
        items = result.return_value
        if not hasattr(items, '__iter__'):
            raise ValueError("Reduce step requires an iterable result")

        accumulator = self.initial
        for item in items:
            # Create a temporary result for each iteration
            temp_result = TaskiqResult(
                is_err=False,
                return_value=(accumulator, item),
                error=None,
                execution_time=0,
                log="Reduce iteration",
            )
            # Execute the aggregation task
            # This is simplified; in practice, might need to handle async properly
            # For now, assume the task takes (accumulator, item) and returns new accumulator
            # But since act is for the step itself, perhaps call the task's act
            await self.task.act(broker, step_number, parent_task_id, task_id, pipe_data, temp_result)
            # Update accumulator - this needs adjustment
            # Actually, this design is flawed; reduce should be handled differently

        # Pass final accumulator to next step - but how?
        # This step needs to be redesigned