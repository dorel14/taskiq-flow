"""Branch step for parallel execution of multiple branches."""

import asyncio
from typing import Any

import pydantic
from taskiq import AsyncBroker, TaskiqResult

from taskiq_pipelines.abc import AbstractStep


class BranchStep(pydantic.BaseModel, AbstractStep, step_name="branch"):
    """Step that executes multiple branches in parallel."""

    branches: list[list[Any]]  # List of lists of DumpedStep

    async def act(
        self,
        broker: AsyncBroker,
        step_number: int,
        parent_task_id: str,
        task_id: str,
        pipe_data: str,
        result: TaskiqResult[Any],
    ) -> None:
        """Execute branches in parallel."""
        # Import here to avoid circular import
        # TODO: Implement proper branch execution
        # For now, this is a placeholder implementation

        # Placeholder: just pass the original result to next step
        pass
