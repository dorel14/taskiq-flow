"""Branch step for parallel execution of multiple branches."""

import asyncio
from typing import Any

import pydantic
from taskiq import AsyncBroker, TaskiqResult

from taskiq_pipelines.abc import AbstractStep
from taskiq_pipelines.pipeliner import Pipeline


class BranchStep(pydantic.BaseModel, AbstractStep, step_name="branch"):
    """Step that executes multiple branches in parallel."""

    branches: list[list[Any]]  # List of lists of DumpedStep

    async def act(
        self,
        broker: AsyncBroker,
        step_number: int,
        parent_task_id: str,
        task_id: str,
        pipe_data: bytes,
        result: TaskiqResult[Any],
    ) -> None:
        """Execute branches in parallel."""
        async def run_branch(branch_steps):
            # Create a sub-pipeline for this branch
            sub_pipeline = Pipeline(broker)
            sub_pipeline.steps = branch_steps
            # Execute with the same input
            return await sub_pipeline.kiq(result.return_value)

        # Run all branches concurrently
        tasks = [run_branch(branch) for branch in self.branches]
        branch_results = await asyncio.gather(*tasks, return_exceptions=True)

        # For now, pass the list of results to the next step
        # In a more advanced implementation, could combine or select
        combined_result = TaskiqResult(
            is_err=False,
            return_value=branch_results,
            error=None,
            execution_time=0,
            log="Branch step completed",
        )

        # Since this is a step, we need to proceed to next, but since it's a step,
        # the middleware will handle the next step with this result
        # For branch, we might need to adjust the flow
        # For simplicity, treat as if it completed and pass combined result