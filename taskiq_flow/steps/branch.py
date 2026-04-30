"""Branch step for parallel execution of multiple branches."""

import asyncio
import logging
from typing import Any

import pydantic
from taskiq import AsyncBroker, TaskiqResult
from taskiq.kicker import AsyncKicker

from taskiq_flow.abc import AbstractStep

logger = logging.getLogger(__name__)


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
        if not self.branches:
            # No branches defined, just pass through the result
            return

        # Execute all branches in parallel
        async def execute_branch(branch_steps: list[Any]) -> Any:
            """Execute a single branch and return its result."""
            if not branch_steps:
                return result.return_value

            current_result = result.return_value

            # Execute each step in the branch sequentially
            for step_data in branch_steps:
                if isinstance(step_data, dict) and "task_name" in step_data:
                    # Create a task kicker for this step
                    kicker = AsyncKicker(
                        task_name=step_data["task_name"],
                        broker=broker,
                        labels=step_data.get("labels", {}),
                    )

                    # Determine parameter passing
                    param_name = step_data.get("param_name")
                    additional_kwargs = step_data.get("additional_kwargs", {})

                    try:
                        if param_name:
                            additional_kwargs[param_name] = current_result
                            task_result = await kicker.kiq(**additional_kwargs)
                        else:
                            task_result = await kicker.kiq(
                                current_result,
                                **additional_kwargs,
                            )

                        # Wait for the task to complete and get the result
                        task_result_data = await broker.result_backend.get_result(
                            task_result.task_id,
                        )
                        current_result = task_result_data.return_value
                    except Exception as e:
                        # If task fails, continue with current result
                        logger.warning(
                            "Task %s failed with error: %s",
                            step_data.get("task_name", "unknown"),
                            e,
                        )

            return current_result

        # Execute all branches concurrently
        branch_tasks = [execute_branch(branch) for branch in self.branches]
        branch_results = await asyncio.gather(*branch_tasks, return_exceptions=True)

        # Combine results - for now, use the result from the first successful branch
        # In a more sophisticated implementation, this could be configurable
        final_result = result.return_value
        for branch_result in branch_results:
            if not isinstance(branch_result, Exception) and branch_result is not None:
                final_result = branch_result
                break

        result.return_value = final_result
