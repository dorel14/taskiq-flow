"""Group step for parallel execution of multiple independent tasks."""

import asyncio
import logging
from typing import Any

import pydantic
from taskiq import AsyncBroker, AsyncTaskiqDecoratedTask, TaskiqResult
from taskiq.kicker import AsyncKicker

from taskiq_flow.abc import AbstractStep

logger = logging.getLogger(__name__)


class GroupStep(pydantic.BaseModel, AbstractStep, step_name="group"):
    """Step that executes multiple independent tasks in parallel."""

    tasks: list[dict[str, Any]]  # List of task specifications

    async def act(
        self,
        broker: AsyncBroker,
        step_number: int,
        parent_task_id: str,
        task_id: str,
        pipe_data: str,
        result: TaskiqResult[Any],
    ) -> None:
        """Execute all tasks in parallel and collect results."""
        if not self.tasks:
            # No tasks to execute, pass through the result
            return

        # Create a task for each task specification
        async def execute_task(task_spec: dict[str, Any]) -> Any:
            """Execute a single task and return its result."""
            # Extract task specification
            task_name = task_spec.get("task_name")
            labels = task_spec.get("labels", {})
            param_name = task_spec.get("param_name")
            additional_kwargs = task_spec.get("additional_kwargs", {}).copy()

            if not task_name:
                raise ValueError("Task specification must include 'task_name'")

            # Create kicker for this task
            kicker: AsyncKicker[Any, Any] = AsyncKicker(
                task_name=task_name,
                broker=broker,
                labels=labels,
            )

            # Determine parameter passing
            if param_name:
                # If param_name is specified, use the previous result
                additional_kwargs[param_name] = result.return_value
                task_result = await kicker.kiq(**additional_kwargs)
            elif param_name is None and additional_kwargs:
                # If no param_name but kwargs provided, use them as-is
                task_result = await kicker.kiq(**additional_kwargs)
            else:
                # No parameters, just execute the task
                task_result = await kicker.kiq()

            # Wait for the task to complete and get the result
            try:
                task_result_data = await broker.result_backend.get_result(
                    task_result.task_id,
                )
                return task_result_data.return_value
            except Exception as e:
                # If task fails, return None
                logger.warning(
                    "Task %s failed with error: %s",
                    task_result.task_id,
                    e,
                )
                return None

        # Execute all tasks in parallel
        task_coroutines = [execute_task(task_spec) for task_spec in self.tasks]
        results = await asyncio.gather(*task_coroutines, return_exceptions=True)

        # Process results - replace exceptions with None
        processed_results: list[Any] = []
        for r in results:
            if isinstance(r, Exception):
                processed_results.append(None)
            else:
                processed_results.append(r)

        # Update the result with the list of all task results
        result.return_value = processed_results

    @classmethod
    def from_tasks(
        cls,
        tasks: list[AsyncKicker[Any, Any] | AsyncTaskiqDecoratedTask[Any, Any]],
        param_names: list[str | None] | None = None,
        **additional_kwargs: Any,
    ) -> "GroupStep":
        """
        Create a group step from a list of tasks.

        :param tasks: List of tasks to execute in parallel.
        :param param_names: Optional list of parameter names for each task.
                           If None, no parameters are passed.
        :param additional_kwargs: Additional kwargs to pass to each task.
        :return: New group step.
        """
        task_specs = []
        for i, task in enumerate(tasks):
            kicker = task.kicker() if hasattr(task, "kicker") else task  # type: ignore
            message = kicker._prepare_message()  # type: ignore
            labels = dict(message.labels)

            # Add retry/timeout labels if present
            retries = getattr(task, "retries", None)
            if retries is not None:
                labels["STEP_RETRIES"] = str(retries)
            timeout = getattr(task, "timeout", None)
            if timeout is not None:
                labels["STEP_TIMEOUT"] = str(timeout)
            retry_delay = getattr(task, "retry_delay", None)
            if retry_delay is not None:
                labels["STEP_RETRY_DELAY"] = str(retry_delay)

            # Determine param_name for this task
            param_name = None
            if param_names and i < len(param_names):
                param_name = param_names[i]

            task_spec = {
                "task_name": message.task_name,
                "labels": labels,
                "param_name": param_name,
                "additional_kwargs": additional_kwargs.get(message.task_name, {}),
            }
            task_specs.append(task_spec)

        return GroupStep(tasks=task_specs)

    @classmethod
    def from_task_dicts(
        cls,
        task_dicts: list[dict[str, Any]],
    ) -> "GroupStep":
        """
        Create a group step from task dictionaries.

        :param task_dicts: List of task specification dictionaries.
                          Each dict should have 'task_name' key.
        :return: New group step.
        """
        return GroupStep(tasks=task_dicts)
