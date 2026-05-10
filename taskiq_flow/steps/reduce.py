"""Step de réduction pour agrégation cumulative.

Applique une fonction de réduction sur une liste de valeurs,
avec support de la pré-traitement par tâche et du chunking.

Auteur: SoniqueBay Team
Version: 1.0.2
"""

import asyncio
from typing import Any

import pydantic
from taskiq import (
    AsyncBroker,
    AsyncTaskiqDecoratedTask,
    Context,
    TaskiqDepends,
    TaskiqResult,
    async_shared_broker,
)
from taskiq.kicker import AsyncKicker

from taskiq_flow.abc import AbstractStep
from taskiq_flow.constants import (
    CURRENT_STEP,
    PIPELINE_DATA,
)
from taskiq_flow.exceptions import AbortPipeline
from taskiq_flow.shared_tasks import identity_task


@async_shared_broker.task(task_name="taskiq_flow.shared.reduce_tasks")
async def reduce_tasks(
    task_ids: list[str],
    initial: Any,
    reduce_func_name: str,
    context: Context = TaskiqDepends(),
) -> Any:
    """
    Reduces results from multiple tasks using a reduction function.

    It waits for all tasks from task_ids to complete and then reduces results
    by applying the specified reduction function cumulatively.

    :param task_ids: list of task ids.
    :param initial: initial value for reduction.
    :param reduce_func_name: name of the reduction function to apply.
    :param context: current execution context, defaults to default_context
    :return: reduced result.
    """
    ordered_ids = task_ids[:]
    tasks_set = set(task_ids)
    while tasks_set:
        for task_id in task_ids:
            if await context.broker.result_backend.is_result_ready(task_id):
                try:
                    tasks_set.remove(task_id)
                except LookupError:
                    continue
        if tasks_set:
            await asyncio.sleep(0.1)  # check_interval equivalent

    results = []
    for task_id in ordered_ids:
        result = await context.broker.result_backend.get_result(task_id)
        results.append(result.return_value)

    # Apply reduction function
    accumulator = initial if initial is not None else results[0] if results else None

    # Get the reduction function from safe builtins
    safe_functions = {
        "sum": lambda acc, x: acc + x,
        "max": lambda acc, x: max(acc, x) if acc is not None else x,
        "min": lambda acc, x: min(acc, x) if acc is not None else x,
        "concat": lambda acc, x: acc + x if acc is not None else x,
        "count": lambda acc, x: acc + 1,
    }

    reduce_func = safe_functions.get(reduce_func_name)
    if reduce_func is None:
        # If no function specified, just return the last result
        return results[-1] if results else accumulator

    # Apply reduction
    for item in results:
        accumulator = reduce_func(accumulator, item)

    return accumulator


class ReduceStep(pydantic.BaseModel, AbstractStep, step_name="reduce"):
    """
    Step de réduction cumulée sur une collection.

    Prend un itérable et réduit ses éléments en une seule valeur
    en appliquant éventuellement une tâche de pré-traitement à
    chaque élément, puis une fonction de réduction.

    Attributs:
        task: Tâche de pré-traitement optionnelle (SequentialStep)
        initial: Valeur initiale de l'accumulateur
        reduce_func: Fonction de réduction (
            "sum", "max", "min", "concat", "count", "last"
        )
    """

    task: Any  # SequentialStep for processing each item
    initial: Any | None = None
    reduce_func: str = (
        "last"  # Reduction function: "sum", "max", "min", "concat", "count", "last"
    )

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
            raise AbortPipeline(reason="Reduce step requires an iterable result")

        # Convert to list if it's not already
        items_list = list(items)
        if not items_list:
            # Empty iterable, return initial value or None
            result.return_value = self.initial
            return

        # Create a task for each item to process (optional preprocessing)
        sub_task_ids: list[str] = []

        # If a task is specified, apply it to each item first
        if hasattr(self.task, "task_name") and self.task.task_name:
            for item in items_list:
                kicker: AsyncKicker[Any, Any] = AsyncKicker(
                    task_name=getattr(self.task, "task_name", "reduce_task"),
                    broker=broker,
                    labels=getattr(self.task, "labels", {}),
                )

                # Determine parameter passing
                if hasattr(self.task, "param_name") and self.task.param_name:
                    # If the task expects a keyword argument
                    additional_kwargs = getattr(
                        self.task,
                        "additional_kwargs",
                        {},
                    ).copy()
                    additional_kwargs[self.task.param_name] = item
                    task = await kicker.kiq(**additional_kwargs)
                elif (
                    hasattr(self.task, "param_name") and self.task.param_name == -1
                ):  # EMPTY_PARAM_NAME
                    # If the task expects no arguments
                    task = await kicker.kiq()
                else:
                    # If the task expects the item as first argument
                    task = await kicker.kiq(
                        item,
                        **getattr(self.task, "additional_kwargs", {}),
                    )
                sub_task_ids.append(task.task_id)
        else:
            # No preprocessing task, create dummy tasks that just return the items
            for item in items_list:
                # Create a simple task that returns the item as-is
                task = await identity_task.kicker().with_broker(broker).kiq(item)
                sub_task_ids.append(task.task_id)

        # Wait for all tasks to complete and reduce results
        await (
            reduce_tasks.kicker()
            .with_task_id(task_id)
            .with_broker(broker)
            .with_labels(**{CURRENT_STEP: str(step_number), PIPELINE_DATA: pipe_data})
            .kiq(
                sub_task_ids,
                initial=self.initial,
                reduce_func_name=self.reduce_func,
            )
        )

    @classmethod
    def from_task(
        cls,
        task: AsyncKicker[Any, Any] | AsyncTaskiqDecoratedTask[Any, Any],
        initial: Any | None = None,
        reduce_func: str = "last",
        **additional_kwargs: Any,
    ) -> "ReduceStep":
        """
        Create new reduce step from task.

        :param task: task to execute for each item.
        :param initial: initial value for reduction.
        :param reduce_func: reduction function to apply
            ("sum", "max", "min", "concat", "count", "last").
        :param additional_kwargs: additional function's kwargs.
        :return: new reduce step.
        """
        return ReduceStep(
            task=task,
            initial=initial,
            reduce_func=reduce_func,
            **additional_kwargs,
        )
