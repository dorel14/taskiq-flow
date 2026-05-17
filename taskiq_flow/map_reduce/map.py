"""
Fonction map pour l'application parallèle d'une tâche.

Ce module fournit la fonction `map` qui applique une tâche à chaque
élément d'une liste en parallèle. C'est un wrapper autour de
MapReduce.map fournissant une interface simplifiée.

Auteur: SoniqueBay Team
Version: 1.0.2
"""

from collections.abc import Callable
from typing import Any

from taskiq import AsyncBroker

from taskiq_flow.map_reduce import MapReduce, MapResult


async def map(
    broker: AsyncBroker,
    task: Callable[..., Any],
    items: list[Any],
    output: str,
    param_name: str | None = None,
    max_parallel: int | None = None,
    **kwargs: Any,
) -> list[Any]:
    """
    Apply a task to each item in parallel.

    Creates parallel execution of the task for each item
    in the input list. Results are collected into a list.

    Args:
        broker: TaskIQ broker for execution
        task: Task to apply to each item
        items: List of items to process
        output: Name for the output list
        param_name: Parameter name to use for items
        max_parallel: Maximum parallel tasks (None = unlimited)
        **kwargs: Additional kwargs to pass to each task

    Returns:
        List of results

    Example:
        results = await map(
            broker,
            process_item,
            [1, 2, 3, 4, 5],
            output="processed_items",
            max_parallel=10,
        )

    """
    result: MapResult[Any] = await MapReduce.map(
        broker,
        task,
        items,
        output,
        param_name,
        max_parallel,
        **kwargs,
    )
    return result.results


__all__ = ["map"]
