"""Fonction reduce pour l'agrégation de résultats.

Ce module fournit la fonction `reduce` qui agrège une liste de valeurs
en un seul résultat en appliquant une fonction de réduction.
C'est un wrapper autour de MapReduce.reduce.

Auteur: SoniqueBay Team
Version: 0.3.2
"""

from collections.abc import Callable
from typing import Any

from taskiq import AsyncBroker

from taskiq_flow.map_reduce import MapReduce


async def reduce(
    broker: AsyncBroker,
    task: Callable[..., Any],
    inputs: list[Any],
    output: str,
    initial: Any = None,
    **kwargs: Any,
) -> Any:
    """
    Reduce a list of values to a single value.

    Applies a reduction function cumulatively to the items
    in the input list.

    Args:
        broker: TaskIQ broker for execution
        task: Reduction task to apply
        inputs: List of input values
        output: Name for the output
        initial: Initial value for reduction
        **kwargs: Additional kwargs to pass to the task

    Returns:
        Reduced value

    Example:
        result = await reduce(
            broker,
            aggregate_stats,
            item_results,
            output="aggregated",
            initial=0,
        )
    """
    return await MapReduce.reduce(
        broker,
        task,
        inputs,
        output,
        initial,
        **kwargs,
    )


__all__ = ["reduce"]
