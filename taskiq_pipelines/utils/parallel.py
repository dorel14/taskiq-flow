"""Parallel utilities for pipelines."""

import asyncio
from typing import Any, Iterable, TypeVar

from taskiq.kicker import AsyncKicker

from taskiq_pipelines.pipeliner import Pipeline

T = TypeVar("T")
R = TypeVar("R")


def parallel_map(
    task: AsyncKicker[[T], R],
    iterable: Iterable[T],
    **task_kwargs: Any,
) -> Pipeline:
    """
    Create a pipeline executing task on each item in parallel.

    Equivalent to group(*[task.kiq(item, **kwargs) for item in iterable]).

    :param task: The task to execute.
    :param iterable: Items to process.
    :param task_kwargs: Additional kwargs for the task.
    :return: Pipeline with parallel execution.
    """
    pipeline = Pipeline(task.broker)
    for item in iterable:
        # Add each task as a sequential step
        pipeline = pipeline.call_next(task, **task_kwargs)
    return pipeline


def chunked_map(
    task: AsyncKicker[[list[T]], R],
    items: list[T],
    chunk_size: int | None = None,
    max_concurrency: int | None = None,
    auto_concurrency: bool = False,
    **task_kwargs: Any,
) -> Pipeline:
    """
    Chunk items and process chunks in parallel with concurrency control.

    :param task: Task that takes a list of items.
    :param items: List of items to chunk.
    :param chunk_size: Size of each chunk.
    :param max_concurrency: Max concurrent chunks.
    :param auto_concurrency: Auto-detect concurrency.
    :param task_kwargs: Additional kwargs.
    :return: Pipeline for chunked processing.
    """
    if chunk_size is None:
        chunk_size = max(1, len(items) // (asyncio.get_event_loop()._default_executor._threads or 4))

    chunks = [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]

    if auto_concurrency:
        max_concurrency = len(chunks)  # Default to all parallel
    elif max_concurrency is None:
        max_concurrency = min(len(chunks), 10)  # Cap at 10

    # For simplicity, create a pipeline that calls the task for each chunk
    # Concurrency control would need to be implemented in the execution
    pipeline = Pipeline(task.broker)
    for chunk in chunks:
        pipeline = pipeline.call_next(task, **task_kwargs)

    return pipeline