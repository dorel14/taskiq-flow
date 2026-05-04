"""Opérations map-reduce pour le traitement par lots.

Ce module fournit des opérations de map et reduce en parallèle
avec des fonctionnalités avancées: chunking intelligent, suivi
de progression, et paramètres configurables. Ces opérations
sont intégrées dans les pipelines dataflow via les steps
MapperStep et ReduceStep.

Auteur: SoniqueBay Team
Version: 0.3.1
"""

import asyncio
import inspect
import itertools
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Generic, TypeVar

from taskiq import AsyncBroker
from taskiq.kicker import AsyncKicker

logger = logging.getLogger(__name__)

# Type variables for generic map-reduce
T = TypeVar("T")  # Input type
R = TypeVar("R")  # Result type
A = TypeVar("A")  # Accumulator type


@dataclass
class MapResult(Generic[R]):
    """Result of a map operation."""

    results: list[R]
    output_name: str
    items_processed: int
    duration: float
    errors: list[Exception] = field(default_factory=list)

    @property
    def success_rate(self) -> float:
        """Calculate success rate of map operation."""
        if self.items_processed == 0:
            return 0.0
        return (self.items_processed - len(self.errors)) / self.items_processed


@dataclass
class ChunkConfig:
    """Configuration for intelligent chunking."""

    chunk_size: int = 100
    max_chunks: int | None = None
    adaptive: bool = False
    min_chunk_size: int = 10
    max_chunk_size: int = 1000

    def calculate_chunks(self, items: list[Any]) -> list[list[Any]]:
        """
        Calculate optimal chunking for given items.

        Args:
            items: List of items to chunk

        Returns:
            List of chunks
        """
        if not items:
            return []

        size = self.chunk_size
        if self.adaptive:
            # Adjust chunk size based on total items
            total = len(items)
            if total < 100:
                size = max(self.min_chunk_size, total // 2)
            elif total > 10000:
                size = min(self.max_chunk_size, max(self.min_chunk_size, total // 100))

        # Create chunks
        chunks = []
        for i in range(0, len(items), size):
            chunk = items[i : i + size]
            chunks.append(chunk)
            if self.max_chunks and len(chunks) >= self.max_chunks:
                break

        return chunks


class MapReduce:
    """
    Opérations map-reduce pour le traitement par lots.

    Fournit des méthodes statiques pour:
    - map: application parallèle d'une tâche à une liste
    - reduce: agrégation cumulative d'une liste
    - map_reduce: pipeline map+reduce complet
    - map_sweep: balayage multi-dimensionnel de paramètres

    Caractéristiques:
    - Gestion automatique de la concurrence via asyncio
    - Support du chunking pour grands volumes
    - Callbacks de progression
    - Limitation de parallélisme (max_parallel)
    - Collecte des erreurs avec rapports de succès

    Exemple map-reduce:
        result = await MapReduce.map_reduce(
            broker,
            extract_features,    # map task
            aggregate,           # reduce task
            items,
            max_parallel=10,
            reduce_chunk_size=100
        )
    """

    @staticmethod
    async def map(
        broker: AsyncBroker,
        task: Callable[..., Any],
        items: list[Any],
        output: str,
        param_name: str | None = None,
        max_parallel: int | None = None,
        chunk_config: ChunkConfig | None = None,
        progress_callback: Callable[[int, int], None] | None = None,
        **kwargs: Any,
    ) -> MapResult[Any]:
        """
        Apply a task to each item in parallel with advanced features.

        Creates parallel execution of the task for each item
        in the input list. Results are collected into a list.
        Supports intelligent chunking and progress tracking.

        Args:
            broker: TaskIQ broker for execution
            task: Task to apply to each item
            items: List of items to process
            output: Name for the output list
            param_name: Parameter name to use for items
            max_parallel: Maximum parallel tasks (None = unlimited)
            chunk_config: Configuration for intelligent chunking
            progress_callback: Optional callback for progress updates
            **kwargs: Additional kwargs to pass to each task

        Returns:
            MapResult with results and metadata

        Example:
            chunk_config = ChunkConfig(chunk_size=50, adaptive=True)
            result = await MapReduce.map(
                broker,
                process_item,
                items,
                output="processed_items",
                max_parallel=10,
                chunk_config=chunk_config,
                progress_callback=lambda done, total: print(f"{done}/{total}"),
            )
        """
        start_time = time.time()

        if not items:
            return MapResult(
                results=[],
                output_name=output,
                items_processed=0,
                duration=0.0,
            )

        # Determine parameter name
        if param_name is None:
            param_name = MapReduce._infer_parameter_name(task)

        # Apply chunking if configured
        if chunk_config:
            chunks = chunk_config.calculate_chunks(items)
            if len(chunks) > 1:
                logger.info(
                    "[MAP] Processing %d items in %d chunks (size=%d)",
                    len(items),
                    len(chunks),
                    chunk_config.chunk_size,
                )
                # Process chunks in parallel
                chunk_results, chunk_errors = await MapReduce._map_chunks(
                    broker,
                    task,
                    chunks,
                    output,
                    param_name,
                    max_parallel,
                    progress_callback,
                    **kwargs,
                )
                # Flatten results
                results = [r for chunk in chunk_results for r in chunk]
                duration = time.time() - start_time
                return MapResult(
                    results=results,
                    output_name=output,
                    items_processed=len(items),
                    duration=duration,
                    errors=chunk_errors,
                )

        # Standard parallel map without chunking
        semaphore = asyncio.Semaphore(max_parallel) if max_parallel else None
        errors: list[Exception] = []

        async def process_item(item: Any, index: int) -> Any:
            """Process a single item."""
            try:
                if semaphore:
                    async with semaphore:
                        return await MapReduce._execute_task(
                            broker,
                            task,
                            {param_name: item},
                            **kwargs,
                        )
                else:
                    return await MapReduce._execute_task(
                        broker,
                        task,
                        {param_name: item},
                        **kwargs,
                    )
            except Exception as e:
                errors.append(e)
                logger.warning("[MAP] Item %d failed: %s", index, str(e))
                raise

        # Execute all tasks in parallel with progress tracking
        total_items = len(items)
        completed = 0

        async def process_with_progress(item: Any, index: int) -> Any:
            """Process item and update progress."""
            nonlocal completed
            try:
                result = await process_item(item, index)
                completed += 1
                if progress_callback:
                    progress_callback(completed, total_items)
                return result
            except Exception:
                completed += 1
                if progress_callback:
                    progress_callback(completed, total_items)
                raise

        results = await asyncio.gather(
            *[process_with_progress(item, i) for i, item in enumerate(items)],
            return_exceptions=True,
        )

        # Filter out exceptions
        final_results: list[Any] = []
        for r in results:
            if isinstance(r, Exception):
                if r not in errors:
                    errors.append(r)
            else:
                final_results.append(r)

        duration = time.time() - start_time

        logger.info(
            "[MAP] Completed %d items in %.2fs (%.1f items/s)",
            len(items),
            duration,
            len(items) / duration if duration > 0 else 0,
        )

        return MapResult(
            results=final_results,
            output_name=output,
            items_processed=len(items),
            duration=duration,
            errors=errors,
        )

    @staticmethod
    async def _map_chunks(
        broker: AsyncBroker,
        task: Callable[..., Any],
        chunks: list[list[Any]],
        output: str,
        param_name: str,
        max_parallel: int | None,
        progress_callback: Callable[[int, int], None] | None,
        **kwargs: Any,
    ) -> tuple[list[list[Any]], list[Exception]]:
        """
        Process multiple chunks in parallel.

        Args:
            broker: TaskIQ broker
            task: Task to execute
            chunks: List of chunks to process
            output: Output name
            param_name: Parameter name
            max_parallel: Max parallel tasks
            progress_callback: Progress callback
            **kwargs: Additional kwargs

        Returns:
            Tuple of (list of chunk results, list of errors from failed chunks)
        """
        semaphore = asyncio.Semaphore(max_parallel) if max_parallel else None
        total_chunks = len(chunks)
        completed_chunks = 0
        errors: list[Exception] = []

        async def process_chunk(chunk: list[Any], chunk_idx: int) -> list[Any]:
            """Process a single chunk."""
            nonlocal completed_chunks
            chunk_results = []

            async def process_item(item: Any, item_idx: int) -> Any:
                if semaphore:
                    async with semaphore:
                        return await MapReduce._execute_task(
                            broker,
                            task,
                            {param_name: item},
                            **kwargs,
                        )
                else:
                    return await MapReduce._execute_task(
                        broker,
                        task,
                        {param_name: item},
                        **kwargs,
                    )

            for i, item in enumerate(chunk):
                try:
                    result = await process_item(item, i)
                    chunk_results.append(result)
                except Exception as e:
                    logger.warning(
                        "[MAP] Chunk %d item %d failed: %s",
                        chunk_idx,
                        i,
                        str(e),
                    )
                    raise

            completed_chunks += 1
            if progress_callback:
                progress_callback(completed_chunks, total_chunks)

            return chunk_results

        chunk_tasks = [process_chunk(chunk, i) for i, chunk in enumerate(chunks)]
        results = await asyncio.gather(*chunk_tasks, return_exceptions=True)
        # Filter out exceptions and return only successful results
        final_results: list[list[Any]] = []
        for r in results:
            if isinstance(r, Exception):
                logger.warning("[MAP] Chunk failed: %s", str(r))
                # Track the error instead of silently returning empty list
                errors.append(r)
                final_results.append([])
            elif isinstance(r, list):
                final_results.append(r)
        return final_results, errors

    @staticmethod
    async def map_sweep(
        broker: AsyncBroker,
        task: Callable[..., Any],
        param_values: dict[str, list[Any]],
        output: str,
        max_parallel: int | None = None,
        **kwargs: Any,
    ) -> MapResult[Any]:
        """
        Multi-dimensional map (sweep) over multiple parameters.

        Creates a Cartesian product of all parameter values and
        executes the task for each combination.

        Args:
            broker: TaskIQ broker for execution
            task: Task to apply
            param_values: Dictionary mapping parameter names to lists of values
            output: Name for the output list
            max_parallel: Maximum parallel tasks
            **kwargs: Additional kwargs to pass to each task

        Returns:
            MapResult with results and metadata

        Example:
            results = await MapReduce.map_sweep(
                broker,
                train_model,
                param_values={
                    "learning_rate": [0.01, 0.001, 0.0001],
                    "batch_size": [32, 64, 128],
                },
                output="experiments",
                max_parallel=5,
            )
        """
        start_time = time.time()

        # Generate all combinations
        param_names = list(param_values.keys())
        value_lists = list(param_values.values())
        combinations = list(itertools.product(*value_lists))

        logger.info(
            "[SWEEP] Running %d combinations for parameters: %s",
            len(combinations),
            param_names,
        )

        semaphore = asyncio.Semaphore(max_parallel) if max_parallel else None
        errors: list[Exception] = []

        async def process_combination(
            combination: tuple[Any, ...],
            index: int,
        ) -> Any:
            """Process a single parameter combination."""
            try:
                # Build input dict
                inputs = dict(zip(param_names, combination, strict=False))
                inputs.update(kwargs)

                if semaphore:
                    async with semaphore:
                        return await MapReduce._execute_task(
                            broker,
                            task,
                            inputs,
                        )
                else:
                    return await MapReduce._execute_task(
                        broker,
                        task,
                        inputs,
                    )
            except Exception as e:
                errors.append(e)
                logger.warning(
                    "[SWEEP] Combination %d failed: %s",
                    index,
                    str(e),
                )
                raise

        results = await asyncio.gather(
            *[process_combination(comb, i) for i, comb in enumerate(combinations)],
            return_exceptions=True,
        )

        # Filter out exceptions
        final_results: list[Any] = []
        for r in results:
            if isinstance(r, Exception):
                if r not in errors:
                    errors.append(r)
            else:
                final_results.append(r)

        duration = time.time() - start_time

        logger.info(
            "[SWEEP] Completed %d combinations in %.2fs",
            len(combinations),
            duration,
        )

        return MapResult(
            results=final_results,
            output_name=output,
            items_processed=len(combinations),
            duration=duration,
            errors=errors,
        )

    @staticmethod
    async def reduce(
        broker: AsyncBroker,
        task: Callable[..., Any],
        inputs: list[Any],
        output: str,
        initial: Any = None,
        chunk_size: int | None = None,
        **kwargs: Any,
    ) -> Any:
        """
        Reduce a list of values to a single value with optional chunking.

        Applies a reduction function cumulatively to the items
        in the input list. Supports chunked reduction for large lists.

        Args:
            broker: TaskIQ broker for execution
            task: Reduction task to apply
            inputs: List of input values
            output: Name for the output
            initial: Initial value for reduction (default: None)
            chunk_size: If provided, perform chunked reduction
            **kwargs: Additional kwargs to pass to the task

        Returns:
            Reduced value

        Example:
            result = await MapReduce.reduce(
                broker,
                aggregate_stats,
                item_results,
                output="aggregated",
                initial=0,
            )
        """
        if not inputs:
            return initial

        # Infer the parameter name from the task signature
        param_name = MapReduce._infer_parameter_name(task)

        # Build the inputs dict with the correct parameter name
        task_inputs = {param_name: inputs, **kwargs}

        # Chunked reduction for large lists
        if chunk_size and len(inputs) > chunk_size:
            logger.info(
                "[REDUCE] Performing chunked reduction of %d items",
                len(inputs),
            )
            chunks = [
                inputs[i : i + chunk_size] for i in range(0, len(inputs), chunk_size)
            ]

            # Reduce each chunk
            chunk_results = []
            for i, chunk in enumerate(chunks):
                chunk_inputs = {param_name: chunk, **kwargs}
                result = await MapReduce._execute_task(
                    broker,
                    task,
                    chunk_inputs,
                )
                chunk_results.append(result)
                logger.debug("[REDUCE] Chunk %d reduced", i)

            # Final reduction of chunk results
            final_inputs = {param_name: chunk_results, **kwargs}
            return await MapReduce._execute_task(
                broker,
                task,
                final_inputs,
            )

        # Standard reduction
        return await MapReduce._execute_task(
            broker,
            task,
            task_inputs,
        )

    @staticmethod
    async def map_reduce(
        broker: AsyncBroker,
        map_task: Callable[..., Any],
        reduce_task: Callable[..., Any],
        items: list[Any],
        map_output: str = "mapped",
        reduce_output: str = "reduced",
        map_param_name: str | None = None,
        max_parallel: int | None = None,
        reduce_chunk_size: int | None = None,
        **kwargs: Any,
    ) -> Any:
        """
        Execute map followed by reduce with advanced options.

        Combines map and reduce operations into a single pipeline
        with support for parallel map and chunked reduction.

        Args:
            broker: TaskIQ broker for execution
            map_task: Task to apply to each item
            reduce_task: Task to aggregate results
            items: List of items to process
            map_output: Name for map output
            reduce_output: Name for reduce output
            map_param_name: Parameter name for map items
            max_parallel: Maximum parallel map tasks
            reduce_chunk_size: Chunk size for reduction (None = no chunking)
            **kwargs: Additional kwargs passed to both map and reduce tasks

        Returns:
            Reduced result

        Example:
            result = await MapReduce.map_reduce(
                broker,
                extract_features,
                aggregate_features,
                track_list,
                max_parallel=10,
                reduce_chunk_size=100,
            )
        """
        logger.info(
            "[MAP-REDUCE] Starting pipeline: %d items",
            len(items),
        )

        # Separate reduce-specific kwargs from map kwargs
        reduce_kwargs = {}
        map_kwargs = {}

        # Extract reduce-specific parameters
        if "initial" in kwargs:
            reduce_kwargs["initial"] = kwargs.pop("initial")
        else:
            # Default initial value for reduction
            reduce_kwargs["initial"] = 0

        # Remaining kwargs go to both map and reduce
        map_kwargs.update(kwargs)
        reduce_kwargs.update(kwargs)

        # Execute map
        map_result = await MapReduce.map(
            broker,
            map_task,
            items,
            output=map_output,
            param_name=map_param_name,
            max_parallel=max_parallel,
            **map_kwargs,
        )

        logger.info(
            "[MAP-REDUCE] Map completed: %d results (%.1f%% success)",
            len(map_result.results),
            map_result.success_rate * 100,
        )

        if not map_result.results:
            logger.warning("[MAP-REDUCE] No results from map phase")
            return None

        # Execute reduce - pass results and reduce-specific kwargs
        reduced = await MapReduce.reduce(
            broker,
            reduce_task,
            map_result.results,
            output=reduce_output,
            chunk_size=reduce_chunk_size,
            **reduce_kwargs,
        )

        logger.info("[MAP-REDUCE] Pipeline completed")

        return reduced

    @staticmethod
    async def _execute_task(
        broker: AsyncBroker,
        task: Callable[..., Any],
        inputs: dict[str, Any],
        **kwargs: Any,
    ) -> Any:
        """
        Execute a single task via TaskIQ.

        Args:
            broker: TaskIQ broker
            task: Task to execute
            inputs: Input parameters
            **kwargs: Additional kwargs

        Returns:
            Task result

        Raises:
            Exception: If task execution fails
        """
        # Create kicker
        kicker: AsyncKicker[Any, Any] = AsyncKicker(
            task_name=getattr(task, "task_name", "unknown"),
            broker=broker,
            labels={},
        )

        # Execute task
        taskiq_task = await kicker.kiq(**inputs, **kwargs)

        # Wait for result
        result = await taskiq_task.wait_result()

        if result.is_err:
            raise Exception(f"Task failed: {result.error}")

        return result.return_value

    @staticmethod
    def _infer_parameter_name(task: Callable[..., Any]) -> str:
        """
        Infer parameter name from task signature.

        Args:
            task: Task to inspect

        Returns:
            Parameter name to use
        """
        try:
            sig = inspect.signature(task)
            params = list(sig.parameters.keys())

            # Skip 'self' and 'cls'
            params = [p for p in params if p not in ("self", "cls")]

            # Return first parameter
            if params:
                return params[0]
        except (AttributeError, TypeError, ValueError):
            pass

        return "item"
