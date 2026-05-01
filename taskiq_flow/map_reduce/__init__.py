"""Map-reduce operations for batch processing."""

import asyncio
import inspect
from collections.abc import Callable
from typing import Any

from taskiq import AsyncBroker
from taskiq.kicker import AsyncKicker


class MapReduce:
    """
    Map-reduce operations for batch processing.

    Provides map and reduce operations that can be integrated
    into dataflow pipelines.
    """

    @staticmethod
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
            results = await MapReduce.map(
                broker,
                process_item,
                [1, 2, 3, 4, 5],
                output="processed_items",
                max_parallel=10,
            )
        """
        if not items:
            return []

        # Determine parameter name
        if param_name is None:
            param_name = MapReduce._infer_parameter_name(task)

        # Create tasks for each item
        semaphore = asyncio.Semaphore(max_parallel) if max_parallel else None

        async def process_item(item: Any, index: int) -> Any:
            """Process a single item."""
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

        # Execute all tasks in parallel
        results = await asyncio.gather(
            *[process_item(item, i) for i, item in enumerate(items)],
            return_exceptions=True,
        )

        # Check for errors
        errors = [r for r in results if isinstance(r, Exception)]
        if errors:
            raise Exception(f"Map operation failed: {errors}")

        return list(results)

    @staticmethod
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

        # Execute reduction
        return await MapReduce._execute_task(
            broker,
            task,
            {"items": inputs, "initial": initial, **kwargs},
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
        **kwargs: Any,
    ) -> Any:
        """
        Execute map followed by reduce.

        Combines map and reduce operations into a single pipeline.

        Args:
            broker: TaskIQ broker for execution
            map_task: Task to apply to each item
            reduce_task: Task to aggregate results
            items: List of items to process
            map_output: Name for map output
            reduce_output: Name for reduce output
            map_param_name: Parameter name for map items
            max_parallel: Maximum parallel map tasks
            **kwargs: Additional kwargs

        Returns:
            Reduced result

        Example:
            result = await MapReduce.map_reduce(
                broker,
                extract_features,
                aggregate_features,
                track_list,
                max_parallel=10,
            )
        """
        # Execute map
        mapped = await MapReduce.map(
            broker,
            map_task,
            items,
            output=map_output,
            param_name=map_param_name,
            max_parallel=max_parallel,
            **kwargs,
        )

        # Execute reduce
        return await MapReduce.reduce(
            broker,
            reduce_task,
            mapped,
            output=reduce_output,
            **kwargs,
        )

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


class PipelineMapReduce:
    """
    Map-reduce operations integrated with Pipeline.

    Provides map and reduce methods that can be chained
    with other pipeline operations.
    """

    def __init__(self, pipeline: Any) -> None:
        """
        Initialize with a pipeline.

        Args:
            pipeline: Pipeline instance
        """
        self.pipeline = pipeline

    async def map(
        self,
        task: Callable[..., Any],
        items: list[Any],
        output: str,
        **kwargs: Any,
    ) -> Any:
        """
        Add map operation to pipeline.

        Args:
            task: Task to apply
            items: Items to process
            output: Output name
            **kwargs: Additional kwargs

        Returns:
            Pipeline result
        """
        # Store items in pipeline data
        self.pipeline._map_items = items
        self.pipeline._map_task = task
        self.pipeline._map_output = output

        # Execute map
        return await MapReduce.map(
            self.pipeline.broker,
            task,
            items,
            output,
            **kwargs,
        )

    async def reduce(
        self,
        task: Callable[..., Any],
        input_name: str,
        output: str,
        **kwargs: Any,
    ) -> Any:
        """
        Add reduce operation to pipeline.

        Args:
            task: Reduction task
            input_name: Input data name
            output: Output name
            **kwargs: Additional kwargs

        Returns:
            Reduced result
        """
        # Get input data from pipeline
        inputs = self.pipeline._data_cache.get(input_name, [])

        # Execute reduce
        return await MapReduce.reduce(
            self.pipeline.broker,
            task,
            inputs,
            output,
            **kwargs,
        )
