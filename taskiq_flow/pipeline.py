"""Enhanced Pipeline class with dataflow support."""

import logging
from typing import Any

from taskiq import AsyncBroker
from taskiq.decor import AsyncTaskiqDecoratedTask

from taskiq_flow.dag_builder import DAGBuilder
from taskiq_flow.dataflow.dag import DAG
from taskiq_flow.dataflow.registry import DataflowRegistry
from taskiq_flow.execution_engine import ExecutionEngine
from taskiq_flow.map_reduce import MapReduce
from taskiq_flow.pipeliner import Pipeline as OriginalPipeline
from taskiq_flow.scheduling.scheduler import LabelBasedScheduler
from taskiq_flow.visualization import DAGVisualizer

logger = logging.getLogger(__name__)


class DataflowPipeline(OriginalPipeline[Any, Any]):
    """
    Enhanced Pipeline with dataflow-based orchestration.

    Extends the original Pipeline class with automatic DAG construction
    based on data dependencies, automatic parallelism, and map/reduce
    operations.

    This class maintains full backward compatibility with the original
    Pipeline class while adding new dataflow capabilities.

    This class maintains full backward compatibility with the original
    Pipeline class while adding new dataflow capabilities.
    """

    def __init__(self, broker: AsyncBroker, *args: Any, **kwargs: Any) -> None:
        """
        Initialize the dataflow pipeline.

        Args:
            broker: TaskIQ broker
            *args: Passed to parent class
            **kwargs: Passed to parent class
        """
        super().__init__(broker, *args, **kwargs)

        # Dataflow-specific attributes
        self._registry: DataflowRegistry | None = None
        self._dag: DAG | None = None
        self._dataflow_tasks: list[AsyncTaskiqDecoratedTask[Any, Any]] = []
        self._registered_tasks: list[dict[str, Any]] = []
        self._data_cache: dict[str, Any] = {}
        self._is_dataflow_built: bool = False
        self._map_operations: list[dict[str, Any]] = []
        self._reduce_operations: list[dict[str, Any]] = []

    @classmethod
    def from_tasks(
        cls,
        broker: AsyncBroker,
        tasks: list[AsyncTaskiqDecoratedTask[Any, Any]],
    ) -> "DataflowPipeline":
        """
        Create a pipeline from decorated tasks.

        Automatically builds the DAG based on data dependencies
        declared via @pipeline_task decorator.

        Args:
            broker: TaskIQ broker
            tasks: List of decorated tasks

        Returns:
            DataflowPipeline instance

        Example:
            @broker.task
            @pipeline_task(output="audio_features")
            async def extract_audio(track_paths):
                ...

            @broker.task
            @pipeline_task(output="mir_features")
            async def compute_mir(audio_features):
                ...

            pipeline = DataflowPipeline.from_tasks(
                broker,
                [extract_audio, compute_mir]
            )
        """
        pipeline = cls(broker)
        pipeline._dataflow_tasks = tasks
        pipeline._build_dataflow_dag()
        return pipeline

    def _build_dataflow_dag(self) -> None:
        """
        Build the dataflow DAG from registered tasks.

        Analyzes task metadata and constructs a DAG representing
        data dependencies between tasks.
        """
        if self._dataflow_tasks:
            self._dag = DAGBuilder.from_tasks(
                self._dataflow_tasks,
            )
            self._is_dataflow_built = True

    def add_dataflow_task(
        self,
        task: AsyncTaskiqDecoratedTask[Any, Any],
    ) -> "DataflowPipeline":
        """
        Add a task to the dataflow pipeline.

        Args:
            task: Decorated task to add

        Returns:
            Self for chaining
        """
        self._dataflow_tasks.append(task)
        self._is_dataflow_built = False
        return self

    async def kiq_dataflow(
        self,
        **inputs: Any,
    ) -> Any:
        """
        Execute pipeline using dataflow orchestration.

        Automatically determines execution order based on data
        dependencies and executes tasks with maximum parallelism.

        Args:
            **inputs: External inputs to the pipeline

        Returns:
            Dictionary of all outputs

        Example:
            result = await pipeline.kiq_dataflow(
                track_paths=["track1.wav", "track2.wav"]
            )
            # result = {"audio_features": ..., "mir_features": ...}
        """
        if not self._is_dataflow_built:
            self._build_dataflow_dag()

        if not self._dag:
            raise ValueError("No DAG built. Add tasks first.")

        # Create execution engine
        engine = ExecutionEngine(
            broker=self.broker,
            dag=self._dag,
            fail_fast=self.options.fail_fast,
            continue_on_error=self.options.continue_on_error,
            skip_failed=self.options.skip_failed,
        )

        # Execute
        return await engine.execute(
            inputs=inputs,
            pipeline_id=self.pipeline_id,
        )

    def map(  # type: ignore[override]
        self,
        task: AsyncTaskiqDecoratedTask[Any, Any],
        items: list[Any],
        output: str,
        **kwargs: Any,
    ) -> "DataflowPipeline":
        """
        Add map operation to pipeline.

        Creates parallel execution of task for each item.

        Args:
            task: Task to apply
            items: Items to process
            output: Output name
            **kwargs: Additional kwargs (including chunk_config, max_parallel, etc.)

        Returns:
            Self for chaining

        Example:
            pipeline.map(
                process_track,
                track_list,
                output="track_features",
                chunk_config=ChunkConfig(chunk_size=50),
                max_parallel=10,
            )
        """
        # Store map operation for execution
        if not hasattr(self, "_map_operations"):
            self._map_operations = []

        self._map_operations.append(
            {
                "type": "map",
                "task": task,
                "items": items,
                "output": output,
                "kwargs": kwargs,
            },
        )

        logger.info(
            "Map operation added to pipeline",
            extra={
                "task_name": task.task_name,
                "output": output,
                "items_count": len(items) if hasattr(items, "__len__") else None,
                "chunk_config": kwargs.get("chunk_config"),
                "max_parallel": kwargs.get("max_parallel"),
            },
        )

        return self

    def reduce(  # type: ignore[override]
        self,
        task: AsyncTaskiqDecoratedTask[Any, Any],
        input_name: str,
        output: str,
        **kwargs: Any,
    ) -> "DataflowPipeline":
        """
        Add reduce operation to pipeline.

        Aggregates results from previous map operation.

        Args:
            task: Reduction task
            input_name: Input data name
            output: Output name
            **kwargs: Additional kwargs (including chunk_size, initial, etc.)

        Returns:
            Self for chaining

        Example:
            pipeline.reduce(
                aggregate_features,
                "track_features",
                output="aggregated",
                chunk_size=100,
                initial=0,
            )
        """
        # Store reduce operation for execution
        if not hasattr(self, "_reduce_operations"):
            self._reduce_operations = []

        self._reduce_operations.append(
            {
                "type": "reduce",
                "task": task,
                "input_name": input_name,
                "output": output,
                "kwargs": kwargs,
            },
        )

        logger.info(
            "Reduce operation added to pipeline",
            extra={
                "task_name": task.task_name,
                "input_name": input_name,
                "output": output,
            },
        )

        return self

    async def kiq_map_reduce(
        self,
        **inputs: Any,
    ) -> Any:
        """
        Execute pipeline with map-reduce operations.

        Executes map operations in parallel, then reduces results.
        Supports advanced features like chunking and progress tracking.

        Args:
            **inputs: External inputs to the pipeline

        Returns:
            Final reduced result

        Example:
            result = await pipeline.kiq_map_reduce(
                track_list=tracks
            )
        """
        logger.info(
            "Starting map-reduce pipeline execution",
            extra={
                "map_operations": len(getattr(self, "_map_operations", [])),
                "reduce_operations": len(getattr(self, "_reduce_operations", [])),
                "inputs": list(inputs.keys()),
            },
        )

        results: dict[str, Any] = {}

        # Execute map operations
        if hasattr(self, "_map_operations"):
            for op in self._map_operations:
                op_dict: dict[str, Any] = op  # type: ignore[assignment]
                # Use advanced map with chunking if configured
                kwargs = op_dict.get("kwargs", {})
                chunk_config = kwargs.get("chunk_config")
                max_parallel = kwargs.get("max_parallel")

                logger.info(
                    "Executing map operation",
                    extra={
                        "task_name": op_dict["task"].task_name,
                        "output": op_dict["output"],
                        "items_count": len(op_dict["items"]),
                        "has_chunk_config": chunk_config is not None,
                        "max_parallel": max_parallel,
                    },
                )

                if chunk_config:
                    # Use advanced map with chunking
                    map_result = await MapReduce.map(
                        self.broker,
                        op_dict["task"],
                        op_dict["items"],
                        output=op_dict["output"],
                        param_name=op_dict.get("param_name"),
                        max_parallel=max_parallel,
                        chunk_config=chunk_config,
                        **{
                            k: v
                            for k, v in kwargs.items()
                            if k not in ("chunk_config", "max_parallel")
                        },
                    )
                    results[op_dict["output"]] = map_result.results
                    # Store metadata for potential use
                    results[f"{op_dict['output']}_metadata"] = {
                        "items_processed": map_result.items_processed,
                        "duration": map_result.duration,
                        "success_rate": map_result.success_rate,
                        "errors": len(map_result.errors),
                    }

                    logger.info(
                        "Map operation completed",
                        extra={
                            "task_name": op_dict["task"].task_name,
                            "output": op_dict["output"],
                            "items_processed": map_result.items_processed,
                            "duration": map_result.duration,
                            "success_rate": map_result.success_rate,
                        },
                    )
                else:
                    # Standard map
                    result = await MapReduce.map(
                        self.broker,
                        op_dict["task"],
                        op_dict["items"],
                        output=op_dict["output"],
                        param_name=op_dict.get("param_name"),
                        max_parallel=max_parallel,
                        **kwargs,
                    )
                    results[op_dict["output"]] = result

                    logger.info(
                        "Standard map operation completed",
                        extra={
                            "task_name": op_dict["task"].task_name,
                            "output": op_dict["output"],
                        },
                    )

        # Execute reduce operations
        if hasattr(self, "_reduce_operations"):
            for op in self._reduce_operations:
                op_dict_reduce: dict[str, Any] = op  # type: ignore[assignment]
                input_data = results.get(op_dict_reduce["input_name"], [])
                kwargs = op_dict_reduce.get("kwargs", {})
                chunk_size = kwargs.get("chunk_size")
                initial = kwargs.get("initial")

                logger.info(
                    "Executing reduce operation",
                    extra={
                        "task_name": op_dict_reduce["task"].task_name,
                        "input_name": op_dict_reduce["input_name"],
                        "output": op_dict_reduce["output"],
                        "input_count": (
                            len(input_data) if hasattr(input_data, "__len__") else None
                        ),
                        "has_chunk_size": chunk_size is not None,
                    },
                )

                if chunk_size:
                    # Use chunked reduction
                    result = await MapReduce.reduce(
                        self.broker,
                        op_dict_reduce["task"],
                        input_data,
                        output=op_dict_reduce["output"],
                        chunk_size=chunk_size,
                        initial=initial if initial is not None else 0,
                        **{
                            k: v
                            for k, v in kwargs.items()
                            if k not in ("chunk_size", "initial")
                        },
                    )
                else:
                    # Standard reduction
                    # Ensure initial is in kwargs if not present
                    reduce_kwargs = kwargs.copy()
                    if "initial" not in reduce_kwargs:
                        reduce_kwargs["initial"] = 0

                    result = await MapReduce.reduce(
                        self.broker,
                        op_dict_reduce["task"],
                        input_data,
                        output=op_dict_reduce["output"],
                        **reduce_kwargs,
                    )

                results[op_dict_reduce["output"]] = result

                logger.info(
                    "Reduce operation completed",
                    extra={
                        "task_name": op_dict_reduce["task"].task_name,
                        "output": op_dict_reduce["output"],
                    },
                )

        logger.info(
            "Map-reduce pipeline execution completed",
            extra={
                "results_count": len(results),
                "result_keys": list(results.keys()),
            },
        )

        # Return the final result (last reduce output or first map output)
        if hasattr(self, "_reduce_operations") and self._reduce_operations:
            return results[self._reduce_operations[-1]["output"]]
        if hasattr(self, "_map_operations") and self._map_operations:
            return results[self._map_operations[-1]["output"]]
        return results

    async def schedule_with_labels(
        self,
        scheduler: "LabelBasedScheduler",
        label: str,
        cron: str | None = None,
        interval_seconds: int | None = None,
        **inputs: Any,
    ) -> str:
        """Schedule the pipeline with label-based scheduling.

        This method schedules the pipeline using TaskIQ LabelScheduleSource,
        which is a lightweight alternative to APScheduler.

        Args:
            scheduler: LabelBasedScheduler instance
            label: Unique label for this schedule
            cron: Cron expression (e.g., "0 9 * * *" for daily at 9 AM)
            interval_seconds: Interval in seconds (alternative to cron)
            **inputs: External inputs to the pipeline

        Returns:
            The schedule ID

        Raises:
            ValueError: If neither cron nor interval is specified
        """
        return await scheduler.schedule_with_label(
            pipeline=self,
            label=label,
            cron=cron,
            interval_seconds=interval_seconds,
            args=(),
            kwargs=inputs,
            enabled=True,
        )

    async def schedule_with_cron(
        self,
        scheduler: "LabelBasedScheduler",
        label: str,
        cron: str,
        **inputs: Any,
    ) -> str:
        """Schedule the pipeline with a cron expression.

        Args:
            scheduler: LabelBasedScheduler instance
            label: Unique label for this schedule
            cron: Cron expression (e.g., "0 9 * * *" for daily at 9 AM)
            **inputs: External inputs to the pipeline

        Returns:
            The schedule ID
        """
        return await scheduler.schedule_with_cron(
            pipeline=self,
            label=label,
            cron=cron,
            args=(),
            kwargs=inputs,
            enabled=True,
        )

    async def schedule_with_interval(
        self,
        scheduler: "LabelBasedScheduler",
        label: str,
        interval_seconds: int,
        **inputs: Any,
    ) -> str:
        """Schedule the pipeline with a fixed interval.

        Args:
            scheduler: LabelBasedScheduler instance
            label: Unique label for this schedule
            interval_seconds: Interval in seconds
            **inputs: External inputs to the pipeline

        Returns:
            The schedule ID
        """
        return await scheduler.schedule_with_interval(
            pipeline=self,
            label=label,
            interval_seconds=interval_seconds,
            args=(),
            kwargs=inputs,
            enabled=True,
        )

    async def kiq_map_reduce_advanced(
        self,
        map_task: AsyncTaskiqDecoratedTask[Any, Any],
        reduce_task: AsyncTaskiqDecoratedTask[Any, Any],
        items: list[Any],
        map_output: str = "mapped",
        reduce_output: str = "reduced",
        map_param_name: str | None = None,
        max_parallel: int | None = None,
        reduce_chunk_size: int | None = None,
        **kwargs: Any,
    ) -> Any:
        """
        Execute advanced map-reduce with full feature support.

        Combines map and reduce operations with support for:
        - Parallel map execution
        - Intelligent chunking
        - Chunked reduction
        - Progress tracking

        Args:
            map_task: Task to apply to each item
            reduce_task: Task to aggregate results
            items: List of items to process
            map_output: Name for map output
            reduce_output: Name for reduce output
            map_param_name: Parameter name for map items
            max_parallel: Maximum parallel map tasks
            reduce_chunk_size: Chunk size for reduction
            **kwargs: Additional kwargs (passed to both map and reduce tasks)

        Returns:
            Reduced result

        Example:
            result = await pipeline.kiq_map_reduce_advanced(
                extract_features,
                aggregate_features,
                track_list,
                max_parallel=10,
                reduce_chunk_size=100,
            )
        """
        return await MapReduce.map_reduce(
            self.broker,
            map_task,
            reduce_task,
            items,
            map_output=map_output,
            reduce_output=reduce_output,
            map_param_name=map_param_name,
            max_parallel=max_parallel,
            reduce_chunk_size=reduce_chunk_size,
            **kwargs,
        )

    async def kiq_map_sweep(
        self,
        task: AsyncTaskiqDecoratedTask[Any, Any],
        param_values: dict[str, list[Any]],
        output: str,
        max_parallel: int | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """
        Execute multi-dimensional parameter sweep.

        Creates a Cartesian product of all parameter values and
        executes the task for each combination.

        Args:
            task: Task to apply
            param_values: Dictionary mapping parameter names to lists of values
            output: Name for the output list
            max_parallel: Maximum parallel tasks
            **kwargs: Additional kwargs

        Returns:
            Dictionary with results and metadata

        Example:
            result = await pipeline.kiq_map_sweep(
                train_model,
                param_values={
                    "learning_rate": [0.01, 0.001, 0.0001],
                    "batch_size": [32, 64, 128],
                },
                output="experiments",
                max_parallel=5,
            )
        """
        map_result = await MapReduce.map_sweep(
            self.broker,
            task,
            param_values,
            output=output,
            max_parallel=max_parallel,
            **kwargs,
        )

        return {
            output: map_result.results,
            "metadata": {
                "items_processed": map_result.items_processed,
                "duration": map_result.duration,
                "success_rate": map_result.success_rate,
                "errors": len(map_result.errors),
            },
        }

    def visualize(self) -> dict[str, Any]:
        """
        Visualize the pipeline DAG.

        Returns:
            JSON representation of DAG

        Example:
            viz = pipeline.visualize()
            print(json.dumps(viz, indent=2))
        """
        if not self._dag:
            self._build_dataflow_dag()

        if not self._dag:
            raise ValueError("No DAG to visualize")

        return DAGVisualizer.to_json(self._dag)

    def visualize_dot(self) -> str:
        """
        Generate DOT format visualization.

        Returns:
            DOT format string

        Example:
            dot = pipeline.visualize_dot()
            print(dot)
        """
        if not self._dag:
            self._build_dataflow_dag()

        if not self._dag:
            raise ValueError("No DAG to visualize")

        return DAGVisualizer.to_dot(self._dag)

    def print_dag(self) -> None:
        """
        Print ASCII representation of DAG.

        Example:
            pipeline.print_dag()
        """
        if not self._dag:
            self._build_dataflow_dag()

        if not self._dag:
            raise ValueError("No DAG to print")

        DAGVisualizer.print_ascii(self._dag)


# Re-export the original Pipeline for backward compatibility
Pipeline = OriginalPipeline

__all__ = [
    "DataflowPipeline",
    "Pipeline",
]
