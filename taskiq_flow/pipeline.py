"""Enhanced Pipeline class with dataflow support."""

from typing import Any

from taskiq import AsyncBroker
from taskiq.decor import AsyncTaskiqDecoratedTask

from taskiq_flow.dag_builder import DAGBuilder
from taskiq_flow.dataflow.dag import DAG
from taskiq_flow.dataflow.registry import DataflowRegistry
from taskiq_flow.execution_engine import ExecutionEngine
from taskiq_flow.map_reduce import MapReduce
from taskiq_flow.pipeliner import Pipeline as OriginalPipeline


class DataflowPipeline(OriginalPipeline[Any, Any]):
    """
    Enhanced Pipeline with dataflow-based orchestration.

    Extends the original Pipeline class with automatic DAG construction
    based on data dependencies, automatic parallelism, and map/reduce
    operations.

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
        self._data_cache: dict[str, Any] = {}
        self._is_dataflow_built: bool = False

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
        )

        # Execute
        outputs = await engine.execute(
            inputs=inputs,
            pipeline_id=self.pipeline_id,
        )

        return outputs

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
            **kwargs: Additional kwargs

        Returns:
            Self for chaining

        Example:
            pipeline.map(
                process_track,
                track_list,
                output="track_features"
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

        return self

    def reduce(
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
            **kwargs: Additional kwargs

        Returns:
            Self for chaining

        Example:
            pipeline.reduce(
                aggregate_features,
                "track_features",
                output="aggregated"
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

        return self

    async def kiq_map_reduce(
        self,
        **inputs: Any,
    ) -> Any:
        """
        Execute pipeline with map-reduce operations.

        Executes map operations in parallel, then reduces results.

        Args:
            **inputs: External inputs to the pipeline

        Returns:
            Final reduced result

        Example:
            result = await pipeline.kiq_map_reduce(
                track_list=tracks
            )
        """
        results: dict[str, Any] = {}

        # Execute map operations
        if hasattr(self, "_map_operations"):
            for op in self._map_operations:
                op_dict: dict[str, Any] = op  # type: ignore[assignment]
                result = await MapReduce.map(
                    self.broker,
                    op_dict["task"],
                    op_dict["items"],
                    op_dict["output"],
                    **op_dict["kwargs"],
                )
                results[op_dict["output"]] = result

        # Execute reduce operations
        if hasattr(self, "_reduce_operations"):
            for op in self._reduce_operations:
                op_dict_reduce: dict[str, Any] = op  # type: ignore[assignment]
                input_data = results.get(op_dict_reduce["input_name"], [])
                result = await MapReduce.reduce(
                    self.broker,
                    op_dict_reduce["task"],
                    input_data,
                    op_dict_reduce["output"],
                    **op_dict_reduce["kwargs"],
                )
                results[op_dict_reduce["output"]] = result

        return results

    def visualize(self) -> dict[str, Any]:
        """
        Visualize the pipeline DAG.

        Returns:
            JSON representation of DAG

        Example:
            viz = pipeline.visualize()
            print(json.dumps(viz, indent=2))
        """
        from taskiq_flow.visualization import DAGVisualizer

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
        from taskiq_flow.visualization import DAGVisualizer

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
        from taskiq_flow.visualization import DAGVisualizer

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
