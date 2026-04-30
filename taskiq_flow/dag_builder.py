"""DAG builder for automatic pipeline construction."""

import inspect
from collections.abc import Callable
from typing import Any

from taskiq import AsyncTaskiqDecoratedTask

from taskiq_flow.dataflow.dag import DAG
from taskiq_flow.dataflow.registry import DataflowRegistry
from taskiq_flow.decorators import get_pipeline_metadata


class DAGBuilder:
    """
    Builds a DAG from registered tasks.

    Analyzes task signatures and metadata to automatically
    determine data dependencies and execution order.
    """

    @staticmethod
    def from_tasks(
        tasks: list[Any],
        registry: DataflowRegistry | None = None,
    ) -> DAG:
        """
        Build a DAG from a list of tasks.

        Args:
            tasks: List of decorated tasks
            registry: Optional pre-populated registry

        Returns:
            A DAG representing task dependencies
        """
        if registry is None:
            registry = DataflowRegistry()

        # Register all tasks
        for task in tasks:
            DAGBuilder._register_task(task, registry)

        # Build and return DAG
        return registry.build_dag()

    @staticmethod
    def from_callable_tasks(
        tasks: list[Callable],
        registry: DataflowRegistry | None = None,
    ) -> DAG:
        """
        Build a DAG from callable tasks (decorated functions).

        Args:
            tasks: List of decorated callable tasks
            registry: Optional pre-populated registry

        Returns:
            A DAG representing task dependencies
        """
        if registry is None:
            registry = DataflowRegistry()

        # Register all tasks
        for task in tasks:
            DAGBuilder._register_callable_task(task, registry)

        # Build and return DAG
        return registry.build_dag()

    @staticmethod
    def _register_task(
        task: Any,
        registry: DataflowRegistry,
    ) -> None:
        """
        Register a single task with the registry.

        Extracts metadata from the task and registers it.
        """
        metadata = get_pipeline_metadata(task)

        if not metadata:
            # Task is not decorated with @pipeline_task
            # Skip or raise error
            return

        output = metadata.get("output")
        inputs = metadata.get("inputs")
        retries = metadata.get("retries", 0)

        if not output:
            # Try to infer from task name or signature
            output = DAGBuilder._infer_output_name(task)

        if inputs is None:
            # Infer from function signature
            inputs = DAGBuilder._infer_inputs(task)

        registry.register_task(
            task,
            output=output,
            inputs=inputs,
            retries=retries,
            task_name=task.task_name,
        )

    @staticmethod
    def _register_callable_task(
        task: Callable,
        registry: DataflowRegistry,
    ) -> None:
        """
        Register a callable task with the registry.

        Extracts metadata from the decorated function.
        """
        metadata = get_pipeline_metadata(task)

        if not metadata:
            # Not a pipeline task
            return

        output = metadata.get("output")
        inputs = metadata.get("inputs")
        retries = metadata.get("retries", 0)

        if not output:
            raise ValueError(f"Task {task.__name__} must specify output name")

        if inputs is None:
            # Infer from function signature
            inputs = DAGBuilder._infer_inputs_from_callable(task)

        # Note: We can't register the callable directly since it's not
        # an AsyncTaskiqDecoratedTask yet. This will be handled when
        # the task is actually decorated by TaskIQ.
        # For now, we store the metadata for later use.
        registry.register_task(
            task,  # This will be replaced with the actual task later
            output=output,
            inputs=inputs,
            retries=retries,
            task_name=task.__name__,
        )

    @staticmethod
    def _infer_output_name(task: Any) -> str:
        """Infer output name from task."""
        # Use task name as default
        return task.task_name

    @staticmethod
    def _infer_inputs(task: AsyncTaskiqDecoratedTask) -> list[str]:
        """
        Infer input names from task signature.

        Examines the task's function signature to determine
        which data inputs it requires.
        """
        try:
            # Try to get the original function
            func = getattr(task, "original_function", None)
            if func is None:
                # Try to get the wrapped function
                func = task

            # If it's still not a function, try to unwrap
            if not callable(func):
                return []

            sig = inspect.signature(func)
            params = list(sig.parameters.values())

            # Skip 'self' and 'cls' parameters
            input_names = []
            for param in params:
                if param.name not in ("self", "cls"):
                    # Check if it has a default value
                    # Parameters without defaults are required inputs
                    if param.default is inspect.Parameter.empty:
                        input_names.append(param.name)

            return input_names
        except (AttributeError, TypeError, ValueError):
            return []

    @staticmethod
    def _infer_inputs_from_callable(task: Callable) -> list[str]:
        """
        Infer input names from a callable.

        Examines the function signature to determine
        which data inputs it requires.
        """
        try:
            sig = inspect.signature(task)
            params = list(sig.parameters.values())

            # Skip 'self' and 'cls' parameters
            input_names = []
            for param in params:
                if param.name not in ("self", "cls"):
                    # Check if it has a default value
                    # Parameters without defaults are required inputs
                    if param.default is inspect.Parameter.empty:
                        input_names.append(param.name)

            return input_names
        except (AttributeError, TypeError):
            return []

    @staticmethod
    def validate_dag(dag: DAG) -> None:
        """
        Validate a DAG for common issues.

        Args:
            dag: The DAG to validate

        Raises:
            ValueError: If the DAG has issues
        """
        if not dag.nodes:
            raise ValueError("DAG has no nodes")

        # Check for circular dependencies
        try:
            dag.topological_sort()
        except ValueError as e:
            raise ValueError(f"Invalid DAG: {e}")

        # Check for disconnected nodes
        if len(dag.nodes) > 1 and not dag.edges:
            # Multiple nodes but no edges - might be intentional
            # (parallel execution from start)
            pass


class PipelineDAGBuilder(DAGBuilder):
    """
    DAG builder specifically for Pipeline class.

    Integrates with the Pipeline class to build DAGs from
    registered tasks.
    """

    def __init__(self, pipeline: Any):
        """
        Initialize the builder.

        Args:
            pipeline: The pipeline instance
        """
        self.pipeline = pipeline
        self.registry = DataflowRegistry()

    def build(self) -> DAG:
        """Build the DAG from pipeline tasks."""
        return self.registry.build_dag()
