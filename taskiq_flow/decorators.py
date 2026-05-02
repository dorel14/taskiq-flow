"""Decorators for pipeline tasks."""

import inspect
from collections.abc import Callable
from dataclasses import dataclass, field
from functools import wraps
from typing import Any


@dataclass
class PipelineTaskMetadata:
    """Metadata for a pipeline task."""

    output: str
    inputs: list[str] | None = None
    retries: int = 0
    task_name: str = ""
    is_pipeline_task: bool = True
    multiple_outputs: bool = False
    output_types: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate metadata after initialization."""
        if not self.output:
            raise ValueError("Pipeline task must specify an output name")

        if self.retries < 0:
            raise ValueError("Retries must be non-negative")

        if self.inputs is not None and not isinstance(self.inputs, list):
            raise ValueError("Inputs must be a list of strings or None")


class PipelineTaskRegistry:
    """Registry for pipeline task metadata."""

    def __init__(self) -> None:
        self._tasks: dict[Any, PipelineTaskMetadata] = {}
        self._outputs: dict[str, Any] = {}

    def register_task(
        self,
        task: Any,
        metadata: PipelineTaskMetadata,
    ) -> None:
        """Register a task with its metadata."""
        self._tasks[task] = metadata
        self._outputs[metadata.output] = task

    def get_metadata(self, task: Any) -> PipelineTaskMetadata | None:
        """Get metadata for a task."""
        return self._tasks.get(task)

    def get_task_by_output(self, output_name: str) -> Any | None:
        """Get task that produces the given output."""
        return self._outputs.get(output_name)

    def get_all_outputs(self) -> list[str]:
        """Get all registered output names."""
        return list(self._outputs.keys())

    def validate_outputs(self) -> None:
        """Validate all registered outputs for conflicts."""
        # Check for duplicate output names across different tasks
        output_to_task: dict[Any, Any] = {}
        for task, metadata in self._tasks.items():
            if metadata.output in output_to_task:
                existing_task = output_to_task[metadata.output]
                if existing_task != task:
                    raise ValueError(
                        f"Duplicate output name '{metadata.output}' used by "
                        f"different tasks",
                    )
            output_to_task[metadata.output] = task

    def clear(self) -> None:
        """Clear all registered tasks and outputs."""
        self._tasks.clear()
        self._outputs.clear()


# Global registry instance
_task_registry: PipelineTaskRegistry = PipelineTaskRegistry()


def pipeline_task(
    output: str,
    inputs: list[str] | None = None,
    retries: int = 0,
) -> Callable[..., Any]:
    """
    Decorator for pipeline tasks.

    Marks a function as a pipeline task with metadata about
    its data dependencies.

    Args:
        output: Name of the data produced by this task
        inputs: Optional list of data names consumed by this task.
                If None, will be inferred from function signature.
        retries: Number of retry attempts on failure

    Returns:
        Decorated function with pipeline metadata

    Example:
        @pipeline_task(output="audio_features")
        async def extract_audio(track_paths):
            return extract_features(track_paths)

        @pipeline_task(output="mir_features")
        async def compute_mir(audio_features):
            return compute_mir_features(audio_features)

    Raises:
        ValueError: If output name is invalid or retries is negative
        PipelineError: If output name conflicts with existing task
    """

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        # Infer inputs if not provided
        inferred_inputs = inputs
        if inferred_inputs is None:
            inferred_inputs = _infer_inputs_from_signature(func)

        # Create metadata
        metadata = PipelineTaskMetadata(
            output=output,
            inputs=inferred_inputs,
            retries=retries,
            task_name=getattr(func, "__name__", str(func)),
        )

        # Choose appropriate wrapper based on function type
        if _is_async_function(func):

            @wraps(func)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                return await func(*args, **kwargs)

            wrapper = async_wrapper
        else:

            @wraps(func)
            def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
                return func(*args, **kwargs)

            wrapper = sync_wrapper

        # Register the task (only the wrapper to avoid conflicts)
        _task_registry.register_task(wrapper, metadata)

        # Attach metadata to wrapper for easy access
        wrapper._pipeline_metadata = metadata  # type: ignore

        return wrapper

    return decorator


def get_pipeline_metadata(func: Any) -> dict[str, Any]:
    """
    Get pipeline metadata for a function or task.

    Args:
        func: Function or task to check

    Returns:
        Dictionary of pipeline metadata, or empty dict if not a pipeline task
    """
    # Check if it's a TaskiqDecoratedTask
    if hasattr(func, "original_function"):
        original = getattr(func, "original_function", None)
        if original is not None:
            metadata = _task_registry.get_metadata(original)
            if metadata:
                return {
                    "output": metadata.output,
                    "inputs": metadata.inputs,
                    "retries": metadata.retries,
                    "is_pipeline_task": metadata.is_pipeline_task,
                    "multiple_outputs": metadata.multiple_outputs,
                    "output_types": metadata.output_types,
                }

    # Check directly in registry
    metadata = _task_registry.get_metadata(func)
    if metadata:
        return {
            "output": metadata.output,
            "inputs": metadata.inputs,
            "retries": metadata.retries,
            "is_pipeline_task": metadata.is_pipeline_task,
            "multiple_outputs": metadata.multiple_outputs,
            "output_types": metadata.output_types,
        }

    # Check if func has attached metadata (legacy support)
    if hasattr(func, "_pipeline_metadata"):
        meta = func._pipeline_metadata
        return {
            "output": meta.output,
            "inputs": meta.inputs,
            "retries": meta.retries,
            "is_pipeline_task": meta.is_pipeline_task,
            "multiple_outputs": meta.multiple_outputs,
            "output_types": meta.output_types,
        }

    # Legacy support for old attribute-based metadata
    if hasattr(func, "_pipeline_task"):
        return {
            "output": getattr(func, "_pipeline_output", ""),
            "inputs": getattr(func, "_pipeline_inputs", []),
            "retries": getattr(func, "_pipeline_retries", 0),
            "is_pipeline_task": True,
            "multiple_outputs": False,
            "output_types": {},
        }

    # Check if it's a function that was decorated
    # by looking at its __wrapped__ attribute (from functools.wraps)
    if hasattr(func, "__wrapped__"):
        wrapped = func.__wrapped__
        metadata = _task_registry.get_metadata(wrapped)
        if metadata:
            return {
                "output": metadata.output,
                "inputs": metadata.inputs,
                "retries": metadata.retries,
                "is_pipeline_task": metadata.is_pipeline_task,
                "multiple_outputs": metadata.multiple_outputs,
                "output_types": metadata.output_types,
            }

    return {}


def is_pipeline_task(func: Any) -> bool:
    """
    Check if a function is a pipeline task.

    Args:
        func: Function to check

    Returns:
        True if the function is a pipeline task
    """
    metadata = get_pipeline_metadata(func)
    return metadata.get("is_pipeline_task", False)


def get_task_outputs(func: Any) -> list[str]:
    """
    Get all output names produced by a task.

    Args:
        func: Task function to check

    Returns:
        List of output names
    """
    metadata = get_pipeline_metadata(func)
    if not metadata:
        return []

    outputs = [metadata["output"]]
    if metadata.get("multiple_outputs"):
        # For multiple outputs, check the output_types dict
        output_types = metadata.get("output_types", {})
        outputs.extend(output_types.keys())

    return outputs


def validate_pipeline_outputs(tasks: list[Any]) -> None:
    """
    Validate that all tasks have unique output names.

    Args:
        tasks: List of task functions to validate

    Raises:
        PipelineError: If there are duplicate output names
    """
    _task_registry.validate_outputs()


def get_all_pipeline_outputs() -> list[str]:
    """
    Get all registered pipeline output names.

    Returns:
        List of all output names across all registered tasks
    """
    return _task_registry.get_all_outputs()


def get_task_by_output(output_name: str) -> Any | None:
    """
    Get the task that produces the given output.

    Args:
        output_name: Name of the output to find

    Returns:
        Task function that produces the output, or None
    """
    return _task_registry.get_task_by_output(output_name)


def pipeline_task_multi_output(
    outputs: dict[str, Any],
    inputs: list[str] | None = None,
    retries: int = 0,
) -> Callable[..., Any]:
    """
    Decorator for pipeline tasks with multiple outputs.

    Marks a function as a pipeline task that produces multiple named outputs.
    The function must return a dictionary mapping output names to values.

    Args:
        outputs: Dictionary mapping output names to their types/descriptions
        inputs: Optional list of data names consumed by this task.
                If None, will be inferred from function signature.
        retries: Number of retry attempts on failure

    Returns:
        Decorated function with pipeline metadata

    Example:
        @pipeline_task_multi_output(
            outputs={"features": dict, "metadata": dict},
            retries=2
        )
        async def process_audio(track_path: str) -> dict:
            features = extract_features(track_path)
            metadata = extract_metadata(track_path)
            return {
                "features": features,
                "metadata": metadata
            }
    """

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        # Infer inputs if not provided
        inferred_inputs = inputs
        if inferred_inputs is None:
            inferred_inputs = _infer_inputs_from_signature(func)

        # Create metadata for multiple outputs
        primary_output = next(iter(outputs.keys()))  # Use first output as primary
        metadata = PipelineTaskMetadata(
            output=primary_output,
            inputs=inferred_inputs,
            retries=retries,
            task_name=getattr(func, "__name__", str(func)),
            multiple_outputs=True,
            output_types=outputs,
        )

        # Choose appropriate wrapper based on function type
        if _is_async_function(func):

            @wraps(func)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                return await func(*args, **kwargs)

            wrapper = async_wrapper
        else:

            @wraps(func)
            def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
                return func(*args, **kwargs)

            wrapper = sync_wrapper

        # Register the task (only the wrapper to avoid conflicts)
        _task_registry.register_task(wrapper, metadata)

        # Register additional outputs
        for output_name in outputs:
            if output_name != primary_output:
                _task_registry._outputs[output_name] = wrapper

        # Attach metadata to wrapper for easy access
        wrapper._pipeline_metadata = metadata  # type: ignore

        return wrapper

    return decorator


def _infer_inputs_from_signature(func: Callable[..., Any]) -> list[str]:
    """
    Infer input parameter names from function signature.

    Args:
        func: Function to analyze

    Returns:
        List of parameter names that should be treated as inputs
    """
    try:
        sig = inspect.signature(func)
        params = list(sig.parameters.values())

        # Skip 'self' and 'cls' parameters, plus parameters with defaults
        # (assuming parameters with defaults are configuration, not data inputs)
        input_names = []
        for param in params:
            if (
                param.name not in ("self", "cls")
                and param.default is inspect.Parameter.empty
            ):
                input_names.append(param.name)

        return input_names
    except (AttributeError, TypeError, ValueError):
        return []


def _is_async_function(func: Callable[..., Any]) -> bool:
    """
    Check if a function is async.

    Args:
        func: Function to check

    Returns:
        True if the function is async
    """
    return inspect.iscoroutinefunction(func)


# Note: Legacy pipeline_task_legacy removed in favor of unified pipeline_task decorator


__all__ = [
    "get_all_pipeline_outputs",
    "get_pipeline_metadata",
    "get_task_by_output",
    "get_task_outputs",
    "is_pipeline_task",
    "pipeline_task",
    "pipeline_task_multi_output",
    "validate_pipeline_outputs",
]
