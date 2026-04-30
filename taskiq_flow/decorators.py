"""Decorators for pipeline tasks."""

import inspect
from collections.abc import Callable
from functools import wraps
from typing import Any

# Storage for pipeline task metadata
_PIPELINE_TASK_METADATA: dict[Any, dict[str, Any]] = {}


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
    """

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(func)
        async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
            return await func(*args, **kwargs)

        @wraps(func)
        def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
            return func(*args, **kwargs)

        # Choose appropriate wrapper based on function type
        wrapper = async_wrapper if _is_async_function(func) else sync_wrapper

        # Attach metadata
        _PIPELINE_TASK_METADATA[wrapper] = {
            "output": output,
            "inputs": inputs,
            "retries": retries,
            "is_pipeline_task": True,
        }

        # Also attach to original function for reference
        _PIPELINE_TASK_METADATA[func] = {
            "output": output,
            "inputs": inputs,
            "retries": retries,
            "is_pipeline_task": True,
        }

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
            metadata = _PIPELINE_TASK_METADATA.get(original)
            if metadata:
                return metadata

    # Check directly
    metadata = _PIPELINE_TASK_METADATA.get(func)
    if metadata:
        return metadata

    # Check if func is a wrapper created by pipeline_task
    # Look for metadata on the wrapper itself
    if hasattr(func, "_pipeline_task"):
        return {
            "output": getattr(func, "_pipeline_output", ""),
            "inputs": getattr(func, "_pipeline_inputs", []),
            "retries": getattr(func, "_pipeline_retries", 0),
            "is_pipeline_task": True,
        }

    # Check if it's a function that was decorated
    # by looking at its __wrapped__ attribute (from functools.wraps)
    if hasattr(func, "__wrapped__"):
        wrapped = func.__wrapped__
        metadata = _PIPELINE_TASK_METADATA.get(wrapped)
        if metadata:
            return metadata

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


def _is_async_function(func: Callable[..., Any]) -> bool:
    """
    Check if a function is async.

    Args:
        func: Function to check

    Returns:
        True if the function is async
    """
    return inspect.iscoroutinefunction(func)


# Legacy decorator support
def pipeline_task_legacy(
    output: str,
    inputs: list[str] | None = None,
    retries: int = 0,
    **kwargs: Any,
) -> Callable[..., Any]:
    """
    Legacy version of pipeline_task decorator.

    Directly attaches metadata to the function.
    """

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        setattr(func, "_pipeline_task", True)  # noqa: B010
        setattr(func, "_pipeline_output", output)  # noqa: B010
        setattr(func, "_pipeline_inputs", inputs)  # noqa: B010
        setattr(func, "_pipeline_retries", retries)  # noqa: B010

        @wraps(func)
        async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
            if _is_async_function(func):
                return await func(*args, **kwargs)
            return func(*args, **kwargs)

        # Attach metadata to wrapper too
        setattr(async_wrapper, "_pipeline_task", True)  # noqa: B010
        setattr(async_wrapper, "_pipeline_output", output)  # noqa: B010
        setattr(async_wrapper, "_pipeline_inputs", inputs)  # noqa: B010
        setattr(async_wrapper, "_pipeline_retries", retries)  # noqa: B010

        return async_wrapper

    return decorator


__all__ = [
    "get_pipeline_metadata",
    "is_pipeline_task",
    "pipeline_task",
    "pipeline_task_legacy",
]
