"""Pipelines for taskiq tasks."""

from taskiq_pipelines.exceptions import AbortPipeline, PipelineError
from taskiq_pipelines.middleware import PipelineMiddleware
from taskiq_pipelines.pipeliner import Pipeline

# New exports
try:
    from taskiq_pipelines.tracking import (
        PipelineTrackingManager,
        TrackingStorageFactory,
    )
except ImportError:
    pass

try:
    from taskiq_pipelines.hooks import HookManager
except ImportError:
    pass

try:
    from taskiq_pipelines.scheduling import PipelineScheduler
except ImportError:
    pass

__all__ = [
    "AbortPipeline",
    "Pipeline",
    "PipelineError",
    "PipelineMiddleware",
    # New
    "PipelineTrackingManager",
    "TrackingStorageFactory",
    "HookManager",
    "PipelineScheduler",
]
