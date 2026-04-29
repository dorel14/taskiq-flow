"""Pipelines for taskiq tasks."""

import contextlib

from taskiq_pipelines.exceptions import AbortPipeline, PipelineError
from taskiq_pipelines.middleware import PipelineMiddleware
from taskiq_pipelines.pipeliner import Pipeline

# New exports
with contextlib.suppress(ImportError):
    from taskiq_pipelines.tracking import (
        PipelineTrackingManager,
        TrackingStorageFactory,
    )

with contextlib.suppress(ImportError):
    from taskiq_pipelines.hooks import HookManager

with contextlib.suppress(ImportError):
    from taskiq_pipelines.scheduling import PipelineScheduler

__all__ = [
    "AbortPipeline",
    "HookManager",
    "Pipeline",
    "PipelineError",
    "PipelineMiddleware",
    "PipelineScheduler",
    # New
    "PipelineTrackingManager",
    "TrackingStorageFactory",
]
