"""TaskIQ Flow - Dataflow-based pipeline orchestration.

This package provides pipeline orchestration capabilities for TaskIQ,
combining sequential workflows with automatic dataflow DAG execution.

Auteur: SoniqueBay Team
Version: 0.2.0
"""

import contextlib

from taskiq_flow.exceptions import AbortPipeline, PipelineError
from taskiq_flow.middleware import PipelineMiddleware
from taskiq_flow.pipeline import DataflowPipeline
from taskiq_flow.pipeliner import Pipeline as OriginalPipeline

# Re-export Pipeline for backward compatibility
Pipeline = OriginalPipeline

# New exports
with contextlib.suppress(ImportError):
    from taskiq_flow.tracking import (
        PipelineTrackingManager,
        TrackingStorageFactory,
    )

with contextlib.suppress(ImportError):
    from taskiq_flow.hooks import HookManager

with contextlib.suppress(ImportError):
    from taskiq_flow.scheduling import PipelineScheduler

# Dataflow exports
with contextlib.suppress(ImportError):
    from taskiq_flow.dataflow import (
        DAG,
        DAGNode,
        DataflowRegistry,
        DataNode,
    )

with contextlib.suppress(ImportError):
    from taskiq_flow.dag_builder import DAGBuilder

with contextlib.suppress(ImportError):
    from taskiq_flow.decorators import (
        get_pipeline_metadata,
        is_pipeline_task,
        pipeline_task,
    )

with contextlib.suppress(ImportError):
    from taskiq_flow.execution_engine import ExecutionEngine

with contextlib.suppress(ImportError):
    from taskiq_flow.map_reduce import MapReduce

with contextlib.suppress(ImportError):
    from taskiq_flow.visualization import (
        DAGVisualizer,
        visualize_pipeline,
    )

__all__ = [
    # Original exports
    "AbortPipeline",
    "HookManager",
    "Pipeline",
    "PipelineError",
    "PipelineMiddleware",
    "PipelineScheduler",
    "PipelineTrackingManager",
    "TrackingStorageFactory",
    # New dataflow exports
    "DataflowPipeline",
    "DataNode",
    "DataflowRegistry",
    "DAG",
    "DAGNode",
    "DAGBuilder",
    "pipeline_task",
    "get_pipeline_metadata",
    "is_pipeline_task",
    "ExecutionEngine",
    "MapReduce",
    "DAGVisualizer",
    "visualize_pipeline",
]
