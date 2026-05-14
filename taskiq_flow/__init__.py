"""
TaskIQ Flow - Orchestration de pipelines basée sur les flux de données.

Ce package fournit des capacités d'orchestration de pipelines pour TaskIQ,
combinant des workflows séquentiels avec exécution automatique de DAG
(dataflow).

Auteur: SoniqueBay Team
Version: 1.0.2
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
        get_all_pipeline_outputs,
        get_pipeline_metadata,
        get_task_by_output,
        get_task_outputs,
        is_pipeline_task,
        pipeline_task,
        pipeline_task_multi_output,
        validate_pipeline_outputs,
    )

with contextlib.suppress(ImportError):
    from taskiq_flow.execution_engine import ExecutionEngine

with contextlib.suppress(ImportError):
    from taskiq_flow.map_reduce import MapReduce

with contextlib.suppress(ImportError):
    from taskiq_flow.api import (
        PipelineVisualizationAPI,
        create_visualization_api,
    )

with contextlib.suppress(ImportError):
    from taskiq_flow.visualization import (
        DAGVisualizer,
        visualize_pipeline,
    )

__all__ = [
    "DAG",
    "AbortPipeline",
    "DAGBuilder",
    "DAGNode",
    "DAGVisualizer",
    "DataNode",
    "DataflowPipeline",
    "DataflowRegistry",
    "ExecutionEngine",
    "HookManager",
    "MapReduce",
    "Pipeline",
    "PipelineError",
    "PipelineMiddleware",
    "PipelineScheduler",
    "PipelineTrackingManager",
    # API exports
    "PipelineVisualizationAPI",
    "TrackingStorageFactory",
    "create_visualization_api",
    "get_all_pipeline_outputs",
    "get_pipeline_metadata",
    "get_task_by_output",
    "get_task_outputs",
    "is_pipeline_task",
    "pipeline_task",
    "pipeline_task_multi_output",
    "validate_pipeline_outputs",
    "visualize_pipeline",
]
