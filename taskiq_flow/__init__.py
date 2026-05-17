"""
TaskIQ Flow - Orchestration de pipelines basée sur les flux de données.

Ce package fournit des capacités d'orchestration de pipelines pour TaskIQ,
combinant des workflows séquentiels avec exécution automatique de DAG
(dataflow).

Auteur: SoniqueBay Team
Version: 1.2.0
"""

import contextlib

from taskiq_flow.exceptions import AbortPipeline, PipelineError
from taskiq_flow.middleware import PipelineMiddleware, TransportMiddleware
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

# Storage & Cache exports (new in v1.2.0)
with contextlib.suppress(ImportError):
    from taskiq_flow.storage import (
        BaseCacheAdapter,
        BaseStorageAdapter,
        InMemoryStorageAdapter,
        RedisStorageAdapter,
        StorageEntry,
    )

with contextlib.suppress(ImportError):
    from taskiq_flow.cache import (
        InMemoryCacheAdapter,
        RedisCacheAdapter,
    )

with contextlib.suppress(ImportError):
    from taskiq_flow.middlewares import (
        CacheMiddleware,
        PipelineRetryMiddleware,
        StorageMiddleware,
        calculate_exponential_backoff_delay,
    )

with contextlib.suppress(ImportError):
    from taskiq_flow.storage.factory import StorageAdapterFactory

__all__ = [
    "DAG",
    "AbortPipeline",
    "BaseCacheAdapter",
    "BaseStorageAdapter",
    "CacheMiddleware",
    "DAGBuilder",
    "DAGNode",
    "DAGVisualizer",
    "DataNode",
    "DataflowPipeline",
    "DataflowRegistry",
    "ExecutionEngine",
    "HookManager",
    "InMemoryCacheAdapter",
    "InMemoryStorageAdapter",
    "MapReduce",
    "Pipeline",
    "PipelineError",
    "PipelineMiddleware",
    "PipelineRetryMiddleware",
    "PipelineScheduler",
    "PipelineTrackingManager",
    "PipelineVisualizationAPI",
    "RedisCacheAdapter",
    "RedisStorageAdapter",
    "StorageAdapterFactory",
    "StorageEntry",
    "StorageMiddleware",
    "TrackingStorageFactory",
    "TransportMiddleware",
    "calculate_exponential_backoff_delay",
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
