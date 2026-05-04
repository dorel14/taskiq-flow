"""REST API for pipeline visualization and management."""

import logging
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from taskiq import AsyncBroker

from taskiq_flow.pipeline import DataflowPipeline
from taskiq_flow.visualization import DAGVisualizer

logger = logging.getLogger(__name__)


class PipelineVisualizationAPI:
    """API REST pour la visualisation et la gestion des pipelines.

    Fournit des endpoints FastAPI pour inspecter les DAGs, déclencher
    des exécutions et suivre le statut des pipelines.

    Auteur: SoniqueBay Team
    Version: 0.3.2
    """

    pipelines: dict[str, DataflowPipeline]

    def __init__(self, broker: AsyncBroker, app: FastAPI | None = None) -> None:
        """Initialize the visualization API.

        Args:
            broker: TaskIQ broker instance
            app: FastAPI app instance (creates new one if not provided)
        """
        self.broker = broker
        self.app = app or FastAPI(title="TaskIQ Flow Visualization API")
        self.pipelines: dict[str, DataflowPipeline] = {}
        self._setup_routes()

    def _setup_routes(self) -> None:
        """Setup API routes."""
        self._add_health_routes()
        self._add_pipeline_routes()
        self._add_dag_routes()

    def _add_health_routes(self) -> None:
        """Add health check routes."""

        @self.app.get("/health")
        async def health_check() -> dict[str, str]:
            """Health check endpoint."""
            return {"status": "healthy"}

    def _add_pipeline_routes(self) -> None:
        """Add pipeline management routes."""

        @self.app.get("/pipelines")
        async def list_pipelines() -> dict[str, list[dict[str, Any]]]:
            """List all registered pipelines."""
            pipeline_info = []
            for pipeline_id, pipeline in self.pipelines.items():
                info = {
                    "id": pipeline_id,
                    "has_dag": pipeline._dag is not None,
                    "task_count": len(pipeline._dataflow_tasks),
                    "registered_tasks": len(pipeline._registered_tasks),
                }
                if pipeline._dag:
                    info["node_count"] = len(pipeline._dag.nodes)
                    info["edge_count"] = len(pipeline._dag.edges)
                pipeline_info.append(info)
            return {"pipelines": pipeline_info}

        @self.app.post("/pipelines/{pipeline_id}")
        async def register_pipeline(
            pipeline_id: str,
            tasks: list[dict[str, Any]],
        ) -> dict[str, str]:
            """Register a pipeline for visualization.

            Note: This endpoint stores pipeline metadata for visualization.
            For actual pipeline execution, use the add_pipeline method or
            create DataflowPipeline instances programmatically.

            Args:
                pipeline_id: Unique pipeline identifier
                tasks: List of task definitions with optional 'name',
                    'dependencies' keys
            """
            logger.info(
                "Registering pipeline %s with %d tasks",
                pipeline_id,
                len(tasks),
            )
            pipeline = DataflowPipeline(self.broker)
            pipeline.pipeline_id = pipeline_id
            pipeline._registered_tasks = tasks
            self.pipelines[pipeline_id] = pipeline
            return {"message": f"Pipeline {pipeline_id} registered"}

        @self.app.get("/pipelines/{pipeline_id}/status")
        async def get_pipeline_status(pipeline_id: str) -> dict[str, Any]:
            """Get pipeline execution status."""
            if pipeline_id not in self.pipelines:
                raise HTTPException(
                    status_code=404,
                    detail=f"Pipeline {pipeline_id} not found",
                )

            return {
                "pipeline_id": pipeline_id,
                "status": "registered",
                "tasks": len(self.pipelines[pipeline_id]._dataflow_tasks),
            }

    def _add_dag_routes(self) -> None:
        """Add DAG visualization routes."""

        @self.app.get("/pipelines/{pipeline_id}/dag")
        async def get_pipeline_dag(pipeline_id: str) -> dict[str, Any]:
            """Get pipeline DAG visualization."""
            if pipeline_id not in self.pipelines:
                raise HTTPException(
                    status_code=404,
                    detail=f"Pipeline {pipeline_id} not found",
                )

            pipeline = self.pipelines[pipeline_id]
            if not pipeline._dag:
                pipeline._build_dataflow_dag()

            if not pipeline._dag:
                raise HTTPException(
                    status_code=404,
                    detail="Pipeline has no DAG",
                )

            return DAGVisualizer.to_json(pipeline._dag)

        @self.app.get("/pipelines/{pipeline_id}/dag/dot")
        async def get_pipeline_dag_dot(pipeline_id: str) -> dict[str, str]:
            """Get pipeline DAG in DOT format."""
            if pipeline_id not in self.pipelines:
                raise HTTPException(
                    status_code=404,
                    detail=f"Pipeline {pipeline_id} not found",
                )

            pipeline = self.pipelines[pipeline_id]
            if not pipeline._dag:
                pipeline._build_dataflow_dag()

            if not pipeline._dag:
                raise HTTPException(
                    status_code=404,
                    detail="Pipeline has no DAG",
                )

            dot = DAGVisualizer.to_dot(pipeline._dag)
            return {"dot": dot}

        @self.app.get("/pipelines/{pipeline_id}/visualize")
        async def visualize_pipeline(pipeline_id: str) -> JSONResponse:
            """Get complete pipeline visualization."""
            if pipeline_id not in self.pipelines:
                raise HTTPException(
                    status_code=404,
                    detail=f"Pipeline {pipeline_id} not found",
                )

            pipeline = self.pipelines[pipeline_id]
            if not pipeline._dag:
                pipeline._build_dataflow_dag()

            if not pipeline._dag:
                raise HTTPException(
                    status_code=404,
                    detail="Pipeline has no DAG",
                )

            dag_json = DAGVisualizer.to_json(pipeline._dag)
            dot = DAGVisualizer.to_dot(pipeline._dag)

            return JSONResponse(
                content={
                    "pipeline_id": pipeline_id,
                    "dag": dag_json,
                    "dot": dot,
                    "levels": [
                        [node.task.task_name for node in level]
                        for level in pipeline._dag.levels
                    ],
                },
            )

    def add_pipeline(self, pipeline_id: str, pipeline: DataflowPipeline) -> None:
        """Add a pipeline to the API.

        Args:
            pipeline_id: Unique pipeline identifier
            pipeline: DataflowPipeline instance
        """
        self.pipelines[pipeline_id] = pipeline
        logger.info("Added pipeline %s to visualization API", pipeline_id)

    def get_app(self) -> FastAPI:
        """Get the FastAPI application.

        Returns:
            FastAPI application instance
        """
        return self.app


def create_visualization_api(
    broker: AsyncBroker,
    app: FastAPI | None = None,
) -> PipelineVisualizationAPI:
    """Create a pipeline visualization API.

    Args:
        broker: TaskIQ broker instance
        app: Optional FastAPI app instance

    Returns:
        PipelineVisualizationAPI instance
    """
    return PipelineVisualizationAPI(broker, app)


__all__ = [
    "PipelineVisualizationAPI",
    "create_visualization_api",
]

