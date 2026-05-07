"""REST API for pipeline visualization and management."""

import logging
from typing import Any

from fastapi import FastAPI, HTTPException, Depends, Request
from fastapi.middleware import Middleware
from slowapi.middleware import SlowAPIMiddleware
from taskiq import AsyncBroker

from taskiq_flow.config import TaskiqFlowConfig
from taskiq_flow.pipeline import DataflowPipeline
from taskiq_flow.registry import register_pipeline
from taskiq_flow.security.auth import create_auth_provider
from taskiq_flow.security.authorization import PipelineAuthorization
from taskiq_flow.security.dependencies import get_authorization, get_auth_provider
from taskiq_flow.security.middleware import SecurityMiddleware
from taskiq_flow.security.rate_limiting import RateLimiter
from taskiq_flow.visualization import DAGVisualizer

logger = logging.getLogger(__name__)


class PipelineVisualizationAPI:
    """API REST pour la visualisation et la gestion des pipelines.

    Fournit des endpoints FastAPI pour inspecter les DAGs, déclencher
    des exécutions et suivre le statut des pipelines.

    Auteur: SoniqueBay Team
    Version: 0.4.5
    """

    pipelines: dict[str, DataflowPipeline]

    def __init__(
        self,
        broker: AsyncBroker,
        app: FastAPI | None = None,
        config: TaskiqFlowConfig | None = None,
    ) -> None:
        """Initialize the visualization API.

        Args:
            broker: TaskIQ broker instance
            app: FastAPI app instance (creates new one if not provided)
            config: TaskiqFlow configuration for security/metrics
        """
        self.broker = broker
        self.app = app or FastAPI(title="TaskIQ Flow Visualization API")
        self.pipelines: dict[str, DataflowPipeline] = {}

        # Store config (load from env if not provided)
        self.config = config or TaskiqFlowConfig()

        # Setup routes first (so they exist for rate limiting decoration)
        self._setup_routes()

        # Setup security if enabled
        if self.config.security_enabled:
            self._setup_security()

        # Setup metrics if enabled
        if self.config.metrics_enabled:
            self._setup_metrics()

    def _setup_security(self) -> None:
        """Configure security middleware and components based on config."""
        from taskiq_flow.security.audit import AuditLogger
        from taskiq_flow.security.https import HTTPSEnforcementMiddleware

        # Create components
        auth_provider = create_auth_provider(self.config)
        authorization = PipelineAuthorization(self.config.pipeline_acls)
        rate_limiter = RateLimiter()
        audit_logger = AuditLogger()

        # Store in app.state for dependency injection
        self.app.state.auth_provider = auth_provider
        self.app.state.authorization = authorization
        self.app.state.rate_limiter = rate_limiter
        self.app.state.audit_logger = audit_logger

        # Add HTTPS enforcement middleware (first in chain)
        if self.config.require_https:
            self.app.add_middleware(HTTPSEnforcementMiddleware)

        # Add SecurityMiddleware (handles auth + audit)
        self.app.add_middleware(
            SecurityMiddleware,
            auth_provider=auth_provider,
            authorization=authorization,
            rate_limiter=rate_limiter,
            audit_logger=audit_logger,
        )

        # Add SlowAPIMiddleware for rate limiting enforcement
        self.app.add_middleware(SlowAPIMiddleware, limiter=rate_limiter.get_limiter())

        # Apply rate limit decorators to all routes
        self._apply_rate_limits()

        logger.info("Security middleware configured (rate_limit=%s)", self.config.rate_limit_default)

    def _apply_rate_limits(self) -> None:
        """Apply rate limits from config to all registered API routes."""
        limiter = self.app.state.rate_limiter.get_limiter()

        for route in self.app.routes:
            # Skip non-API routes (static, docs, openapi)
            if not hasattr(route, "endpoint") or route.endpoint is None:
                continue
            if route.path in ("/docs", "/redoc", "/openapi.json", "/favicon.ico"):
                continue

            endpoint_name = route.name or route.endpoint.__name__
            limit_str = self.app.state.rate_limiter.get_limit(endpoint_name)

            # Wrap endpoint with limit decorator
            try:
                limited = limiter.limit(limit_str)(route.endpoint)
                route.endpoint = limited
            except Exception as e:
                logger.warning("Failed to apply rate limit to %s: %s", endpoint_name, e)

    def _setup_metrics(self) -> None:
        """Configure metrics endpoint if enabled."""
        from taskiq_flow.metrics.exporters.prometheus import get_metrics_endpoint

        self.app.get(self.config.metrics_path)(get_metrics_endpoint())
        logger.info("Metrics endpoint enabled at %s", self.config.metrics_path)

    def _setup_routes(self) -> None:
        """Register all API routes."""
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
            self.add_pipeline(pipeline_id, pipeline)
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
        register_pipeline(pipeline_id, pipeline)  # Also register globally for DAG routes
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
