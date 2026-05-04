"""Example: Using the REST API for pipeline visualization and management.

This example demonstrates how to set up a FastAPI server with
the PipelineVisualizationAPI to expose pipeline management
and visualization endpoints. It also shows how to create
pipelines programmatically and interact with the API.

Note: This example uses uvicorn to run the API server.
Install with: pip install uvicorn[standard]
"""

import asyncio
import logging
from typing import Any

from fastapi import FastAPI, HTTPException
from taskiq import InMemoryBroker
from taskiq.decor import AsyncTaskiqDecoratedTask

from taskiq_flow import DataflowPipeline, pipeline_task
from taskiq_flow.api import PipelineVisualizationAPI, create_visualization_api

# Create broker
broker = InMemoryBroker(await_inplace=True)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
# ============================================================================
# Define sample tasks for testing
# ============================================================================


@broker.task
@pipeline_task(output="user_data")
async def fetch_user_data(user_id: int) -> dict[str, Any]:
    """Fetch user data from database."""
    await asyncio.sleep(0.1)
    return {
        "id": user_id,
        "name": f"User{user_id}",
        "email": f"user{user_id}@example.com",
    }


@broker.task
@pipeline_task(output="order_history")
async def fetch_orders(user_data: dict[str, Any]) -> list[dict[str, Any]]:
    """Fetch order history for user."""
    await asyncio.sleep(0.2)
    user_id = user_data["id"]
    return [
        {"order_id": 100 + user_id, "total": 99.99},
        {"order_id": 101 + user_id, "total": 149.50},
    ]


@broker.task
@pipeline_task(output="recommendations")
async def generate_recommendations(
    user_data: dict[str, Any],
    order_history: list[dict[str, Any]],
) -> list[str]:
    """Generate product recommendations based on user data."""
    await asyncio.sleep(0.15)
    return ["product_A", "product_B", "product_C"]


# Type alias for the decorated tasks
TaskType = AsyncTaskiqDecoratedTask[Any, Any]


# ============================================================================
# Create a sample pipeline
# ============================================================================

sample_pipeline: DataflowPipeline = DataflowPipeline.from_tasks(
    broker,
    [
        fetch_user_data,  # type: ignore
        fetch_orders,  # type: ignore
        generate_recommendations,  # type: ignore
    ],
)
sample_pipeline.pipeline_id = "sample_recommendation_pipeline"


# ============================================================================
# API Server Setup
# ============================================================================


def create_app() -> FastAPI:
    """Create and configure the FastAPI application with visualization API."""
    # Create FastAPI app
    app = FastAPI(
        title="TaskIQ Flow API",
        description="REST API for pipeline visualization and management",
        version="1.0.0",
    )

    # Create the visualization API
    viz_api = create_visualization_api(broker, app)

    # Register the sample pipeline
    viz_api.add_pipeline("sample_recommendation_pipeline", sample_pipeline)

    # Add a custom endpoint example: execute pipeline
    @app.post("/pipelines/{pipeline_id}/execute")
    async def execute_pipeline(
        pipeline_id: str,
        parameters: dict[str, Any],
    ) -> dict[str, Any]:
        """Execute a pipeline with given parameters."""
        if pipeline_id not in viz_api.pipelines:
            raise HTTPException(
                status_code=404,
                detail=f"Pipeline {pipeline_id} not found",
            )

        pipeline = viz_api.pipelines[pipeline_id]
        try:
            result = await pipeline.kiq_dataflow(**parameters)
            return {
                "status": "executed",
                "pipeline_id": pipeline_id,
                "task_id": result.task_id,
                "message": (
                    "Pipeline execution started. Use /result/{task_id} to check status."
                ),
            }
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e)) from e

    # Add endpoint to get pipeline result
    @app.get("/pipelines/result/{task_id}")
    async def get_result(task_id: str) -> dict[str, Any]:
        """Get the result of a pipeline execution."""
        try:
            result = await broker.result_backend.get_result(task_id)
            if result is None:
                raise HTTPException(
                    status_code=404,
                    detail=f"No result found for task_id {task_id}",
                )
            return {"task_id": task_id, "result": result.return_value}
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e)) from e

    return app


# ============================================================================
# Demonstration Functions
# ============================================================================


async def demo_api_usage() -> None:
    """Demonstrate programmatic usage of the PipelineVisualizationAPI."""
    logger.info("\n=== PipelineVisualizationAPI Demo ===\n")

    # Create a standalone FastAPI app
    app = FastAPI()
    viz_api = PipelineVisualizationAPI(broker, app)

    # Register a pipeline using the visualization API's public method
    pipeline: DataflowPipeline = DataflowPipeline.from_tasks(
        broker,
        [fetch_user_data, fetch_orders, generate_recommendations],  # type: ignore
    )
    pipeline.pipeline_id = "demo_pipeline"
    viz_api.add_pipeline("demo_pipeline", pipeline)

    # Use the API's pipeline storage directly (for demonstration purposes)
    logger.info("Registered pipelines:")
    for pid, p in viz_api.pipelines.items():
        viz = p.visualize()
        logger.info(f"  - {pid} (tasks: {len(viz['nodes'])})")

    # Use the public visualize() method to get DAG structure
    # This method internally builds the DAG if needed
    viz_json = pipeline.visualize()
    logger.info("\nPipeline DAG structure:")
    logger.info(f"  Nodes: {[node['id'] for node in viz_json['nodes']]}")
    logger.info(f"  Edges: {viz_json['edges']}")
    logger.info(f"  Levels: {viz_json['levels']}")

    # Get DOT representation using public method
    dot = pipeline.visualize_dot()
    logger.info(f"\nDOT format (first 150 chars): {dot[:150]}...")

    logger.info("\n=== Demo Complete ===")
    logger.info("\nThe API provides the following FastAPI routes:")
    logger.info("  GET  /health")
    logger.info("  GET  /pipelines")
    logger.info("  POST /pipelines/{pipeline_id}")
    logger.info("  GET  /pipelines/{pipeline_id}/status")
    logger.info("  GET  /pipelines/{pipeline_id}/dag")
    logger.info("  GET  /pipelines/{pipeline_id}/dag/dot")
    logger.info("  GET  /pipelines/{pipeline_id}/visualize")


# ============================================================================
# Main
# ============================================================================


async def main() -> None:
    """Run the API example."""
    logger.info("TaskIQ Flow REST API Example")
    logger.info("=" * 50)

    # Demonstrate API usage programmatically
    await demo_api_usage()

    # Show how to run the server (commented out for demo)
    logger.info("\nTo start the API server, run:")
    logger.info("  uvicorn examples.api_example:create_app --reload --port 8000")
    logger.info("\nThen access:")
    logger.info("  - API docs: http://localhost:8000/docs")
    logger.info("  - Health: http://localhost:8000/health")
    logger.info("  - List pipelines: http://localhost:8000/pipelines")
    logger.info("  - Pipeline DAG: http://localhost:8000/pipelines/{pipeline_id}/dag")
    logger.info(
        "  - Pipeline DOT: http://localhost:8000/pipelines/{pipeline_id}/dag/dot",
    )
    logger.info(
        "  - Visualize: http://localhost:8000/pipelines/{pipeline_id}/visualize",
    )
    logger.info(
        "  - Execute: POST http://localhost:8000/pipelines/{pipeline_id}/execute",
    )
    logger.info("\nExample execution request body:")
    logger.info('  {"user_id": 123}')


if __name__ == "__main__":
    # Note: To run the server, you need uvicorn installed
    # This demo runs the programmatic API demo without starting the server
    asyncio.run(main())
