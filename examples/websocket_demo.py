"""
Exemple démontrant l'intégration WebSocket pour le suivi temps réel des pipelines.

Ce module montre l'implémentation FastAPI WebSocket pour les événements de pipeline.

Implémentations :
    - FastAPI WebSocket : Endpoint `/ws/{pipeline_id}` dans un serveur FastAPI

Autheur: SoniqueBay Team
Version: 1.2.0
"""

import asyncio
import logging

from taskiq import InMemoryBroker

from taskiq_flow import Pipeline
from taskiq_flow.hooks import HookManager, setup_websocket_bridge
from taskiq_flow.middleware import PipelineMiddleware

# Configure logging
logging.basicConfig(level=logging.INFO)

# Create broker and pipeline
broker = InMemoryBroker(await_inplace=True).with_middlewares(PipelineMiddleware())


# Define tasks
@broker.task
def add_one(x: int) -> int:
    """Add one to the input."""
    return x + 1


@broker.task
def multiply_by_two(x: int) -> int:
    """Multiply the input by two."""
    return x * 2


async def main() -> None:
    """Run the WebSocket demo."""
    # Set up hooks and WebSocket bridge
    hook_manager = HookManager()

    # Use FastAPI WebSocket (preferred) - integrates with FastAPI routes
    setup_websocket_bridge(hook_manager, use_fastapi=True)

    # Create pipeline with tracking
    pipeline: Pipeline = Pipeline(broker)  # type: ignore
    pipeline.pipeline_id = "websocket_demo"
    pipeline.call_next(
        add_one,
        param_name="x",
    )
    pipeline.call_next(
        multiply_by_two,
        param_name="x",
    )

    # Enable hooks for WebSocket events
    pipeline.with_hooks(hook_manager)

    print("WebSocket server configured (FastAPI WebSocket)")  # noqa: T201
    print("Connect a WebSocket client and subscribe with:")  # noqa: T201
    print("  ws://127.0.0.1:8000/ws/websocket_demo")  # noqa: T201
    print("Then run the pipeline to see real-time events...")  # noqa: T201

    # Run pipeline
    result = await pipeline.kiq(5)  # Start with 5
    print(f"Pipeline result: {result}")  # noqa: T201

    print("Demo complete.")  # noqa: T201


if __name__ == "__main__":
    asyncio.run(main())
