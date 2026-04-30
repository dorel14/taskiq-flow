"""Example demonstrating WebSocket integration for real-time pipeline tracking."""

import asyncio
import logging

from taskiq import InMemoryBroker

from taskiq_flow import Pipeline
from taskiq_flow.hooks import HookManager, setup_websocket_bridge
from taskiq_flow.integration.websocket import get_websocket_server

# Configure logging
logging.basicConfig(level=logging.INFO)

# Create broker and pipeline
broker = InMemoryBroker()


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
    setup_websocket_bridge(hook_manager)

    # Create pipeline with tracking
    pipeline: Pipeline = Pipeline(broker)  # type: ignore
    pipeline.pipeline_id = "websocket_demo"
    pipeline.call_next(
        add_one,
        param_name="value",
    )
    pipeline.call_next(
        multiply_by_two,
        param_name="value",
    )

    # Enable hooks for WebSocket events
    pipeline.with_hooks(hook_manager)

    # Start WebSocket server in background
    websocket_server = get_websocket_server()
    _ = asyncio.create_task(  # noqa: RUF006
        websocket_server.start_server("127.0.0.1", 8765),
    )

    print("WebSocket server started on ws://127.0.0.1:8765")  # noqa: T201
    msg = '{"pipeline_id": "websocket_demo"}'
    print(f"Connect a WebSocket client and subscribe with: {msg}")  # noqa: T201
    print("Then run the pipeline to see real-time events...")  # noqa: T201

    # Wait a moment for server to start
    await asyncio.sleep(1)

    # Run pipeline
    result = await pipeline.kiq(5)  # Start with 5
    print(f"Pipeline result: {result}")  # noqa: T201

    # Keep server running for a bit
    await asyncio.sleep(5)

    print("Demo complete. Server will shut down.")  # noqa: T201


if __name__ == "__main__":
    asyncio.run(main())
