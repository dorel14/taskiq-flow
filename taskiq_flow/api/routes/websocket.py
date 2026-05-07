"""WebSocket routes for real-time pipeline events.

This module provides FastAPI WebSocket endpoints for real-time
pipeline event streaming.

Author: SoniqueBay Team
Version: 0.4.5
"""

from typing import Any

from fastapi import APIRouter, WebSocket

from taskiq_flow.integration.websocket.fastapi_ws import (
    fastapi_websocket_endpoint,
    get_fastapi_ws_manager,
)

router = APIRouter(prefix="/ws", tags=["websocket"])


@router.websocket("/{pipeline_id}")
async def websocket_endpoint(websocket: WebSocket, pipeline_id: str) -> None:
    """WebSocket endpoint for real-time pipeline events.

    Connect to stream pipeline events in real-time.

    Args:
        websocket: The WebSocket connection
        pipeline_id: The pipeline ID to subscribe to

    Example:
        ```javascript
        const ws = new WebSocket("ws://localhost:8000/ws/my_pipeline");
        ws.onmessage = function(event) {
            console.log("Pipeline event:", JSON.parse(event.data));
        };
        ```
    """
    manager = get_fastapi_ws_manager()
    await fastapi_websocket_endpoint(websocket, pipeline_id, manager)


@router.get("/clients/{pipeline_id}")
async def get_clients(pipeline_id: str) -> dict[str, Any]:
    """Get the number of clients subscribed to a pipeline.

    Args:
        pipeline_id: The pipeline ID

    Returns:
        Dictionary with client count
    """
    manager = get_fastapi_ws_manager()
    return {
        "pipeline_id": pipeline_id,
        "client_count": manager.get_client_count(pipeline_id),
    }


@router.get("/pipelines")
async def list_pipelines() -> dict[str, Any]:
    """List all pipelines with active WebSocket subscriptions.

    Returns:
        Dictionary with list of pipeline IDs
    """
    manager = get_fastapi_ws_manager()
    return {"pipelines": manager.get_pipeline_ids()}


__all__ = ["router"]
