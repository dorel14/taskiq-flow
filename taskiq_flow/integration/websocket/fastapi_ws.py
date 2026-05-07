"""FastAPI WebSocket integration for real-time pipeline events.

This module provides a WebSocket manager that integrates with FastAPI's
WebSocket support, allowing unified HTTP/WebSocket API endpoints.

Author: SoniqueBay Team
Version: 0.4.5
"""

import asyncio
import json
import logging
from collections import defaultdict
from contextlib import suppress
from typing import Any

from fastapi import WebSocket, WebSocketDisconnect

logger = logging.getLogger(__name__)


class FastAPIWebSocketManager:
    """Manages WebSocket connections using FastAPI's WebSocket protocol.

    This manager handles client connections, subscriptions by pipeline,
    and event broadcasting to connected clients.
    """

    def __init__(self) -> None:
        """Initialize the WebSocket manager."""
        self._clients: dict[str, set[WebSocket]] = defaultdict(set)
        self._lock: asyncio.Lock = asyncio.Lock()
        self._active: bool = True

    async def connect(self, websocket: WebSocket, pipeline_id: str) -> None:
        """Accept a WebSocket connection and subscribe to a pipeline.

        Args:
            websocket: The WebSocket connection
            pipeline_id: The pipeline ID to subscribe to
        """
        await websocket.accept()
        async with self._lock:
            self._clients[pipeline_id].add(websocket)
        logger.info(f"Client connected to pipeline {pipeline_id}")

    async def disconnect(self, websocket: WebSocket, pipeline_id: str) -> None:
        """Disconnect a WebSocket client.

        Args:
            websocket: The WebSocket connection to close
            pipeline_id: The pipeline ID the client was subscribed to
        """
        async with self._lock:
            self._clients[pipeline_id].discard(websocket)
            if not self._clients[pipeline_id]:
                del self._clients[pipeline_id]
        logger.info(f"Client disconnected from pipeline {pipeline_id}")

    async def broadcast_event(self, pipeline_id: str, event: dict[str, Any]) -> None:
        """Broadcast an event to all clients subscribed to a pipeline.

        Args:
            pipeline_id: The pipeline ID to broadcast to
            event: The event data to broadcast
        """
        async with self._lock:
            clients = list(self._clients.get(pipeline_id, set()))

        if not clients:
            return

        message = json.dumps(event)
        disconnected = []

        for client in clients:
            try:
                await client.send_text(message)
            except Exception as e:
                logger.warning(f"Failed to send message to client: {e}")
                disconnected.append(client)

        # Clean up disconnected clients
        if disconnected:
            async with self._lock:
                self._clients[pipeline_id] -= set(disconnected)

    def get_client_count(self, pipeline_id: str) -> int:
        """Get the number of clients subscribed to a pipeline.

        Args:
            pipeline_id: The pipeline ID

        Returns:
            Number of connected clients
        """
        return len(self._clients.get(pipeline_id, set()))

    def get_pipeline_ids(self) -> list[str]:
        """Get all pipeline IDs with active subscriptions.

        Returns:
            List of pipeline IDs
        """
        return list(self._clients.keys())

    async def close_all(self) -> None:
        """Close all WebSocket connections."""
        async with self._lock:
            for _pipeline_id, clients in self._clients.items():
                for client in clients:
                    with suppress(Exception):
                        await client.close()
            self._clients.clear()
        self._active = False
        logger.info("All WebSocket connections closed")


# Global FastAPI WebSocket manager instance
_fastapi_ws_manager: FastAPIWebSocketManager | None = None


def get_fastapi_ws_manager() -> FastAPIWebSocketManager:
    """Get or create the global FastAPI WebSocket manager.

    Returns:
        The singleton FastAPIWebSocketManager instance
    """
    global _fastapi_ws_manager  # noqa: PLW0603
    if _fastapi_ws_manager is None:
        _fastapi_ws_manager = FastAPIWebSocketManager()
    return _fastapi_ws_manager


async def fastapi_websocket_endpoint(
    websocket: WebSocket,
    pipeline_id: str,
    manager: FastAPIWebSocketManager | None = None,
) -> None:
    """FastAPI WebSocket endpoint handler.

    This function is designed to be used as a FastAPI WebSocket route handler.

    Args:
        websocket: The WebSocket connection from FastAPI
        pipeline_id: The pipeline ID to subscribe to
        manager: Optional custom manager (uses global if not provided)
    """
    ws_manager = manager or get_fastapi_ws_manager()

    try:
        await ws_manager.connect(websocket, pipeline_id)

        # Keep connection alive by waiting for messages
        while True:
            try:
                data = await websocket.receive_text()
                # Handle any client messages if needed
                logger.debug(f"Received message from client: {data}")
            except WebSocketDisconnect:
                break
    except WebSocketDisconnect:
        logger.info(f"Client disconnected from pipeline {pipeline_id}")
    except Exception as e:
        logger.error(f"WebSocket error for pipeline {pipeline_id}: {e}")
    finally:
        await ws_manager.disconnect(websocket, pipeline_id)
