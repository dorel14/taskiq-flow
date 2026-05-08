"""FastAPI WebSocket integration for real-time pipeline events.

This module provides a WebSocket manager that integrates with FastAPI's
WebSocket support, allowing unified HTTP/WebSocket API endpoints.
Uses a channel registry for hierarchical subscriptions.

Author: SoniqueBay Team
Version: 0.4.5
"""

import logging
from typing import Any

from fastapi import WebSocket, WebSocketDisconnect

from taskiq_flow.integration.websocket.channel_registry import ChannelRegistry

logger = logging.getLogger(__name__)


class FastAPIWebSocketManager:
    """Manages WebSocket connections using FastAPI's WebSocket protocol.

    This manager handles client connections, subscriptions by channel,
    and event broadcasting to connected clients. Supports hierarchical
    channel names (e.g., "pipeline.<id>.events") for compatibility
    with tools like chanx.
    """

    def __init__(self, channel_registry: ChannelRegistry | None = None) -> None:
        """Initialize the WebSocket manager.

        Args:
            channel_registry: Optional shared channel registry
        """
        self.channel_registry = channel_registry or ChannelRegistry()
        self._active: bool = True

    async def connect(self, websocket: WebSocket, channel: str) -> None:
        """Accept a WebSocket connection and subscribe to a channel.

        Args:
            websocket: The WebSocket connection
            channel: Channel name to subscribe to (e.g., "pipeline.my_id.events")
        """
        await websocket.accept()
        # For FastAPI, we need to store mapping from websocket->channels
        # For simplicity, treat as single channel per connection (original model)
        await self.channel_registry.subscribe(channel, websocket)
        logger.info("Client connected to channel %s", channel)

    async def disconnect(self, websocket: WebSocket, channel: str) -> None:
        """Disconnect a WebSocket client from a channel.

        Args:
            websocket: The WebSocket connection to close
            channel: The channel the client was subscribed to
        """
        await self.channel_registry.unsubscribe(channel, websocket)
        logger.info("Client disconnected from channel %s", channel)

    async def broadcast_event(self, channel: str, event: dict[str, Any]) -> None:
        """Broadcast an event to all clients subscribed to a channel.

        Args:
            channel: Channel to broadcast to
            event: The event data to broadcast
        """
        await self.channel_registry.broadcast(channel, event)

    def get_client_count(self, channel: str) -> int:
        """Get the number of clients subscribed to a channel.

        Args:
            channel: Channel name

        Returns:
            Number of connected clients
        """
        return self.channel_registry.get_subscriber_count(channel)

    def get_channel_ids(self) -> list[str]:
        """Get all channels with active subscriptions.

        Returns:
            List of channel names
        """
        return self.channel_registry.get_all_channels()

    def get_pipeline_ids(self) -> list[str]:
        """Get all pipeline IDs with active subscriptions.

        Alias for get_channel_ids for backward compatibility.

        Returns:
            List of pipeline IDs (channel names)
        """
        return self.get_channel_ids()

    async def close_all(self) -> None:
        """Close all WebSocket connections."""
        await self.channel_registry.broadcast(
            "system",
            {"type": "shutdown", "message": "Server shutting down"},
        )
        self._active = False
        logger.info("All WebSocket connections closing")


# Global FastAPI WebSocket manager instance
_fastapi_ws_manager: FastAPIWebSocketManager | None = None


def get_fastapi_ws_manager(
    channel_registry: ChannelRegistry | None = None,
) -> FastAPIWebSocketManager:
    """Get or create the global FastAPI WebSocket manager.

    Args:
        channel_registry: Optional shared channel registry

    Returns:
        The singleton FastAPIWebSocketManager instance
    """
    global _fastapi_ws_manager  # noqa: PLW0603
    if _fastapi_ws_manager is None:
        _fastapi_ws_manager = FastAPIWebSocketManager(channel_registry)
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
