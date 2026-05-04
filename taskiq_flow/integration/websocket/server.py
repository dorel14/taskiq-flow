"""Serveur WebSocket pour la diffusion d'événements de pipeline.

Implémentation serveur asynchrone utilisant picows. Gère les
connexions client, les abonnements par pipeline et la diffusion
d'événements.

Auteur: SoniqueBay Team
Version: 0.3.2
"""

import asyncio
import json
import logging
from collections import defaultdict
from typing import Any

from picows import (
    WSFrame,
    WSListener,
    WSMsgType,
    WSTransport,
    WSUpgradeRequest,
    ws_create_server,
)

logger = logging.getLogger(__name__)


class PipelineWebSocketListener(WSListener):
    """WebSocket listener for pipeline event broadcasting."""

    def __init__(self, server: "PipelineWebSocketServer") -> None:
        self.server = server
        self.pipeline_id: str | None = None

    def on_ws_connected(self, transport: WSTransport) -> None:
        """Handle new WebSocket connection."""
        logger.info("New WebSocket client connected")

    def on_ws_frame(self, transport: WSTransport, frame: WSFrame) -> None:
        """Handle incoming WebSocket frame."""
        if frame.msg_type == WSMsgType.TEXT:
            try:
                data = json.loads(frame.get_payload_as_ascii_text())
                pipeline_id = data.get("pipeline_id")
                if pipeline_id:
                    self.pipeline_id = pipeline_id
                    # Create task but don't store reference - fire and forget pattern
                    task = asyncio.create_task(
                        self.server.add_client(pipeline_id, transport),
                    )
                    task.add_done_callback(lambda t: None)
                    logger.info("Client subscribed to pipeline %s", pipeline_id)
            except (json.JSONDecodeError, KeyError):
                logger.warning("Invalid subscription message")
        elif frame.msg_type == WSMsgType.CLOSE:
            if self.pipeline_id:
                task = asyncio.create_task(
                    self.server.remove_client(self.pipeline_id, transport),
                )
                task.add_done_callback(lambda t: None)
                logger.info("Client unsubscribed from pipeline %s", self.pipeline_id)
            transport.send_close(frame.get_close_code(), frame.get_close_message())
            transport.disconnect()

    def on_ws_connection_lost(
        self,
        transport: WSTransport,
        exc: Exception | None,
    ) -> None:
        """Handle connection loss."""
        if self.pipeline_id:
            task = asyncio.create_task(
                self.server.remove_client(self.pipeline_id, transport),
            )
            task.add_done_callback(lambda t: t.exception() if t.exception() else None)
            logger.info("Client lost for pipeline %s", self.pipeline_id)


class PipelineWebSocketServer:
    """WebSocket server for broadcasting pipeline events."""

    def __init__(self, host: str = "127.0.0.1", port: int = 8765) -> None:
        self.clients: dict[str, set[WSTransport]] = defaultdict(set)
        self._lock = asyncio.Lock()
        self.host = host
        self.port = port

    def listener_factory(self, request: WSUpgradeRequest) -> WSListener:
        """Create a new listener for each WebSocket connection."""
        return PipelineWebSocketListener(self)

    async def add_client(self, pipeline_id: str, transport: WSTransport) -> None:
        """Add a client to a pipeline group."""
        async with self._lock:
            self.clients[pipeline_id].add(transport)

    async def remove_client(self, pipeline_id: str, transport: WSTransport) -> None:
        """Remove a client from a pipeline group."""
        async with self._lock:
            self.clients[pipeline_id].discard(transport)
            if not self.clients[pipeline_id]:
                del self.clients[pipeline_id]

    async def broadcast_event(self, pipeline_id: str, event: dict[str, Any]) -> None:
        """Broadcast an event to all clients subscribed to a pipeline."""
        async with self._lock:
            clients = self.clients[pipeline_id].copy()

        if not clients:
            return

        message = json.dumps(event)
        tasks = []
        for transport in clients:
            task = asyncio.create_task(self._send_to_client(transport, message))
            tasks.append(task)

        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            # Log any exceptions that occurred
            for result in results:
                if isinstance(result, Exception):
                    logger.warning(f"Error broadcasting to client: {result}")

    async def _send_to_client(self, transport: WSTransport, message: str) -> None:
        """Send a message to a specific client."""
        try:
            transport.send(WSMsgType.TEXT, message.encode("utf-8"))
        except Exception as exc:
            logger.warning("Failed to send message to client: %s", exc)

    async def start_server(
        self,
        host: str | None = None,
        port: int | None = None,
    ) -> asyncio.Server:
        """Start the WebSocket server."""
        actual_host = host if host is not None else self.host
        actual_port = port if port is not None else self.port
        server = await ws_create_server(self.listener_factory, actual_host, actual_port)
        logger.info("WebSocket server started on ws://%s:%s", actual_host, actual_port)
        return server


# Global server instance
_server: PipelineWebSocketServer | None = None


def get_websocket_server(
    host: str = "127.0.0.1",
    port: int = 8765,
) -> PipelineWebSocketServer:
    """Get or create the global WebSocket server instance.

    Server configuration is only used on first creation.
    """
    global _server  # noqa: PLW0603
    if _server is None:
        _server = PipelineWebSocketServer(host, port)
    # _server should never be None after creation
    return _server

