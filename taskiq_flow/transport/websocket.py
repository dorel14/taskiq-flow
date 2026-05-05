"""WebSocket transport for real-time pipeline events.

Author: SoniqueBay Team
Version: 0.4.0
"""

import logging
import time
from typing import Any

from taskiq_flow.hooks.events import PipelineEvent

logger = logging.getLogger(__name__)


class WebSocketTransport:
    """
    WebSocket transport for pipeline events.

    Sends real-time events to connected WebSocket clients.
    """

    def __init__(self) -> None:
        self._clients: dict[str, Any] = {}
        self._event_queue: list[PipelineEvent] = []

    async def connect(self, client_id: str, websocket: Any) -> None:
        """Register a new WebSocket client."""
        self._clients[client_id] = {
            "websocket": websocket,
            "connected_at": time.time(),
            "last_seen": time.time(),
        }
        logger.info("WebSocket client connected", extra={"client_id": client_id})

    async def disconnect(self, client_id: str) -> None:
        """Unregister a WebSocket client."""
        if client_id in self._clients:
            del self._clients[client_id]
            logger.info("WebSocket client disconnected", extra={"client_id": client_id})

    async def broadcast(
        self,
        event: PipelineEvent,
        filters: dict[str, Any] | None = None,
    ) -> None:
        """Broadcast event to all connected clients or filtered clients."""
        event_data = (
            event.model_dump() if hasattr(event, "model_dump") else event.dict()
        )

        for client_id, client_info in list(self._clients.items()):
            websocket = client_info.get("websocket")
            if websocket is None:
                continue

            # Apply filters if provided
            if filters and not self._match_filters(event_data, filters):
                continue

            try:
                await websocket.send_json(event_data)
                client_info["last_seen"] = time.time()
            except Exception as e:
                logger.warning(
                    "Failed to send event to client",
                    extra={"client_id": client_id, "error": str(e)},
                )
                # Remove disconnected clients
                del self._clients[client_id]

    async def send_to_client(self, client_id: str, event: PipelineEvent) -> None:
        """Send event to a specific client."""
        if client_id not in self._clients:
            logger.warning("Client not found", extra={"client_id": client_id})
            return

        client_info = self._clients[client_id]
        websocket = client_info.get("websocket")
        if websocket is None:
            return

        event_data = (
            event.model_dump() if hasattr(event, "model_dump") else event.dict()
        )
        try:
            await websocket.send_json(event_data)
            client_info["last_seen"] = time.time()
        except Exception as e:
            logger.warning(
                "Failed to send event to client",
                extra={"client_id": client_id, "error": str(e)},
            )

    def _match_filters(self, event: dict[str, Any], filters: dict[str, Any]) -> bool:
        """Check if event matches filter criteria."""
        return all(event.get(key) == value for key, value in filters.items())

    def get_client_count(self) -> int:
        """Get number of connected clients."""
        return len(self._clients)

    def get_client_ids(self) -> list[str]:
        """Get list of connected client IDs."""
        return list(self._clients.keys())
