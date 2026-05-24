"""
WebSocket integration for real-time pipeline events.

Provides WebSocket implementations for broadcasting pipeline events
to connected clients.

- FastAPI WebSocket: Integration with FastAPI for unified API

Author: SoniqueBay Team
Version: 1.1.0
"""

from taskiq_flow.integration.websocket.fastapi_ws import (
    FastAPIWebSocketManager,
    fastapi_websocket_endpoint,
    get_fastapi_ws_manager,
)

__all__ = [
    "FastAPIWebSocketManager",
    "fastapi_websocket_endpoint",
    "get_fastapi_ws_manager",
]
