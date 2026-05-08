"""Intégration WebSocket pour les événements de pipeline en temps réel.

Fournit des implémentations WebSocket pour la diffusion des événements
de pipeline aux clients connectés.

- FastAPI WebSocket: Intégration avec FastAPI pour un API unifiée
- picows: Serveur WebSocket autonome basé sur picows

Author: SoniqueBay Team
Version: 0.4.5
"""

from taskiq_flow.integration.websocket.fastapi_ws import (
    FastAPIWebSocketManager,
    fastapi_websocket_endpoint,
    get_fastapi_ws_manager,
)
from taskiq_flow.integration.websocket.server import (
    PipelineWebSocketServer,
    get_websocket_server,
)

__all__ = [
    "FastAPIWebSocketManager",
    "PipelineWebSocketServer",
    "fastapi_websocket_endpoint",
    "get_fastapi_ws_manager",
    "get_websocket_server",
]
