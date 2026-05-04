"""Intégration WebSocket pour les événements de pipeline en temps réel.

Fournit un serveur WebSocket asynchrone basé sur picows pour
diffuser les événements de pipeline aux clients connectés.

Auteur: SoniqueBay Team
Version: 0.3.2
"""

from taskiq_flow.integration.websocket.server import (
    PipelineWebSocketServer,
    get_websocket_server,
)

__all__ = ["PipelineWebSocketServer", "get_websocket_server"]

