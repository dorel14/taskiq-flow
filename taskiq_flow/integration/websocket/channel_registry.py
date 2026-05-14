"""
Gestionnaire de canaux (channels) pour WebSocket.

Ce module fournit un registre de canaux avec support hiérarchique
et abonnement par motif (wildcards) pour une publication/diffusion
flexible des événements de pipeline.

Auteur: SoniqueBay Team
Version: 1.0.2
"""

import asyncio
import fnmatch
import json
import logging
from collections import defaultdict
from typing import Any, Protocol

logger = logging.getLogger(__name__)


class WebSocketLike(Protocol):
    """Protocol for WebSocket-like objects that can send text."""

    async def send_text(self, data: str) -> None: ...



class ChannelRegistry:
    """
    Registre de canaux avec support de motifs (patterns).

    Permet aux clients de s'abonner à des canaux spécifiques ou par motif,
    par exemple: "pipeline.my_id.events" ou "pipeline.*.steps".

    Attributes:
        _channels: Dictionnaire canal -> ensemble de WebSocket
        _pattern_subscriptions: Liste de (pattern, websocket)
        _lock: Verrou pour accès concurrent

    """

    def __init__(self) -> None:
        """Initialise le registre de canaux."""
        self._channels: dict[str, set[WebSocketLike]] = defaultdict(set)
        self._pattern_subscriptions: list[tuple[str, WebSocketLike]] = []
        self._lock = asyncio.Lock()
        self._active = True

    async def subscribe(self, channel: str, websocket: WebSocketLike) -> None:
        """
        Abonne un WebSocket à un canal exact.

        Args:
            channel: Nom du canal (ex: "pipeline.123.events")
            websocket: Connexion WebSocket à abonner

        """
        async with self._lock:
            self._channels[channel].add(websocket)
        logger.debug("WebSocket subscribed to channel %s", channel)

    async def unsubscribe(self, channel: str, websocket: WebSocketLike) -> None:
        """
        Désabonne un WebSocket d'un canal.

        Args:
            channel: Nom du canal
            websocket: Connexion WebSocket à désabonner

        """
        async with self._lock:
            self._channels[channel].discard(websocket)
            if not self._channels[channel]:
                del self._channels[channel]
        logger.debug("WebSocket unsubscribed from channel %s", channel)

    async def subscribe_pattern(self, pattern: str, websocket: WebSocketLike) -> None:
        """
        Abonne un WebSocket à un motif de canal (wildcard).

        Args:
            pattern: Motif de canal (ex: "pipeline.*.events")
            websocket: Connexion WebSocket à abonner

        """
        async with self._lock:
            self._pattern_subscriptions.append((pattern, websocket))
        logger.debug("WebSocket subscribed to pattern %s", pattern)

    async def unsubscribe_pattern(self, pattern: str, websocket: WebSocketLike) -> None:
        """
        Désabonne un WebSocket d'un motif.

        Args:
            pattern: Motif de canal
            websocket: Connexion WebSocket à désabonner

        """
        async with self._lock:
            self._pattern_subscriptions = [
                (p, ws)
                for p, ws in self._pattern_subscriptions
                if not (p == pattern and ws == websocket)
            ]
        logger.debug("WebSocket unsubscribed from pattern %s", pattern)

    async def unsubscribe_all(self, websocket: WebSocketLike) -> None:
        """Désabonne un WebSocket de tous les canaux et motifs."""
        async with self._lock:
            # Remove from exact channels
            for channel, clients in list(self._channels.items()):
                clients.discard(websocket)
                if not clients:
                    del self._channels[channel]
            # Remove from pattern subscriptions
            self._pattern_subscriptions = [
                (p, ws) for p, ws in self._pattern_subscriptions if ws != websocket
            ]
        logger.debug("WebSocket unsubscribed from all channels")

    def _matches(self, channel: str, pattern: str) -> bool:
        """
        Vérifie si un canal correspond à un motif.

        Args:
            channel: Nom du canal
            pattern: Motif avec wildcards (utilise fnmatch)

        Returns:
            True si le canal correspond au motif

        """
        return fnmatch.fnmatch(channel, pattern)

    async def broadcast(self, channel: str, message: dict[str, Any]) -> None:
        """
        Diffuse un message à tous les abonnés d'un canal.

        Envoie le message aux WebSockets abonnés au canal exact
        ainsi qu'à ceux abonnés via des motifs correspondants.

        Args:
            channel: Canal de diffusion
            message: Message à diffuser (sera sérialisé JSON)

        """
        payload = json.dumps(message)
        disconnected: list[WebSocketLike] = []

        # Envoyer aux abonnés exacts
        async with self._lock:
            exact_clients = list(self._channels.get(channel, []))
            # Copier les patterns et clients correspondants
            pattern_matches = [
                ws
                for pattern, ws in self._pattern_subscriptions
                if self._matches(channel, pattern)
            ]

        all_clients = set(exact_clients) | set(pattern_matches)

        for websocket in all_clients:
            try:
                await websocket.send_text(payload)
            except Exception as e:
                logger.warning("Failed to send message to client: %s", e)
                disconnected.append(websocket)

        # Nettoyer les connexions fermées
        if disconnected:
            async with self._lock:
                for ws in disconnected:
                    # Remove from all exact channels
                    for ch, clients in list(self._channels.items()):
                        clients.discard(ws)
                        if not clients:
                            del self._channels[ch]
                    # Remove from patterns
                    self._pattern_subscriptions = [
                        (p, w) for p, w in self._pattern_subscriptions if w != ws
                    ]

    def get_subscriber_count(self, channel: str) -> int:
        """
        Retourne le nombre d'abonnés à un canal exact.

        Args:
            channel: Nom du canal

        Returns:
            Nombre de WebSocket abonnés

        """
        return len(self._channels.get(channel, set()))

    def get_all_channels(self) -> list[str]:
        """
        Retourne la liste de tous les canaux avec abonnés.

        Returns:
            Liste des noms de canaux

        """
        return list(self._channels.keys())

    def is_empty(self) -> bool:
        """Vérifie s'il n'y a aucun abonné."""
        return len(self._channels) == 0 and len(self._pattern_subscriptions) == 0


__all__ = ["ChannelRegistry"]
