"""
Serveur WebSocket pour la diffusion d'événements de pipeline.

Implémentation serveur asynchrone utilisant picows. Gère les
connexions client, les abonnements par canal (channel) et la diffusion
d'événements. Supporte l'authentification, les ACLs, les limites
de connexion et SSL/TLS.

Auteur: SoniqueBay Team
Version: 1.0.2
"""

import asyncio
import json
import logging
import ssl
from typing import Any, cast

from picows import (
    WSCloseCode,
    WSFrame,
    WSListener,
    WSMsgType,
    WSTransport,
    WSUpgradeRequest,
    ws_create_server,
)
from starlette.requests import Request

from taskiq_flow.integration.websocket.channel_registry import ChannelRegistry
from taskiq_flow.metrics.collector import MetricsCollector
from taskiq_flow.security.auth import AuthProvider
from taskiq_flow.security.authorization import PipelineAuthorization

logger = logging.getLogger(__name__)


class PipelineWebSocketListener(WSListener):
    """WebSocket listener for pipeline event broadcasting with auth."""

    def __init__(
        self,
        server: "PipelineWebSocketServer",
        auth_provider: AuthProvider | None = None,
        authorization: PipelineAuthorization | None = None,
    ) -> None:
        """
        Initialize listener.

        Args:
            server: The parent WebSocket server
            auth_provider: Optional auth provider for token verification
            authorization: Optional authorization manager for pipeline ACLs

        """
        self.server = server
        self.auth_provider = auth_provider
        self.authorization = authorization
        self.pipeline_id: str | None = None
        self.transport: WSTransport | None = None
        self.authenticated = False
        self.user: dict[str, Any] | None = None

    def on_ws_connected(self, transport: Any) -> None:
        """Handle new WebSocket connection."""
        self.transport = transport
        peername = (
            transport.underlying_transport.get_extra_info("peername")
            if transport.underlying_transport
            else "unknown"
        )
        logger.info("New WebSocket client connected from %s", peername)

    def on_ws_frame(self, transport: WSTransport, frame: WSFrame) -> None:  # noqa: PLR0912
        """Handle incoming WebSocket frame."""
        if frame.msg_type == WSMsgType.TEXT:
            try:
                data = json.loads(frame.get_payload_as_ascii_text())
                action = data.get("action", "subscribe")
                if not self.authenticated and self.auth_provider:
                    if action != "auth":
                        transport.send(
                            WSMsgType.TEXT,
                            json.dumps({"error": "Authentication required"}).encode(),
                        )
                        transport.send_close(
                            WSCloseCode.POLICY_VIOLATION, b"Auth required"
                        )
                        return
                    token = data.get("token")
                    if not token:
                        transport.send(
                            WSMsgType.TEXT,
                            json.dumps({"error": "Token missing"}).encode(),
                        )
                        transport.send_close(
                            WSCloseCode.POLICY_VIOLATION, b"Token missing"
                        )
                        return
                    # Validate token using auth_provider
                    # Schedule async verification in event loop
                    _ = asyncio.get_event_loop().create_task(
                        self._verify_auth(token, transport)
                    )
                    return
                if action == "auth":
                    transport.send(
                        WSMsgType.TEXT,
                        json.dumps({"error": "Already authenticated"}).encode(),
                    )
                    return
                if action == "subscribe":
                    channel = data.get("channel")
                    if channel:
                        if self.authorization and self.user:
                            pipeline_id = channel.replace("pipeline.", "").split(".")[0]
                            if not self.authorization.can_read(pipeline_id, self.user):
                                transport.send(
                                    WSMsgType.TEXT,
                                    json.dumps(
                                        {"error": "Access denied to pipeline"}
                                    ).encode(),
                                )
                                return
                        task = asyncio.create_task(
                            self.server.subscribe(channel, cast(Any, transport))
                        )
                        task.add_done_callback(lambda t: None)
                        self.pipeline_id = channel
                        logger.info("Client subscribed to channel %s", channel)
                        MetricsCollector().websocket_message(
                            pipeline_id or "unknown",
                            "in",
                            "subscribe",
                        )
                elif action == "unsubscribe":
                    channel = data.get("channel")
                    if channel:
                        task = asyncio.create_task(
                            self.server.unsubscribe(channel, cast(Any, transport))
                        )
                        task.add_done_callback(lambda t: None)
                        logger.info("Client unsubscribed from channel %s", channel)
                else:
                    logger.warning("Unknown action: %s", action)
            except (json.JSONDecodeError, KeyError):
                logger.warning("Invalid message format")
        elif frame.msg_type == WSMsgType.CLOSE:
            if self.pipeline_id:
                task = asyncio.create_task(
                    self.server.unsubscribe(self.pipeline_id, transport)
                )
                task.add_done_callback(lambda t: None)
            task = asyncio.create_task(self.server.remove_connection(transport))
            task.add_done_callback(lambda t: None)
            logger.info("Client disconnected")
            transport.send_close(frame.get_close_code(), frame.get_close_message())
            transport.disconnect()

    async def _verify_auth(self, token: str, transport: Any) -> None:
        """Verify authentication token asynchronously."""
        try:
            request = Request(
                scope={
                    "type": "http",
                    "method": "GET",
                    "path": "/ws",
                    "headers": {
                        "authorization": f"Bearer {token}",
                    },
                }
            )
            if self.auth_provider is None:
                raise ValueError("Auth provider not configured")
            result = self.auth_provider.verify(request)
            if asyncio.iscoroutine(result):
                user_context = await result
            else:
                user_context = result
            if user_context is None:
                raise ValueError("Auth provider returned None")
            self.authenticated = True
            self.user = user_context
            transport.send(
                WSMsgType.TEXT,
                json.dumps({"status": "authenticated"}).encode(),
            )
        except Exception as e:
            logger.warning("Auth failed: %s", e)
            transport.send(
                WSMsgType.TEXT,
                json.dumps({"error": "Invalid token"}).encode(),
            )
            transport.send_close(WSCloseCode.POLICY_VIOLATION, b"Auth failed")

    def on_ws_connection_lost(
        self,
        transport: Any,
        exc: Exception | None,
    ) -> None:
        """Handle connection loss."""
        if self.pipeline_id:
            task = asyncio.create_task(
                self.server.unsubscribe(self.pipeline_id, transport)
            )
            task.add_done_callback(lambda t: None)
        task = asyncio.create_task(self.server.remove_connection(transport))
        task.add_done_callback(lambda t: None)
        logger.info("Client connection lost: %s", exc)


class PipelineWebSocketServer:
    """
    WebSocket server for broadcasting pipeline events.

    Supports authentication, authorization, connection limits, SSL/TLS,
    and hierarchical channel subscriptions (compatible with chanx).
    """

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 8765,
        channel_registry: ChannelRegistry | None = None,
        auth_provider: AuthProvider | None = None,
        authorization: PipelineAuthorization | None = None,
        max_connections: int = 1000,
        ssl_cert: str | None = None,
        ssl_key: str | None = None,
    ) -> None:
        """
        Initialize the WebSocket server.

        Args:
            host: Host to bind on
            port: Port to listen on
            channel_registry: Registry for channels (created if None)
            auth_provider: Optional auth provider for token verification
            authorization: Optional authorization manager for pipeline ACLs
            max_connections: Maximum concurrent WebSocket connections
            ssl_cert: Path to SSL certificate file (enables TLS if provided)
            ssl_key: Path to SSL private key file

        """
        self.host = host
        self.port = port
        self.channel_registry = channel_registry or ChannelRegistry()
        self.auth_provider = auth_provider
        self.authorization = authorization
        self.max_connections = max_connections
        self.ssl_cert = ssl_cert
        self.ssl_key = ssl_key

        self._connections: set[WSTransport] = set()
        self._lock = asyncio.Lock()
        self._server: asyncio.Server | None = None

    def listener_factory(self, request: WSUpgradeRequest) -> WSListener:
        """Create a new listener for each WebSocket connection."""
        return PipelineWebSocketListener(self, self.auth_provider, self.authorization)

    async def subscribe(self, channel: str, transport: Any) -> None:
        """Subscribe a transport to a channel with ACL check if configured."""
        await self.channel_registry.subscribe(channel, transport)

    async def unsubscribe(self, channel: str, transport: Any) -> None:
        """Unsubscribe a transport from a channel."""
        await self.channel_registry.unsubscribe(channel, transport)

    async def broadcast_event(self, pipeline_id: str, event: dict[str, Any]) -> None:
        """
        Broadcast an event to all clients subscribed to a pipeline.

        Args:
            pipeline_id: ID of the pipeline
            event: Event data to broadcast

        """
        channel = f"pipeline.{pipeline_id}"
        await self.channel_registry.broadcast(channel, event)
        MetricsCollector().websocket_message(
            pipeline_id, "out", event.get("type", "event")
        )

    async def add_connection(self, transport: WSTransport) -> None:
        """
        Register a new connection and enforce limits.

        Args:
            transport: WebSocket transport

        Raises:
            ConnectionError: If max connections exceeded

        """
        async with self._lock:
            if len(self._connections) >= self.max_connections:
                logger.warning("Connection limit reached: %d", self.max_connections)
                transport.send_close(
                    WSCloseCode.TRY_AGAIN_LATER, b"Connection limit exceeded"
                )
                transport.disconnect()
                raise ConnectionError("Max connections exceeded")
            self._connections.add(transport)

    async def remove_connection(self, transport: WSTransport) -> None:
        """Remove a connection from tracking."""
        async with self._lock:
            self._connections.discard(transport)

    def get_connection_count(self) -> int:
        """Get current number of active connections."""
        return len(self._connections)

    async def start_server(
        self,
        host: str | None = None,
        port: int | None = None,
    ) -> asyncio.Server:
        """
        Start the WebSocket server.

        Args:
            host: Override host
            port: Override port

        Returns:
            The asyncio Server instance

        """
        actual_host = host if host is not None else self.host
        actual_port = port if port is not None else self.port

        ssl_context = None
        if self.ssl_cert and self.ssl_key:
            ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
            ssl_context.load_cert_chain(self.ssl_cert, self.ssl_key)
            logger.info("WebSocket server using SSL/TLS")

        self._server = await ws_create_server(
            self.listener_factory,
            actual_host,
            actual_port,
            ssl=ssl_context,
        )
        logger.info("WebSocket server started on ws://%s:%s", actual_host, actual_port)
        return self._server


# Global server instance
_server: PipelineWebSocketServer | None = None


def get_websocket_server(
    host: str = "127.0.0.1",
    port: int = 8765,
    **kwargs: Any,
) -> PipelineWebSocketServer:
    """Get or create the global WebSocket server instance."""
    global _server  # noqa: PLW0603
    if _server is None:
        _server = PipelineWebSocketServer(host, port, **kwargs)
    return _server
