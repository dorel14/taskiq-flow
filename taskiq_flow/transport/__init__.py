"""
Transport layer for real-time pipeline events.

This module provides pluggable transport implementations for streaming
pipeline events to clients.

Author: SoniqueBay Team
Version: 1.0.2
"""

from typing import Any, Protocol

from taskiq_flow.errors import ErrorHandlingMode
from taskiq_flow.hooks.events import PipelineEvent
from taskiq_flow.middlewares.retry import PipelineRetryMiddleware
from taskiq_flow.transport.http_stream import (
    EventQueue,
    HTTPStreamTransport,
    get_http_stream_transport,
)
from taskiq_flow.transport.redis_pubsub import RedisPubSubTransport
from taskiq_flow.transport.websocket import WebSocketTransport


class TransportProtocol(Protocol):
    """Base protocol for event transport."""

    async def connect(self) -> None:
        """Connect to the transport."""
        ...

    async def disconnect(self) -> None:
        """Disconnect from the transport."""
        ...

    async def broadcast(
        self,
        event: PipelineEvent,
        filters: dict[str, Any] | None = None,
    ) -> None:
        """Broadcast an event to all clients or filtered clients."""
        ...

    async def send_to_client(
        self,
        client_id: str,
        event: PipelineEvent,
    ) -> None:
        """Send an event to a specific client."""
        ...


class TransportProtocolAsync(Protocol):
    """Async transport protocol for WebSocket etc."""

    async def send_json(self, data: dict[str, Any]) -> None:
        """Send JSON data to client."""
        ...


class ClientInfo:
    """Information about a connected client."""

    def __init__(
        self,
        client_id: str,
        transport: Any,
        last_seen: float,
    ) -> None:
        self.client_id = client_id
        self.transport = transport
        self.last_seen = last_seen


__all__ = [
    "ClientInfo",
    "ErrorHandlingMode",
    "EventQueue",
    "HTTPStreamTransport",
    "PipelineRetryMiddleware",
    "RedisPubSubTransport",
    "TransportProtocol",
    "TransportProtocolAsync",
    "WebSocketTransport",
    "get_http_stream_transport",
]
