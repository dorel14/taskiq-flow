"""Gestionnaire de hooks pour les événements de pipeline.

Ce module fournit HookManager qui permet d'enregistrer des callbacks
sur les événements de cycle de vie des pipelines. Il gère un
système de publication-abonnement avec weak references pour éviter
les fuites mémoire, et supporte des transports multiples (WebSocket,
etc.) pour la diffusion des événements.

Auteur: SoniqueBay Team
Version: 0.3.1
"""

import asyncio
import logging
import weakref
from collections.abc import Awaitable, Callable
from typing import Any

from .events import PipelineEvent

try:
    from taskiq_flow.integration.websocket.server import get_websocket_server

    WEBSOCKET_AVAILABLE = True
except ImportError:
    WEBSOCKET_AVAILABLE = False

logger = logging.getLogger(__name__)


class TransportMiddleware:
    """Pluggable transport middleware for event broadcasting.

    Supports multiple transport types:
    - websocket: WebSocket broadcast
    - http_stream: HTTP streaming
    - redis_pubsub: Redis pub/sub
    """

    def __init__(self, transport_type: str = "websocket", **kwargs: Any) -> None:
        """Initialize transport middleware.

        Args:
            transport_type: Type of transport (websocket, http_stream, redis_pubsub)
            **kwargs: Transport-specific configuration
        """
        self.transport_type = transport_type
        self.config = kwargs
        self._transport = self._create_transport()

    def _create_transport(self) -> Any:
        """Create transport instance based on type."""
        if self.transport_type == "websocket":
            return self._create_websocket_transport()
        if self.transport_type == "http_stream":
            return self._create_http_stream_transport()
        if self.transport_type == "redis_pubsub":
            return self._create_redis_pubsub_transport()
        raise ValueError(f"Unsupported transport type: {self.transport_type}")

    def _create_websocket_transport(self) -> Any:
        """Create WebSocket transport."""
        if not WEBSOCKET_AVAILABLE:
            logger.warning("picows not available, WebSocket transport disabled")
            return None
        return get_websocket_server(
            host=self.config.get("host", "127.0.0.1"),
            port=self.config.get("port", 8765),
        )

    def _create_http_stream_transport(self) -> Any:
        """Create HTTP stream transport."""
        # Placeholder for HTTP streaming implementation
        logger.info("HTTP stream transport configured (not yet implemented)")
        return None

    def _create_redis_pubsub_transport(self) -> Any:
        """Create Redis pub/sub transport."""
        # Placeholder for Redis pub/sub implementation
        logger.info("Redis pub/sub transport configured (not yet implemented)")
        return None

    async def broadcast(self, event: PipelineEvent) -> None:
        """Broadcast event through transport."""
        if self._transport is None:
            return

        try:
            if self.transport_type == "websocket":
                await self._transport.broadcast_event(
                    event.pipeline_id,
                    event.model_dump(),
                )
        except Exception as e:
            logger.error(f"Failed to broadcast event via {self.transport_type}: {e}")


class HookManager:
    """Manager for registering and dispatching pipeline event hooks."""

    def __init__(self) -> None:
        # Use WeakSet to prevent memory leaks - callbacks will be automatically
        # removed when the objects they belong to are garbage collected
        # For bound methods and local functions, we need to keep a strong reference
        # to prevent them from being garbage collected while still in use
        self._callbacks: dict[str, weakref.WeakSet[Callable[[PipelineEvent], Any]]] = {}
        self._strong_refs: dict[str, list[Callable[[PipelineEvent], Any]]] = {}
        self._lock = asyncio.Lock()
        self._transports: list[TransportMiddleware] = []
        self._event_filters: dict[str, list[Callable[[PipelineEvent], bool]]] = {}

    def register(
        self,
        event_type: str,
        callback: (
            Callable[[PipelineEvent], Any] | Callable[[PipelineEvent], Awaitable[Any]]
        ),
        filter_fn: Callable[[PipelineEvent], bool] | None = None,
    ) -> None:
        """Register a callback for an event type with optional filtering.

        Args:
            event_type: Type of event to register for
            callback: Callback function to invoke
            filter_fn: Optional filter function to apply before invoking callback
        """
        if event_type not in self._callbacks:
            self._callbacks[event_type] = weakref.WeakSet()
            self._strong_refs[event_type] = []
            self._event_filters[event_type] = []

        # Try to add to WeakSet - if it fails (callback not weakref-able),
        # we'll fall back to a regular approach with periodic cleanup
        try:
            self._callbacks[event_type].add(callback)
            # Also keep a strong reference for local functions and bound methods
            # to prevent them from being garbage collected while registered
            self._strong_refs[event_type].append(callback)
            if filter_fn:
                self._event_filters[event_type].append(filter_fn)
            logger.debug(f"Registered callback for event {event_type}")
        except TypeError:
            # Callback is not weakref-able (e.g., lambda, built-in function)
            # For now, we'll skip registration to prevent memory leaks
            logger.warning(
                f"Cannot register non-weakref-able callback for {event_type}",
            )

    def unregister(
        self,
        event_type: str,
        callback: Callable[[PipelineEvent], Any],
    ) -> None:
        """Unregister a callback for an event type."""
        if event_type in self._callbacks:
            try:
                self._callbacks[event_type].discard(
                    callback,
                )  # discard is safer than remove for WeakSet
                logger.debug(f"Unregistered callback for event {event_type}")
            except (ValueError, TypeError):
                pass  # Callback not found or not weakref-able

    def add_transport(self, transport: TransportMiddleware) -> None:
        """Add a transport middleware for event broadcasting.

        Args:
            transport: Transport middleware to add
        """
        self._transports.append(transport)
        logger.info(f"Added {transport.transport_type} transport")

    def remove_transport(self, transport: TransportMiddleware) -> None:
        """Remove a transport middleware.

        Args:
            transport: Transport middleware to remove
        """
        if transport in self._transports:
            self._transports.remove(transport)
            logger.info(f"Removed {transport.transport_type} transport")

    async def dispatch(self, event: PipelineEvent) -> None:
        """Dispatch an event to all registered callbacks and transports."""
        event_type = event.__class__.__name__

        # Get callbacks outside the lock to avoid holding it during execution
        async with self._lock:
            # Convert WeakSet to list to avoid modification during iteration
            callbacks = list(self._callbacks.get(event_type, weakref.WeakSet()))
            filters = self._event_filters.get(event_type, [])

        # Apply filters if any
        if filters and not all(f(event) for f in filters):
            logger.debug(
                f"Event {event_type} filtered out for pipeline {event.pipeline_id}",
            )
            return

        if not callbacks and not self._transports:
            return

        # Dispatch to callbacks concurrently but safely
        tasks = []
        for cb in callbacks:
            # Skip if callback was garbage collected
            if cb is not None:
                task = asyncio.create_task(self._safe_call(cb, event))
                tasks.append(task)

        # Broadcast to transports
        for transport in self._transports:
            task = asyncio.create_task(transport.broadcast(event))
            tasks.append(task)

        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            # Log any exceptions that occurred during dispatch
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    logger.error(
                        f"Callback/transport {i} failed for {event_type}: {result}",
                    )

    async def _safe_call(
        self,
        callback: (
            Callable[[PipelineEvent], Any] | Callable[[PipelineEvent], Awaitable[Any]]
        ),
        event: PipelineEvent,
    ) -> None:
        """Call a callback safely, handling exceptions."""
        try:
            if asyncio.iscoroutinefunction(callback):
                await callback(event)
            else:
                callback(event)
        except Exception as exc:
            # Re-raise to be caught by the dispatch method
            raise exc

    def get_callback_count(self, event_type: str | None = None) -> int:
        """Get the number of registered callbacks."""
        if event_type:
            return len(self._callbacks.get(event_type, weakref.WeakSet()))
        return sum(len(callbacks) for callbacks in self._callbacks.values())

    def get_transport_count(self) -> int:
        """Get the number of registered transports."""
        return len(self._transports)
