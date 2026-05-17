"""
Bridge entre HookManager et le serveur WebSocket pour les événements de pipeline.

Ce module définit WebSocketHookBridge qui écoute les événements
du pipeline via HookManager et les diffuse aux clients WebSocket
connectés. Gère également la reconnexion et la file d'attente
des événements pendant la déconnexion.

Supports both FastAPI WebSocket and picows implementations.

Author: SoniqueBay Team
Version: 1.0.2
"""

import asyncio
import logging
from collections import deque
from typing import Any

from taskiq_flow.hooks.events import PipelineEvent
from taskiq_flow.hooks.manager import HookManager

# Try to import both WebSocket implementations
try:
    from taskiq_flow.integration.websocket.fastapi_ws import (
        get_fastapi_ws_manager,
    )

    FASTAPI_WS_AVAILABLE = True
except ImportError:
    FASTAPI_WS_AVAILABLE = False

try:
    from taskiq_flow.integration.websocket.server import get_websocket_server

    PICOWS_AVAILABLE = True
except ImportError:
    PICOWS_AVAILABLE = False

logger = logging.getLogger(__name__)

_MISSING = object()  # Sentinel for missing attributes


class WebSocketHookBridge:
    """
    Bridge that forwards pipeline events from HookManager to WebSocket clients.

    Supports both FastAPI WebSocket and picows implementations.
    FastAPI WebSocket is preferred when integrated with FastAPI routes.
    """

    def __init__(
        self,
        hook_manager: HookManager,
        max_queue_size: int = 1000,
        use_fastapi: bool = True,
    ) -> None:
        self.hook_manager = hook_manager
        self.max_queue_size = max_queue_size
        self.use_fastapi = use_fastapi and FASTAPI_WS_AVAILABLE

        # Initialize the appropriate WebSocket manager
        self.websocket_manager: Any | None = None

        if self.use_fastapi:
            self.websocket_manager = get_fastapi_ws_manager()
            logger.info("Using FastAPI WebSocket manager")
        elif PICOWS_AVAILABLE:
            self.websocket_manager = get_websocket_server()
            logger.info("Using picows WebSocket server")
        else:
            logger.warning("No WebSocket implementation available")

        self._registered_events: set[str] = set()
        self._event_queue: deque[tuple[str, dict[str, Any]]] = deque(
            maxlen=max_queue_size,
        )
        self._is_connected = False
        self._reconnect_task: asyncio.Task[None] | None = None
        self._reconnect_delay = 1.0  # Start with 1 second delay
        self._max_reconnect_delay = 30.0  # Max 30 seconds between retries
        self._heartbeat_interval = 30.0  # Send heartbeat every 30 seconds
        self._heartbeat_task: asyncio.Task[None] | None = None

    def register_pipeline_events(self) -> None:
        """Register callbacks for all pipeline event types."""
        event_types = [
            "PipelineStartEvent",
            "StepStartEvent",
            "StepCompleteEvent",
            "PipelineCompleteEvent",
            "StepErrorEvent",
            "PipelineErrorEvent",
            "StepRetryEvent",
            "StepSkipEvent",
            "PipelineSkipEvent",
            "DAGUpdatedEvent",
            "CriticalPathChangedEvent",
        ]

        for event_type in event_types:
            if event_type not in self._registered_events:
                self.hook_manager.register(event_type, self._handle_event)
                self._registered_events.add(event_type)

        logger.info("WebSocket bridge registered for pipeline events")

        # Start connection monitoring and heartbeat
        self._start_connection_monitoring()

    def _start_connection_monitoring(self) -> None:
        """Start background tasks for connection monitoring."""
        if self._heartbeat_task is None or self._heartbeat_task.done():
            self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

    async def _heartbeat_loop(self) -> None:
        """Send periodic heartbeats to check connection health."""
        while True:
            try:
                await asyncio.sleep(self._heartbeat_interval)
                # Try to send a heartbeat event
                await self._send_heartbeat()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"Heartbeat failed: {e}")
                self._is_connected = False
                self._start_reconnection()

    async def _send_heartbeat(self) -> None:
        """Send a heartbeat to test connection."""
        try:
            if self.use_fastapi and FASTAPI_WS_AVAILABLE:
                # FastAPI WebSocket implementation
                heartbeat_data = {
                    "type": "heartbeat",
                    "timestamp": PipelineEvent(
                        pipeline_id="system"
                    ).timestamp.isoformat(),
                }
                # Broadcast to a dummy pipeline to test connection
                if self.websocket_manager is None:
                    raise RuntimeError("WebSocket manager not initialized")
                await self.websocket_manager.broadcast_event("system", heartbeat_data)
                self._is_connected = True
            elif PICOWS_AVAILABLE:
                # picows implementation
                heartbeat_data = {
                    "type": "heartbeat",
                    "timestamp": PipelineEvent(
                        pipeline_id="system"
                    ).timestamp.isoformat(),
                }
                if self.websocket_manager is None:
                    raise RuntimeError("WebSocket manager not initialized")
                await self.websocket_manager.broadcast_event("system", heartbeat_data)
                self._is_connected = True
            else:
                self._is_connected = False
                raise RuntimeError("No WebSocket implementation available")
        except Exception:
            self._is_connected = False
            raise

    def unregister_pipeline_events(self) -> None:
        """Unregister callbacks for pipeline events."""
        for event_type in self._registered_events:
            self.hook_manager.unregister(event_type, self._handle_event)
        self._registered_events.clear()

        # Cancel background tasks
        if self._heartbeat_task and not self._heartbeat_task.done():
            self._heartbeat_task.cancel()
        if self._reconnect_task and not self._reconnect_task.done():
            self._reconnect_task.cancel()

        logger.info("WebSocket bridge unregistered from pipeline events")

    def _start_reconnection(self) -> None:
        """Start reconnection process with exponential backoff."""
        if self._reconnect_task is None or self._reconnect_task.done():
            self._reconnect_task = asyncio.create_task(self._reconnection_loop())

    async def _reconnection_loop(self) -> None:
        """Attempt to reconnect with exponential backoff."""
        while not self._is_connected:
            try:
                logger.info(
                    "Attempting to reconnect WebSocket bridge in %ss...",
                    self._reconnect_delay,
                )
                await asyncio.sleep(self._reconnect_delay)

                # Try to send a test message
                await self._send_heartbeat()

                if self._is_connected:
                    logger.info("WebSocket bridge reconnected successfully")
                    # Process queued events
                    await self._process_queued_events()
                    # Reset reconnect delay
                    self._reconnect_delay = 1.0
                    break

            except Exception as e:
                logger.warning(f"Reconnection attempt failed: {e}")
                # Exponential backoff
                self._reconnect_delay = min(
                    self._reconnect_delay * 2,
                    self._max_reconnect_delay,
                )

    async def _process_queued_events(self) -> None:
        """Process events that were queued during disconnection."""
        if not self._event_queue:
            return

        logger.info(f"Processing {len(self._event_queue)} queued events")
        events_to_retry = []

        while self._event_queue:
            pipeline_id, event_data = self._event_queue.popleft()
            try:
                if self.websocket_manager is None:
                    raise RuntimeError("WebSocket manager not available")
                await self.websocket_manager.broadcast_event(pipeline_id, event_data)
            except Exception as e:
                logger.warning(
                    "Failed to send queued event %s: %s",
                    event_data.get("type", "unknown"),
                    e,
                )
                events_to_retry.append((pipeline_id, event_data))

        # Re-queue failed events
        for event in events_to_retry:
            self._event_queue.append(event)

        if events_to_retry:
            logger.warning(
                f"{len(events_to_retry)} events could not be sent and were re-queued",
            )

    async def _handle_event(self, event: PipelineEvent) -> None:
        """Handle a pipeline event and broadcast it via WebSocket."""
        event_data = self._event_to_dict(event)
        pipeline_id = event.pipeline_id

        try:
            if self._is_connected and self.websocket_manager is not None:
                await self.websocket_manager.broadcast_event(pipeline_id, event_data)
                logger.debug(
                    "Broadcasted %s for pipeline %s",
                    event.__class__.__name__,
                    pipeline_id,
                )
            else:
                # Queue event for later delivery
                self._event_queue.append((pipeline_id, event_data))
                logger.debug(
                    "Queued %s for pipeline %s (disconnected)",
                    event.__class__.__name__,
                    pipeline_id,
                )

                # Start reconnection if not already running
                self._start_reconnection()

        except Exception as exc:
            logger.warning(
                f"Failed to broadcast event {event.__class__.__name__}: {exc}",
            )

            # Queue event if broadcast failed
            if self._is_connected:
                self._event_queue.append((pipeline_id, event_data))
                self._is_connected = False
                self._start_reconnection()

    def _event_to_dict(self, event: PipelineEvent) -> dict[str, Any]:
        """Convert a pipeline event to a dictionary for JSON serialization."""
        event_dict: dict[str, Any] = {
            "type": event.__class__.__name__,
            "pipeline_id": event.pipeline_id,
            "timestamp": event.timestamp.isoformat(),
        }

        # Add event-specific fields using getattr to avoid static type errors
        step_index = getattr(event, "step_index", _MISSING)
        if step_index is not _MISSING:
            event_dict["step_index"] = step_index

        task_name = getattr(event, "task_name", _MISSING)
        if task_name is not _MISSING:
            event_dict["task_name"] = task_name

        task_id = getattr(event, "task_id", _MISSING)
        if task_id is not _MISSING:
            event_dict["task_id"] = task_id

        result = getattr(event, "result", _MISSING)
        if result is not _MISSING:
            event_dict["result"] = result

        error = getattr(event, "error", _MISSING)
        if error is not _MISSING:
            event_dict["error"] = error

        return event_dict


# Bridge registry for dependency injection
_bridge_instances: dict[int, WebSocketHookBridge] = {}


def get_websocket_bridge(
    hook_manager: HookManager,
    use_fastapi: bool = True,
) -> WebSocketHookBridge:
    """
    Get or create a WebSocket bridge instance for the given hook manager.

    Args:
        hook_manager: The HookManager to bridge events from
        use_fastapi: Whether to use FastAPI WebSocket implementation

    Returns:
        WebSocketHookBridge instance

    """
    manager_id = id(hook_manager)

    if manager_id not in _bridge_instances:
        _bridge_instances[manager_id] = WebSocketHookBridge(
            hook_manager, use_fastapi=use_fastapi
        )

    return _bridge_instances[manager_id]


def clear_bridge_cache() -> None:
    """Clear all cached bridge instances (useful for testing)."""
    for bridge in _bridge_instances.values():
        bridge.unregister_pipeline_events()
    _bridge_instances.clear()


async def start_websocket_server(host: str = "127.0.0.1", port: int = 8765) -> None:
    """Start the WebSocket server for pipeline event broadcasting."""
    server = get_websocket_server()
    asyncio_server = await server.start_server(host, port)
    try:
        await asyncio_server.serve_forever()
    except Exception as exc:  # pragma: no cover
        logger.exception(f"WebSocket server stopped unexpectedly: {exc}")


def setup_websocket_bridge(
    hook_manager: HookManager,
    use_fastapi: bool = True,
) -> WebSocketHookBridge:
    """
    Set up the WebSocket bridge for a HookManager.

    Args:
        hook_manager: The HookManager to bridge events from
        use_fastapi: Whether to use FastAPI WebSocket (default: True)

    Returns:
        Configured WebSocketHookBridge instance

    """
    bridge = get_websocket_bridge(hook_manager, use_fastapi)
    bridge.register_pipeline_events()
    return bridge
