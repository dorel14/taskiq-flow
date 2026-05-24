"""
HTTP Streaming (SSE) transport for pipeline events.

This module provides a Server-Sent Events endpoint for streaming
pipeline events to HTTP clients without WebSocket overhead.

Author: SoniqueBay Team
Version: 1.0.2
"""

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any

from prometheus_client import Counter, Gauge

from taskiq_flow.hooks.events import PipelineEvent

logger = logging.getLogger(__name__)

# Optional starlette import for auth verification
try:
    from starlette.requests import Request
except ImportError:
    Request = None  # type: ignore

# Metrics
SSE_CONNECTIONS_ACTIVE = Gauge(
    "taskiq_flow_sse_connections_active",
    "Number of active SSE connections",
    ["pipeline_id"],
)
SSE_EVENTS_SENT_TOTAL = Counter(
    "taskiq_flow_sse_events_sent_total",
    "Total number of SSE events sent",
    ["pipeline_id", "event_type"],
)


class EventQueue:
    """Thread-safe async event queue for SSE connections."""

    def __init__(self, max_size: int = 1000) -> None:
        """
        Initialize the event queue.

        Args:
            max_size: Maximum number of queued events before dropping oldest.

        """
        self._queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=max_size)
        self._closed = False

    async def put(self, event: dict[str, Any]) -> None:
        """
        Put an event into the queue.

        Args:
            event: Event data dictionary.

        """
        if self._closed:
            return
        try:
            self._queue.put_nowait(event)
        except asyncio.QueueFull:
            # Drop oldest event to make room
            try:
                self._queue.get_nowait()
                self._queue.put_nowait(event)
            except (asyncio.QueueEmpty, asyncio.QueueFull):
                pass

    async def get(self) -> dict[str, Any] | None:
        """
        Get next event from the queue, waiting if necessary.

        Returns:
            Event data dictionary, or None if queue is closed.

        """
        try:
            return await asyncio.wait_for(self._queue.get(), timeout=30.0)
        except asyncio.TimeoutError:
            # Send keepalive comment to prevent connection timeout
            return {"comment": "keepalive"}
        except asyncio.CancelledError:
            return None

    def close(self) -> None:
        """Close the queue."""
        self._closed = True

    @property
    def closed(self) -> bool:
        """Whether the queue is closed."""
        return self._closed


class HTTPStreamTransport:
    """
    HTTP Streaming (SSE) transport for pipeline events.

    Provides a Server-Sent Events endpoint that clients can connect to
    for real-time pipeline event updates without WebSocket overhead.
    Compatible with EventSource API in browsers.

    Example:
        ```python
        # Server-side setup
        transport = HTTPStreamTransport()
        api.get("/events")(transport.asgi_endpoint)

        # Broadcast events
        await transport.broadcast(event)
        ```

    """

    def __init__(
        self,
        channel_prefix: str = "pipeline",
        heartbeat_interval: float = 15.0,
        max_queue_size: int = 1000,
        auth_provider: Any = None,
        authorization: Any = None,
    ) -> None:
        """
        Initialize the HTTP streaming transport.

        Args:
            channel_prefix: Prefix for channel names.
            heartbeat_interval: Seconds between heartbeat events.
            max_queue_size: Maximum events per connection queue.
            auth_provider: Optional auth provider for token verification.
            authorization: Optional authorization manager for pipeline ACLs.

        """
        self.channel_prefix = channel_prefix
        self.heartbeat_interval = heartbeat_interval
        self.max_queue_size = max_queue_size
        self.auth_provider = auth_provider
        self.authorization = authorization
        self._channels: dict[str, set[EventQueue]] = {}
        self._lock = asyncio.Lock()
        self._heartbeat_task: asyncio.Task[Any] | None = None
        self._active = True

    def _channel_name(self, pipeline_id: str) -> str:
        """
        Generate channel name for a pipeline.

        Args:
            pipeline_id: The pipeline identifier.

        Returns:
            Channel name string.

        """
        return f"{self.channel_prefix}:{pipeline_id}"

    async def subscribe(
        self,
        pipeline_id: str,
        filters: dict[str, Any] | None = None,
    ) -> EventQueue:
        """
        Subscribe to pipeline events.

        Args:
            pipeline_id: The pipeline to subscribe to.
            filters: Optional event filters.

        Returns:
            EventQueue for receiving events.

        """
        channel = self._channel_name(pipeline_id)
        queue = EventQueue(max_size=self.max_queue_size)

        async with self._lock:
            if channel not in self._channels:
                self._channels[channel] = set()
            self._channels[channel].add(queue)

        SSE_CONNECTIONS_ACTIVE.labels(pipeline_id=pipeline_id).inc()
        logger.info(
            "SSE client subscribed to channel %s",
            channel,
            extra={"pipeline_id": pipeline_id},
        )
        return queue

    async def unsubscribe(self, pipeline_id: str, queue: EventQueue) -> None:
        """
        Unsubscribe from pipeline events.

        Args:
            pipeline_id: The pipeline identifier.
            queue: The EventQueue to remove.

        """
        channel = self._channel_name(pipeline_id)

        async with self._lock:
            if channel in self._channels:
                self._channels[channel].discard(queue)
                if not self._channels[channel]:
                    del self._channels[channel]

        queue.close()
        SSE_CONNECTIONS_ACTIVE.labels(pipeline_id=pipeline_id).dec()
        logger.info(
            "SSE client unsubscribed from channel %s",
            channel,
            extra={"pipeline_id": pipeline_id},
        )

    async def broadcast(
        self,
        event: PipelineEvent,
        filters: dict[str, Any] | None = None,
    ) -> None:
        """
        Broadcast an event to all subscribed clients.

        Args:
            event: The pipeline event to broadcast.
            filters: Optional filters to apply before broadcasting.

        """
        event_data = (
            event.model_dump() if hasattr(event, "model_dump") else event.dict()
        )

        # Apply filters if provided
        if filters and not self._match_filters(event_data, filters):
            return

        # Determine target channel
        pipeline_id = event_data.get("pipeline_id", "default")
        channel = self._channel_name(pipeline_id)

        event_dict = self._event_to_sse_event(event_data)

        async with self._lock:
            queues = list(self._channels.get(channel, set()))

        for queue in queues:
            try:
                await queue.put(event_dict)
                SSE_EVENTS_SENT_TOTAL.labels(
                    pipeline_id=pipeline_id,
                    event_type=event_data.get("event_type", "unknown"),
                ).inc()
            except Exception as e:
                logger.warning(
                    "Failed to send SSE event to client",
                    extra={"pipeline_id": pipeline_id, "error": str(e)},
                )

    async def broadcast_global(
        self,
        event: PipelineEvent,
        filters: dict[str, Any] | None = None,
    ) -> None:
        """
        Broadcast an event to ALL channels.

        Used for system-level events that should reach all clients.

        Args:
            event: The pipeline event to broadcast.
            filters: Optional filters to apply before broadcasting.

        """
        event_data = (
            event.model_dump() if hasattr(event, "model_dump") else event.dict()
        )
        event_dict = self._event_to_sse_event(event_data)

        async with self._lock:
            all_queues = set()
            for queues in self._channels.values():
                all_queues.update(queues)

        for queue in all_queues:
            try:
                await queue.put(event_dict)
            except Exception as e:
                logger.warning(
                    "Failed to send SSE global event to client",
                    extra={"error": str(e)},
                )

    def _match_filters(self, event: dict[str, Any], filters: dict[str, Any]) -> bool:
        """
        Check if event matches filter criteria.

        Args:
            event: Event data dictionary.
            filters: Filter criteria.

        Returns:
            True if event matches all filters.

        """
        return all(event.get(key) == value for key, value in filters.items())

    def _event_to_sse_event(self, event_data: dict[str, Any]) -> dict[str, Any]:
        """
        Convert event data to SSE event format.

        Args:
            event_data: Raw event data.

        Returns:
            SSE-formatted event dictionary.

        """
        return {
            "event": event_data.get("event_type", "pipeline_event"),
            "data": json.dumps(event_data, default=str),
            "id": event_data.get("pipeline_id", ""),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    def get_sse_endpoint(
        self,
        pipeline_id: str | None = None,
    ) -> Any:
        """
        Get a FastAPI-compatible SSE endpoint function.

        Args:
            pipeline_id: Optional pipeline ID to subscribe to.
                If None, client must provide via query param.

        Returns:
            StreamingResponse factory function.

        """
        try:
            from starlette.responses import StreamingResponse  # noqa: PLC0415
        except ImportError as e:
            raise RuntimeError(
                "starlette is required for SSE endpoint. "
                "Install with: pip install starlette"
            ) from e

        async def sse_endpoint(request: Any) -> "StreamingResponse | None":
            """
            SSE endpoint handler.

            Args:
                request: Starlette/FastAPI request object.

            Returns:
                StreamingResponse with SSE content.

            """
            # Extract pipeline_id
            pid = pipeline_id or request.query_params.get("pipeline_id", "default")

            # Verify authentication if configured
            if self.auth_provider:
                # Check for token in headers or query params
                token = request.headers.get(
                    "authorization"
                ) or request.query_params.get("token")
                if not token:
                    return StreamingResponse(
                        [],
                        media_type="text/event-stream",
                        status_code=401,
                        headers={"Content-Type": "text/plain"},
                    )

                # Create a mock request to verify token
                try:
                    mock_request = Request(
                        scope={
                            "type": "http",
                            "method": "GET",
                            "path": "/events",
                            "headers": [
                                (
                                    b"authorization",
                                    token.encode() if isinstance(token, str) else token,
                                ),
                            ],
                        }
                    )
                    auth_result = self.auth_provider.verify(mock_request)
                    if asyncio.iscoroutine(auth_result):
                        user = await auth_result
                    else:
                        user = auth_result

                    if not user:
                        return StreamingResponse(
                            [],
                            media_type="text/event-stream",
                            status_code=401,
                            headers={"Content-Type": "text/plain"},
                        )
                except Exception as e:
                    logger.warning(f"Auth verification failed: {e}")
                    return StreamingResponse(
                        [],
                        media_type="text/event-stream",
                        status_code=401,
                        headers={"Content-Type": "text/plain"},
                    )

            queue = await self.subscribe(pid)

            async def event_generator() -> Any:
                """Generate SSE events for this connection."""
                try:
                    while not queue.closed:
                        event = await queue.get()
                        if event is None:
                            break

                        if "comment" in event:
                            yield (f": {event['comment']}\n\n").encode()
                        else:
                            yield (
                                f"event: {event.get('event', 'message')}\n"
                                f"data: {event.get('data', '')}\n"
                                f"id: {event.get('id', '')}\n\n"
                            ).encode()
                        await asyncio.sleep(0)
                finally:
                    await self.unsubscribe(pid, queue)

            return StreamingResponse(
                event_generator(),
                media_type="text/event-stream",
                headers={
                    "Cache-Control": "no-cache",
                    "Connection": "keep-alive",
                    "X-Accel-Buffering": "no",
                },
            )

        return sse_endpoint

    async def close(self) -> None:
        """Close all connections and clean up resources."""
        self._active = False

        async with self._lock:
            for channel_queues in self._channels.values():
                for queue in channel_queues:
                    queue.close()
            self._channels.clear()

        logger.info("HTTP Stream transport closed")


# Global instance for convenience
_global_transport: HTTPStreamTransport | None = None


def get_http_stream_transport() -> HTTPStreamTransport:
    """
    Get or create the global HTTP streaming transport instance.

    Returns:
        HTTPStreamTransport singleton instance.

    """
    global _global_transport  # noqa: PLW0603
    if _global_transport is None:
        _global_transport = HTTPStreamTransport()
    return _global_transport


__all__ = [
    "EventQueue",
    "HTTPStreamTransport",
    "get_http_stream_transport",
]
