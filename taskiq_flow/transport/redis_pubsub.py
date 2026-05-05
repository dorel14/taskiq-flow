"""Redis PubSub transport for pipeline events.

Author: SoniqueBay Team
Version: 0.4.0
"""

import logging
from typing import Any

from taskiq_flow.hooks.events import PipelineEvent

logger = logging.getLogger(__name__)


class RedisPubSubTransport:
    """
    Redis PubSub transport for pipeline events.

    Broadcasts events to multiple clients via Redis pub/sub channels.
    """

    def __init__(
        self,
        redis_client: Any | None = None,
        channel_prefix: str = "pipeline_events",
    ) -> None:
        self.redis = redis_client
        self.channel_prefix = channel_prefix
        self._subscribers: dict[str, Any] = {}

    async def connect(self) -> None:
        """Connect to Redis."""
        if self.redis is None:
            logger.warning("Redis client not configured for RedisPubSubTransport")
            return
        # Connection is handled by the redis client itself
        logger.info("Redis PubSub transport connected")

    async def disconnect(self) -> None:
        """Disconnect from Redis."""
        if self.redis is None:
            return
        # Unsubscribe all channels
        for channel in self._subscribers:
            await self.redis.unsubscribe(channel)
        self._subscribers.clear()

    async def broadcast(
        self,
        event: PipelineEvent,
        filters: dict[str, Any] | None = None,
    ) -> None:
        """Broadcast event via Redis pub/sub."""
        if self.redis is None:
            return

        event_data = (
            event.model_dump() if hasattr(event, "model_dump") else event.dict()
        )

        # Publish to the main channel
        channel = f"{self.channel_prefix}:broadcast"
        try:
            await self.redis.publish(channel, event_data)  # type: ignore[misc]
        except Exception as e:
            logger.error("Failed to publish event to Redis", extra={"error": str(e)})

        # Also publish to pipeline-specific channel if available
        if "pipeline_id" in event_data:
            pipeline_channel = f"{self.channel_prefix}:{event_data['pipeline_id']}"
            try:
                await self.redis.publish(pipeline_channel, event_data)  # type: ignore[misc]
            except Exception as e:
                logger.error(
                    "Failed to publish event to pipeline channel",
                    extra={"error": str(e)},
                )

    async def send_to_client(
        self,
        client_id: str,
        event: PipelineEvent,
    ) -> None:
        """Send event to a specific client via private channel."""
        if self.redis is None:
            return

        event_data = (
            event.model_dump() if hasattr(event, "model_dump") else event.dict()
        )
        channel = f"{self.channel_prefix}:private:{client_id}"

        try:
            await self.redis.publish(channel, event_data)  # type: ignore[misc]
        except Exception as e:
            logger.error("Failed to send event to client", extra={"error": str(e)})

    async def subscribe(
        self,
        client_id: str,
        channel: str | None = None,
        callback: Any = None,
    ) -> None:
        """Subscribe a client to events."""
        if self.redis is None:
            return

        if channel is None:
            channel = f"{self.channel_prefix}:broadcast"

        try:
            pubsub = self.redis.pubsub()
            await pubsub.subscribe(channel)
            self._subscribers[client_id] = {
                "pubsub": pubsub,
                "channel": channel,
                "callback": callback,
            }
        except Exception as e:
            logger.error("Failed to subscribe to channel", extra={"error": str(e)})
