"""WebSocket consumer for pipeline events."""

from typing import Any

try:
    from chanx import ChannelLayer
except ImportError:
    ChannelLayer = type(None)  # type: ignore


class PipelineWebSocketConsumer:
    """WebSocket consumer for pipeline events."""

    def __init__(self, channel_layer: ChannelLayer) -> None:
        if ChannelLayer is None:
            raise ImportError("chanx required for WebSocket consumer")
        self.channel_layer = channel_layer

    async def connect(self, scope: Any, receive: Any, send: Any) -> None:
        """Handle WebSocket connection."""
        # Extract pipeline_id from URL or scope
        pipeline_id = self._get_pipeline_id(scope)
        if pipeline_id:
            await self.channel_layer.group_add(f"pipeline_{pipeline_id}", self.channel_name)

        await send({"type": "websocket.accept"})

    async def disconnect(self, code: int) -> None:
        """Handle WebSocket disconnection."""
        # Clean up group membership if needed

    async def receive(self, text_data: Any = None, bytes_data: Any = None) -> None:
        """Handle incoming messages."""
        # For now, just echo or handle commands

    def _get_pipeline_id(self, scope: Any) -> str | None:
        """Extract pipeline ID from scope."""
        # Example: from URL path /ws/pipeline/{pipeline_id}/
        path = scope.get("path", "")
        if "/ws/pipeline/" in path:
            parts = path.split("/")
            try:
                idx = parts.index("pipeline")
                return parts[idx + 1]
            except (ValueError, IndexError):
                pass
        return None
