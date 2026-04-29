"""WebSocket bridge for pipeline events."""

from typing import TYPE_CHECKING, Any

from .events import PipelineEvent
from .manager import HookManager

if TYPE_CHECKING:
    try:
        from chanx.layer import ChannelLayer
    except ImportError:
        ChannelLayer = None


class WebSocketHookBridge:
    """Bridge between hooks and WebSocket broadcasting."""

    def __init__(self, hook_manager: HookManager, channel_layer: Any) -> None:
        self.hook_manager = hook_manager
        self.channel_layer = channel_layer
        self._register_broadcasts()

    def _register_broadcasts(self) -> None:
        """Register broadcast callbacks for all pipeline events."""
        event_types = [
            "PipelineStartEvent",
            "StepStartEvent",
            "StepCompleteEvent",
            "PipelineCompleteEvent",
            "StepErrorEvent",
            "PipelineErrorEvent",
        ]

        for event_type in event_types:
            self.hook_manager.register(event_type, self._broadcast_event)

    async def _broadcast_event(self, event: PipelineEvent) -> None:
        """Broadcast event to WebSocket channel."""
        try:
            await self.channel_layer.group_send(
                f"pipeline_{event.pipeline_id}",
                {
                    "type": "pipeline.event",
                    "event_type": event.__class__.__name__,
                    "data": event.model_dump(),
                },
            )
        except Exception as exc:
            # Log but don't fail pipeline
            import logging
            logger = logging.getLogger(__name__)
            logger.exception(f"Failed to broadcast event {event.__class__.__name__}: {exc}")
