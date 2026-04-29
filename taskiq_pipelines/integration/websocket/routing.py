"""WebSocket routing for pipeline events."""

try:
    from chanx import URLRouter, ChannelLayer
except ImportError:
    URLRouter = None
    ChannelLayer = None

from .consumer import PipelineWebSocketConsumer


def create_websocket_router(channel_layer: ChannelLayer):
    """Create WebSocket URL router for pipelines."""
    if URLRouter is None:
        raise ImportError("chanx required for WebSocket routing")

    return URLRouter([
        {
            "path": "ws/pipeline/{pipeline_id}/",
            "consumer": PipelineWebSocketConsumer,
            "kwargs": {"channel_layer": channel_layer},
        }
    ])