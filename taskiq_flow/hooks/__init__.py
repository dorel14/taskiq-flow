"""Hooks and events module."""

from .bridge_picows import WebSocketHookBridge, setup_websocket_bridge
from .events import (
    PipelineCompleteEvent,
    PipelineErrorEvent,
    PipelineEvent,
    PipelineStartEvent,
    StepCompleteEvent,
    StepErrorEvent,
    StepStartEvent,
)
from .manager import HookManager

__all__ = [
    "HookManager",
    "PipelineCompleteEvent",
    "PipelineErrorEvent",
    "PipelineEvent",
    "PipelineStartEvent",
    "StepCompleteEvent",
    "StepErrorEvent",
    "StepStartEvent",
    "WebSocketHookBridge",
    "setup_websocket_bridge",
]
