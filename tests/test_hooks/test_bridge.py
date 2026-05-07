# mypy: disable-error-code=no-untyped-def
"""Tests for WebSocket bridge with FastAPI integration."""

from taskiq_flow.hooks import WebSocketHookBridge
from taskiq_flow.hooks.events import PipelineEvent
from taskiq_flow.hooks.manager import HookManager


def test_bridge_import():
    """Test that the bridge module can be imported."""
    assert WebSocketHookBridge is not None


def test_bridge_initialization():
    """Test that the bridge initializes correctly."""
    manager = HookManager()
    bridge = WebSocketHookBridge(manager, use_fastapi=True)
    assert bridge is not None
    assert bridge.hook_manager is manager


def test_bridge_use_fastapi():
    """Test that bridge uses FastAPI WebSocket when available."""
    manager = HookManager()
    bridge = WebSocketHookBridge(manager, use_fastapi=True)
    # Should use FastAPI WebSocket manager if available
    assert hasattr(bridge, "websocket_manager")


def test_event_to_dict():
    """Test event serialization."""
    manager = HookManager()
    bridge = WebSocketHookBridge(manager, use_fastapi=False)

    event = PipelineEvent(pipeline_id="test-pipeline")
    event_dict = bridge._event_to_dict(event)

    assert event_dict["type"] == "PipelineEvent"
    assert event_dict["pipeline_id"] == "test-pipeline"
    assert "timestamp" in event_dict
