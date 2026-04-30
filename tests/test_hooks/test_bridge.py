# mypy: disable-error-code=no-untyped-def
"""Tests for hooks bridge (placeholder)."""

import pytest

# The WebSocket bridge was removed in commit 61511c8 when chanx-based
# integration was deemed non-functional. WebSocket support will be
# re-added in a future version with a stable library.
# See: taskiq-flow/issues/X


@pytest.mark.skip(reason="WebSocket bridge module was removed, will be re-added with stable library")
def test_bridge_placeholder_import():
    """Test that the bridge module can be imported."""
    # Bridge module is now bridge_picows, not bridge
    from taskiq_flow.hooks import WebSocketHookBridge

    assert WebSocketHookBridge is not None


@pytest.mark.skip(reason="WebSocket bridge module was removed, will be re-added with stable library")
def test_bridge_placeholder_comment():
    """Test that the bridge file contains expected placeholder content."""
    import inspect

    from taskiq_flow.hooks import WebSocketHookBridge

    # Check if the class exists
    assert WebSocketHookBridge is not None
