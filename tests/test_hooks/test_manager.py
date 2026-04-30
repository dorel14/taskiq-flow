"""Tests for hook manager."""

from unittest.mock import AsyncMock

import pytest

from taskiq_flow.hooks.events import PipelineStartEvent
from taskiq_flow.hooks.manager import HookManager


@pytest.fixture
def hook_manager():
    """Create a hook manager instance."""
    return HookManager()


def test_hook_manager_creation():
    """Test HookManager creation."""
    manager = HookManager()
    assert manager._callbacks == {}
    assert manager._lock is not None


def test_register_callback(hook_manager):
    """Test registering a callback."""

    def callback(event):
        pass

    hook_manager.register("PipelineStartEvent", callback)
    assert "PipelineStartEvent" in hook_manager._callbacks
    assert callback in hook_manager._callbacks["PipelineStartEvent"]


def test_register_async_callback(hook_manager):
    """Test registering an async callback."""

    async def async_callback(event):
        pass

    hook_manager.register("PipelineStartEvent", async_callback)
    assert async_callback in hook_manager._callbacks["PipelineStartEvent"]


def test_unregister_callback(hook_manager):
    """Test unregistering a callback."""

    def callback(event):
        pass

    hook_manager.register("PipelineStartEvent", callback)
    assert callback in hook_manager._callbacks["PipelineStartEvent"]

    hook_manager.unregister("PipelineStartEvent", callback)
    assert callback not in hook_manager._callbacks["PipelineStartEvent"]


def test_unregister_nonexistent_callback(hook_manager):
    """Test unregistering a callback that doesn't exist."""

    def callback(event):
        pass

    def other_callback(event):
        pass

    hook_manager.register("PipelineStartEvent", callback)

    # Should not raise error
    hook_manager.unregister("PipelineStartEvent", other_callback)
    assert callback in hook_manager._callbacks["PipelineStartEvent"]


@pytest.mark.asyncio
async def test_dispatch_event(hook_manager):
    """Test dispatching an event."""
    callback_mock = AsyncMock()
    hook_manager.register("PipelineStartEvent", callback_mock)

    event = PipelineStartEvent(pipeline_id="test_pipe")
    await hook_manager.dispatch(event)

    callback_mock.assert_called_once_with(event)


@pytest.mark.asyncio
async def test_dispatch_multiple_callbacks(hook_manager):
    """Test dispatching to multiple callbacks."""
    callback1 = AsyncMock()
    callback2 = AsyncMock()

    hook_manager.register("PipelineStartEvent", callback1)
    hook_manager.register("PipelineStartEvent", callback2)

    event = PipelineStartEvent(pipeline_id="test_pipe")
    await hook_manager.dispatch(event)

    callback1.assert_called_once_with(event)
    callback2.assert_called_once_with(event)


@pytest.mark.asyncio
async def test_dispatch_no_callbacks(hook_manager):
    """Test dispatching when no callbacks registered."""
    event = PipelineStartEvent(pipeline_id="test_pipe")

    # Should not raise error
    await hook_manager.dispatch(event)


@pytest.mark.asyncio
async def test_dispatch_failing_callback(hook_manager):
    """Test dispatching when callback fails."""

    def failing_callback(event):
        raise ValueError("Callback failed")

    hook_manager.register("PipelineStartEvent", failing_callback)

    event = PipelineStartEvent(pipeline_id="test_pipe")

    # Should not raise error, should log exception
    await hook_manager.dispatch(event)


@pytest.mark.asyncio
async def test_dispatch_mixed_sync_async_callbacks(hook_manager):
    """Test dispatching to mix of sync and async callbacks."""
    sync_callback = AsyncMock()  # Mock to make it awaitable

    async def async_callback(event):
        pass

    hook_manager.register("PipelineStartEvent", sync_callback)
    hook_manager.register("PipelineStartEvent", async_callback)

    event = PipelineStartEvent(pipeline_id="test_pipe")
    await hook_manager.dispatch(event)

    sync_callback.assert_called_once_with(event)


@pytest.mark.asyncio
async def test_dispatch_wrong_event_type(hook_manager):
    """Test dispatching event with no matching callbacks."""
    callback_mock = AsyncMock()
    hook_manager.register("StepStartEvent", callback_mock)

    event = PipelineStartEvent(pipeline_id="test_pipe")
    await hook_manager.dispatch(event)

    # Should not call callback for different event type
    callback_mock.assert_not_called()
