"""Tests for FastAPI WebSocket integration."""

import pytest

from taskiq_flow.integration.websocket.fastapi_ws import (
    FastAPIWebSocketManager,
    get_fastapi_ws_manager,
)


def test_fastapi_ws_manager_creation() -> None:
    """Test that FastAPIWebSocketManager can be created."""
    manager = FastAPIWebSocketManager()
    assert manager is not None


def test_fastapi_ws_manager_singleton() -> None:
    """Test that get_fastapi_ws_manager returns singleton."""
    manager1 = get_fastapi_ws_manager()
    manager2 = get_fastapi_ws_manager()
    assert manager1 is manager2


def test_fastapi_ws_manager_client_count() -> None:
    """Test client count methods."""
    manager = FastAPIWebSocketManager()
    assert manager.get_client_count("test-pipeline") == 0
    assert manager.get_pipeline_ids() == []


@pytest.mark.asyncio
async def test_fastapi_ws_manager_broadcast() -> None:
    """Test broadcasting events."""
    manager = FastAPIWebSocketManager()

    # Broadcast to non-existent pipeline should not raise
    await manager.broadcast_event("non-existent", {"test": "data"})

    # Clean up
    await manager.close_all()
