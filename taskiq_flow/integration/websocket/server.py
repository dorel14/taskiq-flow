"""
WebSocket server module for FastAPI-only integration.

This module previously provided picows-based standalone WebSocket server.
As of taskiq-flow 1.1, WebSocket integration is FastAPI-only.

Use the FastAPI WebSocket integration via:
    from taskiq_flow.integration.websocket.fastapi_ws import get_fastapi_ws_manager
    from taskiq_flow.hooks.bridge import get_websocket_bridge, setup_websocket_bridge
    from taskiq_flow.hooks.manager import HookManager

    hook_manager = HookManager()
    setup_websocket_bridge(hook_manager, use_fastapi=True)

Author: SoniqueBay Team
Version: 1.1.0
"""

__all__ = ["get_websocket_server"]


def get_websocket_server(*args: object, **kwargs: object) -> None:
    """
    Deprecated: picows was removed in taskiq-flow 1.1.

    Use FastAPI WebSocket integration instead:
        from taskiq_flow.hooks.bridge import (
            get_websocket_bridge, setup_websocket_bridge,
        )
        from taskiq_flow.hooks.manager import HookManager

        hook_manager = HookManager()
        setup_websocket_bridge(hook_manager, use_fastapi=True)

    Raises:
        RuntimeError: Always - picows is no longer supported

    """
    raise RuntimeError(
        "picows is no longer supported. Use FastAPI WebSocket integration instead. "
        "See taskiq_flow.integration.websocket.fastapi_ws and "
        "taskiq_flow.hooks.bridge for the new FastAPI-only approach."
    )
