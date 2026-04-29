"""Hook manager for pipeline events."""

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import Any

from .events import PipelineEvent

logger = logging.getLogger(__name__)


class HookManager:
    """Manager for registering and dispatching pipeline event hooks."""

    def __init__(self) -> None:
        self._callbacks: dict[str, list[Callable[[PipelineEvent], Any]]] = {}
        self._lock = asyncio.Lock()

    def register(
        self,
        event_type: str,
        callback: (
            Callable[[PipelineEvent], Any]
            | Callable[[PipelineEvent], Awaitable[Any]]
        ),
    ) -> None:
        """Register a callback for an event type."""
        if event_type not in self._callbacks:
            self._callbacks[event_type] = []
        self._callbacks[event_type].append(callback)

    def unregister(
        self,
        event_type: str,
        callback: Callable[[PipelineEvent], Any],
    ) -> None:
        """Unregister a callback for an event type."""
        if event_type in self._callbacks:
            try:
                self._callbacks[event_type].remove(callback)
            except ValueError:
                pass  # Callback not found

    async def dispatch(self, event: PipelineEvent) -> None:
        """Dispatch an event to all registered callbacks."""
        event_type = event.__class__.__name__
        async with self._lock:
            callbacks = self._callbacks.get(event_type, []).copy()

        # Dispatch to callbacks concurrently but safely
        tasks = []
        for cb in callbacks:
            task = asyncio.create_task(self._safe_call(cb, event))
            tasks.append(task)

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _safe_call(
        self,
        callback: (
            Callable[[PipelineEvent], Any]
            | Callable[[PipelineEvent], Awaitable[Any]]
        ),
        event: PipelineEvent,
    ) -> None:
        """Call a callback safely, handling exceptions."""
        try:
            if asyncio.iscoroutinefunction(callback):
                await callback(event)
            else:
                callback(event)
        except Exception as exc:
            logger.exception(f"Hook callback failed for {event.__class__.__name__}: {exc}")
