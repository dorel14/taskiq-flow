"""Hook manager for pipeline events."""

import asyncio
import logging
import weakref
from collections.abc import Awaitable, Callable
from typing import Any

from .events import PipelineEvent

logger = logging.getLogger(__name__)


class HookManager:
    """Manager for registering and dispatching pipeline event hooks."""

    def __init__(self) -> None:
        # Use WeakSet to prevent memory leaks - callbacks will be automatically
        # removed when the objects they belong to are garbage collected
        # For bound methods and local functions, we need to keep a strong reference
        # to prevent them from being garbage collected while still in use
        self._callbacks: dict[str, weakref.WeakSet[Callable[[PipelineEvent], Any]]] = {}
        self._strong_refs: dict[str, list[Callable[[PipelineEvent], Any]]] = {}
        self._lock = asyncio.Lock()

    def register(
        self,
        event_type: str,
        callback: (
            Callable[[PipelineEvent], Any] | Callable[[PipelineEvent], Awaitable[Any]]
        ),
    ) -> None:
        """Register a callback for an event type."""
        if event_type not in self._callbacks:
            self._callbacks[event_type] = weakref.WeakSet()
            self._strong_refs[event_type] = []

        # Try to add to WeakSet - if it fails (callback not weakref-able),
        # we'll fall back to a regular approach with periodic cleanup
        try:
            self._callbacks[event_type].add(callback)
            # Also keep a strong reference for local functions and bound methods
            # to prevent them from being garbage collected while registered
            self._strong_refs[event_type].append(callback)
            logger.debug(f"Registered callback for event {event_type}")
        except TypeError:
            # Callback is not weakref-able (e.g., lambda, built-in function)
            # For now, we'll skip registration to prevent memory leaks
            logger.warning(
                f"Cannot register non-weakref-able callback for {event_type}",
            )

    def unregister(
        self,
        event_type: str,
        callback: Callable[[PipelineEvent], Any],
    ) -> None:
        """Unregister a callback for an event type."""
        if event_type in self._callbacks:
            try:
                self._callbacks[event_type].discard(
                    callback,
                )  # discard is safer than remove for WeakSet
                logger.debug(f"Unregistered callback for event {event_type}")
            except (ValueError, TypeError):
                pass  # Callback not found or not weakref-able

    async def dispatch(self, event: PipelineEvent) -> None:
        """Dispatch an event to all registered callbacks."""
        event_type = event.__class__.__name__

        # Get callbacks outside the lock to avoid holding it during execution
        async with self._lock:
            # Convert WeakSet to list to avoid modification during iteration
            callbacks = list(self._callbacks.get(event_type, weakref.WeakSet()))

        if not callbacks:
            return

        # Dispatch to callbacks concurrently but safely
        tasks = []
        for cb in callbacks:
            # Skip if callback was garbage collected
            if cb is not None:
                task = asyncio.create_task(self._safe_call(cb, event))
                tasks.append(task)

        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            # Log any exceptions that occurred during dispatch
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    logger.error(f"Callback {i} failed for {event_type}: {result}")

    async def _safe_call(
        self,
        callback: (
            Callable[[PipelineEvent], Any] | Callable[[PipelineEvent], Awaitable[Any]]
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
            # Re-raise to be caught by the dispatch method
            raise exc

    def get_callback_count(self, event_type: str | None = None) -> int:
        """Get the number of registered callbacks."""
        if event_type:
            return len(self._callbacks.get(event_type, weakref.WeakSet()))
        return sum(len(callbacks) for callbacks in self._callbacks.values())
