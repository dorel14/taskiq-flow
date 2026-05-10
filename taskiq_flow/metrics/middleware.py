"""Metrics collection via HookManager events.

This module provides a subscriber that collects metrics from pipeline
events (start, step completion, errors, retries, WebSocket messages)
and updates the Prometheus metrics.

Auteur: SoniqueBay Team
Version: 1.0.2
"""

import time

from taskiq_flow.hooks.events import (
    PipelineEvent,
    StepCompleteEvent,
    StepErrorEvent,
    StepRetryEvent,
    StepStartEvent,
)
from taskiq_flow.hooks.manager import HookManager
from taskiq_flow.metrics.collector import MetricsCollector


class MetricsMiddleware:
    """Collects metrics by subscribing to pipeline events.

    Must be registered with a HookManager to receive events.
    Handles:
    - Pipeline start/complete
    - Step execution duration
    - Step retries
    - Step errors
    - WebSocket messages (via explicit call, not event)
    """

    def __init__(self, hook_manager: HookManager | None = None) -> None:
        """Initialize metrics collector and optionally register to hook manager.

        Args:
            hook_manager: Optional HookManager to subscribe to events
        """
        self.collector = MetricsCollector()
        self._hook_manager = hook_manager
        self._step_start_times: dict[str, float] = {}
        if hook_manager:
            self._register_handlers()

    def _register_handlers(self) -> None:
        """Subscribe to relevant pipeline events."""
        if self._hook_manager is None:
            return
        self._hook_manager.register("PipelineStartEvent", self.on_pipeline_start)
        self._hook_manager.register("PipelineCompleteEvent", self.on_pipeline_complete)
        self._hook_manager.register("StepStartEvent", self.on_step_start)
        self._hook_manager.register("StepCompleteEvent", self.on_step_complete)
        self._hook_manager.register("StepErrorEvent", self.on_step_error)
        self._hook_manager.register("StepRetryEvent", self.on_step_retry)
        # WebSocket messages not an event yet, but we can increment via separate method

    # Note: These event handlers receive PipelineEvent objects

    async def on_pipeline_start(self, event: PipelineEvent) -> None:
        """Handle pipeline start event."""
        self.collector.pipeline_start(event.pipeline_id)

    async def on_pipeline_complete(self, event: PipelineEvent) -> None:
        """Handle pipeline complete event."""
        self.collector.pipeline_complete(event.pipeline_id, success=True)

    async def on_step_start(self, event: PipelineEvent) -> None:
        """Handle step start event.

        Records start timestamp in a per-step context.
        Pipeline ID available via event.pipeline_id.
        """
        # We need to store start time keyed by task_id to compute duration later.
        # Since we don't have context store, we can record in collector or self.
        # Let's use a simple dict: _step_start_times
        step_event = event if isinstance(event, StepStartEvent) else None
        if step_event:
            self._step_start_times[step_event.task_id] = time.time()

    async def on_step_complete(self, event: PipelineEvent) -> None:
        """Handle step complete event."""
        step_event = event if isinstance(event, StepCompleteEvent) else None
        if step_event:
            task_name = step_event.task_name
            status = "success"
            duration = step_event.duration
            # If we have stored start time, we can use that; else use provided duration.
            if duration == 0.0:
                start = self._step_start_times.pop(step_event.task_id, None)
                if start is not None:
                    duration = time.time() - start
            self.collector.task_executed(task_name, status, duration)

    async def on_step_error(self, event: PipelineEvent) -> None:
        """Handle step error event."""
        step_event = event if isinstance(event, StepErrorEvent) else None
        if step_event:
            task_name = step_event.task_name
            status = "failure"
            # Compute duration from start if available
            start = self._step_start_times.pop(step_event.task_id, None)
            duration = time.time() - start if start else 0.0
            self.collector.task_executed(task_name, status, duration)

    async def on_step_retry(self, event: PipelineEvent) -> None:
        """Handle step retry event."""
        step_event = event if isinstance(event, StepRetryEvent) else None
        if step_event:
            task_name = step_event.task_name
            exception_type = (
                type(step_event.error).__name__ if step_event.error else "Unknown"
            )
            self.collector.task_retried(task_name, exception_type)

    def record_websocket_message(
        self, pipeline_id: str, direction: str, msg_type: str
    ) -> None:
        """Record a WebSocket message metric (called by WebSocket manager)."""
        self.collector.websocket_message(pipeline_id, direction, msg_type)
