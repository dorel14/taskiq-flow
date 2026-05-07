"""Metrics collection via HookManager events.

This module provides a subscriber that collects metrics from pipeline
events (start, step completion, errors, retries, WebSocket messages)
and updates the Prometheus metrics.

Auteur: SoniqueBay Team
Version: 0.4.5
"""

import time
from typing import Any

from taskiq_flow.hooks.events import (
    MetricRecordEvent,
    PipelineCompleteEvent,
    PipelineStartEvent,
    StepCompleteEvent,
    StepErrorEvent,
    StepRetryEvent,
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
        if hook_manager:
            self._register_handlers()

    def _register_handlers(self) -> None:
        """Subscribe to relevant pipeline events."""
        self._hook_manager.register("PipelineStartEvent", self.on_pipeline_start)
        self._hook_manager.register("PipelineCompleteEvent", self.on_pipeline_complete)
        self._hook_manager.register("StepStartEvent", self.on_step_start)
        self._hook_manager.register("StepCompleteEvent", self.on_step_complete)
        self._hook_manager.register("StepErrorEvent", self.on_step_error)
        self._hook_manager.register("StepRetryEvent", self.on_step_retry)
        # WebSocket messages not an event yet, but we can increment via separate method

    # Note: These event handlers receive PipelineEvent objects

    async def on_pipeline_start(self, event: PipelineStartEvent) -> None:
        """Handle pipeline start event."""
        self.collector.pipeline_start(event.pipeline_id)

    async def on_pipeline_complete(self, event: PipelineCompleteEvent) -> None:
        """Handle pipeline complete event."""
        success = True  # Assuming success if no error event; but event may not have status; infer from absence of error?
        # Actually PipelineCompleteEvent has no success flag; we'll assume success is True for complete event.
        self.collector.pipeline_complete(event.pipeline_id, success=True)

    async def on_step_start(self, event: Any) -> None:
        """Handle step start event.

        Records start timestamp in a per-step context.
        Pipeline ID available via event.pipeline_id.
        """
        # We need to store start time keyed by task_id to compute duration later.
        # Since we don't have context store, we can record in collector or self.
        # Let's use a simple dict: _step_start_times
        self._step_start_times[event.task_id] = time.time()

    async def on_step_complete(self, event: StepCompleteEvent) -> None:
        """Handle step complete event."""
        task_name = event.task_name
        status = "success"
        duration = getattr(event, "duration", 0.0)
        # If we have stored start time, we can use that; else use provided duration.
        if duration == 0.0:
            start = self._step_start_times.pop(event.task_id, None)
            if start is not None:
                duration = time.time() - start
        self.collector.task_executed(task_name, status, duration)

    async def on_step_error(self, event: StepErrorEvent) -> None:
        """Handle step error event."""
        task_name = event.task_name  # need to get task_name? StepErrorEvent has task_name? Check events.py.
        # StepErrorEvent fields: pipeline_id, step_index, task_name, task_id, error, attempt, max_attempts
        task_name = event.task_name
        status = "failure"
        # Compute duration from start if available
        start = self._step_start_times.pop(event.task_id, None)
        duration = time.time() - start if start else 0.0
        self.collector.task_executed(task_name, status, duration)

    async def on_step_retry(self, event: StepRetryEvent) -> None:
        """Handle step retry event."""
        task_name = event.task_name
        exception_type = type(event.error).__name__ if event.error else "Unknown"
        self.collector.task_retried(task_name, exception_type)

    def record_websocket_message(self, pipeline_id: str, direction: str, msg_type: str) -> None:
        """Record a WebSocket message metric (called by WebSocket manager)."""
        self.collector.websocket_message(pipeline_id, direction, msg_type)

    # Store step start times
    _step_start_times: dict[str, float] = {}

