"""
Tests for metrics collection.

This module tests the metrics collector and middleware.

Author: SoniqueBay Team
Version: 0.4.5
"""

import asyncio

import pytest

from taskiq_flow.hooks.events import (
    PipelineCompleteEvent,
    PipelineStartEvent,
    StepCompleteEvent,
)
from taskiq_flow.metrics.collector import MetricsCollector
from taskiq_flow.metrics.middleware import MetricsMiddleware


class TestMetricsCollector:
    """Tests for MetricsCollector."""

    def test_singleton(self) -> None:
        """Test that MetricsCollector is a singleton."""
        collector1 = MetricsCollector()
        collector2 = MetricsCollector()

        assert collector1 is collector2

    def test_pipeline_start(self) -> None:
        """Test pipeline start tracking."""
        collector = MetricsCollector()
        collector.pipeline_start("test-pipeline")

        assert "test-pipeline" in collector._pipeline_starts

    def test_pipeline_complete(self) -> None:
        """Test pipeline completion tracking."""
        collector = MetricsCollector()
        collector.pipeline_start("test-pipeline")
        collector.pipeline_complete("test-pipeline", True)

        assert "test-pipeline" not in collector._pipeline_starts

    def test_pipeline_complete_failure(self) -> None:
        """Test pipeline failure tracking."""
        collector = MetricsCollector()
        collector.pipeline_start("test-pipeline")
        collector.pipeline_complete("test-pipeline", False)

        assert "test-pipeline" not in collector._pipeline_starts

    def test_task_executed(self) -> None:
        """Test task execution tracking."""
        collector = MetricsCollector()
        collector.task_executed("test-task", "success", 1.5)

        # Should not raise
        assert True

    def test_task_retried(self) -> None:
        """Test task retry tracking."""
        collector = MetricsCollector()
        collector.task_retried("test-task", "ValueError")

        # Should not raise
        assert True

    def test_websocket_message(self) -> None:
        """Test WebSocket message tracking."""
        collector = MetricsCollector()
        collector.websocket_message("test-pipeline", "in", "status")

        # Should not raise
        assert True

    def test_export_metrics(self) -> None:
        """Test metrics export."""
        collector = MetricsCollector()
        metrics = collector.export_metrics()

        assert isinstance(metrics, bytes)
        assert len(metrics) > 0


class TestMetricsMiddleware:
    """Tests for MetricsMiddleware."""

    def test_middleware_creation(self) -> None:
        """Test middleware creation."""
        middleware = MetricsMiddleware()

        assert middleware is not None
        assert middleware.collector is not None

    def test_on_pipeline_start(self) -> None:
        """Test on_pipeline_start hook."""
        middleware = MetricsMiddleware()

        event = PipelineStartEvent(pipeline_id="test-pipeline")

        asyncio.run(middleware.on_pipeline_start(event))

    def test_on_pipeline_complete(self) -> None:
        """Test on_pipeline_complete hook."""
        middleware = MetricsMiddleware()

        event = PipelineCompleteEvent(pipeline_id="test-pipeline", result=None)

        asyncio.run(middleware.on_pipeline_complete(event))

    def test_on_step_complete(self) -> None:
        """Test on_step_complete hook."""
        middleware = MetricsMiddleware()

        event = StepCompleteEvent(
            pipeline_id="test-pipeline",
            step_index=0,
            task_name="test_task",
            task_id="test-task-1",
            result={"status": "ok"},
            duration=0.5,
        )

        asyncio.run(middleware.on_step_complete(event))


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
