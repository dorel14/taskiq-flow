"""Tests for pipeline scheduler."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from taskiq import InMemoryBroker

from taskiq_flow.scheduling.scheduler import PipelineScheduler


@pytest.mark.asyncio
async def test_scheduler_creation() -> None:
    """Test scheduler creation and configuration."""
    broker = InMemoryBroker()

    # Mock APScheduler to avoid import issues in tests
    with patch(
        "taskiq_flow.scheduling.scheduler.AsyncIOScheduler",
    ) as mock_scheduler:
        mock_instance = MagicMock()
        mock_scheduler.return_value = mock_instance

        scheduler = PipelineScheduler(broker)

        # Verify scheduler was created
        assert scheduler.broker == broker
        assert scheduler.scheduler == mock_instance

        # Verify job store was configured
        mock_instance.add_jobstore.assert_called_once()


def test_scheduler_schedule_methods() -> None:
    """Test that schedule methods exist and have correct signatures."""
    broker = InMemoryBroker()

    with patch(
        "taskiq_flow.scheduling.scheduler.AsyncIOScheduler",
    ) as mock_scheduler:
        mock_instance = MagicMock()
        mock_scheduler.return_value = mock_instance

        scheduler = PipelineScheduler(broker)

        # Test that key methods exist
        assert hasattr(scheduler, "schedule")
        assert hasattr(scheduler, "schedule_at")
        assert hasattr(scheduler, "schedule_interval")
        assert hasattr(scheduler, "start")
        assert hasattr(scheduler, "shutdown")
        assert hasattr(scheduler, "list_jobs")


@pytest.mark.asyncio
async def test_scheduler_async_methods() -> None:
    """Test async methods work correctly."""
    broker = InMemoryBroker()

    with patch(
        "taskiq_flow.scheduling.scheduler.AsyncIOScheduler",
    ) as mock_scheduler:
        mock_instance = AsyncMock()  # Use AsyncMock for async methods
        mock_scheduler.return_value = mock_instance

        scheduler = PipelineScheduler(broker)

        # Test start method
        await scheduler.start()
        mock_instance.start.assert_called_once()

        # Test shutdown method
        await scheduler.shutdown()
        mock_instance.shutdown.assert_called_once_with(wait=True)
