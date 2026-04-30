# mypy: disable-error-code=no-untyped-def
"""Tests for pipeline scheduler."""

import pytest
from taskiq import InMemoryBroker

from taskiq_flow.scheduling.scheduler import PipelineScheduler


@pytest.fixture
def broker():
    """Create a test broker."""
    return InMemoryBroker()


def test_pipeline_scheduler_creation(broker):
    """Test PipelineScheduler creation."""
    try:
        scheduler = PipelineScheduler(broker)
        assert scheduler.broker == broker
        assert scheduler.scheduler is not None
    except ImportError:
        pytest.skip("APScheduler not available")


def test_pipeline_scheduler_with_custom_scheduler(broker):
    """Test PipelineScheduler with custom scheduler."""
    try:
        from apscheduler.schedulers.asyncio import AsyncIOScheduler

        custom_scheduler = AsyncIOScheduler()
        scheduler = PipelineScheduler(broker, scheduler=custom_scheduler)
        assert scheduler.scheduler == custom_scheduler
    except ImportError:
        pytest.skip("APScheduler not available")


def test_pipeline_scheduler_job_store_sqlite(broker):
    """Test configuring SQLite job store."""
    try:
        scheduler = PipelineScheduler(broker, job_store_url="sqlite:///./test.db")
        assert scheduler.scheduler is not None
    except ImportError:
        pytest.skip("APScheduler not available")


def test_pipeline_scheduler_job_store_unsupported(broker):
    """Test configuring unsupported job store."""
    try:
        with pytest.raises(ValueError, match="Unsupported job store URL"):
            PipelineScheduler(broker, job_store_url="unsupported://url")
    except ImportError:
        pytest.skip("APScheduler not available")


@pytest.mark.asyncio
async def test_pipeline_scheduler_list_jobs(broker):
    """Test listing jobs."""
    try:
        scheduler = PipelineScheduler(broker)
        jobs = scheduler.list_jobs()
        assert isinstance(jobs, list)
    except ImportError:
        pytest.skip("APScheduler not available")


@pytest.mark.asyncio
async def test_pipeline_scheduler_remove_job(broker):
    """Test removing a job."""
    try:
        scheduler = PipelineScheduler(broker)
        # Try to remove non-existent job
        result = await scheduler.remove_job("nonexistent")
        assert result is False
    except ImportError:
        pytest.skip("APScheduler not available")


@pytest.mark.asyncio
async def test_pipeline_scheduler_pause_resume_job(broker):
    """Test pausing and resuming a job."""
    try:
        scheduler = PipelineScheduler(broker)
        # Try operations on non-existent job
        paused = await scheduler.pause_job("nonexistent")
        assert paused is False

        resumed = await scheduler.resume_job("nonexistent")
        assert resumed is False
    except ImportError:
        pytest.skip("APScheduler not available")


def test_pipeline_scheduler_import_error():
    """Test that ImportError is raised when APScheduler not available."""
    from unittest.mock import patch

    # The import happens at module level, so we need to patch the
    # AsyncIOScheduler in the scheduler module itself
    with patch("taskiq_flow.scheduling.scheduler.AsyncIOScheduler", None):
        with patch("taskiq_flow.scheduling.scheduler.CronTrigger", None):
            with patch("taskiq_flow.scheduling.scheduler.DateTrigger", None):
                with patch("taskiq_flow.scheduling.scheduler.IntervalTrigger", None):
                    with pytest.raises(ImportError):
                        PipelineScheduler(InMemoryBroker())
