"""Tests for LabelBasedScheduler."""

import pytest
from taskiq import AsyncBroker
from taskiq.brokers.inmemory_broker import InMemoryBroker

from taskiq_flow.decorators import pipeline_task
from taskiq_flow.pipeline import DataflowPipeline
from taskiq_flow.scheduling.scheduler import LabelBasedScheduler


@pytest.fixture
def broker() -> AsyncBroker:
    """Create a test broker."""
    return InMemoryBroker()


@pytest.fixture
def scheduler(broker: AsyncBroker) -> LabelBasedScheduler:
    """Create a label-based scheduler."""
    return LabelBasedScheduler(broker)


@pytest.mark.asyncio
async def test_label_based_scheduler_creation(
    scheduler: LabelBasedScheduler,
) -> None:
    """Test that the scheduler is created correctly."""
    assert scheduler is not None
    assert len(scheduler.list_schedules()) == 0


@pytest.mark.asyncio
async def test_schedule_with_cron(
    scheduler: LabelBasedScheduler,
    broker: AsyncBroker,
) -> None:
    """Test scheduling with cron."""

    @pipeline_task(output="result")
    @broker.task
    async def test_task(x: int) -> int:
        return x + 1

    pipeline = DataflowPipeline(broker)
    pipeline.map(test_task, [1, 2, 3], "result")

    schedule_id = await scheduler.schedule_with_cron(
        pipeline=pipeline,
        label="test-cron",
        cron="* * * * *",  # Every minute
    )

    assert schedule_id == "test-cron"
    assert len(scheduler.list_schedules()) == 1

    schedule = scheduler.get_schedule("test-cron")
    assert schedule is not None
    assert schedule["label"] == "test-cron"
    assert schedule["cron"] == "* * * * *"
    assert schedule["enabled"] is True


@pytest.mark.asyncio
async def test_schedule_with_interval(
    scheduler: LabelBasedScheduler,
    broker: AsyncBroker,
) -> None:
    """Test scheduling with interval."""

    @pipeline_task(output="result")
    @broker.task
    async def test_task(x: int) -> int:
        return x + 1

    pipeline = DataflowPipeline(broker)
    pipeline.map(test_task, [1, 2, 3], "result")

    schedule_id = await scheduler.schedule_with_interval(
        pipeline=pipeline,
        label="test-interval",
        interval_seconds=60,
    )

    assert schedule_id == "test-interval"
    assert len(scheduler.list_schedules()) == 1

    schedule = scheduler.get_schedule("test-interval")
    assert schedule is not None
    assert schedule["label"] == "test-interval"
    assert schedule["interval_seconds"] == 60
    assert schedule["enabled"] is True


@pytest.mark.asyncio
async def test_enable_disable_schedule(
    scheduler: LabelBasedScheduler,
    broker: AsyncBroker,
) -> None:
    """Test enabling and disabling schedules."""

    @pipeline_task(output="result")
    @broker.task
    async def test_task(x: int) -> int:
        return x + 1

    pipeline = DataflowPipeline(broker)
    pipeline.map(test_task, [1, 2, 3], "result")

    await scheduler.schedule_with_cron(
        pipeline=pipeline,
        label="test-schedule",
        cron="* * * * *",
    )

    # Disable the schedule
    assert scheduler.disable_schedule("test-schedule") is True
    schedule = scheduler.get_schedule("test-schedule")
    assert schedule is not None
    assert schedule["enabled"] is False

    # Enable the schedule
    assert scheduler.enable_schedule("test-schedule") is True
    schedule = scheduler.get_schedule("test-schedule")
    assert schedule is not None
    assert schedule["enabled"] is True


@pytest.mark.asyncio
async def test_remove_schedule(
    scheduler: LabelBasedScheduler,
    broker: AsyncBroker,
) -> None:
    """Test removing a schedule."""

    @pipeline_task(output="result")
    @broker.task
    async def test_task(x: int) -> int:
        return x + 1

    pipeline = DataflowPipeline(broker)
    pipeline.map(test_task, [1, 2, 3], "result")

    await scheduler.schedule_with_cron(
        pipeline=pipeline,
        label="test-schedule",
        cron="* * * * *",
    )

    assert len(scheduler.list_schedules()) == 1

    # Remove the schedule
    assert scheduler.remove_schedule("test-schedule") is True
    assert len(scheduler.list_schedules()) == 0
    assert scheduler.get_schedule("test-schedule") is None


@pytest.mark.asyncio
async def test_pipeline_schedule_with_labels(
    broker: AsyncBroker,
) -> None:
    """Test scheduling a pipeline with labels."""

    @pipeline_task(output="result")
    @broker.task
    async def test_task(x: int) -> int:
        return x + 1

    pipeline = DataflowPipeline(broker)
    pipeline.map(test_task, [1, 2, 3], "result")

    scheduler = LabelBasedScheduler(broker)

    schedule_id = await pipeline.schedule_with_cron(
        scheduler=scheduler,
        label="pipeline-cron",
        cron="0 9 * * *",
    )

    assert schedule_id == "pipeline-cron"
    assert len(scheduler.list_schedules()) == 1

    schedule = scheduler.get_schedule("pipeline-cron")
    assert schedule is not None
    assert schedule["label"] == "pipeline-cron"
    assert schedule["cron"] == "0 9 * * *"


@pytest.mark.asyncio
async def test_invalid_schedule_raises_error(
    scheduler: LabelBasedScheduler,
    broker: AsyncBroker,
) -> None:
    """Test that invalid schedule raises an error."""

    @pipeline_task(output="result")
    @broker.task
    async def test_task(x: int) -> int:
        return x + 1

    pipeline = DataflowPipeline(broker)
    pipeline.map(test_task, [1, 2, 3], "result")

    # Neither cron nor interval specified
    with pytest.raises(
        ValueError, match="Either cron or interval_seconds must be specified"
    ):
        await scheduler.schedule_with_label(
            pipeline=pipeline,
            label="invalid-schedule",
        )

    # Both cron and interval specified
    with pytest.raises(
        ValueError, match="Cannot specify both cron and interval_seconds"
    ):
        await scheduler.schedule_with_label(
            pipeline=pipeline,
            label="invalid-schedule",
            cron="* * * * *",
            interval_seconds=60,
        )
