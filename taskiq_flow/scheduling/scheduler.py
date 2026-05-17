"""
Ordonnanceur de pipelines avec APScheduler.

Ce module fournit deux classes principales:
- LabelBasedScheduler: ordonnanceur léger utilisant les labels TaskIQ
- PipelineScheduler: ordonnanceur complet basé sur APScheduler

Permet de programmer l'exécution périodique ou différée des pipelines.

Auteur: SoniqueBay Team
Version: 1.0.2
"""

import logging
from datetime import datetime
from typing import Any, cast

from taskiq import AsyncBroker

try:
    from apscheduler.schedulers.asyncio import AsyncIOScheduler
    from apscheduler.triggers.cron import CronTrigger
    from apscheduler.triggers.date import DateTrigger
    from apscheduler.triggers.interval import IntervalTrigger
except ImportError:
    AsyncIOScheduler = None
    CronTrigger = None
    DateTrigger = None
    IntervalTrigger = None

from taskiq_flow.pipeliner import Pipeline

logger = logging.getLogger(__name__)


async def _run_pipeline(
    pipeline: Pipeline[Any, Any],
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
) -> Any:
    """Execute the pipeline."""
    try:
        logger.info(
            f"Executing scheduled pipeline: {pipeline.pipeline_id or 'unknown'}",
        )
        result = await pipeline.kiq(*args, **kwargs)
        logger.info(
            "Scheduled pipeline completed successfully: %s",
            pipeline.pipeline_id or "unknown",
        )
        return result
    except Exception as e:
        logger.error(
            "Scheduled pipeline failed: %s - %s",
            pipeline.pipeline_id or "unknown",
            e,
        )
        raise


async def _execute_scheduled_pipeline(
    pipeline_id: str,
    args: list[Any] | tuple[Any, ...],
    kwargs: dict[str, Any],
) -> Any:
    """
    Execute a scheduled pipeline by ID.

    This is a module-level function that can be pickled for distributed brokers.
    It retrieves the pipeline from the registry and executes it.

    Args:
        pipeline_id: The ID of the pipeline to execute
        args: Positional arguments to pass to the pipeline
        kwargs: Keyword arguments to pass to the pipeline

    Returns:
        The pipeline result

    """
    # Get the pipeline from the global registry
    pipeline = Pipeline.get_pipeline(pipeline_id)
    if pipeline is None:
        raise ValueError(f"Pipeline not found: {pipeline_id}")

    return await pipeline.kiq(*args, **kwargs)


class LabelBasedScheduler:
    """
    Lightweight scheduler using TaskIQ's LabelScheduleSource.

    This is the default scheduler that uses TaskIQ's built-in label-based
    scheduling mechanism. It doesn't require external dependencies and
    is more lightweight than APScheduler.

    Example:
        scheduler = LabelBasedScheduler(broker)
        await scheduler.schedule_with_label(
            pipeline,
            label="daily-report",
            cron="0 9 * * *",  # Every day at 9 AM
            args=(arg1, arg2),
        )

    """

    def __init__(self, broker: AsyncBroker) -> None:
        """
        Initialize the label-based scheduler.

        Args:
            broker: TaskIQ broker instance

        """
        self.broker = broker
        self._schedules: dict[str, dict[str, Any]] = {}

        # Note: The broker should have a LabelScheduleSource added to it
        # before using this scheduler. This can be done by the user
        # or by using TaskiqScheduler with LabelScheduleSource.

    async def schedule_with_label(
        self,
        pipeline: Pipeline[Any, Any],
        label: str,
        cron: str | None = None,
        interval_seconds: int | None = None,
        args: tuple[Any, ...] = (),
        kwargs: dict[str, Any] | None = None,
        enabled: bool = True,
    ) -> str:
        """
        Schedule a pipeline using a label-based schedule.

        This method creates a LabelSchedule that will trigger the pipeline
        based on the specified cron expression or interval.

        The schedule is added to the task's labels in the format expected
        by LabelScheduleSource:

        ```python
        task.labels = {
            "schedule": [
                {
                    "cron": "0 9 * * *",  # Cron expression
                    "args": [],           # Positional arguments
                    "kwargs": {},         # Keyword arguments
                    "labels": {},         # Additional labels
                    "schedule_id": "..."  # Unique schedule ID
                }
            ]
        }
        ```

        Args:
            pipeline: Pipeline to schedule
            label: Unique label for this schedule
            cron: Cron expression (e.g., "0 9 * * *" for daily at 9 AM)
            interval_seconds: Interval in seconds (alternative to cron)
            args: Positional arguments to pass to the pipeline
            kwargs: Keyword arguments to pass to the pipeline
            enabled: Whether the schedule is enabled

        Returns:
            The schedule ID

        Raises:
            ValueError: If neither cron nor interval is specified

        """
        if not cron and not interval_seconds:
            raise ValueError("Either cron or interval_seconds must be specified")

        if cron and interval_seconds:
            raise ValueError("Cannot specify both cron and interval_seconds")

        # Ensure pipeline has an ID and register it
        if not pipeline.pipeline_id:
            pipeline.pipeline_id = Pipeline.register_pipeline(pipeline)
        else:
            Pipeline.register_pipeline(pipeline)

        task_name = pipeline.pipeline_id

        # Register the task with the broker using a module-level function
        # This ensures the task is picklable for distributed brokers
        task = self.broker.task(name=task_name)(_execute_scheduled_pipeline)

        # Create schedule entry in the format expected by LabelScheduleSource
        schedule_entry: dict[str, Any] = {
            "args": list(args),
            "kwargs": kwargs or {},
            "labels": {},
            "schedule_id": label,
        }

        if cron:
            schedule_entry["cron"] = cron
        elif interval_seconds:
            schedule_entry["interval"] = interval_seconds

        # Add schedule to task labels
        if not hasattr(task, "labels"):
            task.labels = {}

        if "schedule" not in task.labels:
            task.labels["schedule"] = []

        task.labels["schedule"].append(schedule_entry)

        # Store schedule info
        self._schedules[label] = {
            "label": label,
            "cron": cron,
            "interval_seconds": interval_seconds,
            "pipeline_id": pipeline.pipeline_id,
            "enabled": enabled,
            "task_name": task_name,
        }

        logger.info(
            "Label-based schedule created",
            extra={
                "label": label,
                "cron": cron,
                "interval_seconds": interval_seconds,
                "pipeline_id": pipeline.pipeline_id,
                "enabled": enabled,
            },
        )

        return label

    async def schedule_with_cron(
        self,
        pipeline: Pipeline[Any, Any],
        label: str,
        cron: str,
        args: tuple[Any, ...] = (),
        kwargs: dict[str, Any] | None = None,
        enabled: bool = True,
    ) -> str:
        """
        Schedule a pipeline with a cron expression.

        Args:
            pipeline: Pipeline to schedule
            label: Unique label for this schedule
            cron: Cron expression (e.g., "0 9 * * *" for daily at 9 AM)
            args: Positional arguments to pass to the pipeline
            kwargs: Keyword arguments to pass to the pipeline
            enabled: Whether the schedule is enabled

        Returns:
            The schedule ID

        """
        return await self.schedule_with_label(
            pipeline=pipeline,
            label=label,
            cron=cron,
            args=args,
            kwargs=kwargs,
            enabled=enabled,
        )

    async def schedule_with_interval(
        self,
        pipeline: Pipeline[Any, Any],
        label: str,
        interval_seconds: int,
        args: tuple[Any, ...] = (),
        kwargs: dict[str, Any] | None = None,
        enabled: bool = True,
    ) -> str:
        """
        Schedule a pipeline with a fixed interval.

        Args:
            pipeline: Pipeline to schedule
            label: Unique label for this schedule
            interval_seconds: Interval in seconds
            args: Positional arguments to pass to the pipeline
            kwargs: Keyword arguments to pass to the pipeline
            enabled: Whether the schedule is enabled

        Returns:
            The schedule ID

        """
        return await self.schedule_with_label(
            pipeline=pipeline,
            label=label,
            interval_seconds=interval_seconds,
            args=args,
            kwargs=kwargs,
            enabled=enabled,
        )

    def get_schedule(self, label: str) -> dict[str, Any] | None:
        """
        Get a schedule by label.

        Args:
            label: Schedule label

        Returns:
            The schedule if found, None otherwise

        """
        return self._schedules.get(label)

    def list_schedules(self) -> list[dict[str, Any]]:
        """
        List all schedules.

        Returns:
            List of all schedules

        """
        return list(self._schedules.values())

    def enable_schedule(self, label: str) -> bool:
        """
        Enable a schedule.

        Args:
            label: Schedule label

        Returns:
            True if the schedule was found and enabled, False otherwise

        """
        schedule = self._schedules.get(label)
        if schedule:
            schedule["enabled"] = True
            logger.info("Schedule enabled", extra={"label": label})
            return True
        return False

    def disable_schedule(self, label: str) -> bool:
        """
        Disable a schedule.

        Args:
            label: Schedule label

        Returns:
            True if the schedule was found and disabled, False otherwise

        """
        schedule = self._schedules.get(label)
        if schedule:
            schedule["enabled"] = False
            logger.info("Schedule disabled", extra={"label": label})
            return True
        return False

    def remove_schedule(self, label: str) -> bool:
        """
        Remove a schedule.

        Args:
            label: Schedule label

        Returns:
            True if the schedule was found and removed, False otherwise

        """
        if label in self._schedules:
            del self._schedules[label]
            logger.info("Schedule removed", extra={"label": label})
            return True
        return False


class PipelineScheduler:
    """Scheduler for pipeline execution using APScheduler."""

    def __init__(
        self,
        broker: AsyncBroker,
        scheduler: AsyncIOScheduler | None = None,
        job_store_url: str | None = None,
    ) -> None:
        if AsyncIOScheduler is None:
            raise ImportError("APScheduler is required for PipelineScheduler")

        # Assert to help type checker
        assert AsyncIOScheduler is not None  # noqa: S101

        self.broker = broker
        self.scheduler = scheduler or AsyncIOScheduler()
        self._configure_job_store(job_store_url or "sqlite:///./scheduler_jobs.db")

    def _configure_job_store(self, url: str) -> None:
        """Configure job store based on URL."""
        # If APScheduler is not available, we can't configure job stores
        if AsyncIOScheduler is None:
            logger.warning(
                "APScheduler not available, using in-memory job store. "
                "Scheduled jobs will not persist across restarts.",
            )
            return

        try:
            if url.startswith("sqlite"):
                from apscheduler.jobstores.sqlalchemy import (  # noqa: PLC0415
                    SQLAlchemyJobStore,
                )

                self.scheduler.add_jobstore(
                    SQLAlchemyJobStore(url=url),
                    alias="default",
                )
            elif url.startswith("redis"):
                # Placeholder for Redis support
                raise NotImplementedError("Redis job store not implemented yet")
            elif url.startswith(("postgresql", "mysql")):
                from apscheduler.jobstores.sqlalchemy import (  # noqa: PLC0415
                    SQLAlchemyJobStore,
                )

                self.scheduler.add_jobstore(
                    SQLAlchemyJobStore(url=url),
                    alias="default",
                )
            else:
                raise ValueError(f"Unsupported job store URL: {url}")
        except ImportError:
            # Fallback to memory job store if SQLAlchemy is not available
            # This can happen in tests where APScheduler is mocked
            try:
                from apscheduler.jobstores.memory import MemoryJobStore  # noqa: PLC0415

                self.scheduler.add_jobstore(MemoryJobStore(), alias="default")
            except ImportError:
                # Even memory job store is not available
                # This happens when APScheduler is mocked in tests
                # Just call add_jobstore with a simple dict to satisfy the interface
                self.scheduler.add_jobstore({}, alias="default")
            logger.warning(
                "SQLAlchemy not available, using in-memory job store. "
                "Scheduled jobs will not persist across restarts.",
            )

    async def schedule(
        self,
        pipeline: Pipeline[Any, Any],
        cron: str,
        args: tuple[Any, ...] = (),
        kwargs: dict[str, Any] | None = None,
        timezone: str = "UTC",
        misfire_grace_time: int = 300,
        jitter: int | None = None,
    ) -> str:
        """Schedule pipeline with cron trigger."""
        trigger = CronTrigger(
            cron,
            timezone=timezone,
            jitter=jitter,
        )
        job = self.scheduler.add_job(
            _run_pipeline,
            trigger=trigger,
            args=[pipeline, args, kwargs or {}],
            misfire_grace_time=misfire_grace_time,
            jobstore="default",
        )
        return cast(str, job.id)

    async def schedule_at(
        self,
        pipeline: Pipeline[Any, Any],
        run_at: datetime,
        args: tuple[Any, ...] = (),
        kwargs: dict[str, Any] | None = None,
        timezone: str = "UTC",
        misfire_grace_time: int = 300,
    ) -> str:
        """Schedule pipeline to run once at specific datetime."""
        trigger = DateTrigger(run_at, timezone=timezone)
        job = self.scheduler.add_job(
            _run_pipeline,
            trigger=trigger,
            args=[pipeline, args, kwargs or {}],
            misfire_grace_time=misfire_grace_time,
            jobstore="default",
        )
        return cast(str, job.id)

    async def schedule_interval(
        self,
        pipeline: Pipeline[Any, Any],
        hours: int = 0,
        minutes: int = 0,
        seconds: int = 0,
        args: tuple[Any, ...] = (),
        kwargs: dict[str, Any] | None = None,
        timezone: str = "UTC",
        misfire_grace_time: int = 300,
        jitter: int | None = None,
    ) -> str:
        """Schedule pipeline with interval trigger."""
        trigger = IntervalTrigger(
            hours=hours,
            minutes=minutes,
            seconds=seconds,
            timezone=timezone,
            jitter=jitter,
        )
        job = self.scheduler.add_job(
            _run_pipeline,
            trigger=trigger,
            args=[pipeline, args, kwargs or {}],
            misfire_grace_time=misfire_grace_time,
            jobstore="default",
        )
        return cast(str, job.id)

    async def _run_pipeline(
        self,
        pipeline: Pipeline[Any, Any],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> Any:
        """Execute the pipeline."""
        return await _run_pipeline(pipeline, args, kwargs)

    async def start(self) -> None:
        """Start the scheduler."""
        if self.scheduler is None:
            raise RuntimeError("Scheduler is not available")
        scheduler = cast("AsyncIOScheduler", self.scheduler)
        logger.info("Starting pipeline scheduler")
        scheduler.start()

    async def shutdown(self, wait: bool = True) -> None:
        """Shutdown the scheduler."""
        if self.scheduler is None:
            raise RuntimeError("Scheduler is not available")
        scheduler = cast("AsyncIOScheduler", self.scheduler)
        logger.info("Shutting down pipeline scheduler")
        scheduler.shutdown(wait=wait)

    def list_jobs(self) -> Any:
        """List all scheduled jobs."""
        assert self.scheduler is not None  # noqa: S101
        return self.scheduler.get_jobs()

    async def remove_job(self, job_id: str) -> bool:
        """Remove a job by ID."""
        assert self.scheduler is not None  # noqa: S101
        try:
            self.scheduler.remove_job(job_id)
            return True
        except Exception:
            return False

    async def pause_job(self, job_id: str) -> bool:
        """Pause a job."""
        assert self.scheduler is not None  # noqa: S101
        try:
            self.scheduler.pause_job(job_id)
            return True
        except Exception:
            return False

    async def resume_job(self, job_id: str) -> bool:
        """Resume a job."""
        assert self.scheduler is not None  # noqa: S101
        try:
            self.scheduler.resume_job(job_id)
            return True
        except Exception:
            return False
