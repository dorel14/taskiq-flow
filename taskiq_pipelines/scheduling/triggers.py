"""Trigger helpers for scheduling."""

from datetime import datetime
from typing import Any

try:
    from apscheduler.triggers.cron import CronTrigger
    from apscheduler.triggers.date import DateTrigger
    from apscheduler.triggers.interval import IntervalTrigger
except ImportError:
    CronTrigger = None
    DateTrigger = None
    IntervalTrigger = None


def create_cron_trigger(
    expression: str,
    timezone: str = "UTC",
    jitter: int | None = None,
) -> Any:
    """Create a cron trigger."""
    if CronTrigger is None:
        raise ImportError("APScheduler required")
    return CronTrigger(expression, timezone=timezone, jitter=jitter)


def create_date_trigger(
    run_date: datetime,
    timezone: str = "UTC",
) -> Any:
    """Create a date trigger."""
    if DateTrigger is None:
        raise ImportError("APScheduler required")
    return DateTrigger(run_date, timezone=timezone)


def create_interval_trigger(
    weeks: int = 0,
    days: int = 0,
    hours: int = 0,
    minutes: int = 0,
    seconds: int = 0,
    start_date: datetime | None = None,
    end_date: datetime | None = None,
    timezone: str = "UTC",
    jitter: int | None = None,
) -> Any:
    """Create an interval trigger."""
    if IntervalTrigger is None:
        raise ImportError("APScheduler required")
    return IntervalTrigger(
        weeks=weeks,
        days=days,
        hours=hours,
        minutes=minutes,
        seconds=seconds,
        start_date=start_date,
        end_date=end_date,
        timezone=timezone,
        jitter=jitter,
    )


def every_minute(jitter: int | None = None) -> Any:
    """Trigger every minute."""
    return create_cron_trigger("* * * * *", jitter=jitter)


def every_hour(jitter: int | None = None) -> Any:
    """Trigger every hour."""
    return create_cron_trigger("0 * * * *", jitter=jitter)


def every_day(hour: int = 0, minute: int = 0, jitter: int | None = None) -> Any:
    """Trigger every day at specified time."""
    return create_cron_trigger(f"{minute} {hour} * * *", jitter=jitter)


def in_seconds(seconds: int, jitter: int | None = None) -> Any:
    """Trigger in specified seconds."""
    return create_interval_trigger(seconds=seconds, jitter=jitter)


def in_minutes(minutes: int, jitter: int | None = None) -> Any:
    """Trigger in specified minutes."""
    return create_interval_trigger(minutes=minutes, jitter=jitter)


def in_hours(hours: int, jitter: int | None = None) -> Any:
    """Trigger in specified hours."""
    return create_interval_trigger(hours=hours, jitter=jitter)
