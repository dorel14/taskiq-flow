"""Trigger helpers for scheduling."""

from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from apscheduler.triggers.cron import CronTrigger
    from apscheduler.triggers.date import DateTrigger
    from apscheduler.triggers.interval import IntervalTrigger

try:
    from apscheduler.triggers.cron import CronTrigger
    from apscheduler.triggers.date import DateTrigger
    from apscheduler.triggers.interval import IntervalTrigger

    APSCHEDULER_AVAILABLE = True
except ImportError:
    CronTrigger = None
    DateTrigger = None
    IntervalTrigger = None
    APSCHEDULER_AVAILABLE = False

# Type alias for trigger types
if TYPE_CHECKING:
    TriggerType = CronTrigger | DateTrigger | IntervalTrigger
else:
    if APSCHEDULER_AVAILABLE:
        TriggerType = CronTrigger | DateTrigger | IntervalTrigger  # type: ignore
    else:
        TriggerType = type(None)  # type: ignore


def create_cron_trigger(
    expression: str,
    timezone: str = "UTC",
    jitter: int | None = None,
) -> CronTrigger:
    """Create a cron trigger."""
    if CronTrigger is None:
        raise ImportError("APScheduler required")
    # APScheduler 3.x+ expects separate fields, not a single cron string
    # Parse the cron expression
    parts = expression.split()
    if len(parts) != 5:
        raise ValueError(f"Invalid cron expression: {expression}")

    minute, hour, day, month, day_of_week = parts
    return CronTrigger(
        minute=minute,
        hour=hour,
        day=day,
        month=month,
        day_of_week=day_of_week,
        timezone=timezone,
        jitter=jitter,
    )


def create_date_trigger(
    run_date: datetime,
    timezone: str = "UTC",
) -> DateTrigger:
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
) -> IntervalTrigger:
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


def every_minute(jitter: int | None = None) -> CronTrigger:
    """Trigger every minute."""
    return create_cron_trigger("*/1 * * * *", jitter=jitter)


def every_hour(jitter: int | None = None) -> CronTrigger:
    """Trigger every hour."""
    return create_cron_trigger("0 * * * *", jitter=jitter)


def every_day(hour: int = 0, minute: int = 0, jitter: int | None = None) -> CronTrigger:
    """Trigger every day at specified time."""
    return create_cron_trigger(f"{minute} {hour} * * *", jitter=jitter)


def in_seconds(seconds: int, jitter: int | None = None) -> IntervalTrigger:
    """Trigger in specified seconds."""
    return create_interval_trigger(seconds=seconds, jitter=jitter)


def in_minutes(minutes: int, jitter: int | None = None) -> IntervalTrigger:
    """Trigger in specified minutes."""
    return create_interval_trigger(minutes=minutes, jitter=jitter)


def in_hours(hours: int, jitter: int | None = None) -> IntervalTrigger:
    """Trigger in specified hours."""
    return create_interval_trigger(hours=hours, jitter=jitter)
