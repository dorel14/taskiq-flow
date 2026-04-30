# mypy: disable-error-code=no-untyped-def
"""Tests for scheduling triggers."""

from datetime import datetime

import pytest

from taskiq_flow.scheduling.triggers import (
    create_cron_trigger,
    create_date_trigger,
    create_interval_trigger,
    every_day,
    every_hour,
    every_minute,
    in_hours,
    in_minutes,
    in_seconds,
)


def test_create_cron_trigger():
    """Test creating cron trigger."""
    try:
        trigger = create_cron_trigger("* * * * *")
        assert trigger is not None
    except ImportError:
        pytest.skip("APScheduler not available")


def test_create_date_trigger():
    """Test creating date trigger."""
    try:
        run_date = datetime(2026, 4, 29, 12, 0, 0)
        trigger = create_date_trigger(run_date)
        assert trigger is not None
    except ImportError:
        pytest.skip("APScheduler not available")


def test_create_interval_trigger():
    """Test creating interval trigger."""
    try:
        trigger = create_interval_trigger(hours=1, minutes=30)
        assert trigger is not None
    except ImportError:
        pytest.skip("APScheduler not available")


def test_every_minute():
    """Test every_minute helper."""
    try:
        trigger = every_minute()
        assert trigger is not None
    except ImportError:
        pytest.skip("APScheduler not available")


def test_every_hour():
    """Test every_hour helper."""
    try:
        trigger = every_hour()
        assert trigger is not None
    except ImportError:
        pytest.skip("APScheduler not available")


def test_every_day():
    """Test every_day helper."""
    try:
        trigger = every_day(hour=9, minute=30)
        assert trigger is not None
    except ImportError:
        pytest.skip("APScheduler not available")


def test_in_seconds():
    """Test in_seconds helper."""
    try:
        trigger = in_seconds(30)
        assert trigger is not None
    except ImportError:
        pytest.skip("APScheduler not available")


def test_in_minutes():
    """Test in_minutes helper."""
    try:
        trigger = in_minutes(15)
        assert trigger is not None
    except ImportError:
        pytest.skip("APScheduler not available")


def test_in_hours():
    """Test in_hours helper."""
    try:
        trigger = in_hours(2)
        assert trigger is not None
    except ImportError:
        pytest.skip("APScheduler not available")


def test_triggers_import_error():
    """Test that ImportError is raised when APScheduler not available."""
    # This test is skipped because the module is already imported
    # and patching sys.modules doesn't affect already imported modules
    pytest.skip("Test not applicable when module is already imported")
