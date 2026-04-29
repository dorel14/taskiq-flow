"""SQLite job store configuration."""

try:
    from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
except ImportError:
    SQLAlchemyJobStore = None


def create_sqlite_jobstore(url: str = "sqlite:///./scheduler_jobs.db"):
    """Create SQLite job store."""
    if SQLAlchemyJobStore is None:
        raise ImportError("APScheduler required for job stores")
    return SQLAlchemyJobStore(url=url)