"""Configuration du job store SQLite pour APScheduler.

Fournit une configuration simple utilisant SQLite comme backend
de persistance des tâches planifiées.

Auteur: SoniqueBay Team
Version: 1.0.2
"""

from typing import Any

try:
    from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
except ImportError:
    SQLAlchemyJobStore = None


def create_sqlite_jobstore(url: str = "sqlite:///./scheduler_jobs.db") -> Any:
    """Create SQLite job store."""
    if SQLAlchemyJobStore is None:
        raise ImportError("APScheduler required for job stores")
    return SQLAlchemyJobStore(url=url)
