"""
Module d'ordonnancement pour les pipelines.

Fournit les classes et fonctions pour planifier l'exécution
des pipelines via APScheduler ou le système de labels TaskIQ.

Auteur: SoniqueBay Team
Version: 1.2.0
"""

from .scheduler import (
    LabelBasedScheduler,
)  # always works (no APScheduler import at top of scheduler.py)

try:
    from .scheduler import PipelineScheduler
except ImportError:
    PipelineScheduler = None  # type: ignore

try:
    from .storage import JobPersistenceManager
except ImportError:
    JobPersistenceManager = None  # type: ignore

try:
    from .storage import PersistenceAdapter
except ImportError:
    PersistenceAdapter = None  # type: ignore

from .storage import (  # Pydantic, independent of SQLAlchemy
    PipelineExecution,
    SchedulerJob,
)
from .triggers import (  # all guarded in triggers.py itself
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

__all__ = [
    "JobPersistenceManager",
    "LabelBasedScheduler",
    "PersistenceAdapter",
    "PipelineExecution",
    "PipelineScheduler",
    "SchedulerJob",
    "create_cron_trigger",
    "create_date_trigger",
    "create_interval_trigger",
    "every_day",
    "every_hour",
    "every_minute",
    "in_hours",
    "in_minutes",
    "in_seconds",
]
