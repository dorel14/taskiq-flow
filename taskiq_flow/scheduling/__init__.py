"""Module d'ordonnancement pour les pipelines.

Fournit les classes et fonctions pour planifier l'exécution
des pipelines via APScheduler ou le système de labels TaskIQ.

Auteur: SoniqueBay Team
Version: 0.3.2
"""

from .scheduler import LabelBasedScheduler, PipelineScheduler
from .storage import JobPersistenceManager, PipelineExecution, SchedulerJob
from .triggers import (
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
