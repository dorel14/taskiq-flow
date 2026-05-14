"""
Collecteur de métriques pour Taskiq-Flow.

Ce module fournit un collecteur de métriques qui suit les événements
des pipelines et expose les métriques au format Prometheus.

Auteur: SoniqueBay Team
Version: 1.0.2
"""

import time

from prometheus_client import REGISTRY, generate_latest
from typing_extensions import Self

from taskiq_flow.metrics import (
    ACTIVE_PIPELINES,
    PIPELINE_DURATION_SECONDS,
    PIPELINE_EXECUTIONS_TOTAL,
    TASK_DURATION_SECONDS,
    TASK_EXECUTIONS_TOTAL,
    TASK_RETRY_ATTEMPTS,
    WEBSOCKET_MESSAGES_TOTAL,
)


class MetricsCollector:
    """Collecteur de métriques singleton."""

    _instance: Self
    _initialized: bool = False

    def __new__(cls) -> Self:
        """Create or return the singleton instance."""
        if not hasattr(cls, "_instance"):
            instance = super().__new__(cls)
            instance._initialized = False
            cls._instance = instance
        return cls._instance

    def __init__(self) -> None:
        """Initialize the metrics collector."""
        if self._initialized:
            return
        self._initialized = True
        self._pipeline_starts: dict[str, float] = {}

    def pipeline_start(self, pipeline_id: str) -> None:
        """
        Enregistre le début d'exécution d'un pipeline.

        Args:
            pipeline_id: Identifiant du pipeline

        """
        PIPELINE_EXECUTIONS_TOTAL.labels(
            pipeline_id=pipeline_id, status="started"
        ).inc()
        ACTIVE_PIPELINES.labels(pipeline_id=pipeline_id).inc()
        self._pipeline_starts[pipeline_id] = time.time()

    def pipeline_complete(self, pipeline_id: str, success: bool) -> None:
        """
        Enregistre la fin d'exécution d'un pipeline.

        Args:
            pipeline_id: Identifiant du pipeline
            success: Succès de l'exécution

        """
        status = "success" if success else "failure"
        PIPELINE_EXECUTIONS_TOTAL.labels(pipeline_id=pipeline_id, status=status).inc()

        if pipeline_id in self._pipeline_starts:
            duration = time.time() - self._pipeline_starts.pop(pipeline_id)
            PIPELINE_DURATION_SECONDS.labels(pipeline_id=pipeline_id).observe(duration)

        ACTIVE_PIPELINES.labels(pipeline_id=pipeline_id).dec()

    def task_executed(self, task_name: str, status: str, duration: float) -> None:
        """
        Enregistre l'exécution d'une tâche.

        Args:
            task_name: Nom de la tâche
            status: Statut (success/failure)
            duration: Durée d'exécution

        """
        TASK_EXECUTIONS_TOTAL.labels(
            task_name=task_name, status=status, queue="default"
        ).inc()
        TASK_DURATION_SECONDS.labels(task_name=task_name, task_type="mixed").observe(
            duration
        )

    def task_retried(self, task_name: str, exception_type: str) -> None:
        """
        Enregistre un réessai de tâche.

        Args:
            task_name: Nom de la tâche
            exception_type: Type d'exception

        """
        TASK_RETRY_ATTEMPTS.labels(
            task_name=task_name, exception_type=exception_type
        ).inc()

    def step_started(self, pipeline_id: str, task_name: str) -> None:
        """
        Enregistre le démarrage d'une étape.

        Args:
            pipeline_id: Identifiant du pipeline
            task_name: Nom de la tâche

        """
        self.websocket_message(pipeline_id, "in", "step_start")

    def websocket_message(
        self, pipeline_id: str, direction: str, msg_type: str
    ) -> None:
        """
        Enregistre un message WebSocket.

        Args:
            pipeline_id: Identifiant du pipeline
            direction: Direction (in/out)
            msg_type: Type de message

        """
        WEBSOCKET_MESSAGES_TOTAL.labels(
            pipeline_id=pipeline_id, direction=direction, type=msg_type
        ).inc()

    def export_metrics(self) -> bytes:
        """
        Exporte les métriques au format Prometheus.

        Returns:
            Données métriques brutes

        """
        return generate_latest(REGISTRY)


__all__ = ["MetricsCollector"]
