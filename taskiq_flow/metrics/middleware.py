"""Middleware de métriques pour Taskiq-Flow.

Ce module fournit un middleware pour suivre les métriques des pipelines.

Auteur: SoniqueBay Team
Version: 0.4.5
"""

from typing import Any

from taskiq_flow.metrics.collector import MetricsCollector
from taskiq_flow.middleware import PipelineMiddleware


class MetricsMiddleware(PipelineMiddleware):
    """Middleware pour la collecte de métriques."""

    def __init__(self) -> None:
        """Initialise le middleware."""
        self.collector = MetricsCollector()

    async def on_pipeline_start(self, ctx: Any) -> None:
        """
        Appelé au démarrage du pipeline.

        Args:
            ctx: Contexte du pipeline
        """
        self.collector.pipeline_start(ctx.pipeline_id)

    async def on_step_complete(self, ctx: Any, result: Any) -> None:
        """
        Appelé à la fin d'une étape.

        Args:
            ctx: Contexte du pipeline
            result: Résultat de l'étape
        """
        # Le suivi de la durée nécessiterait de stocker l'heure de début dans ctx

    async def on_pipeline_complete(self, ctx: Any, success: bool) -> None:
        """
        Appelé à la fin du pipeline.

        Args:
            ctx: Contexte du pipeline
            success: Succès de l'exécution
        """
        self.collector.pipeline_complete(ctx.pipeline_id, success)


__all__ = ["MetricsMiddleware"]
