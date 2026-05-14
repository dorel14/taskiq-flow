"""
Gestionnaire de suivi (tracking) des pipelines.

PipelineTrackingManager fournit une API de haut niveau pour
initialiser et interroger le suivi d'exécution des pipelines.
Il encapsule la logique de stockage et fournit des méthodes
convenientes pour marquer le début/fin des étapes.

Auteur: SoniqueBay Team
Version: 1.0.2
"""

from typing import Any

from taskiq import AsyncBroker

from taskiq_flow.tracking.factory import TrackingStorageFactory
from taskiq_flow.tracking.models import PipelineStatusInfo
from taskiq_flow.tracking.storage import PipelineStorage


class PipelineTrackingManager:
    """
    Gestionnaire de suivi d'exécution des pipelines.

    Fournit une API de haut niveau pour enregistrer et consulter
    l'état d'avancement des pipelines. Le manager utilise un
    backend de stockage (mémoire ou Redis) pour la persistance.

    Utilisation typique:
        tracking = PipelineTrackingManager()
        pipeline = Pipeline(broker).with_tracking(manager=tracking)
        await pipeline.kiq(...)
        status = await tracking.get_status(pipeline_id)

    Attributes:
        storage: Backend de stockage (implémentation de PipelineStorage)

    """

    def __init__(self, storage: PipelineStorage | None = None) -> None:
        self.storage = storage

    def with_storage(self, storage: PipelineStorage) -> "PipelineTrackingManager":
        """Set the storage backend."""
        self.storage = storage
        return self

    def with_auto_storage(
        self,
        broker: AsyncBroker,
        redis_url: str | None = None,
        ttl_seconds: int = 3600,
    ) -> "PipelineTrackingManager":
        """
        Configuration automatique du stockage selon le broker.

        Détecte le type de broker (Redis, RabbitMQ, etc.) et configure
        le stockage approprié. Si le broker est Redis et qu'aucune
        URL n'est fournie, tente d'extraire l'URL depuis le broker.

        Args:
            broker: Instance du broker TaskIQ
            redis_url: URL de connexion Redis (optionnel)
            ttl_seconds: Durée de vie des données en secondes (défaut: 3600)

        Returns:
            Self pour chaînage

        Example:
            tracking = PipelineTrackingManager().with_auto_storage(broker)

        """
        self.storage = TrackingStorageFactory.create(broker, redis_url, ttl_seconds)
        return self

    async def initiate(self, pipeline_id: str, total_steps: int) -> None:
        """
        Initialise le suivi pour un nouveau pipeline.

        Crée l'entrée de pipeline avec le nombre d'étapes attendu.
        Cette méthode est appelée automatiquement par Pipeline.kiq().

        Args:
            pipeline_id: Identifiant unique du pipeline
            total_steps: Nombre total d'étapes dans le pipeline

        """
        if self.storage:
            await self.storage.create_pipeline(pipeline_id, total_steps)

    async def mark_pipeline_started(self, pipeline_id: str) -> None:
        """Mark pipeline as started."""
        if self.storage:
            await self.storage.start_pipeline(pipeline_id)

    async def mark_pipeline_completed(self, pipeline_id: str, result: Any) -> None:
        """Mark pipeline as completed."""
        if self.storage:
            await self.storage.complete_pipeline(pipeline_id, result)

    async def mark_pipeline_failed(self, pipeline_id: str, error: str) -> None:
        """Mark pipeline as failed."""
        if self.storage:
            await self.storage.fail_pipeline(pipeline_id, error)

    async def mark_step_started(
        self,
        pipeline_id: str,
        step_index: int,
        task_id: str,
        task_name: str,
    ) -> None:
        """Mark step as started."""
        if self.storage:
            await self.storage.start_step(pipeline_id, step_index, task_id, task_name)

    async def mark_step_completed(self, pipeline_id: str, step_index: int) -> None:
        """Mark step as completed."""
        if self.storage:
            await self.storage.complete_step(pipeline_id, step_index)

    async def mark_step_failed(
        self,
        pipeline_id: str,
        step_index: int,
        error: str,
    ) -> None:
        """Mark step as failed."""
        if self.storage:
            await self.storage.fail_step(pipeline_id, step_index, error)

    async def get_status(self, pipeline_id: str) -> PipelineStatusInfo | None:
        """
        Récupère le statut complet d'un pipeline.

        Inclut l'état global, les étapes individuelles, résultats,
        et timestamps. Peut renvoyer None si le pipeline n'existe
        pas ou a expiré.

        Args:
            pipeline_id: Identifiant du pipeline

        Returns:
            PipelineStatusInfo ou None si non trouvé

        """
        if self.storage:
            return await self.storage.get_pipeline_status(pipeline_id)
        return None

    async def list_recent(self, limit: int = 10) -> list[PipelineStatusInfo]:
        """List recent pipelines."""
        if self.storage:
            return await self.storage.list_pipelines(limit)
        return []

    async def cleanup(self, ttl_seconds: int = 3600) -> int:
        """Clean up old data."""
        if self.storage:
            return await self.storage.cleanup_old(ttl_seconds)
        return 0
