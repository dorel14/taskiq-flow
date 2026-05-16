"""
Gestionnaire de suivi (tracking) des pipelines.

PipelineTrackingManager fournit une API de haut niveau pour
initialiser et interroger le suivi d'exécution des pipelines.
Il encapsule la logique de stockage et fournit des méthodes
convenientes pour marquer le début/fin des étapes.

Auteur: SoniqueBay Team
Version: 1.2.0
"""

from typing import Any

from taskiq import AsyncBroker

from taskiq_flow.storage.base import BaseStorageAdapter
from taskiq_flow.tracking.factory import TrackingStorageFactory
from taskiq_flow.tracking.models import PipelineStatusInfo
from taskiq_flow.tracking.storage import PipelineStorage


class _TrackingStorageWrapper(PipelineStorage):
    """
    Wrapper adaptant BaseStorageAdapter à l'interface PipelineStorage.

    Permet d'utiliser un BaseStorageAdapter comme backend de stockage
    pour le PipelineTrackingManager tout en respectant l'interface
    PipelineStorage attendue par les composants existants.

    Args:
        adapter: Adaptateur de stockage à envelopper

    """

    def __init__(self, adapter: BaseStorageAdapter) -> None:
        super().__init__()
        self._adapter = adapter

    async def create_pipeline(self, pipeline_id: str, total_steps: int) -> None:
        """
        Crée une nouvelle entrée de pipeline.

        Args:
            pipeline_id: Identifiant unique du pipeline
            total_steps: Nombre total d'étapes attendues

        """
        await self._adapter.set(
            key=f"pipeline:{pipeline_id}",
            value={
                "pipeline_id": pipeline_id,
                "status": "pending",
                "total_steps": total_steps,
                "current_step": 0,
                "steps": [
                    {
                        "step_index": i,
                        "task_name": "",
                        "task_id": "",
                        "status": "pending",
                    }
                    for i in range(total_steps)
                ],
            },
        )

    async def start_pipeline(self, pipeline_id: str) -> None:
        """
        Marque le pipeline comme démarré.

        Args:
            pipeline_id: Identifiant du pipeline

        """
        data = await self._adapter.get(f"pipeline:{pipeline_id}")
        if data:
            data["status"] = "running"
            await self._adapter.set(key=f"pipeline:{pipeline_id}", value=data)

    async def complete_pipeline(self, pipeline_id: str, result: Any) -> None:
        """
        Marque le pipeline comme terminé avec succès.

        Args:
            pipeline_id: Identifiant du pipeline
            result: Résultat final de l'exécution

        """
        data = await self._adapter.get(f"pipeline:{pipeline_id}")
        if data:
            data["status"] = "completed"
            data["result"] = result
            await self._adapter.set(key=f"pipeline:{pipeline_id}", value=data)

    async def fail_pipeline(self, pipeline_id: str, error: str) -> None:
        """
        Marque le pipeline comme échoué.

        Args:
            pipeline_id: Identifiant du pipeline
            error: Message d'erreur décrivant l'échec

        """
        data = await self._adapter.get(f"pipeline:{pipeline_id}")
        if data:
            data["status"] = "failed"
            data["error"] = error
            await self._adapter.set(key=f"pipeline:{pipeline_id}", value=data)

    async def start_step(
        self,
        pipeline_id: str,
        step_index: int,
        task_id: str,
        task_name: str,
    ) -> None:
        """
        Marque une étape comme démarrée.

        Args:
            pipeline_id: Identifiant du pipeline
            step_index: Index de l'étape (0-based)
            task_id: ID de la tâche TaskIQ
            task_name: Nom de la tâche

        """
        data = await self._adapter.get(f"pipeline:{pipeline_id}")
        if data and "steps" in data and step_index < len(data["steps"]):
            data["steps"][step_index]["status"] = "running"
            data["steps"][step_index]["task_id"] = task_id
            data["steps"][step_index]["task_name"] = task_name
            await self._adapter.set(key=f"pipeline:{pipeline_id}", value=data)

    async def complete_step(self, pipeline_id: str, step_index: int) -> None:
        """
        Marque une étape comme terminée.

        Args:
            pipeline_id: Identifiant du pipeline
            step_index: Index de l'étape

        """
        data = await self._adapter.get(f"pipeline:{pipeline_id}")
        if data and "steps" in data and step_index < len(data["steps"]):
            data["steps"][step_index]["status"] = "completed"
            await self._adapter.set(key=f"pipeline:{pipeline_id}", value=data)

    async def fail_step(self, pipeline_id: str, step_index: int, error: str) -> None:
        """
        Marque une étape comme échouée.

        Args:
            pipeline_id: Identifiant du pipeline
            step_index: Index de l'étape
            error: Message d'erreur

        """
        data = await self._adapter.get(f"pipeline:{pipeline_id}")
        if data and "steps" in data and step_index < len(data["steps"]):
            data["steps"][step_index]["status"] = "failed"
            data["steps"][step_index]["error"] = error
            await self._adapter.set(key=f"pipeline:{pipeline_id}", value=data)

    async def get_pipeline_status(self, pipeline_id: str) -> PipelineStatusInfo | None:
        """
        Récupère le statut complet d'un pipeline.

        Args:
            pipeline_id: Identifiant du pipeline

        Returns:
            PipelineStatusInfo ou None si non trouvé

        """
        data = await self._adapter.get(f"pipeline:{pipeline_id}")
        if data:
            return PipelineStatusInfo(**data)
        return None

    async def list_pipelines(self, limit: int = 10) -> list[PipelineStatusInfo]:
        """
        Liste les pipelines les plus récents.

        Args:
            limit: Nombre maximum de pipelines à retourner

        Returns:
            Liste ordonnée par date de création décroissante

        """
        keys = await self._adapter.keys("pipeline=*")
        pipelines: list[PipelineStatusInfo] = []
        for key in keys[:limit]:
            data = await self._adapter.get(key)
            if data:
                pipelines.append(PipelineStatusInfo(**data))
        return pipelines

    async def cleanup_old(self, ttl_seconds: int = 3600) -> int:
        """
        Nettoie les données anciennes dépassées.

        Args:
            ttl_seconds: Durée de vie en secondes

        Returns:
            Nombre d'éléments supprimés

        """
        return await self._adapter.cleanup(ttl_seconds)


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
        storage: Backend de stockage (implémentation de PipelineStorage
            ou BaseStorageAdapter)

    """

    def __init__(
        self,
        storage: PipelineStorage | BaseStorageAdapter | None = None,
    ) -> None:
        self.storage = storage
        self._active_storage: PipelineStorage | None = None
        if isinstance(storage, BaseStorageAdapter) and not isinstance(
            storage, PipelineStorage
        ):
            self._active_storage = _TrackingStorageWrapper(storage)
        elif isinstance(storage, PipelineStorage):
            self._active_storage = storage

    def with_storage(
        self, storage: PipelineStorage | BaseStorageAdapter
    ) -> "PipelineTrackingManager":
        """
        Set the storage backend.

        Args:
            storage: Backend de stockage (PipelineStorage ou BaseStorageAdapter)

        Returns:
            Self pour chaînage

        """
        self.storage = storage
        if isinstance(storage, BaseStorageAdapter) and not isinstance(
            storage, PipelineStorage
        ):
            self._active_storage = _TrackingStorageWrapper(storage)
        elif isinstance(storage, PipelineStorage):
            self._active_storage = storage
        else:
            self._active_storage = None
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
        if isinstance(self.storage, BaseStorageAdapter) and not isinstance(
            self.storage, PipelineStorage
        ):
            self._active_storage = _TrackingStorageWrapper(self.storage)
        elif isinstance(self.storage, PipelineStorage):
            self._active_storage = self.storage
        else:
            self._active_storage = None
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
        if self._active_storage:
            await self._active_storage.create_pipeline(pipeline_id, total_steps)

    async def mark_pipeline_started(self, pipeline_id: str) -> None:
        """Mark pipeline as started."""
        if self._active_storage:
            await self._active_storage.start_pipeline(pipeline_id)

    async def mark_pipeline_completed(self, pipeline_id: str, result: Any) -> None:
        """Mark pipeline as completed."""
        if self._active_storage:
            await self._active_storage.complete_pipeline(pipeline_id, result)

    async def mark_pipeline_failed(self, pipeline_id: str, error: str) -> None:
        """Mark pipeline as failed."""
        if self._active_storage:
            await self._active_storage.fail_pipeline(pipeline_id, error)

    async def mark_step_started(
        self,
        pipeline_id: str,
        step_index: int,
        task_id: str,
        task_name: str,
    ) -> None:
        """Mark step as started."""
        if self._active_storage:
            await self._active_storage.start_step(
                pipeline_id, step_index, task_id, task_name
            )

    async def mark_step_completed(self, pipeline_id: str, step_index: int) -> None:
        """Mark step as completed."""
        if self._active_storage:
            await self._active_storage.complete_step(pipeline_id, step_index)

    async def mark_step_failed(
        self,
        pipeline_id: str,
        step_index: int,
        error: str,
    ) -> None:
        """Mark step as failed."""
        if self._active_storage:
            await self._active_storage.fail_step(pipeline_id, step_index, error)

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
        if self._active_storage:
            return await self._active_storage.get_pipeline_status(pipeline_id)
        return None

    async def list_recent(self, limit: int = 10) -> list[PipelineStatusInfo]:
        """List recent pipelines."""
        if self._active_storage:
            return await self._active_storage.list_pipelines(limit)
        return []

    async def cleanup(self, ttl_seconds: int = 3600) -> int:
        """Clean up old data."""
        if self._active_storage:
            return await self._active_storage.cleanup_old(ttl_seconds)
        return 0
