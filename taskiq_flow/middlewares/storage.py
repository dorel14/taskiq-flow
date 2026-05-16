"""
Storage Middleware pour TaskIQ-Flow.

Fournit une couche de persistance centralisée et unifiée pour le tracking,
l'ordonnancement et d'autres composants. Les backends de stockage sont
interchangeables via une interface commune.

Auteur: SoniqueBay Team
Version: 1.2.0
"""

import logging
from typing import Any

from taskiq import TaskiqMessage, TaskiqMiddleware, TaskiqResult

from taskiq_flow.storage.base import BaseStorageAdapter

logger = logging.getLogger(__name__)


class StorageMiddleware(TaskiqMiddleware):
    """
    Middleware TaskIQ pour la persistance centralisée.

    Ce middleware intercepte les événements du pipeline et les
    persiste via un adaptateur de stockage configurable. Il peut
    fonctionner en parallèle avec le PipelineMiddleware pour le
    suivi, ou indépendamment pour des besoins de persistance
    généraux.

    Attributes:
        storage: Adaptateur de stockage sous-jacent
        enabled: Indique si la persistance est active

    """

    def __init__(
        self,
        storage: BaseStorageAdapter | None = None,
        enabled: bool = True,
    ) -> None:
        """
        Initialise le middleware de stockage.

        Args:
            storage: Adaptateur de stockage à utiliser
            enabled: Si False, toutes les opérations sont no-op

        """
        self.storage = storage
        self.enabled = enabled
        super().__init__()

    async def post_save(
        self,
        message: "TaskiqMessage",
        result: "TaskiqResult[Any]",
    ) -> None:
        """
        Persiste le résultat de l'exécution d'une tâche.

        Appelé après chaque exécution de tâche. Enregistre
        le résultat dans le stockage si la persistance est activée.

        Args:
            message: Message de la tâche exécutée
            result: Résultat de l'exécution

        """
        if not self.enabled or self.storage is None:
            return

        try:
            task_id = message.task_id
            pipeline_id = message.labels.get("pipeline_id")

            # Build storage key
            key_parts = ["task", task_id]
            if pipeline_id:
                key_parts.insert(0, f"pipeline:{pipeline_id}")

            storage_key = ":".join(key_parts)

            # Store result
            await self.storage.set(
                key=storage_key,
                value={
                    "task_id": task_id,
                    "pipeline_id": pipeline_id,
                    "is_err": result.is_err,
                    "return_value": (
                        str(result.return_value) if not result.is_err else None
                    ),
                    "error": str(result.error) if result.is_err else None,
                    "execution_time": result.execution_time,
                },
                ttl_seconds=self._get_ttl_from_message(message),
            )

            logger.debug(
                "Persisted task result for %s (pipeline=%s)",
                task_id,
                pipeline_id or "none",
            )

        except Exception as e:
            logger.error(
                "Failed to persist task result for %s: %s",
                message.task_id,
                e,
                exc_info=True,
            )

    def pre_execute(  # type: ignore[override]
        self,
        message: "TaskiqMessage",
    ) -> None:
        """Vérifie si le résultat est déjà en cache avant exécution."""

    def _get_ttl_from_message(self, message: "TaskiqMessage") -> int | None:
        """
        Extrait le TTL des labels du message si spécifié.

        Args:
            message: Message de la tâche

        Returns:
            TTL en secondes ou None

        """
        try:
            ttl_str = message.labels.get("cache_ttl")
            if ttl_str is not None:
                return int(ttl_str)
        except (ValueError, TypeError):
            pass
        return None
