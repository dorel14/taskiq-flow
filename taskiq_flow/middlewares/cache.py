"""
Middleware de cache pour TaskIQ-Flow.

Le CacheMiddleware fournit une couche de cache haute performance
pour les workers, s'appuyant sur les sémantiques Dogpile pour
éviter les stampedes cache et assurer la cohérence.

Auteur: SoniqueBay Team
Version: 1.2.0
"""

import logging
from typing import Any

from taskiq import TaskiqMessage, TaskiqMiddleware, TaskiqResult

from taskiq_flow.storage.base import BaseCacheAdapter

logger = logging.getLogger(__name__)


class CacheMiddleware(TaskiqMiddleware):
    """
    Middleware TaskIQ pour la mise en cache des résultats.

    Intercepte les exécutions de tâches et s'appuie sur un
    adaptateur de cache pour éviter les exécutions redondantes.
    Utilise le pattern Dogpile pour gérer la régénération des
    entrées expirées sans stampede.

    Attributes:
        cache: Adaptateur de cache sous-jacent
        enabled: Indique si le cache est actif
        default_ttl: Durée de vie par défaut en secondes

    """

    def __init__(
        self,
        cache: BaseCacheAdapter | None = None,
        enabled: bool = True,
        default_ttl: int = 3600,
    ) -> None:
        """
        Initialise le middleware de cache.

        Args:
            cache: Adaptateur de cache à utiliser.
                Si None, un InMemoryCacheAdapter est utilisé.
            enabled: Si False, toutes les opérations sont no-op
            default_ttl: Durée de vie par défaut en secondes

        """
        self.cache = cache
        self.enabled = enabled
        self.default_ttl = default_ttl
        super().__init__()

    async def pre_execute(
        self,
        message: "TaskiqMessage",
    ) -> Any:
        """Vérifie le cache avant l'exécution de la tâche."""
        if not self.enabled or self.cache is None:
            return message

        try:
            task_id = message.task_id
            cache_key = f"task:{task_id}"

            cached = await self.cache.get(cache_key)

            if cached is not None:
                logger.debug(
                    "Cache hit for task %s, restoring cached result",
                    task_id,
                )
                self._hydrate_from_cache(message, cached)

        except Exception as e:
            logger.warning(
                "Cache pre_execute check failed for task %s: %s",
                task_id,
                e,
                exc_info=True,
            )
        return message

    async def post_save(
        self,
        message: "TaskiqMessage",
        result: "TaskiqResult[Any]",
    ) -> None:
        """
        Met en cache le résultat de l'exécution d'une tâche.

        Stocke le résultat pour les futures exécutions de la même tâche
        avec les mêmes paramètres.

        Args:
            message: Message de la tâche exécutée
            result: Résultat de l'exécution

        """
        if not self.enabled or self.cache is None:
            return

        if result.is_err and not self._should_cache_errors(message):
            return

        try:
            task_id = message.task_id
            cache_key = f"task:{task_id}"
            ttl = self._get_ttl_from_message(message) or self.default_ttl

            cached_value: dict[str, Any] = {
                "is_err": result.is_err,
                "return_value": (result.return_value if not result.is_err else None),
                "error": str(result.error) if result.is_err else None,
                "execution_time": result.execution_time,
            }

            await self.cache.set(
                key=cache_key,
                value=cached_value,
                ttl_seconds=ttl,
            )

            logger.debug(
                "Cached result for task %s (ttl=%ds)",
                task_id,
                ttl,
            )

        except Exception as e:
            logger.warning(
                "Failed to cache result for task %s: %s",
                task_id,
                e,
                exc_info=True,
            )

    def _hydrate_from_cache(
        self,
        message: "TaskiqMessage",
        cached: dict[str, Any],
    ) -> None:
        """Hydrate le message avec les données en cache."""
        message.labels["__cached"] = "true"
        message.labels["__cached_is_err"] = str(cached.get("is_err", False))
        if cached.get("return_value") is not None:
            message.labels["__cached_result"] = str(cached["return_value"])

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

    def _should_cache_errors(self, message: "TaskiqMessage") -> bool:
        """
        Détermine si les erreurs doivent être mises en cache.

        Args:
            message: Message de la tâche

        Returns:
            True si les erreurs doivent être mises en cache

        """
        cache_errors = message.labels.get("cache_errors", "false")
        return str(cache_errors).lower() == "true"
