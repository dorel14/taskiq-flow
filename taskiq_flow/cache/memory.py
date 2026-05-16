"""
Adaptateur de cache en mémoire avec support Dogpile.

Fournit un cache thread-safe avec support TTL, verrouillage
pour éviter les stampedes, et statistiques d'accès.

Attributes:
    _cache: Dictionnaire de stockage principal
    _locks: Dictionnaire de verrous par clé (anti-stampede)
    _hits: Nombre de hits
    _misses: Nombre de misses

Auteur: SoniqueBay Team
Version: 1.2.0

"""

import inspect
import threading
import time as _time
from collections.abc import Callable
from typing import Any

from taskiq_flow.storage.base import BaseCacheAdapter


class InMemoryCacheAdapter(BaseCacheAdapter):
    """
    Adaptateur de cache en mémoire avec sémantiques Dogpile.

    Fournit un cache thread-safe avec support TTL, verrouillage
    pour éviter les stampedes, et statistiques d'accès.
    """

    def __init__(self) -> None:
        self._cache: dict[str, dict[str, Any]] = {}
        self._locks: dict[str, threading.Lock] = {}
        self._lock_creation = threading.Lock()
        self._hits = 0
        self._misses = 0

    def _get_lock(self, key: str) -> threading.Lock:
        """Obtient ou crée un verrou pour une clé donnée."""
        with self._lock_creation:
            if key not in self._locks:
                self._locks[key] = threading.Lock()
            return self._locks[key]

    def _is_expired(self, entry: dict[str, Any]) -> bool:
        """Vérifie si une entrée a expiré."""
        return entry["expire_at"] is not None and _time.monotonic() > entry["expire_at"]

    async def get_or_create(
        self,
        key: str,
        creator: Callable[[], Any],
        ttl_seconds: int = 3600,
    ) -> Any:
        """
        Récupère du cache ou crée avec verrouillage anti-stampede.

        Implémente le pattern Dogpile: si le cache contient une valeur
        périmée, le verrou empêche les autres threads de régénérer
        simultanément.

        Args:
            key: Clé du cache
            creator: Fonction appelée pour créer la valeur si absente
            ttl_seconds: Durée de vie en secondes

        Returns:
            Valeur en cache

        """
        # Fast path: check cache without lock
        entry = self._cache.get(key)
        if entry and not self._is_expired(entry):
            self._hits += 1
            return entry["value"]

        lock = self._get_lock(key)
        with lock:
            # Double-check after acquiring lock
            entry = self._cache.get(key)
            if entry and not self._is_expired(entry):
                self._hits += 1
                return entry["value"]

            # Create the value
            value = creator()
            if inspect.iscoroutine(value):
                # Handle async creators
                value = await value

            self._cache[key] = {
                "value": value,
                "expire_at": (
                    _time.monotonic() + ttl_seconds if ttl_seconds > 0 else None
                ),
            }
            self._misses += 1
            return value

    async def get(self, key: str) -> Any | None:
        """
        Récupère une valeur du cache sans création.

        Args:
            key: Clé du cache

        Returns:
            Valeur en cache ou None

        """
        entry = self._cache.get(key)
        if entry is None or self._is_expired(entry):
            self._misses += 1
            return None
        self._hits += 1
        return entry["value"]

    async def set(
        self,
        key: str,
        value: Any,
        ttl_seconds: int = 3600,
    ) -> None:
        """
        Stocke une valeur dans le cache.

        Args:
            key: Clé du cache
            value: Valeur à mettre en cache
            ttl_seconds: Durée de vie en secondes

        """
        self._cache[key] = {
            "value": value,
            "expire_at": (_time.monotonic() + ttl_seconds if ttl_seconds > 0 else None),
        }

    async def invalidate(self, key: str) -> bool:
        """
        Invalide une entrée du cache.

        Args:
            key: Clé de l'entrée à invalider

        Returns:
            True si l'entrée a été invalidée, False sinon

        """
        if key in self._cache:
            del self._cache[key]
            return True
        return False

    async def clear(self) -> None:
        """Vide intégralement le cache."""
        self._cache.clear()

    def get_stats(self) -> dict[str, Any]:
        """
        Retourne les statistiques du cache.

        Returns:
            Dictionnaire de statistiques

        """
        total = self._hits + self._misses
        return {
            "hits": self._hits,
            "misses": self._misses,
            "hit_rate": self._hits / total if total > 0 else 0.0,
            "size": len(self._cache),
            "keys": list(self._cache.keys()),
        }
