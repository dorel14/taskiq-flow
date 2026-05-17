"""
Adaptateur de stockage en mémoire avec support TTL.

Destiné au développement, aux tests et aux environnements sans Redis.

Auteur: SoniqueBay Team
Version: 1.2.0
"""

import asyncio
import fnmatch
from datetime import datetime, timedelta, timezone
from typing import Any

from taskiq_flow.storage.base import BaseStorageAdapter, StorageEntry


class InMemoryStorageAdapter(BaseStorageAdapter):
    """Adaptateur de stockage en mémoire avec support TTL."""

    def __init__(self) -> None:
        self._pipelines: dict[str, StorageEntry] = {}
        self._lock = asyncio.Lock()

    async def get(self, key: str) -> Any | None:
        """Récupère une valeur par clé."""
        async with self._lock:
            entry = self._pipelines.get(key)
            if entry is None:
                return None
            if entry.is_expired():
                del self._pipelines[key]
                return None
            return entry.value

    async def set(
        self,
        key: str,
        value: Any,
        ttl_seconds: int | None = None,
    ) -> None:
        """Stocke une valeur avec une clé et un TTL optionnel."""
        async with self._lock:
            expires_at = None
            if ttl_seconds is not None:
                expires_at = datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds)
            self._pipelines[key] = StorageEntry(
                key=key,
                value=value,
                expires_at=expires_at,
            )

    async def delete(self, key: str) -> bool:
        """Supprime une entrée par clé."""
        async with self._lock:
            if key in self._pipelines:
                del self._pipelines[key]
                return True
            return False

    async def exists(self, key: str) -> bool:
        """Vérifie si une clé existe."""
        async with self._lock:
            entry = self._pipelines.get(key)
            if entry is None:
                return False
            if entry.is_expired():
                del self._pipelines[key]
                return False
            return True

    async def keys(self, pattern: str = "*") -> list[str]:
        """Liste les clés correspondant à un motif."""
        async with self._lock:
            self._cleanup_expired_no_lock()
            if pattern == "*":
                return list(self._pipelines.keys())
            return [k for k in self._pipelines if fnmatch.fnmatch(k, pattern)]

    async def cleanup(self, ttl_seconds: int = 3600) -> int:
        """Nettoie les entrées expirées."""
        async with self._lock:
            return self._cleanup_expired_no_lock()

    def _cleanup_expired_no_lock(self) -> int:
        """Nettoie les entrées expirées (doit être appelé sous lock)."""
        now = datetime.now(timezone.utc)
        to_remove = [
            key
            for key, entry in self._pipelines.items()
            if entry.expires_at is not None and entry.expires_at < now
        ]
        for key in to_remove:
            del self._pipelines[key]
        return len(to_remove)
