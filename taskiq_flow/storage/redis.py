"""
Adaptateur de stockage Redis.

Implémentation de BaseStorageAdapter utilisant Redis comme backend.
Offre persistance, partage entre instances et support TTL natif.

Auteur: SoniqueBay Team
Version: 1.2.0
"""

import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

try:
    import redis.asyncio as redis_mod
except ImportError:
    redis_mod = None  # type: ignore

from taskiq_flow.storage.base import BaseStorageAdapter, StorageEntry

logger = logging.getLogger(__name__)


class RedisStorageAdapter(BaseStorageAdapter):
    """Adaptateur de stockage Redis avec retry et support TTL."""

    def __init__(
        self,
        redis_url: str = "redis://localhost:6379",
        ttl_seconds: int = 3600,
    ) -> None:
        if redis_mod is None:
            raise ImportError(
                "redis package is required for RedisStorageAdapter. "
                "Install with: pip install redis",
            )
        if redis_url is None:
            redis_url = "redis://localhost:6379"
        self._redis = redis_mod.from_url(redis_url)  # type: ignore[no-untyped-call]
        self._default_ttl = ttl_seconds
        self._key_prefix = "tqs:"

    def _full_key(self, key: str) -> str:
        """Ajoute le préfixe à la clé."""
        return f"{self._key_prefix}{key}"

    async def get(self, key: str) -> Any | None:
        """Récupère une valeur par clé."""
        try:
            full_key = self._full_key(key)
            data = await self._redis.get(full_key)
            if data is None:
                return None
            entry_data = json.loads(data)
            # Convert ISO format strings back to datetime objects
            if entry_data.get("created_at"):
                entry_data["created_at"] = datetime.fromisoformat(
                    entry_data["created_at"]
                )
            if entry_data.get("expires_at"):
                entry_data["expires_at"] = datetime.fromisoformat(
                    entry_data["expires_at"]
                )
            entry = StorageEntry(**entry_data)
            if entry.is_expired():
                await self._redis.delete(full_key)
                return None
            return entry.value
        except Exception as e:
            logger.error("Redis get failed for key %s: %s", key, e)
            raise

    async def set(
        self,
        key: str,
        value: Any,
        ttl_seconds: int | None = None,
    ) -> None:
        """Stocke une valeur avec une clé et un TTL optionnel."""
        try:
            full_key = self._full_key(key)
            ttl = ttl_seconds if ttl_seconds is not None else self._default_ttl
            entry = StorageEntry(
                key=key,
                value=value,
                expires_at=datetime.now(timezone.utc) + timedelta(seconds=ttl),
            )
            serialized = json.dumps(
                {
                    "key": entry.key,
                    "value": entry.value,
                    "created_at": entry.created_at.isoformat(),
                    "expires_at": (
                        entry.expires_at.isoformat() if entry.expires_at else None
                    ),
                    "metadata": entry.metadata,
                },
                default=str,
            )
            await self._redis.setex(full_key, ttl, serialized)
        except Exception as e:
            logger.error("Redis set failed for key %s: %s", key, e)
            raise

    async def delete(self, key: str) -> bool:
        """Supprime une entrée par clé."""
        try:
            full_key = self._full_key(key)
            result = await self._redis.delete(full_key)
            return bool(result > 0)
        except Exception as e:
            logger.error("Redis delete failed for key %s: %s", key, e)
            raise

    async def exists(self, key: str) -> bool:
        """Vérifie si une clé existe."""
        try:
            full_key = self._full_key(key)
            return bool(await self._redis.exists(full_key) > 0)
        except Exception as e:
            logger.error("Redis exists failed for key %s: %s", key, e)
            raise

    async def keys(self, pattern: str = "*") -> list[str]:
        """Liste les clés correspondant à un motif."""
        try:
            full_pattern = self._full_key(pattern)
            cursor = 0
            keys: list[str] = []
            while True:
                cursor, partial = await self._redis.scan(
                    cursor,
                    match=full_pattern,
                    count=100,
                )
                for key_bytes in partial:
                    key_str = key_bytes.decode()
                    if key_str.startswith(self._key_prefix):
                        keys.append(key_str[len(self._key_prefix) :])
                if cursor == 0:
                    break
            return keys
        except Exception as e:
            logger.error("Redis keys failed for pattern %s: %s", pattern, e)
            raise

    async def cleanup(self, ttl_seconds: int = 3600) -> int:
        """Nettoie les entrées expirées via Redis TTL natif."""
        try:
            full_pattern = self._full_key("*")
            cursor = 0
            removed = 0
            while True:
                cursor, partial = await self._redis.scan(
                    cursor,
                    match=full_pattern,
                    count=100,
                )
                for key_bytes in partial:
                    key_str = key_bytes.decode()
                    ttl = await self._redis.ttl(key_str)
                    if ttl == -1:
                        await self._redis.expire(key_str, ttl_seconds)
                if cursor == 0:
                    break
            return removed
        except Exception as e:
            logger.error("Redis cleanup failed: %s", e)
            raise
