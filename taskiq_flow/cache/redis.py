"""
Adaptateur de cache Redis avec support Dogpile.

Fournit un cache distribué basé sur Redis avec verrouillage,
TTL et sémantiques Dogpile pour éviter les stampedes cache.

Auteur: SoniqueBay Team
Version: 1.2.0
"""

import asyncio
import json
import logging
import uuid
from collections.abc import Callable
from typing import Any

try:
    import redis.asyncio as redis_mod
except ImportError:
    redis_mod = None  # type: ignore

from taskiq_flow.serialization import dumps_scientific, loads_scientific
from taskiq_flow.storage.base import BaseCacheAdapter

logger = logging.getLogger(__name__)


class RedisCacheAdapter(BaseCacheAdapter):
    """
    Adaptateur de cache Redis avec verrouillage distribué.

    Utilise Redis pour le stockage cache avec:
    - TTL natif Redis pour l'expiration
    - Verrouillage distribué pour éviter les stampedes
    - Sérialisation JSON pour les valeurs complexes
    """

    def __init__(
        self,
        redis_url: str = "redis://localhost:6379",
        default_ttl: int = 3600,
        lock_timeout: int = 10,
    ) -> None:
        if redis_mod is None:
            raise ImportError(
                "redis package is required for RedisCacheAdapter. "
                "Install with: pip install redis",
            )
        self._redis = redis_mod.from_url(redis_url)  # type: ignore[no-untyped-call]
        self._default_ttl = default_ttl
        self._lock_timeout = lock_timeout
        self._key_prefix = "tqs:cache:"
        self._lock_prefix = "tqs:cache:lock:"
        self._hits = 0
        self._misses = 0

    def _cache_key(self, key: str) -> str:
        return f"{self._key_prefix}{key}"

    def _lock_key(self, key: str) -> str:
        return f"{self._lock_prefix}{key}"

    async def get_or_create(
        self,
        key: str,
        creator: Callable[[], Any],
        ttl_seconds: int = 3600,
    ) -> Any:
        """
        Récupère du cache ou crée avec verrouillage distribué anti-stampede.

        Pattern Dogpile: acquiert un verrou pour régénérer les données
        périmées, évitant que plusieurs processus ne régénèrent
        simultanément la même valeur.
        """
        cache_key = self._cache_key(key)

        # Fast path: try to get from cache
        try:
            data = await self._redis.get(cache_key)
            if data is not None:
                self._hits += 1
                return loads_scientific(data)
        except Exception as e:
            logger.error("Redis cache get failed for key %s: %s", key, e)
            raise

        # Cache miss - acquire lock
        lock_key = self._lock_key(key)
        lock_value = str(uuid.uuid4())
        lock_acquired = False

        try:
            # Try to acquire lock with NX (only if not exists)
            lock_acquired = bool(
                await self._redis.set(
                    lock_key,
                    lock_value,
                    nx=True,
                    ex=self._lock_timeout,
                )
            )

            if not lock_acquired:
                # Another process is creating the value, wait and retry
                await asyncio.sleep(0.1)
                data = await self._redis.get(cache_key)
                if data is not None:
                    self._hits += 1
                    return json.loads(data)
                # If still not available, wait a bit more
                await asyncio.sleep(0.5)
                data = await self._redis.get(cache_key)
                if data is not None:
                    self._hits += 1
                    return json.loads(data)
                raise TimeoutError(
                    f"Timeout waiting for cache lock for key: {key}",
                )

            # We hold the lock - create the value
            value = creator()
            if isinstance(value, object) and hasattr(value, "__await__"):
                value = await value

            ttl = ttl_seconds if ttl_seconds > 0 else self._default_ttl
            serialized = json.dumps(value, default=str)
            await self._redis.setex(cache_key, ttl, serialized)
            self._misses += 1
            return value

        except Exception as e:
            logger.error("Redis cache get_or_create failed for key %s: %s", key, e)
            raise

        finally:
            if lock_acquired:
                # Release lock safely using Lua script
                try:
                    lua_script = """
                    if redis.call("get", KEYS[1]) == ARGV[1] then
                        return redis.call("del", KEYS[1])
                    else
                        return 0
                    end
                    """
                    await self._redis.eval(lua_script, 1, lock_key, lock_value)
                except Exception as e:
                    logger.warning(
                        "Failed to release cache lock for key %s: %s",
                        key,
                        e,
                    )

    async def get(self, key: str) -> Any | None:
        """Récupère une valeur du cache sans création."""
        try:
            data = await self._redis.get(self._cache_key(key))
            if data is not None:
                self._hits += 1
                return loads_scientific(data)
            self._misses += 1
            return None
        except Exception as e:
            logger.error("Redis cache get failed for key %s: %s", key, e)
            raise

    async def set(
        self,
        key: str,
        value: Any,
        ttl_seconds: int = 3600,
    ) -> None:
        """Stocke une valeur dans le cache avec TTL."""
        try:
            ttl = ttl_seconds if ttl_seconds > 0 else self._default_ttl
            serialized = dumps_scientific(value)
            await self._redis.setex(self._cache_key(key), ttl, serialized)
        except Exception as e:
            logger.error("Redis cache set failed for key %s: %s", key, e)
            raise

    async def invalidate(self, key: str) -> bool:
        """Invalide une entrée du cache."""
        try:
            result = await self._redis.delete(self._cache_key(key))
            return bool(result > 0)
        except Exception as e:
            logger.error("Redis cache invalidate failed for key %s: %s", key, e)
            raise

    async def clear(self) -> None:
        """Vide intégralement le cache."""
        try:
            cursor = 0
            while True:
                cursor, keys = await self._redis.scan(
                    cursor,
                    match=f"{self._key_prefix}*",
                    count=100,
                )
                if keys:
                    await self._redis.delete(*keys)
                if cursor == 0:
                    break
        except Exception as e:
            logger.error("Redis cache clear failed: %s", e)
            raise

    def get_stats(self) -> dict[str, Any]:
        """Retourne les statistiques du cache."""
        total = self._hits + self._misses
        return {
            "hits": self._hits,
            "misses": self._misses,
            "hit_rate": self._hits / total if total > 0 else 0.0,
            "type": "redis",
        }
