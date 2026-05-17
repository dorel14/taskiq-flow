"""
Factory de création des adaptateurs de stockage et de cache.

Détecte automatiquement le type de broker et de cache disponibles,
puis crée les adaptateurs appropriés.

Auteur: SoniqueBay Team
Version: 1.2.0
"""

try:
    import redis
except ImportError:
    redis = None  # type: ignore

from taskiq import AsyncBroker

from taskiq_flow.cache.memory import InMemoryCacheAdapter
from taskiq_flow.cache.redis import RedisCacheAdapter
from taskiq_flow.config import TaskiqFlowConfig
from taskiq_flow.storage.base import BaseCacheAdapter, BaseStorageAdapter
from taskiq_flow.storage.memory import InMemoryStorageAdapter
from taskiq_flow.storage.redis import RedisStorageAdapter
from taskiq_flow.storage.sqlite import SQLiteStorageAdapter


class StorageAdapterFactory:
    """Factory for creating storage and cache adapters based on configuration."""

    @staticmethod
    def create_storage_adapter(
        config: TaskiqFlowConfig | None = None,
        broker: AsyncBroker | None = None,
        redis_url: str | None = None,
        ttl_seconds: int = 3600,
    ) -> BaseStorageAdapter:
        """
        Create a storage adapter based on configuration or auto-detection.

        Priority order when type is "auto":
        1. Redis (if redis_url or broker is Redis)
        2. SQLite (default fallback for persistent storage)
        3. InMemory (if no Redis available)

        Args:
            config: TaskiqFlowConfig instance (optional)

            broker: AsyncBroker instance for auto-detection (optional)

            redis_url: Redis URL override (optional)

            ttl_seconds: Default TTL in seconds

        Returns:
            A storage adapter instance

        """
        storage_type = config.storage_type if config else "auto"
        redis_url = redis_url or (config.storage_redis_url if config else None)
        ttl = config.storage_ttl_seconds if config else ttl_seconds

        if storage_type == "redis" or (
            storage_type == "auto" and _has_redis_available()
        ):
            url: str | None = redis_url or _extract_redis_url_from_broker(broker)
            if url is None:
                url = "redis://localhost:6379"
            return RedisStorageAdapter(redis_url=url, ttl_seconds=ttl)

        if storage_type == "sqlite":
            sqlite_url = (
                config.storage_sqlite_url
                if config
                else "sqlite+aiosqlite:///taskiq_flow.db"
            )
            async_mode = config.storage_async_mode if config else True
            return SQLiteStorageAdapter(
                db_url=sqlite_url,
                async_mode=async_mode,
            )

        if storage_type == "sqlalchemy":
            sa_url = (
                config.storage_sqlalchemy_url
                if config and config.storage_sqlalchemy_url
                else "sqlite+aiosqlite:///taskiq_flow.db"
            )
            async_mode = config.storage_async_mode if config else True
            return SQLiteStorageAdapter(
                db_url=sa_url,
                async_mode=async_mode,
            )

        return InMemoryStorageAdapter()

    @staticmethod
    def create_cache_adapter(
        config: TaskiqFlowConfig | None = None,
        redis_url: str | None = None,
        default_ttl: int = 3600,
        lock_timeout: int = 10,
    ) -> BaseCacheAdapter:
        """
        Create a cache adapter based on configuration.

        Args:
            config: TaskiqFlowConfig instance (optional)

            redis_url: Redis URL override (optional)

            default_ttl: Default TTL in seconds

            lock_timeout: Lock timeout for anti-stampede

        Returns:
            A cache adapter instance

        """
        cache_type = config.cache_type if config else "auto"
        redis_url = redis_url or (config.cache_redis_url if config else None)

        if cache_type == "redis":
            # Explicitly requested redis, use the provided url
            # or the default from the adapter
            url: str = redis_url or "redis://localhost:6379"
            return RedisCacheAdapter(
                redis_url=url,
                default_ttl=(
                    default_ttl if config is None else config.cache_default_ttl
                ),
                lock_timeout=(
                    lock_timeout if config is None else config.cache_lock_timeout
                ),
            )
        if cache_type == "auto" and _has_redis_available():
            # In auto mode, we only use redis if we have a url (from config or broker)
            redis_url_auto: str | None = redis_url or _extract_redis_url_from_broker(
                None
            )
            if redis_url_auto is not None:
                return RedisCacheAdapter(
                    redis_url=redis_url_auto,
                    default_ttl=(
                        default_ttl if config is None else config.cache_default_ttl
                    ),
                    lock_timeout=(
                        lock_timeout if config is None else config.cache_lock_timeout
                    ),
                )

        return InMemoryCacheAdapter()

    @staticmethod
    def create_default_middlewares(
        config: TaskiqFlowConfig | None = None,
        broker: AsyncBroker | None = None,
    ) -> dict[str, object]:
        """
        Create a set of default middlewares from configuration.

        Args:
            config: TaskiqFlowConfig instance

            broker: AsyncBroker instance for auto-detection

        Returns:
            Dictionary of middleware instances keyed by name

        """
        from taskiq_flow.middlewares.cache import CacheMiddleware  # noqa: PLC0415
        from taskiq_flow.middlewares.storage import (  # noqa: PLC0415
            StorageMiddleware,
        )

        storage_adapter = StorageAdapterFactory.create_storage_adapter(
            config=config,
            broker=broker,
        )
        cache_adapter = StorageAdapterFactory.create_cache_adapter(config=config)

        return {
            "storage": StorageMiddleware(storage=storage_adapter),
            "cache": CacheMiddleware(cache=cache_adapter),
        }


def _has_redis_available() -> bool:
    """
    Check if redis package is available.

    Returns:
        True if redis is installed, False otherwise

    """
    return redis is not None


def _extract_redis_url_from_broker(broker: AsyncBroker | None) -> str | None:
    """
    Extract Redis URL from broker if possible.

    Args:
        broker: AsyncBroker instance or None

    Returns:
        Redis URL string or None

    """
    if broker is None:
        return None

    # Try to get URL from broker's url attribute
    broker_url = getattr(broker, "url", None)
    if broker_url and str(broker_url).startswith("redis://"):
        return str(broker_url)

    # Try taskiq_reds
    try:
        from taskiq_redis.broker import RedisBroker  # noqa: PLC0415

        if isinstance(broker, RedisBroker):
            return getattr(broker, "url", None) or "redis://localhost:6379"
    except ImportError:
        pass

    return None
