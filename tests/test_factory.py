"""
Tests for StorageAdapterFactory.

This module tests the factory that creates storage and cache
adapters based on configuration and auto-detection.
"""

from unittest.mock import MagicMock, patch

import pytest
from taskiq import InMemoryBroker

from taskiq_flow.cache.memory import InMemoryCacheAdapter
from taskiq_flow.cache.redis import RedisCacheAdapter
from taskiq_flow.config import TaskiqFlowConfig
from taskiq_flow.middlewares.cache import CacheMiddleware
from taskiq_flow.middlewares.storage import StorageMiddleware
from taskiq_flow.storage.factory import (
    StorageAdapterFactory,
    _extract_redis_url_from_broker,
    _has_redis_available,
)
from taskiq_flow.storage.memory import InMemoryStorageAdapter
from taskiq_flow.storage.redis import RedisStorageAdapter
from taskiq_flow.storage.sqlite import SQLiteStorageAdapter


class TestCreateStorageAdapter:
    """Tests for create_storage_adapter."""

    @patch("taskiq_flow.storage.factory._has_redis_available")
    def test_auto_with_no_redis(self, mock_has_redis: MagicMock) -> None:
        """Test auto-detection falls back to InMemory without Redis."""
        mock_has_redis.return_value = False
        adapter = StorageAdapterFactory.create_storage_adapter()
        assert isinstance(adapter, InMemoryStorageAdapter)

    @patch("taskiq_flow.storage.factory._has_redis_available")
    def test_auto_with_inmemory_broker(self, mock_has_redis: MagicMock) -> None:
        """Test auto-detection with InMemoryBroker."""
        mock_has_redis.return_value = False
        broker = InMemoryBroker()
        adapter = StorageAdapterFactory.create_storage_adapter(broker=broker)
        assert isinstance(adapter, InMemoryStorageAdapter)

    def test_memory_type(self) -> None:
        """Test explicit memory type."""
        config = TaskiqFlowConfig(storage_type="memory")
        adapter = StorageAdapterFactory.create_storage_adapter(config=config)
        assert isinstance(adapter, InMemoryStorageAdapter)

    def test_sqlite_type(self) -> None:
        """Test explicit sqlite type."""
        config = TaskiqFlowConfig(storage_type="sqlite")
        adapter = StorageAdapterFactory.create_storage_adapter(config=config)
        assert isinstance(adapter, SQLiteStorageAdapter)

    def test_sqlite_with_custom_url(self) -> None:
        """Test SQLite with custom URL."""
        config = TaskiqFlowConfig(
            storage_type="sqlite",
            storage_sqlite_url="sqlite+aiosqlite:///custom.db",
        )
        adapter = StorageAdapterFactory.create_storage_adapter(config=config)
        assert isinstance(adapter, SQLiteStorageAdapter)
        assert adapter.db_url == "sqlite+aiosqlite:///custom.db"

    def test_sqlalchemy_type(self) -> None:
        """Test explicit sqlalchemy type."""
        config = TaskiqFlowConfig(storage_type="sqlalchemy")
        adapter = StorageAdapterFactory.create_storage_adapter(config=config)
        assert isinstance(adapter, SQLiteStorageAdapter)

    def test_sqlalchemy_with_custom_url(self) -> None:
        """Test sqlalchemy with custom URL."""
        config = TaskiqFlowConfig(
            storage_type="sqlalchemy",
            storage_sqlalchemy_url="sqlite+aiosqlite:///alchemy.db",
        )
        adapter = StorageAdapterFactory.create_storage_adapter(config=config)
        assert isinstance(adapter, SQLiteStorageAdapter)
        assert adapter.db_url == "sqlite+aiosqlite:///alchemy.db"

    def test_redis_with_url(self) -> None:
        """Test Redis with explicit URL."""
        adapter = StorageAdapterFactory.create_storage_adapter(
            redis_url="redis://localhost:6379"
        )
        # This will work if redis is installed (even if Redis is not running)
        # The adapter creation shouldn't fail for URL-based creation
        assert isinstance(adapter, RedisStorageAdapter)

    def test_redis_type(self) -> None:
        """Test explicit redis type."""
        config = TaskiqFlowConfig(storage_type="redis")
        adapter = StorageAdapterFactory.create_storage_adapter(config=config)
        assert isinstance(adapter, RedisStorageAdapter)

    def test_custom_ttl(self) -> None:
        """Test custom TTL setting."""
        config = TaskiqFlowConfig(
            storage_type="memory",
            storage_ttl_seconds=7200,
        )
        adapter = StorageAdapterFactory.create_storage_adapter(config=config)
        # Memory adapter doesn't use TTL at construction, but verify config works
        assert isinstance(adapter, InMemoryStorageAdapter)

    @patch("taskiq_flow.storage.factory._has_redis_available")
    def test_none_config_defaults_to_auto(self, mock_has_redis: MagicMock) -> None:
        """Test None config falls back to auto."""
        mock_has_redis.return_value = False
        adapter = StorageAdapterFactory.create_storage_adapter(config=None)
        assert isinstance(adapter, InMemoryStorageAdapter)


class TestCreateCacheAdapter:
    """Tests for create_cache_adapter."""

    @patch("taskiq_flow.storage.factory._has_redis_available")
    def test_auto_fallback(self, mock_has_redis: MagicMock) -> None:
        """Test auto falls back to InMemoryCache."""
        mock_has_redis.return_value = False
        adapter = StorageAdapterFactory.create_cache_adapter()
        assert isinstance(adapter, InMemoryCacheAdapter)

    def test_memory_type(self) -> None:
        """Test explicit memory cache type."""
        config = TaskiqFlowConfig(cache_type="memory")
        adapter = StorageAdapterFactory.create_cache_adapter(config=config)
        assert isinstance(adapter, InMemoryCacheAdapter)

    @patch("taskiq_flow.storage.factory._has_redis_available")
    def test_redis_type(self, mock_has_redis: MagicMock) -> None:
        """Test explicit Redis cache type."""
        mock_has_redis.return_value = True
        config = TaskiqFlowConfig(cache_type="redis")
        adapter = StorageAdapterFactory.create_cache_adapter(config=config)
        assert isinstance(adapter, RedisCacheAdapter)

    @patch("taskiq_flow.storage.factory._has_redis_available")
    def test_redis_with_url(self, mock_has_redis: MagicMock) -> None:
        """Test Redis cache with explicit URL."""
        mock_has_redis.return_value = True
        adapter = StorageAdapterFactory.create_cache_adapter(
            redis_url="redis://localhost:6379"
        )
        assert isinstance(adapter, RedisCacheAdapter)

    def test_custom_ttl(self) -> None:
        """Test custom default TTL."""
        config = TaskiqFlowConfig(
            cache_type="memory",
            cache_default_ttl=600,
        )
        adapter = StorageAdapterFactory.create_cache_adapter(config=config)
        # The adapter creation should succeed
        assert adapter is not None

    def test_custom_lock_timeout(self) -> None:
        """Test custom lock timeout."""
        config = TaskiqFlowConfig(
            cache_type="memory",
            cache_lock_timeout=5,
        )
        adapter = StorageAdapterFactory.create_cache_adapter(config=config)
        assert adapter is not None


class TestCreateDefaultMiddlewares:
    """Tests for create_default_middlewares."""

    def test_create_with_inmemory_broker(self) -> None:
        """Test creating default middlewares with InMemoryBroker."""
        broker = InMemoryBroker()
        middlewares = StorageAdapterFactory.create_default_middlewares(broker=broker)

        assert "storage" in middlewares
        assert "cache" in middlewares

        assert isinstance(middlewares["storage"], StorageMiddleware)
        assert isinstance(middlewares["cache"], CacheMiddleware)

    def test_create_with_config(self) -> None:
        """Test creating middlewares with explicit config."""
        config = TaskiqFlowConfig(
            storage_type="memory",
            cache_type="memory",
        )
        middlewares = StorageAdapterFactory.create_default_middlewares(config=config)

        assert "storage" in middlewares
        assert "cache" in middlewares

    def test_storage_middleware_in_dict(self) -> None:
        """Test that storage key contains StorageMiddleware."""
        broker = InMemoryBroker()
        middlewares = StorageAdapterFactory.create_default_middlewares(broker=broker)

        assert isinstance(middlewares["storage"], StorageMiddleware)

    def test_cache_middleware_in_dict(self) -> None:
        """Test that cache key contains CacheMiddleware."""
        broker = InMemoryBroker()
        middlewares = StorageAdapterFactory.create_default_middlewares(broker=broker)

        assert isinstance(middlewares["cache"], CacheMiddleware)


class TestRedisExtraction:
    """Tests for Redis URL extraction from broker."""

    def test_extract_url_from_mock_redis_broker(self) -> None:
        """Test extracting Redis URL from a mock RedisBroker."""
        mock_broker = MagicMock()
        mock_broker.url = "redis://test-host:6379"

        url = _extract_redis_url_from_broker(mock_broker)
        # The function returns the url if it starts with redis://
        assert url == "redis://test-host:6379"

    def test_extract_url_none_for_inmemory(self) -> None:
        """Test that InMemoryBroker returns None."""
        broker = InMemoryBroker()

        url = _extract_redis_url_from_broker(broker)
        assert url is None

    def test_has_redis_available(self) -> None:
        """Test _has_redis_available function."""
        # This test should pass as long as we can import the function
        result = _has_redis_available()
        assert isinstance(result, bool)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
