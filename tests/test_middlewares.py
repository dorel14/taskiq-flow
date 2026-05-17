"""
Tests for StorageMiddleware and CacheMiddleware.

This module tests the middleware components that integrate
storage and caching into the TaskIQ pipeline.
"""

from unittest.mock import MagicMock

import pytest
from taskiq import TaskiqMessage, TaskiqResult

from taskiq_flow.cache.memory import InMemoryCacheAdapter
from taskiq_flow.middlewares.cache import CacheMiddleware
from taskiq_flow.middlewares.storage import StorageMiddleware
from taskiq_flow.storage.base import BaseStorageAdapter
from taskiq_flow.storage.memory import InMemoryStorageAdapter

# ============================================================================
# Tests for StorageMiddleware
# ============================================================================


@pytest.fixture
def storage() -> MagicMock:
    """Create a mock storage adapter."""
    return MagicMock(spec=BaseStorageAdapter)


@pytest.fixture
def storage_middleware(storage: MagicMock) -> StorageMiddleware:
    """Create a StorageMiddleware with mock storage."""
    return StorageMiddleware(storage=storage)


@pytest.fixture
def disabled_middleware() -> StorageMiddleware:
    """Create a disabled StorageMiddleware."""
    return StorageMiddleware(enabled=False)


class TestStorageMiddlewareInit:
    """Tests for StorageMiddleware initialization."""

    def test_create_with_storage(self, storage: MagicMock) -> None:
        """Test creating middleware with storage backend."""
        middleware = StorageMiddleware(storage=storage)
        assert middleware.storage == storage
        assert middleware.enabled is True

    def test_create_without_storage(self) -> None:
        """Test creating middleware without storage backend."""
        middleware = StorageMiddleware(storage=None)
        assert middleware.storage is None

    def test_create_disabled(self) -> None:
        """Test creating a disabled middleware."""
        middleware = StorageMiddleware(enabled=False)
        assert middleware.enabled is False


class TestStorageMiddlewarePostSave:
    """Tests for post_save hook."""

    @pytest.mark.asyncio
    async def test_post_save_with_storage(
        self, storage_middleware: StorageMiddleware, storage: MagicMock
    ) -> None:
        """Test post_save persists result to storage."""
        message = MagicMock(spec=TaskiqMessage)
        message.task_id = "test_task_id"
        message.labels = {"pipeline_id": "test_pipeline"}

        result = MagicMock(spec=TaskiqResult)
        result.is_err = False
        result.return_value = "test_result"
        result.error = None
        result.execution_time = 0.5

        await storage_middleware.post_save(message, result)

        storage.set.assert_called_once()
        call_args = storage.set.call_args
        assert "test_task_id" in call_args[1]["key"]
        assert call_args[1]["value"]["return_value"] == "test_result"

    @pytest.mark.asyncio
    async def test_post_save_without_pipeline_id(
        self, storage_middleware: StorageMiddleware, storage: MagicMock
    ) -> None:
        """Test post_save without pipeline_id in labels."""
        message = MagicMock(spec=TaskiqMessage)
        message.task_id = "test_task_id"
        message.labels = {}

        result = MagicMock(spec=TaskiqResult)
        result.is_err = False
        result.return_value = "test_result"
        result.error = None
        result.execution_time = 0.5

        await storage_middleware.post_save(message, result)

        storage.set.assert_called_once()
        call_args = storage.set.call_args
        assert "pipeline" not in call_args[1]["key"]

    @pytest.mark.asyncio
    async def test_post_save_disabled(
        self, disabled_middleware: StorageMiddleware, storage: MagicMock
    ) -> None:
        """Test post_save does nothing when disabled."""
        message = MagicMock(spec=TaskiqMessage)
        message.task_id = "test_task_id"
        message.labels = {}

        result = MagicMock(spec=TaskiqResult)
        result.is_err = False
        result.return_value = "value"

        await disabled_middleware.post_save(message, result)

        storage.set.assert_not_called()

    @pytest.mark.asyncio
    async def test_post_save_no_storage(self) -> None:
        """Test post_save does nothing without storage."""
        middleware = StorageMiddleware(storage=None)

        message = MagicMock(spec=TaskiqMessage)
        message.task_id = "test_task_id"

        result = MagicMock(spec=TaskiqResult)
        result.is_err = False
        result.return_value = "value"

        # Should not raise
        await middleware.post_save(message, result)

    @pytest.mark.asyncio
    async def test_post_save_error_persistence(
        self, storage_middleware: StorageMiddleware, storage: MagicMock
    ) -> None:
        """Test post_save with error result."""
        message = MagicMock(spec=TaskiqMessage)
        message.task_id = "test_task_id"
        message.labels = {}

        result = MagicMock(spec=TaskiqResult)
        result.is_err = True
        result.return_value = None
        result.error = ValueError("test error")
        result.execution_time = 0.1

        await storage_middleware.post_save(message, result)

        storage.set.assert_called_once()
        call_args = storage.set.call_args
        assert call_args[1]["value"]["is_err"] is True

    @pytest.mark.asyncio
    async def test_post_save_with_ttl(
        self, storage_middleware: StorageMiddleware, storage: MagicMock
    ) -> None:
        """Test post_save with custom TTL from message labels."""
        message = MagicMock(spec=TaskiqMessage)
        message.task_id = "test_task_id"
        message.labels = {"cache_ttl": "7200"}

        result = MagicMock(spec=TaskiqResult)
        result.is_err = False
        result.return_value = "value"
        result.error = None
        result.execution_time = 0.1

        await storage_middleware.post_save(message, result)

        storage.set.assert_called_once()
        call_args = storage.set.call_args
        assert call_args[1]["ttl_seconds"] == 7200


class TestStorageMiddlewarePreExecute:
    """Tests for pre_execute hook."""

    @pytest.mark.asyncio
    async def test_pre_execute_returns_none(
        self, storage_middleware: StorageMiddleware
    ) -> None:
        """Test pre_execute always returns None."""
        message = MagicMock(spec=TaskiqMessage)
        storage_middleware.pre_execute(message)
        # pre_execute always returns None


# ============================================================================
# Tests for CacheMiddleware
# ============================================================================


@pytest.fixture
def cache() -> MagicMock:
    """Create a mock cache adapter."""
    return MagicMock(spec=InMemoryCacheAdapter)


@pytest.fixture
def cache_middleware(cache: MagicMock) -> CacheMiddleware:
    """Create a CacheMiddleware with mock cache."""
    return CacheMiddleware(cache=cache)


@pytest.fixture
def disabled_cache_middleware() -> CacheMiddleware:
    """Create a disabled CacheMiddleware."""
    return CacheMiddleware(enabled=False)


class TestCacheMiddlewareInit:
    """Tests for CacheMiddleware initialization."""

    def test_create_with_cache(self, cache: MagicMock) -> None:
        """Test creating middleware with cache backend."""
        middleware = CacheMiddleware(cache=cache)
        assert middleware.cache == cache
        assert middleware.enabled is True

    def test_create_without_cache(self) -> None:
        """Test creating middleware without cache backend."""
        middleware = CacheMiddleware(cache=None)
        assert middleware.cache is None

    def test_create_with_default_ttl(self) -> None:
        """Test creating middleware with default TTL."""
        middleware = CacheMiddleware(default_ttl=600)
        assert middleware.default_ttl == 600


class TestCacheMiddlewarePreExecute:
    """Tests for pre_execute hook (cache-first pattern)."""

    @pytest.mark.asyncio
    async def test_pre_execute_with_cached_value(
        self, cache_middleware: CacheMiddleware, cache: MagicMock
    ) -> None:
        """Test pre_execute restores cached result."""
        message = MagicMock(spec=TaskiqMessage)
        message.task_id = "task123"
        message.labels = {}

        cache.get.return_value = {
            "is_err": False,
            "return_value": "cached_result",
            "error": None,
            "execution_time": 0.1,
        }

        await cache_middleware.pre_execute(message)

        cache.get.assert_called_once_with("task:task123")
        assert message.labels["__cached"] == "true"
        assert message.labels["__cached_is_err"] == "False"
        assert message.labels["__cached_result"] == "cached_result"

    @pytest.mark.asyncio
    async def test_pre_execute_with_miss(
        self, cache_middleware: CacheMiddleware, cache: MagicMock
    ) -> None:
        """Test pre_execute with cache miss."""
        message = MagicMock(spec=TaskiqMessage)
        message.task_id = "task123"
        message.labels = {}

        cache.get.return_value = None

        await cache_middleware.pre_execute(message)

        cache.get.assert_called_once_with("task:task123")
        assert "__cached" not in message.labels

    @pytest.mark.asyncio
    async def test_pre_execute_disabled(
        self, disabled_cache_middleware: CacheMiddleware, cache: MagicMock
    ) -> None:
        """Test pre_execute does nothing when disabled."""
        message = MagicMock(spec=TaskiqMessage)
        message.task_id = "task123"

        await disabled_cache_middleware.pre_execute(message)

        cache.get.assert_not_called()
        assert message.task_id == "task123"

    @pytest.mark.asyncio
    async def test_pre_execute_no_cache(self) -> None:
        """Test pre_execute with no cache backend."""
        middleware = CacheMiddleware(cache=None)

        message = MagicMock(spec=TaskiqMessage)
        message.task_id = "task123"

        result = await middleware.pre_execute(message)

        assert result is message

    @pytest.mark.asyncio
    async def test_pre_execute_error_handling(
        self, cache_middleware: CacheMiddleware, cache: MagicMock
    ) -> None:
        """Test pre_execute handles errors gracefully."""
        message = MagicMock(spec=TaskiqMessage)
        message.task_id = "task123"
        message.labels = {}

        cache.get.side_effect = Exception("Redis error")

        # Should not raise, just log the error
        result = await cache_middleware.pre_execute(message)

        assert result is message


class TestCacheMiddlewarePostSave:
    """Tests for post_save hook (cache write-through)."""

    @pytest.mark.asyncio
    async def test_post_save_success(
        self, cache_middleware: CacheMiddleware, cache: MagicMock
    ) -> None:
        """Test post_save caches successful results."""
        message = MagicMock(spec=TaskiqMessage)
        message.task_id = "task123"
        message.labels = {}

        result = MagicMock(spec=TaskiqResult)
        result.is_err = False
        result.return_value = "success_value"
        result.error = None
        result.execution_time = 0.5

        await cache_middleware.post_save(message, result)

        cache.set.assert_called_once()
        call_args = cache.set.call_args
        assert call_args[1]["key"] == "task:task123"
        cached_value = call_args[1]["value"]
        assert cached_value["is_err"] is False
        assert cached_value["return_value"] == "success_value"
        assert cached_value["execution_time"] == 0.5

    @pytest.mark.asyncio
    async def test_post_save_error_not_cached(
        self, cache_middleware: CacheMiddleware, cache: MagicMock
    ) -> None:
        """Test post_save does not cache errors by default."""
        message = MagicMock(spec=TaskiqMessage)
        message.task_id = "task123"
        message.labels = {}

        result = MagicMock(spec=TaskiqResult)
        result.is_err = True
        result.return_value = None
        result.error = ValueError("error")
        result.execution_time = 0.1

        await cache_middleware.post_save(message, result)

        cache.set.assert_not_called()

    @pytest.mark.asyncio
    async def test_post_save_error_with_flag(
        self, cache_middleware: CacheMiddleware, cache: MagicMock
    ) -> None:
        """Test post_save caches errors when flag is set."""
        message = MagicMock(spec=TaskiqMessage)
        message.task_id = "task123"
        message.labels = {"cache_errors": "true"}

        result = MagicMock(spec=TaskiqResult)
        result.is_err = True
        result.return_value = None
        result.error = ValueError("error")
        result.execution_time = 0.1

        await cache_middleware.post_save(message, result)

        cache.set.assert_called_once()
        call_args = cache.set.call_args
        assert call_args[1]["value"]["is_err"] is True
        assert "error" in call_args[1]["value"]

    @pytest.mark.asyncio
    async def test_post_save_with_ttl(
        self, cache_middleware: CacheMiddleware, cache: MagicMock
    ) -> None:
        """Test post_save with custom TTL."""
        message = MagicMock(spec=TaskiqMessage)
        message.task_id = "task123"
        message.labels = {}

        result = MagicMock(spec=TaskiqResult)
        result.is_err = False
        result.return_value = "value"
        result.error = None
        result.execution_time = 0.1

        await cache_middleware.post_save(message, result)

        cache.set.assert_called_once()
        call_args = cache.set.call_args
        assert call_args[1]["ttl_seconds"] == 3600  # default

    @pytest.mark.asyncio
    async def test_post_save_disabled(
        self, disabled_cache_middleware: CacheMiddleware, cache: MagicMock
    ) -> None:
        """Test post_save does nothing when disabled."""
        message = MagicMock(spec=TaskiqMessage)
        message.task_id = "task123"

        result = MagicMock(spec=TaskiqResult)
        result.is_err = False
        result.return_value = "value"

        await disabled_cache_middleware.post_save(message, result)

        cache.set.assert_not_called()


class TestCacheMiddlewareHelpers:
    """Tests for helper methods."""

    @pytest.mark.asyncio
    async def test_get_ttl_from_message(
        self, cache_middleware: CacheMiddleware
    ) -> None:
        """Test TTL extraction from message labels."""
        message = MagicMock()

        # No TTL in labels
        message.labels = {}
        assert cache_middleware._get_ttl_from_message(message) is None

        # Valid TTL in labels
        message.labels = {"cache_ttl": "7200"}
        assert cache_middleware._get_ttl_from_message(message) == 7200

        # Invalid TTL
        message.labels = {"cache_ttl": "invalid"}
        assert cache_middleware._get_ttl_from_message(message) is None

    @pytest.mark.asyncio
    async def test_should_cache_errors(self, cache_middleware: CacheMiddleware) -> None:
        """Test error caching flag evaluation."""
        message = MagicMock()

        # Default: don't cache errors
        message.labels = {}
        assert cache_middleware._should_cache_errors(message) is False

        # Explicit: cache errors
        message.labels = {"cache_errors": "true"}
        assert cache_middleware._should_cache_errors(message) is True

        # Explicit: don't cache errors
        message.labels = {"cache_errors": "false"}
        assert cache_middleware._should_cache_errors(message) is False


class TestIntegrationWithRealAdapters:
    """Integration tests with real adapters."""

    @pytest.mark.asyncio
    async def test_storage_middleware_with_in_memory(self) -> None:
        """Test StorageMiddleware with InMemoryStorageAdapter."""
        storage = InMemoryStorageAdapter()
        middleware = StorageMiddleware(storage=storage)

        message = MagicMock(spec=TaskiqMessage)
        message.task_id = "integration_test"
        message.labels = {"pipeline_id": "test_pipe"}

        result = MagicMock(spec=TaskiqResult)
        result.is_err = False
        result.return_value = "test_value"
        result.error = None
        result.execution_time = 0.1

        await middleware.post_save(message, result)

        # Verify stored
        stored = await storage.get("pipeline:test_pipe:task:integration_test")
        assert stored is not None
        assert stored["return_value"] == "test_value"

    @pytest.mark.asyncio
    async def test_cache_middleware_with_in_memory(self) -> None:
        """Test CacheMiddleware with InMemoryCacheAdapter."""
        cache = InMemoryCacheAdapter()
        middleware = CacheMiddleware(cache=cache)

        message = MagicMock(spec=TaskiqMessage)
        message.task_id = "cache_test"
        message.labels = {}

        result = MagicMock(spec=TaskiqResult)
        result.is_err = False
        result.return_value = "cached_value"
        result.error = None
        result.execution_time = 0.1

        # Cache miss -> should pass through
        pre_result = await middleware.pre_execute(message)
        assert pre_result is message
        assert "__cached" not in message.labels

        # Save to cache
        await middleware.post_save(message, result)

        # Cache hit -> should hydrate message
        message2 = MagicMock(spec=TaskiqMessage)
        message2.task_id = "cache_test"
        message2.labels = {}

        await middleware.pre_execute(message2)
        assert message2.labels.get("__cached") == "true"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
