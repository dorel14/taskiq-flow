"""
Tests for RedisCacheAdapter.

This module tests the Redis cache adapter
with distributed locking and TTL support.
Requires a running Redis instance.
"""

import asyncio
import contextlib
from collections.abc import AsyncGenerator
from unittest.mock import patch

import pytest

from taskiq_flow.cache.redis import RedisCacheAdapter

redis = pytest.importorskip("redis")


@pytest.fixture
async def redis_cache() -> RedisCacheAdapter:
    """Create a Redis cache instance for testing."""
    pytest.importorskip("redis")

    try:
        cache = RedisCacheAdapter("redis://localhost:6379")
        # Test connection
        await cache._redis.ping()
    except Exception as e:
        pytest.skip(f"Redis not available: {e}")

    # Clean up before tests
    await cache.clear()
    return cache


@pytest.fixture(autouse=True)
async def cleanup_redis_cache(
    redis_cache: RedisCacheAdapter,
) -> AsyncGenerator[None, None]:
    """Clean up cache keys after each test."""
    yield
    with contextlib.suppress(Exception):
        await redis_cache.clear()


class TestRedisCacheGetOrCreate:
    """Tests for get_or_create with distributed locking."""

    @pytest.mark.asyncio
    async def test_get_or_create_new(self, redis_cache: RedisCacheAdapter) -> None:
        """Test creating a new cached value."""
        result = await redis_cache.get_or_create("key1", lambda: "value1")
        assert result == "value1"

    @pytest.mark.asyncio
    async def test_get_or_create_existing(self, redis_cache: RedisCacheAdapter) -> None:
        """Test getting an existing cached value."""
        await redis_cache.get_or_create("key1", lambda: "value1")
        result = await redis_cache.get_or_create("key1", lambda: "wrong_value")
        assert result == "value1"

    @pytest.mark.asyncio
    async def test_get_or_create_with_ttl(self, redis_cache: RedisCacheAdapter) -> None:
        """Test get_or_create with custom TTL."""
        result = await redis_cache.get_or_create("key_ttl", lambda: 42, ttl_seconds=100)
        assert result == 42

    @pytest.mark.asyncio
    async def test_get_or_create_async_creator(
        self, redis_cache: RedisCacheAdapter
    ) -> None:
        """Test get_or_create with async creator."""

        async def async_creator() -> str:
            await asyncio.sleep(0.01)
            return "async_value"

        result = await redis_cache.get_or_create("async_key", async_creator)
        assert result == "async_value"


class TestRedisCacheGet:
    """Tests for get operations."""

    @pytest.mark.asyncio
    async def test_get_existing(self, redis_cache: RedisCacheAdapter) -> None:
        """Test getting an existing value."""
        await redis_cache.set("key1", "value1")
        result = await redis_cache.get("key1")
        assert result == "value1"

    @pytest.mark.asyncio
    async def test_get_nonexistent(self, redis_cache: RedisCacheAdapter) -> None:
        """Test getting a nonexistent value."""
        result = await redis_cache.get("nonexistent")
        assert result is None


class TestRedisCacheSet:
    """Tests for set operations."""

    @pytest.mark.asyncio
    async def test_set_and_get(self, redis_cache: RedisCacheAdapter) -> None:
        """Test basic set and get."""
        await redis_cache.set("key1", "value1")
        result = await redis_cache.get("key1")
        assert result == "value1"

    @pytest.mark.asyncio
    async def test_set_with_ttl(self, redis_cache: RedisCacheAdapter) -> None:
        """Test setting with TTL."""
        await redis_cache.set("ttl_key", "ttl_value", ttl_seconds=100)
        result = await redis_cache.get("ttl_key")
        assert result == "ttl_value"

    @pytest.mark.asyncio
    async def test_overwrite_key(self, redis_cache: RedisCacheAdapter) -> None:
        """Test overwriting an existing key."""
        await redis_cache.set("key1", "value1")
        await redis_cache.set("key1", "value2")
        result = await redis_cache.get("key1")
        assert result == "value2"


class TestRedisCacheInvalidate:
    """Tests for invalidate operations."""

    @pytest.mark.asyncio
    async def test_invalidate_existing(self, redis_cache: RedisCacheAdapter) -> None:
        """Test invalidating an existing key."""
        await redis_cache.set("key1", "value1")
        result = await redis_cache.invalidate("key1")
        assert result is True
        assert await redis_cache.get("key1") is None

    @pytest.mark.asyncio
    async def test_invalidate_nonexistent(self, redis_cache: RedisCacheAdapter) -> None:
        """Test invalidating a nonexistent key."""
        result = await redis_cache.invalidate("nonexistent")
        assert result is False


class TestRedisCacheClear:
    """Tests for clear operations."""

    @pytest.mark.asyncio
    async def test_clear(self, redis_cache: RedisCacheAdapter) -> None:
        """Test clearing all cached values."""
        await redis_cache.set("key1", "value1")
        await redis_cache.set("key2", "value2")
        await redis_cache.clear()

        assert await redis_cache.get("key1") is None
        assert await redis_cache.get("key2") is None


class TestRedisCacheStats:
    """Tests for statistics tracking."""

    @pytest.mark.asyncio
    async def test_initial_stats(self, redis_cache: RedisCacheAdapter) -> None:
        """Test initial cache statistics."""
        stats = redis_cache.get_stats()
        assert "hits" in stats
        assert "misses" in stats
        assert "hit_rate" in stats
        assert stats["type"] == "redis"

    @pytest.mark.asyncio
    async def test_stats_after_hit(self, redis_cache: RedisCacheAdapter) -> None:
        """Test statistics after a cache hit."""
        await redis_cache.set("key1", "value1")
        await redis_cache.get("key1")

        stats = redis_cache.get_stats()
        assert stats["hits"] >= 1

    @pytest.mark.asyncio
    async def test_stats_after_miss(self, redis_cache: RedisCacheAdapter) -> None:
        """Test statistics after a cache miss."""
        await redis_cache.get("nonexistent")

        stats = redis_cache.get_stats()
        assert stats["misses"] >= 1


class TestRedisCacheInit:
    """Tests for initialization."""

    def test_create_without_redis(self) -> None:
        """Test that creating adapter without redis package raises error."""
        with (
            patch("taskiq_flow.cache.redis.redis_mod", None),
            pytest.raises(ImportError, match="redis package is required"),
        ):
            RedisCacheAdapter("redis://localhost:6379")

    def test_default_values(self) -> None:
        """Test that default values are set correctly."""
        # This will fail if redis is not available, so we just check init
        cache = RedisCacheAdapter("redis://localhost:6379")
        assert cache._default_ttl == 3600
        assert cache._lock_timeout == 10
        assert cache._key_prefix == "tqs:cache:"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
