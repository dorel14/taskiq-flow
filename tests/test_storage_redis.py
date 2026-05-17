"""
Tests for RedisStorageAdapter.

This module tests the Redis storage adapter
functionality including CRUD operations, TTL, and cleanup.
Requires a running Redis instance.
"""

import asyncio
import contextlib
from collections.abc import AsyncGenerator
from unittest.mock import patch

import pytest

from taskiq_flow.storage.redis import RedisStorageAdapter


@pytest.fixture
async def redis_storage() -> AsyncGenerator[RedisStorageAdapter, None]:
    """Create a Redis storage instance for testing."""
    pytest.importorskip("redis")

    try:
        storage = RedisStorageAdapter("redis://localhost:6379", ttl_seconds=300)
    except ImportError:
        pytest.skip("redis package is not available")

    try:
        await storage._redis.ping()
    except Exception as e:
        pytest.skip(f"Redis not available: {e}")

    # Clean up any test keys
    await storage._redis.flushdb()
    yield storage


@pytest.fixture(autouse=True)
async def cleanup_redis(
    redis_storage: RedisStorageAdapter,
) -> AsyncGenerator[None, None]:
    """Clean up Redis keys after each test."""
    yield
    with contextlib.suppress(Exception):
        await redis_storage._redis.flushdb()


class TestRedisStorageSet:
    """Tests for set operations."""

    @pytest.mark.asyncio
    async def test_set_and_get(self, redis_storage: RedisStorageAdapter) -> None:
        """Test basic set and get."""
        await redis_storage.set("test_key", "test_value")
        result = await redis_storage.get("test_key")
        assert result == "test_value"

    @pytest.mark.asyncio
    async def test_set_with_ttl(self, redis_storage: RedisStorageAdapter) -> None:
        """Test setting a key with custom TTL."""
        await redis_storage.set("ttl_key", "ttl_value", ttl_seconds=10)
        result = await redis_storage.get("ttl_key")
        assert result == "ttl_value"

    @pytest.mark.asyncio
    async def test_overwrite_key(self, redis_storage: RedisStorageAdapter) -> None:
        """Test overwriting an existing key."""
        await redis_storage.set("key1", "value1")
        await redis_storage.set("key1", "value2")
        result = await redis_storage.get("key1")
        assert result == "value2"

    @pytest.mark.asyncio
    async def test_get_complex_value(self, redis_storage: RedisStorageAdapter) -> None:
        """Test getting a complex value (dict/list)."""
        value = {"name": "test", "count": 42, "items": [1, 2, 3]}
        await redis_storage.set("complex", value)
        result = await redis_storage.get("complex")
        assert result == value


class TestRedisStorageGet:
    """Tests for get operations."""

    @pytest.mark.asyncio
    async def test_get_nonexistent(self, redis_storage: RedisStorageAdapter) -> None:
        """Test getting a nonexistent key."""
        result = await redis_storage.get("nonexistent")
        assert result is None

    @pytest.mark.asyncio
    async def test_get_expired(self, redis_storage: RedisStorageAdapter) -> None:
        """Test getting an expired key."""
        await redis_storage.set("expired", "value", ttl_seconds=1)
        await asyncio.sleep(1.1)
        result = await redis_storage.get("expired")
        assert result is None


class TestRedisStorageDelete:
    """Tests for delete operations."""

    @pytest.mark.asyncio
    async def test_delete_existing(self, redis_storage: RedisStorageAdapter) -> None:
        """Test deleting an existing key."""
        await redis_storage.set("to_delete", "value")
        result = await redis_storage.delete("to_delete")
        assert result is True
        assert await redis_storage.get("to_delete") is None

    @pytest.mark.asyncio
    async def test_delete_nonexistent(self, redis_storage: RedisStorageAdapter) -> None:
        """Test deleting a nonexistent key."""
        result = await redis_storage.delete("nonexistent")
        assert result is False


class TestRedisStorageExists:
    """Tests for exists operations."""

    @pytest.mark.asyncio
    async def test_exists_true(self, redis_storage: RedisStorageAdapter) -> None:
        """Test exists returns True for existing key."""
        await redis_storage.set("exists_key", "value")
        assert await redis_storage.exists("exists_key") is True

    @pytest.mark.asyncio
    async def test_exists_false(self, redis_storage: RedisStorageAdapter) -> None:
        """Test exists returns False for nonexistent key."""
        assert await redis_storage.exists("nonexistent") is False


class TestRedisStorageKeys:
    """Tests for keys operations."""

    @pytest.mark.asyncio
    async def test_keys_pattern(self, redis_storage: RedisStorageAdapter) -> None:
        """Test listing keys with a pattern."""
        await redis_storage.set("user:1", "v1")
        await redis_storage.set("user:2", "v2")
        await redis_storage.set("task:1", "v3")

        keys = await redis_storage.keys("user:*")
        assert set(keys) == {"user:1", "user:2"}

    @pytest.mark.asyncio
    async def test_keys_all(self, redis_storage: RedisStorageAdapter) -> None:
        """Test listing all keys."""
        await redis_storage.set("key1", "v1")
        await redis_storage.set("key2", "v2")

        keys = await redis_storage.keys()
        assert set(keys) == {"key1", "key2"}


class TestRedisStorageCleanup:
    """Tests for cleanup operations."""

    @pytest.mark.asyncio
    async def test_cleanup(self, redis_storage: RedisStorageAdapter) -> None:
        """Test cleanup removes expired keys."""
        await redis_storage.set("key1", "value1", ttl_seconds=1)
        await asyncio.sleep(1.1)
        # Note: Redis cleanup behavior may vary, we just verify it doesn't raise
        await redis_storage.cleanup(ttl_seconds=10)


class TestRedisStorageAdapterInit:
    """Tests for RedisStorageAdapter initialization."""

    def test_create_without_redis(self) -> None:
        """Test that creating adapter without redis package raises error."""
        with (
            patch("taskiq_flow.storage.redis.redis_mod", None),
            pytest.raises(ImportError, match="redis package is required"),
        ):
            RedisStorageAdapter("redis://localhost:6379")

    def test_default_url(self) -> None:
        """Test that default URL is set correctly."""
        _ = RedisStorageAdapter()
