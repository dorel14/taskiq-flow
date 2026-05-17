"""
Tests for InMemoryCacheAdapter.

This module tests the in-memory cache adapter
with Dogpile-style locking and TTL support.
"""

import asyncio
import time

import pytest

from taskiq_flow.cache.memory import InMemoryCacheAdapter


@pytest.fixture
def cache() -> InMemoryCacheAdapter:
    """Create a fresh InMemoryCacheAdapter instance."""
    return InMemoryCacheAdapter()


class TestInMemoryCacheGetOrCreate:
    """Tests for get_or_create with Dogpile pattern."""

    @pytest.mark.asyncio
    async def test_get_or_create_new_value(self, cache: InMemoryCacheAdapter) -> None:
        """Test creating a new cached value."""
        result = await cache.get_or_create("key1", lambda: "value1")
        assert result == "value1"

    @pytest.mark.asyncio
    async def test_get_or_create_existing(self, cache: InMemoryCacheAdapter) -> None:
        """Test getting an existing cached value."""
        await cache.get_or_create("key1", lambda: "value1")
        result = await cache.get_or_create("key1", lambda: "value2")
        assert result == "value1"

    @pytest.mark.asyncio
    async def test_get_or_create_with_custom_ttl(
        self, cache: InMemoryCacheAdapter
    ) -> None:
        """Test get_or_create with custom TTL."""
        result = await cache.get_or_create("key_ttl", lambda: 42, ttl_seconds=100)
        assert result == 42

    @pytest.mark.asyncio
    async def test_get_or_create_async_creator(
        self, cache: InMemoryCacheAdapter
    ) -> None:
        """Test get_or_create with an async creator function."""

        async def async_creator() -> str:
            await asyncio.sleep(0.01)
            return "async_value"

        result = await cache.get_or_create("async_key", async_creator)
        assert result == "async_value"

    @pytest.mark.asyncio
    async def test_get_or_create_dogpile_lock(
        self, cache: InMemoryCacheAdapter
    ) -> None:
        """Test that Dogpile lock prevents duplicate creation."""
        creation_count = 0

        def creator() -> str:
            nonlocal creation_count
            creation_count += 1
            return f"created_{creation_count}"

        # Multiple concurrent calls should result in only one creation
        tasks = [cache.get_or_create("dogpile_key", creator) for _ in range(5)]
        results = await asyncio.gather(*tasks)

        assert all(r == "created_1" for r in results)
        assert creation_count == 1


class TestInMemoryCacheGet:
    """Tests for get operations."""

    @pytest.mark.asyncio
    async def test_get_existing(self, cache: InMemoryCacheAdapter) -> None:
        """Test getting an existing value."""
        await cache.set("key1", "value1")
        result = await cache.get("key1")
        assert result == "value1"

    @pytest.mark.asyncio
    async def test_get_nonexistent(self, cache: InMemoryCacheAdapter) -> None:
        """Test getting a nonexistent value."""
        result = await cache.get("nonexistent")
        assert result is None

    @pytest.mark.asyncio
    async def test_get_expired(self, cache: InMemoryCacheAdapter) -> None:
        """Test getting an expired value."""
        await cache.set("expired_key", "value", ttl_seconds=1)
        time.sleep(1.1)
        result = await cache.get("expired_key")
        assert result is None


class TestInMemoryCacheSet:
    """Tests for set operations."""

    @pytest.mark.asyncio
    async def test_set_and_retrieve(self, cache: InMemoryCacheAdapter) -> None:
        """Test basic set and retrieve."""
        await cache.set("key1", "value1")
        result = await cache.get("key1")
        assert result == "value1"

    @pytest.mark.asyncio
    async def test_set_with_ttl(self, cache: InMemoryCacheAdapter) -> None:
        """Test setting with TTL."""
        await cache.set("ttl_key", "ttl_value", ttl_seconds=100)
        result = await cache.get("ttl_key")
        assert result == "ttl_value"

    @pytest.mark.asyncio
    async def test_set_overwrite(self, cache: InMemoryCacheAdapter) -> None:
        """Test overwriting an existing key."""
        await cache.set("key1", "value1")
        await cache.set("key1", "value2")
        result = await cache.get("key1")
        assert result == "value2"

    @pytest.mark.asyncio
    async def test_set_zero_ttl(self, cache: InMemoryCacheAdapter) -> None:
        """Test setting with zero TTL (no expiration)."""
        await cache.set("zero_ttl", "value", ttl_seconds=0)
        result = await cache.get("zero_ttl")
        assert result == "value"


class TestInMemoryCacheInvalidate:
    """Tests for invalidate operations."""

    @pytest.mark.asyncio
    async def test_invalidate_existing(self, cache: InMemoryCacheAdapter) -> None:
        """Test invalidating an existing key."""
        await cache.set("key1", "value1")
        result = await cache.invalidate("key1")
        assert result is True
        assert await cache.get("key1") is None

    @pytest.mark.asyncio
    async def test_invalidate_nonexistent(self, cache: InMemoryCacheAdapter) -> None:
        """Test invalidating a nonexistent key."""
        result = await cache.invalidate("nonexistent")
        assert result is False


class TestInMemoryCacheClear:
    """Tests for clear operations."""

    @pytest.mark.asyncio
    async def test_clear(self, cache: InMemoryCacheAdapter) -> None:
        """Test clearing all cached values."""
        await cache.set("key1", "value1")
        await cache.set("key2", "value2")
        await cache.clear()

        assert await cache.get("key1") is None
        assert await cache.get("key2") is None

    @pytest.mark.asyncio
    async def test_clear_empty(self, cache: InMemoryCacheAdapter) -> None:
        """Test clearing an empty cache."""
        await cache.clear()
        # Should not raise


class TestInMemoryCacheStats:
    """Tests for statistics tracking."""

    @pytest.mark.asyncio
    async def test_initial_stats(self, cache: InMemoryCacheAdapter) -> None:
        """Test initial cache statistics."""
        stats = cache.get_stats()
        assert stats["hits"] == 0
        assert stats["misses"] == 0
        assert stats["hit_rate"] == 0.0
        assert stats["size"] == 0
        assert stats["keys"] == []

    @pytest.mark.asyncio
    async def test_stats_after_operations(self, cache: InMemoryCacheAdapter) -> None:
        """Test statistics after cache operations."""
        # Miss on get
        await cache.get("nonexistent")
        # Set and hit
        await cache.set("key1", "value1")
        await cache.get("key1")

        stats = cache.get_stats()
        assert stats["hits"] >= 1
        assert stats["misses"] >= 1
        assert stats["size"] == 1
        assert "key1" in stats["keys"]

    @pytest.mark.asyncio
    async def test_stats_hit_rate(self, cache: InMemoryCacheAdapter) -> None:
        """Test hit rate calculation."""
        await cache.set("key1", "value1")
        await cache.get("key1")
        await cache.get("key1")

        stats = cache.get_stats()
        assert stats["hit_rate"] == 1.0


class TestInMemoryCacheConcurrency:
    """Tests for concurrent access."""

    @pytest.mark.asyncio
    async def test_concurrent_set_get(self, cache: InMemoryCacheAdapter) -> None:
        """Test concurrent set and get operations."""

        async def set_values(start: int, count: int) -> None:
            for i in range(start, start + count):
                await cache.set(f"key_{i}", f"value_{i}")

        async def get_values(start: int, count: int) -> list[str | None]:
            results = []
            for i in range(start, start + count):
                val = await cache.get(f"key_{i}")
                results.append(val)
            return results

        await asyncio.gather(
            set_values(0, 10),
            set_values(10, 10),
        )
        results = await asyncio.gather(
            get_values(0, 10),
            get_values(10, 10),
        )

        all_results = results[0] + results[1]
        assert all(r is not None for r in all_results)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
