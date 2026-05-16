"""
Tests for InMemoryStorageAdapter.

This module tests the in-memory storage adapter
functionality including CRUD operations, TTL, and cleanup.
"""

import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from taskiq_flow.storage.base import StorageEntry
from taskiq_flow.storage.memory import InMemoryStorageAdapter


@pytest.fixture
def storage() -> InMemoryStorageAdapter:
    """Create a fresh InMemoryStorageAdapter instance."""
    return InMemoryStorageAdapter()


class TestInMemoryStorageCreation:
    """Tests for InMemoryStorageAdapter initialization."""

    def test_create_empty_storage(self) -> None:
        """Test creating an empty storage adapter."""
        storage = InMemoryStorageAdapter()
        assert storage._pipelines == {}
        assert storage._lock is not None


class TestInMemoryStorageSet:
    """Tests for set operations."""

    @pytest.mark.asyncio
    async def test_set_and_get_simple_value(
        self, storage: InMemoryStorageAdapter
    ) -> None:
        """Test setting and getting a simple value."""
        await storage.set("key1", "value1")
        result = await storage.get("key1")
        assert result == "value1"

    @pytest.mark.asyncio
    async def test_set_and_get_complex_value(
        self, storage: InMemoryStorageAdapter
    ) -> None:
        """Test setting and getting a complex value."""
        value = {"name": "test", "count": 42, "items": [1, 2, 3]}
        await storage.set("complex_key", value)
        result = await storage.get("complex_key")
        assert result == value

    @pytest.mark.asyncio
    async def test_overwrite_existing_key(
        self, storage: InMemoryStorageAdapter
    ) -> None:
        """Test overwriting an existing key."""
        await storage.set("key1", "value1")
        await storage.set("key1", "value2")
        result = await storage.get("key1")
        assert result == "value2"

    @pytest.mark.asyncio
    async def test_set_with_ttl(self, storage: InMemoryStorageAdapter) -> None:
        """Test setting a value with TTL."""
        await storage.set("key_ttl", "value_ttl", ttl_seconds=1)
        result = await storage.get("key_ttl")
        assert result == "value_ttl"

    @pytest.mark.asyncio
    async def test_set_with_none_value(self, storage: InMemoryStorageAdapter) -> None:
        """Test setting a value of None."""
        await storage.set("key_none", None)
        result = await storage.get("key_none")
        assert result is None


class TestInMemoryStorageGet:
    """Tests for get operations."""

    @pytest.mark.asyncio
    async def test_get_nonexistent_key(self, storage: InMemoryStorageAdapter) -> None:
        """Test getting a key that doesn't exist."""
        result = await storage.get("nonexistent")
        assert result is None

    @pytest.mark.asyncio
    async def test_get_expired_key(self, storage: InMemoryStorageAdapter) -> None:
        """Test getting an expired key."""
        await storage.set("key_expired", "value", ttl_seconds=1)
        # Wait for expiration
        await asyncio.sleep(1.1)
        result = await storage.get("key_expired")
        assert result is None

    @pytest.mark.asyncio
    async def test_get_multiple_keys(self, storage: InMemoryStorageAdapter) -> None:
        """Test getting multiple keys."""
        await storage.set("key1", "value1")
        await storage.set("key2", "value2")
        await storage.set("key3", "value3")

        result1 = await storage.get("key1")
        result2 = await storage.get("key2")
        result3 = await storage.get("key3")

        assert result1 == "value1"
        assert result2 == "value2"
        assert result3 == "value3"


class TestInMemoryStorageDelete:
    """Tests for delete operations."""

    @pytest.mark.asyncio
    async def test_delete_existing_key(self, storage: InMemoryStorageAdapter) -> None:
        """Test deleting an existing key."""
        await storage.set("key1", "value1")
        result = await storage.delete("key1")
        assert result is True
        assert await storage.get("key1") is None

    @pytest.mark.asyncio
    async def test_delete_nonexistent_key(
        self, storage: InMemoryStorageAdapter
    ) -> None:
        """Test deleting a key that doesn't exist."""
        result = await storage.delete("nonexistent")
        assert result is False


class TestInMemoryStorageExists:
    """Tests for exists operations."""

    @pytest.mark.asyncio
    async def test_exists_true(self, storage: InMemoryStorageAdapter) -> None:
        """Test exists returns True for existing key."""
        await storage.set("key1", "value1")
        assert await storage.exists("key1") is True

    @pytest.mark.asyncio
    async def test_exists_false(self, storage: InMemoryStorageAdapter) -> None:
        """Test exists returns False for nonexistent key."""
        assert await storage.exists("nonexistent") is False

    @pytest.mark.asyncio
    async def test_exists_expired(self, storage: InMemoryStorageAdapter) -> None:
        """Test exists returns False for expired key."""
        await storage.set("key_expired", "value", ttl_seconds=1)
        await asyncio.sleep(1.1)
        assert await storage.exists("key_expired") is False


class TestInMemoryStorageKeys:
    """Tests for keys operations."""

    @pytest.mark.asyncio
    async def test_keys_all(self, storage: InMemoryStorageAdapter) -> None:
        """Test listing all keys."""
        await storage.set("key1", "value1")
        await storage.set("key2", "value2")
        await storage.set("key3", "value3")

        keys = await storage.keys()
        assert set(keys) == {"key1", "key2", "key3"}

    @pytest.mark.asyncio
    async def test_keys_with_pattern(self, storage: InMemoryStorageAdapter) -> None:
        """Test listing keys with a pattern."""
        await storage.set("user:1", "value1")
        await storage.set("user:2", "value2")
        await storage.set("task:1", "value3")

        keys = await storage.keys("user:*")
        assert set(keys) == {"user:1", "user:2"}

    @pytest.mark.asyncio
    async def test_keys_empty(self, storage: InMemoryStorageAdapter) -> None:
        """Test listing keys when empty."""
        keys = await storage.keys()
        assert keys == []

    @pytest.mark.asyncio
    async def test_keys_wildcard(self, storage: InMemoryStorageAdapter) -> None:
        """Test listing keys with wildcard."""
        await storage.set("prefix_key1", "value1")
        await storage.set("prefix_key2", "value2")
        await storage.set("other", "value3")

        keys = await storage.keys("prefix_*")
        assert len(keys) == 2
        assert "prefix_key1" in keys
        assert "prefix_key2" in keys


class TestInMemoryStorageCleanup:
    """Tests for cleanup operations."""

    @pytest.mark.asyncio
    async def test_cleanup_no_expired(self, storage: InMemoryStorageAdapter) -> None:
        """Test cleanup when no keys are expired."""
        await storage.set("key1", "value1", ttl_seconds=3600)
        count = await storage.cleanup(ttl_seconds=3600)
        assert count == 0

    @pytest.mark.asyncio
    async def test_cleanup_expired(self, storage: InMemoryStorageAdapter) -> None:
        """Test cleanup removes expired keys."""
        await storage.set("key1", "value1", ttl_seconds=1)
        await asyncio.sleep(1.1)
        count = await storage.cleanup(ttl_seconds=1)
        assert count == 1
        assert await storage.get("key1") is None

    @pytest.mark.asyncio
    async def test_cleanup_mixed(self, storage: InMemoryStorageAdapter) -> None:
        """Test cleanup with mixed expired and non-expired keys."""
        await storage.set("key1", "value1", ttl_seconds=1)
        await storage.set("key2", "value2", ttl_seconds=3600)
        await asyncio.sleep(1.1)
        count = await storage.cleanup(ttl_seconds=2)
        assert count == 1
        assert await storage.get("key1") is None
        assert await storage.get("key2") == "value2"

    @pytest.mark.asyncio
    async def test_cleanup_empty(self, storage: InMemoryStorageAdapter) -> None:
        """Test cleanup on empty storage."""
        count = await storage.cleanup()
        assert count == 0


class TestStorageEntry:
    """Tests for StorageEntry class."""

    def test_create_entry(self) -> None:
        """Test creating a storage entry."""
        now = datetime.now(timezone.utc)
        entry = StorageEntry(key="test", value="data", created_at=now)
        assert entry.key == "test"
        assert entry.value == "data"
        assert entry.created_at == now
        assert entry.expires_at is None
        assert entry.metadata == {}

    def test_create_entry_with_expiry(self) -> None:
        """Test creating a storage entry with expiry."""
        expires = datetime.now(timezone.utc) + timedelta(seconds=3600)
        entry = StorageEntry(
            key="test", value="data", expires_at=expires, metadata={"source": "test"}
        )
        assert entry.expires_at == expires
        assert entry.metadata == {"source": "test"}

    def test_entry_not_expired(self) -> None:
        """Test that entry with no expiry is not expired."""
        entry = StorageEntry(key="test", value="data")
        assert entry.is_expired() is False

    def test_entry_expired(self) -> None:
        """Test that expired entry is detected."""
        past = datetime.now(timezone.utc) - timedelta(seconds=1)
        entry = StorageEntry(key="test", value="data", expires_at=past)
        assert entry.is_expired() is True

    def test_remaining_ttl(self) -> None:
        """Test remaining TTL calculation."""
        future = datetime.now(timezone.utc) + timedelta(seconds=100)
        entry = StorageEntry(key="test", value="data", expires_at=future)
        remaining = entry.remaining_ttl()
        assert remaining is not None
        assert remaining > 0
        assert remaining <= 100

    def test_remaining_ttl_none(self) -> None:
        """Test remaining TTL when no expiry."""
        entry = StorageEntry(key="test", value="data")
        assert entry.remaining_ttl() is None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
