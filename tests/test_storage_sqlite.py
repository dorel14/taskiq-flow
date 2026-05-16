"""
Tests for SQLiteStorageAdapter.

This module tests the SQLite storage adapter
functionality including CRUD operations, TTL, and cleanup.
"""

import asyncio
import contextlib
import os
import tempfile
from collections.abc import Generator
from pathlib import Path

import pytest

from taskiq_flow.storage.sqlite import SQLiteStorageAdapter


@pytest.fixture
def sqlite_storage() -> Generator[SQLiteStorageAdapter, None, None]:
    """Create a SQLite storage instance with a temporary database file."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp_file:
        tmp_path = Path(tmp_file.name)
    os.environ["TEST_DB_URL"] = f"sqlite+aiosqlite:///{tmp_path}"
    storage = SQLiteStorageAdapter(
        db_url=f"sqlite+aiosqlite:///{tmp_path}",
        async_mode=True,
    )
    yield storage
    # Cleanup
    with contextlib.suppress(OSError):
        tmp_path.unlink()


@pytest.fixture
def sqlite_storage_sync() -> Generator[SQLiteStorageAdapter, None, None]:
    """Create a synchronous SQLite storage instance."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp_file:
        tmp_path = Path(tmp_file.name)
    storage = SQLiteStorageAdapter(
        db_url=f"sqlite:///{tmp_path}",
        async_mode=False,
    )
    yield storage
    with contextlib.suppress(OSError):
        tmp_path.unlink()


class TestSQLiteStorageInit:
    """Tests for SQLiteStorageAdapter initialization."""

    def test_create_default(self) -> None:
        """Test creating adapter with default settings."""
        storage = SQLiteStorageAdapter(async_mode=False)
        assert storage.db_url == "sqlite+aiosqlite:///taskiq_flow.db"
        assert storage.async_mode is False

    def test_create_async(self) -> None:
        """Test creating adapter in async mode."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp_file:
            tmp_path = Path(tmp_file.name)
        try:
            storage = SQLiteStorageAdapter(
                db_url=f"sqlite+aiosqlite:///{tmp_path}",
                async_mode=True,
            )
            assert storage.async_mode is True
        finally:
            with contextlib.suppress(OSError):
                tmp_path.unlink()

    def test_create_sync(self) -> None:
        """Test creating adapter in sync mode."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp_file:
            tmp_path = Path(tmp_file.name)
        try:
            storage = SQLiteStorageAdapter(
                db_url=f"sqlite:///{tmp_path}",
                async_mode=False,
            )
            assert storage.async_mode is False
        finally:
            with contextlib.suppress(OSError):
                tmp_path.unlink()


class TestSQLiteStorageSet:
    """Tests for set operations."""

    @pytest.mark.asyncio
    async def test_set_and_get_async(
        self, sqlite_storage: SQLiteStorageAdapter
    ) -> None:
        """Test basic set and get in async mode."""
        await sqlite_storage.set("key1", "value1")
        result = await sqlite_storage.get("key1")
        assert result == "value1"

    @pytest.mark.asyncio
    async def test_set_with_ttl(self, sqlite_storage: SQLiteStorageAdapter) -> None:
        """Test setting a value with TTL."""
        await sqlite_storage.set("key_ttl", "value_ttl", ttl_seconds=100)
        result = await sqlite_storage.get("key_ttl")
        assert result == "value_ttl"

    @pytest.mark.asyncio
    async def test_overwrite_key(self, sqlite_storage: SQLiteStorageAdapter) -> None:
        """Test overwriting an existing key."""
        await sqlite_storage.set("key1", "value1")
        await sqlite_storage.set("key1", "value2")
        result = await sqlite_storage.get("key1")
        assert result == "value2"

    @pytest.mark.asyncio
    async def test_set_complex_value(
        self, sqlite_storage: SQLiteStorageAdapter
    ) -> None:
        """Test setting a complex value."""
        value = {"name": "test", "count": 42, "items": [1, 2, 3]}
        await sqlite_storage.set("complex", value)
        result = await sqlite_storage.get("complex")
        assert result == value


class TestSQLiteStorageGet:
    """Tests for get operations."""

    @pytest.mark.asyncio
    async def test_get_nonexistent(self, sqlite_storage: SQLiteStorageAdapter) -> None:
        """Test getting a nonexistent key."""
        result = await sqlite_storage.get("nonexistent")
        assert result is None

    @pytest.mark.asyncio
    async def test_get_expired(self, sqlite_storage: SQLiteStorageAdapter) -> None:
        """Test getting an expired key."""
        await sqlite_storage.set("expired", "value", ttl_seconds=1)
        await asyncio.sleep(1.1)
        result = await sqlite_storage.get("expired")
        assert result is None


class TestSQLiteStorageDelete:
    """Tests for delete operations."""

    @pytest.mark.asyncio
    async def test_delete_existing(self, sqlite_storage: SQLiteStorageAdapter) -> None:
        """Test deleting an existing key."""
        await sqlite_storage.set("to_delete", "value")
        result = await sqlite_storage.delete("to_delete")
        assert result is True
        assert await sqlite_storage.get("to_delete") is None

    @pytest.mark.asyncio
    async def test_delete_nonexistent(
        self, sqlite_storage: SQLiteStorageAdapter
    ) -> None:
        """Test deleting a nonexistent key."""
        result = await sqlite_storage.delete("nonexistent")
        assert result is False


class TestSQLiteStorageExists:
    """Tests for exists operations."""

    @pytest.mark.asyncio
    async def test_exists_true(self, sqlite_storage: SQLiteStorageAdapter) -> None:
        """Test exists returns True for existing key."""
        await sqlite_storage.set("exists_key", "value")
        assert await sqlite_storage.exists("exists_key") is True

    @pytest.mark.asyncio
    async def test_exists_false(self, sqlite_storage: SQLiteStorageAdapter) -> None:
        """Test exists returns False for nonexistent key."""
        assert await sqlite_storage.exists("nonexistent") is False

    @pytest.mark.asyncio
    async def test_exists_expired(self, sqlite_storage: SQLiteStorageAdapter) -> None:
        """Test exists returns False for expired key."""
        await sqlite_storage.set("expired", "value", ttl_seconds=1)
        await asyncio.sleep(1.1)
        assert await sqlite_storage.exists("expired") is False


class TestSQLiteStorageKeys:
    """Tests for keys operations."""

    @pytest.mark.asyncio
    async def test_keys_all(self, sqlite_storage: SQLiteStorageAdapter) -> None:
        """Test listing all keys."""
        await sqlite_storage.set("key1", "v1")
        await sqlite_storage.set("key2", "v2")

        keys = await sqlite_storage.keys()
        assert set(keys) == {"key1", "key2"}

    @pytest.mark.asyncio
    async def test_keys_with_pattern(
        self, sqlite_storage: SQLiteStorageAdapter
    ) -> None:
        """Test listing keys with a pattern."""
        await sqlite_storage.set("user:1", "v1")
        await sqlite_storage.set("user:2", "v2")
        await sqlite_storage.set("task:1", "v3")

        keys = await sqlite_storage.keys("user:*")
        assert set(keys) == {"user:1", "user:2"}

    @pytest.mark.asyncio
    async def test_keys_empty(self, sqlite_storage: SQLiteStorageAdapter) -> None:
        """Test listing keys when storage is empty."""
        keys = await sqlite_storage.keys()
        assert keys == []


class TestSQLiteStorageCleanup:
    """Tests for cleanup operations."""

    @pytest.mark.asyncio
    async def test_cleanup_no_expired(
        self, sqlite_storage: SQLiteStorageAdapter
    ) -> None:
        """Test cleanup when no keys are expired."""
        await sqlite_storage.set("key1", "value1", ttl_seconds=3600)
        count = await sqlite_storage.cleanup(ttl_seconds=3600)
        assert count == 0

    @pytest.mark.asyncio
    async def test_cleanup_expired(self, sqlite_storage: SQLiteStorageAdapter) -> None:
        """Test cleanup removes expired keys."""
        await sqlite_storage.set("key1", "value1", ttl_seconds=1)
        await asyncio.sleep(1.1)
        count = await sqlite_storage.cleanup(ttl_seconds=2)
        assert count == 1

    @pytest.mark.asyncio
    async def test_cleanup_empty(self, sqlite_storage: SQLiteStorageAdapter) -> None:
        """Test cleanup on empty storage."""
        count = await sqlite_storage.cleanup()
        assert count == 0


class TestSQLiteStorageSyncMode:
    """Tests for synchronous SQLite storage mode."""

    @pytest.mark.asyncio
    async def test_set_and_get_sync(
        self, sqlite_storage_sync: SQLiteStorageAdapter
    ) -> None:
        """Test basic set and get in sync mode."""
        await sqlite_storage_sync.set("key1", "value1")
        result = await sqlite_storage_sync.get("key1")
        assert result == "value1"

    @pytest.mark.asyncio
    async def test_delete_sync(self, sqlite_storage_sync: SQLiteStorageAdapter) -> None:
        """Test delete in sync mode."""
        await sqlite_storage_sync.set("key1", "value1")
        result = await sqlite_storage_sync.delete("key1")
        assert result is True
        assert await sqlite_storage_sync.get("key1") is None

    @pytest.mark.asyncio
    async def test_exists_sync(self, sqlite_storage_sync: SQLiteStorageAdapter) -> None:
        """Test exists in sync mode."""
        await sqlite_storage_sync.set("key1", "value1")
        assert await sqlite_storage_sync.exists("key1") is True
        assert await sqlite_storage_sync.exists("nonexistent") is False


class TestSQLiteStorageInitTables:
    """Tests for table initialization."""

    def test_tables_created_on_init(self) -> None:
        """Test that tables are created during initialization."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp_file:
            tmp_path = Path(tmp_file.name)
        try:
            storage = SQLiteStorageAdapter(
                db_url=f"sqlite:///{tmp_path}",
                async_mode=False,
            )
            # If we got here without error, tables were created
            assert storage._sync_engine is not None
        finally:
            with contextlib.suppress(OSError):
                tmp_path.unlink()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
