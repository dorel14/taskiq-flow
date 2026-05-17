---
title: API Reference: Storage
nav_order: 31
color_scheme: dark
---
# API Reference: Storage

**Pluggable persistence layer — adapters, factory, and `StorageMiddleware`**

> **Version**: {VERSION} | **New in v1.2.0** | **Module**: `taskiq_flow.storage`, `taskiq_flow.middlewares.storage`

---

## Overview

Taskiq-Flow v1.2.0 introduces a **centralized storage layer** that decouples all persistence concerns (tracking, scheduling, results history) from the broker implementation. The storage system provides:

- **One unified interface** — `BaseStorageAdapter` works with every backend
- **Three built-in adapters** — InMemory, Redis, SQLite/SQLAlchemy
- **Auto-detection factory** — `StorageAdapterFactory` picks the right backend automatically
- **Middleware integration** — `StorageMiddleware` plugs into the TaskIQ middleware pipeline

Use `StorageMiddleware` instead of ad-hoc persistence code: it intercepts task events and stores results via a pluggable adapter.

---

## Module: `taskiq_flow.storage`

### `StorageEntry`

```python
from taskiq_flow.storage import StorageEntry
from datetime import datetime, timezone

entry = StorageEntry(
    key="pipeline:my_run:task:abc123",
    value={"status": "completed", "result": 42},
    expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
    metadata={"pipeline_id": "my_run"},
)
```

A typed container for a single stored value with optional TTL and metadata.

| Attribute | Type | Description |
|-----------|------|-------------|
| `key` | `str` | Unique storage key |
| `value` | `Any` | Stored value (JSON-serializable recommended) |
| `created_at` | `datetime` | Timestamp of creation (UTC) |
| `expires_at` | `datetime \| None` | Expiration timestamp; `None` = never expires |
| `metadata` | `dict` | Arbitrary metadata tags |

| Method | Signature | Description |
|--------|-----------|-------------|
| `is_expired()` | `() -> bool` | Returns `True` if the entry has expired |
| `remaining_ttl()` | `() -> float \| None` | Seconds until expiry; `None` if never expires |

---

### `BaseStorageAdapter` (ABC)

```python
from taskiq_flow.storage import BaseStorageAdapter

class MyAdapter(BaseStorageAdapter):
    async def get(self, key: str) -> Any | None: ...
    async def set(self, key: str, value: Any, ttl_seconds: int | None = None) -> None: ...
    async def delete(self, key: str) -> bool: ...
    async def exists(self, key: str) -> bool: ...
    async def keys(self, pattern: str = "*") -> list[str]: ...
    async def cleanup(self, ttl_seconds: int = 3600) -> int: ...
```

Abstract interface that all storage backends must implement. Use this to implement a custom backend (e.g., PostgreSQL, DynamoDB).

| Method | Description |
|--------|-------------|
| `get(key)` | Retrieve value by key; returns `None` if missing or expired |
| `set(key, value, ttl_seconds)` | Store a value with optional TTL in seconds |
| `delete(key)` | Remove entry by key; returns `True` if deleted |
| `exists(key)` | Check whether a key exists |
| `keys(pattern)` | List keys matching a glob pattern (e.g., `"pipeline:*"`) |
| `cleanup(ttl_seconds)` | Purge expired entries; returns count deleted |

---

### `InMemoryStorageAdapter`

```python
from taskiq_flow.storage import InMemoryStorageAdapter

storage = InMemoryStorageAdapter()
# Usage transparent — same interface as other adapters
```

In-process dict-based adapter with per-key TTL support. Best suited for development, testing, and single-process deployments.

| Feature | Status |
|---------|--------|
| TTL |  Per-key TTL via `set(key, value, ttl_seconds=…)` |
| Concurrency |  Protected by `asyncio.Lock` |
| Persistence across restarts |  Volatile |
| Distributed sharing |  Process-local only |
| Pattern scanning (`keys("*")`) |  `fnmatch`-based |

---

### `RedisStorageAdapter`

```python
from taskiq_flow.storage import RedisStorageAdapter

storage = RedisStorageAdapter(
    redis_url="redis://localhost:6379",
    ttl_seconds=3600,  # Default TTL
)
```

Redis-backed persistent adapter with native TTL support, JSON serialization, and async I/O.

```python
# Store
await storage.set("pipeline:run42:status", {"phase": "running"}, ttl_seconds=86400)

# Retrieve
status = await storage.get("pipeline:run42:status")

# Pattern scan
keys = await storage.keys("pipeline:run42:*")
```

| Feature | Status |
|---------|--------|
| Native TTL |  Redis `EXPIRE` per-key |
| JSON serialization |  Automatic via `json.dumps/loads` |
| Distributed sharing |  All workers share the same Redis |
| Persistent across restarts |  (as long as Redis persists) |
| Dependency | `pip install redis` |

---

### `SQLiteStorageAdapter`

```python
from taskiq_flow.storage import SQLiteStorageAdapter

storage = SQLiteStorageAdapter(
    db_url="sqlite+aiosqlite:///taskiq-flow.db",
    async_mode=True,
)
```

SQLite/SQLAlchemy-backed adapter for persistent local storage without an external service.

```python
# Store
await storage.set("pipeline:run42:status", {"phase": "completed"})

# Works with any SQLAlchemy URL: SQLite, PostgreSQL, MySQL, etc.
pg = SQLiteStorageAdapter(db_url="postgresql+asyncpg://user:pw@host/db", async_mode=True)
```

| Feature | Status |
|---------|--------|
| Persistent |  On-disk SQLite (or any SQLAlchemy DB) |
| Async mode |  `asyncio`-compatible via `aiosqlite`/`asyncpg` |
| Distributed sharing |  Only shared via a network database (PostgreSQL, MySQL) |
| Dependency | `aiosqlite` (bundled), `sqlalchemy` (bundled) |

---

## Module: `taskiq_flow.storage.factory`

### `StorageAdapterFactory`

```python
from taskiq_flow.storage.factory import StorageAdapterFactory
from taskiq_flow.config import TaskiqFlowConfig

# Auto-detect best adapter from configuration
config = TaskiqFlowConfig()  # reads from env vars or defaults
adapter = StorageAdapterFactory.create_storage_adapter(config=config)

# Or specify broker for broker-based detection
adapter = StorageAdapterFactory.create_storage_adapter(
    config=config,
    broker=redis_broker,
    redis_url="redis://localhost:6379",
    ttl_seconds=7200,
)
```

Priority order for `create_storage_adapter(type="auto")`:

| Priority | Backend | Condition |
|----------|---------|-----------|
| 1 | `RedisStorageAdapter` | `storage_type="redis"` or broker is RedisBroker |
| 2 | `SQLiteStorageAdapter` | `storage_type="sqlite"` or `"sqlalchemy"` |
| 3 | `InMemoryStorageAdapter` | Fallback when no Redis/SQLite configured |

| Factory Method | Description |
|----------------|-------------|
| `create_storage_adapter(config, broker, redis_url, ttl_seconds)` | Create a `BaseStorageAdapter` |
| `create_cache_adapter(config, redis_url, default_ttl, lock_timeout)` | Create a `BaseCacheAdapter` |
| `create_default_middlewares(config, broker)` | Create both `StorageMiddleware` and `CacheMiddleware` |

---

## Module: `taskiq_flow.middlewares.storage`

### `StorageMiddleware`

```python
from taskiq_flow.middlewares import StorageMiddleware
from taskiq_flow.storage import InMemoryStorageAdapter

storage = InMemoryStorageAdapter()
middleware = StorageMiddleware(storage=storage, enabled=True)

broker.add_middlewares(middleware)
```

`StorageMiddleware` intercepts the TaskIQ lifecycle and persists task results
through the configured `BaseStorageAdapter`. It complements `PipelineMiddleware`
by offering **a centralized and pluggable** persistence layer.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `storage` | `BaseStorageAdapter \| None` | `None` | Storage backend to use. Auto-creates `InMemoryStorageAdapter` if `None`. |
| `enabled` | `bool` | `True` | Toggle persistence on/off globally |

| Hook | Signature | Description |
|------|-----------|-------------|
| `post_save(message, result)` | Persists `TaskiqResult` to storage keyed by `task_id` (optionally prefixed by `pipeline_id`) |

**Storage key format**: `pipeline:{pipeline_id}:task:{task_id}` or `task:{task_id}`.

---

## Examples

### Using `StorageAdapterFactory` for quick setup

```python
from taskiq_flow.storage.factory import StorageAdapterFactory

# Zero-config — auto-detects from environment
storage = StorageAdapterFactory.create_storage_adapter()

# From TaskiqFlowConfig
from taskiq_flow.config import TaskiqFlowConfig
config = TaskiqFlowConfig(
    storage_type="redis",
    storage_redis_url="redis://localhost:6379",
)
storage = StorageAdapterFactory.create_storage_adapter(config=config)
```

### Using `StorageMiddleware` in a broker

```python
from taskiq import InMemoryBroker
from taskiq_flow.middlewares import StorageMiddleware
from taskiq_flow.storage import InMemoryStorageAdapter

broker = InMemoryBroker(await_inplace=True)
middleware = StorageMiddleware(storage=InMemoryStorageAdapter())
broker.add_middlewares(middleware, PipelineMiddleware())
```

### Using `create_default_middlewares`

```python
from taskiq_flow.storage.factory import StorageAdapterFactory

middlewares = StorageAdapterFactory.create_default_middlewares()
broker.add_middlewares(
    middlewares["storage"],   # StorageMiddleware
    middlewares["cache"],     # CacheMiddleware
    PipelineMiddleware(),
)
```

---

## Choosing a Storage Backend

| Backend | Use Case | Pros | Cons |
|---------|----------|------|------|
| `InMemoryStorageAdapter` | Development, tests, single-process | Zero dependencies, fast | Volatile, not shared |
| `RedisStorageAdapter` | Production, distributed | Fast, shared, survives restarts | Requires Redis |
| `SQLiteStorageAdapter` | Lightweight persistent, no external service | No external service, SQL queries | Single-writer contention |

---

## Further Reading

- **[Storage & Cache Middleware Guide]({{ '/en/guides/cache/' | relative_url }})** — Complete middleware setup
- **[Cache API Reference]({{ '/en/api/cache/' | relative_url }})** — Dogpile-based caching
- **[Pipeline Guide]({{ '/en/guides/pipelines/' | relative_url }})** — How pipelines use storage

---

*New in v1.2.0. Storage adapters are fully interchangeable: swap the adapter without changing any application logic.*
