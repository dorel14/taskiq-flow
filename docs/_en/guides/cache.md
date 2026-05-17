---
title: Storage & Cache Middleware Guide
nav_order: 23
---
# Storage & Cache Middleware Guide

**Centralized persistence with StorageMiddleware and Dogpile caching with CacheMiddleware**

> **Version**: {VERSION} | **New in v1.2.0** | **Related**: [Execution Guide]({{ '/en/guides/execution/' | relative_url }}), [API Reference — Storage]({{ '/en/api/storage/' | relative_url }}), [API Reference — Cache]({{ '/en/api/cache/' | relative_url }})

---

## Overview

v1.2.0 introduces **two new middlewares** that modularize persistence and caching concerns:

| Middleware | Responsibility |
|------------|----------------|
| `StorageMiddleware` | Centralized, pluggable persistence for task results, pipeline state, and execution history |
| `CacheMiddleware` | Dogpile-based worker caching to avoid redundant task executions |

Both implement the `TaskiqMiddleware` lifecycle (`pre_execute`, `post_save`) and can be active **simultaneously** with `PipelineMiddleware`, `TransportMiddleware`, and `PipelineRetryMiddleware`.

---

## StorageMiddleware — Centralized Persistence

`StorageMiddleware` captures every task result and stores it via a configured `BaseStorageAdapter`. Unlike the previous approach where tracking and scheduling persisted independently, there is now **one unified store**.

### Why StorageMiddleware?

- **Single source of truth** — all task results, pipeline history, and scheduling metadata live in one place
- **Pluggable backend** — swap InMemory for Redis or SQLite without changing application code
- **Auto-detection** — `StorageAdapterFactory` picks the right backend from environment and config
- **Isolation** — storage, cache, and tracking concerns are each in their own layer

### Installation

No extra install required — included in `taskiq-flow`.

```bash
# For Redis backend
pip install redis

# SQLite backend included via aiosqlite (bundled)
```

### Basic Usage

```python
from taskiq import InMemoryBroker
from taskiq_flow import PipelineMiddleware, DataflowPipeline, pipeline_task
from taskiq_flow.middlewares import StorageMiddleware
from taskiq_flow.storage import InMemoryStorageAdapter

broker = InMemoryBroker(await_inplace=True)

# central persistence layer — store all task results automatically
storage = InMemoryStorageAdapter()
broker.add_middlewares(
    StorageMiddleware(storage=storage, enabled=True),
    PipelineMiddleware(),
)
```

### Production with Redis

```python
from taskiq_flow.middlewares import StorageMiddleware
from taskiq_flow.storage import RedisStorageAdapter

broker.add_middlewares(
    StorageMiddleware(
        storage=RedisStorageAdapter(
            redis_url="redis://localhost:6379",
            ttl_seconds=86400,   # 24-hour retention
        ),
    ),
    PipelineMiddleware(),
)
```

### Task Result Keys

`StorageMiddleware` stores results under keys derived from the `TaskiqMessage` labels:

| Key Pattern | Example |
|-------------|---------|
| `pipeline:{pipeline_id}:task:{task_id}` | `pipeline:audio_v1:task:abc123` |
| `task:{task_id}` | `task:abc123` |

Stored value shape:
```json
{
  "task_id": "abc123",
  "pipeline_id": "audio_v1",
  "is_err": false,
  "return_value": "{...}",
  "error": null,
  "execution_time": 0.42
}
```

### Manual Inspector Usage

```python
storage = InMemoryStorageAdapter()
await storage.set("my_key", {"status": "running"}, ttl_seconds=600)

# Later...
result = await storage.get("my_key")
exists = await storage.exists("my_key")

# Pattern-based listing
keys = await storage.keys("pipeline:my_run:*")

# Cleanup expired entries
deleted = await storage.cleanup(ttl_seconds=3600)
```

### TTL and Expiration

All three adapters support per-key TTL. Entries that have expired are lazily cleaned on access and eagerly via `cleanup()`:

```python
# Store with 24-hour TTL
await storage.set("status", {"running": True}, ttl_seconds=86_400)

# Check remaining time before it expires
entry = StorageEntry(key="status", value={"running": True}, ...)
seconds_left = entry.remaining_ttl()
```

---

## CacheMiddleware — Dogpile Worker Caching

`CacheMiddleware` prevents redundant task executions by caching task **outputs** at the worker level. The Dogpile pattern ensures that only one coroutine regenerates an expired entry while others wait.

### Why CacheMiddleware?

- **Reduce unnecessary work** — skip re-executing idempotent tasks whose inputs haven't changed
- **Lower latency** — cached results are returned instantly without scheduling
- **Stampede protection** — Dogpile lock prevents thundering-herd at TTL expiry
- **Pluggable backend** — InMemory for single-worker, Redis for distributed

### Basic Usage

```python
from taskiq_flow.middlewares import CacheMiddleware
from taskiq_flow.cache import InMemoryCacheAdapter

# Task results are cached for 1 hour by default
broker.add_middlewares(
    CacheMiddleware(
        cache=InMemoryCacheAdapter(),
        default_ttl=3600,
        enabled=True,
    )
)
```

After this, every task's result is cached automatically. A second task execution with the same result is reduced to a cache lookup.

### Producer/Consumer Middleware Ordering

Middleware order matters. `CacheMiddleware` should be placed **before** `StorageMiddleware` in the chain so that cache hits short-circuit before any persistence write is attempted:

```python
# Correct ordering — cache checked first, then storage
broker.add_middlewares(
    CacheMiddleware(),      # ← checked first (pre_execute runs first)
    StorageMiddleware(),    # ← persisted if not cached
    PipelineMiddleware(),   # ← orchestrates downstream
)
```

### Per-Task Overrides

Set TTL and error-caching per task execution via `TaskiqMessage` labels:

```python
# In a task, override cache TTL for this execution
result = await some_task.kiq(
    input_data,
    labels={"cache_ttl": "7200", "cache_errors": "true"},
)
```

| Label | Values | Effect |
|-------|--------|--------|
| `cache_ttl` | `int` (seconds) | Override the `default_ttl` for this single execution |
| `cache_errors` | `"true"` / `"false"` | Cache error results when `"true"` (disabled by default) |

---

## StorageAdapterFactory — Zero-Config Setup

`StorageAdapterFactory` auto-creates the right adapters from `TaskiqFlowConfig` (read from env vars):

```python
from taskiq_flow.storage.factory import StorageAdapterFactory
from taskiq_flow.config import TaskiqFlowConfig

# Get both middlewares in one call — sensible defaults
config = TaskiqFlowConfig(
    storage_type="redis",                    # "redis" | "sqlite" | "inmemory" | "auto"
    storage_redis_url="redis://localhost:6379",
    storage_ttl_seconds=86_400,              # 24 h
    cache_type="redis",
    cache_redis_url="redis://localhost:6379",
    cache_default_ttl=3600,
)
middlewares = StorageAdapterFactory.create_default_middlewares(config=config)

broker.add_middlewares(
    middlewares["cache"],      # CacheMiddleware
    middlewares["storage"],    # StorageMiddleware
    PipelineMiddleware(),
)
```

Environment variables (all optional):

| Env Var | Description |
|---------|-------------|
| `TASKIQ_FLOW_STORAGE_TYPE` | `"redis"`, `"sqlite"`, `"inmemory"`, or `"auto"` |
| `TASKIQ_FLOW_STORAGE_REDIS_URL` | Redis URL for storage |
| `TASKIQ_FLOW_STORAGE_TTL_SECONDS` | Default storage TTL |
| `TASKIQ_FLOW_CACHE_TYPE` | `"redis"`, `"inmemory"`, or `"auto"` |
| `TASKIQ_FLOW_CACHE_REDIS_URL` | Redis URL for cache |

---

## Comparison: Storage vs Cache

| Aspect | `StorageMiddleware` | `CacheMiddleware` |
|--------|--------------------|--------------------|
| Purpose | Long-term persistence of task/pipeline state | Short-term deduplication of task results |
| TTL | Hours to days (`storage_ttl_seconds`) | Minutes to hours (`default_ttl`) |
| Scope | Pipeline IDs, task IDs, scheduling metadata | Individual task result IDs |
| Backend | InMemory / Redis / SQLite | InMemory / Redis |
| Dogpile stampede | N/A |  Yes |
| Auto-dedup | N/A |  Yes |

Use **both** together for a complete production setup:


```python
from taskiq_flow.storage.factory import StorageAdapterFactory
from taskiq_flow.config import TaskiqFlowConfig

config = TaskiqFlowConfig(
    storage_type="redis",
    storage_redis_url="redis://localhost:6379",
    cache_type="redis",
    cache_redis_url="redis://localhost:6379",
)
middlewares = StorageAdapterFactory.create_default_middlewares(config=config)
broker.add_middlewares(
    middlewares["cache"],
    middlewares["storage"],
    PipelineMiddleware(),
)
```

---

## Monitoring

### Cache Hit Rate

```python
stats = cache.get_stats()
print(f"Hit rate: {stats['hit_rate']:.1%}")  # 94.5%
print(f"Hits: {stats['hits']}, Misses: {stats['misses']}")
```

Aim for a hit rate above 80% for reproducible pipelines with stable inputs.

### Storage Size

```python
all_keys = await storage.keys("*")
print(f"Total stored entries: {len(all_keys)}")
```

---

## Troubleshooting

| Symptom | Likely Cause | Fix |
|---------|-------------|-----|
| Every task is a cache miss | TTL too short or inputs too variable | Increase `default_ttl`; check task arguments |
| Cache stampede on expiry | Using `InMemoryCacheAdapter` without Dogpile | Switch to `RedisCacheAdapter` (proper distributed locking) |
| Storage grows without bounds | No TTL set on entries | Set `ttl_seconds` on `StorageMiddleware`; run `cleanup()` periodically |
| Workers share stale results | Redis TTL not respected | Verify Redis `EXPIRE` is applied; check Redis config |

---

*New in v1.2.0. Both middlewares are additive — drop them into an existing broker without redesign.*
