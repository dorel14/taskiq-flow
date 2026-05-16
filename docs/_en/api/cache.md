---
title: API Reference: Cache
nav_order: 32
color_scheme: dark
---
# API Reference: Cache

**Dogpile-based caching with cache stampede sémantics**

> **Version**: {VERSION} | **New in v1.2.0** | **Module**: `taskiq_flow.cache`, `taskiq_flow.middlewares.cache`

---

## Overview

Taskiq-Flow v1.2.0 introduces a **cache layer** for workers built around the **Dogpile pattern**. The key principle: when a cached entry expires, only one thread/process is allowed to regenerate it. All other requestors wait and then pick up the fresh value — eliminating the stampede.

```
Concurrent requests at TTL expiry:

Without Dogpile:  [task runs × 10 simultaneously] → overload
With Dogpile:     [1 task runs, 9 wait] → one result shared
```

---

## `BaseCacheAdapter` (ABC)

```python
from taskiq_flow.storage.base import BaseCacheAdapter

class MyCacheAdapter(BaseCacheAdapter):
    async def get_or_create(self, key, creator, ttl_seconds=3600) -> Any: ...
    async def get(self, key) -> Any | None: ...
    async def set(self, key, value, ttl_seconds=3600) -> None: ...
    async def invalidate(self, key) -> bool: ...
    async def clear(self) -> None: ...
    def get_stats(self) -> dict: ...
```

Abstract interface; implement it to integrate a new cache backend.

| Method | Dogpile-Safe? | Description |
|--------|--------------|-------------|
| `get_or_create(key, creator, ttl)` | **Yes** | Read-through with lock: creates via `creator()` only if missing/expired, atomically |
| `get(key)` | Read side | Cache read; `None` on miss |
| `set(key, value, ttl)` | Write side | Store with optional TTL seconds |
| `invalidate(key)` | — | Evict a single entry immediately |
| `clear()` | — | Flush the entire cache |
| `get_stats()` | — | Return `{"hits", "misses", "hit_rate", "size", "keys"}` |

---

## `InMemoryCacheAdapter`

```python
from taskiq_flow.cache import InMemoryCacheAdapter

cache = InMemoryCacheAdapter()

result = await cache.get_or_create(
    "expensive_computation",
    lambda: compute_expensive(),
    ttl_seconds=300,
)
print(cache.get_stats())
# {"hits": 42, "misses": 3, "hit_rate": 0.93, "size": 41, "keys": [...]}
```

| Feature | Detail |
|---------|--------|
| Thread safety |  Per-key `threading.Lock` |
| TTL |  Monotonic clock; no system-clock dependency |
| Dogpile lock |  Lock released only when creator finishes |
| Async creator |  `creator()` may return a coroutine; it is awaited automatically |
| Stats |  `get_stats()`: hits, misses, hit_rate, size, keys |

---

## `RedisCacheAdapter`

```python
from taskiq_flow.cache import RedisCacheAdapter

cache = RedisCacheAdapter(
    redis_url="redis://localhost:6379",
    default_ttl=3600,
    lock_timeout=10,
)
result = await cache.get_or_create("shared_computation",
                                    lambda: compute_expensive(),
                                    ttl_seconds=300)
```

Distributed cache with Redis-backed Dogpile locking.

| Feature | Detail |
|---------|--------|
| Distributed Dogpile lock |  `SETNX`-based; multiple workers safely share one entry |
| Native Redis TTL |  `EXPIRE` per key |
| JSON serialization |  Automatic for non-primitive types |
| Lock timeout | Configurable; prevents deadlocks if a worker crashes mid-generation |

---

## `CacheMiddleware`

```python
from taskiq_flow.middlewares import CacheMiddleware
from taskiq_flow.cache import InMemoryCacheAdapter

broker.add_middlewares(
    PipelineMiddleware(),
    CacheMiddleware(cache=InMemoryCacheAdapter(), default_ttl=3600),
)
```

`CacheMiddleware` is the production-ready way to enable caching on a broker. It hooks into both `pre_execute` and `post_save`:

- **`pre_execute`** — Returns cached result if present; task is skipped (short-circuited).
- **`post_save`** — Stores the successful result in cache for next time.

| Constructor Parameter | Type | Default | Description |
|----------------------|------|---------|-------------|
| `cache` | `BaseCacheAdapter \| None` | `None` | Cache backend; `None` → `InMemoryCacheAdapter` |
| `enabled` | `bool` | `True` | Global toggle |
| `default_ttl` | `int` | `3600` | Default cache lifetime in seconds |

**Per-task label overrides:**

| Message Label | Values | Effect |
|---------------|--------|--------|
| `cache_ttl` | integer seconds | Override TTL for this task execution |
| `cache_errors` | `"true"` | Cache failed (error) results too |

---

## Choosing a Cache Backend

| Backend | When to use |
|---------|-------------|
| `InMemoryCacheAdapter` | Development, tests, single-worker |
| `RedisCacheAdapter` | Production, multi-worker, distributed |

---

## Further Reading

- **[Storage & Cache Middleware Guide]({{ '/en/guides/cache/' | relative_url }})** — Full middleware configuration
- **[Storage API Reference]({{ '/en/api/storage/' | relative_url }})** — Storage adapters

---

*New in v1.2.0. Cache adapters are async and interchangeable at instantiation time.*
