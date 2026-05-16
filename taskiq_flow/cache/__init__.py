"""
Module de cache pour TaskIQ-Flow.

Fournit des adaptateurs de cache interchangeables pour les workers,
avec support Dogpile (anti-stampede), TTL et verrouillage.

Auteur: SoniqueBay Team
Version: 1.2.0
"""

from taskiq_flow.cache.memory import InMemoryCacheAdapter

try:
    from taskiq_flow.cache.redis import RedisCacheAdapter
except ImportError:
    RedisCacheAdapter = None  # type: ignore

__all__ = [
    "InMemoryCacheAdapter",
    "RedisCacheAdapter",
]
