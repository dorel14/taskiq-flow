"""
Module de stockage pour TaskIQ-Flow.

Fournit des adaptateurs de stockage interchangeables pour le tracking,
l'ordonnancement et d'autres composants.

Auteur: SoniqueBay Team
Version: 1.2.0
"""

from taskiq_flow.storage.base import (
    BaseCacheAdapter,
    BaseStorageAdapter,
    StorageEntry,
)
from taskiq_flow.storage.memory import InMemoryStorageAdapter
from taskiq_flow.storage.redis import RedisStorageAdapter
from taskiq_flow.storage.sqlite import SQLiteStorageAdapter

__all__ = [
    "BaseCacheAdapter",
    "BaseStorageAdapter",
    "InMemoryStorageAdapter",
    "RedisStorageAdapter",
    "SQLiteStorageAdapter",
    "StorageEntry",
]
