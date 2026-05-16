---
title: Référence API : Stockage
nav_order: 31
color_scheme: dark
---
# Référence API : Stockage

**Couche de persistance centralisée — adaptateurs, factory et StorageMiddleware**

> **Version** : {VERSION} | **Nouveau en v1.2.0** | **Module** : `taskiq_flow.storage`, `taskiq_flow.middlewares.storage`

---

## Aperçu

Taskiq-Flow v1.2.0 introduit une **couche de stockage centralisée** qui découple les préoccupations de persistance du broker sous-jacent. Le système de stockage offre :

- **Une interface unifiée** — `BaseStorageAdapter` fonctionne avec tous les backends
- **Trois adaptateurs natifs** — InMemory, Redis, SQLite/SQLAlchemy
- **Factory d'auto-détection** — `StorageAdapterFactory` choisit le bon backend automatiquement
- **Intégration middleware** — `StorageMiddleware` se branche dans le cycle de vie TaskIQ

Utilisez `StorageMiddleware` plutôt que du code ad-hoc : il intercepte les événements de tâche et persiste les résultats via un adaptateur interchangeable.

---

## Module `taskiq_flow.storage`

### `StorageEntry`

```python
from taskiq_flow.storage import StorageEntry
from datetime import datetime, timezone

entry = StorageEntry(
    key="pipeline:run42:task:abc123",
    value={"statut": "terminé", "resultat": 42},
    expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
    metadata={"pipeline_id": "run42"},
)
```

Conteneur typé pour une valeur stockée avec TTL optionnel et métadonnées.

| Attribut | Type | Description |
|----------|------|-------------|
| `key` | `str` | Clé unique de l'entrée |
| `value` | `Any` | Valeur stockée (recommandé : sérialisable JSON) |
| `created_at` | `datetime` | Horodatage de création (UTC) |
| `expires_at` | `datetime \| None` | Horodatage d'expiration ; `None` = pas d'expiration |
| `metadata` | `dict` | Métadonnées arbitraires |

| Méthode | Signature | Description |
|---------|-----------|-------------|
| `is_expired()` | `() -> bool` | `True` si l'entrée a expiré |
| `remaining_ttl()` | `() -> float \| None` | Secondes restantes avant expiration ; `None` si jamais |

---

### `BaseStorageAdapter` (ABC)

```python
from taskiq_flow.storage import BaseStorageAdapter

class MonAdaptateur(BaseStorageAdapter):
    async def get(self, key: str) -> Any | None: ...
    async def set(self, key: str, value: Any, ttl_seconds=None) -> None: ...
    async def delete(self, key: str) -> bool: ...
    async def exists(self, key: str) -> bool: ...
    async def keys(self, pattern="*") -> list[str]: ...
    async def cleanup(self, ttl_seconds=3600) -> int: ...
```

Interface abstraite que tous les backends de stockage doivent implémenter. Utilisez-la pour créer un backend personnalisé (PostgreSQL, DynamoDB, etc.).

| Méthode | Description |
|---------|-------------|
| `get(cle)` | Récupérer une valeur par clé ; `None` si absente ou expirée |
| `set(cle, valeur, ttl_seconds)` | Stocker une valeur avec TTL optionnel en secondes |
| `delete(cle)` | Supprimer l'entrée ; retourne `True` si supprimée |
| `exists(cle)` | Vérifier l'existence d'une clé |
| `keys(motif)` | Lister les clés correspondant à un motif glob (ex. `"pipeline:*"`) |
| `cleanup(ttl_seconds)` | Purger les entrées expirées ; retourne le nombre supprimé |

---

### `InMemoryStorageAdapter`

```python
from taskiq_flow.storage import InMemoryStorageAdapter

stockage = InMemoryStorageAdapter()
```

Adaptateur en mémoire basé sur un `dict` avec support de TTL par clé. Idéal pour le développement, les tests et les déploiements mono-processus.

---

### `RedisStorageAdapter`

```python
from taskiq_flow.storage import RedisStorageAdapter

stockage = RedisStorageAdapter(
    redis_url="redis://localhost:6379",
    ttl_seconds=3600,
)
```

Adaptateur persistant basé sur Redis avec TTL natif et sérialisation JSON.

| Fonctionnalité | Statut |
|---------------|--------|
| TTL natif |  Par clé via `EXPIRE` Redis |
| Sérialisation JSON |  Automatique |
| Partage distribué |  Tous les workers partagent le même Redis |
| Persistance |  Tant que Redis persiste |

---

### `SQLiteStorageAdapter`

```python
from taskiq_flow.storage import SQLiteStorageAdapter

stockage = SQLiteStorageAdapter(
    db_url="sqlite+aiosqlite:///taskiq-flow.db",
    async_mode=True,
)
```

Adaptateur SQLite/SQLAlchemy pour une persistance locale sans service externe.

---

## Module `taskiq_flow.storage.factory`

### `StorageAdapterFactory`

```python
from taskiq_flow.storage.factory import StorageAdapterFactory
config = TaskiqFlowConfig()
adaptateur = StorageAdapterFactory.create_storage_adapter(config=config)
```

Ordre de priorité pour `create_storage_adapter(type="auto")` :

| Priorité | Backend | Condition |
|----------|---------|-----------|
| 1 | `RedisStorageAdapter` | `storage_type="redis"` ou broker est RedisBroker |
| 2 | `SQLiteStorageAdapter` | `storage_type="sqlite"` ou `"sqlalchemy"` |
| 3 | `InMemoryStorageAdapter` | Fallback |

| Méthode de Factory | Description |
|-------------------|-------------|
| `create_storage_adapter(config, broker, …)` | Crée un `BaseStorageAdapter` |
| `create_cache_adapter(config, …)` | Crée un `BaseCacheAdapter` |
| `create_default_middlewares(config, broker)` | Crée `StorageMiddleware` et `CacheMiddleware` |

---

## Module `taskiq_flow.middlewares.storage`

### `StorageMiddleware`

```python
from taskiq_flow.middlewares import StorageMiddleware
broker.add_middlewares(
    StorageMiddleware(storage=InMemoryStorageAdapter(), enabled=True),
    PipelineMiddleware(),
)
```

Intercepte le cycle de vie TaskIQ et persiste les résultats de tâche via l'adaptateur de stockage configuré.

| Paramètre | Type | Défaut | Description |
|-----------|------|--------|-------------|
| `storage` | `BaseStorageAdapter \| None` | `None` | Backend de stockage |
| `enabled` | `bool` | `True` | Active/désactive la persistance |

| Hook | Signature | Description |
|------|-----------|-------------|
| `post_save(message, result)` | Persiste `TaskiqResult` dans le stockage | Clé : `task:{task_id}` ou `pipeline:{pipeline_id}:task:{task_id}` |

---

## Choix d'un Backend

| Backend | Cas d'usage | Avantages | Inconvénients |
|---------|------------|-----------|---------------|
| `InMemoryStorageAdapter` | Dev, tests, mono-processus | Zéro dépendance, rapide | Volatile, non partagé |
| `RedisStorageAdapter` | Production, distribué | Rapide, partagé, persisté | Requiert Redis |
| `SQLiteStorageAdapter` | Persistance légère sans service externe | Pas de service externe | Contention mono-écriture |

---

## Lectures Associées

- **[Guide Stockage & Cache]({{ '/fr/guides/cache/' | relative_url }})** — Configuration complète des middlewares
- **[Référence API : Cache]({{ '/fr/api/cache/' | relative_url }})** — Adaptateurs de cache Dogpile

---

*Nouveau en v1.2.0. Les adaptateurs de stockage sont entièrement interchangeables : changez l'adaptateur sans toucher la logique métier.*
