---
title: Guide des Middlewares Stockage & Cache
nav_order: 23
---
# Guide des Middlewares Stockage & Cache

**Persistance centralisée avec StorageMiddleware et cache Dogpile avec CacheMiddleware**

> **Version** : {VERSION} | **Nouveau en v1.2.0** | **Lié** : [Guide d'Exécution]({{ '/fr/guides/execution/' | relative_url }}), [Référence API — Stockage]({{ '/fr/api/storage/' | relative_url }}), [Référence API — Cache]({{ '/fr/api/cache/' | relative_url }})

---

## Aperçu

v1.2.0 introduit **deux nouveaux middlewares** qui modularisent la persistance et le cache :

| Middleware | Responsabilité |
|------------|----------------|
| `StorageMiddleware` | Persistance centralisée et pluggable des résultats, de l'état pipeline et de l'historique d'exécution |
| `CacheMiddleware` | Cache de workers Dogpile pour éviter les exécutions redondantes |

Implémentent tous deux le cycle de vie `TaskiqMiddleware` (`pre_execute`, `post_save`) et peuvent être **actifs simultanément** avec `PipelineMiddleware`, `TransportMiddleware` et `PipelineRetryMiddleware`.

---

## StorageMiddleware — Persistance Centralisée

`StorageMiddleware` capture chaque résultat de tâche et le stocke via un `BaseStorageAdapter` configuré. Contrairement à l'ancienne approche où le suivi et la planification persistaient indépendamment, il y a maintenant **un seul magasin unifié**.

### Pourquoi StorageMiddleware ?

- **Source de vérité unique** — tous les résultats, l'état des pipelines et la planification dans un seul endroit
- **Backend interchangeable** — passez de InMemory à Redis ou SQLite sans modifier le code métier
- **Auto-détection** — `StorageAdapterFactory` choisit automatiquement le bon backend
- **Isolation** — stockage, cache et suivi sont chacun dans leur propre couche

### Utilisation Basique

```python
from taskiq import InMemoryBroker
from taskiq_flow import PipelineMiddleware, DataflowPipeline, pipeline_task
from taskiq_flow.middlewares import StorageMiddleware
from taskiq_flow.storage import InMemoryStorageAdapter

broker = InMemoryBroker(await_inplace=True)
broker.add_middlewares(
    StorageMiddleware(storage=InMemoryStorageAdapter(), enabled=True),
    PipelineMiddleware(),
)
```

### Production avec Redis

```python
from taskiq_flow.middlewares import StorageMiddleware
from taskiq_flow.storage import RedisStorageAdapter

broker.add_middlewares(
    StorageMiddleware(
        storage=RedisStorageAdapter(
            redis_url="redis://localhost:6379",
            ttl_seconds=86400,
        ),
    ),
    PipelineMiddleware(),
)
```

### Clés de Stockage

`StorageMiddleware` stocke les résultats sous des clés dérivées des labels `TaskiqMessage` :

| Motif de Clé | Exemple |
|-------------|---------|
| `pipeline:{pipeline_id}:task:{task_id}` | `pipeline:audio_v1:task:abc123` |
| `task:{task_id}` | `task:abc123` |

Forme de la valeur stockée :
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

### TTL et Expiration

Tous les adaptateurs supportent le TTL par clé. Les entrées expirées sont nettoyées paresseusement à l'accès et activement via `cleanup()` :

---

## CacheMiddleware — Cache Dogpile Workers

`CacheMiddleware` évite les exécutions redondantes de tâches en mettant en cache les **résultats** des tâches au niveau worker. Le pattern Dogpile garantit qu'un seul coroutine régénère une entrée expirée.

### Pourquoi CacheMiddleware ?

- **Réduire le travail inutile** — sauter les tâches idempotentes dont les entrées n'ont pas changé
- **Latence plus faible** — les résultats en cache sont retournés instantanément
- **Protection anti-stampede** — verrou Dogpile empêche la foule à l'expiration TTL
- **Backend interchangeable** — InMemory pour mono-worker, Redis pour distribué

### Ordre des Middlewares

L'ordre des middlewares importe. `CacheMiddleware` doit être placé **avant** `StorageMiddleware` :

```python
# Ordre correct — cache vérifié d'abord, stockage ensuite
broker.add_middlewares(
    CacheMiddleware(),      # ← vérifié en premier
    StorageMiddleware(),    # ← écrit en base seulement si pas en cache
    PipelineMiddleware(),   # ← orchestre les tâches en aval
)
```

### Surcharges par Tâche via Labels

```python
# Sur une tâche spécifique, augmenter TTL à 2 heures et cacher les erreurs
result = await tache_couteuse.kiq(
    donnees_entree,
    labels={"cache_ttl": "7200", "cache_errors": "true"},
)
```

---

## StorageAdapterFactory — Configuration Zéro

`StorageAdapterFactory` crée automatiquement les bons adaptateurs depuis `TaskiqFlowConfig` :

```python
from taskiq_flow.storage.factory import StorageAdapterFactory
from taskiq_flow.config import TaskiqFlowConfig

config = TaskiqFlowConfig(
    storage_type="redis",
    storage_redis_url="redis://localhost:6379",
    storage_ttl_seconds=86400,
    cache_type="redis",
    cache_redis_url="redis://localhost:6379",
)
middlewares = StorageAdapterFactory.create_default_middlewares(config=config)

broker.add_middlewares(
    middlewares["cache"],     # CacheMiddleware
    middlewares["storage"],   # StorageMiddleware
    PipelineMiddleware(),
)
```

Variables d'environnement (toutes optionnelles) :

| Var d'env | Description |
|-----------|-------------|
| `TASKIQ_FLOW_STORAGE_TYPE` | `"redis"`, `"sqlite"`, `"inmemory"`, `"auto"` |
| `TASKIQ_FLOW_CACHE_TYPE` | `"redis"`, `"inmemory"`, `"auto"` |

---

## Comparatif : Stockage vs Cache

| Aspect | `StorageMiddleware` | `CacheMiddleware` |
|--------|--------------------|-------------------|
| Objectif | Persistance long terme (état, résultats, planification) | Déduplication court terme des résultats de tâches |
| TTL typique | Heures à jours | Minutes à heures |
| Portée | IDs de pipelines et de tâches | IDs de résultats de tâches individuelles |
| Backend | InMemory / Redis / SQLite | InMemory / Redis |
| Anti-stampede | N/A |  Oui |
| Auto-dédup | N/A |  Oui |

---

## Monitoring

### Taux de Hits de Cache

```python
stats = cache.get_stats()
print(f"Taux de hit : {stats['hit_rate']:.1%}")
print(f"Hits: {stats['hits']}, Misses: {stats['misses']}")
```

Ciblez un taux de hit > 80 % pour des pipelines reproduductibles avec entrées stables.

### Surveillance du Stockage

```python
from datetime import datetime, timezone

# Lister tous les pipelines suivis
toutes_cles = await storage.keys("pipeline:*")
print(f"Entrées totales : {len(toutes_cles)}")

# Nettoyage périodique des entrées expirées
supprimes = await storage.cleanup(ttl_seconds=3600)
print(f"Entrées expirées supprimées : {supprimes}")
```

---

## Dépannage

| Symptôme | Cause Probable | Correctif |
|---------|----------------|-----------|
| Tous les caches sont misses | TTL trop court ou entrées trop variables | Augmenter `default_ttl` ; vérifier les arguments des tâches |
| Stampede sur expirations | `InMemoryCacheAdapter` sans Dogpile distribué | Passer à `RedisCacheAdapter` pour verrou distribué |
| Croissance stockage illimitée | Aucun TTL défini | Définir `ttl_seconds` ; exécuter `cleanup()` régulièrement |
| Workers partagent résultats périmés | Redis TTL non appliqué | Vérifier `EXPIRE` Redis ; contrôler la configuration Redis |

---

## Installation Complète Production

```bash
pip install "taskiq-flow[all]"    # Toutes les fonctionnalités
docker run -p 6379:6379 redis:7   # Redis pour stockage et cache distribué
```

```python
from taskiq_flow.storage.factory import StorageAdapterFactory
from taskiq_flow.config import TaskiqFlowConfig

config = TaskiqFlowConfig(
    storage_type="redis",
    storage_redis_url="redis://localhost:6379",
    storage_ttl_seconds=86_400,
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

*Nouveau en v1.2.0. Les deux middlewares sont additifs — ajoutez-les à un broker existant sans refactoring.*
