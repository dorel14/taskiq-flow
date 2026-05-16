---
title: Référence API : Cache
nav_order: 32
color_scheme: dark
---
# Référence API : Cache

**Mise en cache avec sémantiques Dogpile (anti-stampede)**

> **Version** : {VERSION} | **Nouveau en v1.2.0** | **Module** : `taskiq_flow.cache`, `taskiq_flow.middlewares.cache`

---

## Aperçu

Taskiq-Flow v1.2.0 introduit une **couche de cache** pour les workers, construite autour du **pattern Dogpile**. Le principe clé : lorsqu'une entrée de cache expire, seul un thread/poste est autorisé à la régénérer. Tous les autres attendent et récupèrent la valeur fraîche — annulant complètement le stampede.

```
Requêtes concurrentes à expiration TTL :

Sans Dogpile :  [tâche exécutée × 10 en parallèle] → surcharge
Avec Dogpile :  [1 tâche s'exécute, 9 attendent] → résultat unique partagé
```

---

## `BaseCacheAdapter` (ABC)

```python
from taskiq_flow.storage.base import BaseCacheAdapter

class MonAdaptateurCache(BaseCacheAdapter):
    async def get_or_create(self, key, creator, ttl_seconds=3600) -> Any: ...
    async def get(self, key) -> Any | None: ...
    async def set(self, key, value, ttl_seconds=3600) -> None: ...
    async def invalidate(self, key) -> bool: ...
    async def clear(self) -> None: ...
    def get_stats(self) -> dict: ...
```

Interface abstraite à implémenter pour tout nouveau backend de cache.

| Méthode | Anti-Stampede ? | Description |
|---------|----------------|-------------|
| `get_or_create(cle, creator, ttl)` | **Oui** | Lecture atomique : exécute `creator()` seulement si absent/expiré, avec verrou |
| `get(cle)` | Côté lecture | Consultation cache ; `None` en cas de miss |
| `set(cle, valeur, ttl)` | Côté écriture | Stockage avec TTL optionnel en secondes |
| `invalidate(cle)` | — | Éviction immédiate d'une entrée |
| `clear()` | — | Vider le cache entièrement |
| `get_stats()` | — | `{"hits", "misses", "hit_rate", "size", "keys"}` |

---

## `InMemoryCacheAdapter`

```python
from taskiq_flow.cache import InMemoryCacheAdapter

cache = InMemoryCacheAdapter()

resultat = await cache.get_or_create(
    "calcul_couteux",
    lambda: calculer_couteux(),
    ttl_seconds=300,
)
stats = cache.get_stats()
```

| Fonctionnalité | Détail |
|---------------|--------|
| Sécurité thread |  Verrou par clé `threading.Lock` |
| TTL |  Horloge monotone ; indépendant de l'heure système |
| Verrou Dogpile |  Libéré seulement quand `creator` termine |
| `creator()` async |  Si `creator()` retourne une coroutine, elle est `await`ée |
| Statistiques |  `get_stats()` : hits, misses, hit_rate, size, keys |

---

## `RedisCacheAdapter`

```python
from taskiq_flow.cache import RedisCacheAdapter

cache = RedisCacheAdapter(
    redis_url="redis://localhost:6379",
    default_ttl=3600,
    lock_timeout=10,
)
resultat = await cache.get_or_create("calcul_partage",
                                      lambda: calculer_couteux(),
                                      ttl_seconds=300)
```

Cache distribué avec verrouillage Redis pour Dogpile anti-stampede.

| Fonctionnalité | Détail |
|---------------|--------|
| Verrou distribué |  `SETNX` : plusieurs workers partagent une seule entrée |
| TTL natif Redis |  `EXPIRE` par clé |
| Sérialisation JSON |  Automatique pour types non primitifs |
| Délai de verrou max | Configurable ; évite les deadlocks si worker crashe |

---

## `CacheMiddleware`

```python
from taskiq_flow.middlewares import CacheMiddleware
broker.add_middlewares(
    PipelineMiddleware(),
    CacheMiddleware(cache=InMemoryCacheAdapter(), default_ttl=3600),
)
```

`CacheMiddleware` est la manière production-ready d'activer le cache sur un broker. Il se branche sur `pre_execute` et `post_save` :

- **`pre_execute`** — Retourne le résultat en cache si présent ; la tâche est court-circuitée.
- **`post_save`** — Stocke le résultat en cache pour la prochaine exécution.

| Paramètre du Constructeur | Type | Défaut | Description |
|--------------------------|------|--------|-------------|
| `cache` | `BaseCacheAdapter \| None` | `None` | Backend ; `None` → `InMemoryCacheAdapter` |
| `enabled` | `bool` | `True` | Toggle global |
| `default_ttl` | `int` | `3600` | Durée de vie par défaut en secondes |

**Surcharges par label de tâche :**

| Label Message | Valeurs | Effet |
|--------------|---------|-------|
| `cache_ttl` | secondes (entier) | Remplacer le TTL défaut pour cette exécution |
| `cache_errors` | `"true"` | Mettre en cache les résultats d'erreur aussi |

---

## Choix d'un Backend de Cache

| Backend | Quand l'utiliser |
|---------|-----------------|
| `InMemoryCacheAdapter` | Développement, tests, worker unique |
| `RedisCacheAdapter` | Production, multi-worker, distribué |

---

## Lectures Associées

- **[Guide des Middlewares]({{ '/fr/guides/cache/' | relative_url }})** — Configuration complète
- **[Référence API : Stockage]({{ '/fr/api/storage/' | relative_url }})** — Adaptateurs de stockage

---

*Nouveau en v1.2.0. Les adaptateurs de cache sont async et interchangeables à l'instanciation.*
