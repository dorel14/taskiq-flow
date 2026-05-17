---
title: Guide d'Optimisation des Performances
nav_order: 27
color_scheme: dark
---
# Guide d'Optimisation des Performances

**Parallélisme conscient des ressources, optimisation mémoire et stratégies de mise à l'échelle**

> **Version** : {VERSION} | **Lié** : [Guide d'Exécution]({{ '/fr/guides/execution/' | relative_url }}), [Guide de Suivi]({{ '/fr/guides/tracking/' | relative_url }})

---

## Aperçu

Taskiq-Flow est conçu pour une exécution asynchrone hautes performances. Ce guide couvre les techniques d'optimisation pour maximiser le débit, minimiser la latence et utiliser efficacement les ressources système.

Sujets abordés :

- Réglage du parallélisme (`max_parallel`)
- Profilage CPU et RAM
- Profils de ressources des tâches
- Stratégies de gestion mémoire
- Identification des goulots d'étranglement
- Passage d'un worker unique à distribué

---

## 1. Comprendre le Paysage des Performances

L'optimisation des performances implique des compromis :

| Dimension | Ce qui est affecté | Compromis typique |
|-----------|--------------------|-------------------|
| **Concurrence** | Débit (tâches/seconde) | Utilisation mémoire, changement de contexte |
| **Parallélisme** | Utilisation CPU | Surcharge de coordination |
| **Latence** | Temps de complétion des tâches | Consommation de ressources |
| **Mémoire** | Capacité du jeu de données | Pauses GC, efficacité du cache |
| **I/O** | Appels services externes | Bande passante réseau, limites de connexions |

**Aperçu clé** : Le parallélisme de Taskiq-Flow est limité par les paramètres `max_parallel` à travers les étapes du pipeline, et par les ressources système disponibles (cœurs CPU, RAM).

---

## 2. Réglage du Parallélisme

### 2.1. Le Paramètre `max_parallel`

Contrôle l'exécution concurrente des tâches au niveau de l'étape :

```python
# Pipeline Séquentiel
pipeline.map(process_item, items, max_parallel=10)  # Max 10 concurrentes

# Pipeline Dataflow : configuration au niveau pipeline
pipeline = DataflowPipeline(broker, max_parallel=20)

# MapReduce
mapped = await MapReduce.map(
    broker,
    process_item,
    items,
    max_parallel=15
)
```

**Comportement par défaut** : Sans `max_parallel`, Taskiq-Flow tente d'exécuter toutes les tâches indépendantes concurremment (essentiellement illimité). C'est acceptable pour les petits nombres (<100) mais dangereux pour les grands jeux de données.

### 2.2. Déterminer le `max_parallel` Optimal

#### Pour les Tâches Liées aux I/O (appels réseau, I/O disque)

```python
# Attente I/O élevée, CPU faible : peut gérer beaucoup de tâches concurrentes
pipeline.map(fetch_url, url_list, max_parallel=50)
# Règle empirique : 2–5 × nombre de cœurs CPU
```

**Justification** : Pendant qu'une tâche attend le réseau, une autre utilise le CPU. Une haute concurrence sature les pipelines d'I/O.

#### Pour les Tâches Intensives en CPU (calculs, transcodage)

```python
# Intensif en CPU : limiter au nombre de cœurs (ou légèrement plus)
import os
cpu_cores = os.cpu_count() or 4
pipeline.map(transcode, files, max_parallel=cpu_cores + 2)
# Règle empirique : cœurs CPU ± 2
```

**Justification** : Le GIL de Python limite le vrai parallélisme ; `asyncio` bénéficie toujours de plusieurs cœurs quand les tâches libèrent le GIL (NumPy, extensions C). Une sur-inscription entraîne des surcoûts de changement de contexte.

#### Pour les Charges de Travail Mixtes

Profilez et ajustez :

```python
# Commencez prudent
for parallel in [5, 10, 20, 50]:
    start = time.time()
    await pipeline.kiq_dataflow(data)
    duration = time.time() - start
    print(f"Parallélisme {parallel} : {duration:.2f}s")
```

Trouvez le **coude de la courbe** — point où augmenter le parallélisme donne des rendements décroissants.

### 2.3. Limite Globale de Parallélisme

Définissez une limite globale sur tous les pipelines :

```python
from taskiq_flow.optimization.parallel import set_max_parallel_tasks

set_max_parallel_tasks(100)  # Ne jamais dépasser 100 tâches concurrentes globalement
```

Utile dans les systèmes multi-tenants pour empêcher un pipeline d'en asphyxier d'autres.

---

## 3. Ordonnancement Conscient des Ressources

Taskiq-Flow peut ordonnancer les tâches selon les besoins CPU/RAM (nécessite un pool de workers conscient des ressources — avancé).

### 3.1. Annoter les Tâches avec Besoins en Ressources

```python
from taskiq_flow import CPUProfile, RAMProfile

@broker.task
@CPUProfile(cpu_units=2)  # Nécessite 2 cœurs CPU
@RAMProfile(ram_mb=4096)   # Nécessite 4 Go RAM
def heavy_computation(data):
    # Ne s'exécutera que sur des workers avec ressources suffisantes
    pass
```

### 3.2. Pool de Workers Conscient des Ressources

```python
from taskiq_flow import ResourceAwareWorkerPool

pool = ResourceAwareWorkerPool(
    workers=[
        {"cpu_cores": 8, "ram_gb": 32, "labels": {"gpu": True}},
        {"cpu_cores": 4, "ram_gb": 16, "labels": {"gpu": False}},
    ]
)

# Les tâches sont automatiquement routées vers les workers compatibles
```

**Note** : Cette fonctionnalité nécessite une implémentation worker personnalisée ; les brokers standards ignorent les profils de ressources.

---

## 4. Optimisation Mémoire

### 4.1. Éviter les Transferts de Données Volumineuses en Mémoire

Passez des références au lieu des données complètes :

```python
#  Mauvais : copie le jeu de données complet pour chaque appel de tâche
pipeline.map(process, large_dataset)  # Chaque tâche reçoit une copie complète

#  Mieux : passez des identifiants, récupérez dans la tâche
@broker.task
def process(item_id: str):
    item = database.get(item_id)  # Récupération à la demande
    return process_item(item)

pipeline.map(process, item_ids)  # Seuls les IDs sont passés
```

### 4.2. Streamer les Gros Jeux de Données

Utilisez le découpage en chunks :

```python
def chunked(iterable, chunk_size=100):
    for i in range(0, len(iterable), chunk_size):
        yield iterable[i:i + chunk_size]

for chunk in chunked(large_list, 100):
    results = await pipeline.kiq_dataflow(chunk)
    # Traitez les résultats avant le prochain chunk pour libérer la mémoire
```

### 4.3. Nettoyer les Résultats Après Utilisation

Les résultats de pipeline restent dans le stockage de suivi. Nettoyez après usage :

```python
# Après traitement, supprimez l'enregistrement du pipeline
await tracking.delete_pipeline(pipeline.pipeline_id)
```

Ou définissez un TTL sur le stockage :

```python
RedisPipelineStorage(redis, ttl_seconds=86400)  # Suppression auto après 1 jour
```

---

## 5. Profilage & Détection des Goulots d'Étranglement

### 5.1. Chronométrage Intégré

Chaque étape enregistre la durée automatiquement (avec le suivi activé) :

```python
status = await tracking.get_status(pipeline_id)
for step in status.steps:
    print(f"{step.name}: {step.duration_ms}ms")
```

Identifiez les étapes les plus lentes → cibles d'optimisation.

### 5.2. Profilage Mémoire

Utilisez `tracemalloc` de Python :

```python
import tracemalloc

tracemalloc.start()

# Exécutez le pipeline
await pipeline.kiq(data)

# Vérifiez l'usage mémoire
current, peak = tracemalloc.get_traced_memory()
print(f"Actuel : {current/1024/1024:.1f} Mo")
print(f"Pic : {peak/1024/1024:.1f} Mo")
tracemalloc.stop()
```

### 5.3. Profilage CPU

```python
import cProfile
import pstats

profiler = cProfile.Profile()
profiler.enable()

await pipeline.kiq(data)

profiler.disable()
stats = pstats.Stats(profiler)
stats.sort_stats('cumulative')
stats.print_stats(20)  # Top 20 fonctions
```

### 5.4. Profilage Spécifique Asyncio

`uvloop` pour une boucle d'événements plus rapide :

```python
import uvloop
uvloop.install()  # Remplace la boucle asyncio par défaut
```

Amélioration benchmark : `uvloop` peut fournir un gain 2×–3× pour les charges liées aux I/O.

---

## 6. Optimisation Base de Données / Services Externes

### 6.1. Pool de Connexions

Pour les bases de données (PostgreSQL, Redis), réutilisez les connexions :

```python
from asyncpg import create_pool

pool = await create_pool(database="...", min_size=5, max_size=20)

@broker.task
async def db_task(query: str):
    async with pool.acquire() as conn:
        return await conn.fetch(query)
```

### 6.2. Opérations par Lots

Au lieu de nombreux petits appels, faites des lots :

```python
#  N appels séparés
for item in items:
    await db.insert(item)

#  Insertion par lot unique
await db.bulk_insert(items)
```

### 6.3. Mise en Cache des Résultats

```python
from functools import lru_cache

@broker.task
@lru_cache(maxsize=1000)
def expensive_computation(key: str):
    return compute(key)
```

Ou utilisez un cache Redis :

```python
import redis
cache = redis.Redis(...)

@broker.task
async def cached_task(key: str):
    cached = await cache.get(key)
    if cached:
        return json.loads(cached)
    result = await compute(key)
    await cache.setex(key, 3600, json.dumps(result))
    return result
```

---

## 7. Mise à l'Échelle Distribuée

### 7.1. Workers Multiples

Mise à l'échelle horizontale en lançant plusieurs processus worker :

```bash
# Terminal 1
taskiq worker --broker redis://localhost:6379

# Terminal 2
taskiq worker --broker redis://localhost:6379

# Terminal 3
taskiq worker --broker redis://localhost:6379
```

Tous les workers partagent le même broker (Redis) et traitent les tâches concurremment.

**Débit ≈ (# workers) × (tâches/worker/seconde)**.

### 7.2. Gestion du Pool de Workers

Utilisez un gestionnaire de processus (systemd, supervisord, Docker Compose) :

```yaml
# docker-compose.yml
services:
  worker-1:
    image: taskiq-flow-worker
    command: taskiq worker --broker ${REDIS_URL}
  worker-2:
    image: taskiq-flow-worker
    command: taskiq worker --broker ${REDIS_URL}
  worker-3:
    image: taskiq-flow-worker
    command: taskiq worker --broker ${REDIS_URL}
```

### 7.3. Priorisation des Files

Routez les pipelines critiques vers des files dédiées :

```python
@broker.task(queue="high_priority")
def critical_task(): ...

# Les workers peuvent être configurés pour traiter certaines files en priorité
```

### 7.4. Géodistribution

Pour des déploiements mondiaux à faible latence, déployez des workers dans plusieurs régions avec un broker global (Kafka) ou des clusters Redis régionaux avec réplication.

---

## 8. Benchmarking

Mesurez avant et après optimisation :

```python
import time

async def benchmark(pipeline, iterations=10):
    durations = []
    for _ in range(iterations):
        start = time.perf_counter()
        result = await pipeline.kiq(data)
        await result.wait_result()
        duration = time.perf_counter() - start
        durations.append(duration)

    avg = sum(durations) / len(durations)
    p95 = sorted(durations)[int(0.95 * len(durations))]
    print(f"Moyenne: {avg:.3f}s, P95: {p95:.3f}s")
    return durations
```

**Métriques clés** :

- **Débit** : tâches/seconde
- **Latence P50/P95/P99** : médiane, 95ème, 99ème percentile
- **Pic mémoire** : mémoire résidente maximale
- **Utilisation CPU** : % de cœurs utilisés

---

## 9. Checklist Production

- [ ] Définir `max_parallel` adapté au type de tâche (CPU vs I/O)
- [ ] Utiliser le pool de connexions pour services externes
- [ ] Activer le stockage Redis pour le suivi (éviter les fuites mémoire)
- [ ] Définir un TTL sur le stockage de suivi/résultats
- [ ] Configurer les timeouts sur toutes les tâches
- [ ] Ajouter des politiques de retry avec backoff et jitter
- [ ] Surveiller l'usage mémoire et définir des alertes
- [ ] Profiler les tâches lentes avec cProfile/tracemalloc
- [ ] Mettre à l'échelle les workers horizontalement selon la profondeur de file
- [ ] Utiliser les priorités de file pour les pipelines critiques
- [ ] Implémenter la DLQ et réviser régulièrement les tâches échouées
- [ ] Tester les scénarios de panne (partitions réseau, pannes service)

---

## 10. Dépannage des Performances

### Pipeline Lent

**Étapes de diagnostic** :

1. Vérifiez les durées d'étapes dans le suivi :
   ```python
   status = await tracking.get_status(pipeline_id)
   slowest = max(status.steps, key=lambda s: s.duration_ms)
   print(f"Étape la plus lente : {slowest.name} à {slowest.duration_ms}ms")
   ```

2. Profilez avec cProfile pour voir où le temps est passé
3. Vérifiez que `max_parallel` n'est pas trop bas
4. Cherchez des I/O bloquants (utilisez des librairies async)

### Utilisation Mémoire Élevée

**Causes & corrections** :

| Cause | Correction |
|-------|------------|
| Gros jeu de données dans une seule étape | Découper les données, traiter par lots |
| Résultats s'accumulant dans le stockage de suivi | Définir TTL, supprimer après usage |
| Fuite mémoire dans le code de tâche | Profiler avec `tracemalloc`, corriger les fuites |
| Trop de tâches parallèles | Réduire `max_parallel` |

### Worker en Manque (Starvation)

**Symptôme** : Tâches en file mais non exécutées.

**Corrections** :
- Augmenter le nombre de processus workers
- Vérifier que le broker (Redis) a assez de connexions
- Chercher des tâches longues bloquant la file
- Envisager les priorités de tâches ou files séparées

---

## 11. Avancé : Exécuteurs Personnalisés

Pour des charges spécialisées, implémentez des exécuteurs personnalisés :

```python
from taskiq_flow import ExecutionEngine
from taskiq_flow.dataflow import DAG

class GPUOptimizedEngine(ExecutionEngine):
    async def schedule_task(self, task_node, inputs):
        # Logique d'ordonnancement personnalisée : router les tâches GPU vers workers GPU
        if task_node.labels.get("requires_gpu"):
            return await self.gpu_worker_pool.submit(task_node, inputs)
        return await super().schedule_task(task_node, inputs)

engine = GPUOptimizedEngine(broker, dag)
results = await engine.execute(inputs)
```

### 11.1. ResourceAwareExecutor et TaskResourceProfile

TaskIQ-Flow fournit un exécuteur conscient des ressources qui peut être utilisé
pour allouer des tâches aux workers en fonction de leurs besoins en ressources :

```python
from taskiq_flow import ResourceAwareExecutor, TaskResourceProfile

# Définir un profil de ressources pour les tâches lourdes
heavy_profile = TaskResourceProfile(
    estimated_memory_mb=2048,
    estimated_cpu_cores=4.0,
)

@broker.task
@heavy_profile
def heavy_computation(data):
    # Cette tâche nécessite 4 cœurs CPU et 2 Go de RAM
    return process_heavy_data(data)

# Utiliser ResourceAwareExecutor pour l'exécution
executor = ResourceAwareExecutor(
    broker=broker,
    max_parallel=10,
)
```

`ResourceAwareExecutor` évalue les profils de ressources des tâches et les
distribue aux workers disponibles en fonction de leur capacité.
`TaskResourceProfile` permet d'annoter chaque tâche avec ses besoins estimés
en mémoire et CPU.

## 13. Résumé

L'optimisation des performances est itérative :

1. **Mesurer** — établir une baseline avec des benchmarks
2. **Identifier** — trouver les goulots avec le profilage
3. **Régler** — ajuster `max_parallel`, profils de ressources, batch
4. **Mettre à l'échelle** — ajouter des workers, optimiser services externes
5. **Surveiller** — suivre les métriques en production
6. **Répéter** — l'optimisation ne s'arrête jamais

---

## Prochaines Étapes

- **[Guide de Suivi]({{ '/fr/guides/tracking/' | relative_url }})** — Surveiller les métriques des pipelines
- **[Guide Dataflow]({{ '/fr/guides/dataflow/' | relative_url }})** — Guide complet sur les pipelines DAG et l'architecture dataflow
- **[Guide API]({{ '/fr/guides/api/' | relative_url }})** — Construire des tableaux de bord pour la performance
- **[Exemple : Pipeline Audio Dataflow]({{ '/fr/examples/dataflow-audio-pipeline/' | relative_url }})** — Voir l'optimisation en action

---

 *Allez vite, mais mesurez d'abord.*
