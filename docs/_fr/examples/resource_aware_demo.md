---
permalink: /fr/examples/resource-aware-demo/
title: Exemple: resource_aware_demo.py
nav_order: 49
color_scheme: dark
---
# Exemple: resource_aware_demo.py

**Parallélisme dynamique basé sur CPU/mémoire**

> **Version** : {VERSION} | **Fichier** : `examples/resource_aware_demo.py`

---

## Aperçu

Cet exemple démontre les fonctionnalités `ResourceAwareExecutor` et `TaskResourceProfile` introduites en v0.4.5. Il montre comment :

- Annoter tâches avec besoins ressources (CPU, mémoire, I/O vs CPU)
- Calculer parallélisme optimal basé sur ressources système courantes
- Ajuster `max_parallel` dynamiquement pour ne pas surcharger l'hôte
- Appliquer différentes stratégies de parallélisme pour tâches I/O-bound vs CPU-bound

---

## Ce Que Cet Exemple Montre

- Définition de `TaskResourceProfile` pour tâches
- Création d'un `ResourceAwareExecutor` avec limites système
- Interrogation de `get_optimal_parallelism()` pour types de tâches
- Utilisation de profils ressource dans DataflowPipeline
- Directives de réglage manuel de parallélisme

---

## Parcours Du Code

### 1. Configuration Resource-Aware Executor

```python
from taskiq_flow.optimization import ResourceAwareExecutor, TaskResourceProfile

executor = ResourceAwareExecutor(
    max_cpu_percent=80.0,   # Ne pas dépasser 80% d'usage CPU
    max_memory_percent=80.0,  # Ne pas dépasser 80% de RAM
    min_parallel=1,
    max_parallel=20,
)

# Interroger parallélisme optimal pour une estimation de tâche
optimal_light = executor.get_optimal_parallelism(
    task_memory_estimate=50,   # 50 MB par tâche
    task_cpu_estimate=0.2,     # 0.2 cores par tâche
)
print(f"Optimal pour tâches légères: {optimal_light}")

optimal_heavy = executor.get_optimal_parallelism(
    task_memory_estimate=200,  # 200 MB par tâche
    task_cpu_estimate=1.0,     # 1.0 core par tâche
)
print(f"Optimal pour tâches lourdes: {optimal_heavy}")
```

L'exécuteur interroge l'usage système courant (via `psutil`) et calcule combien de tâches du profil donné peuvent s'exécuter en parallèle sans dépasser les limites configurées.

---

### 2. Annoter Tâches avec Profils Ressources

```python
@broker.task
@pipeline_task(
    output="light_result",
    resources=TaskResourceProfile(
        estimated_memory_mb=50,
        estimated_cpu_cores=0.2,
        io_bound=True,
    ),
)
async def light_task(item: int) -> dict:
    await asyncio.sleep(0.1)  # Simule I/O
    return {"item": item, "result": item * 2}

@broker.task
@pipeline_task(
    output="heavy_result",
    resources=TaskResourceProfile(
        estimated_memory_mb=200,
        estimated_cpu_cores=1.0,
        io_bound=False,
    ),
)
async def heavy_task(item: int) -> dict:
    total = 0
    for _ in range(100000):
        total += item * 2
    return {"item": item, "result": total}
```

**Champs ResourceProfile :**

- `estimated_memory_mb`: Usage mémoire estimé par instance de tâche
- `estimated_cpu_cores`: Cores CPU requis (0.5 = demi-core)
- `io_bound`: True pour tâches I/O-heavy (réseau, disque), False pour CPU-heavy

---

### 3. Utiliser Profils Ressources dans Pipelines

Le paramètre `max_parallel` de `DataflowPipeline` agit comme borne supérieure. `ResourceAwareExecutor` peut calculer un `max_parallel` dynamique avant lancement :

```python
# Calculer parallélisme optimal pour état système courant
current_parallel = executor.get_optimal_parallelism(
    task_memory_estimate=50,
    task_cpu_estimate=0.2,
)

pipeline = DataflowPipeline(broker, max_parallel=current_parallel)
pipeline.map(light_task, items=list(range(20)), output="light_results")
results = await pipeline.kiq_dataflow()
```

Pour charges de travail mixtes, sommez l'usage ressource à travers tâches parallèles.

---

### 4. Directives de Réglage Manuel Parallélisme

```python
import psutil

cpu_count = psutil.cpu_count() or 4
memory_gb = psutil.virtual_memory().total / (1024 ** 3)

# Tâches I/O-bound : pouvez oversubscribe CPU (passent du temps en attente)
io_parallel = min(50, cpu_count * 5)

# Tâches CPU-bound : limitez aux cores disponibles ± petite marge
cpu_parallel = min(cpu_count + 2, 20)

print(f"max_parallel recommandé pour tâches I/O-bound: {io_parallel}")
print(f"max_parallel recommandé pour tâches CPU-bound: {cpu_parallel}")
```

Commencez conservateur, benchmarkez, et ajustez.

---

## Sortie Attendue

```
=== Resource-Aware Parallelism Demo ===

Current system state:
  CPU Usage: ? (will query at runtime)
  Memory: ? (will query at runtime)

--- Light tasks (I/O bound) ---
  Optimal parallelism for light tasks: 25

--- Heavy tasks (CPU bound) ---
  Optimal parallelism for heavy tasks: 4

Note: Actual values depend on current system load.


=== Pipeline with Resource-Aware Execution ===

Pipeline structure:
  [items:20] --light_task--> [light_results]
  [items:10] --heavy_task--> [heavy_results]
  [light_results, heavy_results] --combine--> [final]

Executing pipeline...
✅ Pipeline completed: {'light_results': [...], 'heavy_results': [...], 'final': {...}}

TaskResourceProfile allows you to annotate tasks with resource requirements.
ResourceAwareExecutor uses these profiles to compute optimal parallelism.


=== Manual Parallelism Tuning ===

System: 8 CPU cores, 15.6 GB RAM
Recommended max_parallel for I/O-bound tasks: 40
Recommended max_parallel for CPU-bound tasks: 10
Start with conservative values and benchmark:
  pipeline.map(light_task, items, max_parallel=10)
  pipeline.map(heavy_task, items, max_parallel=cpu_count)


=== Resource-Aware Demo Complete ===

Key takeaways:
1. Use TaskResourceProfile to annotate task resource needs
2. ResourceAwareExecutor computes optimal parallelism at runtime
3. Adjust max_parallel based on task type (I/O vs CPU)
4. Monitor system resources and tune accordingly
```

---

## Points Clés

### Pourquoi Parallélisme Aware-Ressource ?

Sans conscience ressource, `max_parallel` trop haut peut :
- Épuiser mémoire → OOM kills
- Saturer CPU → tâches thrashing, ralentissement global
- Priver autres services sur même hôte

`ResourceAwareExecutor` empêche ça en interrogeant usage système courant et calculant niveaux de parallélisme sûrs.

### Meilleures Pratiques

1. **Profilez vos tâches** : Mesurez usage mémoire/CPU réel en production
2. **Valeurs par défaut conservatrices** : Commencez avec `max_parallel=5` et augmentez
3. **Monitorer** : Surveillez métriques système pendant exécution pipelines
4. **Ajustez par type de tâche** : Tâches I/O-bound peuvent être plus parallèles que CPU-bound
5. **Utilisez bornes `min_parallel` et `max_parallel`** : `ResourceAwareExecutor` respecte ces bornes

### Intégration avec Monitoring

Combinez avec métriques Prometheus :

```python
from taskiq_flow.metrics import MetricsMiddleware
broker.add_middlewares(MetricsMiddleware())
```

Suivez :
- `taskiq_flow_worker_cpu_usage_percent`
- `taskiq_flow_worker_memory_usage_bytes`
- `taskiq_flow_pipeline_executions_total`

---

## Chemin d'Apprentissage

Après cet exemple :

1. **[Guide Performance]({{ '/fr/guides/performance/' | relative_url }})** — Plongée profonde parallélisme aware-ressource
2. **[API Module Optimization]({{ '/fr/api/optimization/' | relative_url }})** — Référence complète `ResourceAwareExecutor`
3. **[Guide Suivi]({{ '/fr/guides/tracking/' | relative_url }})** — Monitorer usage ressource dans le temps

---

*Cet exemple empêche vos pipelines de submerger l'hôte. Testez toujours les profils ressource avec volumes de données réalistes.*
