---
title: API d'Optimisation
nav_order: 35
permalink: /fr/api/optimization/
color_scheme: dark
---
# API d'Optimisation

**Parallélisme aware-ressource et optimisation d'exécution**

> **Version** : {VERSION} | **Module** : `taskiq_flow.optimization`, `taskiq_flow.optimization.parallel`

---

## Aperçu

Le module `taskiq_flow.optimization` fournit des outils pour optimiser l'exécution de pipeline basée sur les ressources système. Il aide à éviter de surcharger l'hôte en ajustant dynamiquement le parallélisme.

Composants principaux :

- **`ResourceAwareExecutor`** — Calcule le parallélisme optimal selon contraintes CPU/mémoire
- **`TaskResourceProfile`** — Annotate tâches avec besoins ressources
- **`get_default_executor()`** — Retourne un exécuteur singleton avec paramètres système par défaut

---

## ResourceAwareExecutor

```python
from taskiq_flow.optimization import ResourceAwareExecutor

executor = ResourceAwareExecutor(
    max_cpu_percent=80.0,      # Usage CPU max autorisé (pourcentage)
    max_memory_percent=80.0,   # Usage mémoire max autorisé (pourcentage)
    min_parallel=1,            # Plancher parallélisme minimum
    max_parallel=100,          # Plafond parallélisme maximum
)
```

### Méthodes

#### `get_optimal_parallelism(task_memory_estimate: int, task_cpu_estimate: float) -> int`

Calcule le nombre max d'instances de tâches concurrentes qui tiennent dans les limites de ressources.

**Paramètres :**
- `task_memory_estimate` — Mémoire attendue par tâche (MB)
- `task_cpu_estimate` — Cores CPU attendus par tâche (0.5 = demi-core)

**Retour :** Nombre optimal d'instances parallèles

**Exemple :**

```python
optimal = executor.get_optimal_parallelism(
    task_memory_estimate=100,   # 100 MB par tâche
    task_cpu_estimate=0.5,      # 0.5 core par tâche
)
print(f"Peut exécuter jusqu'à {optimal} tâches en parallèle")
```

L'exécuteur interroge `psutil` pour l'usage système courant et calcule la capacité restante.

---

## TaskResourceProfile

```python
from taskiq_flow.optimization import TaskResourceProfile

profile = TaskResourceProfile(
    estimated_memory_mb=256,     # Mémoire needed par tâche
    estimated_cpu_cores=1.0,     # Cores CPU nécessaires
    io_bound=False,              # True = attente I/O, False = intensif CPU
)
```

Utilisation avec `@pipeline_task` :

```python
@broker.task
@pipeline_task(
    output="result",
    resources=TaskResourceProfile(
        estimated_memory_mb=512,
        estimated_cpu_cores=2.0,
        io_bound=False,
    ),
)
async def heavy_computation(data: dict) -> dict:
    ...
```

### Champs

| Champ | Type | Description |
|-------|------|-------------|
| `estimated_memory_mb` | int | RAM attendue par instance de tâche |
| `estimated_cpu_cores` | float | Cores CPU requis (0.25, 0.5, 1.0, etc.) |
| `io_bound` | bool | `True` si la tâche attend (réseau/disque), `False` si CPU-intensive |

---

## get_default_executor

```python
from taskiq_flow.optimization import get_default_executor

executor = get_default_executor()
# Retourne un singleton ResourceAwareExecutor avec paramètres par défaut
```

Pratique pour un accès rapide sans configuration manuelle.

---

## Intégration avec DataflowPipeline

Passez `max_parallel` calculé par l'exécuteur à votre pipeline :

```python
from taskiq_flow import DataflowPipeline

executor = ResourceAwareExecutor()
optimal_parallel = executor.get_optimal_parallelism(
    task_memory_estimate=50,
    task_cpu_estimate=0.2,
)

pipeline = DataflowPipeline(broker, max_parallel=optimal_parallel)
pipeline.map(light_task, items, output="results")
results = await pipeline.kiq_dataflow()
```

Pour charges de travail mixtes, calculez un `max_parallel` sûr qui accommode le type de tâche le plus gourmand en ressources.

---

## Meilleures Pratiques

1. **Profilez vos tâches** — Mesurez mémoire/CPU réels en production
2. **Défauts conservateurs** — Commencez avec `max_parallel=5` et augmentez graduellement
3. **Monitorez métriques système** — Surveillez `psutil.cpu_percent()` et `memory.percent` durant exécution
4. **Différenciez types de tâches** — Tâches I/O-bound peuvent avoir `max_parallel` plus élevé que CPU-bound
5. **Utilisez bornes** — `ResourceAwareExecutor` respecte `min_parallel` et `max_parallel`

---

## Lié

- **[Guide Performance]({{ '/fr/guides/performance/' | relative_url }})** — Discussion approfondie parallélisme aware-ressource
- **[Guide Suivi]({{ '/fr/guides/tracking/' | relative_url }})** — Monitorer usage ressources dans le temps
- **[Exemple : Resource-Aware Demo]({{ '/fr/examples/resource-aware-demo/' | relative_url }})** — Démo complète fonctionnelle

---

*Le module d'optimisation assure que les pipelines s'adaptent sans submerger l'hôte. Testez toujours les profils ressource avec volumes de données réalistes.*
