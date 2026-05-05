---
title: Référence API: Composants Principaux
nav_order: 30
---
# Référence API: Composants Principaux

**Pipeline, DataflowPipeline, PipelineMiddleware, PipelineContext et exceptions principales**

> **Version** : 0.3.2 | **Module** : `taskiq_flow.core`, `taskiq_flow.pipeline`, `taskiq_flow.middleware`

---

## Classes Principales

### Pipeline (SequentialPipeline)

Le pipeline séquentiel classique pour l'orchestration linéaire de tâches.

```python
from taskiq_flow import Pipeline

pipeline = Pipeline(broker)
```

**Constructeur**:
```python
Pipeline(
    broker: BaseBroker,
    max_parallel: int = None,   # Limite globale de parallélisme
    timeout: float = None,      # Timeout global en secondes
    pipeline_id: str = None     # Auto-généré si non fourni
)
```

**Méthodes**:

| Méthode | Signature | Description |
|---------|-----------|-------------|
| `call_next` | `call_next(task, *args, **kwargs) -> Pipeline` | Enchaîne une tâche; passe résultat précédent comme premier arg |
| `call_after` | `call_after(task, *args, **kwargs) -> Pipeline` | Exécute tâche sans consommer résultat précédent |
| `map` | `map(task, max_parallel=None, output_name=None) -> Pipeline` | Applique tâche à chaque élément d'un résultat itérable |
| `filter` | `filter(task) -> Pipeline` | Garde éléments où tâche retourne truthy |
| `group` | `group(tasks, param_names=None) -> Pipeline` | Exécute multiples tâches en parallèle depuis même entrée |
| `kiq` | `kiq(*args, **kwargs) -> Task` | Démarre exécution pipeline |
| `with_tracking` | `with_tracking(tracking_manager) -> Pipeline` | Attache gestionnaire de suivi |
| `with_hooks` | `with_hooks(hook_manager) -> Pipeline` | Attache gestionnaire hooks pour événements |
| `with_retry` | `with_retry(...) -> Pipeline` | Configure politique de retry |
| `with_timeout` | `with_timeout(seconds) -> Pipeline` | Définit timeout |
| `with_context` | `with_context(enable=True) -> Pipeline` | Active passage PipelineContext aux tâches |

**Exemple**:
```python
pipeline = (
    Pipeline(broker)
    .call_next(tache1)
    .call_next(tache2, facteur=2)
    .map(tache3, max_parallel=10)
    .filter(valider)
    .with_tracking(suivi)
)
résultat = await pipeline.kiq(entrée_initiale)
```

---

### DataflowPipeline

Construction automatique de DAG depuis dépendances entre tâches via décorateurs `@pipeline_task`.

```python
from taskiq_flow import DataflowPipeline

pipeline = DataflowPipeline.from_tasks(
    broker,
    [tache_a, tache_b, tache_c]
)
```

**Constructeur**:
```python
DataflowPipeline(
    broker: BaseBroker,
    tasks: list[Callable] = None,
    max_parallel: int = None,
    timeout: float = None,
    pipeline_id: str = None
)
```

**Méthodes de Classe**:

| Méthode | Description |
|----------|-------------|
| `from_tasks(broker, tasks, **kwargs)` | Construit pipeline depuis liste de fonctions de tâche avec décorateurs `@pipeline_task` |

**Méthodes d'Instance** (la plupart partagées avec `Pipeline`):

| Méthode | Description |
|----------|-------------|
| `print_dag()` | Affiche DAG ASCII en console |
| `visualize()` | Retourne représentation JSON du DAG |
| `visualize_dot()` | Retourne chaîne DOT Graphviz |
| `kiq_dataflow(**kwargs)` | Exécute pipeline avec entrées nommées |

**Exemple**:
```python
@broker.task
@pipeline_task(output="features")
def extract(données): ...

@broker.task
@pipeline_task(output="tags")
def tag(features): ...

pipeline = DataflowPipeline.from_tasks(broker, [extract, tag])
pipeline.print_dag()
# Sortie:
# Niveau 0: extract
# Niveau 1: tag

résultats = await pipeline.kiq_dataflow(data=données_entrée)
# résultats = {"features": ..., "tags": ...}
```

---

### PipelineMiddleware

Le middleware qui orchestre l'exécution des étapes de pipeline.

```python
from taskiq_flow import PipelineMiddleware

broker.add_middlewares(PipelineMiddleware())
```

**Responsabilités**:

- Intercepte completion des tâches
- Détermine prochaine étape à exécuter
- Gère transitions d'état du pipeline
- Passe résultats entre étapes
- Émet événements hooks

**Note** : Ce middleware **doit** être ajouté au broker pour que tout pipeline fonctionne.

---

### PipelineContext

Métadonnées passées aux tâches quand `with_context(enable=True)` est défini.

```python
from taskiq_flow import PipelineContext

@broker.task
async def ma_tache(données: str, context: PipelineContext):
    print(f"Pipeline: {context.pipeline_id}")
    print(f"Étape: {context.step_index}")
    print(f"Tâche ID: {context.task_id}")
```

**Champs**:

| Champ | Type | Description |
|-------|------|-------------|
| `pipeline_id` | `str` | ID unique instance pipeline |
| `step_index` | `int` | Numéro étape courante (0-indexé) |
| `task_id` | `str` | ID tâche taskiq sous-jacente |
| `execution_mode` | `str` | `"sequential"`, `"parallel"`, `"map_reduce"` |
| `started_at` | `datetime` | Horodatage début pipeline |
| `broker` | `BaseBroker` | Référence instance broker |

---

## Exceptions Principales

Toutes exceptions héritent de classe de base `TaskiqFlowError`.

```python
from taskiq_flow import TaskiqFlowError
```

| Exception | Signification | Cause Typique |
|-----------|---------------|---------------|
| `PipelineError` | Échec générique pipeline | Étape échouée |
| `CycleError` | Dépendance circulaire détectée | DAG a cycle |
| `TaskNotFoundError` | Tâche non dans registry | Tâche manquante dans DataflowPipeline |
| `InvalidOutputError` | Conflit clé de sortie | Deux tâches déclarent même sortie |
| `ConfigurationError` | Config pipeline invalide | Middleware manquant, paramètres incorrects |
| `TrackingError` | Échec opération suivi | Stockage indisponible |

**Exemple gestion**:
```python
try:
    résultat = await pipeline.kiq(données)
except CycleError as e:
    print(f"Cycle DAG détecté: {e}")
except PipelineError as e:
    print(f"Pipeline échoué: {e}")
```

---

## Utilitaires

### DataflowRegistry

Pour construction manuelle de DAG et inspection.

```python
from taskiq_flow import DataflowRegistry

registry = DataflowRegistry()
registry.register_task(tache, output="sortie", inputs=["entrée"])
dag = registry.build_dag()
```

Voir documentation détaillée dans `docs/fr/api/dataflow.md`.

---

### ExecutionEngine

Exécuteur de DAG bas niveau pour cas avancés.

```python
from taskiq_flow import ExecutionEngine

engine = ExecutionEngine(broker, dag)
résultats = await engine.execute(inputs={"x": 1, "y": 2})
```

Voir API docs execution.

---

### PipelineScheduler

Planification cron de pipelines.

```python
from taskiq_flow import PipelineScheduler

scheduler = PipelineScheduler(broker)
await scheduler.schedule(pipeline, cron="* * * * *")
await scheduler.start()
```

Voir guide planification.

---

## Compatibilité Version

Cette documentation couvre **Taskiq-Flow v0.3.0+**.

Stabilité API:
- `Pipeline` et `DataflowPipeline`: Stable (v0.3+)
- Décorateur `pipeline_task`: Stable (v0.3+)
- `PipelineMiddleware`: Stable (v0.3+)
- `PipelineScheduler`: Stable (v0.3+)
- `PipelineTrackingManager`: Stable (v0.3+)

Changements cassants notés dans [CHANGELOG.md](https://github.com/SoniqueBay/taskiq-flow/blob/main/CHANGELOG.md).

---

*Pour exemples détaillés, voir section [Exemples]({{ '/fr/examples/' | relative_url }}). Pour doc méthode par méthode, se référer aux docstrings Python inline (`help(Pipeline)`).*
