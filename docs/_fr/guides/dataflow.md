---
title: Guide Dataflow
nav_order: 29
---

# Guide Dataflow

**Construire des pipelines complexes et parallèles grâce à l'orchestration basée sur les données**

> **Version** : {VERSION} | **Lié** : [Guide des Pipelines]({{ '/fr/guides/pipelines/' | relative_url }}), [Guide d'Exécution]({{ '/fr/guides/execution/' | relative_url }}), [Guide des Concepts Fondamentaux]({{ '/fr/guides/core-concepts/' | relative_url }})

---

## Aperçu

Le système **dataflow** de Taskiq-Flow est la méthode la plus puissante pour orchestrer des workflows complexes. Contrairement aux pipelines séquentiels où vous enchaînez manuellement les étapes, les pipelines dataflow construisent automatiquement un Graphe Orienté Acyclique (DAG) à partir de vos déclarations de tâches, permettant :

- **Résolution automatique des dépendances** — les tâches déclarent ce qu'elles produisent et consomment
- **Parallélisme automatique** — les tâches indépendantes s'exécutent simultanément sans configuration manuelle
- **Exécution pilotée par les données** — le flux de données détermine l'ordre d'exécution
- **Construction dynamique de pipelines** — ajouter des tâches flexiblement à l'exécution

Ce guide couvre l'ensemble du système dataflow : le DAG, le registre, les décorateurs, le moteur d'exécution et les patterns avancés.

---

## 1. Concepts Fondamentaux

### 1.1. Le paradigme Dataflow

Dans un pipeline dataflow, les tâches sont connectées par **dépendances de données** plutôt que par un ordre explicite :

```
Séquentiel :              Dataflow :
task1 → task2 → task3     task1 ──→ task2
                              └──→ task3  (parallèle !)
```

Chaque tâche déclare :
- **`output`** : La donnée produite (ex : `"features"`)
- **`inputs`** : Les données consommées (ex : `["features", "config"]`)

La bibliothèque résout automatiquement les dépendances et construit le graphe d'exécution.

### 1.2. Composants Clés

| Composant | Rôle | Module |
|-----------|------|--------|
| `@pipeline_task` | Décorateur pour déclarer les E/S d'une tâche | `taskiq_flow.decorators` |
| `DataNode` | Représente un artefact de données dans le graphe | `taskiq_flow.dataflow.node` |
| `DAG` / `DAGNode` | Structure de graphe pour le suivi des dépendances | `taskiq_flow.dataflow.dag` |
| `DataflowRegistry` | Registre central pour la construction du DAG | `taskiq_flow.dataflow.registry` |
| `DataCache` | Stocke les résultats intermédiaires pendant l'exécution | `taskiq_flow.dataflow.cache` |
| `DataflowPipeline` | Pipeline de haut niveau avec orchestration dataflow | `taskiq_flow.pipeline` |
| `ExecutionEngine` | Exécuteur DAG bas niveau avec parallélisme | `taskiq_flow.execution_engine` |

---

## 2. Déclaration des Tâches Dataflow

### 2.1. Le décorateur `@pipeline_task`

Marquez une fonction comme tâche de pipeline avec un output explicite :

```python
from taskiq import InMemoryBroker
from taskiq_flow import pipeline_task, DataflowPipeline

broker = InMemoryBroker()

@broker.task
@pipeline_task(output="features")
async def extract_features(paths: list[str]) -> dict:
    """Extrait les caractéristiques audio depuis des chemins de fichiers."""
    return {"tempo": 120.0, "energy": 0.8}
```

**Paramètres :**

| Paramètre | Type | Défaut | Description |
|------------|------|--------|-------------|
| `output` | `str` | **requis** | Nom de la donnée produite par cette tâche |
| `inputs` | `list[str]` | `None` (inféré) | Noms des données consommées |
| `retries` | `int` | `0` | Nombre de tentatives en cas d'échec |
| `retry_delay` | `float` | `1.0` | Délai initial entre les essais (secondes) |
| `retry_backoff` | `float` | `2.0` | Multiplicateur du délai entre les essais |
| `retry_jitter` | `bool` | `True` | Ajouter un jitter aléatoire aux délais |
| `max_retry_time` | `int` | `300` | Temps maximum total pour les retries (secondes) |
| `resources` | `dict` | `{}` | Estimation des ressources (memory, cpu) |

### 2.2. Inférence automatique des entrées

Si `inputs` n'est pas spécifié, les noms sont inférés depuis la signature de la fonction :

```python
@broker.task
@pipeline_task(output="stats")
def compute_stats(features: dict, config: dict) -> dict:
    # inputs automatiquement inférés : ["features", "config"]
    return {"count": len(features)}
```

Les paramètres nommés `self`, `cls`, ou ceux ayant une valeur par défaut sont exclus.

### 2.3. Sorties Multiples

Utilisez `@pipeline_task_multi_output` lorsqu'une tâche produit plusieurs artefacts :

```python
from taskiq_flow.decorators import pipeline_task_multi_output

@broker.task
@pipeline_task_multi_output(
    outputs={"features": dict, "metadata": dict},
    retries=2
)
async def process_audio(path: str) -> dict:
    features = extract(path)
    meta = get_metadata(path)
    return {
        "features": features,  # → sortie "features"
        "metadata": meta,       # → sortie "metadata"
    }
```

La **première clé** est la sortie principale ; toutes les clés sont enregistrées comme sorties nommées.

---

## 3. Construction des Pipelines Dataflow

### 3.1. `DataflowPipeline.from_tasks()`

L'approche recommandée pour la plupart des cas :

```python
pipeline = DataflowPipeline.from_tasks(
    broker,
    [extract_features, compute_stats, generate_report]
)
```

Le DAG est construit automatiquement :
- `extract_features` produit `"features"` — aucune dépendance → Niveau 0
- `compute_stats` consomme `"features"` → dépend de `extract_features` → Niveau 1
- `generate_report` consomme `"stats"` → dépend de `compute_stats` → Niveau 2

### 3.2. Ajout dynamique de tâches

```python
pipeline = DataflowPipeline(broker)
pipeline.add_dataflow_task(extract_features)
pipeline.add_dataflow_task(compute_stats)
pipeline.add_dataflow_task(generate_report)

# Le DAG est reconstruit paresseusement à la première exécution
results = await pipeline.kiq_dataflow(paths=["chanson.mp3"])
```

### 3.3. Fan-Out / Fan-In (Dépendances Multiples)

Des tâches peuvent consommer plusieurs sorties, et plusieurs tâches peuvent partager une dépendance :

```python
@broker.task
@pipeline_task(output="audio")
def load_audio(path: str) -> dict: ...

@broker.task
@pipeline_task(output="transcription")
def transcribe(audio: dict) -> str: ...

@broker.task
@pipeline_task(output="tags")
def generate_tags(audio: dict) -> list[str]: ...  # parallèle avec transcribe

@broker.task
@pipeline_task(output="report")
def create_report(
    transcription: str,
    tags: list[str]
) -> dict: ...  # fan-in : attend les deux

pipeline = DataflowPipeline.from_tasks(
    broker,
    [load_audio, transcribe, generate_tags, create_report]
)
# DAG :
#   load_audio → (transcribe ∥ generate_tags) → create_report
```

### 3.4. Entrées Externes

Passez des données à l'exécution qui ne sont produites par aucune tâche :

```python
results = await pipeline.kiq_dataflow(
    user_id="user_123",        # entrée externe
    config={"mode": "fast"}    # entrée externe
)
```

Les entrées externes sont identifiées automatiquement — ce sont les paramètres sans producteur correspondant.

---

## 4. Construction et Inspection du DAG

### 4.1. Le `DataflowRegistry`

Pour des cas avancés, construisez des DAGs manuellement :

```python
from taskiq_flow import DataflowRegistry

registry = DataflowRegistry()

# Enregistrez les tâches avec leurs E/S explicites
registry.register_task(
    task=load_data,
    output="raw_data",
    inputs=["source_url"]  # entrée externe
)
registry.register_task(
    task=clean_data,
    output="clean_data",
    inputs=["raw_data"]
)
registry.register_task(
    task=save_data,
    output="saved",
    inputs=["clean_data"]
)

# Inspectez le graphe
print("Tâches:", [t.task_name for t in registry.get_tasks()])
print("Sorties:", registry.get_outputs())
print("Entrées externes:", registry.get_external_inputs())

# Construisez le DAG
dag = registry.build_dag()
dag.print()
```

### 4.2. Méthodes d'Inspection du DAG

```python
from taskiq_flow.dataflow import DAG, DAGNode

# Ordre topologique
ordered = dag.topological_sort()
for node in ordered:
    print(f"Niveau {node.level}: {node.task_name}")

# Groupes d'exécution parallèle
dag.compute_levels()
for i, level in enumerate(dag.levels):
    names = [n.task_name for n in level]
    print(f"Niveau {i} (parallèle): {names}")

# Tâches prêtes étant donné un ensemble complété
ready = dag.get_ready_tasks(completed={node_a})

# Visualisation (nécessite networkx)
from taskiq_flow.visualization import DAGVisualizer
viz = DAGVisualizer(dag)
print(viz.to_json())
print(viz.to_graphviz())
print(viz.visualize_ascii())
```

### 4.3. Chemin Critique et Groupes Parallèles

```python
from taskiq_flow.visualization import DAGVisualizer

viz = DAGVisualizer(dag)

# Chemin critique = chaîne d'exécution la plus longue
critical = viz.detect_critical_path()
print(f"Chemin critique: {' → '.join(critical)}")

# Groupes de tâches pouvant s'exécuter en parallèle
groups = viz.find_parallelizable_groups()
for i, group in enumerate(groups):
    print(f"Groupe parallèle {i}: {group}")
```

---

## 5. Exécution

### 5.1. Lancer des Pipelines Dataflow

```python
# Exécuter et récupérer toutes les sorties
results = await pipeline.kiq_dataflow(track_paths=["chanson1.mp3", "chanson2.mp3"])
print(results)
# {"audio_features": {...}, "mir_features": {...}, "tags": [...], "embedding": [...]}
```

### 5.2. Le Moteur d'Exécution

`DataflowPipeline` utilise `ExecutionEngine` en interne pour l'exécution basée sur le DAG :

```python
from taskiq_flow import ExecutionEngine

# Exécution personnalisée avec contrôle fin
engine = ExecutionEngine(
    broker=broker,
    dag=dag,
    max_parallel=10,
    error_mode=ErrorHandlingMode.CONTINUE_ON_ERROR,
    resource_aware=True,
)

outputs = await engine.execute(
    inputs={"source_file": "data.csv"},
    pipeline_id="mon_pipeline"
)
```

**Fonctionnalités d'exécution :**
- Ordre topologique — les tâches s'exécutent après leurs dépendances
- Exécution parallèle — les tâches indépendantes tournent simultanément
- Retry par tâche — configuré via `@pipeline_task(retries=N)`
- Modes d'erreur — `FAIL_FAST`, `CONTINUE_ON_ERROR`, `SKIP_FAILED`
- Parallélisme basé sur les ressources — ajuste la concurrence selon CPU/mémoire

### 5.3. Opérations Map-Reduce

Pour le traitement par lots dans les pipelines :

```python
from taskiq_flow import MapReduce

# Map parallèle
mapped = await MapReduce.map(
    broker,
    process_item,
    items=list(range(100)),
    output="processed",
    max_parallel=10,
)

# Agrégation
result = await MapReduce.reduce(
    broker,
    aggregate_results,
    mapped.results,
    output="final",
    initial=0,
)

# Map-reduce combiné
final = await MapReduce.map_reduce(
    broker,
    map_task=process_item,
    reduce_task=aggregate_results,
    items=list(range(1000)),
    max_parallel=20,
    reduce_chunk_size=100,
)
```

**Fonctionnalités map :**
- Parallélisme automatique avec `asyncio`
- Chunking intelligent pour les grands ensembles
- Callbacks de progression
- Collecte des erreurs avec taux de réussite

### 5.4. Map/Reduce au Niveau Pipeline

`DataflowPipeline` intègre les opérations map-reduce :

```python
pipeline = DataflowPipeline.from_tasks(
    broker, [extract_features]
)

# Ajouter une opération map (traitement parallèle de nombreux éléments)
pipeline.map(
    process_track,
    track_list,
    output="track_features",
    max_parallel=10,
)

# Ajouter une opération reduce (agrégation)
pipeline.reduce(
    aggregate_features,
    input_name="track_features",
    output="playlist_stats",
)

# Exécuter
results = await pipeline.kiq_map_reduce(track_list=tracks)
```

---

## 6. Combiner Pipelines Séquentiels et Dataflow

### 6.1. Pattern Hybride

Utilisez des pipelines séquentiels pour les flux linéaires et dataflow pour les sous-workflows complexes :

```python
# Coquille séquentielle
main_pipeline = Pipeline(broker)

@broker.task
def run_dataflow_subset(data: list) -> dict:
    # Pipeline dataflow interne
    sub_pipeline = DataflowPipeline.from_tasks(
        broker,
        [task_a, task_b, task_c]
    )
    return await sub_pipeline.kiq_dataflow(data=data)

main_pipeline.call_next(run_dataflow_subset).call_next(finalize)
```

### 6.2. Planification de Pipelines

Planifiez des pipelines dataflow avec cron ou intervalles :

```python
from taskiq_flow import PipelineScheduler

scheduler = PipelineScheduler(broker)

# Planification cron
await scheduler.schedule(
    pipeline,
    cron="0 2 * * *",  # Quotidien à 2h du matin
    kwargs={"paths": ["daily_files/*.mp3"]}
)

# Planification par intervalle
await scheduler.schedule(
    pipeline,
    interval_seconds=3600,  # Toutes les heures
    label="hourly_analysis"
)
```

---

## 7. Cache et Résultats Intermédiaires

### 7.1. `DataCache`

Le `DataCache` stocke les résultats intermédiaires pendant l'exécution du pipeline :

```python
from taskiq_flow.dataflow.cache import DataCache

cache = DataCache()

# Stocker des résultats
cache.set("features", {"tempo": 120.0})
cache.set("tags", ["electronic", "dance"])

# Récupérer
features = cache.get("features")

# Vérifier l'existence
if cache.has("embedding"):
    embedding = cache.get("embedding")

# Injection automatique de dépendances pour les tâches
inputs = cache.inject(["features", "tags"])
# → {"features": {...}, "tags": [...]}

# Effacer entre les exécutions
cache.clear()
```

---

## 8. Gestion des Erreurs dans Dataflow

### 8.1. Modes d'Erreur

```python
from taskiq_flow.errors import ErrorHandlingMode

# Arrêter au premier erreur (défaut)
engine = ExecutionEngine(broker, dag, error_mode=ErrorHandlingMode.FAIL_FAST)

# Continuer malgré les erreurs
engine = ExecutionEngine(broker, dag, error_mode=ErrorHandlingMode.CONTINUE_ON_ERROR)

# Ignorer les tâches échouées
engine = ExecutionEngine(broker, dag, error_mode=ErrorHandlingMode.SKIP_FAILED)
```

### 8.2. Configuration des Retries

Configurez les retries au niveau de chaque tâche :

```python
@broker.task
@pipeline_task(
    output="reliable_feature",
    retries=3,
    retry_delay=2.0,
    retry_backoff=2.0,
)
def fetch_with_retry(url: str) -> dict:
    # Sera réessayé jusqu'à 3 fois avec backoff exponentiel
    ...
```

---

## 9. Gestion des Ressources

### 9.1. Parallélisme Basé sur les Ressources

Contrôlez le parallélisme en fonction de l'utilisation estimée des ressources :

```python
from taskiq_flow.optimization.parallel import ResourceAwareExecutor

executor = ResourceAwareExecutor(
    max_cpu_percent=80.0,
    max_memory_percent=80.0,
    min_parallel=1,
    max_parallel=10,
)

engine = ExecutionEngine(
    broker,
    dag,
    resource_aware=True,
    resource_profiles={
        "heavy_task": {"estimated_memory_mb": 500, "estimated_cpu_cores": 2.0},
        "light_task": {"estimated_memory_mb": 50, "estimated_cpu_cores": 0.5},
    },
)
```

---

## 10. Visualisation

### 10.1. Visualisation Intégrée

```python
# ASCII dans la console
pipeline.print_dag()

# JSON pour interfaces web
viz = pipeline.visualize()  # → {"nodes": [...], "edges": [...], "levels": [...]}

# DOT pour Graphviz
dot = pipeline.visualize_dot()
# Rendu : dot -Tpng pipeline.dot -o pipeline.png
```

### 10.2. Visualisation Avancée (nécessite networkx)

```python
from taskiq_flow.visualization import DAGVisualizer

viz = DAGVisualizer(dag)

# Formats d'exportation
viz.to_json()           # JSON pour frontend
viz.to_cytoscape_json() # Format Cytoscape.js
viz.to_graphviz()       # Format DOT
viz.visualize_ascii()   # Art ASCII

# Analyse
viz.detect_critical_path()      # Chemin d'exécution le plus long
viz.find_parallelizable_groups()  # Tâches exécutables en parallèle
```

### 10.3. Diagrammes Mermaid

```python
from taskiq_flow.visualization import MermaidGenerator

mermaid = MermaidGenerator(dag)
print(mermaid.to_mermaid())           # Diagramme basique
print(mermaid.to_mermaid_with_styling())  # Stylisé avec couleurs
```

---

## 11. Exemple Complet : Pipeline de Traitement Audio

```python
import asyncio
from taskiq import InMemoryBroker
from taskiq_flow import DataflowPipeline, pipeline_task

broker = InMemoryBroker()

@broker.task
@pipeline_task(output="audio_features")
async def extract_audio(paths: list[str]) -> dict:
    return {"duration": 180.0, "tempo": 120.0}

@broker.task
@pipeline_task(output="mir_features")
async def compute_mir(audio_features: dict) -> dict:
    return {"key": "C major", "loudness": -12.5}

@broker.task
@pipeline_task(output="tags")
async def generate_tags(mir_features: dict) -> list[str]:
    return ["electronic", "dance"]

@broker.task
@pipeline_task(output="embedding")
async def create_embedding(
    mir_features: dict,
    tags: list[str]
) -> list[float]:
    return [0.1, 0.2, 0.3, 0.4, 0.5]

async def main():
    pipeline = DataflowPipeline.from_tasks(
        broker,
        [extract_audio, compute_mir, generate_tags, create_embedding]
    )

    # Inspecter avant exécution
    pipeline.print_dag()

    # Exécuter
    results = await pipeline.kiq_dataflow(
        paths=["track1.mp3", "track2.mp3"]
    )

    print(results)
    # {
    #   "audio_features": {"duration": 180.0, "tempo": 120.0},
    #   "mir_features": {"key": "C major", "loudness": -12.5},
    #   "tags": ["electronic", "dance"],
    #   "embedding": [0.1, 0.2, 0.3, 0.4, 0.5]
    # }

asyncio.run(main())
```

---

## 12. Pièges Courants

| Symptôme | Cause | Solution |
|----------|-------|----------|
| Toutes les tâches s'exécutent séquentiellement | Utilisation de `Pipeline` au lieu de `DataflowPipeline` | Passer à `DataflowPipeline` |
| Erreurs "sortie manquante" | `@pipeline_task(output=...)` ne correspond pas au paramètre en aval | Aligner le nom de sortie avec le nom du paramètre |
| "No DAG built" | `kiq_dataflow()` appelé sans tâches ajoutées | Ajouter des tâches via `from_tasks()` ou `add_dataflow_task()` |
| Tâches exécutées deux fois | Mélange de `.call_next()` et dépendances `@pipeline_task` | Utiliser une seule approche de manière cohérente |
| Deadlock détecté | Dépendance circulaire dans le flux de données | Redéfinir avec un flux de données avant uniquement |
| Explosion mémoire | Trop de tâches parallèles | Définir `max_parallel` ou utiliser le mode resource-aware |

---

## 13. Conseils de Performance

1. **Limiter le parallélisme** — Utiliser `max_parallel` pour contrôler le nombre de tâches simultanées
2. **Utiliser map-reduce pour les lots** — `MapReduce.map()` avec chunking pour les grands ensembles
3. **Profiler l'utilisation des ressources** — Définir `resource_profiles` pour le parallélisme adaptatif
4. **Éviter les résultats intermédiaires volumineux** — Streamer les données quand c'est possible
5. **Réutiliser le DAG** — Construire le DAG une fois, exécuter plusieurs fois avec des entrées différentes

---

## 14. Résumé de l'API

### `DataflowPipeline`

| Méthode | Description |
|---------|-------------|
| `from_tasks(broker, tasks)` | Créer un pipeline à partir d'une liste de tâches |
| `add_dataflow_task(task)` | Ajouter une tâche dynamiquement |
| `kiq_dataflow(**inputs)` | Exécuter le pipeline avec orchestration dataflow |
| `kiq_map_reduce(**inputs)` | Exécuter en mode map-reduce |
| `kiq_map_reduce_advanced(...)` | Exécution map-reduce avancée avec options complètes |
| `kiq_map_sweep(task, param_values, ...)` | Balayage multi-dimensionnel de paramètres |
| `visualize()` | Obtenir le DAG en JSON |
| `visualize_dot()` | Obtenir le DAG au format DOT |
| `print_dag()` | Affichage ASCII dans la console |
| `schedule_with_cron(scheduler, label, cron, **inputs)` | Planification avec expression cron |
| `schedule_with_labels(scheduler, label, ...)` | Planification avec LabelBasedScheduler |
| `map(task, items, output, ...)` | Ajouter une opération map |
| `reduce(task, input_name, output, ...)` | Ajouter une opération reduce |

### `DataflowRegistry`

| Méthode | Description |
|---------|-------------|
| `register_task(task, output, inputs, **meta)` | Enregistrer une tâche avec ses métadonnées E/S |
| `build_dag()` | Construire le DAG à partir des tâches enregistrées |
| `get_producer(data_name)` | Trouver la tâche productrice pour une donnée |
| `get_consumers(data_name)` | Trouver les tâches consommatrices d'une donnée |
| `get_external_inputs()` | Lister les entrées externes |
| `get_outputs()` | Lister toutes les sorties |
| `get_tasks()` | Lister toutes les tâches enregistrées |

### `ExecutionEngine`

| Méthode | Description |
|---------|-------------|
| `execute(inputs, pipeline_id)` | Exécuter le DAG avec les entrées fournies |
| `get_execution_report()` | Obtenir les statistiques d'exécution |

### `MapReduce`

| Méthode | Description |
|---------|-------------|
| `map(broker, task, items, output, ...)` | Opération map en parallèle |
| `reduce(broker, task, inputs, output, ...)` | Opération de réduction |
| `map_reduce(broker, map_task, reduce_task, items, ...)` | Map + reduce combinés |
| `map_sweep(broker, task, param_values, output, ...)` | Balayage multi-dimensionnel |

---

*Maîtrisez le dataflow pour construire des workflows complexes et parallèles. Pour les patterns séquentiels, voir le [Guide des Pipelines]({{ '/fr/guides/pipelines/' | relative_url }}).*