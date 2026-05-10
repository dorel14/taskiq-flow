---
title: Guide des Pipelines
nav_order: 20
---
# Guide des Pipelines

**Motifs de pipelines séquentiels et dataflow, configurations et bonnes pratiques**

> **Version** : {VERSION} | **Lié** : [Guide d'Exécution]({{ '/fr/guides/execution/' | relative_url }}), [Guide des Tâches]({{ '/fr/guides/tasks/' | relative_url }}), [Guide Dataflow]({{ '/fr/guides/dataflow/' | relative_url }})

---

## Aperçu

Taskiq-Flow propose deux types principaux de pipelines pour orchestrer des workflows de tâches：

1. **SequentialPipeline** — Enchaînement manuel des étapes pour des workflows linéaires
2. **DataflowPipeline** — Construction automatique de DAG depuis les dépendances entre tâches

Pour une exploration approfondie des patterns dataflow, voir le [Guide Dataflow]({{ '/fr/guides/dataflow/' | relative_url }}).

Ce guide explore les deux types, leurs cas d'usage, et comment choisir entre eux.

---

## 1. Pipeline Séquentiel

Le modèle classique où vous enchaînez explicitement les étapes dans l'ordre.

### 1.1. Structure de Base

```python
from taskiq_flow import Pipeline

pipeline = (
    Pipeline(broker)
    .call_next(task1)
    .call_next(task2)
    .call_next(task3)
)
```

**Exécution** : `task1 → task2 → task3` (synchroniquement)

### 1.2. Opérations Disponibles

#### `.call_next(task, *args, **kwargs)`

Exécute une tâche, passant le résultat précédent comme premier argument：

```python
pipeline.call_next(process_data).call_next(save_result)
# process_data receives output of previous step
# save_result receives output of process_data
```

**Parameter binding**:
- By position: result becomes first argument
- By name: `pipeline.call_next(task, param_name=previous_result)`

Example:
```python
@broker.task
def multiply(value: int, factor: int) -> int:
    return value * factor

pipeline.call_next(add_one).call_next(multiply, factor=3)
# add_one output → multiply(value=...), factor=3
```

#### `.call_after(task, *args, **kwargs)`

Execute a task **without** consuming the previous result (fire-and-forget within pipeline):

```python
pipeline.call_next(process).call_after(log_completion)
# log_completion runs after process but doesn't receive process's output
```

Useful for side effects (logging, notifications) that shouldn't transform the data flow.

#### `.map(task, max_parallel=None)`

Apply a task to each element of an iterable result in parallel:

```python
# Previous step returned: [1, 2, 3, 4]
pipeline.map(process_item)
# Runs process_item(1), process_item(2), ... concurrently
# Collects results: [processed1, processed2, ...]
```

**Options**:
- `max_parallel=10` — limit concurrent executions
- `output_name="results"` — custom output key (default: task output name)

#### `.filter(task)`

Keep elements where the task returns truthy:

```python
# Previous step returned: [1, 2, 3, 4]
pipeline.filter(is_even)
# Keeps elements where is_even(element) returns True
# Result: [2, 4]
```

#### `.group(tasks, param_names=None)`

Execute multiple independent tasks in parallel, starting from the same input:

```python
pipeline.group(
    [task_a, task_b, task_c],
    param_names=["x", "y", "z"]  # bind input to these parameters
)
# All three tasks receive the same previous result
# Returns: [result_a, result_b, result_c]
```

---

## 2. Pipeline Dataflow

> Pour un guide complet sur les patterns dataflow, voir le [Guide Dataflow]({{ '/fr/guides/dataflow/' | relative_url }}).

Construction automatique de DAG via annotations `@pipeline_task(output=...)`.

### 2.1. Déclaration des Sorties de Tâche

```python
from taskiq_flow import pipeline_task, DataflowPipeline

@broker.task
@pipeline_task(output="features")
def extract_features(data: list[str]) -> dict:
    return {"count": len(data)}

@broker.task
@pipeline_task(output="stats")
def compute_stats(features: dict) -> dict:
    return {"entries": features["count"] * 2}

@broker.task
@pipeline_task(output="report")
def generate_report(stats: dict) -> str:
    return f"Stats: {stats}"
```

**Key**: The `output` parameter declares what this task produces. Downstream tasks declare matching parameter names to consume those outputs.

### 2.2. Building the Pipeline

```python
pipeline = DataflowPipeline.from_tasks(
    broker,
    [extract_features, compute_stats, generate_report]
)
```

**Automatic dependency resolution**:

1. `extract_features` produces `features` — no dependencies
2. `compute_stats` needs `features` — depends on `extract_features`
3. `generate_report` needs `stats` — depends on `compute_stats`

**Resulting DAG**:
```
extract_features → compute_stats → generate_report
```

### 2.3. Multiple Consumers

Multiple tasks can consume the same output; they'll all wait for the producer:

```python
@broker.task
@pipeline_task(output="features")
def extract(data): ...

@broker.task
@pipeline_task(output="tags")
def tag(features: dict): ...   # consumer 1 of features

@broker.task
@pipeline_task(output="embedding")
def embed(features: dict): ... # consumer 2 of features
```

**Clé** : Le paramètre `output` déclare ce que cette tâche produit. Les tâches en aval déclarent des noms de paramètres correspondants pour consommer ces sorties.

### 2.2. Construction du Pipeline

```python
pipeline = DataflowPipeline.from_tasks(
    broker,
    [extract_features, compute_stats, generate_report]
)

**Automatic dependency resolution**:

1. `extract_features` produces `features` — no dependencies
2. `compute_stats` needs `features` — depends on `extract_features`
3. `generate_report` needs `stats` — depends on `compute_stats`

**Resulting DAG**:
```
extract_features → compute_stats → generate_report
```

**Résolution automatique des dépendances**：

1. `extraire_features` produit `features` — aucune dépendance
2. `calculer_stats` a besoin de `features` — dépend de `extraire_features`
3. `générer_rapport` a besoin de `stats` — dépend de `calculer_stats`

**DAG résultant**：
```
extraire_features → calculer_stats → générer_rapport
```

### 2.3. Multiple Consommateurs

Multiple tasks can consume the same output; they'll all wait for the producer:

```python
@broker.task
@pipeline_task(output="features")
def extract(data): ...

@broker.task
@pipeline_task(output="tags")
def tag(features: dict): ...   # consumer 1 of features

@broker.task
@pipeline_task(output="embedding")
def embed(features: dict): ... # consumer 2 of features
```

### 2.4. Paramètres d'Entrée

Les pipelines dataflow acceptent des entrées externes via `kiq_dataflow(**kwargs)`：

```python
résultats = await pipeline.kiq_dataflow(data=["fichier1.mp3", "fichier2.mp3"])
# Le paramètre `data` est apparié à toute tâche en ayant besoin
# Doit correspondre à un nom de paramètre d'une tâche sans producteur (entrée externe)
```

---

## 3. Configuration du Pipeline

### 3.1. Ajout du Suivi

```python
from taskiq_flow import PipelineTrackingManager

suivi = PipelineTrackingManager().with_auto_storage(broker)
pipeline = Pipeline(broker).with_tracking(suivi)
```

Voir [Guide de Suivi]({{ '/fr/guides/tracking/' | relative_url }}) pour plus de détails.

### 3.2. Définition d'un ID de Pipeline Personnalisé

```python
pipeline.pipeline_id = "my_workflow_001"
# If not set, a UUID is automatically generated
```

Important pour le suivi et les abonnements WebSocket.

### 3.3. Attachement des Hooks (WebSocket)

```python
from taskiq_flow.hooks import HookManager

hooks = HookManager()
pipeline = Pipeline(broker).with_hooks(hooks)
```

Voir [Guide WebSocket]({{ '/fr/guides/websocket/' | relative_url }}).

### 3.4. Retry & Politiques d'Erreur

```python
pipeline.with_retry(
    max_attempts=3,
    delay=1.0,
    backoff=2.0
)
pipeline.on_error("continue")  # ou "stop"
```

Voir [Guide de Retry]({{ '/fr/guides/retry/' | relative_url }}).

### 3.5. Timeouts

```python
pipeline.with_timeout(seconds=60)
```

---

## 4. Cycle de Vie du Pipeline

### 4.1. Création → Exécution → Completion

```
1.  pipeline = Pipeline(broker)           # Créer l'objet pipeline
2.  pipeline.call_next(...)               # Enchaîner les étapes
3.  task = await pipeline.kiq(entrée)      # Lancer
4.  résultat = await task.wait_result()   # Attendre & récupérer
```

### 4.2. Réutilisabilité

Les objets Pipeline sont **à usage unique**. Pour des exécutions répétées, créez un nouveau pipeline ou utilisez `PipelineScheduler`：

```python
# Correct: create a fresh pipeline each time
async def execute_workflow(data):
    pipeline = Pipeline(broker).call_next(step1).call_next(step2)
    return await pipeline.kiq(data)
```

---

## 5. Visualisation des Pipelines

### 5.1. DAG ASCII (Console)

```python
pipeline.print_dag()
```

Example output:
```
Execution Order DAG:
  Level 0: task_a
  Level 1: task_b, task_c
  Level 2: task_d
```

### 5.2. JSON for Web UIs

```python
viz = pipeline.visualize()  # returns a dict
print(viz)
```

Structure:
```json
{
  "nodes": [
    {"id": "task_a", "outputs": ["x", "y"]},
    {"id": "task_b", "inputs": ["x"]}
  ],
  "edges": [{"from": "task_a", "to": "task_b"}]
}
```

### 5.3. Format DOT (Graphviz)

```python
dot = pipeline.visualize_dot()
with open("pipeline.dot", "w") as f:
    f.write(dot)
# Rendre: dot -Tpng pipeline.dot -o pipeline.png
```

Le diagramme résultant montre les nœuds, liens et ordre d'exécution.

---

## 6. Inspection du Pipeline (DataflowRegistry)

For advanced use cases, manually construct and inspect the dataflow graph:

```python
from taskiq_flow import DataflowRegistry

registry = DataflowRegistry()

# Register tasks with explicit I/O
registry.register_task(
    task=load_data,
    output="raw",
    inputs=["source"]  # external input
)
registry.register_task(
    task=clean,
    output="clean",
    inputs=["raw"]
)
registry.register_task(
    task=save,
    output="saved",
    inputs=["clean"]
)

# Inspect structure
print("Tasks:", [t.task_name for t in registry.get_tasks()])
print("Outputs:", registry.get_outputs())           # ["raw", "clean", "saved"]
print("External inputs:", registry.get_external_inputs())  # ["source"]

# Find dependencies
producer = registry.get_producer("clean")   # returns TaskNode for 'clean'
consumers = registry.get_consumers("raw")   # list of tasks needing 'raw'

# Build DAG
dag = registry.build_dag()
dag.print()
order = dag.topological_sort()  # list of tasks in execution order
levels = dag.levels              # list of lists (parallel groups)
```

Voir `examples/registry_discovery_example.py` pour une utilisation complète.

---

## 7. Choix entre Types de Pipeline

| Critère | SequentialPipeline | DataflowPipeline |
|---------|-------------------|------------------|
| **Forme du workflow** | Linéaire, avec embranchements occasionnels | DAG complexe avec nombreuses branches |
| **Dépendances des tâches** | Implicites (ordre d'enchaînement) | Explicites (`@pipeline_task`) |
| **Parallélisme** | Manuel (`.group()`) | Automatique (tâches indépendantes) |
| **Flexibilité** | Contrôle total de l'ordre | Déclaratif ; la bibliothèque optimise |
| **Workflows dynamiques** | Difficile (fixé au moment de la construction) | Facile (peut ajouter des tâches flexiblement) |
| **Idéal pour** | ETL étapes linéaires, batch simple | Traitement audio/vidéo, pipelines ML |

**Règle empirique**：
- **SequentialPipeline** pour des workflows simples à ordre fixe
- **DataflowPipeline** pour des workflows complexes, ramifiés ou réutilisables

---

## 8. Bonnes Pratiques

### 8.1. Nommage des Tâches et Sorties

Utiliser des noms de sortie clairs et uniques：

```python
@pipeline_task(output="user_features")  # clair
@pipeline_task(output="features_2")     # ambigu (si plusieurs features existent)
```

### 8.2. Éviter les Dépendances Circulaires

DataflowPipeline détecte les cycles et lève `CycleError` pendant `build_dag()`. Concevoir avec un flux de données avant uniquement.

### 8.3. Minimiser l'État Partagé

Chaque tâche doit être pure (la sortie dépend uniquement des entrées) pour la sécurité en parallèle.

### 8.4. Versionner les IDs de Pipeline

Inclure la version dans les IDs de pipeline pour le suivi：

```python
pipeline.pipeline_id = f"analyse_audio_v1_{int(time.time())}"
```

### 8.5. Utiliser `.call_after()` pour les Effets Secondaires

Ne pas corrompre le flux de données avec logs/métriques：

```python
pipeline.call_next(processus).call_after(journaliser_résultat)  # correct
pipeline.call_next(processus_et_journaliser)                      # anti-pattern
```

### 8.6. Limiter le Parallélisme pour les Tâches Ressource-Intensives

```python
# Transcodage intensif en CPU
pipeline.map(transcoder, fichiers, max_parallel=2)
```

### 8.7. Valider le DAG Avant Exécution

```python
pipeline.print_dag()  # Toujours inspecter les pipelines complexes
input("Appuyer sur Entrée pour exécuter...")
```

---

## 9. Pièges Courants

| Symptôme | Cause probable | Correction |
|----------|----------------|------------|
| Tâche exécutée deux fois | `.call_next()` et tâche dépendante tous deux déclarés | Supprimer l'appel redondant; Dataflow gère les dépendances |
| Sortie manquante | `@pipeline_task(output=...)` ne correspond pas au paramètre en aval | Aligner le nom de sortie avec le nom du paramètre |
| Toutes les tâches séquentielles | Utilisation de Pipeline au lieu de DataflowPipeline | Passer à DataflowPipeline pour le parallélisme automatique |
| Résultats None | Oubli de `broker.add_middlewares(PipelineMiddleware())` | Ajouter le middleware avant de créer des pipelines |
| Pipeline stale réutilisé | Tentative d'appeler `kiq()` deux fois sur le même objet pipeline | Créer un pipeline frais par exécution |

---

## 10. Motifs Avancés

### 10.1. Hybride Séquentiel + Dataflow

Combiner les deux types pour un contrôle maximal：

```python
# Coquille séquentielle
séquentiel = Pipeline(broker)

# À l'intérieur d'une étape, lancer un sous-pipeline dataflow
@broker.task
async def traiter_lot(données: list) -> dict:
    sous_pipeline = DataflowPipeline.from_tasks(
        broker,
        [sous_tache1, sous_tache2, sous_tache3]
    )
    return await sous_pipeline.kiq_dataflow(data=données)

séquentiel.call_next(traiter_lot).call_next(finaliser)
```

### 10.2. Construction de Pipeline Dynamique

Construire des pipelines à l'exécution selon la configuration：

```python
def build_pipeline(config: dict) -> Pipeline:
    steps = []
    if config.get("preprocess"):
        steps.append(preprocess_task)
    if config.get("analyze"):
        steps.append(analyze_task)
    # ...
    pipeline = Pipeline(broker)
    for step in steps:
        pipeline.call_next(step)
    return pipeline
```

### 10.3. Branchement Conditionnel

Utiliser `.filter()` et les étapes de condition：

```python
high_value = pipeline.filter(is_high_value)
high_value.call_next(premium_processing)
low_value = pipeline.filter(is_low_value)
low_value.call_next(standard_processing)

# Merge
merged = high_value.group([premium_processing, standard_processing])
```

Voir [steps/condition.py](https://github.com/dorel14/taskiq-flow/blob/main/taskiq_flow/steps/condition.py) pour `IfStep`.

---

## 11. Checklist de Vérification

Avant d'exécuter un pipeline, vérifier :

- [ ] Type de pipeline choisi correctement (Séquentiel vs Dataflow)
- [ ] Toutes les fonctions décorées avec `@broker.task`
- [ ] Dataflow: toutes les tâches concernées décorées avec `@pipeline_task(output=…)`
- [ ] Les noms de sortie correspondent exactement aux noms de paramètres en aval
- [ ] `PipelineMiddleware` ajouté au broker
- [ ] `pipeline_id` défini si suivi/WebSocket nécessaire
- [ ] DAG inspecté avec `print_dag()` pour les pipelines complexes
- [ ] Limites de parallélisme (`max_parallel`) définies appropriément
- [ ] Timeouts configurés pour les tâches longues
- [ ] Exécution d'exemple réussie avant utilisation en production

---

## Lectures Complémentaires

- **[Guide d'Exécution]({{ '/fr/guides/execution/' | relative_url }})** — Comment les pipelines s'exécutent, gestion d'erreurs, timeouts
- **[Guide des Tâches]({{ '/fr/guides/tasks/' | relative_url }})** — Écriture des fonctions de tâche et décorateurs
- **[Exemples]({{ '/fr/examples/' | relative_url }})** — Démonstrations complètes de pipelines

---

*Maîtriser les pipelines pour orchestrer n'importe quel workflow. Ensuite, apprendre sur la [Définition des Tâches]({{ '/fr/guides/tasks/' | relative_url }}).*
