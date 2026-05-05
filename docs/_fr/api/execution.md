---
title: Référence API : Moteur d'Exécution
nav_order: 32
---
# Référence API : Moteur d'Exécution

**ExecutionEngine, DAG, DAGNode, DAGBuilder et MapReduce**

> **Version** : 0.3.2 | **Module** : `taskiq_flow.execution_engine`, `taskiq_flow.dataflow.dag`, `taskiq_flow.map_reduce`

---

## ExecutionEngine

Moteur de bas niveau pour exécuter des DAGs directement, évitant l'abstraction Pipeline.

```python
from taskiq_flow import ExecutionEngine, DataflowRegistry

# Construire le registre manuellement
registry = DataflowRegistry()
registry.register_task(load, output="raw", inputs=[])
registry.register_task(process, output="clean", inputs=["raw"])
registry.register_task(save, output="saved", inputs=["clean"])

# Construire le DAG
dag = registry.build_dag()

# Créer le moteur
engine = ExecutionEngine(broker, dag)

# Exécuter
results = await engine.execute(inputs={"source": "data.csv"})
# results = {"raw": ..., "clean": ..., "saved": ...}
```

**Constructeur** :
```python
ExecutionEngine(
    broker: BaseBroker,
    dag: DAG,
    max_parallel: int = None,
    on_step_complete: callable = None
)
```

**Méthodes** :

| Méthode | Signature | Description |
|---------|-----------|-------------|
| `execute` | `execute(inputs: dict) -> dict` | Exécute le DAG avec les entrées données |
| `execute_async` | `execute_async(inputs: dict) -> AsyncIterator` | Stream les résultats au fur et à mesure |
| `cancel` | `cancel()` | Arrête l'exécution en cours |

**Événements** :

```python
async def on_step(task_name: str, result: Any):
    print(f"Étape {task_name} terminée")

engine = ExecutionEngine(broker, dag, on_step_complete=on_step)
```

---

## DAG (Directed Acyclic Graph)

Représente le graphe d'exécution des tâches.

```python
from taskiq_flow.dataflow import DAG, DAGNode

dag = DAG()
node = DAGNode(task=my_task, output="result", inputs=["input_a"])
dag.add_node(node)
```

**Méthodes DAG** :

| Méthode | Description |
|---------|-------------|
| `add_node(node: DAGNode)` | Ajoute un nœud tâche |
| `add_edge(from_task, to_task)` | Ajoute une dépendance |
| `topological_sort() -> list[DAGNode]` | Retourne l'ordre d'exécution |
| `get_parallel_levels() -> list[list[DAGNode]]` | Groupe les nœuds par niveau d'exécution parallèle |
| `validate()` | Vérifie cycles, nœuds manquants |
| `print()` | Visualisation ASCII vers console |

**Propriétés DAG** :

| Propriété | Type | Description |
|-----------|------|-------------|
| `nodes` | `list[DAGNode]` | Tous les nœuds du graphe |
| `edges` | `set[tuple[DAGNode, DAGNode]]` | Arêtes de dépendance |
| `roots` | `list[DAGNode]` | Nœuds sans dépendances |
| `leaves` | `list[DAGNode]` | Nœuds sans dépendants |

---

## DAGNode

Représente une tâche unique dans le DAG avec sa spécification E/S.

```python
from taskiq_flow.dataflow import DAGNode

node = DAGNode(
    task=my_task_function,
    output="result_key",
    inputs=["input_a", "input_b"],
    metadata={"description": "Ma tâche"}
)
```

**Propriétés** :

| Propriété | Type | Description |
|-----------|------|-------------|
| `task` | `Callable` | La fonction tâche |
| `task_name` | `str` | Nom auto-généré ou personnalisé |
| `output` | `str` | Clé de sortie (unique) |
| `outputs` | `list[str]` | Clés de sortie (multiples) |
| `inputs` | `list[str]` | Clés d'entrée requises |
| `metadata` | `dict` | Métadonnées arbitraires |

---

## DAGBuilder

Helper pour construire des DAGs par programmation (moins courant ; utilisez généralement DataflowRegistry).

```python
from taskiq_flow import DAGBuilder

builder = DAGBuilder()
builder.add_task(task1, output="a", inputs=[])
builder.add_task(task2, output="b", inputs=["a"])
builder.add_task(task3, output="c", inputs=["a", "b"])

dag = builder.build()
```

**Pattern Builder** :

```python
dag = (DAGBuilder()
    .node(load, output="raw", inputs=[])
    .node(process, output="clean", inputs=["raw"])
    .node(save, output="saved", inputs=["clean"])
    .build()
)
```

---

## MapReduce

Utilitaire pour map parallèle suivi d'un reduce.

### MapReduce.map

```python
from taskiq_flow import MapReduce

mapped = await MapReduce.map(
    broker,
    map_func,          # Fonction tâche à appliquer
    items: Iterable,   # Éléments à traiter
    output: str = "mapped",
    max_parallel: int = None
)
# Retourne : MapReduceResult (comme une Task)
```

### MapReduce.reduce

```python
reduced = await MapReduce.reduce(
    broker,
    reduce_func,       # Fonction d'agrégation
    mapped_result,     # Résultat de MapReduce.map
    input_name: str,   # Nom de la sortie mappée à consommer
    output: str = "reduced"
)
# Retourne : Task (avec résultat final)
```

### MapReduce.map_reduce (combiné)

```python
final = await MapReduce.map_reduce(
    broker,
    map_func,
    items,
    reduce_func,
    map_output="mapped",
    reduce_output="final",
    max_parallel=10
)
```

Les trois retournent des objets Task ; appelez `.wait_result()` pour récupérer la valeur.

---

## DataflowRegistry (Avancé)

Enregistrement manuel des tâches pour construction dynamique de pipeline.

```python
from taskiq_flow import DataflowRegistry

registry = DataflowRegistry()

# Enregistrer les tâches avec E/S explicites
registry.register_task(
    task=extract,
    output="features",
    inputs=["audio_files"]  # entrée externe
)
registry.register_task(
    task=tag,
    output="tags",
    inputs=["features"]  # dépend de la sortie de extract
)

# Inspection
print("Tâches:", [t.task_name for t in registry.get_tasks()])
print("Sorties:", registry.get_outputs())
print("Entrées externes:", registry.get_external_inputs())

# Construction du DAG
dag = registry.build_dag()
dag.print()

# Exécution via ExecutionEngine
engine = ExecutionEngine(broker, dag)
results = await engine.execute(inputs={"audio_files": files})
```

**Requêtes Registry** :

| Méthode | Description |
|---------|-------------|
| `get_tasks()` | Liste tous les objets TaskNode |
| `get_outputs()` | Liste toutes les clés de sortie |
| `get_external_inputs()` | Liste les entrées non produites par une tâche |
| `get_producer(output_key)` | Retourne la tâche produisant cette sortie |
| `get_consumers(input_key)` | Liste les tâches consommant cette entrée |
| `build_dag()` | Construit le DAG, valide, retourne prêt à exécuter |

---

## Notes de Version

- **ExecutionEngine** introduit en v0.3.0
- `DAG` et `DAGNode` sont utilisés en interne par DataflowPipeline
- MapReduce disponible depuis v0.2.0

---

## Prochaines Étapes

- **[API Suivi]({{ '/fr/api/tracking/' | relative_url }})** — Surveiller l'exécution avec PipelineTrackingManager
- **[API WebSocket]({{ '/fr/api/websocket/' | relative_url }})** — HookManager et système d'événements
- **[API Core]({{ '/fr/api/core/' | relative_url }})** — Référence Pipeline et middleware

---

*Pour cas avancés uniquement. 95% des utilisateurs devraient se contenter des abstractions Pipeline et DataflowPipeline.*
