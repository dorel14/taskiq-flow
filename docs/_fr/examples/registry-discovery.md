---
title: Exemple : registry_discovery_example.py
nav_order: 43
---
# Exemple : registry_discovery_example.py

**Construction manuelle de DataflowRegistry, inspection du DAG et exécution pas-à-pas**

> **Version** : 0.3.2 | **Fichier** : `examples/registry_discovery_example.py`

---

## Aperçu

Cet exemple avancé démontre les mécanismes internes du système de résolution automatique de dépendances de Taskiq-Flow utilisant `DataflowRegistry`. Il montre comment :

- Enregistrer manuellement les tâches avec leurs déclarations E/S
- Inspecter le graphe de flux de données avant exécution
- Construire et valider un DAG
- Exécuter des pipelines en utilisant directement `ExecutionEngine`
- Comprendre la provenance des données et les dépendances des tâches

**C'est le mécanisme central derrière `DataflowPipeline.from_tasks()`.**

---

## Ce Que Cet Exemple Montre

- API complète de `DataflowRegistry`
- Construction manuelle de DAG à partir des métadonnées de tâche
- Interrogation des dépendances (producteurs/consommateurs)
- Tri topologique et détection de niveaux parallèles
- Exécution directe via `ExecutionEngine`
- Le `DataCache` pour l'exécution manuelle étape par étape
- Détection d'erreurs (dépendances manquantes, cycles)

---

## Parcours du Code

### Définition des Tâches (identique au style dataflow_audio)

```python
from taskiq_flow.dataflow.registry import DataflowRegistry
from taskiq_flow.execution_engine import ExecutionEngine
from taskiq_flow.dataflow.cache import DataCache
from taskiq_flow.visualization import DAGVisualizer

@broker.task
@pipeline_task(output="raw_data")
async def load_data(source: str) -> dict:
    return {"source": source, "records": [...]}

@broker.task
@pipeline_task(output="cleaned_data")
async def clean_data(raw_data: dict) -> dict:
    records = [r for r in raw_data["records"] if r["value"] > 0]
    return {"source": raw_data["source"], "records": records}

@broker.task
@pipeline_task(output="features")
async def extract_features(cleaned_data:dict) -> dict:
    total = sum(r["value"] for r in cleaned_data["records"])
    return {"total": total, "count": len(cleaned_data["records"])}

@broker.task
@pipeline_task(output="report")
async def generate_report(features: dict) -> dict:
    return {"report_id": "RPT-001", "summary": features}
```

---

## Exemple 1 : Construction Manuelle du Registre & Inspection

```python
async def example_manual_registry():
    registry = DataflowRegistry()

    # Enregistrer les tâches manuellement
    registry.register_task(load_data, output="raw_data", inputs=["source"])
    registry.register_task(clean_data, output="cleaned_data", inputs=["raw_data"])
    registry.register_task(extract_features, output="features", inputs=["cleaned_data"])
    registry.register_task(generate_report, output="report", inputs=["features"])

    # Inspecter le registre
    print(f"Tâches: {[t.task_name for t in registry.get_tasks()]}")
    # ['load_data', 'clean_data', 'extract_features', 'generate_report']

    # Interroger les dépendances
    deps = registry.get_data_dependencies(generate_report)
    print(f"generate_report dépend de: {deps}")  # ['features']

    # Trouver qui produit 'features'
    producer = registry.get_producer("features")
    print(f"'features' produit par: {producer.task_name}")  # extract_features

    # Trouver qui consomme 'raw_data'
    consumers = registry.get_consumers("raw_data")
    print(f"'raw_data' consommé par: {[c.task_name for c in consumers]}")  # [clean_data]

    # Entrées externes (non produites par une tâche)
    external = registry.get_external_inputs()
    print(f"Entrées externes: {external}")  # ['source']

    # Sorties (résultats finaux)
    outputs = registry.get_outputs()
    print(f"Sorties du pipeline: {outputs}")  # toutes les sorties
```

**Méthodes clés** :

| Méthode | Retourne |
|---------|----------|
| `get_tasks()` | Tous les objets `TaskNode` enregistrés |
| `get_outputs()` | Toutes les clés de sortie |
| `get_external_inputs()` | Entrées non produites par une tâche |
| `get_producer(output_key)` | Tâche produisant cette sortie |
| `get_consumers(input_key)` | Tâches consommant cette entrée |
| `get_data_dependencies(task)` | Liste des clés d'entrée pour une tâche |

---

## Exemple 2 : Construction et Visualisation du DAG

```python
    # Construction du DAG
    dag = registry.build_dag()

    print(f"DAG: {len(dag.nodes)} nœuds, {len(dag.edges)} arêtes")

    # Ordre d'exécution (tri topologique)
    order = dag.topological_sort()
    for i, node in enumerate(order):
        print(f"{i+1}. {node.task_name}")

    # Niveaux d'exécution parallèle
    for level_idx, level_nodes in enumerate(dag.levels):
        tasks = [n.task_name for n in level_nodes]
        print(f"Niveau {level_idx}: {tasks}")

    # Visualisation ASCII
    dag.print()

    # Format DOT
    dot = DAGVisualizer.to_dot(dag)
    with open("pipeline.dot", "w") as f:
        f.write(dot)
```

**Propriétés DAG** :
- `dag.nodes` — Tous les nœuds
- `dag.edges` — Arêtes de dépendance
- `dag.roots` — Nœuds sans dépendances
- `dag.leaves` — Nœuds sans dépendants
- `dag.levels` — Groupes de tâches exécutables en parallèle
- `dag.topological_sort()` — Ordre d'exécution linéaire

---

## Exemple 3 : Validation & Détection d'Erreurs

```python
async def example_validation():
    registry = DataflowRegistry()
    registry.register_task(load_data, output="raw_data", inputs=["source"])

    # Cassé : dépend d'une sortie inexistante
    @broker.task
    @pipeline_task(output="result")
    async def broken_task(nonexistent_data: dict):
        return {"result": "broken"}

    registry.register_task(broken_task, output="result", inputs=["nonexistent_data"])

    try:
        dag = registry.build_dag()  # Lève ValueError
    except ValueError as e:
        print(f"Erreur attendue capturée: {e}")
        # "La tâche 'broken_task' requiert l'entrée 'nonexistent_data' mais aucune tâche ne la produit"
```

**Validations effectuées** :
- Toutes les entrées déclarées doivent être produites par une tâche (ou être externes)
- Pas de dépendances circulaires (cycles)
- Pas de noms de sortie en double

---

## Exemple 4 : Exécution avec ExecutionEngine

```python
async def example_execution_with_engine():
    registry = DataflowRegistry()
    registry.register_task(load_data, output="raw_data", inputs=["source"])
    registry.register_task(clean_data, output="cleaned_data", inputs=["raw_data"])
    registry.register_task(extract_features, output="features", inputs=["cleaned_data"])
    registry.register_task(generate_report, output="report", inputs=["features"])

    dag = registry.build_dag()

    engine = ExecutionEngine(
        broker=broker,
        dag=dag,
        fail_fast=True,
        max_parallel=4,
    )

    results = await engine.execute(
        inputs={"source": "local://data/file.csv"},
        pipeline_id="manual_pipeline_example",
    )

    # results = {"raw_data": ..., "cleaned_data": ..., "features": ..., "report": ...}
```

`ExecutionEngine` est l'exécuteur de bas niveau qui exécute un DAG.

---

## Exemple 5 : Exécution Pas-à-Pas Manuelle avec DataCache

Montre la boucle d'exécution interne :

```python
async def example_manual_execution_with_cache():
    registry = DataflowRegistry()
    # enregistrer les tâches...
    dag = registry.build_dag()

    cache = DataCache()

    # Initialiser les entrées externes
    cache.set("source", "local://data/file.csv")

    completed_nodes = set()

    while True:
        ready = dag.get_ready_tasks(completed_nodes)
        if not ready:
            break

        for node in ready:
            task = node.task
            deps = registry.get_data_dependencies(task)

            # Injection des dépendances depuis le cache
            args = cache.inject(deps)  # {'raw_data': {...}, ...}

            # Exécution de la tâche
            result = await task.kiq(**args)
            output_value = (await result.wait_result()).return_value

            # Stockage de la sortie dans le cache
            output_name = registry.get_task_metadata(task)["output"]
            cache.set(output_name, output_value)

            completed_nodes.add(node)

    # Sorties finales dans le cache
    final_report = cache.get("report")
```

---

## Pourquoi C'Important

Comprendre `DataflowRegistry` vous aide à :

1. **Déboguer des pipelines complexes** — Inspecter le DAG avant exécution
2. **Construire des pipelines dynamiques** — Assembler des pipelines à la volée selon la configuration
3. **Implémenter une orchestration personnalisée** — Utiliser `ExecutionEngine` directement
4. **Comprendre la provenance des données** — Tracer l'origine de chaque sortie

---

## Chemin d'Apprentissage

Après cet exemple :

1. **[Guide Dataflow]({{ '/fr/guides/pipelines.md#2-dataflow-pipeline' | relative_url }})** — Utilisation haut niveau
2. **[API ExecutionEngine]({{ '/fr/api/execution/' | relative_url }})** — Contrôle d'exécution bas niveau
3. **[DAGBuilder]({{ '/fr/api/execution.md#dagbuilder' | relative_url }})** — Construction programmatique de DAG

---

 *Sujet avancé. La plupart des utilisateurs utiliseront `DataflowPipeline.from_tasks()` qui encapsule ce registry en interne. Explorez ceci uniquement si vous avez besoin de construction dynamique de pipelines.*
