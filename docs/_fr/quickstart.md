---
title: Guide de Démarrage Rapide
nav_order: 10
color_scheme: dark
---
# Guide de Démarrage Rapide

**Se familiariser avec Taskiq-Flow en 5 minutes**

> **Version** : {VERSION} | **Prérequis** : Python 3.9+, bases d'asyncio

---

## Aperçu

Ce guide vous aidera à créer vos premiers pipelines avec Taskiq-Flow. À la fin, vous comprendrez :

- Comment configurer un broker et ajouter le PipelineMiddleware
- Définir des tâches avec `@broker.task`
- Construire des pipelines séquentiels avec `.call_next()`, `.map()`, `.filter()`
- Exécuter des pipelines et récupérer les résultats
- Les bases des pipelines dataflow avec `@pipeline_task`

---

## Prérequis

```bash
pip install taskiq taskiq-flow
```

Pour ce guide, nous utilisons le broker en mémoire qui ne nécessite aucun service externe.

---

## 1. Pipeline Séquentiel Basique

### 1.1. Configuration

Créez un fichier Python `quickstart_basic.py` :

```python
import asyncio
from taskiq import InMemoryBroker
from taskiq_flow import Pipeline, PipelineMiddleware

# Initialiser le broker et ajouter le middleware requis
broker = InMemoryBroker()
broker.add_middlewares(PipelineMiddleware())
```

### 1.2. Définir les Tâches

Toutes les fonctions dans un pipeline doivent être des tâches taskiq (décorées avec `@broker.task`) :

```python
@broker.task
def add_one(value: int) -> int:
    """Ajouter 1 à la valeur d'entrée."""
    return value + 1

@broker.task
def repeat(value: int, times: int) -> list[int]:
    """ Répéter une valeur plusieurs fois."""
    return [value] * times

@broker.task
def is_positive(value: int) -> bool:
    """Vérifier si la valeur est positive ou nulle."""
    return value >= 0
```

### 1.3. Construire et Exécuter le Pipeline

```python
async def main():
    # Construire le pipeline en enchaînant les opérations
    pipeline = (
        Pipeline(broker)
        .call_next(add_one)           # Étape 1: 1 → 2
        .call_next(repeat, times=4)   # Étape 2: 2 → [2, 2, 2, 2]
        .map(add_one)                  # Étape 3: appliquer à chaque élément → [3, 3, 3, 3]
        .filter(is_positive)           # Étape 4: garder les éléments où le résultat est True
    )

    # Lancer le pipeline avec une entrée initiale
    task = await pipeline.kiq(1)

    # Attendre la fin et récupérer le résultat
    result = await task.wait_result()
    print("Résultat :", result.return_value)  # Sortie: [3, 3, 3, 3]

asyncio.run(main())
```

**Sortie attendue** :
```
Résultat : [3, 3, 3, 3]
```

### 1.4. Comment Ça Marche

| Étape | Opération | Entrée | Sortie |
|-------|-----------|--------|--------|
| 1 | `.call_next(add_one)` | `1` | `2` |
| 2 | `.call_next(repeat, times=4)` | `2` | `[2, 2, 2, 2]` |
| 3 | `.map(add_one)` | `[2, 2, 2, 2]` | `[3, 3, 3, 3]` (parallèle) |
| 4 | `.filter(is_positive)` | `[3, 3, 3, 3]` | `[3, 3, 3, 3]` (inchangé) |

**Points clés** :

- Le `PipelineMiddleware` gère le routage des tâches ; il **doit** être ajouté au broker.
- Chaque étape reçoit la sortie de l'étape précédente comme entrée.
- `.map()` et `.filter()` opèrent sur des résultats itérables et exécutent les éléments en parallèle.
- `pipeline.kiq(entrée_initiale)` démarre le pipeline et renvoie un objet `Task`.
- `task.wait_result()` bloque jusqu'à la fin du pipeline.

---

## 2. Pipeline Dataflow (DAG Automatique)

Pour des workflows plus complexes, utilisez `DataflowPipeline` qui construit automatiquement un graphe de dépendances.

### 2.1. Définir des Tâches avec `@pipeline_task`

Marquez les sorties de tâche avec le décorateur `@pipeline_task` :

```python
from taskiq_flow import DataflowPipeline, pipeline_task

@broker.task
@pipeline_task(output="features")
def extract_audio(track_paths: list[str]) -> dict:
    """Extraire les caractéristiques audio des pistes."""
    print(f"Extraction des caractéristiques de {len(track_paths)} pistes...")
    return {"duration": 180.0, "tempo": 120.0, "energy": 0.8}

@broker.task
@pipeline_task(output="tags")
def generate_tags(features: dict) -> list[str]:
    """Générer des tags basés sur les caractéristiques audio."""
    print(f"Génération de tags depuis les caractéristiques : {features}")
    return ["electronic", "dance", "upbeat"]

@broker.task
@pipeline_task(output="embedding")
def compute_embedding(features: dict) -> list[float]:
    """Calculer l'incorporation vectorielle depuis les caractéristiques."""
    print(f"Calcul de l'incorporation depuis {features}")
    return [0.1, 0.2, 0.3, 0.4, 0.5]
```

**Fonctionnement de la résolution de dépendances** :
- `extract_audio` déclare `output="features"`
- `generate_tags` a le paramètre `features: dict` → dépend automatiquement de `extract_audio`
- `compute_embedding` dépend aussi de `extract_audio` (même paramètre `features`)
- Taskiq-Flow construit un DAG et exécute les tâches indépendantes en parallèle

### 2.2. Construire et Exécuter

```python
async def main():
    # Construire automatiquement le DAG depuis la liste de tâches
    pipeline = DataflowPipeline.from_tasks(
        broker,
        [extract_audio, generate_tags, compute_embedding]
    )

    # Optionnel: visualiser le DAG
    pipeline.print_dag()

    # Exécuter avec les données d'entrée (seulement les entrées externes nécessaires)
    results = await pipeline.kiq_dataflow(track_paths=["chanson1.mp3", "chanson2.mp3"])
    print("Résultats :", results)
    # Sortie: {
    #   "features": {"duration": 180.0, ...},
    #   "tags": ["electronic", "dance", "upbeat"],
    #   "embedding": [0.1, 0.2, 0.3, 0.4, 0.5]
    # }

asyncio.run(main())
```

**Exemple de sortie DAG** (affiché dans la console)：
```
Ordre d'Exécution DAG:
  Niveau 0 (parallèle): extract_audio
  Niveau 1 (parallèle): generate_tags, compute_embedding
  Sorties finales: features, tags, embedding
```

### 2.3. Visualiser le Pipeline

```python
# DAG ASCII dans la console
pipeline.print_dag()

# Représentation JSON pour interfaces web
viz_json = pipeline.visualize()
print(viz_json)

# Format DOT pour Graphviz
dot = pipeline.visualize_dot()
with open("pipeline.dot", "w") as f:
    f.write(dot)
# Rendre: dot -Tpng pipeline.dot -o pipeline.png
```

---

## 3. Motifs Courants

### 3.1. Motif Map-Reduce

Traiter des éléments en parallèle, puis agréger :

```python
from taskiq_flow import MapReduce

# Phase Map: traiter chaque piste indépendamment
mapped = await MapReduce.map(
    broker,
    process_track,          # fonction de tâche
    track_list,             # itérable d'éléments
    output="processed",     # nom de la sortie intermédiaire
    max_parallel=10         # limiter la concurrence
)

# Phase Reduce: agréger tous les résultats
reduced = await MapReduce.reduce(
    broker,
    aggregate_results,      # fonction d'agrégation
    mapped,                 # objet MapReduceResult
    input_name="processed", # consommer la sortie mappée
    output="final_stats"
)

print("Final :", reduced.return_value)
```

Voir `examples/dataflow_audio_pipeline.py` pour un pipeline audio complet.

### 3.2. Exécution Parallèle Groupée

Exécuter plusieurs tâches indépendantes simultanément :

```python
pipeline = Pipeline(broker)

pipeline.group(
    [task_a, task_b, task_c],
    param_names=["input_a", "input_b", "input_c"]
)
# Retourne : [resultat_a, resultat_b, resultat_c]
```

### 3.3. Pipeline avec Suivi

Surveiller le statut du pipeline en temps réel :

```python
from taskiq_flow import PipelineTrackingManager

tracking = PipelineTrackingManager().with_auto_storage(broker)
pipeline = Pipeline(broker).with_tracking(tracking)

task = await pipeline.kiq(données)

# Vérifier le statut ultérieurement
statut = await tracking.get_status(pipeline.pipeline_id)
print(f"Statut : {statut.status}, Étapes complétées : {len(statut.steps)}")
```

---

## 4. Exécuter les Exemples

Le répertoire `examples/` contient des démonstrations complètes exécutables :

```bash
# Pipeline séquentiel basique
python examples/quickstart.py

# Suivi et monitoring
python examples/tracking_demo.py

# Pipelines planifiés (cron)
python examples/scheduled_pipeline.py

# DAG dataflow complet avec map-reduce
python examples/dataflow_audio_pipeline.py

# Construction manuelle de DAG avec DataflowRegistry
python examples/registry_discovery_example.py

# Streaming d'événements WebSocket
python examples/websocket_demo.py

# API REST avec FastAPI
python examples/api_example.py
```

---

## 5. Prochaines Étapes

Avec les bases acquises, explorez les guides approfondis :

| Sujet | Guide |
|-------|-------|
| Pipelines séquentiels et dataflow | [Guide des Pipelines]({{ '/fr/guides/pipelines/' | relative_url }}) |
| **Approfondissement Dataflow** | **[Guide Dataflow]({{ '/fr/guides/dataflow/' | relative_url }})** |
| Définition des tâches et décorateurs | [Guide des Tâches]({{ '/fr/guides/tasks/' | relative_url }}) |
| Modes d'exécution et gestion d'erreurs | [Guide d'Exécution]({{ '/fr/guides/execution/' | relative_url }}) |
| Monitoring en temps réel | [Guide de Suivi]({{ '/fr/guides/tracking/' | relative_url }}) |
| Tableaux de bord en direct | [Guide WebSocket]({{ '/fr/guides/websocket/' | relative_url }}) |
| Planification cron | [Guide de Planification]({{ '/fr/guides/scheduling/' | relative_url }}) |
| Récupération d'erreurs | [Guide de Retry]({{ '/fr/guides/retry/' | relative_url }}) |
| Optimisation des performances | [Guide de Performance]({{ '/fr/guides/performance/' | relative_url }}) |
| Intégration API REST | [Guide API]({{ '/fr/guides/api/' | relative_url }}) |
| Référence API complète | [Référence API]({{ '/fr/api/' | relative_url }}) |

---

## Dépannage

### Erreur "PipelineMiddleware non trouvé"

**Symptôme** : Les tâches échouent avec des erreurs de middleware.

**Solution** : Assurez-vous que `PipelineMiddleware()` est ajouté au broker avant de créer des pipelines :

```python
broker.add_middlewares(PipelineMiddleware())  # Obligatoire
```

### Erreur "Task not found" ou "Result is None"

**Symptôme** : `wait_result()` retourne `None`.

**Cause** : InMemoryBroker fonctionne uniquement dans le même processus. Pour des setups multi-Workers distribués, utilisez Redis ou un broker persistant.

**Solution** : Passez à `RedisStreamBroker` avec un backend de résultats partagé :

```python
from taskiq_flow.broker import RedisStreamBroker
broker = RedisStreamBroker(redis_url="redis://localhost:6379")
```

### Connexion WebSocket Refusée

**Symptôme** : Le client ne peut pas se connecter au serveur WebSocket.

**Solution** : Assurez-vous que le serveur WebSocket est en cours d'exécution et que le port est accessible：

```python
server = get_websocket_server(host="0.0.0.0", port=8765)
await server.start_server()
```

---

## Lectures Complémentaires

- **[Référence API Complète]({{ '/fr/api/' | relative_url }})** — Documentation complète des classes et méthodes
- **[Galerie d'Exemples]({{ '/fr/examples/' | relative_url }})** — Explications détaillées de chaque script d'exemple
- **[README du Projet](https://github.com/dorel14/taskiq-flow/blob/main/README.fr.md)** — Vue d'ensemble, installation et philosophie

---

*Prêt à approfondir ? Continuez avec le [Guide des Pipelines]({{ '/fr/guides/pipelines/' | relative_url }}).*
