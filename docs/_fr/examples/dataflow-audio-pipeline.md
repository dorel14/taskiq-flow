---
permalink: /fr/examples/dataflow-audio-pipeline/
title: Exemple: dataflow_audio_pipeline.py
nav_order: 42
---
# Exemple: dataflow_audio_pipeline.py

**DAG dataflow complet avec exécution parallèle, map-reduce et visualisation**

> **Version** : 0.3.2 | **Fichier** : `examples/dataflow_audio_pipeline.py`

---

## Aperçu

Cet exemple exhaustif démontre toute la puissance de DataflowPipeline avec:

- Construction automatique de DAG depuis dépendances tâches
- Exécution parallèle de tâches indépendantes
- Motif map-reduce pour traitement par lots
- Visualisation de pipeline (DOT, JSON, ASCII)
- Workflows mixtes séquentiels et parallèles

C'est l'exemple de référence pour comprendre l'architecture dataflow.

---

## Ce Que Cet Exemple Montre

- Utilisation du décorateur **`@pipeline_task`** avec sorties simples et multiples
- **Résolution automatique de dépendances** — les tâches déclarent leurs sorties; tâches en aval consomment par nom de paramètre
- **Exécution parallèle** — tâches avec même dépendance s'exécutent concurremment
- **Motif map-reduce** — traitement batch avec `.map()` et `.reduce()`
- **Visualisation DAG** — affichage ASCII, export DOT, JSON

---

## Explication du Code

### Définition des Tâches

```python
from taskiq import InMemoryBroker
from taskiq_flow import DataflowPipeline, pipeline_task

broker = InMemoryBroker(await_inplace=True)

# Tâche 1: Extraire caractéristiques audio (aucune dépendance)
@broker.task
@pipeline_task(output="audio_features")
async def extract_audio_features(track_paths: list[str]) -> dict:
    features = {...}
    return features

# Tâche 2: Calculer features MIR (dépend de audio_features)
@broker.task
@pipeline_task(output="mir_features")
async def compute_mir_features(audio_features: dict) -> dict:
    # Reçoit audio_features automatiquement
    return {...}

# Tâche 3: Générer tags (dépend de mir_features)
@broker.task
@pipeline_task(output="tags")
async def generate_tags(mir_features: dict) -> list[str]:
    return ["electronic", "dance"]

# Tâche 4: Créer embedding (dépend de mir_features ET tags)
@broker.task
@pipeline_task(output="vector")
async def create_embedding(mir_features: dict, tags: list[str]) -> list[float]:
    # Reçoit les deux entrées automatiquement
    return [0.1, 0.5, 0.8]
```

Le pipeline construit automatiquement ce DAG:
```
extract_audio_features → compute_mir_features → generate_tags
                            ↓
                         create_embedding (après compute_mir_features, parallèle à generate_tags)
```

---

## Exemple 1: Pipeline Séquentiel avec Dépendances Automatiques

```python
async def example_sequential_pipeline():
    pipeline = DataflowPipeline.from_tasks(
        broker,
        [
            extract_audio_features,
            compute_mir_features,
            generate_tags,
            create_embedding,
        ],
    )

    pipeline.print_dag()
    # Sortie:
    # Ordre Exécution DAG:
    #   Niveau 0 (parallèle): extract_audio_features
    #   Niveau 1 (parallèle): compute_mir_features
    #   Niveau 2 (parallèle): generate_tags, create_embedding
    #   Sorties finales: audio_features, mir_features, tags, vector

    résultats = await pipeline.kiq_dataflow(track_paths=["track1.mp3"])
    # résultats = {
    #   "audio_features": {...},
    #   "mir_features": {...},
    #   "tags": [...],
    #   "vector": [...]
    # }
```

**Résolution dépendances**:
1. `extract_audio_features` aucune dépendance → exécute en premier
2. `compute_mir_features` besoin `audio_features` → exécute après étape 1
3. `generate_tags` besoin `mir_features` → exécute après étape 2
4. `create_embedding` besoin `mir_features` et `tags` → exécute après étapes 2 & 3 complétées

---

## Exemple 2: Exécution Parallèle

Avec ajout de `extract_spectral_features` qui dépend aussi seulement de `audio_features`:

```python
@broker.task
@pipeline_task(output="spectral_features")
async def extract_spectral_features(audio_features: dict) -> dict:
    await asyncio.sleep(0.2)
    return {"spectral_rolloff": 5000.0}

@broker.task
@pipeline_task(output="combined_features")
async def combine_features(
    mir_features: dict,
    spectral_features: dict,
    tags: list[str],
) -> dict:
    return {**mir_features, **spectral_features, "tags": tags}

pipeline = DataflowPipeline.from_tasks(
    broker,
    [
        extract_audio_features,
        compute_mir_features,        # Niveau 1
        extract_spectral_features,   # Niveau 1 (parallèle à compute_mir_features)
        generate_tags,               # Niveau 2 (dépend de mir_features)
        combine_features,            # Niveau 2 (dépend de mir_features + spectral_features + tags)
    ],
)
```

**Niveaux d'exécution**:
- Niveau 0: `extract_audio_features`
- Niveau 1: `compute_mir_features`, `extract_spectral_features` (parallèle)
- Niveau 2: `generate_tags`, `combine_features` (parallèle après leurs dépendances satisfaites)

---

## Exemple 3: Motif Map-Reduce

Traiter multiples pistes en parallèle, puis agréger:

```python
# Map: traiter chaque piste indépendamment
@broker.task
@pipeline_task(output="track_features")
async def process_single_track(track: str) -> dict:
    return {"track": track, "duration": 180.0, "bpm": 120}

# Reduce: agréger toutes features de pistes
@broker.task
@pipeline_task(output="playlist_stats")
async def aggregate_track_features(track_features: list[dict]) -> dict:
    total_duration = sum(t["duration"] for t in track_features)
    avg_bpm = sum(t["bpm"] for t in track_features) / len(track_features)
    return {"total_tracks": len(track_features), "total_duration": total_duration, "avg_bpm": avg_bpm}

# Construire pipeline
pipeline = DataflowPipeline(broker)
pipeline.map(
    process_single_track,
    tracks,  # ["track1.mp3", "track2.mp3", ...]
    output="track_features",
    max_parallel=4,
)
pipeline.reduce(
    aggregate_track_features,
    input_name="track_features",
    output="playlist_stats",
)

résultats = await pipeline.kiq_map_reduce()
# résultats = {"track_features": [...], "playlist_stats": {...}}
```

---

## Exemple 4: Visualisation

Le pipeline fournit multiples formats de visualisation:

```python
# ASCII art (console)
pipeline.print_dag()

# JSON (pour UIs web)
viz_json = pipeline.visualize()
# Structure:
# {
#   "nodes": [{"id": "nom_tache", "outputs": [...], "inputs": [...]}, ...],
#   "edges": [{"from": "tache_a", "to": "tache_b"}],
#   "levels": [["tache1"], ["tache2", "tache3"], ...]
# }

# Format DOT (pour Graphviz)
dot = pipeline.visualize_dot()
# Sauvegarder et rendre:
# with open("pipeline.dot", "w") as f:
#     f.write(dot)
# Exécuter: dot -Tpng pipeline.dot -o pipeline.png
```

---

## Exécuter l'Exemple

```bash
python examples/dataflow_audio_pipeline.py
```

Sortie attendue inclut:
- Affichages DAG ASCII montrant ordre exécution
- Représentation DOT DAG extrait
- Structure JSON DAG extrait

---

## Points Clés à Retenir

1. **Résolution automatique de dépendances** — Pas besoin d'enchaîner manuellement; juste déclarer sorties
2. **Exécution parallèle** — Tâches indépendantes s'exécutent concurremment automatiquement
3. **Programmation dataflow** — Tâches sont fonctions pures; sortie va vers entrées
4. **Débogage visuel** — `print_dag()` montre exactement comment tâches s'exécuteront
5. **Motifs évolutifs** — Map-reduce intégré pour charges batch

---

## Chemin d'Apprentissage

Après cet exemple:

1. **[Guide DataflowPipeline]({{ '/fr/guides/pipelines.md#2-pipeline-dataflow' | relative_url }})** — Plongée profonde fonctionnalités dataflow
2. **[Guide d'Exécution]({{ '/fr/guides/execution/' | relative_url }})** — Parallélisme, timeouts, gestion erreurs
3. **[Guide de Performance]({{ '/fr/guides/performance/' | relative_url }})** — Réglage `max_parallel`, profils ressources

---

*C'est l'exemple flagship. Étudiez-le thoroughly pour comprendre modèle dataflow Taskiq-Flow.*
