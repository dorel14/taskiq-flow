---
permalink: /fr/api/decorators/
title: Référence API: Décorateurs
nav_order: 31
color_scheme: dark
---
# Référence API: Décorateurs

**Décorateurs de tâches, @pipeline_task, et utilitaires**

> **Version** : 0.4.5 | **Module** : `taskiq_flow.decorators`

---

## Aperçu

Le décorateur `@pipeline_task` annote les tâches taskiq avec des déclarations de sortie, permettant la résolution automatique de dépendances dans DataflowPipeline.

---

## @pipeline_task

Marque une tâche avec ce qu'elle produit pour les consommateurs en aval.

```python
from taskiq_flow import pipeline_task

@broker.task
@pipeline_task(output="features")
def extract(données: list[str]) -> dict:
    return compute_features(données)
```

**Paramètres**:

| Paramètre | Type | Description |
|-----------|------|-------------|
| `output` | `str` | Nom clé sortie unique |
| `outputs` | `list[str]` | Clés sortie multiples (pour retours tuple) |
| `inputs` | `list[str]` | Dépendances entrée explicites (optionnel, auto-détecté) |
| `description` | `str` | Description lisible humain (pour documentation) |

### Sortie unique (plus courant)

```python
@broker.task
@pipeline_task(output="données_traitées")
def process(données_brutes: str) -> dict:
    return {"result": données_brutes.upper()}
```

### Sorties multiples

```python
@broker.task
@pipeline_task(outputs=["features", "metadata"])
def split_output(audio: np.ndarray) -> tuple[dict, dict]:
    features = extract_features(audio)
    metadata = extract_meta(audio)
    return features, metadata  # déballé vers les deux sorties
```

Les tâches en aval peuvent consommer soit sortie:

```python
@broker.task
@pipeline_task(output="tags")
def tag(features: dict): ...  # consomme sortie 'features'

@broker.task
@pipeline_task(output="info")
def describe(metadata: dict): ...  # consomme sortie 'metadata'
```

---

## @pipeline_task_multi_output

Alias pour `@pipeline_task(outputs=[...])`. Apporte clarté pour tâches multi-sorties:

```python
from taskiq_flow import pipeline_task_multi_output

@broker.task
@pipeline_task_multi_output(outputs=["x", "y"])
def split(valeur: int) -> tuple[int, int]:
    return valeur // 2, valeur % 2
```

---

## Fonctions Utilitaires

### get_task_outputs(task: Callable) -> list[str]

Obtenir clés sortie déclarées pour une tâche:

```python
from taskiq_flow import get_task_outputs

sorties = get_task_outputs(tache_extract)
print(sorties)  # ['features']
```

### get_task_inputs(task: Callable) -> list[str]

Obtenir dépendances entrée déclarées:

```python
from taskiq_flow import get_task_inputs

entrées = get_task_inputs(tache_tag)
print(entrées)  # ['features']
```

### is_pipeline_task(task: Callable) -> bool

Vérifier si fonction décorée avec `@pipeline_task`:

```python
from taskiq_flow import is_pipeline_task

if is_pipeline_task(ma_fonction):
    print("Ceci est une tâche pipeline avec déclarations sortie")
```

### resolve_task_dependencies(tasks: list[Callable]) -> dict

Construire carte dépendances:

```python
from taskiq_flow import resolve_task_dependencies

deps = resolve_task_dependencies([tache_a, tache_b, tache_c])
# Retourne: {tache_a: [], tache_b: ['features'], tache_c: ['tags']}
```

---

## Ordre des Décorateurs

L'ordre des décorateurs compte: `@broker.task` doit être le plus externe (appliqué en dernier), `@pipeline_task` interne (appliqué en premier):

```python
# CORRECT
@broker.task
@pipeline_task(output="resultat")
def ma_tache(): ...

# INCORRECT (échouera)
@pipeline_task(output="resultat")
@broker.task
def ma_tache(): ...
```

Pourquoi: `@broker.task` enveloppe la fonction; `@pipeline_task` attache métadonnées à la fonction originale. Python applique décorateurs bas-vers-haut.

---

## Type Hints & Analyse Statique

Les type hints aident IDEs et linters à comprendre le dataflow:

```python
from typing import TypedDict

class AudioFeatures(TypedDict):
    duration: float
    tempo: float

@broker.task
@pipeline_task(output="features")
def extract(chemin: str) -> AudioFeatures:
    return {"duration": 180.0, "tempo": 120.0}

@broker.task
@pipeline_task(output="tags")
def tag(features: AudioFeatures) -> list[str]:  # type-safe
    return ["rapide", "électronique"]
```

Utiliser `TypedDict` ou modèles Pydantic pour meilleure autocomplétion IDE et vérification mypy.

---

## Versionnage & Métadonnées

Attacher version et autres métadonnées:

```python
@broker.task(
    nom="extract_features_v2",
    labels={"version": "2.0.0", "expérimental": False}
)
@pipeline_task(
    output="features",
    description="Extraire caractéristiques audio (v2 avec estimation tempo améliorée)"
)
def extract(chemin: str) -> dict:
    ...
```

---

## Pièges Courants

| Piège | Conséquence | Correction |
|-------|-------------|------------|
| `@broker.task` manquant | Tâche non enregistrée avec broker | Ajouter décorateur |
| `output` non défini | Aucun consommateur en aval ne peut en dépendre | Toujours déclarer `output` pour tâches dataflow |
| Mismatch nom sortie | Tâche en aval ne reçoit pas entrée | S'assurer nom paramètre en aval correspond `output` amont |
| Utiliser `@pipeline_task` sur tâches SequentialPipeline | Aucun effet mais inutile | Seulement nécessaire pour DataflowPipeline |

---

## Exemple: Pipeline Dataflow Complet

```python
from taskiq import InMemoryBroker
from taskiq_flow import DataflowPipeline, pipeline_task

broker = InMemoryBroker()

@broker.task
@pipeline_task(output="brut")
def charger(source: str) -> dict:
    return {"data": lire_fichier(source)}

@broker.task
@pipeline_task(output="propre")
def nettoyer(brut: dict) -> dict:
    return {"data": prétraiter(brut["data"])}

@broker.task
@pipeline_task(output="stats")
def analyser(propre: dict) -> dict:
    return calculer_stats(propre["data"])

# Construire
pipeline = DataflowPipeline.from_tasks(broker, [charger, nettoyer, analyser])

# Exécuter
résultats = await pipeline.kiq_dataflow(source="data.csv")
# résultats = {"brut": {...}, "propre": {...}, "stats": {...}}
```

---

*Pour API tâches complète, voir [Guide des Tâches]({{ '/fr/guides/tasks/' | relative_url }}). Pour écrire décorateurs personnalisés, étendre `BaseTaskDecorator` depuis `taskiq_flow.decorators`.*
