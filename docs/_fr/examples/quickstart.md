---
title: Exemple: quickstart.py
nav_order: 41
---
# Exemple: quickstart.py

**Pipeline séquentiel basique avec opérations map, filter, et group**

> **Version** : 0.3.2 | **Fichier** : `examples/quickstart.py`

---

## Aperçu

Cet exemple démontre les fondamentaux de Taskiq-Flow en utilisant un pipeline séquentiel classique. Il couvre:

- Définition de tâches avec `@broker.task`
- Construction de pipeline avec `.call_next()`, `.map()`, `.filter()`
- Exécution du pipeline et récupération des résultats
- Compréhension du flux de données à travers étapes

---

## Explication Pas-à-Pas du Code

```python
import asyncio
from taskiq import InMemoryBroker
from taskiq_flow import Pipeline, PipelineMiddleware

# 1. Initialiser broker et ajouter middleware
broker = InMemoryBroker()
broker.add_middlewares(PipelineMiddleware())

# 2. Définir les tâches
@broker.task
def add_one(value: int) -> int:
    return value + 1

@broker.task
def repeat(value: int, times: int) -> list[int]:
    return [value] * times

@broker.task
def is_positive(value: int) -> bool:
    return value >= 0

# 3. Construire le pipeline
async def main():
    pipeline = (
        Pipeline(broker)
        .call_next(add_one)           # Étape 1: 1 → 2
        .call_next(repeat, times=4)   # Étape 2: 2 → [2,2,2,2]
        .map(add_one)                  # Étape 3: [2,2,2,2] → [3,3,3,3]
        .filter(is_positive)           # Étape 4: garder positifs (tous gardés)
    )

    # 4. Exécuter
    task = await pipeline.kiq(1)
    result = await task.wait_result()
    print("Résultat:", result.return_value)  # [3, 3, 3, 3]

asyncio.run(main())
```

---

## Explication Étape par Étape

### Étape 1: `call_next(add_one)`

- **Entrée**: `1`
- **Opération**: `add_one(1) = 2`
- **Sortie**: `2`

### Étape 2: `call_next(repeat, times=4)`

- **Entrée**: `2`
- **Opération**: `repeat(2, times=4) = [2, 2, 2, 2]`
- **Sortie**: `[2, 2, 2, 2]`

### Étape 3: `map(add_one)`

- **Entrée**: `[2, 2, 2, 2]` (itérable)
- **Opération**: Appliquer `add_one` à chaque élément **en parallèle**
  - `add_one(2) = 3`
  - `add_one(2) = 3`
  - `add_one(2) = 3`
  - `add_one(2) = 3`
- **Sortie**: `[3, 3, 3, 3]`

### Étape 4: `filter(is_positive)`

- **Entrée**: `[3, 3, 3, 3]` (itérable)
- **Opération**: Garder éléments où `is_positive(element) == True`
  - Tous 4 éléments positifs → tous gardés
- **Sortie**: `[3, 3, 3, 3]`

---

## Concepts Clés Démontrés

1. **Définition de tâche** — Toute étape de pipeline doit être une tâche (`@broker.task`)
2. **Exigence middleware** — `PipelineMiddleware` **doit** être ajouté au broker
3. **Flux de données** — Chaque étape reçoit sortie précédente (sauf `call_after`)
4. **Exécution parallèle** — `.map()` exécute éléments concurremment
5. **Enchaînement** — Les méthodes retournent pipeline pour interface fluide

---

## Exécuter l'Exemple

```bash
python examples/quickstart.py
```

Sortie attendue:
```
Résultat: [3, 3, 3, 3]
```

---

## Variations à Tester

### Utiliser `filter` pour éliminer négatifs

```python
@broker.task
def subtract_three(valeur: int) -> int:
    return valeur - 5  # résultats en [-2, -2, -2, -2]

pipeline = (
    Pipeline(broker)
    .call_next(add_one)
    .call_next(repeat, times=4)
    .map(subtract_three)  # [2,2,2,2] → [-2,-2,-2,-2]
    .filter(is_positive)   # [] — tous filtrés
)
```

### Utiliser `group` pour tâches indépendantes parallèles

```python
@broker.task
def tache_a(x: int) -> int: return x * 2
@broker.task
def tache_b(x: int) -> int: return x + 10
@broker.task
def tache_c(x: int) -> int: return x ** 2

pipeline = Pipeline(broker).call_next(add_one)  # 1 → 2
pipeline.group([tache_a, tache_b, tache_c], param_names=["x"])
# Les trois reçoivent 2 et s'exécutent en parallèle
# Résultat: [4, 12, 4]
```

---

## Chemin d'Apprentissage

Après cet exemple:

1. **[Pipelines Dataflow]({{ '/fr/guides/pipelines.md#2-pipeline-dataflow' | relative_url }})** — Construction automatique de DAG
2. **[Définition des Tâches]({{ '/fr/guides/tasks/' | relative_url }})** — Fonctionnalités avancées de tâches
3. **[Suivi]({{ '/fr/guides/tracking/' | relative_url }})** — Monitor exécutions de pipeline
4. **[MapReduce]({{ '/fr/guides/execution.md#3-motif-map-reduce' | relative_url }})** — Motif de traitement par lots

---

*Cet exemple est le "Hello World" de Taskiq-Flow. Maîtriser-le avant de passer à motifs plus complexes.*
