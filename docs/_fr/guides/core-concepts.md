---
title: Guide des Concepts Fondamentaux
nav_order: 12
permalink: /fr/guides/core-concepts/
color_scheme: dark
---
# Guide des Concepts Fondamentaux

**Comprendre les concepts fondamentaux de Taskiq-Flow et son architecture**

> **Version** : {VERSION}

---

## Aperçu

Taskiq-Flow est basé sur deux modèles principaux :

1. **SequentialPipeline** — Exécution linéaire étape par étape
2. **DataflowPipeline** — Construction automatique de DAG à partir des dépendances

Pour une compréhension approfondie du modèle dataflow, voir le [Guide Dataflow]({{ '/fr/guides/dataflow/' | relative_url }}).

Comprendre ces modèles vous aide à choisir la bonne approche pour votre workflow.

---

## 1. Le modèle Sequential Pipeline

Dans un pipeline séquentiel, vous définissez explicitement l'ordre des opérations :

```python
pipeline = (
    Pipeline(broker)
    .call_next(step1)
    .call_next(step2)
    .map(step3)        # Parallèle sur liste
    .filter(step4)     # Conditionnel
)
```

**Propriétés clés :**
- L'ordre d'exécution est explicite
- Chaque étape reçoit la sortie de l'étape précédente
- `.map()` et `.filter()` traitent les itérables en parallèle
- Idéal pour les workflows linéaires avec des branches occasionnelles

---

## 2. Le modèle Dataflow Pipeline

Les pipelines dataflow vous permettent de déclarer les dépendances entre tâches. La bibliothèque détermine l'ordre d'exécution :

```python
@broker.task
@pipeline_task(output="features")
def extract(data): ...

@broker.task
@pipeline_task(output="stats")
def compute(features): ...  # dépend automatiquement de 'extract'

pipeline = DataflowPipeline.from_tasks(broker, [extract, compute])
```

**Propriétés clés :**
- Les tâches déclarent ce qu'elles produisent (`output=`)
- Les tâches en aval reçoivent automatiquement les entrées nécessaires via la correspondance des paramètres
- Les tâches indépendantes s'exécutent en parallèle automatiquement
- Idéal pour les workflows complexes et branchés

---

## 3. Les Tâches sont Tout

Chaque fonction dans un pipeline **doit** être une tâche taskiq (décorée avec `@broker.task`) :

```python
@broker.task
def process(value: int) -> int:
    return value * 2
```

Les tâches s'exécutent de manière asynchrone, peuvent être relancées, et sont orchestrées par le broker.

---

## 4. Le Middleware est Essentiel

Le `PipelineMiddleware` est requis. Il intercepte la completion des tâches et déclenche l'étape suivante :

```python
from taskiq_flow import PipelineMiddleware

broker.add_middlewares(PipelineMiddleware())
```

Sans lui, les pipelines ne fonctionneront pas.

---

## 5. Le Backend de Résultats est Essentiel

Pour les configurations multi-workers ou distribuées, utilisez un broker persistant (Redis, Kafka, etc.). L'`InMemoryBroker` fonctionne uniquement pour le développement en simple processus.

---

## 6. Suivi et Monitoring (Optionnel mais Recommandé)

Ajoutez le suivi en temps réel avec `PipelineTrackingManager` :

```python
from taskiq_flow import PipelineTrackingManager

tracking = PipelineTrackingManager().with_auto_storage(broker)
pipeline = Pipeline(broker).with_tracking(tracking)
```

Cela vous donne le statut du pipeline, l'historique des étapes, et les métriques.

---

## 7. Tableau Comparatif

| Fonctionnalité | SequentialPipeline | DataflowPipeline |
|----------------|-------------------|------------------|
| Contrôle de l'ordre | Explicite | Automatique |
| Parallélisme | Manuel (`.group()`) | Automatique (tâches indépendantes) |
| Dépendances | Implicite (enchaînement) | Explicite (`@pipeline_task`) |
| Idéal pour | ETL linéaire | Workflows complexes et branchés |
| Flexibilité | Contrôle total | Déclaratif |

---

## 8. Quand utiliser l'un ou l'autre ?

**Utilisez SequentialPipeline quand :**
- Votre workflow est une ligne droite
- Vous voulez un contrôle précis de l'ordre
- Vous avez des opérations map/filter occasionnelles

**Utilisez DataflowPipeline quand :**
- Les tâches ont des dépendances de données claires
- Vous voulez une exécution parallèle automatique
- Vous construisez des graphes de tâches réutilisables
- Votre workflow se divise (fan-out/fan-in)

---

## Prochaines étapes

Maintenant que vous comprenez les concepts :

- **[Installation]({{ '/fr/guides/installation/' | relative_url }})** — Si vous n'avez pas encore installé
- **[Guide de Démarrage Rapide]({{ '/fr/quickstart/' | relative_url }})** — Tutoriel pratique
- **[Guide des Pipelines]({{ '/fr/guides/pipelines/' | relative_url }})** — Approfondissement sur les types de pipelines

---

*Concepts clairs ? Passez à l'[Installation]({{ '/fr/guides/installation/' | relative_url }}) ou au [Guide de Démarrage Rapide]({{ '/fr/quickstart/' | relative_url }}).*
