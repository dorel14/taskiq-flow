---
permalink: /fr/examples/tracking-demo/
title: Exemple: tracking_demo.py
nav_order: 45
---
# Exemple: tracking_demo.py

**Suivi d'exécution de pipeline avec PipelineTrackingManager**

> **Version** : 0.4.0 | **Fichier** : `examples/tracking_demo.py`

---

## Aperçu

Cet exemple démontre comment monitorer l'exécution de pipeline en temps réel en utilisant `PipelineTrackingManager`. Il couvre:

- Configuration du suivi avec sélection automatique de stockage
- Attacher le suivi à un pipeline
- Exécuter un pipeline et vérifier son statut
- Accéder à l'historique d'exécution étape par étape

---

## Ce Que Cet Example Montre

- Créer un `PipelineTrackingManager` avec stockage auto
- Utiliser `.with_tracking()` sur un pipeline
- Attendre complétion du pipeline
- Interroger le statut du pipeline depuis le tracking manager
- Logger la progression des étapes

---

## Explication du Code

```python
import asyncio
import logging

from taskiq import InMemoryBroker
from taskiq_flow import Pipeline, PipelineMiddleware
from taskiq_flow.tracking import PipelineTrackingManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Créer broker
broker = InMemoryBroker(await_inplace=True)

# Définir une tâche avec délai pour montrer le suivi en action
@broker.task
async def slow_task(x: int) -> int:
    """Tâche lente qui double l'entrée."""
    await asyncio.sleep(1)
    print(f"Slow task appelée avec {x}")
    return x * 2

async def main():
    # 1. Configuration suivi avec sélection auto stockage
    tracking_manager = PipelineTrackingManager().with_auto_storage(broker)

    # 2. Créer middleware avec tracking manager
    middleware = PipelineMiddleware(tracking_manager=tracking_manager)
    broker_with_middleware = broker.with_middlewares(middleware)

    # 3. Créer pipeline avec suivi activé
    pipeline = (
        Pipeline(broker_with_middleware)
        .with_tracking(manager=tracking_manager)
        .call_next(slow_task)
        .call_next(slow_task)
    )

    # 4. Exécuter le pipeline
    result = await pipeline.kiq(10)
    await result.wait_result()

    # 5. Interroger le statut de tracking
    pipeline_id = pipeline.pipeline_id
    if pipeline_id is None:
        raise RuntimeError("Pipeline has no ID")

    status = await tracking_manager.get_status(pipeline_id)
    if status is None:
        raise RuntimeError("Failed to get pipeline status")

    logger.info(f"Pipeline status: {status.status}")
    logger.info(f"Steps completed: {len(status.steps)}")

asyncio.run(main())
```

---

## Points Clés

### Configuration Tracking

```python
tracking_manager = PipelineTrackingManager().with_auto_storage(broker)
```

- `with_auto_storage()` sélectionne automatiquement backend stockage selon broker
- Pour `InMemoryBroker`, utilise `InMemoryPipelineStorage`
- Pour brokers Redis, utilise `RedisPipelineStorage`

### Attacher Suivi au Pipeline

```python
pipeline = Pipeline(broker).with_tracking(manager=tracking_manager)
```

Le tracking manager **doit** être attaché **avant** l'appel à `pipeline.kiq()`.

### Inspection Statut

Après exécution, l'objet `PipelineStatus` contient:

- `status` — Statut global (`COMPLETED`, `FAILED`, etc.)
- `steps` — Liste d'objets `StepStatus`, un par étape
- `started_at` / `completed_at` — Horodatages
- `duration_ms` — Temps exécution total
- `result` — Valeur retour finale (si terminé)

Chaque `StepStatus` inclut:

- `step_name` — Nom de la tâche
- `status` — Statut de l'étape
- `duration_ms` — Durée d'exécution
- `result` — Valeur de retour

---

## Sortie Attendue

```
INFO:__main__:Pipeline status: COMPLETED
INFO:__main__:Steps completed: 2
```

Vous verrez aussi logs des appels slow_task avec délais 1 seconde.

---

## Variations

### Accéder aux Détails d'Étape

```python
for step in status.steps:
    logger.info(f"Étape '{step.step_name}' a pris {step.duration_ms:.0f}ms")
    if step.result:
        logger.info(f"  Résultat: {step.result}")
```

### Suivre Multiples Pipelines

```python
# Lancer plusieurs pipelines concurremment
tasks = [pipeline.kiq(i) for i in range(5)]
await asyncio.gather(*[t.wait_result() for t in tasks])

# Lister tous pipelines suivis
all_statuses = await tracking_manager.list_pipelines()
for s in all_statuses:
    print(f"{s.pipeline_id}: {s.status}")
```

---

## Chemin d'Apprentissage

Après cet exemple:

1. **[Guide de Suivi]({{ '/fr/guides/tracking/' | relative_url }})** — Fonctionnalités complètes tracking (backends stockage, métriques)
2. **[Guide WebSocket]({{ '/fr/guides/websocket/' | relative_url }})** — Streaming temps réel événements de tracking
3. **[Guide API]({{ '/fr/guides/api/' | relative_url }})** — Exposer données tracking via REST API

---

*Cet exemple montre les bases. Pour production, utiliser stockage Redis et ajouter écouteurs pour alertes.*
