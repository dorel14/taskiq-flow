---
permalink: /fr/examples/scheduled-pipeline/
title: Exemple: scheduled_pipeline.py
nav_order: 44
color_scheme: dark
---
# Exemple: scheduled_pipeline.py

**Planification de pipelines avec déclencheurs cron et intervalles**

> **Version** : {VERSION} | **Fichier** : `examples/scheduled_pipeline.py`

---

## Aperçu

Cet exemple démontre comment planifier des pipelines pour exécution périodique en utilisant `LabelBasedScheduler`. Il couvre:

- Planification cron (avec précision seconde)
- Planification par intervalle
- Lister et inspecter jobs planifiés

**Note** : Cet exemple utilise `LabelBasedScheduler`, mécanisme de planification par labels de TaskIQ. Pour planification cron production, considérer `PipelineScheduler` avec intégration APScheduler.

---

## Ce Que Cet Exemple Montre

- Créer un pipeline simple
- Utiliser `LabelBasedScheduler` pour planifier exécutions pipeline
- Expressions cron avec précision seconde
- Planification par intervalle
- Lister schedules actifs

---

## Explication du Code

```python
import asyncio
from taskiq import InMemoryBroker
from taskiq_flow import Pipeline, PipelineMiddleware
from taskiq_flow.scheduling import LabelBasedScheduler

# Créer broker
broker = InMemoryBroker(await_inplace=True).with_middlewares(PipelineMiddleware())

# Définir tâche simple
@broker.task
async def log_message(msg: str) -> str:
    """Logger un message."""
    return f"Traited: {msg}"

async def main():
    # Créer pipeline
    pipeline = Pipeline(broker).call_next(log_message)

    # Créer scheduler
    scheduler = LabelBasedScheduler(broker)

    # Planifier avec expression cron (toutes les 5 secondes)
    schedule_id = await scheduler.schedule_with_cron(
        pipeline=pipeline,
        label="every-5-seconds",
        cron="*/5 * * * * *",  # cron 6 champs pour précision seconde
        args=("Hello from scheduled pipeline!",),
    )
    print(f"Scheduled with cron: {schedule_id}")

    # Planifier avec intervalle (toutes les 3 secondes)
    interval_id = await scheduler.schedule_with_interval(
        pipeline=pipeline,
        label="every-3-seconds",
        interval_seconds=3,
        args=("Interval scheduled run!",),
    )
    print(f"Scheduled with interval: {interval_id}")

    # Attendre quelques exécutions
    print("Waiting for pipeline executions (12 seconds)...")
    await asyncio.sleep(12)

    # Lister jobs planifiés
    schedules = scheduler.list_schedules()
    print(f"Active schedules: {len(schedules)}")
    for sched in schedules:
        print(f"  - {sched['label']}: cron={sched.get('cron')}, enabled={sched['enabled']}")

asyncio.run(main())
```

---

## Méthodes de Planification

### Planification Cron

```python
schedule_id = await scheduler.schedule_with_cron(
    pipeline=pipeline,
    label="mon-schedule",
    cron="*/5 * * * * *",  # Toutes 5 secondes (cron 6 champs)
    args=("message",),
)
```

**Format cron 6 champs**：`seconde minute Heure jour mois jour-semaine`

Exemples:
- `*/5 * * * * *` — Toutes les 5 secondes
- `0 * * * * *` — Toutes les minutes à seconde 0
- `0 0 * * * *` — Toutes les heures à minute 0, seconde 0

### Planification Intervalle

```python
interval_id = await scheduler.schedule_with_interval(
    pipeline=pipeline,
    label="interval-3s",
    interval_seconds=3,
    args=("message",),
)
```

Exécute toutes les N secondes, indépendamment de l'heure système.

---

## Sortie Attendue

```
Scheduled with cron: schedule_123456
Scheduled with interval: interval_789012
Waiting for pipeline executions (12 seconds)...
INFO:root:Processed: Hello from scheduled pipeline!
INFO:root:Processed: Interval scheduled run!
INFO:root:Processed: Hello from scheduled pipeline!
INFO:root:Processed: Interval scheduled run!
...
Active schedules: 2
  - every-5-seconds: cron=*/5 * * * * *, enabled=True
  - every-3-seconds: cron=None, enabled=True
```

Vous devriez voir le message logué plusieurs fois comme schedules se déclenchent.

---

## Points Clés

### Planification par Label

- Chaque schedule requiert un `label` unique (utilisé pour identification)
- Les labels peuvent activer/désactiver schedules dynamiquement
- Le scheduler gère persistance schedules selon votre broker

### Limite InMemoryBroker

Avec `InMemoryBroker`, schedules fonctionnent seulement pendant processus en cours; perdus au redémarrage. Pour planification persistante, utiliser brokers Redis avec stores de schedule appropriés.

### Multiples Schedules

Vous pouvez planifier même pipeline plusieurs fois avec labels, expressions cron, ou arguments différents.

---

## Variations

### Planification Avancée avec PipelineScheduler

Pour planification plus avancée (timezones, gestion misfire), utiliser `PipelineScheduler`:

```python
from taskiq_flow import PipelineScheduler

scheduler = PipelineScheduler(broker)
job_id = await scheduler.schedule(
    pipeline,
    cron="0 9 * * *",  # Quotidien à 9h
    args=("daily",)
)
await scheduler.start()
```

Voir [Guide de Planification]({{ '/fr/guides/scheduling/' | relative_url }}) pour détails complets sur `PipelineScheduler`.

---

## Chemin d'Apprentissage

Après cet exemple:

1. **[Guide de Planification]({{ '/fr/guides/scheduling/' | relative_url }})** — Planification cron et intervalle complète
2. **[PipelineScheduler API]({{ '/fr/api/core.md#pipelinescheduler' | relative_url }})** — Référence API
3. **[Guide de Retry]({{ '/fr/guides/retry/' | relative_url }})** — Gestion échecs pipelines planifiés

---

*Cet exemple montre bases planification par label. Pour production, explorer PipelineScheduler avec stores de jobs externes (PostgreSQL/Redis).*
