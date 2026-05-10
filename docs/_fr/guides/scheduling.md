---
title: Guide de Planification des Pipelines
nav_order: 25
---
# Guide de Planification des Pipelines

**Planification cron, intervalles et exécutions uniques avec PipelineScheduler**

> **Version** : {VERSION} | **Lié** : [Guide d'Exécution]({{ '/fr/guides/execution/' | relative_url }}), [Guide de Suivi]({{ '/fr/guides/tracking/' | relative_url }})

---

## Aperçu

Taskiq-Flow inclut un système de planification puissant pour exécuter des pipelines à heures fixes ou intervalles réguliers, construit sur APScheduler.

Ce guide couvre :

- `PipelineScheduler` — Interface principale de planification
- Expressions cron et motifs
- Planification par intervalle
- Exécutions uniques (one-off)
- Gestion des fuseaux horaires
- Persistance et gestion des jobs
- Gestion des exécutions manquées

---

## 1. Démarrage Rapide

```python
from taskiq_flow import Pipeline, PipelineScheduler

# Create your pipeline
pipeline = Pipeline(broker).call_next(my_task).call_next(another_task)

# Créer le planificateur
scheduler = PipelineScheduler(broker)

# Planifier pour exécution chaque minute
job_id = await scheduler.schedule(
    pipeline,
    cron="* * * * *",  # Toutes les minutes
    args=("données",)  # Arguments passés à pipeline.kiq()
)

# Démarrer le planificateur (tourne en arrière-plan)
await scheduler.start()

# ... garder votre application en vie ...
# le scheduler tourne en tâches de fond

# Arrêt gracieux
await scheduler.shutdown()
```

C'est la base. Explorons les fonctionnalités en détail.

---

## 2. PipelineScheduler

La classe principale pour planifier les exécutions de pipeline.

### 2.1. Initialisation

```python
from taskiq_flow import PipelineScheduler

scheduler = PipelineScheduler(
    broker,
    store="memory",  # "memory" ou "sqlite"
    store_path="./scheduler_jobs.db"  # pour store sqlite
)
```

**Options de stockage**:

| Store | Persistance | Multi-worker | Cas d'usage |
|-------|-------------|--------------|-------------|
| `"memory"` | ❌ Non | ❌ Non | Développement, mono-processus |
| `"sqlite"` | ✅ Oui | ⚠️ Limité* | Production mono-worker, persistance simple |
| `"postgresql"` (via URL) | ✅ Oui | ✅ Oui | Production multi-worker, haute disponibilité |
| `"mysql"` (via URL) | ✅ Oui | ✅ Oui | Production multi-worker, alternative PostgreSQL |
| `"redis"` | ❌ | ❌ | **Non implémenté** (placeholder lève `NotImplementedError`) |

*Le store sqlite fonctionne avec une seule instance de scheduler ; multiples workers nécessitent DB externe (PostgreSQL/MySQL).

**Recommandation** :
- Dev/mocks → `store="memory"`
- Production mono-worker → `store="sqlite"` avec chemin persistant
- Production distribué → `store="postgresql://user:pass@host/dbname"` (recommandé) #pragma: allowlist secret

> **Note** : Le support PostgreSQL et MySQL est **déjà implémenté** dans `taskiq_flow.scheduling.storage.JobPersistenceManager` et fonctionne via SQLAlchemy avec `sqlalchemy.asyncio.AsyncSession`. Voir la section [Stockage Avancé (PostgreSQL/MySQL)](#stockage-avancé-postgresqlmysql) ci-dessous.

### 2.2. Démarrage & Arrêt

```python
# Démarrer le scheduler (commence à surveiller les schedules)
await scheduler.start()

# Tourner en arrière-plan pendant que l'app tourne
# Typiquement intégré aux événements de lifespan FastAPI/Quart

# Arrêt gracieux
await scheduler.shutdown()
# Attend que les jobs en cours finissent, annule les pending
```

**Démarrage automatique avec context manager**:

```python
async with PipelineScheduler(broker) as scheduler:
    await scheduler.schedule(pipeline, cron="*/5 * * * *")
    # Le scheduler démarre automatiquement sur __aenter__
    # ... exécuter votre app ...
# Arrêt automatique sur __aexit__
```

---

## 3. Méthodes de Planification

### 3.1. Planification Cron

```python
job_id = await scheduler.schedule(
    pipeline,
    cron="0 * * * *",  # Toutes les heures à minute 0
    args=("input_data",),
    kwargs={"key": "value"},
    pipeline_id="job_horaire_001"
)
```

**Format expression cron**: `minute heure jour mois jour-semaine`

| Champ | Valeurs autorisées | Caractères spéciaux |
|-------|-------------------|---------------------|
| Minute | 0-59 | `* , - /` |
| Heure | 0-23 | `* , - /` |
| Jour | 1-31 | `* , - / ?` |
| Mois | 1-12 | `* , - /` |
| Jour semaine | 0-6 (Dim-Sam) | `* , - / ?` |

**Exemples**:

```python
"*/5 * * * *"          # Toutes les 5 minutes
"0 9 * * *"            # Quotidien à 9h00
"0 0 * * 0"            # Hebdomadaire dimanche à minuit
"0 0 1 * *"            # Mensuel le 1er à minuit
"0 0 1 1 *"            # Annuel 1er janvier à minuit
```

### 3.2. Planification par Intervalle

```python
# Exécuter toutes les N secondes/minutes/heures/jours/semaines
job_id = await scheduler.schedule_interval(
    pipeline,
    seconds=30,       # Toutes les 30 secondes
    # minutes=5,     # Toutes les 5 minutes
    # hours=1,       # Toutes les heures
    args=(data,)
)
```

**Note** : La planification par intervalle utilise `IntervalTrigger` d'APScheduler. Le cron est généralement préféré en production (plus flexible, gère DST).

### 3.3. Exécution Unique (Run At)

Planifier une seule exécution future:

```python
from datetime import datetime, timedelta

job_id = await scheduler.schedule_at(
    pipeline,
    run_at=datetime.now() + timedelta(hours=2),  # Dans 2 heures
    args=(payload,)
)
```

Ou planifier pour un horaire calendaire spécifique:

```python
run_time = datetime(2026, 12, 31, 23, 59, 59)
await scheduler.schedule_at(pipeline, run_at=run_time)
```

---

## 4. Configuration du Job

### 4.1. ID de Job

Chaque job planifié reçoit un identifiant unique:

```python
job_id = await scheduler.schedule(pipeline, cron="* * * * *")
print(job_id)  # ex: "job_20260505_abcdef123456"
```

Personnaliser l'ID:

```python
job_id = await scheduler.schedule(
    pipeline,
    cron="0 9 * * *",
    job_id="etl_quotidien_9h"  # ID lisible par humain
)
```

Utile pour gestion ultérieure (update, cancel, list).

### 4.2. Arguments & Kwargs

Passer des arguments à la méthode `kiq()` du pipeline:

```python
await scheduler.schedule(
    pipeline,
    cron="* * * * *",
    args=("positional_arg",),     # tuple
    kwargs={"option": True},      # dict
    pipeline_id="my_pipeline"     # explicit pipeline ID
)
```

Le scheduler appelle : `await pipeline.kiq(*args, **kwargs)` à chaque déclenchement.

### 4.3. ID de Pipeline

Chaque exécution planifiée peut surcharger l'ID par défaut du pipeline:

```python
pipeline = Pipeline(broker)  # génère ID aléatoire par défaut

# Planifier avec ID explicite (assure unicité pour suivi)
await scheduler.schedule(
    pipeline,
    cron="*/5 * * * *",
    pipeline_id="my_pipeline_v1"
)
```

**Bonne pratique** : Inclure timestamp ou version dans l'ID pour suivi:

```python
job_id = f"batch_process_v2_{int(time.time())}"
```

---

## 5. Gestion des Jobs

### 5.1. Lister les Jobs Planifiés

```python
jobs = await scheduler.list_jobs()
for job in jobs:
    print(f"ID: {job.id}")
    print(f"  Trigger: {job.trigger}")
    print(f"  Next run: {job.next_run_time}")
    print(f"  Pipeline: {job.pipeline_id}")
```

### 5.2. Obtenir les Détails d'un Job

```python
job = await scheduler.get_job(job_id)
if job:
    print(f"Job {job.id} prévu pour {job.next_run_time}")
```

### 5.3. Modifier un Job

```python
# Replanifier un job existant
await scheduler.reschedule_job(
    job_id,
    cron="0 */2 * * *"  # Changer pour toutes les 2 heures
)

# Mettre à jour les arguments du job
await scheduler.modify_job(
    job_id,
    args=("nouvel_arg",),
    kwargs={"mis_à_jour": True}
)
```

### 5.4. Supprimer (Annuler) un Job

```python
await scheduler.remove_job(job_id)
# Les exécutions futures sont annulées ; le job en cours continue
```

### 5.5. Pause & Reprise

```python
# Mettre en pause temporairement un job
await scheduler.pause_job(job_id)

# Reprendre plus tard
await scheduler.resume_job(job_id)
```

---

## 6. Suivi des Exécutions Planifiées

Chaque exécution de pipeline planifiée est automatiquement suivie si le pipeline a le suivi activé:

```python
tracking = PipelineTrackingManager().with_auto_storage(broker)
pipeline = Pipeline(broker).with_tracking(tracking)

scheduler = PipelineScheduler(broker)
await scheduler.schedule(pipeline, cron="*/5 * * * *")

# Later, query execution history
history = await tracking.get_history()
for run in history:
    print(f"Run {run.pipeline_id}: {run.status} at {run.started_at}")
```

**Distinguer les runs planifiés** : Utiliser des `pipeline_id` descriptifs:

```python
await scheduler.schedule(
    pipeline,
    cron="0 2 * * *",  # Quotidien 2h
    pipeline_id=f"etl_quotidien_{datetime.now().strftime('%Y%m%d')}"
)
# Chaque jour reçoit un ID unique pour suivi
```

---

## 7. Gestion des Exécutions Manquées

Quand l'heure de déclenchement d'un job planifié est manquée (ex: downtime du scheduler, job long), APScheduler fournit des contrôles:

### 7.1. Coalescing (Regroupement)

Combiner multiples runs manqués en une seule exécution:

```python
from apscheduler.triggers.cron import CronTrigger

trigger = CronTrigger(
    hour=9,
    minute=0,
    coalesce=True  # Si scheduler down à 9h00, lance une fois à 9h05 au lieu de 5 fois
)

job = await scheduler.schedule(pipeline, trigger=trigger)
```

### 7.2. Max Instances (Instances Max)

Empêcher exécutions qui se chevauchent du même job:

```python
# Un nouveau run ne démarre pas si l'instance précédente tourne encore
trigger = CronTrigger(minute="*/5", max_instances=1, coalesce=True)
job = await scheduler.schedule(pipeline, trigger=trigger)
# Si un run 9h00 est encore en cours à 9h05, le run 9h05 est sauté
```

### 7.3. Misfire Grace Time (Délai de grâce après manqué)

Permettre une fenêtre après l'heure planifiée pendant laquelle l'exécution est toujours valide:

```python
from apscheduler.triggers.cron import CronTrigger

# Si le scheduler redémarre dans les 10 minutes après l'heure planifiée, lance quand même
trigger = CronTrigger(
    minute="*/5",
    misfire_grace_time=600  # 10 minutes en secondes
)

job = await scheduler.schedule(pipeline, trigger=trigger)
```

---

## 8. Fuseaux Horaires

Par défaut, APScheduler utilise le fuseau horaire système. Pour production, définir explicitement:

```python
from apscheduler.triggers.cron import CronTrigger
import pytz

# Planifier pour 9h00 dans le fuseau New York
trigger = CronTrigger(
    hour=9,
    minute=0,
    timezone=pytz.timezone("America/New_York")
)

job = await scheduler.schedule(pipeline, trigger=trigger)
```

Ou définir globalement sur le scheduler:

```python
scheduler = PipelineScheduler(
    broker,
    timezone="UTC"  # ou "America/Los_Angeles", "Europe/Paris", ...
)
```

**Gestion de l'heure d'été (DST)** : Les triggers cron avec fuseau explicite gèrent automatiquement les transitions DST. Les jobs planifiés à "9h00" s'exécutent toujours à 9h00 locale quand l'horloge change.

---

## 9. Triggers Personnalisés

Au-delà du cron et intervalles, utiliser n'importe quel trigger APScheduler:

```python
from apscheduler.triggers.date import DateTrigger
from datetime import datetime, timedelta

# Exécution unique à datetime spécifique
trigger = DateTrigger(run_date=datetime(2026, 12, 31, 23, 59, 59))
job = await scheduler.schedule(pipeline, trigger=trigger)

# Exécution après délai (from now)
trigger = DateTrigger(run_date=datetime.now() + timedelta(minutes=10))
job = await scheduler.schedule(pipeline, trigger=trigger)
```

Voir documentation APScheduler pour triggers avancés (calendaires, etc.).

---

## 10. Gestion des Erreurs

### 10.1. Capturer les Erreurs d'Exécution de Job

Encapsuler l'exécution du pipeline avec gestion d'erreur:

```python
@broker.task
async def my_pipeline_task(data):
    try:
        result = await process(data)
        return result
    except Exception as exc:
        # Log error, but let scheduler continue
        logger.error(f"Pipeline failed: {exc}")
        raise  # Scheduler records failure, continues with next schedule
```

### 10.2. Callbacks d'Erreur au Niveau Scheduler

```python
scheduler = PipelineScheduler(broker)

@scheduler.on_error
async def handle_scheduler_error(job_id, exception):
    logger.error(f"Job {job_id} échoué avec: {exception}")
    envoyer_alerte_email(job_id, exception)

await scheduler.start()
```

### 10.3. Dead Letter Queue (DLQ)

Pour les jobs qui échouent répétitivement, router vers DLQ:

```python
from taskiq_flow.middlewares.retry import RetryMiddleware

# Configurer retry avec backoff
broker.add_middlewares(
    RetryMiddleware(
        max_retries=3,
        delay=10,
        backoff=2
    )
)

# Après max retries, la tâche va dans DLQ (si broker supporte)
# RedisStreamBroker: dead_letter_stream
# KafkaBroker: dead_letter_topic
```

---

## 11. Monitoring des Jobs Planifiés

### 11.1. Health Check

```python
async def scheduler_health():
    stats = scheduler.get_stats()
    return {
        "scheduled_jobs": len(scheduler.get_jobs()),
        "running_jobs": stats.active_jobs,
        "next_run": min(job.next_run_time for job in scheduler.get_jobs())
    }
```

### 11.2. Logging

Configurer logging structuré:

```python
import logging
logger = logging.getLogger("taskiq_flow.scheduler")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Logs du scheduler:
# 2026-05-05 10:00:00 - taskiq_flow.scheduler - INFO - Running job daily_etl_9am
# 2026-05-05 10:00:05 - taskiq_flow.scheduler - INFO - Job daily_etl_9am completed successfully
```

### 11.3. Métriques

Intégrer avec Prometheus:

```python
from prometheus_client import Counter, Gauge

SCHEDULED_JOBS = Gauge('scheduled_jobs_total', 'Total jobs planifiés')
JOB_RUNS = Counter('scheduler_job_runs_total', 'Exécutions job', ['job_id'])
JOB_FAILURES = Counter('scheduler_job_failures_total', 'Échecs job', ['job_id'])

class MetricsScheduler(PipelineScheduler):
    async def _run_job(self, job_id, pipeline):
        JOB_RUNS.labels(job_id=job_id).inc()
        try:
            await super()._run_job(job_id, pipeline)
        except Exception:
            JOB_FAILURES.labels(job_id=job_id).inc()
            raise
```

---

## 12. Considérations de Production

### 12.1. Haute Disponibilité

Pour déploiements production HA, lancer multiples instances de scheduler avec un job store partagé:

```python
# Scheduler 1
scheduler1 = PipelineScheduler(
    broker,
    store="postgresql",
    # pragma: allownextline secret
    db_url="postgresql+asyncpg://user:pass@host/db"  
)

# Scheduler 2 (config identique) — seul un acquittera les jobs
scheduler2 = PipelineScheduler(
    broker,
    store="postgresql",
    # pragma: allownextline secret
    db_url="postgresql+asyncpg://user:pass@host/db"  # pragma: allowlist secret
)
# Les job stores d'APScheduler utilisent un verrouillage ligne ; un scheduler par job
```

Voir [Stockage Avancé (PostgreSQL/MySQL)](#stockage-avancé-postgresqlmysql) pour la configuration détaillée.

### 12.2. Jobs de Longue Durée

Si une exécution de pipeline peut dépasser son intervalle de schedule:

```python
# S'assurer pas de chevauchement
trigger = CronTrigger(minute="*/5", max_instances=1, coalesce=True)
job = await scheduler.schedule(pipeline, trigger=trigger)

# Le pipeline lui-même a timeout
pipeline.with_timeout(seconds=300)  # 5 minutes max
```

### 12.3. Comportement au Démarrage

Au redémarrage du scheduler, les jobs manqués sont gérés selon `misfire_grace_time`:

```python
# Scheduler redémarre à 9h05, job planifié pour 9h00
# Avec misfire_grace_time=600 (10 min) : job lance à 9h05
# Avec misfire_grace_time=0 : job sauté
trigger = CronTrigger(hour=9, misfire_grace_time=600)
```

### 12.5. Stockage Avancé (PostgreSQL/MySQL)

`JobPersistenceManager` supporte nativement PostgreSQL et MySQL via SQLAlchemy AsyncEngine.

#### Configuration PostgreSQL (recommandé pour production)

```python
from taskiq_flow.scheduling.storage import JobPersistenceManager

# PostgreSQL avec asyncpg
storage = JobPersistenceManager(
    db_url="postgresql+asyncpg://user:pass@localhost:5432/taskiq_flow",  # pragma: allowlist secret
    async_mode=True,
)

# Avec le helper pour générer l'URL
storage = JobPersistenceManager(
    db_url=JobPersistenceManager.get_connection_url(
        "postgresql",
        host="localhost",
        port=5432,
        user="taskiq",
        password="secret",        # pragma: allowlist secret
        database="taskiq_flow",
    ),
    async_mode=True,
)
```

#### Configuration MySQL

```python
storage = JobPersistenceManager(
    db_url="mysql+aiomysql://user:pass@localhost:3306/taskiq_flow",  # pragma: allowlist secret
    async_mode=True,
)
```

#### Configuration SQLite (développement)

```python
# Sync (développement)
storage = JobPersistenceManager(
    db_url="sqlite:///jobs.db",
    async_mode=False,
)

# Async (recommandé même pour SQLite en production)
storage = JobPersistenceManager(
    db_url="sqlite+aiosqlite:///jobs.db",
    async_mode=True,
)
```

#### Intégration avec la Persistance APScheduler

```python
from taskiq_flow.scheduling.scheduler import PipelineScheduler
from taskiq_flow.scheduling.storage import JobPersistenceManager

storage = JobPersistenceManager(
    db_url="postgresql+asyncpg://user:pass@localhost:5432/taskiq_flow",  # pragma: allowlist secret
)

# Le store URL est passé au PipelineScheduler
scheduler = PipelineScheduler(
    broker,
    job_store_url="postgresql+asyncpg://user:pass@localhost:5432/taskiq_flow",  # pragma: allowlist secret
)
```

#### Opérations CRUD du JobPersistenceManager

```python
from datetime import datetime, timezone
from taskiq_flow.scheduling.storage import JobPersistenceManager, SchedulerJob, PipelineExecution

storage = JobPersistenceManager(db_url="sqlite:///test.db")

# Sauvegarder un job
job = SchedulerJob(
    id="job_001",
    pipeline_id="etl_daily",
    label="ETL Quotidien",
    cron="0 2 * * *",
    timezone="UTC",
)
await storage.save_job(job)

# Charger tous les jobs
jobs = await storage.load_jobs()
for j in jobs:
    print(f"{j.id}: {j.cron} - {j.pipeline_id}")

# Sauvegarder l'historique d'exécution
execution = PipelineExecution(
    job_id="job_001",
    pipeline_id="etl_daily",
    status="success",
    started_at=datetime.now(timezone.utc),
    completed_at=datetime.now(timezone.utc),
    duration_seconds=45.2,
)
await storage.save_execution_history("job_001", execution)

# Récupérer l'historique
history = await storage.get_execution_history("job_001", limit=10)
for run in history:
    print(f"  {run.status} - {run.duration_seconds}s at {run.started_at}")
```

| Backend | Async | Multi-worker | Production |
|---------|-------|--------------|------------|
| SQLite | ✅ `sqlite+aiosqlite` | ⚠️ Single-writer | Dev / petits projets |
| PostgreSQL | ✅ `postgresql+asyncpg` | ✅ Full | ✅ Recommandé |
| MySQL | ✅ `mysql+aiomysql` | ✅ Full | ✅ Supporté |

---

## 13. Motifs Courants

### 13.1. Pipeline ETL Quotidien

```python
@scheduler.schedule(
    pipeline=etl_pipeline,
    cron="0 2 * * *",  # 2h00 quotidien
    pipeline_id="etl_quotidien"
)
async def run_daily_etl():
    pass
```

### 13.2. Health Check Périodique

```python
health_pipeline = Pipeline(broker).call_next(health_check_task)

await scheduler.schedule_interval(
    health_pipeline,
    minutes=5,
    pipeline_id="health_check_5m"
)
```

### 13.3. Planification Dynamique

Créer et annuler des jobs à la volée:

```python
# Planifier on-demand
job_id = await scheduler.schedule(
    pipeline,
    run_at=datetime.now() + timedelta(minutes=10)
)

# Annuler si plus nécessaire
await scheduler.remove_job(job_id)
```

### 13.4. Pipelines en Chaîne

Pipeline A déclenche Pipeline B via scheduling:

```python
@broker.task
async def pipeline_a_finished(result):
    # Schedule pipeline B after completion of A
    job_id = await scheduler.schedule_at(
        pipeline_b,
        run_at=datetime.now() + timedelta(minutes=5)
    )
    return job_id
```

---

## 14. Dépannage

### Jobs Ne Lancés Pas

**Symptôme** : Les jobs planifiés ne s'exécutent jamais.

**Corrections** :
- Vérifier `await scheduler.start()` est appelé
- Vérifier validité expression cron: `CronTrigger.from_crontab("* * * * *")`
- Vérifier timezone correspond à l'heure attendue (vérifier TZ serveur)
- Confirmer job bien planifié (job_id non None)
- Vérifier logs scheduler pour erreurs

### Exécution Dupliquée

**Symptôme** : Même job s'exécute fois multiples concurremment.

**Corrections** :
- Définir `max_instances=1` dans trigger
- Utiliser `coalesce=True` pour combiner runs manqués
- S'assurer qu'une seule instance de scheduler tourne (HA a besoin de store partagé)

### Persistance Job Store Ne Fonctionne Pas

**Symptôme** : Jobs disparaissent après restart malgré store sqlite.

**Corrections** :
- Utiliser `store="sqlite"` et spécifier `store_path`
- S'assurer que le chemin de fichier est accessible et persiste entre redémarrages
- Ne pas mélanger stores memory et sqlite dans même app

### Problèmes Timezone

**Symptôme** : Job s'exécute à mauvaise heure (décalage de plusieurs heures).

**Corrections** :
- Définir timezone explicite sur scheduler: `PipelineScheduler(broker, timezone="UTC")`
- Ou sur trigger: `CronTrigger(hour=9, timezone=pytz.timezone("America/New_York"))`
- Vérifier timezone système du serveur correspond aux attentes

---

## 15. Résumé

PipelineScheduler fournit planification robuste, production-ready :

| Fonctionnalité | API |
|----------------|-----|
| **Cron** | `scheduler.schedule(pipeline, cron="* * * * *")` |
| **Intervalle** | `scheduler.schedule_interval(pipeline, minutes=5)` |
| **One-off** | `scheduler.schedule_at(pipeline, run_at=datetime)` |
| **Gestion** | `list_jobs()`, `remove_job()`, `pause_job()` |
| **Persistance** | SQLite (mono-worker), PostgreSQL/MySQL (multi-worker) |
| **Tracking** | Automatique avec PipelineTrackingManager |
| **Concurrence** | `max_instances`, `coalesce` contrôles |

**Setup production typique**:

```python
tracking = PipelineTrackingManager().with_storage(RedisPipelineStorage(redis))
pipeline = Pipeline(broker).with_tracking(tracking)

scheduler = PipelineScheduler(
    broker,
    job_store_url="postgresql+asyncpg://user:pass@host/taskiq_flow",  # pragma: allowlist secret
)
await scheduler.start()

# Schedule your jobs...
```

---

## Prochaines Étapes

- **[Guide de Retry]({{ '/fr/guides/retry/' | relative_url }})** — Récupération d'erreur et politiques de retry
- **[Guide de Performance]({{ '/fr/guides/performance/' | relative_url }})** — Optimiser performance pipelines planifiés
- **[Guide de Suivi]({{ '/fr/guides/tracking/' | relative_url }})** — Monitorer l'historique des jobs planifiés

---

*Planifiez des pipelines comme des cron jobs. Suivez-les comme jamais.*
