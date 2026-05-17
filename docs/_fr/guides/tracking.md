---
title: Guide de Suivi et Monitoring des Pipelines
nav_order: 23
---
# Guide de Suivi et Monitoring des Pipelines

**Suivi en temps réel et historique des exécutions avec PipelineTrackingManager**

> **Version**: 1.0.0 | **Lié** : [Guide d'Exécution]({{ '/fr/guides/execution/' | relative_url }}), [Guide WebSocket]({{ '/fr/guides/websocket/' | relative_url }})

---

## Aperçu

Taskiq-Flow offre des capacités complètes de suivi pour monitorer les exécutions de pipeline en temps réel et historiquement. Ce guide couvre :

- `PipelineTrackingManager` — Coordonnateur central de suivi
- Backends de stockage (Mémoire, Redis)
- Requêtes de statut et historique
- Collecte de métriques
- Écoute d'événements au niveau étape

---

## 1. Démarrage Rapide

```python
from taskiq_flow import Pipeline, PipelineTrackingManager

# Initialize tracking with automatic storage selection
tracking = PipelineTrackingManager().with_auto_storage(broker)

# Attach tracking to pipeline
pipeline = Pipeline(broker).with_tracking(tracking)

# Execute
task = await pipeline.kiq(data)
result = await task.wait_result()

# Query status
status = await tracking.get_status(pipeline.pipeline_id)
print(f"Status: {status.status}")        # COMPLETED
print(f"Steps: {len(status.steps)}")     # Number of steps executed
print(f"Duration: {status.duration_ms}ms")
```

C'est le pattern de base. Approfondissons.

---

## 2. PipelineTrackingManager

Le composant central pour enregistrer et récupérer les données d'exécution des pipelines.

### 2.1. Initialisation

```python
from taskiq_flow import PipelineTrackingManager, InMemoryPipelineStorage, RedisPipelineStorage

# Option 1: Auto-select based on broker (recommended)
tracking = PipelineTrackingManager().with_auto_storage(broker)
# Uses Redis if broker supports it, else falls back to Memory

# Option 2: Explicit memory storage (development only)
tracking = PipelineTrackingManager().with_storage(InMemoryPipelineStorage())

# Option 3: Explicit Redis storage (production)
tracking = PipelineTrackingManager().with_storage(
    RedisPipelineStorage(redis_client)
)

# Option 4: Custom storage backend
tracking = PipelineTrackingManager().with_storage(CustomStorage())
```

### 2.2. Durée de Vie du Stockage

- **InMemoryPipelineStorage** : Vit dans le processus Python seulement ; perdu au redémarrage
- **RedisPipelineStorage** : Persistant entre processus ; survit aux redémarrages

Choisir selon le déploiement：
- Développement local → Mémoire
- Production mono-worker → Mémoire (si pas de redémarrage)
- Multi-workers / distribué → Redis (ou autre stockage partagé)

---

## 3. Modèle de Statut de Pipeline

Chaque pipeline suivi produit un objet `PipelineStatus`:

```python
from taskiq_flow.tracking.models import PipelineStatus

statut: PipelineStatus
```

**Champs**：

| Champ | Type | Description |
|-------|------|-------------|
| `pipeline_id` | `str` | Identifiant unique de l'instance de pipeline |
| `statut` | `str` | `EN_ATTENTE`, `EN_COURSE`, `TERMINÉ`, `ÉCHOUÉ`, `ANNULÉ` |
| `pipeline_type` | `str` | `"sequential"` ou `"dataflow"` |
| `démarré_à` | `datetime` | Horodatage de début d'exécution |
| `terminé_à` | `datetime` | Horodatage de fin (si terminé) |
| `durée_ms` | `float` | Temps d'exécution total en millisecondes |
| `étapes` | `list[StepStatus]` | Détail par étape |
| `résultat` | `Any` | Valeur de retour finale (si terminé) |
| `erreur` | `str` | Message d'erreur (si échoué) |

**Champs StepStatus**:

| Champ | Type | Description |
|-------|------|-------------|
| `step_name` | `str` | Nom de la tâche |
| `statut` | `str` | `EN_ATTENTE`, `EN_COURSE`, `TERMINÉ`, `ÉCHOUÉ` |
| `démarré_à` | `datetime` | Heure de début d'étape |
| `terminé_à` | `datetime` | Heure de fin d'étape |
| `durée_ms` | `float` | Temps d'exécution de l'étape |
| `résultat` | `Any` | Valeur de retour de l'étape |
| `erreur` | `str` | Message d'erreur si échec |

---

## 4. Interrogation des Statuts

### 4.1. Obtenir le Statut d'un Pipeline

```python
status = await tracking.get_status(pipeline_id)

if status.status == "COMPLETED":
    print(f"Pipeline completed in {status.duration_ms}ms")
    print(f"Result: {status.result}")
elif status.status == "FAILED":
    print(f"Failed: {status.error}")
```

### 4.2. Lister Tous les Pipelines

```python
all_statuses = await tracking.list_pipelines()
for status in all_statuses:
    print(f"{status.pipeline_id}: {status.status}")
```

### 4.3. Filtrer par Statut

```python
running = await tracking.list_pipelines(filter_status="RUNNING")
failed = await tracking.list_pipelines(filter_status="FAILED")
completed = await tracking.list_pipelines(filter_status="COMPLETED")
```

### 4.4. Obtenir l'Historique

```python
# Get last 10 pipelines
history = await tracking.get_history(limit=10)

# Filter by date range
from datetime import datetime, timedelta
week_ago = datetime.now() - timedelta(days=7)
recent = await tracking.get_history(since=week_ago)
```

### 4.5. Supprimer les Anciens Enregistrements

```python
# Delete records older than 30 days
deleted = await tracking.cleanup_old(days=30)
print(f"Deleted {deleted} old pipeline records")

# Delete specific pipeline
await tracking.delete_pipeline(pipeline_id)
```

---

## 5. Backends de Stockage

### 5.1. InMemoryPipelineStorage

```python
from taskiq_flow.tracking import InMemoryPipelineStorage

storage = InMemoryPipelineStorage()
tracking = PipelineTrackingManager().with_storage(storage)

# Data lives only in Python process
# Lost on restart
# Suitable for: development, testing, one-shot scripts
```

**Avantages**：
- Zéro configuration
- Rapide (pas d'I/O réseau)
- Simple

**Inconvénients**：
- Non partageable entre workers
- Perdu au redémarrage
- Taille d'historique limitée

### 5.2. RedisPipelineStorage

```python
from taskiq_flow.tracking import RedisPipelineStorage
import redis.asyncio as redis

client_redis = redis.Redis(host="localhost", port=6379, decode_responses=True)
stockage = RedisPipelineStorage(client_redis)
tracking = PipelineTrackingManager().with_storage(stockage)
```

**Configuration**：

```python
# Avec préfixe de clé et TTL personnalisés
stockage = RedisPipelineStorage(
    client_redis,
    key_prefix="taskiq_flow:suivi:",
    ttl_secondes=604800  # rétention 7 jours
)
```

**Avantages**：
- Partagé entre multiples workers
- Persiste au redémarrage
- Évolutif
- Peut être en cluster pour haute disponibilité

**Inconvénients**：
- Requiert un serveur Redis
- Latence réseau
- Gestion TTL nécessaire (éviter croissance illimitée)

### 5.3. Stockage Personnalisé

Implémenter le protocole `TrackingStorage`:

```python
from taskiq_flow.tracking.storage import TrackingStorage
from taskiq_flow.tracking.models import PipelineStatus

class PostgresStorage(TrackingStorage):
    async def save_status(self, status: PipelineStatus):
        # Insert/update in PostgreSQL
        pass

    async def get_status(self, pipeline_id: str) -> PipelineStatus | None:
        # Fetch from DB
        pass

    async def list_pipelines(self, filter_status: str | None = None):
        # Query with optional filter
        pass

    async def delete_pipeline(self, pipeline_id: str):
        # Remove record
        pass

tracking = PipelineTrackingManager().with_storage(PostgresStorage())
```

---

## 6. Suivi en Temps Réel avec WebSocket

Pour des mises à jour de tableau de bord en direct, combiner `PipelineTrackingManager` avec `HookManager`:

```python
from taskiq_flow.hooks import HookManager, TrackingEventBroadcaster

hook_manager = HookManager()
broadcaster = TrackingEventBroadcaster(tracking, hook_manager)
tracking.add_listener(broadcaster.on_status_update)

pipeline = Pipeline(broker).with_hooks(hook_manager).with_tracking(tracking)
```

Les événements de pipeline sont maintenant diffusés via WebSocket en temps réel.

Voir [Guide WebSocket]({{ '/fr/guides/websocket/' | relative_url }}) pour la configuration complète。

---

## 7. Collecte de Métriques

Collecter des statistiques de performance au fil du temps:

```python
# Collect statistics
stats = await tracking.get_metrics(days=7)

print(f"Total executions: {stats.total_pipelines}")
print(f"Success rate: {stats.success_rate:.1%}")
print(f"Avg duration: {stats.avg_duration_ms:.0f}ms")
print(f"Failure reasons: {stats.failure_reasons}")
```

**Métriques courantes**：

- Débit (pipelines/minute)
- Ratio succès/échec
- Durée moyenne des étapes
- Étapes les plus longues
- Heures de pointe

Intégrer avec des systèmes de monitoring (Prometheus, Grafana):

```python
from prometheus_client import Counter, Histogram

PIPELINES_TOTAL = Counter('pipelines_total', 'Total pipelines', ['status'])
PIPELINE_DURATION = Histogram('pipeline_duration_seconds', 'Pipeline execution duration')

class PrometheusExporter:
    async def on_pipeline_complete(self, status: PipelineStatus):
        PIPELINES_TOTAL.labels(status=status.status).inc()
        PIPELINE_DURATION.observe(status.duration_ms / 1000)
```

---

## 8. Écouteurs d'Événements

Attacher des callbacks aux événements de suivi:

```python
class MyListener:
    async def on_pipeline_start(self, pipeline_id: str):
        print(f"Pipeline {pipeline_id} started")
        send_slack_notification(f"Pipeline {pipeline_id} started")

    async def on_step_complete(self, pipeline_id: str, step_name: str, result: Any):
        log_step_metric(step_name, result)

    async def on_pipeline_complete(self, pipeline_id: str, status: PipelineStatus):
        if status.status == "FAILED":
            alert_failure(pipeline_id)

listener = MyListener()
tracking.add_listener(listener)
```

**Méthodes d'écouteur** (toutes optionnelles):

- `on_pipeline_start(pipeline_id: str)`
- `on_step_start(pipeline_id: str, step_name: str)`
- `on_step_complete(pipeline_id: str, step_name: str, résultat: Any)`
- `on_pipeline_complete(pipeline_id: str, statut: PipelineStatus)`
- `on_pipeline_error(pipeline_id: str, erreur: str)`

---

## 9. Visualisation des Données de Suivi

### 9.1. Sortie Console

```python
status = await tracking.get_status(pipeline_id)
print(f"\n{'='*60}")
print(f"Pipeline: {status.pipeline_id}")
print(f"Status: {status.status}")
print(f"Duration: {status.duration_ms:.0f}ms")
print(f"Steps:")
for step in status.steps:
    bar = "█" * int(step.duration_ms / 10)
    print(f"  {step.step_name:<30} {bar} {step.duration_ms:.0f}ms")
```

### 9.2. JSON Export

```python
import json
status_dict = status.model_dump(mode="json", exclude={"result"})  # exclude large results
print(json.dumps(status_dict, indent=2, default=str))
```

### 9.3. Intégration avec Tableaux de Bord

Utiliser les endpoints API REST (voir [Guide API]({{ '/fr/guides/api/' | relative_url }})) pour construire des tableaux de bord personnalisés:

```javascript
// Frontend fetch
fetch('/api/pipelines/{pipeline_id}/status')
  .then(res => res.json())
  .then(statut => {
    // Rendre graphique temporel des durées d'étapes
    // Afficher badges succès/échec
  });
```

---

## 10. Meilleures Pratiques de Production

### 10.1. Utiliser Redis en Production

Toujours utiliser `RedisPipelineStorage` en production:

```python
# config.py
URL_REDIS = os.getenv("URL_REDIS", "redis://localhost:6379")

# app.py
from redis.asyncio import Redis
client_redis = Redis.from_url(REDIS_URL)
tracking = PipelineTrackingManager().with_storage(
    RedisPipelineStorage(client_redis, ttl_seconds=2592000)  # 30 days
)
```

### 10.2. Configurer des Politiques de Rétention

```python
# Periodic cleanup job (daily)
async def cleanup_old_tracking():
    deleted = await tracking.cleanup_old(days=7)
    print(f"Cleaned up {deleted} old pipeline records")

# Use APScheduler to run daily
from taskiq_flow import PipelineScheduler
scheduler = PipelineScheduler(broker)
scheduler.schedule_at(cleanup_old_tracking, run_at="0 3 * * *")  # 3am daily
```

### 10.3. Monitor Tracking Health

```python
# Health check for monitoring systems
async def health_check():
    try:
        test_pipeline = Pipeline(broker).with_tracking(tracking)
        await test_pipeline.kiq("health_check")
        return {"status": "healthy"}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}
```

### 10.4. Limiter la Taille de l'Historique

```python
# Keep only last N pipelines per pipeline_id pattern
import fnmatch

patterns = ["batch_job_*", "etl_*"]
for pattern in patterns:
    old = await tracking.list_pipelines()
    matches = [p for p in old if fnmatch.fnmatch(p.pipeline_id, pattern)]
    if len(matches) > 100:
        for old_pipeline in matches[-100:]:
            await tracking.delete_pipeline(old_pipeline.pipeline_id)
```

---

## 11. Dépannage

### Erreur "Aucun stockage configuré"

**Symptôme** : `RuntimeError: No tracking storage configured`

**Solution** : Add storage before using tracking:

```python
tracking = PipelineTrackingManager().with_auto_storage(broker)
# or
tracking = PipelineTrackingManager().with_storage(InMemoryPipelineStorage())
```

### Missing Tracking Data

**Symptom**: `get_status()` returns `None` even though pipeline ran

**Causes & fixes**:

1. **Tracking not attached**:
   ```python
   pipeline = Pipeline(broker).with_tracking(tracking)  # Must call with_tracking()
   ```

2. **Different brokers** — Ensure same `broker` instance between task and pipeline.

3. **Storage lifetime** — In-memory storage lost on restart; switch to Redis.

4. **Pipeline ID mismatch** — Confirm `pipeline.pipeline_id` matches the query.

### Dégradation des Performance avec Redis

**Symptôme** : Le suivi ralentit l'exécution du pipeline

**Correctifs**：
- Utiliser le pooling de connexions Redis
- Mettre à jour les statuts en batch (regrouper plusieurs étapes)
- Écritures batch asynchrones (comportement par défaut)
- Augmenter `maxmemory` Redis et utiliser politique d'éviction appropriée

---

## 12. Résumé

| Fonctionnalité | Mémoire | Redis |
|----------------|---------|-------|
| **Multi-processus** |  Non |  Oui |
| **Persistant** |  Non |  Oui |
| **État partagé** |  Non |  Oui |
| **Vitesse** |  Plus rapide |  Rapide (réseau) |
| **Configuration requise** | Aucune | Serveur Redis |

**Recette basique**:
```python
suivi = PipelineTrackingManager().with_auto_storage(broker)
pipeline = Pipeline(broker).with_tracking(suivi)
```

**Recette production**:
```python
suivi = PipelineTrackingManager().with_storage(
    RedisPipelineStorage(client_redis, ttl_secondes=604800)
)
pipeline = Pipeline(broker).with_tracking(suivi)
```

---

## Prochaines Étapes

- **[Streaming WebSocket]({{ '/fr/guides/websocket/' | relative_url }})** — Livraison d'événements en direct pour tableaux de bord
- **[Guide Dataflow]({{ '/fr/guides/dataflow/' | relative_url }})** — Pipeline DAG complet avec parallélisme automatique
- **[Planification]({{ '/fr/guides/scheduling/' | relative_url }})** — Exécution périodique automatique de pipelines
- **[Performance]({{ '/fr/guides/performance/' | relative_url }})** — Optimiser la surcharge de suivi

---

*Tout suivre. Visualiser avec [WebSocket]({{ '/fr/guides/websocket/' | relative_url }}).*
