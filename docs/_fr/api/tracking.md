---
permalink: /fr/api/tracking/
title: Référence API: Suivi & Monitoring
nav_order: 33
color_scheme: dark
---
# Référence API: Suivi & Monitoring

**PipelineTrackingManager, backends de stockage, et modèles de statut**

> **Version** : {VERSION} | **Module** : `taskiq_flow.tracking`, `taskiq_flow.tracking.models`

---

## PipelineTrackingManager

Coordinateur central pour enregistrer et récupérer les données d'exécution des pipelines.

```python
from taskiq_flow import PipelineTrackingManager

tracking = PipelineTrackingManager()
tracking = tracking.with_auto_storage(broker)
# or
tracking = tracking.with_storage(InMemoryPipelineStorage())
```

**Configuration**:

```python
tracking = PipelineTrackingManager(
    storage=None,         # Optional pre-configured storage
    max_history=1000,    # Max pipeline records (memory store only)
    auto_cleanup=True    # Auto-purge old records
)
```

**Sélection stockage** (via `with_auto_storage`):

| Broker | Stockage auto-sélectionné |
|--------|-------------------------|
| `InMemoryBroker` | `InMemoryPipelineStorage` |
| `RedisBroker` | `RedisPipelineStorage` |
| Autre | Fallback mémoire |

---

## Méthodes

### Attacher aux Pipelines

```python
pipeline = Pipeline(broker).with_tracking(tracking)
# or
pipeline.with_tracking(tracking)  # in-place modification
```

The tracking manager **must** be attached **before** calling `pipeline.kiq()`.

### Interroger les Statuts

```python
# Get status of specific pipeline execution
status = await tracking.get_status(pipeline_id: str) -> PipelineStatus | None

# List all tracked pipelines
all_statuses = await tracking.list_pipelines(
    filter_status: str | None = None,  # Filter by status
    limit: int = 100
) -> list[PipelineStatus]

# Get execution history
history = await tracking.get_history(
    since: datetime | None = None,
    until: datetime | None = None,
    limit: int = 100
) -> list[PipelineStatus]
```

### Maintenance

```python
# Delete specific pipeline record
await tracking.delete_pipeline(pipeline_id: str)

# Delete records older than N days
deleted = await tracking.cleanup_older_than(days: int = 30) -> int

# Get aggregated metrics
metrics = await tracking.get_metrics(
    days: int = 7
) -> TrackingMetrics
```

### Event Listeners

```python
class MyListener:
    async def on_pipeline_start(self, pipeline_id: str):
        print(f"Pipeline {pipeline_id} démarré")

    async def on_pipeline_complete(self, pipeline_id: str, status: PipelineStatus):
        alert_if_failed(status)

listener = MyListener()
tracking.add_listener(listener)
```

**Hooks écouteur** (tous optionnels):

- `on_pipeline_start(pipeline_id)`
- `on_step_start(pipeline_id, step_name)`
- `on_step_complete(pipeline_id, step_name, result)`
- `on_pipeline_complete(pipeline_id, statut)`
- `on_pipeline_error(pipeline_id, error)`

---

## Backends de Stockage

### InMemoryPipelineStorage

```python
from taskiq_flow.tracking import InMemoryPipelineStorage

storage = InMemoryPipelineStorage(max_records=1000)
tracking = PipelineTrackingManager().with_storage(storage)
```

**Characteristics**:
- Zero configuration
- Fast (no I/O)
- **Not shared between workers**
- Lost on process restart
- Good for: development, testing, single-process

**Parameters**:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `max_records` | `int` | 1000 | Maximum pipeline records to keep (LRU eviction) |

---

### RedisPipelineStorage

```python
from taskiq_flow.tracking import RedisPipelineStorage
import redis.asyncio as redis

redis_client = redis.Redis(host="localhost", port=6379, decode_responses=True)
storage = RedisPipelineStorage(
    redis_client,
    key_prefix="taskiq_flow:tracking:",
    ttl_seconds=604800  # 7 days
)
tracking = PipelineTrackingManager().with_storage(storage)
```

**Characteristics**:
- Shared between multiple workers
- Survives restarts
- Scalable (Redis cluster)
- TTL-based expiration
- Good for: production, distributed deployments

**Parameters**:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `redis_client` | `Redis` | **required** | Connected Redis client |
| `key_prefix` | `str` | `"taskiq_flow:tracking:"` | Prefix for all keys |
| `ttl_seconds` | `int` | 604800 (7d) | Automatic expiration after N seconds |
| `serializer` | `Callable` | `json.dumps` | Custom serialization function |
```

**Caractéristiques**:
- Zéro configuration
- Rapide (pas d'I/O)
- **Non partagé entre workers**
- Perdu au redémarrage du processus
- Bon pour: développement, tests, mono-processus

**Paramètres**:

| Paramètre | Type | Défaut | Description |
|-----------|------|---------|-------------|
| `max_records` | `int` | 1000 | Max enregistrements pipelines à retenir (éviction LRU) |

---

### RedisPipelineStorage

```python
from taskiq_flow.tracking import RedisPipelineStorage
import redis.asyncio as redis

client_redis = redis.Redis(host="localhost", port=6379, decode_responses=True)
stockage = RedisPipelineStorage(
    client_redis,
    key_prefix="taskiq_flow:tracking:",
    ttl_seconds=604800  # 7 jours
)
tracking = PipelineTrackingManager().with_storage(storage)
```

**Caractéristiques**:
- Partagé entre multiples workers
- Persiste au redémarrage
- Évolutif (cluster Redis)
- Expiration basée TTL
- Bon pour: production, déploiements distribués

**Paramètres**:

| Paramètre | Type | Défaut | Description |
|-----------|------|---------|-------------|
| `client_redis` | `Redis` | **requis** | Client Redis connecté |
| `key_prefix` | `str` | `"taskiq_flow:tracking:"` | Préfixe pour toutes clés |
| `ttl_seconds` | `int` | 604800 (7j) | Expiration automatique après N secondes |
| `serializer` | `Callable` | `json.dumps` | Fonction de sérialisation personnalisée |


---

## Modèles de Données

### PipelineStatus

Statut complet d'une exécution de pipeline.

```python
from taskiq_flow.tracking.models import PipelineStatus

statut: PipelineStatus
```

**Attributs**:

| Attribut | Type | Description |
|----------|------|-------------|
| `pipeline_id` | `str` | Identifiant unique |
| `status` | `str` | `PENDING`, `RUNNING`, `COMPLETED`, `FAILED`, `CANCELLED` |
| `pipeline_type` | `str` | `"sequential"` ou `"dataflow"` |
| `started_at` | `datetime` | Horodatage début exécution |
| `completed_at` | `datetime | None` | He fin si terminé |
| `duration_ms` | `float` | Durée totale en millisecondes |
| `steps` | `list[StepStatus]` | Objets statut par étape |
| `result` | `Any` | Valeur de retour finale (si terminé) |
| `error` | `str | None` | Message d'erreur si échec |

**Méthodes**:
- `model_dump()` — Retourne dictionnaire (modèle Pydantic)
- `is_finished()` — True si état terminal (COMPLETED/FAILED/CANCELLED)

---

### StepStatus

Statut d'une seule étape de pipeline.

```python
from taskiq_flow.tracking.models import StepStatus

étape: StepStatus
```

**Attributs**:

| Attribut | Type | Description |
|----------|------|-------------|
| `step_name` | `str` | Nom de la tâche |
| `status` | `str` | `PENDING`, `RUNNING`, `COMPLETED`, `FAILED` |
| `started_at` | `datetime` | Heure début étape |
| `completed_at` | `datetime | None` | Heure fin étape |
| `duration_ms` | `float` | Durée d'exécution |
| `result` | `Any` | Valeur de retour |
| `error` | `str | None` | Message d'erreur |
| `retry_count` | `int` | Nombre de tentatives de retry |

---

### TrackingMetrics

Statistiques agrégées (retournées par `get_metrics()`).

```python
from taskiq_flow.tracking.models import TrackingMetrics

métriques: TrackingMetrics
```

**Attributs**:

| Attribut | Type | Description |
|----------|------|-------------|
| `total_pipelines` | `int` | Total exécutions suivies |
| `completed` | `int` | Complétions réussies |
| `failed` | `int` | Exécutions échouées |
| `success_rate` | `float` | Ratio complété / total |
| `avg_duration_ms` | `float` | Durée moyenne pipeline |
| `p95_duration_ms` | `float` | Durée percentile 95 |
| `failure_reasons` | `dict[str, int]` | Type erreur → compte |
| `most_frequent_step` | `str  | None` | Étape échouant le plus souvent |

---

## Implémentation Stockage Personnalisé

Implémenter protocole `TrackingStorage` pour backend personnalisé:

```python
from taskiq_flow.tracking.storage import TrackingStorage
from taskiq_flow.tracking.models import PipelineStatus

class PostgresStorage(TrackingStorage):
    async def save_status(self, status: PipelineStatus):
        """Save status to PostgreSQL."""
        ...

    async def get_status(self, pipeline_id: str) -> PipelineStatus | None:
        """Fetch from DB."""
        ...

    async def list_pipelines(self, filter_status: str | None = None):
        """Query with optional filter."""
        ...

    async def delete_pipeline(self, pipeline_id: str):
        """Remove record."""
        ...

tracking = PipelineTrackingManager().with_storage(PostgresStorage())
```

All storage methods must be async.

---

## Meilleures Pratiques

1. **Production** : Toujours utiliser stockage Redis (partagé, persistant)
2. **TTL** : Définir TTL approprié (7–30 jours) pour limiter croissance stockage
3. **Écouteurs** : Ajouter écouteurs d'alerte pour échecs
4. **Nettoyage** : Planifier nettoyage périodique (cron quotidien)
5. **Indexation** : Pour stores DB personnalisés, indexer sur `pipeline_id`, `started_at` pour performance requêtes

---

## Dépannage

| Problème | Cause Probable | Solution |
|----------|----------------|----------|
| `get_status()` returns `None` | Tracking not attached, or wrong `pipeline_id` | Ensure `pipeline.with_tracking(tracking)` called before `kiq()` |
| Storage errors | Redis connection failed | Check Redis is running, connection string valid |
| Memory growth (memory store) | No old record cleanup | Set `max_records` or use Redis with TTL |
| Listeners not firing | Not added before pipeline start | Call `tracking.add_listener()` before `pipeline.kiq()` |

---

*Combiner avec [WebSocket]({{ '/fr/api/websocket/' | relative_url }}) pour streaming temps réel. Voir [Guide de Suivi]({{ '/fr/guides/tracking/' | relative_url }}) pour patterns d'utilisation.*
