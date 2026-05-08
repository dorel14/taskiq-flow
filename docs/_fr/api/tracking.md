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

suivi = PipelineTrackingManager()
suivi = suivi.with_auto_storage(broker)
# ou
suivi = suivi.with_storage(InMemoryPipelineStorage())
```

**Configuration**:

```python
suivi = PipelineTrackingManager(
    storage=None,         # Stockage pré-configuré optionnel
    max_history=1000,    # Max enregistrements pipelines (store mémoire seulement)
    auto_cleanup=True    # Purge automatique anciens enregistrements
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
pipeline = Pipeline(broker).with_tracking(suivi)
# ou
pipeline.with_tracking(suivi)  # modification in-place
```

Le gestionnaire de suivi **doit** être attaché **avant** l'appel à `pipeline.kiq()`.

### Interroger les Statuts

```python
# Obtenir statut d'une exécution pipeline spécifique
statut = await suivi.get_status(pipeline_id: str) -> PipelineStatus | None

# Lister tous les pipelines suivis
tous_statuts = await suivi.list_pipelines(
    filtre_statut: str | None = None,  # Filtrer par statut
    limit: int = 100
) -> list[PipelineStatus]

# Obtenir historique exécutions
historique = await suivi.get_historique(
    depuis: datetime | None = None,
    jusqu_à: datetime | None = None,
    limit: int = 100
) -> list[PipelineStatus]
```

### Maintenance

```python
# Supprimer enregistrement pipeline spécifique
await suivi.delete_pipeline(pipeline_id: str)

# Supprimer enregistrements plus vieux que N jours
supprimés = await suivi.cleanup_older_than(days: int = 30) -> int

# Obtenir métriques agrégées
métriques = await suivi.get_metrics(
    days: int = 7
) -> TrackingMetrics
```

### Écouteurs d'Événements

```python
class MonÉcouteur:
    async def on_pipeline_start(self, pipeline_id: str):
        print(f"Pipeline {pipeline_id} démarré")

    async def on_pipeline_complete(self, pipeline_id: str, statut: PipelineStatus):
        alerter_si_échec(statut)

écouteur = MonÉcouteur()
suivi.add_listener(écouteur)
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

stockage = InMemoryPipelineStorage(max_records=1000)
suivi = PipelineTrackingManager().with_storage(stockage)
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
suivi = PipelineTrackingManager().with_storage(stockage)
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

class StockagePostgres(TrackingStorage):
    async def save_status(self, statut: PipelineStatus):
        """Sauvegarder ou mettre à jour statut pipeline."""
        ...

    async def get_status(self, pipeline_id: str) -> PipelineStatus | None:
        """Récupérer statut pipeline par ID."""
        ...

    async def list_pipelines(self, filtre_statut: str | None = None,
                             limit: int = 100) -> list[PipelineStatus]:
        """Lister pipelines, optionnellement filtrés par statut."""
        ...

    async def delete_pipeline(self, pipeline_id: str):
        """Supprimer enregistrement pipeline."""
        ...

    async def cleanup_older_than(self, days: int) -> int:
        """Supprimer enregistrements plus vieux que N jours. Retourne compte supprimés."""
        ...

suivi = PipelineTrackingManager().with_storage(StockagePostgres())
```

Toutes méthodes stockage doivent être async.

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
| `get_status()` retourne `None` | Suivi non attaché, ou `pipeline_id` incorrect | S'assurer `pipeline.with_tracking(suivi)` appelé avant `kiq()` |
| Erreurs de stockage | Connexion Redis échouée | Vérifier Redis tourne, chaîne connexion valide |
| Croissance mémoire (store mémoire) | Pas de purge anciens enregistrements | Définir `max_records` ou utiliser Redis avec TTL |
| Écouteurs ne se déclenchent pas | Non ajoutés avant démarrage pipeline | Appeler `suivi.add_listener()` avant `pipeline.kiq()` |

---

*Combiner avec [WebSocket]({{ '/fr/api/websocket/' | relative_url }}) pour streaming temps réel. Voir [Guide de Suivi]({{ '/fr/guides/tracking/' | relative_url }}) pour patterns d'utilisation.*
