---
permalink: /fr/api/websocket/
title: 'Référence API: Intégration WebSocket'
nav_order: 34
color_scheme: dark
---
# Référence API: Intégration WebSocket

**HookManager, pont WebSocket, et transport d'événements temps réel via FastAPI**

> **Version** : {VERSION} | **Module** : `taskiq_flow.hooks`, `taskiq_flow.integration.websocket`

---

## HookManager

Bus central d'événements qui collecte les événements d'exécution de pipeline et les dispatch aux abonnés.

```python
from taskiq_flow.hooks import HookManager

hook_manager = HookManager()
pipeline = Pipeline(broker).with_hooks(hook_manager)
```

**Événements émis**:

- `PipelineStartEvent` — Exécution pipeline démarrée
- `StepStartEvent` — Une étape a démarré
- `StepCompleteEvent` — Une étape complétée
- `PipelineCompleteEvent` — Pipeline terminé avec succès
- `StepErrorEvent` — Une étape a échoué
- `PipelineErrorEvent` — Pipeline a échoué

### Ajouter Hooks Personnalisés

```python
class MonHook:
    async def on_pipeline_start(self, event):
        print(f"Pipeline {event.pipeline_id} démarré")

    async def on_step_complete(self, event):
        print(f"Étape {event.step_name} finie en {event.duration_ms}ms")

hook = MonHook()
hook_manager.add_hook(hook)
```

**Méthodes hooks** (toutes optionnelles):

| Méthode | Événement |
|----------|-----------|
| `on_pipeline_start(event: PipelineStartEvent)` | Pipeline démarré |
| `on_step_start(event: StepStartEvent)` | Étape en démarrage |
| `on_step_complete(event: StepCompleteEvent)` | Étape terminée |
| `on_pipeline_complete(event: PipelineCompleteEvent)` | Pipeline complété |
| `on_step_error(event: StepErrorEvent)` | Étape échouée |
| `on_pipeline_error(event: PipelineErrorEvent)` | Erreur pipeline |

---

## Types d'Événements

Tous événements sont modèles Pydantic avec discriminateur `type`.

### PipelineStartEvent

```python
from taskiq_flow.hooks import PipelineStartEvent

event = PipelineStartEvent(
    pipeline_id="mon_pipeline",
    pipeline_type="sequential",
    timestamp=datetime.now(),
    input=données_initiales
)
```

**Champs**:

| Champ | Type | Description |
|-------|------|-------------|
| `type` | `Literal["PipelineStartEvent"]` | Discriminateur type d'événement |
| `pipeline_id` | `str` | Instance pipeline |
| `pipeline_type` | `str` | `"sequential"` ou `"dataflow"` |
| `timestamp` | `datetime` | Horodatage événement |
| `input` | `Any` | Données d'entrée initiales |

---

### StepStartEvent

```python
from taskiq_flow.hooks import StepStartEvent

event = StepStartEvent(
    pipeline_id="mon_pipeline",
    step_name="process_data",
    step_index=2,
    task_id="task_abc123",
    timestamp=datetime.now()
)
```

**Champs**:

| Champ | Type | Description |
|-------|------|-------------|
| `type` | `Literal["StepStartEvent"]` | Type événement |
| `pipeline_id` | `str` | Pipeline origine |
| `step_name` | `str` | Nom de la tâche |
| `step_index` | `int` | Position dans pipeline (0-indexé) |
| `task_id` | `str` | ID tâche taskiq sous-jacente |
| `timestamp` | `datetime` | Horodatage événement |

---

### StepCompleteEvent

```python
from taskiq_flow.hooks import StepCompleteEvent

event = StepCompleteEvent(
    pipeline_id="mon_pipeline",
    step_name="process_data",
    step_index=2,
    result={"processed": 42},
    duration_ms=150.5,
    timestamp=datetime.now()
)
```

**Champs**:

| Champ | Type | Description |
|-------|------|-------------|
| `type` | `Literal["StepCompleteEvent"]` | Type événement |
| `pipeline_id` | `str` | Pipeline origine |
| `step_name` | `str` | Tâche complétée |
| `step_index` | `int` | Position étape |
| `result` | `Any` | Valeur de retour tâche |
| `duration_ms` | `float` | Temps d'exécution en millisecondes |
| `timestamp` | `datetime` | Horodatage événement |

---

### PipelineCompleteEvent

```python
from taskiq_flow.hooks import PipelineCompleteEvent

event = PipelineCompleteEvent(
    pipeline_id="mon_pipeline",
    pipeline_type="dataflow",
    status="COMPLETED",
    duration_ms=1250.3,
    result={"final": "output"},
    timestamp=datetime.now()
)
```

**Champs**:

| Champ | Type | Description |
|-------|------|-------------|
| `type` | `Literal["PipelineCompleteEvent"]` | Type événement |
| `pipeline_id` | `str` | ID pipeline |
| `pipeline_type` | `str` | Type pipeline |
| `status` | `str` | `"COMPLETED"`, `"FAILED"`, `"CANCELLED"` |
| `duration_ms` | `float` | Temps total exécution |
| `result` | `Any` | Résultat final pipeline |
| `timestamp` | `datetime` | Horodatage événement |

---

### Événements Erreur

```python
from taskiq_flow.hooks import StepErrorEvent, PipelineErrorEvent

erreur_étape = StepErrorEvent(
    pipeline_id="mon_pipeline",
    step_name="failing_task",
    error="ValueError: invalid input",
    timestamp=datetime.now()
)

erreur_pipeline = PipelineErrorEvent(
    pipeline_id="mon_pipeline",
    error="Pipeline abandonné après 3 échecs",
    timestamp=datetime.now()
)
```

---

## Intégration WebSocket (FastAPI)

Taskiq-Flow utilise une intégration WebSocket **FastAPI uniquement**. Les événements WebSocket sont servis via une route FastAPI `WebSocket` à `/ws/{pipeline_id}` dans votre application.

### setup_websocket_bridge

Connecte le système d'événements `HookManager` à la couche de transport WebSocket :

```python
from taskiq_flow.hooks import HookManager, setup_websocket_bridge

hook_manager = HookManager()
setup_websocket_bridge(hook_manager)
# Tous les événements du pipeline sont maintenant diffusés vers le transport WebSocket
```

Une fois le pont actif, montez l'endpoint WebSocket FastAPI dans votre application :

```python
from fastapi import FastAPI, WebSocket
from taskiq_flow.integration.websocket.fastapi_ws import fastapi_websocket_endpoint

app = FastAPI()

@app.websocket("/ws/{pipeline_id}")
async def ws_endpoint(websocket: WebSocket, pipeline_id: str):
    await fastapi_websocket_endpoint(websocket, pipeline_id)
```

Les clients se connectent à `ws://<host>:<port>/ws/<pipeline_id>` et reçoivent les événements du pipeline en temps réel.

### get_fastapi_ws_manager

Obtenir l'instance singleton `FastAPIWebSocketManager` :

```python
from taskiq_flow.integration.websocket.fastapi_ws import get_fastapi_ws_manager

manager = get_fastapi_ws_manager()
print(f"Canaux actifs : {manager.get_channel_ids()}")
print(f"Nombre de clients : {manager.get_client_count('pipeline_1')}")
```

### fastapi_websocket_endpoint

Gestionnaire de route WebSocket FastAPI. Utilisez-le comme callback de route :

```python
from taskiq_flow.integration.websocket.fastapi_ws import fastapi_websocket_endpoint

@app.websocket("/ws/{pipeline_id}")
async def ws_endpoint(websocket: WebSocket, pipeline_id: str):
    await fastapi_websocket_endpoint(websocket, pipeline_id)
```

L'endpoint accepte la connexion, souscrit le client au canal `pipeline_id`, suit la métrique de connexion et se déconnecte proprement à la déconnexion du client.

---

### get_websocket_server ⚠️ Déprécié

:::warning Déprécié depuis v1.1
La fonction `get_websocket_server()` et la classe `PipelineWebSocketServer` ont été supprimées en v1.1. Le serveur picows autonome n'est plus supporté.
:::

```python
from taskiq_flow.integration.websocket import get_websocket_server

serveur = get_websocket_server(
    host="0.0.0.0",
    port=8765,
    transport=None  # Utilise WebSocketTransport par défaut
)
await serveur.start_server()
```

:::danger Lève RuntimeError
Appeler `get_websocket_server()` lève systématiquement `RuntimeError`. Utilisez l'intégration FastAPI à la place :

```python
from fastapi import FastAPI, WebSocket
from taskiq_flow.integration.websocket.fastapi_ws import fastapi_websocket_endpoint

app = FastAPI()

@app.websocket("/ws/{pipeline_id}")
async def ws_endpoint(websocket: WebSocket, pipeline_id: str):
    await fastapi_websocket_endpoint(websocket, pipeline_id)
```
:::

---

## Filtrage d'Événements

Réduire le trafic en filtrant les événements :

```python
from taskiq_flow.hooks import EventFilter

# Pipelines spécifiques uniquement
filtre = EventFilter(pipeline_ids=["pipeline_1", "pipeline_2"])

# Événements d'étape uniquement
filtre = EventFilter(event_types=["StepStartEvent", "StepCompleteEvent"])

# Les deux
filtre = EventFilter(
    pipeline_ids=["*"],  # tous les pipelines (ou spécifiques)
    event_types=["StepCompleteEvent", "PipelineCompleteEvent"]
)

hook_manager.add_filter(filtre)
```

### Logique EventFilter

```
Événement → Vérifier correspondance pipeline_id ? → Vérifier correspondance event_type ? → Émettre ?
```

Les deux filtres sont **ET** logique interne : un événement passe s'il correspond *aux deux* filtres pipeline_ids ET event_types. Utiliser `"*"` pour tout matcher.

---

## Protocole WebSocket

### Connexion

Les clients se connectent via WebSocket standard vers la route FastAPI :

```
ws://localhost:8000/ws/mon_pipeline
```

Pour connexions sécurisées (WSS), terminer TLS au reverse proxy (nginx, Traefik).

### Abonnement

Après connexion, le client est déjà abonné au pipeline identifié par le chemin de l'URL. Messages d'abonnement supplémentaires possibles :

```json
{
  "type": "subscribe",
  "pipeline_id": "mon_pipeline"
}
```

Abonnement wildcard :

```json
{
  "type": "subscribe",
  "pipeline_id": "*"
}
```

Désabonnement :

```json
{
  "type": "unsubscribe",
  "pipeline_id": "mon_pipeline"
}
```

### Format Message (Serveur → Client)

Tous messages sont JSON avec champ `type` :

```json
{
  "type": "StepCompleteEvent",
  "pipeline_id": "pipeline_123",
  "step_name": "process_data",
  "duration_ms": 150.2,
  "timestamp": "2026-05-05T16:30:00Z"
}
```

Référence complète des champs dans le [Guide WebSocket]({{ '/fr/guides/websocket/' | relative_url }}).

---

## Route FastAPI de Référence

### Montage de l'Endpoint WebSocket

```python
from fastapi import FastAPI, WebSocket
from taskiq_flow.integration.websocket.fastapi_ws import fastapi_websocket_endpoint

app = FastAPI()

@app.websocket("/ws/{pipeline_id}")
async def ws_endpoint(websocket: WebSocket, pipeline_id: str):
    await fastapi_websocket_endpoint(websocket, pipeline_id)
```

### Interrogation des Clients Connectés

```python
from taskiq_flow.integration.websocket.fastapi_ws import get_fastapi_ws_manager

manager = get_fastapi_ws_manager()
nb_clients = manager.get_client_count("pipeline_1")
```

Les endpoints REST complets pour l'interrogation des clients et pipelines sont disponibles dans le module de routes API :

```python
from taskiq_flow.api.routes.websocket import router as ws_router
app.include_router(ws_router, prefix="/api")
```

---

## Coordination Multi-Worker

Pour plusieurs workers partageant l'état des événements via Redis :

```python
from taskiq_flow.transport import RedisPubSubTransport
from taskiq_flow.hooks.bridge import setup_websocket_bridge
import redis

redis_client = redis.Redis(host="localhost", port=6379)
transport = RedisPubSubTransport(redis_client)
setup_websocket_bridge(hook_manager, use_fastapi=True)
```

Tous les workers souscrivent au même canal Redis pub/sub ; les événements de n'importe quel worker sont diffusés à tous les clients WebSocket connectés à n'importe quel worker. Les connexions WebSocket sont toujours servies via l'application FastAPI.

> **Prérequis** : Installer l'extra `[brokers]` : `pip install "taskiq-flow[brokers]"` pour le support Redis.

---

## Considérations de Production

### Limites de Connexion

```python
import asyncio

# Limiter les connexions WebSocket concurrentes
MAX_CONNECTIONS = 1000
sémaphore = asyncio.Semaphore(MAX_CONNECTIONS)

# Dans le gestionnaire de connexion :
async def handle_connection(websocket):
    if not sémaphore.acquire(blocking=False):
        await websocket.close(code=1013, reason="Trop de connexions")
        return
    try:
        await websocket_service.handle(websocket)
    finally:
        sémaphore.release()
```

### Arrêt Gracieux

```python
async def shutdown():
    manager = get_fastapi_ws_manager()
    await manager.close_all()
```

### Monitoring

Exposer les métriques via l'endpoint de métriques intégré :

```python
@app.get("/ws/metrics")
async def ws_metrics():
    manager = get_fastapi_ws_manager()
    return {
        "canaux": manager.get_channel_ids(),
    }
```

### SSL/TLS

Terminez TLS au niveau d'un reverse proxy (nginx, Traefik) — jamais directement dans l'application :

```nginx
server {
    listen 443 ssl;
    location /ws {
        proxy_pass http://localhost:8000;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
    }
}
```

> **Note** : Le `PipelineWebSocketServer` historique avec SSL inline (`ssl_cert`/`ssl_key`) a été supprimé en v1.1. Utilisez un reverse proxy pour la terminaison TLS.

---

## Dépannage

| Problème | Diagnostic | Correction |
|----------|------------|------------|
| Clients ne reçoivent événements | `setup_websocket_bridge()` non appelé | Appeler avant démarrage pipeline |
| Connexion refusée | App FastAPI non démarrée | Lancer avec `uvicorn app:app` |
| Événements retardés | Filtre événements bloque | Vérifier configuration filtre |
| CPU élevé | Trop de connexions | Appliquer limites connexions |

---

## Résumé

| Composant | Rôle |
|-----------|------|
| `HookManager` | Collecte événements depuis pipelines |
| Sous-classes `BaseEvent` | Données événements structurées |
| `EventFilter` | Diffusion sélective événements |
| `setup_websocket_bridge()` | Connecte HookManager au transport WebSocket |
| `FastAPIWebSocketManager` | Gestion des clients WebSocket (singleton) |
| `fastapi_websocket_endpoint()` | Gestionnaire de route WebSocket FastAPI |
| `Route FastAPI /ws/{pipeline_id}` | Endpoint WebSocket côté client |

**Configuration minimale FastAPI** :

```python
from fastapi import FastAPI, WebSocket
from taskiq_flow.integration.websocket.fastapi_ws import fastapi_websocket_endpoint
from taskiq_flow.hooks import HookManager, setup_websocket_bridge

hook_manager = HookManager()
setup_websocket_bridge(hook_manager)
pipeline = Pipeline(broker).with_hooks(hook_manager)

app = FastAPI()

@app.websocket("/ws/{pipeline_id}")
async def ws_endpoint(websocket: WebSocket, pipeline_id: str):
    await fastapi_websocket_endpoint(websocket, pipeline_id)
```

---

*Pour détails implémentation client, voir le [Guide WebSocket]({{ '/fr/guides/websocket/' | relative_url }}). Pour stratégies filtrage, section 7 de ce guide.*
