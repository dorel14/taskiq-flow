---
permalink: /fr/api/websocket/
title: Référence API: WebSocket
nav_order: 34
---
# Référence API: WebSocket

**HookManager, système d'événements, couche transport, et serveur WebSocket**

> **Version** : 0.3.2 | **Module** : `taskiq_flow.hooks`, `taskiq_flow.transport.websocket`

---

## HookManager

Bus central d'événements qui collecte les événements d'exécution de pipeline et les dispatch aux abonnés.

```python
from taskiq_flow.hooks import HookManager

gestionnaire_hooks = HookManager()
pipeline = Pipeline(broker).with_hooks(gestionnaire_hooks)
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
gestionnaire_hooks.add_hook(hook)
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

## Transport WebSocket

### setup_websocket_bridge

Connecte `HookManager` à la couche de transport WebSocket:

```python
from taskiq_flow.hooks import HookManager, setup_websocket_bridge

gestionnaire_hooks = HookManager()
setup_websocket_bridge(gestionnaire_hooks)
# Maintenant tous les hooks sont transférés au serveur WebSocket
```

Cela installe un pont qui transfère les événements du `HookManager` aux serveurs WebSocket connectés.

### get_websocket_server

Factory pour obtenir ou créer un serveur WebSocket:

```python
from taskiq_flow.integration.websocket import get_websocket_server

serveur = get_websocket_server(
    host="0.0.0.0",
    port=8765,
    transport=None  # Utilise WebSocketTransport par défaut
)
await serveur.start_server()
```

**Paramètres**:

| Paramètre | Type | Défaut | Description |
|-----------|------|---------|-------------|
| `host` | `str` | `"0.0.0.0"` | Adresse d'écoute |
| `port` | `int` | `8765` | Port d'écoute |
| `transport` | `WebSocketTransport | None` | Auto-créé si None |

Le serveur est un singleton par configuration; appels subséquents à `get_websocket_server()` avec même host/port retournent même instance.

---

## Filtrage d'Événements

Réduire trafic en filter les événements:

```python
from taskiq_flow.hooks import EventFilter

# Seulement pipelines spécifiques
filtre = EventFilter(pipeline_ids=["pipeline_1", "pipeline_2"])

# Seulement événements d'étape
filtre = EventFilter(event_types=["StepStartEvent", "StepCompleteEvent"])

# Les deux
filtre = EventFilter(
    pipeline_ids=["*"],  # tous pipelines (ou spécifiques)
    event_types=["StepCompleteEvent", "PipelineCompleteEvent"]
)

gestionnaire_hooks.add_filter(filtre)
```

### Logique EventFilter

```
Événement → Vérifier correspondance pipeline_id? → Vérifier correspondance event_type? → Émettre?
```

Les deux filtres sont **OU** logique interne: un événement passe s'il correspond AUX DEUX filtres pipeline_ids ET event_types. Utiliser `"*"` pour tout matcher.

---

## Protocole WebSocket

### Connexion

Client se connecte via WebSocket standard:

```
ws://localhost:8765
```

Pour connexions sécurisées (WSS), terminer SSL au reverse proxy (nginx, Traefik).

### Abonnement

Après connexion, client envoie message abonnement:

```json
{
  "type": "subscribe",
  "pipeline_id": "mon_pipeline"
}
```

Abonnement wildcard (recevoir tous événements):

```json
{
  "type": "subscribe",
  "pipeline_id": "*"
}
```

Désabonnement:

```json
{
  "type": "unsubscribe",
  "pipeline_id": "mon_pipeline"
}
```

### Format Message (Serveur → Client)

Tous messages sont JSON avec champ `type`:

```json
{
  "type": "StepCompleteEvent",
  "pipeline_id": "pipeline_123",
  "step_name": "process_data",
  "duration_ms": 150.2,
  "timestamp": "2026-05-05T16:30:00Z"
}
```

Référence complète champs dans Guide WebSocket.

---

## Transport Personnalisé

Pour cas avancés, implémenter son propre transport:

```python
from taskiq_flow.transport import WebSocketTransport

class MonTransport(WebSocketTransport):
    async def broadcast(self, event: BaseEvent):
        # Logique routage personnalisée (ex: Redis Pub/Sub, Kafka)
        await self.redis.publish("pipeline_events", event.json())

transport = MonTransport()
serveur = get_websocket_server(transport=transport)
```

---

## Coordination Multi-Worker

Pour multiples processus Python partageant état événements:

```python
from taskiq_flow.transport import RedisPubSubTransport

transport = RedisPubSubTransport(client_redis)
serveur = get_websocket_server(transport=transport)
# Tous workers sur même canal Redis partagent événements
```

Tous workers souscivent au même canal Redis pub/sub; événements de n'importe quel worker sont broadcast aux clients WebSocket connectés à n'importe quel worker.

---

## Considérations Production

### Limites de Connexion

```python
import asyncio

# Limiter connexions WebSocket concurrentes
MAX_CONNECTIONS = 1000
sémaphore = asyncio.Semaphore(MAX_CONNECTIONS)

# Dans gestionnaire connexion:
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
    await server.close()  # Arrêter accepter nouvelles connexions
    await server.wait_closed()  # Attendre fermeture connexions existantes
```

### Monitoring

Exposer métriques:

```python
@app.get("/ws/metrics")
async def ws_metrics():
    return {
        "connexions": server.connection_count(),
        "messages_envoyés": server.messages_sent,
        "messages_par_seconde": server.rate()
    }
```

---

## Dépannage

| Problème | Diagnostic | Correction |
|----------|------------|------------|
| Clients ne reçoivent événements | `setup_websocket_bridge()` non appelé | Appeler avant démarrage pipeline |
| Connexion refusée | Serveur non démarré | Appeler `await server.start_server()` |
| Événements retardés | Filtre événements bloque | Vérifier configuration filtre |
| CPU élevé | Trop de connexions | Appliquer limites connexions |

---

## Résumé

| Composant | Rôle |
|-----------|------|
| `HookManager` | Collecte événements depuis pipelines |
| Classes `BaseEvent` | Données événements structurées |
| `EventFilter` | Diffusion sélective événements |
| `WebSocketTransport` | Transport bas niveau (défaut ou custom) |
| `WebSocketServer` | Gère connexions clients |
| `get_websocket_server()` | Factory/singleton accès |

**Configuration minimale**:

```python
crochets = HookManager()
setup_websocket_bridge(crochets)
pipeline = Pipeline(broker).with_hooks(crochets)
serveur = get_websocket_server()
await serveur.start_server()
```

---

*Pour détails implémentation client, voir [Guide WebSocket]({{ '/fr/guides/websocket/' | relative_url }}). Pour stratégies filtrage, section 7 de ce guide.*
