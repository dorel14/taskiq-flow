---
permalink: /fr/examples/websocket-demo/
title: 'Exemple: websocket_demo.py'
nav_order: 46
color_scheme: dark
---
# Exemple: websocket_demo.py

**Streaming d'événements en temps réel via WebSockets**

> **Version** : {VERSION} | **Fichier** : `examples/websocket_demo.py`

---

## Aperçu

Cet exemple démontre comment mettre en place le streaming d'événements de pipeline en temps réel via WebSocket en utilisant l'intégration **FastAPI uniquement de Taskiq-Flow**.

Ce guide couvre :

- Créer un `HookManager` et le connecter au transport WebSocket via `setup_websocket_bridge()`
- Exécuter un pipeline avec événements WebSocket actifs
- Connecter un client WebSocket à la route FastAPI
- Observer les événements d'étape terminée en direct

**Note** : Les événements WebSocket sont servis par votre application FastAPI à `/ws/{pipeline_id}`. Le serveur autonome historique (`get_websocket_server`, `PipelineWebSocketServer`) a été supprimé en v1.1. Utilisez la route WebSocket FastAPI à la place.

---

## Prérequis

```bash
pip install "taskiq-flow[brokers]" "fastapi[standard]" uvicorn
```

---

## Ce Que Cet Exemple Montre

- Configuration de `HookManager` avec `setup_websocket_bridge()`
- Attacher des hooks à un pipeline
- Exécuter le pipeline et diffuser des événements vers les clients WebSocket connectés
- Connecter un client WebSocket via FastAPI à `/ws/{pipeline_id}`

---

## Partie 1 : Script Pipeline (Python)

Ce script configure le pipeline, les hooks, et l'application FastAPI. Démarrez-le en premier.

```python
import asyncio
from taskiq import InMemoryBroker
from taskiq_flow import Pipeline
from taskiq_flow.hooks import HookManager, setup_websocket_bridge
from taskiq_flow.middleware import PipelineMiddleware
from fastapi import FastAPI, WebSocket
from taskiq_flow.integration.websocket.fastapi_ws import (
    fastapi_websocket_endpoint,
    get_fastapi_ws_manager,
)

# Create broker
broker = InMemoryBroker(await_inplace=True).with_middlewares(PipelineMiddleware())

# Define simple tasks
@broker.task
def add_one(x: int) -> int:
    return x + 1

@broker.task
def multiply_by_two(x: int) -> int:
    return x * 2

# -- WebSocket setup --------------------------------------------------------

# 1. Create hook manager
hook_manager = HookManager()

# 2. Set up the WebSocket bridge (connects HookManager → FastAPI WebSocket transport)
setup_websocket_bridge(hook_manager)

# 3. Mount the FastAPI WebSocket route
app = FastAPI()

@app.websocket("/ws/{pipeline_id}")
async def ws_endpoint(websocket: WebSocket, pipeline_id: str):
    """
    WebSocket endpoint for real-time pipeline events.
    Clients connect to: ws://localhost:8000/ws/{pipeline_id}
    """
    await fastapi_websocket_endpoint(websocket, pipeline_id)

# -- Pipeline ---------------------------------------------------------------

async def main():
    # 4. Create pipeline and attach hooks
    pipeline = Pipeline(broker)
    pipeline.pipeline_id = "websocket_demo"
    pipeline.call_next(add_one, param_name="x")
    pipeline.call_next(multiply_by_two, param_name="x")
    pipeline.with_hooks(hook_manager)

    # 5. Execute the pipeline (events are broadcast to connected WS clients)
    result = await pipeline.kiq(5)  # Start with 5 → 6 → 12
    print(f"Pipeline result: {result}")

    # Keep FastAPI app running briefly (in production, run uvicorn separately)
    await asyncio.sleep(5)
    print("Demo complete. Shutting down.")

asyncio.run(main())
```

Démarrez le script avec :

```bash
uvicorn examples.websocket_demo:app --host 0.0.0.0 --port 8000
```

Ou en mode développement :

```bash
uvicorn examples.websocket_demo:app --reload
```

---

## Partie 2 : Client WebSocket (JavaScript)

Ouvrez une console navigateur ou un script Node.js :

```javascript
// Se connecter à la route WebSocket FastAPI
const ws = new WebSocket('ws://localhost:8000/ws/websocket_demo');

// S'abonner au pipeline de démo (déjà fait en se connectant à /ws/websocket_demo)
ws.onopen = () => {
    console.log('Connecté au serveur WebSocket');
    ws.send(JSON.stringify({
        type: 'subscribe',
        pipeline_id: 'websocket_demo'
    }));
};

// Gérer les événements entrants
ws.onmessage = (event) => {
    const data = JSON.parse(event.data);
    console.log('Événement:', data.type, data);

    switch (data.type) {
        case 'StepCompleteEvent':
            console.log(`Étape ${data.step_name} finie:`, data.result);
            break;
        case 'PipelineCompleteEvent':
            console.log('Pipeline terminé avec statut:', data.status);
            console.log('Résultat final:', data.result);
            break;
    }
};
```

---

## Séquence d'Événements

Lorsque le pipeline s'exécute, les événements suivants sont broadcastés :

1. **PipelineStartEvent**
   ```json
   {"type": "PipelineStartEvent", "pipeline_id": "websocket_demo", "timestamp": "..."}
   ```

2. **StepStartEvent** (pour add_one)
   ```json
   {"type": "StepStartEvent", "pipeline_id": "websocket_demo", "step_name": "add_one", ...}
   ```

3. **StepCompleteEvent** (pour add_one)
   ```json
   {"type": "StepCompleteEvent", "pipeline_id": "websocket_demo", "step_name": "add_one", "result": 6, "duration_ms": 1.2, ...}
   ```

4. **StepStartEvent** (pour multiply_by_two)

5. **StepCompleteEvent** (pour multiply_by_two)

6. **PipelineCompleteEvent**
   ```json
   {"type": "PipelineCompleteEvent", "pipeline_id": "websocket_demo", "status": "COMPLETED", "result": 12, ...}
   ```

---

## Points Clés Configuration

### 1. Créer HookManager
```python
hook_manager = HookManager()
```

### 2. Installer Pont WebSocket
```python
setup_websocket_bridge(hook_manager)
```
Cela connecte le système d'événements HookManager au transport WebSocket.

### 3. Monter Route FastAPI WebSocket
```python
@app.websocket("/ws/{pipeline_id}")
async def ws_endpoint(websocket: WebSocket, pipeline_id: str):
    await fastapi_websocket_endpoint(websocket, pipeline_id)
```
La route WebSocket est servie par votre application FastAPI.

### 4. Attacher Hooks au Pipeline
```python
pipeline = Pipeline(broker).with_hooks(hook_manager)
```
Sans ceci, le pipeline n'émettra pas d'événements WebSocket.

### 5. Définir pipeline_id
```python
pipeline.pipeline_id = "websocket_demo"
```
Nécessaire pour que les clients s'abonnent à des pipelines spécifiques.

---

## Personnalisation

### Multiples Pipelines
```python
pipeline1 = Pipeline(broker).with_hooks(hook_manager)
pipeline1.pipeline_id = "pipeline_1"

pipeline2 = Pipeline(broker).with_hooks(hook_manager)
pipeline2.pipeline_id = "pipeline_2"
```

Les clients peuvent s'abonner aux IDs de pipeline spécifiques via `/ws/{pipeline_id}`.

### Filtrage d'Événements
```python
from taskiq_flow.hooks import EventFilter

# Only send step completion events
filter = EventFilter(
    pipeline_ids=["*"],
    event_types=["StepCompleteEvent", "PipelineCompleteEvent"]
)
hook_manager.add_filter(filter)
```

---

## Dépannage

### Aucun Événement Reçu
- Vérifier `setup_websocket_bridge(hook_manager)` appelé **avant** `pipeline.kiq()`
- Vérifier `pipeline.with_hooks(hook_manager)` appelé
- Vérifier `pipeline.pipeline_id` défini
- Vérifier que la route WebSocket FastAPI est montée à `/ws/{pipeline_id}`

### Connexion Refusée
- Vérifier que l'application FastAPI est lancée : `uvicorn examples.websocket_demo:app`
- Vérifier que le port correspond à la chaîne de connexion client

### Événements Dans le Désordre
WebSocket livre les messages en ordre ; si désordre, vérifier problèmes réseau ou middleware personnalisé émettant des événements incorrectement.

---

## Chemin d'Apprentissage

Après cet exemple :

1. **[Guide WebSocket]({{ '/fr/guides/websocket/' | relative_url }})** — Configuration WebSocket complète, filtrage, déploiement production
2. **[Guide de Suivi]({{ '/fr/guides/tracking/' | relative_url }})** — Stockage historique données alongside événements temps réel
3. **[Guide API]({{ '/fr/guides/api/' | relative_url }})** — Exposer via REST pour clients non-WebSocket

---

*Cet exemple montre les bases du streaming temps réel avec l'intégration WebSocket FastAPI uniquement de Taskiq-Flow. Pour production, ajouter authentication, pooling connexions, et scaling horizontal avec transport Redis Pub/Sub.*
