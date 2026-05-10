---
permalink: /fr/examples/websocket-demo/
title: Exemple: websocket_demo.py
nav_order: 46
color_scheme: dark
---
# Exemple: websocket_demo.py

**Streaming d'événements en temps réel via WebSockets**

> **Version** : {VERSION} | **Fichier** : `examples/websocket_demo.py`

---

## Aperçu

Cet exemple démontre comment configurer un serveur WebSocket qui diffuse en temps réel les événements d'exécution de pipeline. Il couvre:

- Créer un `HookManager` et le connecter au transport WebSocket
- Démarrer un serveur WebSocket sur un host/port spécifique
- S'abonner aux événements de pipeline depuis un client
- Observer les événements de complétion d'étape en direct

**Note** : Ceci est une démo minimale. Pour production, ajouter authentication, gestion d'erreurs, et gestion connexions robuste.

---

## Ce Que Cet Exemple Montre

- Configuration `HookManager` avec `setup_websocket_bridge()`
- Attacher hooks à un pipeline
- Démarrer le serveur WebSocket
- Comment les clients peuvent se connecter et s'abonner
- Les messages d'événements broadcastés

---

## Explication du Code

```python
import asyncio
from taskiq import InMemoryBroker
from taskiq_flow import Pipeline
from taskiq_flow.hooks import HookManager, setup_websocket_bridge
from taskiq_flow.integration.websocket import get_websocket_server
from taskiq_flow.middleware import PipelineMiddleware

# Create broker
broker = InMemoryBroker(await_inplace=True).with_middlewares(PipelineMiddleware())

# Define simple tasks
@broker.task
def add_one(x: int) -> int:
    return x + 1

@broker.task
def multiply_by_two(x: int) -> int:
    return x * 2

async def main():
    # 1. Configure hook manager and WebSocket bridge
    hook_manager = HookManager()
    setup_websocket_bridge(hook_manager)

    # 2. Create pipeline and attach hooks
    pipeline = Pipeline(broker)
    pipeline.pipeline_id = "websocket_demo"
    pipeline.call_next(add_one, param_name="x")
    pipeline.call_next(multiply_by_two, param_name="x")
    pipeline.with_hooks(hook_manager)

    # 3. Start WebSocket server in background
    websocket_server = get_websocket_server()
    _ = asyncio.create_task(
        websocket_server.start_server("127.0.0.1", 8765),
    )

    print("WebSocket server started on ws://127.0.0.1:8765")
    msg = '{"pipeline_id": "websocket_demo"}'
    print(f"Connect a WebSocket client and subscribe with: {msg}")
    print("Then run the pipeline to see real-time events...")

    # Wait for server to start
    await asyncio.sleep(1)

    # 4. Execute the pipeline
    result = await pipeline.kiq(5)  # Start with 5 → 6 → 12
    print(f"Pipeline result: {result}")

    # Keep server alive briefly
    await asyncio.sleep(5)
    print("Demo complete. Server will shut down.")

asyncio.run(main())
```

---

## Séquence d'Événements

Quand le pipeline s'exécute, événements suivants sont broadcastés:

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

## Implémentation Client (JavaScript)

Ouvrir console navigateur ou script Node.js:

```javascript
// Se connecter au serveur WebSocket
const ws = new WebSocket('ws://127.0.0.1:8765');

// S'abonner au pipeline de démo
ws.onopen = () => {
    console.log('Connecté au serveur WebSocket');
    ws.send(JSON.stringify({
        type: 'subscribe',
        pipeline_id: 'websocket_demo'
    }));
};

// Gérer événements entrants
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

## Points Clés Configuration

### 1. Créer HookManager
```python
hook_manager = HookManager()
```

### 2. Installer Pont WebSocket
```python
setup_websocket_bridge(hook_manager)
```
Cela connecte système événements HookManager au transport WebSocket.

### 3. Attacher Hooks au Pipeline
```python
pipeline = Pipeline(broker).with_hooks(hook_manager)
```
Sans ceci, le pipeline n'émettra pas événements vers WebSocket.

### 4. Define pipeline_id
```python
pipeline.pipeline_id = "my_pipeline"
```
Necessary for clients to subscribe to specific pipelines.

### 5. Start Server
```python
server = get_websocket_server(host="127.0.0.1", port=8765)
await server.start_server()
```

---

## Personnalisation

### Change Port
```python
server = get_websocket_server(port=9000)
```

### Multiple Pipelines
```python
pipeline1 = Pipeline(broker).with_hooks(hook_manager)
pipeline1.pipeline_id = "pipeline_1"

pipeline2 = Pipeline(broker).with_hooks(hook_manager)
pipeline2.pipeline_id = "pipeline_2"
```

Clients can subscribe to specific pipeline IDs.

### Event Filtering
```python
from taskiq_flow.hooks import EventFilter

# Only send step completion events
filter = EventFilter(
    pipeline_ids=["*"],
    event_types=["StepCompleteEvent", "PipelineCompleteEvent"]
)
hook_manager.add_filter(filter)
```

### Multiple Pipelines
```python
pipeline1 = Pipeline(broker).with_hooks(hook_manager)
pipeline1.pipeline_id = "pipeline_1"

pipeline2 = Pipeline(broker).with_hooks(hook_manager)
pipeline2.pipeline_id = "pipeline_2"
```

Clients can subscribe to specific pipeline IDs.

### Event Filtering

### Multiples Pipelines
```python
pipeline1 = Pipeline(broker).with_hooks(hook_manager)
pipeline1.pipeline_id = "pipeline_1"

pipeline2 = Pipeline(broker).with_hooks(hook_manager)
pipeline2.pipeline_id = "pipeline_2"
```

Les clients peuvent s'abonner à IDs pipeline spécifiques.

### Filtrage Événements
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

### Connexion Refusée
- Vérifier `await server.start_server()` appelé avant connexion
- Vérifier host/port correspondent client connection string

### Événements Dans Désordre
WebSocket livre messages en ordre; si désordre, vérifier problèmes réseau ou middleware custom émettant événements incorrectement.

---

## Chemin d'Apprentissage

Après cet exemple:

1. **[Guide WebSocket]({{ '/fr/guides/websocket/' | relative_url }})** — Configuration WebSocket complète, filtrage, déploiement production
2. **[Guide de Suivi]({{ '/fr/guides/tracking/' | relative_url }})** — Stockage historique données alongside événements temps réel
3. **[Guide API]({{ '/fr/guides/api/' | relative_url }})** — Exposer via REST pour clients non-WebSocket

---

*Cet exemple montre bases streaming temps réel. Pour production, ajouter authentication, pooling connexions, et scaling horizontal avec transport Redis Pub/Sub.*
