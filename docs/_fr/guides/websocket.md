---
title: Guide WebSocket
nav_order: 24
---
# Guide WebSocket

**Streaming d'événements en temps réel pour tableaux de bord et monitoring**

> **Version**: 1.0.0 | **Lié**: [Guide de Suivi]({{ '/fr/guides/tracking/' | relative_url }}), [Guide API]({{ '/fr/guides/api/' | relative_url }})

---

## Aperçu

L'intégration WebSocket de Taskiq-Flow fournit un streaming en direct des événements d'exécution de pipeline — idéal pour construire des tableaux de bord en temps réel, affichages de progression et outils de monitoring.

Ce guide couvre :

- Configuration d'un serveur WebSocket
- Abonnement des clients aux événements de pipeline
- Types d'événements et charges utiles
- Configuration de la couche de transport
- Considérations de déploiement en production

---

## 1. Architecture

```
[Pipeline] → [HookManager] → [WebSocketBridge] → [Serveur WebSocket] → [Clients]
```

**Composants**:

1. **Pipeline** — Émet des événements via des hooks à chaque étape du cycle de vie
2. **HookManager** — Collecte les événements des pipelines
3. **WebSocketBridge** — Connecte HookManager au transport WebSocket
4. **Serveur WebSocket** — Gère les connexions clients et diffuse
5. **Client** — Navigateur web, app de monitoring, tableau de bord

---

## 2. Démarrage Rapide

### 2.1. Configuration Côté Serveur

```python
import asyncio
from taskiq import InMemoryBroker
from taskiq_flow import Pipeline
from taskiq_flow.hooks import HookManager, setup_websocket_bridge
from taskiq_flow.integration.websocket import get_websocket_server

# 1. Créer le broker et le gestionnaire de hooks
broker = InMemoryBroker()
gestionnaire_hooks = HookManager()

# 2. Configurer le pont WebSocket
setup_websocket_bridge(gestionnaire_hooks)  # connecte HookManager → transport WebSocket

# 3. Créer un pipeline avec hooks attachés
pipeline = Pipeline(broker)
pipeline.pipeline_id = "workflow_demo"
pipeline.with_hooks(gestionnaire_hooks)

# Ajouter des tâches au pipeline...

# 4. Démarrer le serveur WebSocket
async def main():
    serveur = get_websocket_server(host="0.0.0.0", port=8765)
    await serveur.start_server()

    # 5. Exécuter le pipeline
    résultat = await pipeline.kiq(données)

    # 6. Garder le serveur actif (ou intégrer à la boucle d'événements de votre app)
    await asyncio.Event().wait()

asyncio.run(main())
```

### 2.2. Connexion Client (JavaScript)

```javascript
// Se connecter au serveur WebSocket
const ws = new WebSocket('ws://localhost:8765');

// S'abonner à un pipeline spécifique
ws.onopen = () => {
    ws.send(JSON.stringify({
        type: 'subscribe',
        pipeline_id: 'workflow_demo'
    }));
};

// Recevoir les événements
ws.onmessage = (event) => {
    const eventData = JSON.parse(event.data);
    console.log('Événement pipeline:', eventData);

    switch (eventData.type) {
        case 'PipelineStartEvent':
            showPipelineStarted();
            break;
        case 'StepStartEvent':
            showStepProgress(eventData.step_name);
            break;
        case 'StepCompleteEvent':
            updateProgress(eventData.step_name, eventData.duration_ms);
            break;
        case 'PipelineCompleteEvent':
            showResults(eventData.result);
            break;
        case 'PipelineErrorEvent':
            showError(eventData.error);
            break;
    }
};
```

---

## 3. Types d'Événements

Tous les événements sont serialisables JSON avec un champ `type` indiquant le genre d'événement.

### 3.1. PipelineStartEvent

```json
{
  "type": "PipelineStartEvent",
  "pipeline_id": "workflow_demo",
  "pipeline_type": "sequential",
  "timestamp": "2026-04-29T18:50:19+02:00",
  "input": {...}
}
```

Émis quand un pipeline commence son exécution.

### 3.2. StepStartEvent

```json
{
  "type": "StepStartEvent",
  "pipeline_id": "workflow_demo",
  "step_name": "process_data",
  "step_index": 2,
  "task_id": "abc123",
  "timestamp": "2026-04-29T18:50:19.5+02:00"
}
```

Émis avant que chaque étape ne démarre.

### 3.3. StepCompleteEvent

```json
{
  "type": "StepCompleteEvent",
  "pipeline_id": "workflow_demo",
  "step_name": "process_data",
  "step_index": 2,
  "result": {"processed": 42},
  "duration_ms": 150.5,
  "timestamp": "2026-04-29T18:50:19.7+02:00"
}
```

Émis après qu'une étape se termine avec succès.

### 3.4. PipelineCompleteEvent

```json
{
  "type": "PipelineCompleteEvent",
  "pipeline_id": "workflow_demo",
  "pipeline_type": "sequential",
  "status": "COMPLETED",
  "duration_ms": 1250.3,
  "result": {"final": "output"},
  "timestamp": "2026-04-29T18:50:20.5+02:00"
}
```

Émis quand le pipeline entier se termine avec succès.

### 3.5. StepErrorEvent

```json
{
  "type": "StepErrorEvent",
  "pipeline_id": "workflow_demo",
  "step_name": "failing_task",
  "error": "ValueError: invalid input",
  "timestamp": "2026-04-29T18:50:19.9+02:00"
}
```

Émis quand une étape échoue.

### 3.6. PipelineErrorEvent

```json
{
  "type": "PipelineErrorEvent",
  "pipeline_id": "workflow_demo",
  "error": "Pipeline échoué à l'étape 'validate'",
  "timestamp": "2026-04-29T18:50:20.2+02:00"
}
```

Émis quand le pipeline abandonne suite à une erreur irrécupérable.

---

## 4. Implémentation Côté Client

### 4.1. Client JavaScript Basique

```javascript
class MoniteurPipeline {
    constructor(url, pipelineId) {
        this.url = url;
        this.pipelineId = pipelineId;
        this.ws = null;
        this.events = [];
        this.callbacks = {};
    }

    connect() {
        this.ws = new WebSocket(this.url);

        this.ws.onopen = () => {
            console.log('Connecté au serveur WebSocket');
            this.subscribe(this.pipelineId);
        };

        this.ws.onmessage = (event) => {
            const data = JSON.parse(event.data);
            this.handleEvent(data);
        };

        this.ws.onerror = (err) => {
            console.error('Erreur WebSocket:', err);
        };

        this.ws.onclose = () => {
            console.log('Connexion WebSocket fermée');
            this.reconnect();
        };
    }

    subscribe(pipelineId) {
        this.ws.send(JSON.stringify({
            type: 'subscribe',
            pipeline_id: pipelineId
        }));
    }

    handleEvent(event) {
        this.events.push(event);
        const eventType = event.type;

        if (this.callbacks[eventType]) {
            this.callbacks[eventType](event);
        }

        // Gestionnaire d'événement générique
        if (this.callbacks['*']) {
            this.callbacks['*'](event);
        }
    }

    on(eventType, callback) {
        this.callbacks[eventType] = callback;
    }

    reconnect() {
        setTimeout(() => this.connect(), 3000);
    }
}

// Utilisation
monitor = new MoniteurPipeline('ws://localhost:8765', 'pipeline_123');
monitor.on('StepCompleteEvent', (event) => {
    console.log(`Étape ${event.step_name} complétée en ${event.duration_ms}ms`);
});
monitor.on('PipelineCompleteEvent', (event) => {
    console.log('Pipeline terminé avec statut:', event.status);
});
monitor.connect();
```

### 4.2. Client Python (pour scripts)

```python
import asyncio
import websockets
import json

async def monitor_pipeline(uri, pipeline_id):
    async with websockets.connect(uri) as websocket:
        # S'abonner
        await websocket.send(json.dumps({
            "type": "subscribe",
            "pipeline_id": pipeline_id
        }))

        # Recevoir les événements
        async for message in websocket:
            event = json.loads(message)
            print(f"[{event['type']}] {event}")

            if event['type'] == 'PipelineCompleteEvent':
                print(f"Pipeline terminé: {event['status']}")

asyncio.run(monitor_pipeline('ws://localhost:8765', 'pipeline_123'))
```

---

## 5. Gestion des Abonnements

### 5.1. S'abonner à un Pipeline

Les clients envoient un message d'abonnement:

```json
{
  "type": "subscribe",
  "pipeline_id": "mon_pipeline_001"
}
```

Après abonnement, tous les événements pour ce pipeline sont relayés.

### 5.2. Se Désabonner

```json
{
  "type": "unsubscribe",
  "pipeline_id": "mon_pipeline_001"
}
```

### 5.3. S'abonner à Tous les Pipelines (Caractère générique)

```json
{
  "type": "subscribe",
  "pipeline_id": "*"
}
```

**Attention** : Diffuser tous les événements peut générer un trafic important dans les systèmes à haut débit.

### 5.4. Multiples Abonnements

Un client peut s'abonner à plusieurs pipelines:

```javascript
monitor.subscribe('pipeline_1');
monitor.subscribe('pipeline_2');
// Reçoit les événements des deux, distingués par le champ pipeline_id
```

---

## 6. Configuration du Serveur

### 6.1. Hôte et Port Personnalisés

```python
# Utiliser une interface et port spécifiques
serveur = get_websocket_server(host='127.0.0.1', port=8765)
await serveur.start_server()

# Ou lier à toutes les interfaces (exposer au réseau)
serveur = get_websocket_server(host='0.0.0.0', port=8765)
```

### 6.2. CORS et En-têtes de Sécurité

Si derrière un reverse proxy (nginx, Traefik), configurer les en-têtes CORS:

```nginx
# nginx.conf
location /ws {
    proxy_pass http://localhost:8765;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "upgrade";
    add_header Access-Control-Allow-Origin "*";
    add_header Access-Control-Allow-Credentials true;
}
```

### 6.3. Terminaison SSL/TLS

Terminer SSL au reverse proxy:

```nginx
# HTTPS → WSS forwarding
location /ws {
    proxy_pass http://localhost:8765;
    # WSS (WebSocket sécurisé) géré par la config SSL nginx
}
```

Le client se connecte avec:

```javascript
const ws = new WebSocket('wss://votredomaine.com/ws');
```

### 6.4. Multiples Workers

Pour multiples processus Python, chacun a besoin de son propre serveur WebSocket sur un port différent (ou utiliser un broker de messages comme Redis Pub/Sub pour coordonner):

```python
# Worker 1
serveur1 = get_websocket_server(port=8765)

# Worker 2
serveur2 = get_websocket_server(port=8766)

# Load balancer distribue les connexions WebSocket
```

Pour un véritable partage d'événements multi-worker, utiliser le transport Redis:

```python
from taskiq_flow.transport import RedisPubSubTransport

transport = RedisPubSubTransport(client_redis)
serveur = get_websocket_server(transport=transport)
# Maintenant tous les workers partagent l'état des événements via Redis
```

---

## 7. Filtrage des Événements

Réduire la bande passante en filtrant côté serveur:

```python
from taskiq_flow.hooks import EventFilter

# Envoyer seulement les événements pour pipelines spécifiques
filtre = EventFilter(pipeline_ids=['pipeline_1', 'pipeline_2'])
gestionnaire_hooks.add_filter(filtre)

# Seulement les événements d'étape (pas niveau pipeline)
filtre = EventFilter(event_types=['StepStartEvent', 'StepCompleteEvent'])
gestionnaire_hooks.add_filter(filtre)
```

Filtrage côté client également possible:

```javascript
monitor.on('StepCompleteEvent', (event) => {
    if (event.step_name === 'étape_importante') {
        highlightStep(event.step_name);
    }
});
```

---

## 8. Référence des Messages

### Requête d'Abonnement

| Champ | Type | Description |
|-------|------|-------------|
| `type` | `"subscribe"` | Type de message|
| `pipeline_id` | `str` ou `"*"` | Pipeline auquel s'abonner |

### Requête de Désabonnement

| Champ | Type | Description |
|-------|------|-------------|
| `type` | `"unsubscribe"` | Type de message |
| `pipeline_id` | `str` | Pipeline dont se désabonner |

### Message d'Événement (serveur → client)

| Champ | Type | Description |
|-------|------|-------------|
| `type` | `str` | Type d'événement (voir Section 3) |
| `pipeline_id` | `str` | ID du pipeline d'origine |
| `timestamp` | `ISO 8601 str` | Horodatage de l'événement |

Champs additionnels selon le type d'événement (voir ci-dessus).

---

## 9. Déploiement en Production

### 9.1. Déploiement Docker

```dockerfile
# Dockerfile
FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

CMD ["python", "-m", "mon_app_websocket"]
```

```yaml
# docker-compose.yml
version: '3.8'
services:
  redis:
    image: redis:7-alpine
  app:
    build: .
    ports:
      - "8765:8765"
    environment:
      - REDIS_URL=redis://redis:6379
    depends_on:
      - redis
```

### 9.2. Service Systemd

```ini
# /etc/systemd/system/taskiq-flow-ws.service
[Unit]
Description=Serveur WebSocket Taskiq-Flow
After=network.target

[Service]
Type=simple
User=appuser
WorkingDirectory=/opt/taskiq-flow
ExecStart=/usr/bin/python3 -m mon_app_websocket
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

### 9.3. Monitoring

Health check endpoint:

```python
from aiohttp import web

async def health(request):
    return web.json_response({"status": "healthy"})

app = web.Application()
app.router.add_get('/health', health)
```

Ou utiliser l'endpoint health intégré (/health) depuis le [Guide API]({{ '/fr/guides/api/' | relative_url }}).

### 9.4. Scalabilité

Pour déploiements haut débit:

- **Scaling horizontal** : Déployer multiples instances de serveur WebSocket avec sessions sticky ou transport Redis Pub/Sub
- **Load balancing** : Utiliser nginx ou HAProxy avec support WebSocket
- **Limites de connexion** : Configurer max connexions par worker (limites OS)
- **Compression de messages** : Activer permessage-deflate pour payloads larges

---

## 10. Considérations de Sécurité

### 10.1. Authentification

Exiger des tokens d'authentification à la connexion:

```python
# Validation côté serveur
async def authenticate(websocket, token):
    if not validate_token(token):
        await websocket.close(code=4001, reason="Unauthorized")
        return False
    return True

# Le client envoie le token à la connexion
ws = new WebSocket(`ws://localhost:8765?token=${authToken}`);
```

### 10.2. Autorisation

Filtrer les événements par permissions utilisateur:

```python
class FiltreAuth(EventFilter):
    def __init__(self, user_id, pipelines_autorises):
        self.user_id = user_id
        self.allowed = pipelines_autorises

    def should_emit(self, event):
        return event.pipeline_id in self.allowed
```

### 10.3. Limitation de Débit (Rate Limiting)

Prévenir les abus:

```python
from collections import defaultdict
import time

class LimiteurDebit:
    def __init__(self, max_events_per_second=100):
        self.limits = defaultdict(list)

    def allow(self, client_id):
        now = time.time()
        self.limits[client_id] = [
            t for t in self.limits[client_id] if now - t < 1
        ]
        if len(self.limits[client_id]) < 100:
            self.limits[client_id].append(now)
            return True
        return False
```

---

## 11. Dépannage

### Connexion Refusée

**Symptôme** : Le client ne peut pas se connecter, erreur "Connection refused".

**Corrections**：
- Vérifier que le serveur tourne: `netstat -lnp | grep 8765`
- Vérifier les règles firewall permettent le port 8765
- S'assurer que le host binding correspond (0.0.0.0 pour accès externe)

### Aucun Événement Reçu Après Connexion

**Symptôme** : Connexion réussie, mais aucun événement n'arrive.

**Corrections**：
- S'assurer que le pipeline a `pipeline_id` défini
- Confirmer que `pipeline.with_hooks(gestionnaire_hooks)` est appelé
- Vérifier que `setup_websocket_bridge(gestionnaire_hooks)` est appelé avant que le pipeline ne démarre
- Vérifier le format du message d'abonnement (voir Section 5)

### Utilisation Mémoire Élevée

**Symptôme** : Mémoire serveur augmente avec le temps.

**Corrections**：
- Limiter le nombre de pipelines suivis
- Implémenter nettoyage automatique des clients déconnectés
- Utiliser transport Redis pour externaliser l'état du processus
- Définir limite max de connexions

### Événements Dans le Désordre

**Symptôme** : Le client reçoit StepComplete avant StepStart.

**Corrections**：
- Utiliser garanties de livraison séquentielle (par défaut pour WebSocket)
- S'assurer que tous les hooks sont correctement attachés
- Vérifier les middleware personnalisés qui pourraient émettre des événements asynchrone

---

## 12. Résumé

| Composant | Responsabilité |
|-----------|----------------|
| `Pipeline` | Génère événements d'exécution |
| `HookManager` | Collecte événements des pipelines |
| `WebSocketBridge` | Achemine événements vers transport WebSocket |
| `Serveur WebSocket` | Gère connexions clients, diffuse |
| `Client` | S'abonne, reçoit, affiche événements |

**Configuration basique (5 lignes)**:

```python
crochets = HookManager()
setup_websocket_bridge(crochets)
pipeline = Pipeline(broker).with_hooks(crochets)
serveur = get_websocket_server()
await serveur.start_server()
```

---

## Prochaines Étapes

- **[Guide de Suivi]({{ '/fr/guides/tracking/' | relative_url }})** — Backend de stockage et requêtes historiques
- **[Guide API]({{ '/fr/guides/api/' | relative_url }})** — Endpoints REST pour backends de tableau de bord
- **[Exemples: Démo WebSocket]({{ '/fr/examples/websocket-demo/' | relative_url }})** — Code complet fonctionnel

---

*Streamer les événements de pipeline en direct. Combiner avec [Stockage de Suivi]({{ '/fr/guides/tracking/' | relative_url }}) pour historique persistant.*
