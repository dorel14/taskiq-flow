---
title: Guide API REST
nav_order: 28
---
# Guide API REST

**Gestion de pipelines via FastAPI : exécution distante, visualisation, tableaux de bord**

> **Version** : 0.3.2 | **Lié** : [Guide de Suivi]({{ '/fr/guides/tracking.md' | relative_url }}), [Guide WebSocket]({{ '/fr/guides/websocket.md' | relative_url }})

---

## Aperçu

Taskiq-Flow inclut une API REST basée sur FastAPI pour gérer les pipelines à distance. Construisez des tableaux de bord, intégrations CI/CD, ou tout système interagissant avec les pipelines via HTTP.

Ce guide couvre :

- Configuration du serveur API
- Endpoints disponibles
- Visualisation des DAG
- Exécution distante de pipelines
- Extensions d'endpoints personnalisés
- Authentification et sécurité
- Déploiement en production

---

## 1. Configuration Rapide

```python
from fastapi import FastAPI
from taskiq import InMemoryBroker
from taskiq_flow import DataflowPipeline, pipeline_task, create_visualization_api

# 1. Créer broker et tâches
broker = InMemoryBroker(await_inplace=True)

@broker.task
@pipeline_task(output="resultat")
async def process(données: str) -> dict:
    return {"traité": données.upper()}

# 2. Construire pipeline
pipeline = DataflowPipeline.from_tasks(broker, [process])
pipeline.pipeline_id = "mon_pipeline"

# 3. Créer app FastAPI avec API de visualisation
app = FastAPI(title="API Taskiq-Flow", version="0.3.0")
viz_api = create_visualization_api(broker, app)
viz_api.add_pipeline("mon_pipeline", pipeline)

# 4. Lancer avec uvicorn
# uvicorn main:app --reload --port 8000
```

Tous les endpoints sont automatiquement montés sous `/pipelines`.

---

## 2. Endpoints Disponibles

L'API de visualisation fournit ces routes :

### 2.1. Health Check

```
GET /health
```

Retourne statut simple:

```json
{
  "statut": "healthy",
  "timestamp": "2026-05-05T12:00:00Z"
}
```

### 2.2. Lister Tous les Pipelines

```
GET /pipelines
```

Liste tous les pipelines enregistrés avec métadonnées:

```json
[
  {
    "pipeline_id": "analyse_audio_v1",
    "pipeline_type": "dataflow",
    "tasks": ["extract", "tag", "embed"],
    "created_at": "2026-05-05T10:00:00Z"
  }
]
```

### 2.3. Enregistrer un Nouveau Pipeline

```
POST /pipelines/{pipeline_id}
```

Corps de requête:

```json
{
  "pipeline_type": "dataflow",
  "tasks": ["task1", "task2"]
}
```

Ou utiliser l'API Python directement (recommandé):

```python
viz_api.add_pipeline("nouveau_pipeline", objet_pipeline)
```

### 2.4. Obtenir le Statut d'un Pipeline

```
GET /pipelines/{pipeline_id}/status
```

Retourne statut d'exécution courant si un run est actif:

```json
{
  "pipeline_id": "mon_pipeline_123",
  "statut": "RUNNING",
  "étapes_complétées": 3,
  "total_étapes": 5,
  "démarré_à": "2026-05-05T12:00:00Z"
}
```

### 2.5. Obtenir le DAG en JSON

```
GET /pipelines/{pipeline_id}/dag
```

Retourne la structure de graphe orienté acyclique:

```json
{
  "nodes": [
    {"id": "extract", "outputs": ["features"]},
    {"id": "tag", "inputs": ["features"], "outputs": ["tags"]},
    {"id": "embed", "inputs": ["features"], "outputs": ["embedding"]}
  ],
  "edges": [
    {"from": "extract", "to": "tag"},
    {"from": "extract", "to": "embed"}
  ]
}
```

### 2.6. Obtenir le DAG au Format DOT

```
GET /pipelines/{pipeline_id}/dag/dot
```

Retourne chaîne DOT compatible Graphviz:

```
digraph "mon_pipeline" {
  node [shape=box];
  extract -> tag;
  extract -> embed;
}
```

### 2.7. Visualisation Complète de Pipeline

```
GET /pipelines/{pipeline_id}/visualize
```

Retourne métadonnées complètes du pipeline:

```json
{
  "pipeline_id": "mon_pipeline",
  "type": "dataflow",
  "tasks": [
    {
      "name": "extract",
      "outputs": ["features"],
      "inputs": [],
      "description": "Extraire caractéristiques audio"
    },
    {
      "name": "tag",
      "inputs": ["features"],
      "outputs": ["tags"],
      "description": "Générer tags"
    }
  ],
  "execution_levels": [
    ["extract"],
    ["tag", "embed"]
  ]
}
```

---

## 3. Exécution de Pipelines via API

L'API de base se concentre sur gestion et visualisation. Pour exécuter des pipelines à distance, ajouter un endpoint personnalisé:

```python
from fastapi import FastAPI, HTTPException
from taskiq_flow.api import PipelineVisualizationAPI

app = FastAPI()
viz_api = PipelineVisualizationAPI(broker, app)

@app.post("/pipelines/{pipeline_id}/execute")
async def execute_pipeline(
    pipeline_id: str,
    parameters: dict,
    wait: bool = False,
    timeout: int = 30
):
    """
    Exécute un pipeline avec paramètres donnés.

    - **pipeline_id**: ID pipeline enregistré
    - **parameters**: Dict paramètres d'entrée
    - **wait**: Si True, bloque jusqu'à complétion et retourne résultat
    - **timeout**: Secondes avant timeout
    """
    if pipeline_id not in viz_api.pipelines:
        raise HTTPException(status_code=404, detail="Pipeline non trouvé")

    pipeline = viz_api.pipelines[pipeline_id]

    try:
        task = await pipeline.kiq_dataflow(**parameters)

        if wait:
            result = await task.wait_result(timeout=timeout)
            return {
                "task_id": task.task_id,
                "statut": "COMPLETED",
                "resultat": result.return_value
            }
        else:
            return {
                "task_id": task.task_id,
                "statut": "STARTED"
            }
    except asyncio.TimeoutError:
        raise HTTPException(status_code=504, detail="Exécution pipeline timed out")
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))

@app.get("/pipelines/result/{task_id}")
async def get_result(task_id: str):
    """Récupère le résultat d'une exécution de pipeline."""
    result = await broker.get_result(task_id)
    if result is None:
        raise HTTPException(status_code=404, detail="Résultat non trouvé ou expiré")
    return {"task_id": task_id, "resultat": result.return_value}
```

### 3.1. Exécution Async (Fire-and-Forget)

```bash
curl -X POST "http://localhost:8000/pipelines/mon_pipeline/execute" \
  -H "Content-Type: application/json" \
  -d '{"parameters": {"données": "valeur"}, "wait": false}'

# Réponse:
{
  "task_id": "abc123def456",
  "statut": "STARTED"
}
```

### 3.2. Exécution Synchrone (Attendre Résultat)

```bash
curl -X POST "http://localhost:8000/pipelines/mon_pipeline/execute" \
  -H "Content-Type: application/json" \
  -d '{"parameters": {"données": "valeur"}, "wait": true, "timeout": 60}'

# Réponse (après complétion pipeline):
{
  "task_id": "abc123def456",
  "statut": "COMPLETED",
  "resultat": {"traité": "VALEUR"}
}
```

---

## 4. Intégration avec Tableaux de Bord Frontend

### 4.1. Exemple Dashboard React

```typescript
const PipelineStatus = ({ pipelineId }) => {
  const [status, setStatus] = useState(null);

  useEffect(() => {
    fetch(`/pipelines/${pipelineId}/status`)
      .then(res => res.json())
      .then(data => setStatus(data));

    // Poll toutes les 5 secondes
    const interval = setInterval(() => {
      fetch(`/pipelines/${pipelineId}/status`)
        .then(res => res.json())
        .then(setStatus);
    }, 5000);

    return () => clearInterval(interval);
  }, [pipelineId]);

  return (
    <div>
      <h3>Pipeline: {pipelineId}</h3>
      <p>Statut: {status?.statut}</p>
      <p>Progression: {status?.étapes_complétées} / {status?.total_étapes}</p>
    </div>
  );
};
```

### 4.2. Visualisation DAG

Utiliser endpoint DOT avec Graphviz:

```javascript
const renderDAG = async (pipelineId) => {
  const response = await fetch(`/pipelines/${pipelineId}/dag/dot`);
  const dot = await response.text();

  // Utiliser viz.js ou d3-graphviz côté client
  d3.select("#dag")
    .graphviz()
    .renderDot(dot);
};
```

---

## 5. Authentification & Sécurité

### 5.1. Authentification par Clé API

```python
from fastapi import Security, HTTPException
from fastapi.security import APIKeyHeader

api_key_header = APIKeyHeader(name="X-API-Key")

async def verify_api_key(api_key: str = Security(api_key_header)):
    if api_key != os.getenv("API_SECRET"):
        raise HTTPException(status_code=403, detail="Invalid API key")
    return api_key

@app.get("/pipelines")
async def list_pipelines(api_key: str = Security(verify_api_key)):
    return viz_api.list_pipelines()
```

### 5.2. Authentification JWT

```python
from jose import jwt
from fastapi import Depends

async def get_current_user(token: str = Depends(oauth2_scheme)):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
        return payload["sub"]
    except jwt.JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")

@app.post("/pipelines/{pipeline_id}/execute")
async def execute(
    pipeline_id: str,
    parameters: dict,
    user: str = Depends(get_current_user)
):
    # Logger action user pour audit
    logger.info(f"User {user} executed {pipeline_id}")
    return await run_pipeline(pipeline_id, parameters)
```

---

## 6. Limitation de Débit (Rate Limiting)

Protéger l'API contre abus:

```python
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

@app.post("/pipelines/{pipeline_id}/execute")
@limiter.limit("10/minute")  # Max 10 exécutions par minute par IP
async def execute_pipeline(pipeline_id: str, parameters: dict):
    # ...
```

---

## 7. Configuration CORS

Permettre requêtes cross-origin pour frontend web:

```python
from fastapi.middleware.cors import CORSMiddleware

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://votre-dashboard.com"],
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)
```

---

## 8. Déploiement en Production

### 8.1. Gunicorn + Workers Uvicorn

```bash
# Lancer avec multiples workers pour concurrence
gunicorn -k uvicorn.workers.UvicornWorker -w 4 main:app --bind 0.0.0.0:8000

# 4 processus workers gèrent requêtes concurrentes
```

### 8.2. Docker

```dockerfile
FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
```

```yaml
# docker-compose.yml
services:
  api:
    build: .
    ports:
      - "8000:8000"
    environment:
      - REDIS_URL=redis://redis:6379
    depends_on:
      - redis
  redis:
    image: redis:7-alpine
```

### 8.3. Derrière Reverse Proxy (nginx)

```nginx
server {
    listen 80;
    server_name api.taskiq-flow.example.com;

    location / {
        proxy_pass http://localhost:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_http_version 1.1;
        proxy_set_header Connection "";
    }
}
```

### 8.4. HTTPS avec Let's Encrypt

```bash
# Utiliser certbot avec nginx
sudo certbot --nginx -d api.taskiq-flow.example.com
```

Configurer HTTPS → redirect vers HTTP upstream:

```nginx
location / {
    proxy_pass http://localhost:8000;
    proxy_set_header X-Forwarded-Proto $scheme;
}
```

---

## 9. Monitoring de Santé API

### 9.1. Endpoint Health Check

```python
from fastapi import FastAPI
import psutil

app = FastAPI()

@app.get("/health")
async def health():
    return {
        "statut": "sain",
        "timestamp": datetime.utcnow().isoformat(),
        "broker_connecté": broker.is_connected(),
        "memoire_mb": psutil.Process().memory_info().rss / 1024 / 1024
    }
```

### 9.2. Métriques avec Prometheus

```python
from prometheus_fastapi_instrumentator import Instrumentator

Instrumentator().instrument(app).expose(app, endpoint="/metrics")
```

Expose `/metrics` avec métriques Prometheus standard (compte requêtes, latence, etc.).

### 9.3. Versionnement API

```python
app = FastAPI(
    title="API Taskiq-Flow",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Préfixer toutes routes avec /api/v1
from fastapi import APIRouter
api_router = APIRouter(prefix="/api/v1")
api_router.include_router(viz_api.router)
app.include_router(api_router)
```

---

## 10. Gestion des Erreurs

Gestion centralisée erreurs:

```python
from fastapi import Request
from fastapi.responses import JSONResponse
from taskiq.exceptions import TaskiqError

@app.exception_handler(TaskiqError)
async def taskiq_exception_handler(request: Request, exc: TaskiqError):
    return JSONResponse(
        status_code=500,
        content={
            "error": exc.__class__.__name__,
            "message": str(exc),
            "pipeline_id": getattr(exc, "pipeline_id", None)
        }
    )
```

Réponses d'erreur standardisées:

```json
{
  "error": "PipelineExecutionError",
  "message": "Task 'process' échoué après 3 retries",
  "pipeline_id": "analyse_audio_123",
  "step": "extract_audio",
  "timestamp": "2026-05-05T12:00:00Z"
}
```

---

## 11. Exemple Client API

Client Python pour interagir avec l'API:

```python
import httpx

class ClientTaskiqFlow:
    def __init__(self, base_url: str, api_key: str = None):
        self.base_url = base_url.rstrip("/")
        self.headers = {"X-API-Key": api_key} if api_key else {}

    async def list_pipelines(self):
        async with httpx.AsyncClient() as client:
            resp = await client.get(f"{self.base_url}/pipelines", headers=self.headers)
            resp.raise_for_status()
            return resp.json()

    async def execute(self, pipeline_id: str, parameters: dict, wait: bool = False):
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"{self.base_url}/pipelines/{pipeline_id}/execute",
                json={"parameters": parameters, "wait": wait},
                headers=self.headers
            )
            resp.raise_for_status()
            return resp.json()

    async def get_result(self, task_id: str):
        async with httpx.AsyncClient() as client:
            resp = await client.get(f"{self.base_url}/pipelines/result/{task_id}", headers=self.headers)
            resp.raise_for_status()
            return resp.json()

# Utilisation
client = ClientTaskiqFlow("http://localhost:8000")
pipelines = await client.list_pipelines()
résultat = await client.execute("mon_pipeline", {"données": "test"}, wait=True)
```

---

## 12. Résumé

| Fonctionnalité | Endpoint | Méthode |
|----------------|----------|---------|
| Health check | `/health` | GET |
| Lister pipelines | `/pipelines` | GET |
| Statut pipeline | `/pipelines/{id}/status` | GET |
| DAG (JSON) | `/pipelines/{id}/dag` | GET |
| DAG (DOT) | `/pipelines/{id}/dag/dot` | GET |
| Visualisation complète | `/pipelines/{id}/visualize` | GET |
| Exécuter pipeline | `/pipelines/{id}/execute` | POST (custom) |
| Obtenir résultat | `/pipelines/result/{task_id}` | GET (custom) |

**Point clé**: L'API donne contrôle complet sur cycle de vie pipeline — enregistrer, inspecter, exécuter, récupérer résultats — parfait pour tableaux de bord et intégrations personnalisés.

---

## Prochaines Étapes

- **[Guide WebSocket]({{ '/fr/guides/websocket.md' | relative_url }})** — Streaming d'événements en temps réel pour mises à jour live
- **[Guide de Suivi]({{ '/fr/guides/tracking.md' | relative_url }})** — Données historiques d'exécution pour analytics
- **[Exemple: Serveur API]({{ '/fr/examples/api-example.md' | relative_url }})** — App FastAPI complète fonctionnelle

---

*Gérez des pipelines de n'importe où. Construisez tableaux de bord, automatisation, intégrations.*
