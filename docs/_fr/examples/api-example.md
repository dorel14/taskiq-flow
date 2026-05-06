---
permalink: /fr/examples/api-example/
title: Exemple: api_example.py
nav_order: 47
color_scheme: dark
---
# Exemple: api_example.md

**Intégration FastAPI pour gestion distante de pipelines**

> **Version** : 0.4.5 | **Fichier** : `examples/api_example.py`

---

## Aperçu

Cet exemple exhaustif démontre comment construire une API REST production-ready pour Taskiq-Flow en utilisant FastAPI. Il couvre:

- Configuration FastAPI avec endpoints visualization pipeline
- Enregistrer pipelines programmatiquement
- Ajouter endpoints personnalisés pour exécution distante pipeline
- Récupérer résultats pipeline via API
- Documentation OpenAPI/Swagger complète

**Prérequis**: Installer FastAPI et uvicorn:
```bash
pip install fastapi uvicorn[standard]
```

---

## Ce Que Cet Exemple Montre

- Utilisation de `PipelineVisualizationAPI` pour endpoints built-in
- Enregistrement pipelines avec l'API
- Création endpoints personnalisés pour exécution distante pipeline
- Récupération résultats par task ID
- Structure API complète production

---

## Explication du Code

### 1. Définir Tâches et Pipeline

```python
from fastapi import FastAPI, HTTPException
from taskiq import InMemoryBroker
from taskiq_flow import DataflowPipeline, pipeline_task

broker = InMemoryBroker(await_inplace=True)

@broker.task
@pipeline_task(output="user_data")
async def fetch_user_data(user_id: int) -> dict:
    """Récupérer données utilisateur depuis base."""
    await asyncio.sleep(0.1)
    return {"id": user_id, "name": f"User{user_id}", "email": f"user{user_id}@example.com"}

@broker.task
@pipeline_task(output="order_history")
async def fetch_orders(user_data: dict) -> list:
    """Récupérer historique commandes utilisateur."""
    await asyncio.sleep(0.2)
    user_id = user_data["id"]
    return [{"order_id": 100 + user_id, "total": 99.99}]

@broker.task
@pipeline_task(output="recommendations")
async def generate_recommendations(user_data: dict, order_history: list):
    """Générer recommandations."""
    await asyncio.sleep(0.15)
    return ["product_A", "product_B", "product_C"]

# Construire pipeline
sample_pipeline = DataflowPipeline.from_tasks(
    broker,
    [fetch_user_data, fetch_orders, generate_recommendations],
)
sample_pipeline.pipeline_id = "sample_recommendation_pipeline"
```

### 2. Créer App FastAPI avec Visualization API

```python
from taskiq_flow.api import create_visualization_api, PipelineVisualizationAPI

def create_app() -> FastAPI:
    app = FastAPI(title="TaskIQ Flow API", version="1.0.0")

    # Créer visualization API (monte automatiquement endpoints /pipelines)
    viz_api = create_visualization_api(broker, app)
    viz_api.add_pipeline("sample_recommendation_pipeline", sample_pipeline)

    # Ajouter endpoints personnalisés ci-dessous...
    return app
```

`create_visualization_api()` ajoute automatiquement endpoints:
- `GET /pipelines` — Lister tous les pipelines enregistrés
- `GET /pipelines/{pipeline_id}` — Obtenir pipeline par ID
- `GET /pipelines/{pipeline_id}/dag` — DAG JSON
- `GET /pipelines/{pipeline_id}/dag/dot` — DAG format DOT
- `GET /pipelines/{pipeline_id}/visualize` — Métadonnées complètes

### 3. Ajouter Endpoint Exécution Personnalisé

```python
@app.post("/pipelines/{pipeline_id}/execute")
async def execute_pipeline(
    pipeline_id: str,
    parameters: dict[str, Any],
) -> dict[str, Any]:
    """Exécute un pipeline avec paramètres donnés."""
    if pipeline_id not in viz_api.pipelines:
        raise HTTPException(status_code=404, detail=f"Pipeline {pipeline_id} non trouvé")

    pipeline = viz_api.pipelines[pipeline_id]
    try:
        result = await pipeline.kiq_dataflow(**parameters)
        return {
            "status": "executed",
            "pipeline_id": pipeline_id,
            "task_id": result.task_id,
            "message": "Pipeline execution started. Use /result/{task_id} pour vérifier statut.",
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
```

### 4. Ajouter Endpoint Récupération Résultat

```python
@app.get("/pipelines/result/{task_id}")
async def get_result(task_id: str) -> dict[str, Any]:
    """Récupère le résultat d'une exécution de pipeline."""
    try:
        result = await broker.result_backend.get_result(task_id)
        if result is None:
            raise HTTPException(status_code=404, detail=f"Aucun résultat trouvé pour task_id {task_id}")
        return {"task_id": task_id, "result": result.return_value}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
```

### 5. Lancer le Serveur

```bash
uvicorn examples.api_example:create_app --reload --port 8000
```

Ou programmatiquement:

```python
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(create_app(), host="0.0.0.0", port=8000)
```

---

## Référence des Endpoints API

### Built-in (depuis `create_visualization_api`)

| Méthode | Endpoint | Description |
|---------|----------|-------------|
| GET | `/health` | Health check |
| GET | `/pipelines` | Lister pipelines enregistrés |
| POST | `/pipelines/{pipeline_id}` | Enregistrer nouveau pipeline |
| GET | `/pipelines/{pipeline_id}/status` | Obtenir statut exécution courant |
| GET | `/pipelines/{pipeline_id}/dag` | Obtenir DAG en JSON |
| GET | `/pipelines/{pipeline_id}/dag/dot` | Obtenir DAG en format DOT |
| GET | `/pipelines/{pipeline_id}/visualize` | Métadonnées complètes visualization |

### Personnalisés (définis dans exemple)

| Méthode | Endpoint | Description |
|---------|----------|-------------|
| POST | `/pipelines/{pipeline_id}/execute` | Exécuter pipeline avec paramètres |
| GET | `/pipelines/result/{task_id}` | Obtenir résultat par task ID |

---

## Tester l'API

### 1. Docs Interactives
Ouvrir http://localhost:8000/docs pour Swagger UI.

### 2. Exécuter Pipeline

```bash
curl -X POST "http://localhost:8000/pipelines/sample_recommendation_pipeline/execute" \
  -H "Content-Type: application/json" \
  -d '{"user_id": 123}'
```

Réponse:
```json
{
  "status": "executed",
  "pipeline_id": "sample_recommendation_pipeline",
  "task_id": "abc123def456",
  "message": "Pipeline execution started..."
}
```

### 3. Sonder pour Résultat

```bash
curl "http://localhost:8000/pipelines/result/abc123def456"
```

Réponse:
```json
{
  "task_id": "abc123def456",
  "result": {
    "user_data": {"id": 123, "name": "User123", ...},
    "order_history": [...],
    "recommendations": ["product_A", "product_B", "product_C"]
  }
}
```

### 4. Voir DAG

```bash
curl "http://localhost:8000/pipelines/sample_recommendation_pipeline/dag"
```

Retourne structure JSON du graphe pipeline.

---

## Utilisation API Programmatique

Vous pouvez aussi utiliser classes API directement sans HTTP:

```python
from taskiq_flow.api import PipelineVisualizationAPI

app = FastAPI()
viz_api = PipelineVisualizationAPI(broker, app)

# Enregistrer pipeline
viz_api.add_pipeline("mon_pipe", mon_pipeline)

# Lister pipelines enregistrés
for pid, p in viz_api.pipelines.items():
    print(f"Pipeline: {pid}, tasks: {len(p.visualize()['nodes'])}")

# Obtenir visualization
dag_json = mon_pipeline.visualize()
dot = mon_pipeline.visualize_dot()
```

Utile pour construire backends dashboard personnalisés ou outils CLI.

---

## Considérations Production

### 1. Utiliser Broker Persistant
```python
from taskiq import RedisStreamBroker
broker = RedisStreamBroker(redis_url="redis://localhost:6379")
```

### 2. Ajouter Authentication
```python
from fastapi import Depends, Security
from fastapi.security import APIKeyHeader

api_key_header = APIKeyHeader(name="X-API-Key")

@app.post("/pipelines/{pipeline_id}/execute")
async def execute(..., api_key: str = Security(verify_api_key)):
    # ...
```

### 3. Ajouter Rate Limiting
```python
from slowapi import Limiter
limiter = Limiter(key_func=get_remote_address)
@app.post("/pipelines/{pipeline_id}/execute")
@limiter.limit("10/minute")
async def execute(...):
    # ...
```

### 4. Activer CORS pour Frontend Web
```python
from fastapi.middleware.cors import CORSMiddleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://votre-dashboard.com"],
    allow_methods=["*"],
    allow_headers=["*"],
)
```

### 5. Déployer avec Gunicorn
```bash
gunicorn -k uvicorn.workers.UvicornWorker -w 4 main:app --bind 0.0.0.0:8000
```

---

## Chemin d'Apprentissage

Après cet exemple:

1. **[Guide API]({{ '/fr/guides/api/' | relative_url }})** — Documentation complète endpoints REST et meilleures pratiques
2. **[Guide WebSocket]({{ '/fr/guides/websocket/' | relative_url }})** — Ajouter mises à jour temps réel à votre API
3. **[Guide de Suivi]({{ '/fr/guides/tracking/' | relative_url }})** — Stocker historique exécution pour analytics

---

*Cet exemple fournit fondation API complète production-ready. Étendez-le avec authentication, rate limiting, et endpoints personnalisés pour votre cas d'usage spécifique.*
