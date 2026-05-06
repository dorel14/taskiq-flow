---
title: REST API Guide
nav_order: 28
---
# REST API Guide

**FastAPI-based pipeline management, visualization, and remote execution**

> **Version**: 0.4.0 | **Related**: [Tracking Guide]({{ '/en/guides/tracking/' | relative_url }}), [WebSocket Guide]({{ '/en/guides/websocket/' | relative_url }})

---

## Overview

Taskiq-Flow includes a FastAPI-based REST API for managing pipelines remotely. Build dashboards, CI/CD integrations, or any system that needs to interact with pipelines via HTTP.

This guide covers:

- Setting up the API server
- Available endpoints
- Pipeline visualization endpoints
- Custom endpoint extensions
- Authentication considerations
- Production deployment patterns

---

## 1. Quick Setup

```python
from fastapi import FastAPI
from taskiq import InMemoryBroker
from taskiq_flow import DataflowPipeline, pipeline_task, create_visualization_api

# 1. Create broker and tasks
broker = InMemoryBroker(await_inplace=True)

@broker.task
@pipeline_task(output="result")
async def process(data: str) -> dict:
    return {"processed": data.upper()}

# 2. Build pipeline
pipeline = DataflowPipeline.from_tasks(broker, [process])
pipeline.pipeline_id = "my_pipeline"

# 3. Create FastAPI app with visualization API
app = FastAPI(title="Taskiq-Flow API", version="0.3.0")
viz_api = create_visualization_api(broker, app)
viz_api.add_pipeline("my_pipeline", pipeline)

# 4. Run with uvicorn
# uvicorn main:app --reload --port 8000
```

All endpoints are automatically mounted under `/pipelines`.

---

## 2. Available Endpoints

The visualization API provides these routes:

### 2.1. Health Check

```
GET /health
```

Returns simple health status:

```json
{
  "status": "healthy",
  "timestamp": "2026-05-05T12:00:00Z"
}
```

### 2.2. List All Pipelines

```
GET /pipelines
```

Lists all registered pipelines with metadata:

```json
[
  {
    "pipeline_id": "audio_analysis_v1",
    "pipeline_type": "dataflow",
    "tasks": ["extract", "tag", "embed"],
    "created_at": "2026-05-05T10:00:00Z"
  }
]
```

### 2.3. Register a New Pipeline

```
POST /pipelines/{pipeline_id}
```

Request body:

```json
{
  "pipeline_type": "dataflow",
  "tasks": ["task1", "task2"]
}
```

Or use the Python API directly (recommended):

```python
viz_api.add_pipeline("new_pipeline", pipeline_object)
```

### 2.4. Get Pipeline Status

```
GET /pipelines/{pipeline_id}/status
```

Returns current execution status if a run is active:

```json
{
  "pipeline_id": "my_pipeline_123",
  "status": "RUNNING",
  "steps_completed": 3,
  "total_steps": 5,
  "started_at": "2026-05-05T12:00:00Z"
}
```

### 2.5. Get DAG as JSON

```
GET /pipelines/{pipeline_id}/dag
```

Returns the directed acyclic graph structure:

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

### 2.6. Get DAG in DOT Format

```
GET /pipelines/{pipeline_id}/dag/dot
```

Returns Graphviz-compatible DOT string for visualization:

```
digraph "my_pipeline" {
  node [shape=box];
  extract -> tag;
  extract -> embed;
}
```

### 2.7. Full Pipeline Visualization

```
GET /pipelines/{pipeline_id}/visualize
```

Returns comprehensive pipeline metadata:

```json
{
  "pipeline_id": "my_pipeline",
  "type": "dataflow",
  "tasks": [
    {
      "name": "extract",
      "outputs": ["features"],
      "inputs": [],
      "description": "Extract features from audio"
    },
    {
      "name": "tag",
      "inputs": ["features"],
      "outputs": ["tags"],
      "description": "Generate tags"
    }
  ],
  "execution_levels": [
    ["extract"],
    ["tag", "embed"]
  ]
}
```

---

## 3. Executing Pipelines via API

The core API focuses on management and visualization. To execute pipelines remotely, add a custom endpoint:

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
    Execute a pipeline with given parameters.

    - **pipeline_id**: Registered pipeline ID
    - **parameters**: Dict of input parameters
    - **wait**: If True, block until completion and return result
    - **timeout**: Seconds to wait before timing out
    """
    if pipeline_id not in viz_api.pipelines:
        raise HTTPException(status_code=404, detail="Pipeline not found")

    pipeline = viz_api.pipelines[pipeline_id]

    try:
        task = await pipeline.kiq_dataflow(**parameters)

        if wait:
            result = await task.wait_result(timeout=timeout)
            return {
                "task_id": task.task_id,
                "status": "COMPLETED",
                "result": result.return_value
            }
        else:
            return {
                "task_id": task.task_id,
                "status": "STARTED"
            }
    except asyncio.TimeoutError:
        raise HTTPException(status_code=504, detail="Pipeline execution timed out")
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))

@app.get("/pipelines/result/{task_id}")
async def get_result(task_id: str):
    """Get the result of a pipeline execution."""
    result = await broker.get_result(task_id)
    if result is None:
        raise HTTPException(status_code=404, detail="Result not found or expired")
    return {"task_id": task_id, "result": result.return_value}
```

### 3.1. Execute Async (Fire-and-Forget)

```bash
curl -X POST "http://localhost:8000/pipelines/my_pipeline/execute" \
  -H "Content-Type: application/json" \
  -d '{"parameters": {"data": "input_value"}, "wait": false}'

# Response:
{
  "task_id": "abc123def456",
  "status": "STARTED"
}
```

### 3.2. Execute Synchronous (Wait for Result)

```bash
curl -X POST "http://localhost:8000/pipelines/my_pipeline/execute" \
  -H "Content-Type: application/json" \
  -d '{"parameters": {"data": "input_value"}, "wait": true, "timeout": 60}'

# Response (after pipeline completes):
{
  "task_id": "abc123def456",
  "status": "COMPLETED",
  "result": {"processed": "INPUT_VALUE"}
}
```

---

## 4. Integration with Frontend Dashboards

### 4.1. React Dashboard Example

```typescript
// React component displaying pipeline status
const PipelineStatus = ({ pipelineId }) => {
  const [status, setStatus] = useState(null);

  useEffect(() => {
    fetch(`/pipelines/${pipelineId}/status`)
      .then(res => res.json())
      .then(data => setStatus(data));

    // Poll every 5 seconds
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
      <p>Status: {status?.status}</p>
      <p>Progress: {status?.steps_completed} / {status?.total_steps}</p>
    </div>
  );
};
```

### 4.2. DAG Visualization

Use the DOT endpoint with Graphviz:

```javascript
const renderDAG = async (pipelineId) => {
  const response = await fetch(`/pipelines/${pipelineId}/dag/dot`);
  const dot = await response.text();

  // Use viz.js or d3-graphviz client-side
  d3.select("#dag")
    .graphviz()
    .renderDot(dot);
};
```

---

## 5. Authentication & Security

### 5.1. API Key Authentication

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

### 5.2. JWT Authentication

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
    # Log user's action for audit trail
    logger.info(f"User {user} executed {pipeline_id}")
    return await run_pipeline(pipeline_id, parameters)
```

---

## 6. Rate Limiting

Protect the API from abuse:

```python
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

@app.post("/pipelines/{pipeline_id}/execute")
@limiter.limit("10/minute")  # Max 10 executions per minute per IP
async def execute_pipeline(pipeline_id: str, parameters: dict):
    # ...
```

---

## 7. CORS Configuration

Enable cross-origin requests for web frontend:

```python
from fastapi.middleware.cors import CORSMiddleware

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://your-dashboard.com"],
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)
```

---

## 8. Production Deployment

### 8.1. Gunicorn + Uvicorn Workers

```bash
# Run with multiple workers for concurrency
gunicorn -k uvicorn.workers.UvicornWorker -w 4 main:app --bind 0.0.0.0:8000

# 4 worker processes handle concurrent requests
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

### 8.3. Behind Reverse Proxy (nginx)

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

### 8.4. HTTPS with Let's Encrypt

```bash
# Using certbot with nginx
sudo certbot --nginx -d api.taskiq-flow.example.com
```

Configure HTTPS → redirect to HTTP upstream:

```nginx
location / {
    proxy_pass http://localhost:8000;
    proxy_set_header X-Forwarded-Proto $scheme;
}
```

---

## 9. Monitoring API Health

### 9.1. Health Check Endpoint

```python
from fastapi import FastAPI
import psutil

app = FastAPI()

@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "broker_connected": broker.is_connected(),
        "memory_mb": psutil.Process().memory_info().rss / 1024 / 1024
    }
```

### 9.2. Metrics with Prometheus

```python
from prometheus_fastapi_instrumentator import Instrumentator

Instrumentator().instrument(app).expose(app, endpoint="/metrics")
```

Exposes `/metrics` with standard Prometheus metrics (request count, latency, etc.).

### 9.3. API Versioning

```python
app = FastAPI(
    title="Taskiq-Flow API",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Prefix all routes with /api/v1
from fastapi import APIRouter
api_router = APIRouter(prefix="/api/v1")
api_router.include_router(viz_api.router)
app.include_router(api_router)
```

---

## 10. Error Handling

Centralized error handling:

```python
from fastapi import Request
from fastapi.responses import JSONResponse

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

Standardized error responses:

```json
{
  "error": "PipelineExecutionError",
  "message": "Task 'process' failed after 3 retries",
  "pipeline_id": "audio_analysis_123",
  "step": "extract_audio",
  "timestamp": "2026-05-05T12:00:00Z"
}
```

---

## 11. API Client Example

Python client for interacting with the API:

```python
import httpx

class TaskiqFlowClient:
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

# Usage
client = TaskiqFlowClient("http://localhost:8000")
pipelines = await client.list_pipelines()
result = await client.execute("my_pipeline", {"data": "test"}, wait=True)
```

---

## 12. Summary

| Feature | Endpoint | Method |
|---------|----------|--------|
| Health check | `/health` | GET |
| List pipelines | `/pipelines` | GET |
| Pipeline status | `/pipelines/{id}/status` | GET |
| Get DAG (JSON) | `/pipelines/{id}/dag` | GET |
| Get DAG (DOT) | `/pipelines/{id}/dag/dot` | GET |
| Full visualization | `/pipelines/{id}/visualize` | GET |
| Execute pipeline | `/pipelines/{id}/execute` | POST (custom) |
| Get result | `/pipelines/result/{task_id}` | GET (custom) |

**Key takeaway**: The API gives you full control over pipeline lifecycle — register, inspect, execute, and retrieve results — perfect for custom dashboards and integrations.

---

## Next Steps

- **[WebSocket Guide]({{ '/en/guides/websocket/' | relative_url }})** — Real-time event streaming for live updates
- **[Tracking Guide]({{ '/en/guides/tracking/' | relative_url }})** — Historical execution data for analytics
- **[Example: API Server]({{ '/en/examples/api-example/' | relative_url }})** — Complete working FastAPI app

---

*Manage pipelines from anywhere. Build dashboards, automation, and integrations.*
