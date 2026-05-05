---
title: Example: api_example.py
nav_order: 47
---
# Example: api_example.py

**REST API for pipeline management, visualization, and remote execution with FastAPI**

> **Version**: 0.3.2 | **File**: `examples/api_example.py`

---

## Overview

This comprehensive example demonstrates how to build a production-ready REST API for Taskiq-Flow using FastAPI. It covers:

- Setting up FastAPI with pipeline visualization endpoints
- Registering pipelines programmatically
- Adding custom endpoints for pipeline execution
- Retrieving pipeline results via API
- Full OpenAPI/Swagger documentation

**Prerequisites**: Install FastAPI and uvicorn:
```bash
pip install fastapi uvicorn[standard]
```

---

## What This Example Shows

- Using `PipelineVisualizationAPI` for built-in endpoints
- Registering pipelines with the API
- Creating custom endpoints to execute pipelines remotely
- Retrieving results by task ID
- Complete production API structure

---

## Code Walkthrough

### 1. Define Tasks and Pipeline

```python
from fastapi import FastAPI, HTTPException
from taskiq import InMemoryBroker
from taskiq_flow import DataflowPipeline, pipeline_task

broker = InMemoryBroker(await_inplace=True)

@broker.task
@pipeline_task(output="user_data")
async def fetch_user_data(user_id: int) -> dict:
    """Fetch user data from database."""
    await asyncio.sleep(0.1)
    return {"id": user_id, "name": f"User{user_id}", "email": f"user{user_id}@example.com"}

@broker.task
@pipeline_task(output="order_history")
async def fetch_orders(user_data: dict) -> list:
    """Fetch order history for user."""
    await asyncio.sleep(0.2)
    user_id = user_data["id"]
    return [{"order_id": 100 + user_id, "total": 99.99}]

@broker.task
@pipeline_task(output="recommendations")
async def generate_recommendations(user_data: dict, order_history: list):
    """Generate recommendations."""
    await asyncio.sleep(0.15)
    return ["product_A", "product_B", "product_C"]

# Build pipeline
sample_pipeline = DataflowPipeline.from_tasks(
    broker,
    [fetch_user_data, fetch_orders, generate_recommendations],
)
sample_pipeline.pipeline_id = "sample_recommendation_pipeline"
```

### 2. Create FastAPI App with Visualization API

```python
from taskiq_flow.api import create_visualization_api, PipelineVisualizationAPI

def create_app() -> FastAPI:
    app = FastAPI(title="TaskIQ Flow API", version="1.0.0")

    # Create visualization API (auto-mounts /pipelines endpoints)
    viz_api = create_visualization_api(broker, app)
    viz_api.add_pipeline("sample_recommendation_pipeline", sample_pipeline)

    # Custom endpoints below...
    return app
```

The `create_visualization_api()` automatically adds these endpoints:
- `GET /health`
- `GET /pipelines`
- `POST /pipelines/{pipeline_id}` (register)
- `GET /pipelines/{pipeline_id}/status`
- `GET /pipelines/{pipeline_id}/dag`
- `GET /pipelines/{pipeline_id}/dag/dot`
- `GET /pipelines/{pipeline_id}/visualize`

### 3. Add Custom Execute Endpoint

```python
@app.post("/pipelines/{pipeline_id}/execute")
async def execute_pipeline(
    pipeline_id: str,
    parameters: dict[str, Any],
) -> dict[str, Any]:
    """Execute a pipeline with given parameters."""
    if pipeline_id not in viz_api.pipelines:
        raise HTTPException(status_code=404, detail=f"Pipeline {pipeline_id} not found")

    pipeline = viz_api.pipelines[pipeline_id]
    try:
        result = await pipeline.kiq_dataflow(**parameters)
        return {
            "status": "executed",
            "pipeline_id": pipeline_id,
            "task_id": result.task_id,
            "message": "Pipeline execution started. Use /result/{task_id} to check status.",
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
```

### 4. Add Result Retrieval Endpoint

```python
@app.get("/pipelines/result/{task_id}")
async def get_result(task_id: str) -> dict[str, Any]:
    """Get the result of a pipeline execution."""
    try:
        result = await broker.result_backend.get_result(task_id)
        if result is None:
            raise HTTPException(status_code=404, detail=f"No result found for task_id {task_id}")
        return {"task_id": task_id, "result": result.return_value}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
```

### 5. Run the Server

```bash
uvicorn examples.api_example:create_app --reload --port 8000
```

Or programmatically:

```python
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(create_app(), host="0.0.0.0", port=8000)
```

---

## API Endpoints Reference

### Built-in (from `create_visualization_api`)

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Health check |
| GET | `/pipelines` | List all registered pipelines |
| POST | `/pipelines/{pipeline_id}` | Register new pipeline |
| GET | `/pipelines/{pipeline_id}/status` | Get current execution status |
| GET | `/pipelines/{pipeline_id}/dag` | Get DAG as JSON |
| GET | `/pipelines/{pipeline_id}/dag/dot` | Get DAG as DOT string |
| GET | `/pipelines/{pipeline_id}/visualize` | Full visualization metadata |

### Custom (defined in example)

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/pipelines/{pipeline_id}/execute` | Execute pipeline with parameters |
| GET | `/pipelines/result/{task_id}` | Get result by task ID |

---

## Testing the API

### 1. Interactive Docs
Open http://localhost:8000/docs for Swagger UI.

### 2. Execute Pipeline

```bash
curl -X POST "http://localhost:8000/pipelines/sample_recommendation_pipeline/execute" \
  -H "Content-Type: application/json" \
  -d '{"user_id": 123}'
```

Response:
```json
{
  "status": "executed",
  "pipeline_id": "sample_recommendation_pipeline",
  "task_id": "abc123def456",
  "message": "Pipeline execution started..."
}
```

### 3. Poll for Result

```bash
curl "http://localhost:8000/pipelines/result/abc123def456"
```

Response:
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

### 4. View DAG

```bash
curl "http://localhost:8000/pipelines/sample_recommendation_pipeline/dag"
```

Returns JSON structure of the pipeline graph.

---

## Programmatic API Usage

You can also use the API classes directly without HTTP:

```python
from taskiq_flow.api import PipelineVisualizationAPI

app = FastAPI()
viz_api = PipelineVisualizationAPI(broker, app)

# Register pipeline
viz_api.add_pipeline("my_pipe", my_pipeline)

# List registered pipelines
for pid, p in viz_api.pipelines.items():
    print(f"Pipeline: {pid}, tasks: {len(p.visualize()['nodes'])}")

# Get visualization
dag_json = my_pipeline.visualize()
dot = my_pipeline.visualize_dot()
```

This is useful for building custom dashboard backends or CLI tools.

---

## Production Considerations

### 1. Use Persistent Broker
```python
from taskiq import RedisStreamBroker
broker = RedisStreamBroker(redis_url="redis://localhost:6379")
```

### 2. Add Authentication
```python
from fastapi import Depends, Security
from fastapi.security import APIKeyHeader

api_key_header = APIKeyHeader(name="X-API-Key")

@app.post("/pipelines/{pipeline_id}/execute")
async def execute(..., api_key: str = Security(verify_api_key)):
    # ...
```

### 3. Add Rate Limiting
```python
from slowapi import Limiter
limiter = Limiter(key_func=get_remote_address)
@app.post("/pipelines/{pipeline_id}/execute")
@limiter.limit("10/minute")
async def execute(...):
    # ...
```

### 4. Enable CORS for Web Frontend
```python
from fastapi.middleware.cors import CORSMiddleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://your-dashboard.com"],
    allow_methods=["*"],
    allow_headers=["*"],
)
```

### 5. Deploy with Gunicorn
```bash
gunicorn -k uvicorn.workers.UvicornWorker -w 4 main:app --bind 0.0.0.0:8000
```

---

## Learning Path

After this example:

1. **[API Guide]({{ '/en/guides/api.md' | relative_url }})** — Full REST API documentation and best practices
2. **[WebSocket Guide]({{ '/en/guides/websocket.md' | relative_url }})** — Add real-time updates to your API
3. **[Tracking Guide]({{ '/en/guides/tracking.md' | relative_url }})** — Store execution history for analytics

---

*This example provides a complete, production-ready API foundation. Extend it with authentication, rate limiting, and custom endpoints for your specific use case.*
