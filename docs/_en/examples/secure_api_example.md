---
permalink: /en/examples/secure-api-example/
title: Example: secure_api_example.py
nav_order: 46
color_scheme: dark
---
# Example: secure_api_example.py

**API security with authentication, rate limiting, and audit logging**

> **Version**: 0.4.5 | **File**: `examples/secure_api_example.py`

---

## Overview

This example demonstrates how to secure your Taskiq-Flow FastAPI integration using the built-in security features introduced in v0.4.5. It covers:

- Configuring `TaskiqFlowConfig` with security settings
- Setting up API key authentication with role-based access control
- Enabling rate limiting on API endpoints
- Adding audit logging for compliance
- Creating a secure FastAPI application with JWT support

---

## What This Example Shows

- Creating a `TaskiqFlowConfig` with `security_enabled=True`
- Defining API keys with roles and pipeline ACLs
- Integrating `create_visualization_api()` with security config
- Adding custom audit logging endpoints
- Running a secure API server

---

## Code Walkthrough

### 1. Broker and Tasks Setup

```python
from taskiq import InMemoryBroker
from taskiq_flow import DataflowPipeline, pipeline_task

broker = InMemoryBroker(await_inplace=True)

@broker.task
@pipeline_task(output="result")
async def process_data(data: str) -> dict:
    return {"processed": data.upper(), "status": "ok"}

@broker.task
@pipeline_task(output="validated")
async def validate_result(result: dict) -> dict:
    if result.get("status") != "ok":
        raise ValueError("Invalid result")
    return {**result, "validated": True}

pipeline = DataflowPipeline.from_tasks(broker, [process_data, validate_result])
pipeline.pipeline_id = "secure_demo_pipeline"
```

---

### 2. Security Configuration

```python
from taskiq_flow import TaskiqFlowConfig

config = TaskiqFlowConfig(
    security_enabled=True,
    auth_provider="api_key",
    api_keys={
        "sk_admin_full": {
            "role": "admin",
            "pipelines": ["*"],  # Access to all pipelines
        },
        "sk_viewer_reports": {
            "role": "viewer",
            "pipelines": ["report_*"],  # Only pipelines starting with 'report_'
        },
    },
    require_https=False,  # Set to True in production
    rate_limit_enabled=True,
    rate_limit_default="60/minute",
)
```

**Key security features:**

- **Authentication**: API key-based (supports JWT as alternative)
- **Authorization**: Pipeline-level ACLs with wildcard patterns
- **Rate Limiting**: Per-endpoint limits using slowapi
- **HTTPS Enforcement**: Configurable (disabled here for local dev)

---

### 3. Creating the Secure API

```python
from fastapi import FastAPI
from taskiq_flow import create_visualization_api

app = FastAPI(title="Secure Taskiq-Flow API", version="0.4.5")
viz_api = create_visualization_api(broker, app, config=config)
viz_api.add_pipeline("secure_demo_pipeline", pipeline)
```

The `create_visualization_api` function automatically applies security middleware when `config.security_enabled=True`. All endpoints require authentication and respect rate limits.

---

### 4. Custom Audit Logging

```python
from taskiq_flow.security.audit import AuditLogger

audit_logger = AuditLogger()

@app.post("/execute-with-audit")
async def execute_with_audit(data: str, user: str = "demo_user"):
    await audit_logger.log_access(
        user={"sub": user},
        action="execute_pipeline",
        pipeline_id=pipeline.pipeline_id,
        success=True,
        details={"input_length": len(data)},
    )
    result = await pipeline.kiq_dataflow(data=data)
    return {"task_id": result.task_id, "status": "started"}
```

Audit logging records every access for compliance and monitoring.

---

### 5. Running the Server

```python
if __name__ == "__main__":
    import uvicorn
    print("API Key for admin: sk_admin_full")
    print("API Key for viewer: sk_viewer_reports")
    uvicorn.run(app, host="0.0.0.0", port=8000)
```

Test with:

```bash
curl -H "X-API-Key: sk_admin_full" http://localhost:8000/pipelines
```

Swagger docs at: http://localhost:8000/docs (security enforced there too).

---

## Security Features Explained

### Authentication Providers

| Provider | How it works |
|----------|--------------|
| `api_key` | Simple header-based API keys (`X-API-Key: <key>`) |
| `jwt` | Bearer token authentication with JWT validation |

Switch via `auth_provider` parameter in config.

### Authorization (ACLs)

Pipeline ACLs control which roles can access which pipelines:

```python
pipeline_acls = {
    "*": {"read": ["admin", "viewer"]},  # All pipelines
    "report_*": {"write": ["admin"]},     # Only admin can modify report pipelines
}
```

Wildcards (`*`) supported in pipeline IDs.

### Rate Limiting

Uses `slowapi` under the hood. Configure per-endpoint or default:

```python
rate_limit_enabled=True
rate_limit_default="100/minute"
```

Custom limits per route:

```python
@viz_api.router.post("/pipelines/{pipeline_id}/execute", rate_limit="10/minute")
async def execute_pipeline(...):
    ...
```

### Audit Logging

All authenticated requests are logged automatically. Custom audit events:

```python
await audit_logger.log_access(
    user=user_dict,
    action="pipeline_execute",
    pipeline_id="my_pipeline",
    success=True,
    details={"param": "value"},
)
```

---

## Expected Output

When you start the server:

```
Starting secure API server...
API Key for admin: sk_admin_full
API Key for viewer: sk_viewer_reports

Test with:
  curl -H "X-API-Key: sk_admin_full" http://localhost:8000/pipelines

Docs at: http://localhost:8000/docs
```

When calling secured endpoints without a key:

```json
{
  "detail": "Not authenticated"
}
```

With valid API key:

```json
{
  "pipelines": ["secure_demo_pipeline"]
}
```

---

## Key Points

### Production Checklist

- [ ] Set `require_https=True` for production
- [ ] Use strong, randomly generated API keys
- [ ] Store keys in environment variables or secret manager
- [ ] Enable audit logging to file/database
- [ ] Configure fine-grained ACLs per pipeline
- [ ] Set appropriate rate limits per endpoint
- [ ] Use JWT auth for OAuth2 integration
- [ ] Rotate API keys periodically

### Switching to JWT

```python
config = TaskiqFlowConfig(
    auth_provider="jwt",
    jwt_secret="your-super-secret-key-change-this",
    jwt_algorithm="HS256",
)
```

Then authenticate with:

```bash
curl -H "Authorization: Bearer <jwt-token>" http://localhost:8000/pipelines
```

---

## Learning Path

After this example:

1. **[Security Guide]({{ '/en/guides/api/#security-observability' | relative_url }})** — Full security & observability features
2. **[API Guide]({{ '/en/guides/api/' | relative_url }})** — FastAPI integration patterns
3. **[WebSocket Security]({{ '/en/guides/websocket/#websocket-security' | relative_url }})** — Securing real-time connections

---

*This example demonstrates production-ready security patterns. Adapt the ACLs and rate limits to your specific use case.*
