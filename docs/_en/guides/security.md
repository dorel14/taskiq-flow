---
title: Security Guide
nav_order: 29
color_scheme: dark
---
# Security Guide

This guide explains how to secure your TaskIQ-Flow installation using the built-in security features: authentication, authorization, rate limiting, and audit logging.

## Overview

TaskIQ-Flow provides a flexible security system that can be enabled via configuration. When security is enabled (`security.enabled = true`), the following features are active:

- **Authentication**: Verify client identities using API keys or JWT tokens.
- **Authorization**: Enforce access control lists (ACLs) on pipelines and tasks.
- **Rate Limiting**: Limit the number of requests per client or IP address.
- **Audit Logging**: Log security-relevant events for monitoring and compliance.

## Configuration

Security features are configured in the `TaskiqFlowConfig` object or via environment variables. The main security settings are:

```python
from taskiq_flow import TaskiqFlowConfig

config = TaskiqFlowConfig(
    security=SecurityConfig(
        enabled=True,
        api_keys={
            "admin-key": {
                "role": "admin",
                "permissions": ["read", "write", "execute"],
                "pipeline_whitelist": ["*"],
            },
            "viewer-key": {
                "role": "viewer",
                "permissions": ["read"],
                "pipeline_whitelist": ["pipeline1", "pipeline2"],
            },
        },
        jwt_secret_key="your-secret-key", #pragma: allowlist secret
        rate_limit_per_minute=60,
        audit_log_path="audit.log",
    )
)
```

Alternatively, you can set environment variables:

- `TASKIQ_FLOW_SECURITY_ENABLED=true`
- `TASKIQ_FLOW_SECURITY_API_KEYS` (JSON string)
- `TASKIQ_FLOW_SECURITY_JWT_SECRET_KEY`
- `TASKIQ_FLOW_SECURITY_RATE_LIMIT_PER_MINUTE`
- `TASKIQ_FLOW_SECURITY_AUDIT_LOG_PATH`

## Authentication

TaskIQ-Flow supports two authentication methods:

### API Key Authentication

Clients must include their API key in the `X-API-Key` header for HTTP requests or in the `auth` field of WebSocket connect messages.

Example HTTP request:
```http
GET /api/pipelines
X-API-Key: admin-key 
```

### JWT Authentication

If a JWT secret is configured, clients can authenticate using a JSON Web Token (JWT) in the `Authorization` header:

```
Authorization: Bearer <jwt-token>
```

The JWT must contain a `sub` (subject) field identifying the user and optionally a `role` field.

## Authorization

Once authenticated, users are assigned a role and permissions. The system checks these permissions against the requested action and resource.

### Permissions

- `read`: View pipeline metadata, status, and results.
- `write`: Create, update, or delete pipelines.
- `execute`: Trigger pipeline execution.
- `admin`: Full access to all features, including security configuration.

### Pipeline Whitelist

Each API key can optionally specify a list of pipelines the user is allowed to access. If the whitelist is empty or contains `"*"`, the user can access all pipelines.

## Rate Limiting

To prevent abuse, TaskIQ-Flow limits the number of requests per client (identified by API key or IP address) per minute. The limit is configurable via `rate_limit_per_minute` (default: 60).

When the limit is exceeded, the server responds with HTTP 429 (Too Many Requests) or closes the WebSocket connection with a policy violation.

## Audit Logging

Security-relevant events are logged to the audit log (if configured) for later analysis. Events include:

- Authentication successes and failures
- Authorization denials
- Rate limit throttling
- Security configuration changes

The audit log is a simple text file with one JSON object per line, making it easy to parse with tools like `jq` or ingest into a SIEM system.

## WebSocket Security

WebSocket connections follow the same security model as HTTP:

1. During the WebSocket upgrade handshake, the client must authenticate via the `X-API-Key` header or a JWT in the `Authorization` header.
2. After authentication, each WebSocket message (e.g., subscribe/unsubscribe) is checked for permissions.
3. Unauthorized attempts result in an error message and connection termination.

## Example: Securing a Pipeline

Here's a complete example of securing a pipeline that processes sensitive data:

```python
from taskiq import Taskiq, InMemoryBroker
from taskiq_flow import TaskiqFlowConfig, SecurityConfig
from taskiq_flow.api import create_api
from taskiq_flow.events import HookManager

# Configure security
security_config = SecurityConfig(
    enabled=True,
    api_keys={
        "processor-key": {
            "role": "processor",
            "permissions": ["read", "execute"],
            "pipeline_whitelist": ["data-pipeline"],
        },
        "admin-key": {
            "role": "admin",
            "permissions": ["read", "write", "execute", "admin"],
            "pipeline_whitelist": ["*"],
        },
    },
    jwt_secret_key="super-secret", #pragma: allowlist secret
    rate_limit_per_minute=30,
    audit_log_path="security-audit.log",
)

config = TaskiqFlowConfig(security=security_config)

# Initialize broker and app
broker = InMemoryBroker()
taskiq = Taskiq(broker)
taskiq_app = create_api(taskiq, config=config)

# Define a secure pipeline
@taskiq.task
def process_sensitive_data(data: str) -> str:
    # Simulate processing
    return data.upper()

# Register the pipeline with the broker (optional, for manual triggering)
```

Start the application with `uvicorn taskiq_app:app --host 0.0.0.0 --port 8000`. All endpoints will now require authentication.

## Testing Security

To verify your security setup, try accessing an endpoint without credentials:

```bash
curl -i http://localhost:8000/api/pipelines
# Should return 401 Unauthorized

curl -i -H "X-API-Key: invalid-key" http://localhost:8000/api/pipelines
# Should return 401 Unauthorized

curl -i -H "X-API-Key: processor-key" http://localhost:8000/api/pipelines
# Should return 200 OK with pipeline list (if processor-key has read permission)
```

For WebSocket testing, use a WebSocket client library and include the `X-API-Key` header during the upgrade request.

## Conclusion

By following this guide, you can secure your TaskIQ-Flow instance to protect sensitive data and ensure that only authorized users can perform specific actions. For more details, refer to the API reference documentation.

---