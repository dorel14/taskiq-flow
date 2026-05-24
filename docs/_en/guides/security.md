---
title: Security Guide
nav_order: 29
color_scheme: dark
---
# Security Guide

This guide explains how to secure your TaskIQ-Flow installation using the built-in security features: authentication, authorization, rate limiting, HTTPS enforcement, and audit logging.

## Overview

TaskIQ-Flow provides a flexible security system that can be enabled via configuration. When security is enabled (`security_enabled = True`), the following features are active:

- **Authentication**: Verify client identities using API keys or JWT tokens.
- **Authorization**: Enforce access control lists (ACLs) on pipelines and tasks.
- **HTTPS Enforcement**: Block plain HTTP requests when `require_https` is `True`.
- **Rate Limiting**: Limit the number of requests per client or IP address, per endpoint.
- **Audit Logging**: Log security-relevant events for monitoring and compliance.

## Configuration

Security features are configured in the `TaskiqFlowConfig` object or via environment variables. The main security settings are:

```python
from taskiq_flow import TaskiqFlowConfig

config = TaskiqFlowConfig(
    # ---- Security toggle ----
    security_enabled=True,
    # ---- Authentication ----
    auth_provider="api_key",           # "api_key" or "jwt"
    api_keys={
        "admin-key": {
            "role": "admin",
            "pipelines": ["*"],
            "permissions": ["read", "execute", "admin"],
        },
        "viewer-key": {
            "role": "viewer",
            "pipelines": ["pipeline1", "pipeline2"],
            "permissions": ["read"],
        },
    },
    jwt_secret="your-jwt-secret",  # pragma: allowlist secret  # noqa: S105 — documented placeholder, not a real secret
    # ---- HTTPS enforcement ----
    require_https=True,
    # ---- Authorization (ACLs) ----
    pipeline_acls={
        "pipeline1": {
            "read": ["admin", "viewer"],
            "execute": ["admin"],
        },
    },
    # ---- Rate limiting ----
    rate_limit_enabled=True,
    rate_limit_default="100/minute",
    # ---- WebSocket ----
    websocket_require_auth=True,
    websocket_max_connections=1000,
)
```

Audit logs are handled by :class:`~taskiq_flow.security.audit.AuditLogger`,
instantiated automatically by the API (no configuration field needed).

Alternatively, you can set environment variables:

| Environment variable | Config field | Description |
|---|---|---|
| `TASKIQ_FLOW_SECURITY_ENABLED` | `security_enabled` | Enable/disable security (`true`/`false`) |
| `TASKIQ_FLOW_AUTH_PROVIDER` | `auth_provider` | Auth provider: `api_key` or `jwt` |
| `TASKIQ_FLOW_API_KEYS` | `api_keys` | JSON string of API key configs |
| `TASKIQ_FLOW_JWT_SECRET` | `jwt_secret` | JWT signing secret |
| `TASKIQ_FLOW_REQUIRE_HTTPS` | `require_https` | Enforce HTTPS (`true`/`false`) |
| `TASKIQ_FLOW_PIPELINE_ACLS` | `pipeline_acls` | JSON string of pipeline ACLs |
| `TASKIQ_FLOW_RATE_LIMIT_ENABLED` | `rate_limit_enabled` | Enable/disable rate limiting |
| `TASKIQ_FLOW_RATE_LIMIT_DEFAULT` | `rate_limit_default` | Default rate limit string (e.g. `"100/minute"`) |
| `TASKIQ_FLOW_WEBSOCKET_REQUIRE_AUTH` | `websocket_require_auth` | Require auth on WebSocket connections |

## Authentication

TaskIQ-Flow supports two authentication methods:

### API Key Authentication

Clients must include their API key in the `X-API-Key` header for HTTP requests or in the `auth` field of WebSocket connect messages.

Example HTTP request:

```http
GET /api/pipelines
X-API-Key: admin-key #pragma: allowlist secret
```

### JWT Authentication

If a JWT secret is configured (via `jwt_secret`), clients can authenticate using a JSON Web Token (JWT) in the `Authorization` header:

```
Authorization: Bearer <jwt-token>
```

The JWT must contain a `sub` (subject) field identifying the user and a `roles` list.

## Authorization

Once authenticated, users are assigned a role and permissions. The system checks these permissions against the requested action and resource using two mechanisms:

- **Pipeline ACLs** (`pipeline_acls`) — per-pipeline, per-permission access control.
- **Pipeline whitelist** (`pipelines` key in each API key entry) — simple allow-list of pipeline IDs; `"*"` means all pipelines.

### Permissions

- `read`: View pipeline metadata, status, and results.
- `execute`: Trigger pipeline execution.
- `admin`: Full access to all features, including security configuration.

### Pipeline Whitelist

Each API key entry may specify a `pipelines` list of pipeline IDs the key is allowed to access. If the list is empty or contains `"*"`, the key can access all pipelines.

## HTTPS Enforcement

When `require_https` is `True` (default), the :class:`~taskiq_flow.security.https.HTTPSEnforcementMiddleware` rejects all plain HTTP requests with HTTP 403.

The middleware respects the `X-Forwarded-Proto` header so deployments behind a TLS-terminating reverse proxy continue to work correctly.

## Deployment

### Docker Deployment

Run TaskIQ-Flow behind a WSGI/ASGI server such as Uvicorn with Docker:

```dockerfile
# Dockerfile
FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

EXPOSE 8000
CMD ["uvicorn", "my_app:app", "--host", "0.0.0.0", "--port", "8000"]
```

```yaml
# docker-compose.yml
services:
  redis:
    image: redis:7-alpine
    command: redis-server --requirepass your-redis-password # pragma: allowlist secret  # noqa: S105 — documented placeholder, not a real secret
    volumes:
      - redis_data:/data

  web:
    build: .
    ports:
      - "8000:8000"
    environment:
      - TASKIQ_FLOW_SECURITY_ENABLED=true
      - TASKIQ_FLOW_AUTH_PROVIDER=api_key
      - TASKIQ_FLOW_API_KEYS={"admin-key":{"role":"admin","pipelines":["*"],"permissions":["read","execute","admin"]}}
      - TASKIQ_FLOW_RATE_LIMIT_ENABLED=true
      - REDIS_URL=redis://redis:6379
    depends_on:
      - redis

volumes:
  redis_data:
```

### Reverse Proxy (nginx)

Place nginx in front of the application to terminate TLS, enforce HTTPS, and add security headers:

```nginx
# /etc/nginx/sites-available/taskiq-flow
server {
    listen 80;
    server_name taskiq-flow.example.com;
    return 301 https://$host$request_uri;  # Redirect HTTP → HTTPS
}

server {
    listen 443 ssl http2;
    server_name taskiq-flow.example.com;

    ssl_certificate /etc/letsencrypt/live/taskiq-flow.example.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/taskiq-flow.example.com/privkey.pem;
    ssl_protocols TLSv1.2 TLSv1.3;
    ssl_ciphers HIGH:!aNULL:!MD5;

    # Security headers
    add_header X-Frame-Options DENY;
    add_header X-Content-Type-Options nosniff;
    add_header X-XSS-Protection "1; mode=block";
    add_header Strict-Transport-Security "max-age=31536000; includeSubDomains" always;

    location / {
        proxy_pass http://localhost:8000;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }

    location /ws {
        proxy_pass http://localhost:8000;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
}
```

With this configuration:
1. All HTTP traffic is redirected to HTTPS (`require_https` also enforced inside the app)
2. Security headers are added to every response
3. WebSocket connections are proxied with correct upgrade headers
4. The app receives `X-Forwarded-Proto=https`, so the HTTPS enforcement middleware does not block legitimate requests

## Rate Limiting

To prevent abuse, TaskIQ-Flow limits the number of requests per endpoint per client IP address. The default limits are:

| Endpoint | Default limit |
|---|---|
| List pipelines | 60/minute |
| Get DAG | 120/minute |
| Get critical path | 120/minute |
| Get parallel groups | 120/minute |
| Execute pipeline | 10/minute |
| Get status | 30/minute |
| WebSocket connect | 5/minute |

The default limit for unknown endpoints is `rate_limit_default` (default: `"100/minute"`).

When the limit is exceeded, the server responds with HTTP 429 (Too Many Requests).

## Audit Logging

Security-relevant events are logged by :class:`~taskiq_flow.security.audit.AuditLogger` for later analysis. Events include:

- Authentication successes and failures
- Authorization denials
- Rate limit throttling events
- Pipeline actions (read, execute)
- Security configuration changes

Audit entries are emitted as Python ``logging`` records with structured ``extra`` fields, making them easy to ship to a SIEM. The built-in logger name is ``taskiq_flow.audit``.

## WebSocket Security

WebSocket connections follow the same security model as HTTP:

1. During the WebSocket upgrade handshake, the client must authenticate via the `X-API-Key` header or a JWT in the `Authorization` header.
2. After authentication, each WebSocket subscription message is checked against the user's pipeline ACLs.
3. Unauthorized attempts result in an error message and connection termination.

## Example: Securing a Pipeline

Here is a complete example of securing a TaskIQ-Flow API with the **current** API (`create_visualization_api`, flat `TaskiqFlowConfig` fields):

```python
from taskiq import Taskiq, InMemoryBroker
from taskiq_flow import TaskiqFlowConfig, create_visualization_api
from taskiq_flow.security import AuditLogger

# ── 1. Configure security ──────────────────────────────────────────
config = TaskiqFlowConfig(
    security_enabled=True,
    auth_provider="api_key",
    api_keys={
        "processor-key": {
            "role": "processor",
            "pipelines": ["data-pipeline"],
            "permissions": ["read", "execute"],
        },
        "admin-key": {
            "role": "admin",
            "pipelines": ["*"],
            "permissions": ["read", "execute", "admin"],
        },
    },
    jwt_secret="super-secret",  # pragma: allowlist secret
    require_https=True,
    pipeline_acls={
        "data-pipeline": {
            "read": ["processor", "admin"],
            "execute": ["processor", "admin"],
        },
    },
    rate_limit_enabled=True,
    rate_limit_default="30/minute",
    websocket_require_auth=True,
)

# ── 2. Initialize broker and API ───────────────────────────────────
broker = InMemoryBroker()
taskiq = Taskiq(broker)
app = create_visualization_api(broker)

# ── 3. Optional: custom audit logger ───────────────────────────────
audit_logger = AuditLogger()

# ── 4. Start ───────────────────────────────────────────────────────
# uvicorn app:app --host 0.0.0.0 --port 8000
# All endpoints now require authentication.
```

## Testing Security

```bash
# No credentials → 401 Unauthorized
curl -i http://localhost:8000/pipelines

# Invalid key → 403 Forbidden
curl -i -H "X-API-Key: invalid-key" http://localhost:8000/pipelines

# Valid viewer key → 200 OK
curl -i -H "X-API-Key: viewer-key" http://localhost:8000/pipelines
```

For WebSocket testing, use a WebSocket client library and include the `X-API-Key` header during the upgrade request.

## Conclusion

By following this guide you can secure your TaskIQ-Flow instance to protect
sensitive data and ensure that only authorized users can perform specific
actions. For full API reference, see the API documentation.

---
