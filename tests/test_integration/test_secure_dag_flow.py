"""
Tests for secure DAG flow with authentication and authorization.

This module tests the SecurityMiddleware in isolation
with API key authentication.

Author: SoniqueBay Team
Version: 1.2.0
"""

from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from taskiq_flow.security import (
    APIKeyAuthProvider,
    AuditLogger,
    Permission,
    PipelineAuthorization,
    RateLimiter,
    SecurityMiddleware,
)


@pytest.fixture
def auth_provider() -> APIKeyAuthProvider:
    """Create API key auth provider with test keys."""
    keys = {
        "admin-key": {
            "role": "admin",
            "pipeline_whitelist": ["*"],
            "permissions": ["read", "execute", "admin"],
        },
        "user-key": {
            "role": "user",
            "pipeline_whitelist": ["test-pipeline", "allowed-pipeline"],
            "permissions": ["read", "execute"],
        },
        "readonly-key": {
            "role": "readonly",
            "pipeline_whitelist": ["readonly-pipeline"],
            "permissions": ["read"],
        },
        "no-pipeline-key": {
            "role": "user",
            "pipeline_whitelist": [],
            "permissions": ["read"],
        },
    }
    return APIKeyAuthProvider(keys)


@pytest.fixture
def authorization() -> PipelineAuthorization:
    """Create pipeline authorization with test ACLs."""
    authz = PipelineAuthorization()
    authz.set_acl("*", Permission.READ, ["admin"])
    authz.set_acl("*", Permission.EXECUTE, ["admin"])
    authz.set_acl("test-pipeline", Permission.READ, ["user", "admin"])
    authz.set_acl("test-pipeline", Permission.EXECUTE, ["user", "admin"])
    authz.set_acl("allowed-pipeline", Permission.READ, ["user", "admin"])
    authz.set_acl("allowed-pipeline", Permission.EXECUTE, ["user", "admin"])
    authz.set_acl("readonly-pipeline", Permission.READ, ["readonly", "user", "admin"])
    return authz


@pytest.fixture
def rate_limiter() -> RateLimiter:
    """Create a rate limiter."""
    return RateLimiter()


@pytest.fixture
def audit_logger() -> AuditLogger:
    """Create an audit logger."""
    return AuditLogger()


@pytest.fixture
def secured_app(
    auth_provider: APIKeyAuthProvider,
    authorization: PipelineAuthorization,
    rate_limiter: RateLimiter,
    audit_logger: AuditLogger,
) -> FastAPI:
    """Create a FastAPI app with SecurityMiddleware applied."""
    app = FastAPI()

    @app.get("/api/pipelines")
    async def list_pipelines() -> dict[str, list[str]]:
        """List all accessible pipelines."""
        return {"pipelines": ["test-pipeline", "allowed-pipeline"]}

    @app.get("/api/pipelines/{pipeline_id}")
    async def get_pipeline(pipeline_id: str) -> dict[str, str]:
        """Get pipeline info."""
        return {"pipeline_id": pipeline_id}

    @app.post("/api/pipelines/{pipeline_id}/execute")
    async def execute_pipeline(
        pipeline_id: str, body: dict[str, Any] | None = None
    ) -> dict[str, str]:
        """Execute a pipeline."""
        return {"status": "executed", "pipeline_id": pipeline_id}

    app.add_middleware(
        SecurityMiddleware,
        auth_provider=auth_provider,
        authorization=authorization,
        rate_limiter=rate_limiter,
        audit_logger=audit_logger,
    )
    return app


@pytest.fixture
def client(secured_app: FastAPI) -> TestClient:
    """Create a test client."""
    return TestClient(secured_app)


# -- Unauthenticated -----------------------------------------------------------


def test_unauthenticated_access_is_denied(client: TestClient) -> None:
    """Unauthenticated requests are denied with 401."""
    response = client.get("/api/pipelines")
    assert response.status_code == 401
    assert "detail" in response.json()


def test_missing_api_key_detail(client: TestClient) -> None:
    """Error detail indicates missing API key."""
    response = client.get("/api/pipelines")
    assert response.status_code == 401
    assert "Missing API key" in response.json()["detail"]


# -- Invalid key ---------------------------------------------------------------


def test_invalid_api_key_is_denied(client: TestClient) -> None:
    """Invalid API key requests are denied with 403."""
    headers = {"X-API-Key": "invalid-key"}
    response = client.get("/api/pipelines", headers=headers)
    assert response.status_code == 403
    assert "detail" in response.json()


def test_invalid_api_key_detail(client: TestClient) -> None:
    """Error detail indicates invalid API key."""
    headers = {"X-API-Key": "wrong-key"}
    response = client.get("/api/pipelines", headers=headers)
    assert response.status_code == 403
    assert "Invalid API key" in response.json()["detail"]


# -- Authorization -------------------------------------------------------------


def test_key_without_whitelist_is_forbidden(client: TestClient) -> None:
    """Valid key with empty pipeline whitelist gets 403."""
    headers = {"X-API-Key": "no-pipeline-key"}
    response = client.get("/api/pipelines", headers=headers)
    assert response.status_code == 403
    assert "Forbidden" in response.json()["detail"]


def test_readonly_key_can_read(client: TestClient) -> None:
    """Readonly key can read list pipelines."""
    headers = {"X-API-Key": "readonly-key"}
    response = client.get("/api/pipelines", headers=headers)
    assert response.status_code == 200


def test_user_key_can_access_whitelisted_pipeline(client: TestClient) -> None:
    """User key can access its whitelisted pipelines."""
    headers = {"X-API-Key": "user-key"}
    assert (
        client.get("/api/pipelines/allowed-pipeline", headers=headers).status_code
        == 200
    )
    assert (
        client.get("/api/pipelines/test-pipeline", headers=headers).status_code == 200
    )


def test_user_key_cannot_access_non_whitelisted_pipeline(client: TestClient) -> None:
    """User key cannot access a non-whitelisted pipeline."""
    headers = {"X-API-Key": "user-key"}
    response = client.get("/api/pipelines/forbidden-pipeline", headers=headers)
    assert response.status_code == 403


# -- Admin ---------------------------------------------------------------------


def test_admin_key_has_full_access(client: TestClient) -> None:
    """Admin key can access any pipeline."""
    headers = {"X-API-Key": "admin-key"}
    response = client.get("/api/pipelines/any-pipeline", headers=headers)
    assert response.status_code == 200


def test_admin_key_can_execute(client: TestClient) -> None:
    """Admin key can execute a pipeline."""
    headers = {"X-API-Key": "admin-key"}
    response = client.post(
        "/api/pipelines/test-pipeline/execute",
        json={"input": 5},
        headers=headers,
    )
    assert response.status_code == 200


# -- OPTIONS / CORS ------------------------------------------------------------


def test_options_request_does_not_crash(client: TestClient) -> None:
    """OPTIONS requests should not crash."""
    response = client.options("/api/pipelines")
    assert response.status_code < 500
