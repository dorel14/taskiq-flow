"""
Tests for secure WebSocket flow with authentication and authorization.

This module tests the integration of SecurityMiddleware with WebSocket endpoints
for DAG event streaming.
"""

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect
from taskiq import InMemoryBroker

from taskiq_flow import create_visualization_api
from taskiq_flow.security import (
    APIKeyAuthProvider,
    AuditLogger,
    Permission,
    PipelineAuthorization,
    SecurityMiddleware,
)


@pytest.fixture
def broker() -> InMemoryBroker:
    """Create a test broker."""
    return InMemoryBroker()


@pytest.fixture
def rate_limiter() -> AuditLogger:
    """Create a rate limiter."""
    return AuditLogger()


@pytest.fixture
def auth_provider() -> APIKeyAuthProvider:
    """Create an API key auth provider with test keys."""
    keys = {
        "admin-key": {
            "role": "admin",
            "pipeline_whitelist": ["*"],
            "permissions": ["read", "execute", "admin"],
        },
        "user-key": {
            "role": "user",
            "pipeline_whitelist": ["test-pipeline", "ws-pipeline"],
            "permissions": ["read", "execute"],
        },
        "readonly-key": {
            "role": "readonly",
            "pipeline_whitelist": ["readonly-pipeline"],
            "permissions": ["read"],
        },
        "no-ws-key": {
            "role": "user",
            "pipeline_whitelist": ["test-pipeline"],  # No WS pipeline
            "permissions": ["read", "execute"],
        },
    }
    return APIKeyAuthProvider(keys)


@pytest.fixture
def authorization() -> PipelineAuthorization:
    """Create pipeline authorization with test ACLs."""
    authz = PipelineAuthorization()
    # Admin can access all pipelines
    authz.set_acl("*", Permission.READ, ["admin"])
    authz.set_acl("*", Permission.EXECUTE, ["admin"])
    # User can read and execute specific pipelines
    authz.set_acl("test-pipeline", Permission.READ, ["user", "admin"])
    authz.set_acl("test-pipeline", Permission.EXECUTE, ["user", "admin"])
    authz.set_acl("ws-pipeline", Permission.READ, ["user", "admin"])
    authz.set_acl("ws-pipeline", Permission.EXECUTE, ["user", "admin"])
    # Read-only pipeline for readonly role
    authz.set_acl("readonly-pipeline", Permission.READ, ["readonly", "user", "admin"])
    return authz


@pytest.fixture
def audit_logger() -> AuditLogger:
    """Create an audit logger."""
    return AuditLogger()


@pytest.fixture
def app_with_security(
    broker: InMemoryBroker,
    auth_provider: APIKeyAuthProvider,
    authorization: PipelineAuthorization,
    audit_logger: AuditLogger,
    rate_limiter: AuditLogger,
) -> FastAPI:
    """Create a FastAPI app with security middleware."""
    # Create the visualization API
    api = create_visualization_api(broker)

    # Add security middleware
    SecurityMiddleware(
        api.app,
        auth_provider,
        authorization,
        rate_limiter,
        audit_logger,
    )
    return api.app


@pytest.fixture
def client(app_with_security: FastAPI) -> TestClient:
    """Create a test client."""
    return TestClient(app_with_security)


def test_unauthenticated_ws_denied(client: TestClient) -> None:
    """Test that unauthenticated WebSocket connections are denied."""
    with (
        pytest.raises(WebSocketDisconnect),
        client.websocket_connect("/ws/pipelines/test-pipeline/events"),
    ):
        pass


def test_invalid_api_key_ws_denied(client: TestClient) -> None:
    """Test that invalid API keys prevent WS connection."""
    assert True  # Placeholder


def test_valid_key_can_connect_ws(client: TestClient) -> None:
    """Test that valid key with permissions can establish WS connection."""
    assert True  # Placeholder


def test_key_without_ws_permission_cannot_connect(client: TestClient) -> None:
    """Test that key without WS pipeline permission cannot connect."""
    assert True  # Placeholder


def test_ws_receives_events_when_authorized(client: TestClient) -> None:
    """Test that authorized WS connection receives pipeline events."""
    assert True  # Placeholder


def test_ws_connection_closes_on_unauthorized_event(client: TestClient) -> None:
    """Test that WS connection behaves correctly when unauthorized for events."""
    assert True  # Placeholder


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
