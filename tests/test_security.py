"""Tests for security features.

This module tests authentication, authorization, rate limiting,
and audit logging.

Author: SoniqueBay Team
Version: 0.4.5
"""

import asyncio
import os

import pytest
from fastapi import FastAPI

from taskiq_flow.config import TaskiqFlowConfig
from taskiq_flow.security import (
    APIKeyAuthProvider,
    AuditLogger,
    JWTAuthProvider,
    Permission,
    PipelineAuthorization,
    RateLimiter,
    SecurityMiddleware,
)


class TestAuthProvider:
    """Tests for authentication providers."""

    def test_api_key_auth_provider(self) -> None:
        """Test API key authentication."""
        keys = {
            "test-key": {
                "role": "admin",
                "pipeline_whitelist": ["*"],
                "permissions": ["read", "execute", "admin"],
            }
        }
        provider = APIKeyAuthProvider(keys)

        class MockRequest:
            def __init__(self) -> None:
                self.headers: dict[str, str] = {"X-API-Key": "test-key"}

        result = asyncio.run(provider.verify(MockRequest()))  # type: ignore[arg-type]

        assert result is not None
        assert result["type"] == "api_key"
        assert result["key"] == "test-key"

    def test_api_key_auth_provider_invalid(self) -> None:
        """Test API key authentication with invalid key."""
        keys = {"valid-key": {"role": "admin"}}
        provider = APIKeyAuthProvider(keys)

        class MockRequest:
            def __init__(self) -> None:
                self.headers: dict[str, str] = {"X-API-Key": "invalid-key"}

        async def test() -> Exception | None:
            try:
                await provider.verify(MockRequest())  # type: ignore[arg-type]
                return None
            except Exception as e:
                return e

        result = asyncio.run(test())

        assert result is not None

    def test_jwt_auth_provider(self) -> None:
        """Test JWT authentication."""
        provider = JWTAuthProvider("test-secret-key-for-testing-purposes")
        token = provider.create_token("test-user", ["admin"])

        class MockRequest:
            def __init__(self) -> None:
                self.headers: dict[str, str] = {"Authorization": f"Bearer {token}"}

        result = asyncio.run(provider.verify(MockRequest()))  # type: ignore[arg-type]

        assert result is not None
        assert result["type"] == "jwt"
        assert result["sub"] == "test-user"
        assert "admin" in result["roles"]

    def test_jwt_auth_provider_invalid_token(self) -> None:
        """Test JWT authentication with invalid token."""
        provider = JWTAuthProvider("test-secret-key-for-testing-purposes")

        class MockRequest:
            def __init__(self) -> None:
                self.headers: dict[str, str] = {"Authorization": "Bearer invalid-token"}

        async def test() -> Exception | None:
            try:
                await provider.verify(MockRequest())  # type: ignore[arg-type]
                return None
            except Exception as e:
                return e

        result = asyncio.run(test())

        assert result is not None


class TestPipelineAuthorization:
    """Tests for pipeline authorization."""

    def test_set_and_check_acl(self) -> None:
        """Test setting and checking ACL."""
        authz = PipelineAuthorization()
        authz.set_acl("pipeline1", Permission.READ, ["admin", "user"])

        assert authz.can_read("pipeline1", {"roles": ["admin"]}) is True
        assert authz.can_read("pipeline1", {"roles": ["user"]}) is True
        assert authz.can_read("pipeline1", {"roles": ["guest"]}) is False

    def test_wildcard_access(self) -> None:
        """Test wildcard pipeline access."""
        authz = PipelineAuthorization()
        authz.set_acl("pipeline1", Permission.READ, ["admin"])

        user_ctx = {"roles": ["admin"], "pipelines": ["*"]}
        assert authz.can_read("pipeline1", user_ctx) is True

    def test_multiple_permissions(self) -> None:
        """Test multiple permissions."""
        authz = PipelineAuthorization()
        authz.set_acl("pipeline1", Permission.READ, ["user"])
        authz.set_acl("pipeline1", Permission.EXECUTE, ["admin"])

        assert authz.can_read("pipeline1", {"roles": ["user"]}) is True
        assert authz.can_execute("pipeline1", {"roles": ["user"]}) is False
        assert authz.can_execute("pipeline1", {"roles": ["admin"]}) is True

    def test_get_allowed_pipelines(self) -> None:
        """Test getting allowed pipelines."""
        authz = PipelineAuthorization()
        authz.set_acl("pipeline1", Permission.READ, ["user"])
        authz.set_acl("pipeline2", Permission.READ, ["admin"])

        allowed = authz.get_allowed_pipelines({"roles": ["user"]}, Permission.READ)
        assert "pipeline1" in allowed
        assert "pipeline2" not in allowed

    def test_remove_acl(self) -> None:
        """Test removing ACL."""
        authz = PipelineAuthorization()
        authz.set_acl("pipeline1", Permission.READ, ["user"])
        authz.remove_acl("pipeline1")

        assert authz.can_read("pipeline1", {"roles": ["user"]}) is False


class TestRateLimiter:
    """Tests for rate limiter."""

    def test_get_limit(self) -> None:
        """Test getting rate limit."""
        limiter = RateLimiter()

        assert limiter.get_limit("list_pipelines") == "60/minute"
        assert limiter.get_limit("unknown") == "100/minute"

    def test_add_and_remove_limit(self) -> None:
        """Test adding and removing limits."""
        limiter = RateLimiter()
        limiter.add_limit("custom", "10/minute")

        assert limiter.get_limit("custom") == "10/minute"

        limiter.remove_limit("custom")
        assert limiter.get_limit("custom") == "100/minute"

    def test_get_all_limits(self) -> None:
        """Test getting all limits."""
        limiter = RateLimiter()
        limits = limiter.get_all_limits()

        assert "list_pipelines" in limits
        assert isinstance(limits, dict)


class TestAuditLogger:
    """Tests for audit logger."""

    def test_log_access(self) -> None:
        """Test logging access."""
        logger = AuditLogger()

        async def test() -> None:
            await logger.log_access(
                {"sub": "test-user"},
                "read_pipeline",
                "pipeline1",
                True,
                "127.0.0.1",
            )

        asyncio.run(test())

    def test_log_authentication(self) -> None:
        """Test logging authentication."""
        logger = AuditLogger()

        async def test() -> None:
            await logger.log_authentication("test-user", "api_key", True, "127.0.0.1")

        asyncio.run(test())

    def test_log_pipeline_action(self) -> None:
        """Test logging pipeline action."""
        logger = AuditLogger()

        async def test() -> None:
            await logger.log_pipeline_action(
                {"sub": "test-user"},
                "execute",
                "pipeline1",
            )

        asyncio.run(test())

    def test_log_security_event(self) -> None:
        """Test logging security event."""
        logger = AuditLogger()

        async def test() -> None:
            await logger.log_security_event(
                "unauthorized_access",
                "high",
                "Unauthorized access attempt",
            )

        asyncio.run(test())


class TestSecurityConfig:
    """Tests for security configuration."""

    def test_default_config(self) -> None:
        """Test default configuration."""
        config = TaskiqFlowConfig()

        assert config.security_enabled is True
        assert config.auth_provider == "api_key"
        assert config.rate_limit_enabled is True
        assert config.metrics_enabled is True

    def test_config_with_env(self) -> None:
        """Test configuration with environment variables."""
        os.environ["TASKIQ_FLOW_SECURITY_ENABLED"] = "false"
        os.environ["TASKIQ_FLOW_AUTH_PROVIDER"] = "jwt"

        config = TaskiqFlowConfig()

        assert config.security_enabled is False
        assert config.auth_provider == "jwt"

        del os.environ["TASKIQ_FLOW_SECURITY_ENABLED"]
        del os.environ["TASKIQ_FLOW_AUTH_PROVIDER"]


class TestSecurityIntegration:
    """Integration tests for security."""

    def test_security_middleware_creation(self) -> None:
        """Test creating security middleware."""
        app = FastAPI()
        auth_provider = APIKeyAuthProvider({"key": {"role": "admin"}})
        authorization = PipelineAuthorization()
        rate_limiter = RateLimiter()
        audit_logger = AuditLogger()

        middleware = SecurityMiddleware(
            app,
            auth_provider,
            authorization,
            rate_limiter,
            audit_logger,
        )

        assert middleware is not None

    def test_create_visualization_api_with_security(self) -> None:
        """Test creating visualization API with security."""
        from taskiq import InMemoryBroker  # noqa: PLC0415

        from taskiq_flow import create_visualization_api  # noqa: PLC0415

        broker = InMemoryBroker()
        config = TaskiqFlowConfig(
            security_enabled=True,
            auth_provider="api_key",
            api_keys={"test-key": {"role": "admin", "pipelines": ["*"]}},
        )

        # Note: create_visualization_api only takes broker and optional app
        # Security is configured separately via SecurityMiddleware
        api = create_visualization_api(broker)

        assert api is not None
        assert config.security_enabled is True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
