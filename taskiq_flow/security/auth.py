"""
Authentication providers for Taskiq-Flow.

This module provides authentication providers to secure the API
and WebSocket connections, including API key and JWT authentication.

Author: SoniqueBay Team
Version: 1.2.0
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

import jwt
from fastapi import HTTPException, Request
from fastapi.security import APIKeyHeader, HTTPAuthorizationCredentials, HTTPBearer

logger = logging.getLogger(__name__)


class AuthProvider:
    """Base class for authentication providers."""

    async def verify(self, request: Request) -> dict[str, Any] | None:
        """
        Verify authentication credentials.

        Args:
            request: FastAPI request object

        Returns:
            User context dictionary or ``None`` if not authenticated

        """
        raise NotImplementedError


class APIKeyAuthProvider(AuthProvider):
    """API key authentication provider."""

    def __init__(self, keys: dict[str, dict[str, Any]]) -> None:
        """
        Initialise the provider with a key dictionary.

        Args:
            keys: Dictionary mapping each API key to a metadata dict
                containing at least a ``role`` field.

        """
        self.keys = keys

    async def verify(self, request: Request) -> dict[str, Any] | None:
        """
        Verify the API key from request headers.

        Args:
            request: FastAPI request object

        Returns:
            User context dictionary

        Raises:
            HTTPException: ``401`` if the key is missing,
                           ``403`` if the key is invalid.

        """
        api_key = request.headers.get("X-API-Key")
        if not api_key:
            raise HTTPException(status_code=401, detail="Missing API key")
        if api_key not in self.keys:
            raise HTTPException(status_code=403, detail="Invalid API key")
        return {"key": api_key, **self.keys[api_key], "type": "api_key"}


class JWTAuthProvider(AuthProvider):
    """JWT authentication provider."""

    def __init__(self, secret: str, algorithm: str = "HS256") -> None:
        """
        Initialise the JWT provider.

        Args:
            secret: Secret used to sign tokens
            algorithm: Signing algorithm

        """
        self.secret = secret
        self.algorithm = algorithm

    async def verify(self, request: Request) -> dict[str, Any] | None:
        """
        Verify the JWT token from the Authorization header.

        Args:
            request: FastAPI request object

        Returns:
            User context dictionary

        Raises:
            HTTPException: ``401`` if the token is missing, expired, or invalid.

        """
        auth_header = request.headers.get("Authorization", "")
        if not auth_header.startswith("Bearer "):
            raise HTTPException(status_code=401, detail="Token missing")

        token = auth_header.replace("Bearer ", "", 1)
        try:
            payload = jwt.decode(token, self.secret, algorithms=[self.algorithm])
            return {**payload, "type": "jwt"}
        except jwt.ExpiredSignatureError:
            raise HTTPException(status_code=401, detail="Token expired") from None
        except jwt.PyJWTError:
            raise HTTPException(status_code=401, detail="Invalid token") from None

    def create_token(
        self,
        subject: str,
        roles: list[str],
        expires_delta: timedelta | None = None,
    ) -> str:
        """
        Create a signed JWT token.

        Args:
            subject: Token subject (``sub`` claim)
            roles: List of user roles
            expires_delta: Token lifetime; defaults to 24 h

        Returns:
            Signed JWT string

        """
        to_encode: dict[str, Any] = {
            "sub": subject,
            "roles": roles,
            "iat": datetime.now(timezone.utc),
        }
        if expires_delta:
            expire = datetime.now(timezone.utc) + expires_delta
        else:
            expire = datetime.now(timezone.utc) + timedelta(hours=24)
        to_encode["exp"] = expire
        return jwt.encode(to_encode, self.secret, algorithm=self.algorithm)


# FastAPI dependency descriptors. ``auto_error=False`` instructs FastAPI to
# return ``None`` when the header or token is absent.
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)
bearer_auth = HTTPBearer(auto_error=False)


async def get_api_key_user(
    api_key: str | None,
) -> dict[str, Any] | None:
    """
    Retrieve the user context from the API key header.

    This dependency is intended for use **after** :class:`SecurityMiddleware`
    has already authenticated the user and stored the context in
    ``request.state.user``.

    Args:
        api_key: API key value resolved from the ``X-API-Key`` header by
            :data:`api_key_header`; ``None`` when the header is absent.

    """
    if not api_key:
        return None
    return {"key": api_key, "type": "api_key"}


async def get_jwt_user(
    token: HTTPAuthorizationCredentials | None,
) -> dict[str, Any] | None:
    """
    Retrieve the user context from the JWT Bearer token.

    This dependency is intended for use **after** :class:`SecurityMiddleware`
    has already authenticated the user and stored the context in
    ``request.state.user``.

    Args:
        token: JWT credentials provided by :data:`bearer_auth`.

    """
    if not token:
        return None
    return {"token": token.credentials, "type": "jwt"}


def create_auth_provider(config: Any) -> AuthProvider:
    """
    Build and return an :class:`AuthProvider` based on configuration.

    Args:
        config: Configuration object with ``auth_provider`` attribute.

    """
    provider_type = getattr(config, "auth_provider", "api_key").lower()
    if provider_type == "api_key":
        api_keys = getattr(config, "api_keys", None) or {}
        if not api_keys:
            logger.warning(
                "No API keys configured; API-key auth will reject all requests"
            )
        return APIKeyAuthProvider(api_keys)
    if provider_type == "jwt":
        jwt_secret = getattr(config, "jwt_secret", None)
        if not jwt_secret:
            raise ValueError("JWT secret is required for JWT authentication")
        return JWTAuthProvider(jwt_secret)
    raise ValueError(f"Unknown authentication provider type: {provider_type}")


__all__ = [
    "APIKeyAuthProvider",
    "AuthProvider",
    "JWTAuthProvider",
    "api_key_header",
    "bearer_auth",
    "create_auth_provider",
    "get_api_key_user",
    "get_jwt_user",
]
