"""Authentification pour Taskiq-Flow.

Ce module fournit des fournisseurs d'authentification pour sécuriser
l'API et les connexions WebSocket.

Auteur: SoniqueBay Team
Version: 1.0.2
"""

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import jwt
from fastapi import Depends, HTTPException
from fastapi.security import (
    APIKeyHeader,
    HTTPAuthorizationCredentials,
    HTTPBearer,
)
from starlette.requests import Request

from taskiq_flow.config import TaskiqFlowConfig

logger = logging.getLogger(__name__)


class AuthProvider(ABC):
    """Fournisseur d'authentification abstrait."""

    @abstractmethod
    async def verify(self, request: Request) -> dict[str, Any] | None:
        """
        Vérifie la requête et retourne le contexte utilisateur.

        Args:
            request: Requête FastAPI

        Returns:
            Contexte utilisateur ou None
        """


class APIKeyAuthProvider(AuthProvider):
    """Fournisseur d'authentification par clé API."""

    def __init__(self, keys: dict[str, dict[str, Any]]) -> None:
        """
        Initialise le fournisseur.

        Args:
            keys: Dictionnaire des clés API
                 {clé: {role, pipeline_whitelist, permissions}}
        """
        self.keys = keys

    async def verify(self, request: Request) -> dict[str, Any] | None:
        """
        Vérifie la clé API dans les en-têtes.

        Args:
            request: Requête FastAPI

        Returns:
            Contexte utilisateur

        Raises:
            HTTPException: Si la clé est invalide
        """
        api_key = request.headers.get("X-API-Key")
        if not api_key or api_key not in self.keys:
            raise HTTPException(403, "Clé API invalide")
        return {"key": api_key, **self.keys[api_key], "type": "api_key"}


class JWTAuthProvider(AuthProvider):
    """Fournisseur d'authentification JWT."""

    def __init__(self, secret: str, algorithm: str = "HS256") -> None:
        """
        Initialise le fournisseur JWT.

        Args:
            secret: Secret pour signer les tokens
            algorithm: Algorithme de chiffrement
        """
        self.secret = secret
        self.algorithm = algorithm

    async def verify(self, request: Request) -> dict[str, Any] | None:
        """
        Vérifie le token JWT dans les en-têtes.

        Args:
            request: Requête FastAPI

        Returns:
            Contexte utilisateur

        Raises:
            HTTPException: Si le token est invalide ou expiré
        """
        auth_header = request.headers.get("Authorization", "")
        if not auth_header.startswith("Bearer "):
            raise HTTPException(401, "Token manquant")

        token = auth_header.replace("Bearer ", "")
        try:
            payload = jwt.decode(token, self.secret, algorithms=[self.algorithm])
            return {**payload, "type": "jwt"}
        except jwt.ExpiredSignatureError:
            raise HTTPException(401, "Token expiré") from None
        except jwt.PyJWTError:
            raise HTTPException(401, "Token invalide") from None

    def create_token(
        self,
        subject: str,
        roles: list[str],
        expires_delta: timedelta | None = None,
    ) -> str:
        """
        Crée un token JWT.

        Args:
            subject: Sujet du token
            roles: Rôles de l'utilisateur
            expires_delta: Durée de validité

        Returns:
            Token JWT
        """
        to_encode = {"sub": subject, "roles": roles, "iat": datetime.now(timezone.utc)}
        if expires_delta:
            expire = datetime.now(timezone.utc) + expires_delta
        else:
            expire = datetime.now(timezone.utc) + timedelta(hours=24)
        to_encode.update({"exp": expire})
        return jwt.encode(to_encode, self.secret, algorithm=self.algorithm)


# Dépendances FastAPI
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)
bearer_auth = HTTPBearer(auto_error=False)


async def get_api_key_user(
    api_key: str = Depends(api_key_header),
    auth_provider: AuthProvider | None = None,
) -> dict[str, Any]:
    """
    Dépendance FastAPI pour l'authentification par clé API.

    Args:
        api_key: Clé API
        auth_provider: Fournisseur d'authentification

    Returns:
        Contexte utilisateur
    """
    if not auth_provider:
        raise HTTPException(500, "AuthProvider non configuré")

    # Simuler une requête avec l'en-tête
    @dataclass
    class MockRequest:
        """Mock request for testing auth providers."""

        headers: dict[str, str]

    request = MockRequest({"X-API-Key": api_key})
    user_context = await auth_provider.verify(request)  # type: ignore[arg-type]
    if not user_context:
        raise HTTPException(403, "Accès refusé")
    return user_context


async def get_jwt_user(
    credentials: HTTPAuthorizationCredentials | None = None,
    auth_provider: AuthProvider | None = None,
) -> dict[str, Any]:
    """
    Dépendance FastAPI pour l'authentification JWT.

    Args:
        credentials: Identifiants
        auth_provider: Fournisseur d'authentification

    Returns:
        Contexte utilisateur
    """
    if credentials is None:
        raise HTTPException(401, "Missing credentials")
    if not auth_provider:
        raise HTTPException(500, "AuthProvider non configuré")

    # Simuler une requête avec l'en-tête
    @dataclass
    class MockRequest:
        """Mock request for testing auth providers."""

        headers: dict[str, str]

    request = MockRequest({"Authorization": f"Bearer {credentials.credentials}"})
    user_context = await auth_provider.verify(request)  # type: ignore[arg-type]
    if not user_context:
        raise HTTPException(403, "Accès refusé")
    return user_context


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


def create_auth_provider(config: TaskiqFlowConfig) -> AuthProvider:
    """Factory to create an AuthProvider based on configuration.

    Args:
        config: TaskiqFlowConfig instance

    Returns:
        Configured AuthProvider instance

    Raises:
        ValueError: If auth_provider type is unknown
    """
    provider_type = config.auth_provider.lower()
    if provider_type == "api_key":
        if not config.api_keys:
            logger.warning(
                "No API keys configured; API key auth will reject all requests"
            )
        return APIKeyAuthProvider(keys=config.api_keys)
    if provider_type == "jwt":
        if not config.jwt_secret:
            raise ValueError("jwt_secret must be set for JWT auth")
        return JWTAuthProvider(secret=config.jwt_secret)
    raise ValueError(f"Unknown auth provider: {provider_type}")
