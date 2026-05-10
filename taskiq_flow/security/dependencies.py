"""Dépendances FastAPI pour l'authentification et l'autorisation.

Ce module fournit des dépendances injectables dans les routes
pour l'authentification (via SecurityMiddleware) et l'autorisation
basée sur les ACLs de pipelines.

Auteur: SoniqueBay Team
Version: 1.0.2
"""

from typing import Any

from fastapi import Depends, HTTPException, Request, status

from taskiq_flow.security.auth import AuthProvider
from taskiq_flow.security.authorization import AuthorizationError, PipelineAuthorization


def get_auth_provider(request: Request) -> AuthProvider:
    """Dependency to get the configured auth provider from app state.

    Args:
        request: FastAPI request (provides access to app.state)

    Returns:
        AuthProvider instance from app state

    Raises:
        HTTPException: 500 if auth provider not configured
    """
    provider = getattr(request.app.state, "auth_provider", None)
    if provider is None:
        raise HTTPException(
            status_code=500,
            detail="Authentication provider not configured",
        )
    return provider


def get_authorization(request: Request) -> PipelineAuthorization:
    """Dependency to get the authorization manager from app state.

    Args:
        request: FastAPI request (provides access to app.state)

    Returns:
        PipelineAuthorization instance from app state

    Raises:
        HTTPException: 500 if authorization manager not configured
    """
    authorization = getattr(request.app.state, "authorization", None)
    if authorization is None:
        raise HTTPException(
            status_code=500,
            detail="Authorization manager not configured",
        )
    return authorization


async def get_current_user(request: Request) -> dict[str, Any]:
    """Extract authenticated user from request state (set by SecurityMiddleware).

    This dependency must be used after SecurityMiddleware has run.
    Returns the user_context dict stored in request.state.user.

    Raises:
        HTTPException: 401 if user is not authenticated

    Returns:
        User context dictionary
    """
    user = getattr(request.state, "user", None)
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return user


async def verify_pipeline_access(
    pipeline_id: str,
    request: Request,
    user: dict[str, Any] = Depends(get_current_user),  # noqa: B008
    authorization: PipelineAuthorization = Depends(get_authorization),  # noqa: B008
) -> dict[str, Any]:
    """Verify that the current user can access the requested pipeline.

    This dependency combines authentication (via get_current_user) and
    authorization (pipeline ACL check). Use in DAG routes.

    Args:
        pipeline_id: The pipeline identifier from path parameter
        user: Authenticated user context (injected)
        authorization: Authorization manager (injected)

    Returns:
        The user context if authorized

    Raises:
        HTTPException: 403 if user lacks read access to the pipeline
    """
    try:
        if not authorization.can_read(pipeline_id, user):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Access denied to pipeline '{pipeline_id}'",
            )
        return user
    except AuthorizationError as e:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=str(e),
        ) from e


__all__ = [
    "get_auth_provider",
    "get_authorization",
    "get_current_user",
    "verify_pipeline_access",
]
