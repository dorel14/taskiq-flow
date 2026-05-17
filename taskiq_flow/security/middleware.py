"""
Middleware de sécurité pour Taskiq-Flow.

Ce module fournit le middleware FastAPI :class:`SecurityMiddleware` qui
applique les contrôles de sécurité sur chaque requête entrante :

1. **Authentification** via le fournisseur configuré (API key ou JWT).
2. **Audit** : journalisation de la requête après passage au handler.

Le rate limiting est géré séparément par ``SlowAPIMiddleware`` (slowapi),
ajouté par :meth:`PipelineVisualizationAPI._setup_security`.

Auteur: SoniqueBay Team
Version: 1.2.0
"""

import logging
import time
from collections.abc import Awaitable, Callable
from typing import Any

from fastapi import HTTPException, Request, Response

try:
    from fastapi.middleware.base import BaseHTTPMiddleware
except ImportError:
    # Pour les anciennes versions de FastAPI
    from starlette.middleware.base import BaseHTTPMiddleware

logger = logging.getLogger(__name__)


class SecurityMiddleware(BaseHTTPMiddleware):  # type: ignore[misc]
    """Middleware de sécurité global."""

    def __init__(
        self,
        app: Any,
        auth_provider: Any,
        authorization: Any,
        rate_limiter: Any,
        audit_logger: Any,
    ) -> None:
        """
        Initialise le middleware.

        Args:
            app: Application FastAPI
            auth_provider: Fournisseur d'authentification
            authorization: Gestionnaire d'autorisation
            rate_limiter: Limiteur de débit
            audit_logger: Enregistreur d'audit

        """
        super().__init__(app)
        self.auth_provider = auth_provider
        self.authorization = authorization
        self.rate_limiter = rate_limiter
        self.audit_logger = audit_logger

    async def dispatch(
        self, request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        """
        Traite la requête avec les contrôles de sécurité.

        Args:
            request: Requête entrante
            call_next: Fonction pour appeler le prochain middleware

        Returns:
            Réponse

        """
        # Authentifier l'utilisateur via le provider
        user_context = None
        try:
            user_context = await self.auth_provider.verify(request)
            if user_context:
                request.state.user = user_context
        except HTTPException:
            raise
        except Exception as e:
            logger.warning("Authentication failed: %s", e)

        # Rate limiting is handled by SlowAPIMiddleware (added to FastAPI app)
        # This SecurityMiddleware handles auth + audit only

        start_time = time.time()
        response = await call_next(request)
        duration = time.time() - start_time

        # Audit logging (if authenticated)
        if user_context:
            # pipeline_id from route dependencies (verify_pipeline_access)
            # stores result in request.state.pipeline_id if available.
            # Otherwise, extract from path (fallback for non-dependent endpoints).
            pipeline_id = getattr(request.state, "pipeline_id", None)
            if pipeline_id is None:
                # Fallback: parse URL (ex: /pipelines/{pipeline_id}/...)
                # Note: routing not yet performed in middleware,
                # so we manually parse known segments.
                parts = request.url.path.strip("/").split("/")
                if len(parts) >= 2 and parts[0] == "pipelines":
                    pipeline_id = parts[1]
            action = request.method + "_" + request.url.path.replace("/", "_")
            await self.audit_logger.log_access(
                user_context,
                action,
                pipeline_id,
                response.status_code < 400,
                request.client.host if request.client else None,
                {"duration": duration, "status_code": response.status_code},
            )

        return response


__all__ = ["SecurityMiddleware"]
