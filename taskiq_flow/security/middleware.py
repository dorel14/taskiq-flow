"""Middleware de sécurité pour Taskiq-Flow.

Ce module fournit un middleware FastAPI pour la sécurité globale,
incluant l'authentification, l'autorisation et la limitation de débit.

Auteur: SoniqueBay Team
Version: 0.4.5
"""

import contextlib
import time
from collections.abc import Awaitable, Callable
from typing import Any

from fastapi import Request, Response

try:
    from fastapi.middleware.base import BaseHTTPMiddleware
except ImportError:
    # Pour les anciennes versions de FastAPI
    from starlette.middleware.base import BaseHTTPMiddleware


class SecurityMiddleware(BaseHTTPMiddleware):
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
        # Limitation de débit
        endpoint = request.url.path
        self.rate_limiter.get_limit(endpoint)
        # TODO: Implémenter la vérification de la limite

        # Authentification
        user_context = None
        with contextlib.suppress(Exception):
            user_context = await self.auth_provider.verify(request)

        # Autorisation (pour les endpoints spécifiques aux pipelines)
        pipeline_id = request.path_params.get("pipeline_id")
        if (
            pipeline_id
            and user_context
            and not self.authorization.can_read(pipeline_id, user_context)
        ):
            await self.audit_logger.log_access(
                user_context,
                "unauthorized_access",
                pipeline_id,
                False,
                request.client.host if request.client else None,
            )
            return Response(status_code=403, content="Accès refusé")

        # Traiter la requête
        start_time = time.time()
        response = await call_next(request)
        duration = time.time() - start_time

        # Journalisation d'audit
        if pipeline_id and user_context:
            action = request.method + "_" + endpoint.replace("/", "_")
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
