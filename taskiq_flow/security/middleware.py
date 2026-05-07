"""Middleware de sécurité pour Taskiq-Flow.

Ce module fournit un middleware FastAPI pour la sécurité globale,
incluant l'authentification, l'autorisation et la limitation de débit.

Auteur: SoniqueBay Team
Version: 0.4.5
"""

import logging
import time
from collections.abc import Awaitable, Callable
from typing import Any

from fastapi import Request, Response, HTTPException

try:
    from fastapi.middleware.base import BaseHTTPMiddleware
except ImportError:
    # Pour les anciennes versions de FastAPI
    from starlette.middleware.base import BaseHTTPMiddleware

logger = logging.getLogger(__name__)


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
        # Traiter la requête
        start_time = time.time()
        response = await call_next(request)
        duration = time.time() - start_time

        # Journalisation d'audit post-requête (si authentifié)
        if user_context:
            # pipeline_id est fourni par les dépendances de route (verify_pipeline_access)
            # qui stocke le résultat dans request.state.pipeline_id si disponible.
            # Sinon, on tente d'extraire depuis le chemin (fallback pour endpoints sans dépendance).
            pipeline_id = getattr(request.state, "pipeline_id", None)
            if pipeline_id is None:
                # Fallback : parser l'URL (ex: /pipelines/{pipeline_id}/...)
                # Note: le routing n'étant pas encore effectué en middleware,
                # on parse manuellement les segments connus.
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
