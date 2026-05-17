"""
Middleware d'enforcement HTTPS pour Taskiq-Flow.

Ce module fournit le middleware :class:`HTTPSEnforcementMiddleware` qui
rejette les requêtes HTTP non chiffrées lorsque ``config.require_https``
est défini à ``True``.

Respecte l'en-tête ``X-Forwarded-Proto`` pour les déploiements derrière
un reverse proxy ou un load-balancer qui termine TLS, afin d'éviter les
faux positifs dans les environnements conteneurisés ou cloud.

Auteur: SoniqueBay Team
Version: 1.2.0
"""

import logging
from typing import Any, cast

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware

logger = logging.getLogger(__name__)


class HTTPSEnforcementMiddleware(BaseHTTPMiddleware):
    """
    Middleware qui enforce la connexion HTTPS.

    Bloque les requêtes HTTP si require_https est True.
    Respecte l'en-tête X-Forwarded-Proto pour les déploiements derrière
    un reverse proxy/load balancer qui termine TLS.

    Attributes:
        require_https: Si True, rejette les requêtes non-HTTPS

    """

    def __init__(self, app: Any, require_https: bool = True) -> None:
        """
        Initialize HTTPS enforcement middleware.

        Args:
            app: ASGI application
            require_https: Whether to enforce HTTPS (default True)

        """
        super().__init__(app)
        self.require_https = require_https

    async def dispatch(self, request: Request, call_next: Any) -> Response:
        """
        Process request and enforce HTTPS if configured.

        Args:
            request: Incoming request
            call_next: Next middleware/application

        Returns:
            Response object

        """
        if self.require_https:
            # Determine if request is secure
            is_secure = request.url.scheme == "https"

            # Check X-Forwarded-Proto header (common when behind proxy)
            if not is_secure:
                forwarded_proto = request.headers.get("X-Forwarded-Proto", "")
                # X-Forwarded-Proto can be comma-separated; first is client proto
                first_proto = forwarded_proto.split(",")[0].strip().lower()
                is_secure = first_proto == "https"

            if not is_secure:
                logger.warning(
                    "Insecure request rejected",
                    extra={
                        "path": request.url.path,
                        "client": request.client.host if request.client else None,
                    },
                )
                return Response(
                    status_code=403,
                    content="HTTPS required",
                )

        return cast(Response, await call_next(request))


__all__ = ["HTTPSEnforcementMiddleware"]
