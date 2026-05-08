"""Limitation de débit (Rate Limiting) pour Taskiq-Flow.

Ce module fournit une limitation de débit pour les endpoints API,
utilisant slowapi pour la gestion des limites.

Auteur: SoniqueBay Team
Version: 0.4.5
"""

from slowapi import Limiter
from slowapi.util import get_remote_address


class RateLimiter:
    """Gestionnaire de limitation de débit."""

    def __init__(self, default_limits: dict[str, str] | None = None) -> None:
        """
        Initialise le limiteur.

        Args:
            default_limits: Limites par défaut par endpoint
        """
        self.limiter = Limiter(key_func=get_remote_address)
        self.default_limits = default_limits or {
            "list_pipelines": "60/minute",
            "get_dag": "120/minute",
            "get_critical_path": "120/minute",
            "get_parallel_groups": "120/minute",
            "execute_pipeline": "10/minute",
            "get_status": "30/minute",
            "websocket_connect": "5/minute",
        }

    def get_limit(self, endpoint: str) -> str:
        """
        Obtient la limite pour un endpoint.

        Args:
            endpoint: Nom de l'endpoint

        Returns:
            Chaîne de limite (ex: "60/minute")
        """
        return self.default_limits.get(endpoint, "100/minute")

    def get_limiter(self) -> Limiter:
        """
        Obtient l'instance Limiter.

        Returns:
            Instance Limiter
        """
        return self.limiter

    def add_limit(self, endpoint: str, limit: str) -> None:
        """
        Ajoute ou met à jour une limite.

        Args:
            endpoint: Nom de l'endpoint
            limit: Limite (ex: "60/minute")
        """
        self.default_limits[endpoint] = limit

    def remove_limit(self, endpoint: str) -> None:
        """
        Supprime une limite.

        Args:
            endpoint: Nom de l'endpoint
        """
        self.default_limits.pop(endpoint, None)

    def get_all_limits(self) -> dict[str, str]:
        """
        Obtient toutes les limites.

        Returns:
            Dictionnaire des limites
        """
        return self.default_limits.copy()


__all__ = ["RateLimiter"]
