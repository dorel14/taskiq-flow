"""Configuration pour Taskiq-Flow.

Ce module fournit une configuration centralisée pour la sécurité,
les métriques et autres paramètres.

Auteur: SoniqueBay Team
Version: 1.0.2
"""

from typing import Any

from pydantic_settings import BaseSettings, SettingsConfigDict


class TaskiqFlowConfig(BaseSettings):
    """Configuration globale de Taskiq-Flow."""

    model_config = SettingsConfigDict(
        env_prefix="TASKIQ_FLOW_",
        case_sensitive=False,
        extra="ignore",
    )

    # Sécurité
    security_enabled: bool = True
    auth_provider: str = "api_key"
    api_keys: dict[str, dict[str, Any]] = {}
    jwt_secret: str | None = None
    require_https: bool = True

    # Autorisation
    pipeline_acls: dict[str, dict[str, list[str]]] = {}

    # Limitation de débit
    rate_limit_enabled: bool = True
    rate_limit_default: str = "100/minute"

    # Observabilité
    metrics_enabled: bool = True
    metrics_path: str = "/metrics"
    log_level: str = "INFO"

    # WebSocket
    websocket_require_auth: bool = True
    websocket_max_connections: int = 1000
    websocket_ssl_cert: str | None = None
    websocket_ssl_key: str | None = None


__all__ = ["TaskiqFlowConfig"]
