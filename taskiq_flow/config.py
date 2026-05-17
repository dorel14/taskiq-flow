"""
Configuration pour Taskiq-Flow.

Ce module fournit une configuration centralisée pour la sécurité,
les métriques, le stockage et le cache.

Auteur: SoniqueBay Team
Version: 1.2.0
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

    # Stockage
    storage_enabled: bool = True
    storage_type: str = "auto"  # "auto", "memory", "redis", "sqlite", "sqlalchemy"
    storage_redis_url: str | None = None
    storage_sqlite_url: str = "sqlite+aiosqlite:///taskiq_flow.db"
    storage_sqlalchemy_url: str | None = None
    storage_ttl_seconds: int = 3600
    storage_async_mode: bool = True

    # Cache
    cache_enabled: bool = True
    cache_type: str = "auto"  # "auto", "memory", "redis"
    cache_redis_url: str | None = None
    cache_default_ttl: int = 3600
    cache_lock_timeout: int = 10


__all__ = ["TaskiqFlowConfig"]
