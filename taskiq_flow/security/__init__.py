"""
Sécurité pour Taskiq-Flow.

Ce module fournit des fonctionnalités de sécurité incluant
l'authentification, l'autorisation, le rate limiting, l'enforcement HTTPS
et la journalisation d'audit pour l'API et les connexions WebSocket.

Composants :
    - Authentification par clé API ou JWT
    - Autorisation basée sur les ACLs de pipelines
    - Limitation de débit par endpoint (slowapi)
    - Middleware d'enforcement HTTPS
    - Journalisation d'audit structurée

Auteur: SoniqueBay Team
Version: 1.2.0
"""

from taskiq_flow.config import TaskiqFlowConfig
from taskiq_flow.security.audit import AuditLogger
from taskiq_flow.security.auth import (
    APIKeyAuthProvider,
    AuthProvider,
    JWTAuthProvider,
)
from taskiq_flow.security.authorization import Permission, PipelineAuthorization
from taskiq_flow.security.middleware import SecurityMiddleware
from taskiq_flow.security.rate_limiting import RateLimiter

__all__ = [
    "APIKeyAuthProvider",
    "AuditLogger",
    "AuthProvider",
    "JWTAuthProvider",
    "Permission",
    "PipelineAuthorization",
    "RateLimiter",
    "SecurityMiddleware",
    "TaskiqFlowConfig",
]
