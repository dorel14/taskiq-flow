"""
Journalisation d'audit pour Taskiq-Flow.

Ce module fournit un système de journalisation d'audit structurée pour suivre
les accès et les actions sur les pipelines. Les événements sont enregistrés
avec un horodatage UTC et peuvent être ingérés dans un système SIEM.

Événements journalisés :
    - Accès aux endpoints (succès / échec)
    - Tentatives d'authentification (API key, JWT)
    - Actions sur les pipelines (lecture, exécution)
    - Événements de sécurité (accès non autorisé, modification de config)

Auteur: SoniqueBay Team
Version: 1.2.0
"""

import logging
from datetime import datetime, timezone
from typing import Any


class AuditLogger:
    """Enregistreur d'événements d'audit."""

    def __init__(self, logger_name: str = "taskiq_flow.audit") -> None:
        """
        Initialise l'enregistreur.

        Args:
            logger_name: Nom du logger

        """
        self.logger = logging.getLogger(logger_name)

    async def log_access(
        self,
        user: dict[str, Any],
        action: str,
        pipeline_id: str,
        success: bool,
        ip: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        """
        Enregistre un accès au système.

        Args:
            user: Contexte utilisateur
            action: Action effectuée
            pipeline_id: Identifiant du pipeline
            success: Succès de l'action
            ip: Adresse IP
            details: Détails supplémentaires

        """
        self.logger.info(
            "AUDIT",
            extra={
                "user": user.get("sub") or user.get("key"),
                "action": action,
                "pipeline_id": pipeline_id,
                "success": success,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "ip": ip,
                "details": details or {},
            },
        )

    async def log_authentication(
        self,
        user_id: str,
        method: str,
        success: bool,
        ip: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        """
        Enregistre une tentative d'authentification.

        Args:
            user_id: Identifiant utilisateur
            method: Méthode d'authentification
            success: Succès de l'authentification
            ip: Adresse IP
            details: Détails supplémentaires

        """
        self.logger.info(
            "AUTHENTICATION",
            extra={
                "user_id": user_id,
                "method": method,
                "success": success,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "ip": ip,
                "details": details or {},
            },
        )

    async def log_pipeline_action(
        self,
        user: dict[str, Any],
        action: str,
        pipeline_id: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        """
        Enregistre une action sur un pipeline.

        Args:
            user: Contexte utilisateur
            action: Action effectuée
            pipeline_id: Identifiant du pipeline
            details: Détails supplémentaires

        """
        self.logger.info(
            "PIPELINE_ACTION",
            extra={
                "user": user.get("sub") or user.get("key"),
                "action": action,
                "pipeline_id": pipeline_id,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "details": details or {},
            },
        )

    async def log_security_event(
        self,
        event_type: str,
        severity: str,
        message: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        """
        Enregistre un événement de sécurité.

        Args:
            event_type: Type d'événement
            severity: Sévérité (low, medium, high, critical)
            message: Message
            details: Détails supplémentaires

        """
        self.logger.warning(
            "SECURITY_EVENT: %s",
            message,
            extra={
                "event_type": event_type,
                "severity": severity,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "details": details or {},
            },
        )


__all__ = ["AuditLogger"]
