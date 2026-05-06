"""Autorisation pour Taskiq-Flow.

Ce module gère les listes de contrôle d'accès (ACL) pour les pipelines,
permettant de définir quels rôles peuvent accéder à quels pipelines.

Auteur: SoniqueBay Team
Version: 0.4.5
"""

from enum import Enum
from typing import Any


class Permission(Enum):
    """Permissions disponibles pour les pipelines."""

    READ = "read"
    EXECUTE = "execute"
    ADMIN = "admin"


class PipelineAuthorization:
    """Gestion des autorisations pour les pipelines."""

    def __init__(self) -> None:
        """Initialise l'autorisation."""
        # pipeline_id -> {permission: [roles]}
        self.acls: dict[str, dict[str, list[str]]] = {}

    def set_acl(
        self, pipeline_id: str, permission: Permission, roles: list[str]
    ) -> None:
        """
        Définit une ACL pour un pipeline.

        Args:
            pipeline_id: Identifiant du pipeline
            permission: Permission à définir
            roles: Rôles autorisés
        """
        if pipeline_id not in self.acls:
            self.acls[pipeline_id] = {}
        self.acls[pipeline_id][permission.value] = roles

    def can(
        self,
        pipeline_id: str,
        permission: Permission,
        user_context: dict[str, Any],
    ) -> bool:
        """
        Vérifie si un utilisateur a une permission.

        Args:
            pipeline_id: Identifiant du pipeline
            permission: Permission à vérifier
            user_context: Contexte utilisateur

        Returns:
            True si l'utilisateur a la permission
        """
        user_roles = user_context.get("roles", [])
        acl = self.acls.get(pipeline_id, {})

        # Accès wildcard pour tous les pipelines
        if "*" in user_context.get("pipelines", []):
            return True

        # Vérifier les rôles spécifiques
        allowed_roles = acl.get(permission.value, [])
        return any(role in allowed_roles for role in user_roles)

    def can_read(self, pipeline_id: str, user_context: dict[str, Any]) -> bool:
        """
        Vérifie si un utilisateur peut lire un pipeline.

        Args:
            pipeline_id: Identifiant du pipeline
            user_context: Contexte utilisateur

        Returns:
            True si l'utilisateur peut lire
        """
        return self.can(pipeline_id, Permission.READ, user_context)

    def can_execute(self, pipeline_id: str, user_context: dict[str, Any]) -> bool:
        """
        Vérifie si un utilisateur peut exécuter un pipeline.

        Args:
            pipeline_id: Identifiant du pipeline
            user_context: Contexte utilisateur

        Returns:
            True si l'utilisateur peut exécuter
        """
        return self.can(pipeline_id, Permission.EXECUTE, user_context)

    def can_admin(self, pipeline_id: str, user_context: dict[str, Any]) -> bool:
        """
        Vérifie si un utilisateur a les droits admin sur un pipeline.

        Args:
            pipeline_id: Identifiant du pipeline
            user_context: Contexte utilisateur

        Returns:
            True si l'utilisateur a les droits admin
        """
        return self.can(pipeline_id, Permission.ADMIN, user_context)

    def get_allowed_pipelines(
        self, user_context: dict[str, Any], permission: Permission
    ) -> list[str]:
        """
        Obtient la liste des pipelines auxquels un utilisateur a accès.

        Args:
            user_context: Contexte utilisateur
            permission: Permission requise

        Returns:
            Liste des identifiants de pipeline
        """
        user_roles = user_context.get("roles", [])
        allowed = []

        for pipeline_id, acl in self.acls.items():
            allowed_roles = acl.get(permission.value, [])
            if any(role in allowed_roles for role in user_roles):
                allowed.append(pipeline_id)

        return allowed

    def remove_acl(self, pipeline_id: str) -> None:
        """
        Supprime toutes les ACL pour un pipeline.

        Args:
            pipeline_id: Identifiant du pipeline
        """
        self.acls.pop(pipeline_id, None)

    def update_acl(self, pipeline_id: str, acls: dict[Permission, list[str]]) -> None:
        """
        Met à jour plusieurs ACL pour un pipeline.

        Args:
            pipeline_id: Identifiant du pipeline
            acls: Dictionnaire des permissions et rôles
        """
        for permission, roles in acls.items():
            self.set_acl(pipeline_id, permission, roles)


__all__ = ["Permission", "PipelineAuthorization"]
