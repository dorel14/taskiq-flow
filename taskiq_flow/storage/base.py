"""
Interfaces de base pour les adaptateurs de stockage et de cache.

Définit les contrats abstraits que tous les backends de stockage
et de cache doivent implémenter.

Attributes:
    StorageEntry: Représente une entrée de stockage générique.
    BaseStorageAdapter: Interface abstraite pour les adaptateurs de stockage.
    BaseCacheAdapter: Interface abstraite pour les adaptateurs de cache.

Auteur: SoniqueBay Team
Version: 1.2.0

"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any


class StorageEntry:
    """
    Représente une entrée de stockage générique.

    Attributes:
        key: Clé unique de l'entrée
        value: Valeur stockée
        created_at: Horodatage de création
        expires_at: Horodatage d'expiration (optionnel)
        metadata: Métadonnées supplémentaires

    """

    def __init__(
        self,
        key: str,
        value: Any,
        created_at: datetime | None = None,
        expires_at: datetime | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        self.key = key
        self.value = value
        self.created_at = created_at or datetime.now(timezone.utc)
        self.expires_at = expires_at
        self.metadata = metadata or {}

    def is_expired(self) -> bool:
        """
        Vérifie si l'entrée a expiré.

        Returns:
            True si expiré, False sinon

        """
        if self.expires_at is None:
            return False
        return datetime.now(timezone.utc) > self.expires_at

    def remaining_ttl(self) -> float | None:
        """
        Retourne le temps restant en secondes avant expiration.

        Returns:
            Temps restant en secondes ou None

        """
        if self.expires_at is None:
            return None
        delta = (self.expires_at - datetime.now(timezone.utc)).total_seconds()
        return max(0.0, delta)


class BaseStorageAdapter(ABC):
    """
    Interface abstraite pour les adaptateurs de stockage.

    Tous les backends de stockage (mémoire, Redis, etc.) doivent
    implémenter cette interface pour assurer l'interopérabilité.

    Méthodes obligatoires:
        - get: Récupérer une valeur par clé
        - set: Stocker une valeur avec clé
        - delete: Supprimer une entrée par clé
        - exists: Vérifier l'existence d'une clé
        - keys: Lister les clés correspondant à un motif
        - cleanup: Nettoyer les entrées expirées
    """

    @abstractmethod
    async def get(self, key: str) -> Any | None:
        """
        Récupère une valeur par clé.

        Args:
            key: Clé de l'entrée

        Returns:
            Valeur stockée ou None si absente

        """
        ...

    @abstractmethod
    async def set(
        self,
        key: str,
        value: Any,
        ttl_seconds: int | None = None,
    ) -> None:
        """
        Stocke une valeur avec une clé.

        Args:
            key: Clé de l'entrée
            value: Valeur à stocker
            ttl_seconds: Durée de vie en secondes (optionnel)

        """
        ...

    @abstractmethod
    async def delete(self, key: str) -> bool:
        """
        Supprime une entrée par clé.

        Args:
            key: Clé de l'entrée

        Returns:
            True si l'entrée a été supprimée, False sinon

        """
        ...

    @abstractmethod
    async def exists(self, key: str) -> bool:
        """
        Vérifie si une clé existe.

        Args:
            key: Clé à vérifier

        Returns:
            True si la clé existe, False sinon

        """
        ...

    @abstractmethod
    async def keys(self, pattern: str = "*") -> list[str]:
        """
        Liste les clés correspondant à un motif.

        Args:
            pattern: Motif de correspondance (glob)

        Returns:
            Liste des clés correspondantes

        """
        ...

    @abstractmethod
    async def cleanup(self, ttl_seconds: int = 3600) -> int:
        """
        Nettoie les entrées expirées.

        Args:
            ttl_seconds: Seuil d'âge en secondes

        Returns:
            Nombre d'entrées supprimées

        """
        ...


class BaseCacheAdapter(ABC):
    """
    Interface abstraite pour les adaptateurs de cache.

    Les adaptateurs de cache fournissent un accès rapide aux données
    fréquemment utilisées avec support TTL et verrouillage.
    S'appuie sur les sémantiques Dogpile pour la cohérence.

    Méthodes obligatoires:
        - get_or_create: Récupérer ou créer une entrée cache
        - invalidate: Invalider une entrée par clé
        - clear: Vider le cache entier
        - get_stats: Obtenir les statistiques du cache
    """

    @abstractmethod
    async def get_or_create(
        self,
        key: str,
        creator: Callable[[], Any],
        ttl_seconds: int = 3600,
    ) -> Any:
        """
        Récupère une valeur du cache ou la crée si absente.

        Args:
            key: Clé du cache
            creator: Fonction appelée pour créer la valeur si absente
            ttl_seconds: Durée de vie en secondes

        Returns:
            Valeur en cache

        """
        ...

    @abstractmethod
    async def get(self, key: str) -> Any | None:
        """
        Récupère une valeur du cache sans création.

        Args:
            key: Clé du cache

        Returns:
            Valeur en cache ou None

        """
        ...

    @abstractmethod
    async def set(
        self,
        key: str,
        value: Any,
        ttl_seconds: int = 3600,
    ) -> None:
        """
        Stocke une valeur dans le cache.

        Args:
            key: Clé du cache
            value: Valeur à mettre en cache
            ttl_seconds: Durée de vie en secondes

        """
        ...

    @abstractmethod
    async def invalidate(self, key: str) -> bool:
        """
        Invalide une entrée du cache.

        Args:
            key: Clé de l'entrée à invalider

        Returns:
            True si l'entrée a été invalidée, False sinon

        """
        ...

    @abstractmethod
    async def clear(self) -> None:
        """Vide intégralement le cache."""
        ...

    @abstractmethod
    def get_stats(self) -> dict[str, Any]:
        """
        Retourne les statistiques du cache.

        Returns:
            Dictionnaire de statistiques

        """
        ...
